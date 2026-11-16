//! Jellyfin trick play pictures (seek previews), made in the background while
//! a video plays.
//!
//! Off by default (`TrickplayWhileStreaming`). When on, a play-start report
//! queues a low-priority ffmpeg that reads the file through this server's own
//! stream proxy, decodes key frames only and writes one 320x180 picture every
//! 10 seconds, 25 to a sheet, under `{data_dir}/trickplay/{set id}/{n}.jpg`.
//!
//! A pass starts at the sheet the viewer is on, reads to the end of the file,
//! then comes back for every sheet still missing, and moves (a few times at
//! most) when the viewer jumps to a part with no pictures yet. The set's
//! folder is the only record: sheets on disk are kept and built on, and the
//! set is `done` only once a run reached the end and no sheet is missing.
//!
//! A set belongs to a title and a picture count (see [`set_id`]), so every
//! version of a title whose measured length gives the same number of pictures
//! shares one set, and a version of another length gets its own. One pass runs
//! at a time, a pass is shared by every play of the same set, it gives way
//! while nobody is watching it and carries on when someone reports again, and
//! it stops a grace period after the last of those plays ends.
//!
//! With `TrickplayRoughFirst` also on, a pass over a large file first makes a
//! few single pictures spread across it, one short seek each (see
//! [`rough_order`]), and composes stand-in sheets from them
//! (`rough-{n}.jpg`). The sheet route sends a stand-in only inside its "not
//! ready" answer; the finished sheet is the only success. Stand-ins are never
//! named as sheets, and go when a compose finds their sheet made or the set is
//! done.
//!
//! With `TrickplayOtherVersion` also on, a pass whose file cannot be read goes
//! on with another version of the same title that is certain to give the same
//! pictures (see [`other_versions`]), into the same set: after the file's own
//! read failed, or at once when its addon is not to be read. A version that
//! fails is left alone for [`OTHER_FAILURE_WAIT`], and a pass reads at most
//! [`MAX_SOURCES`] sources. When another version's read ends by itself short
//! of the end with no sign that its source failed (a shorter file), the
//! sheets it made are deleted.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use tokio::{sync::oneshot, task::JoinHandle};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    AppContext,
    addons::Addon,
    api,
    common::HideConsole,
    db,
    ids::UserId,
    services::StreamService,
    stream::{StreamDescriptor, StreamInfo},
};

/// Seconds between two pictures.
pub const INTERVAL_SECS: u64 = 10;
/// Picture width, also the only width served.
pub const WIDTH: u32 = 320;
/// Picture height.
pub const HEIGHT: u32 = 180;
/// Pictures across one sheet.
pub const TILE_WIDTH: u32 = 5;
/// Pictures down one sheet.
pub const TILE_HEIGHT: u32 = 5;
/// Passes that may run at once.
pub const MAX_PASSES: usize = 1;
/// How long a pass outlives the last play of its file.
pub const GRACE: Duration = Duration::from_secs(60);
/// How long a play runs before its pass may start, so the player's first
/// buffering has the source to itself.
pub const START_DELAY: Duration = Duration::from_secs(15);
/// How long after a failed pass the same file gets no new one.
pub const FAILURE_WAIT: Duration = Duration::from_secs(10 * 60);
/// How long another version that could not be read is not read again.
pub const OTHER_FAILURE_WAIT: Duration = Duration::from_secs(24 * 60 * 60);
/// Sources one pass reads at most: the file played and other versions.
pub const MAX_SOURCES: usize = 3;
/// How long the size of another version is waited for before it is read.
pub const SIZE_WAIT: Duration = Duration::from_secs(30);
/// How often a pass checks that someone is still watching it.
pub const WATCH_CHECK: Duration = Duration::from_secs(15);
/// A pass whose plays have all been silent this long gives way to one that
/// is waiting for the slot.
pub const QUIET_WAITING: Duration = Duration::from_secs(60);
/// A pass whose plays have all been silent this long stops even with nobody
/// waiting. It carries on when one of them reports again.
pub const QUIET_ALONE: Duration = Duration::from_secs(5 * 60);
/// Days pictures are kept after they were last shown, unless set otherwise.
pub const DEFAULT_KEEP_DAYS: i64 = 30;
/// Longest keep time that can be set, in days.
pub const MAX_KEEP_DAYS: i64 = 3_650;
/// Shortest runtime that gets pictures.
pub const MIN_RUNTIME_SECS: u64 = 300;
/// Longest runtime that gets pictures.
pub const MAX_RUNTIME_SECS: u64 = 4 * 60 * 60;
/// No pass starts, and a running one stops, below this much free disk.
pub const MIN_FREE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// The shortest wait for the first sheet of a run that starts part way into
/// the file. A source that gives none in time is read from its start.
pub const SEEK_WAIT: Duration = Duration::from_secs(120);
/// A pass follows the viewer to a part with no pictures at most this often.
pub const FOLLOW_EVERY: Duration = Duration::from_secs(30);
/// A pass follows the viewer at most this many times.
pub const MAX_FOLLOWS: u32 = 4;
/// Directory under `data_dir` that holds every set of pictures.
pub const ROOT_DIR: &str = "trickplay";
/// Rough pictures made at most before the detailed ones.
pub const ROUGH_PICTURES: usize = 17;
/// The rough stage ends after this long.
pub const ROUGH_BUDGET: Duration = Duration::from_secs(45);
/// One rough picture is given up after this long.
pub const ROUGH_SEEK_WAIT: Duration = Duration::from_secs(10);
/// The rough stage ends after this many failed pictures in a row.
pub const ROUGH_MAX_FAILS: u32 = 3;
/// Smallest file, in bytes, that gets a rough stage.
pub const ROUGH_MIN_BYTES: u64 = 4 * 1024 * 1024 * 1024;
/// Smallest picture count (60 minutes) that gets a rough stage when the
/// file's size is not known.
pub const ROUGH_MIN_PICTURES: u64 = 360;
/// Fewest sheets still to make for a rough stage, so a set that is nearly
/// whole is not held back.
pub const ROUGH_MIN_MISSING: usize = 4;

/// Seconds of video one sheet covers.
const SHEET_SECS: u64 = INTERVAL_SECS * (TILE_WIDTH * TILE_HEIGHT) as u64;
/// Estimated bits per second of pictures (about 13 KB a picture), as
/// Jellyfin's `Bandwidth`. Clients ignore it.
const BANDWIDTH: i32 = 10_400;
/// Written into a set's directory once it is complete.
const DONE_MARKER: &str = "done";
/// Written into a set's directory once a run has reached the end of the file.
const END_MARKER: &str = "end";
/// Present in a set's directory while a pass is at work there; one left by a
/// crash tells the start-up prune to check the sheets.
const BUSY_MARKER: &str = "busy";
/// Runs one pass makes at most: two (from the viewer to the end, then the
/// beginning), two for each move to the viewer (the run it starts, and one
/// more for the stretch it splits), one after a run that found nothing and
/// one after a run that could not jump.
const MAX_RUNS: u32 = 2 * MAX_FOLLOWS + 4;
/// The slowest read assumed when waiting for the first sheet of a run that
/// starts part way: the wait covers one sheet's share of the file at this
/// rate, in bytes a second.
const SEEK_READ_BYTES_PER_SEC: u64 = 5 * 1024 * 1024;
/// Temporary files of an unfinished write older than this are deleted.
const TMP_MAX_AGE: Duration = Duration::from_secs(10 * 60);
/// A served directory's use time is updated at most this often.
const TOUCH_EVERY: Duration = Duration::from_secs(60);
/// Smallest disk allowance, in MB.
const MIN_CACHE_MB: i64 = 100;
/// How long `kill_all` waits for each pass before killing its process.
const KILL_WAIT: Duration = Duration::from_secs(5);
const TICKS_PER_SEC: i64 = 10_000_000;
/// The rough stage composes its sheets again once this many rough pictures
/// are on disk, and once more at its end if any were made since.
const ROUGH_COMPOSE_AT: [usize; 5] = [2, 3, 5, 9, 17];
/// JPEG quality of a composed rough sheet.
const ROUGH_QUALITY: u8 = 70;

/// Why a stream gets no pictures.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum_macros::Display, strum_macros::EnumString,
)]
#[strum(serialize_all = "snake_case")]
pub enum Ineligible {
    /// No stream row, or one with no stream information.
    NoStream,
    LiveTv,
    Rtsp,
    Catchup,
    Torrent,
    Hls,
    NoRuntime,
    TooShort,
    TooLong,
    Iptv,
    Redirect,
    /// The addon's own switch is off, or its service list leaves the stream
    /// out.
    AddonExcluded,
}

/// Whether the switch is on.
pub fn enabled(cfg: &api::ServerConfiguration) -> bool {
    cfg.trickplay_while_streaming
        .unwrap_or(false)
}

/// Whether rough pictures are made first and sent while a sheet is not
/// ready: both switches on.
pub fn rough_first(cfg: &api::ServerConfiguration) -> bool {
    enabled(cfg)
        && cfg
            .trickplay_rough_first
            .unwrap_or(false)
}

/// Whether another version is read when the one played cannot be: both
/// switches on.
pub fn other_version(cfg: &api::ServerConfiguration) -> bool {
    enabled(cfg)
        && cfg
            .trickplay_other_version
            .unwrap_or(false)
}

/// The disk allowance for pictures, in bytes.
pub fn cap_bytes(cfg: &api::ServerConfiguration) -> u64 {
    let mb = cfg
        .trickplay_cache_size_mb
        .unwrap_or(1024)
        .max(MIN_CACHE_MB);
    u64::try_from(mb).unwrap_or(0) * 1024 * 1024
}

/// How long a set is kept after it was last shown: `None` never deletes by
/// age (0, or a negative value), otherwise at most 3,650 days.
pub fn keep_for(cfg: &api::ServerConfiguration) -> Option<Duration> {
    let days = cfg
        .trickplay_keep_days
        .unwrap_or(DEFAULT_KEEP_DAYS)
        .min(MAX_KEEP_DAYS);
    let days = u64::try_from(days)
        .ok()
        .filter(|d| *d > 0)?;
    Some(Duration::from_secs(days * 24 * 60 * 60))
}

/// `{data_dir}/trickplay`.
pub fn root_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(ROOT_DIR)
}

/// The directory holding one set's sheets.
pub fn set_dir(root: &Path, set_id: Uuid) -> PathBuf {
    root.join(
        set_id
            .as_simple()
            .to_string(),
    )
}

/// Path of sheet `n` of a set.
pub fn sheet_path(root: &Path, set_id: Uuid, n: u32) -> PathBuf {
    sheet_file(&set_dir(root, set_id), n)
}

/// Path of sheet `n` in a set's directory.
fn sheet_file(dir: &Path, n: u32) -> PathBuf {
    dir.join(format!("{n}.jpg"))
}

/// Path of the rough stand-in for sheet `n` of a set (see
/// [`compose_rough`]). Never a name [`parse_sheet`] accepts.
pub fn rough_sheet_path(root: &Path, set_id: Uuid, n: u32) -> PathBuf {
    rough_sheet_file(&set_dir(root, set_id), n)
}

/// Path of the rough stand-in for sheet `n` in a set's directory.
fn rough_sheet_file(dir: &Path, n: u32) -> PathBuf {
    dir.join(format!("rough-{n}.jpg"))
}

/// Path of rough picture `g` (its number on the 10-second grid) in a set's
/// directory.
fn rough_file(dir: &Path, g: u64) -> PathBuf {
    dir.join(format!("r{g}.jpg"))
}

/// Whether bytes are a whole JPEG, as [`sheet_whole`] judges a file.
pub fn jpeg_whole(bytes: &[u8]) -> bool {
    bytes.len() >= 4
        && bytes.starts_with(&[0xFF, 0xD8])
        && bytes.ends_with(&[0xFF, 0xD9])
}

/// The runtime used to judge a stream, in seconds: the probed runtime, then
/// the item's own runtime, then the duration the source listed.
pub fn runtime_s(stream: &db::Media, item_runtime_s: Option<i64>) -> Option<u64> {
    let probed = stream
        .probe_data
        .as_ref()
        .and_then(|p| p.run_time_ticks)
        .map(|ticks| ticks / TICKS_PER_SEC);
    let listed = stream
        .stream_info
        .as_ref()
        .and_then(|info| info.duration);
    probed
        .filter(|s| *s > 0)
        .or(item_runtime_s.filter(|s| *s > 0))
        .or(listed.filter(|s| *s > 0))
        .and_then(|s| u64::try_from(s).ok())
}

/// Pictures a length in ticks gives: one every 10 seconds, rounded up.
/// [`manifest`], [`set_id`] and a pass's own count all use it, so a client
/// is told the count its set is keyed by.
pub fn picture_count(ticks: i64) -> u64 {
    let interval_ticks = INTERVAL_SECS as i64 * TICKS_PER_SEC;
    u64::try_from(ticks.max(0))
        .unwrap_or(0)
        .div_ceil(interval_ticks as u64)
}

/// A stream's own measured length in ticks: probed here or taken from
/// RemuxDB. A length guessed from the file name, or the title's listed
/// runtime stored in its place, is not one.
pub fn measured_ticks(stream: &db::Media) -> Option<i64> {
    let probe = stream
        .probe_data
        .as_ref()?;
    let measured = matches!(
        probe
            .remux
            .as_ref()
            .and_then(|r| r.source),
        Some(api::ProbeOrigin::Ffprobe | api::ProbeOrigin::RemuxDb)
    );
    probe
        .run_time_ticks
        .filter(|t| measured && *t > 0)
}

/// The set a stream's pictures belong to. Versions of one title share a set
/// when their measured lengths give the same picture count, so each is told
/// the same count and the same sheet numbers hold the same moments. Anything
/// else (not measured, no title, not a stream row) keeps a set of its own,
/// named by its own id.
pub fn set_id(stream: &db::Media) -> Uuid {
    match (stream.kind == db::MediaKind::Stream, stream.parent_id) {
        (true, Some(parent)) => measured_ticks(stream).map_or(stream.id, |ticks| {
            Uuid::new_v5(
                &parent,
                format!("trickplay:{}", picture_count(ticks)).as_bytes(),
            )
        }),
        _ => stream.id,
    }
}

/// [`set_id`], except that a stream whose own folder exists keeps it: a set
/// made before the stream was measured, or one moved there because the
/// stream turned out longer than its stored length.
pub fn set_for(root: &Path, stream: &db::Media) -> Uuid {
    if set_dir(root, stream.id).is_dir() {
        stream.id
    } else {
        set_id(stream)
    }
}

/// The checks that need only the stream row. Used both to advertise pictures
/// and before a pass.
pub fn eligible_row(
    media: &db::Media,
    info: &StreamInfo,
    runtime_s: Option<u64>,
) -> Result<(), Ineligible> {
    let under_channel = media
        .parent
        .as_deref()
        .is_some_and(db::Media::is_live);
    if media.is_live() || under_channel {
        return Err(Ineligible::LiveTv);
    }
    if matches!(info.descriptor, StreamDescriptor::Rtsp { .. }) {
        return Err(Ineligible::Rtsp);
    }
    if info
        .catchup_source
        .is_some()
    {
        return Err(Ineligible::Catchup);
    }
    if info.is_p2p() {
        return Err(Ineligible::Torrent);
    }
    let hls_container = media
        .probe_data
        .as_ref()
        .and_then(|p| {
            p.container
                .as_ref()
        })
        .is_some_and(|c| c.is_hls_input());
    let hls_url = info
        .descriptor
        .as_http_url()
        .is_some_and(crate::playback::engine::is_hls_input_url);
    if hls_container || hls_url {
        return Err(Ineligible::Hls);
    }
    match runtime_s {
        None => Err(Ineligible::NoRuntime),
        Some(s) if s < MIN_RUNTIME_SECS => Err(Ineligible::TooShort),
        Some(s) if s > MAX_RUNTIME_SECS => Err(Ineligible::TooLong),
        Some(_) => Ok(()),
    }
}

/// Whether an addon's own switch and service list let a stream get pictures.
/// An empty list allows every stream. A list allows only a stream whose
/// service id or inner addon name equals an entry, ignoring ASCII case, so a
/// stream that names neither gets none.
pub fn addon_allows(
    addon_trickplay: bool,
    filter: &[String],
    service_id: Option<&str>,
    stream_addon: Option<&str>,
) -> bool {
    if !addon_trickplay {
        return false;
    }
    if filter.is_empty() {
        return true;
    }
    [service_id, stream_addon]
        .into_iter()
        .flatten()
        .any(|name| {
            filter
                .iter()
                .any(|entry| entry.eq_ignore_ascii_case(name))
        })
}

/// Whether an addon row is one of the IPTV kinds. A stored row carries its
/// preset id (`iptv-m3u`, `iptv-xtream`); `iptv` is the runtime kind's id.
fn is_iptv_kind(kind: &str) -> bool {
    matches!(kind, "iptv" | "iptv-m3u" | "iptv-xtream")
}

/// The checks that need the addon that produced the stream, before a pass.
/// Item builders use the same checks through [`AddonRule`].
pub fn eligible_pass(
    addon: Option<&Addon>,
    info: &StreamInfo,
) -> Result<(), Ineligible> {
    let Some(addon) = addon else {
        return Ok(());
    };
    if is_iptv_kind(
        &addon
            .preset
            .kind,
    ) {
        return Err(Ineligible::Iptv);
    }
    if addon.http_redirect_stream {
        return Err(Ineligible::Redirect);
    }
    if !addon_allows(
        addon.trickplay,
        &addon.trickplay_service_filter,
        info.service_id
            .as_deref(),
        info.stream_addon
            .as_deref(),
    ) {
        return Err(Ineligible::AddonExcluded);
    }
    Ok(())
}

/// What advertising needs to know about one addon: the checks of
/// [`eligible_pass`], kept in memory so item builders run no query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddonRule {
    pub iptv: bool,
    pub redirect: bool,
    pub trickplay: bool,
    pub service_filter: Vec<String>,
}

impl AddonRule {
    pub fn new(addon: &Addon) -> Self {
        Self {
            iptv: is_iptv_kind(
                &addon
                    .preset
                    .kind,
            ),
            redirect: addon.http_redirect_stream,
            trickplay: addon.trickplay,
            service_filter: addon
                .trickplay_service_filter
                .clone(),
        }
    }

    fn allows(&self, info: &StreamInfo) -> bool {
        !self.iptv
            && !self.redirect
            && addon_allows(
                self.trickplay,
                &self.service_filter,
                info.service_id
                    .as_deref(),
                info.stream_addon
                    .as_deref(),
            )
    }
}

/// Every stored addon's [`AddonRule`], by addon id.
pub type AddonRules = HashMap<Uuid, AddonRule>;

/// Whether the addon that produced a stream lets it be advertised. A stream
/// with no addon, or one whose addon is not in `rules`, is judged by its row
/// alone.
pub fn addon_ok(rules: &AddonRules, info: &StreamInfo) -> bool {
    info.addon_id
        .and_then(|id| rules.get(&id))
        .is_none_or(|rule| rule.allows(info))
}

/// How another version of a title is held, cheapest to read first: a file on
/// this server's disk, one on a library addon's WebDAV server, then one a
/// named service marks as cached, outside usenet before usenet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Held {
    Local,
    Opendal,
    Cached,
    CachedUsenet,
}

/// How a stream is held, or `None` when it may not stand in for another: a
/// plain link with no named service (as IPTV on demand is), one not marked
/// cached, or a torrent. A cached mark with no service is not enough.
fn held(info: &StreamInfo) -> Option<Held> {
    match info.descriptor {
        StreamDescriptor::Local(_) => return Some(Held::Local),
        StreamDescriptor::Opendal { .. } => return Some(Held::Opendal),
        StreamDescriptor::Torrent { .. } | StreamDescriptor::Rtsp { .. } => {
            return None;
        }
        StreamDescriptor::Http { .. } => {}
    }
    let cached = info
        .service_id
        .as_deref()
        .is_some_and(|s| !s.is_empty())
        && info.service_cached == Some(true);
    if !cached {
        return None;
    }
    let usenet = info
        .usenet_guid
        .is_some()
        || info
            .nzb_url
            .is_some();
    Some(if usenet {
        Held::CachedUsenet
    } else {
        Held::Cached
    })
}

/// The other versions of `playing`'s title that may be read in its place:
/// a file on this server's disk, then one on a library addon's WebDAV
/// server, then one a named service marks as cached (outside usenet before
/// usenet), then the smallest listed file, then the listing order. Each must
/// give the same pictures at the same places: its own measured length gives
/// `playing`'s picture count (so none when `playing` has no measured length),
/// it has no folder of its own, it passes the row checks, its addon is in
/// `rules` and allows it, a plain link names a service that marks it cached
/// (a cached mark alone is what IPTV on demand lists), a service's file lists
/// its size, and its stored length is of the file it lists. `versions` are
/// the title's stream rows.
pub fn other_versions<'a>(
    playing: &db::Media,
    versions: &'a [db::Media],
    item_runtime_s: Option<i64>,
    rules: &AddonRules,
    has_own_folder: impl Fn(Uuid) -> bool,
) -> Vec<&'a db::Media> {
    let set = set_id(playing);
    if set == playing.id {
        return Vec::new();
    }
    let mut found: Vec<(Held, i64, i64, &db::Media)> = versions
        .iter()
        .filter(|row| {
            row.id != playing.id
                && row.kind == db::MediaKind::Stream
                && set_id(row) == set
                && !has_own_folder(row.id)
        })
        .filter_map(|row| {
            let info = row
                .stream_info
                .as_ref()?;
            eligible_row(row, info, runtime_s(row, item_runtime_s)).ok()?;
            let allowed = info
                .addon_id
                .and_then(|id| rules.get(&id))
                .is_some_and(|rule| rule.allows(info));
            if !allowed {
                return None;
            }
            let held = held(info)?;
            let size = info
                .size
                .filter(|s| *s > 0);
            if held >= Held::Cached && size.is_none() {
                return None;
            }
            let probed_size = row
                .probe_data
                .as_ref()
                .and_then(|p| p.size);
            if info.is_other_file(probed_size) {
                return None;
            }
            Some((
                held,
                size.unwrap_or(i64::MAX),
                row.idx
                    .unwrap_or(i64::MAX),
                row,
            ))
        })
        .collect();
    found.sort_by_key(|(held, size, idx, _)| (*held, *size, *idx));
    found
        .into_iter()
        .map(|(_, _, _, row)| row)
        .collect()
}

/// Whether `found`, the size in bytes another version's source gave just
/// before it is read, is that of the file it lists (see
/// [`StreamInfo::is_other_file`]). A source that gave none is not read.
fn is_listed_file(info: &StreamInfo, found: Option<u64>) -> bool {
    let found = found
        .and_then(|s| i64::try_from(s).ok())
        .filter(|s| *s > 0);
    found.is_some() && !info.is_other_file(found)
}

/// Jellyfin's `Trickplay` entry for one length in ticks, or `None` when that
/// length is unknown, under 5 minutes or over 4 hours.
fn manifest_entry(
    ticks: Option<i64>,
) -> Option<HashMap<String, api::TrickplayInfoDto>> {
    let ticks = ticks.filter(|t| *t > 0)?;
    let secs = ticks / TICKS_PER_SEC;
    if secs < MIN_RUNTIME_SECS as i64 || ticks > MAX_RUNTIME_SECS as i64 * TICKS_PER_SEC
    {
        return None;
    }
    let info = api::TrickplayInfoDto {
        width: WIDTH as i32,
        height: HEIGHT as i32,
        tile_width: TILE_WIDTH as i32,
        tile_height: TILE_HEIGHT as i32,
        thumbnail_count: i32::try_from(picture_count(ticks)).unwrap_or(i32::MAX),
        interval: (INTERVAL_SECS * 1000) as i32,
        bandwidth: BANDWIDTH,
    };
    Some(HashMap::from([(WIDTH.to_string(), info)]))
}

/// Jellyfin's `Trickplay` field for an item: one entry per media source whose
/// row passes `row_ok`, keyed by the source id in simple form, then by width;
/// with no media sources listed, one entry under the item id. Each source is
/// judged and counted by its own length, the item's when it lists none, so a
/// file holding two episodes or a cut of another length is told its own
/// count. Empty for anything but a film, episode or stream, and for a source
/// under 5 minutes or over 4 hours long.
pub fn manifest(
    dto: &api::BaseItemDto,
    row_ok: impl Fn(&str) -> bool,
) -> HashMap<String, HashMap<String, api::TrickplayInfoDto>> {
    let mut out = HashMap::new();
    let playable = matches!(
        dto.type_,
        api::MediaType::Movie | api::MediaType::Episode | api::MediaType::Video
    );
    if !playable || dto.is_live == Some(true) {
        return out;
    }
    let item_ticks = dto
        .run_time_ticks
        .filter(|t| *t > 0);
    for source in dto
        .media_sources
        .iter()
        .flatten()
    {
        if source.is_infinite_stream {
            continue;
        }
        let ticks = source
            .run_time_ticks
            .filter(|t| *t > 0)
            .or(item_ticks);
        let Some(entry) = manifest_entry(ticks) else {
            continue;
        };
        let id = source
            .id
            .as_simple()
            .to_string();
        if !row_ok(&id) {
            continue;
        }
        out.insert(id, entry);
    }
    // A list asked for without `MediaSources`: the first source a player is
    // given carries the item id, so that is the key it looks up.
    if dto
        .media_sources
        .is_none()
        && let Some(entry) = manifest_entry(item_ticks)
    {
        let id = dto
            .id
            .as_simple()
            .to_string();
        if row_ok(&id) {
            out.insert(id, entry);
        }
    }
    out
}

/// The sheet number in a requested file name: ASCII digits with no sign and
/// no leading zero (except `0` itself), at most 6 of them, then `.jpg`.
pub fn parse_sheet(file: &str) -> Option<u32> {
    let digits = file.strip_suffix(".jpg")?;
    let valid = !digits.is_empty()
        && digits.len() <= 6
        && digits
            .bytes()
            .all(|b| b.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'));
    if !valid {
        return None;
    }
    digits
        .parse()
        .ok()
}

/// What ffmpeg reads: a local file by its path, anything else through this
/// server's own stream proxy, never the source's own URL.
pub fn pass_input(info: &StreamInfo, stream_id: Uuid, port: u16) -> String {
    match &info.descriptor {
        StreamDescriptor::Local(path) => path
            .to_string_lossy()
            .into_owned(),
        _ => format!(
            "http://127.0.0.1:{port}/stream/{stream_id}?token={}",
            crate::stream::stream_link_token(stream_id)
        ),
    }
}

/// ffmpeg's arguments for one run, starting at sheet `start_sheet`. A run
/// that starts part way seeks there (`-ss`, before the input) and numbers its
/// sheets from there; `start_time=0` keeps its pictures on the same
/// 10-second grid as a read from the start.
pub fn pass_args(input: &str, out_dir: &Path, start_sheet: u32) -> Vec<String> {
    let mut args: Vec<String> = [
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-threads",
        "1",
        "-filter_threads",
        "1",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    args.extend(
        crate::playback::engine::ffmpeg_http_input_args(input)
            .iter()
            .map(|s| s.to_string()),
    );
    let filter = format!(
        "fps=1/{INTERVAL_SECS}:start_time=0,\
         scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=decrease,\
         pad={WIDTH}:{HEIGHT}:(ow-iw)/2:(oh-ih)/2,\
         tile={TILE_WIDTH}x{TILE_HEIGHT}"
    );
    let output = out_dir
        .join("%d.jpg")
        .to_string_lossy()
        .into_owned();
    args.extend(["-skip_frame".to_string(), "nokey".to_string()]);
    if start_sheet > 0 {
        args.extend([
            "-ss".to_string(),
            (u64::from(start_sheet) * SHEET_SECS).to_string(),
        ]);
    }
    args.extend([
        "-i".to_string(),
        input.to_string(),
        "-an".to_string(),
        "-sn".to_string(),
        "-dn".to_string(),
        "-vf".to_string(),
        filter,
        "-q:v".to_string(),
        "5".to_string(),
        "-f".to_string(),
        "image2".to_string(),
        "-atomic_writing".to_string(),
        "1".to_string(),
        "-start_number".to_string(),
        start_sheet.to_string(),
        output,
        "-progress".to_string(),
        "pipe:1".to_string(),
        "-nostats".to_string(),
    ]);
    args
}

/// ffmpeg's arguments for one rough picture at `at_s` seconds: a jump there
/// (`-ss` before the input, to the key frame at or before it, kept as found),
/// one key frame decoded, scaled and padded as a sheet's picture, written to
/// `out` once whole.
pub fn grab_args(input: &str, at_s: u64, out: &Path) -> Vec<String> {
    let mut args: Vec<String> = [
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-threads",
        "1",
        "-filter_threads",
        "1",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    args.extend(
        crate::playback::engine::ffmpeg_http_input_args(input)
            .iter()
            .map(|s| s.to_string()),
    );
    args.extend(["-skip_frame".to_string(), "nokey".to_string()]);
    if at_s > 0 {
        args.extend([
            "-ss".to_string(),
            at_s.to_string(),
            "-noaccurate_seek".to_string(),
        ]);
    }
    let filter = format!(
        "scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=decrease,\
         pad={WIDTH}:{HEIGHT}:(ow-iw)/2:(oh-ih)/2"
    );
    args.extend([
        "-i".to_string(),
        input.to_string(),
        "-an".to_string(),
        "-sn".to_string(),
        "-dn".to_string(),
        "-frames:v".to_string(),
        "1".to_string(),
        "-vf".to_string(),
        filter,
        "-q:v".to_string(),
        "5".to_string(),
        "-f".to_string(),
        "image2".to_string(),
        "-atomic_writing".to_string(),
        "1".to_string(),
        "-update".to_string(),
        "1".to_string(),
        out.to_string_lossy()
            .into_owned(),
    ]);
    args
}

/// The rough pictures of a file of `pictures` pictures, at most `cap`, in the
/// order they are made: the first, the last, then the middle of every gap
/// between two already in the list, level by level, left to right. A gap with
/// no picture strictly inside it is left alone, so none repeats.
pub fn rough_order(pictures: u64, cap: usize) -> Vec<u64> {
    let mut order = Vec::new();
    if pictures == 0 || cap == 0 {
        return order;
    }
    order.push(0);
    let last = pictures - 1;
    if last > 0 && order.len() < cap {
        order.push(last);
    }
    let mut gaps = vec![(0, last)];
    while order.len() < cap && !gaps.is_empty() {
        let mut next = Vec::new();
        for (from, to) in gaps {
            if to - from < 2 {
                continue;
            }
            if order.len() >= cap {
                break;
            }
            let middle = from + (to - from) / 2;
            order.push(middle);
            next.push((from, middle));
            next.push((middle, to));
        }
        gaps = next;
    }
    order
}

/// The picture of `made` closest to `g`, the earlier one on a tie.
fn nearest(made: &[u64], g: u64) -> Option<u64> {
    made.iter()
        .copied()
        .min_by_key(|m| (m.abs_diff(g), *m))
}

/// Whether a pass gets a rough stage, once the switch for it is on: a source
/// that can jump within the file, a file of at least [`ROUGH_MIN_BYTES`] (at
/// least [`ROUGH_MIN_PICTURES`] pictures when its size is not known), and at
/// least [`ROUGH_MIN_MISSING`] sheets still to make.
fn rough_stage_wanted(
    can_jump: bool,
    size: Option<u64>,
    pictures: u64,
    missing: usize,
) -> bool {
    let large = match size {
        Some(bytes) => bytes >= ROUGH_MIN_BYTES,
        None => pictures >= ROUGH_MIN_PICTURES,
    };
    can_jump && large && missing >= ROUGH_MIN_MISSING
}

/// The rough pictures on disk (`r{g}.jpg`, whole), below `pictures`, lowest
/// first.
fn rough_made(dir: &Path, pictures: u64) -> Vec<u64> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut made: Vec<u64> = entries
        .flatten()
        .filter_map(|entry| {
            let g = entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix('r'))
                .and_then(parse_sheet)
                .map(u64::from)?;
            (g < pictures && sheet_whole(&entry.path())).then_some(g)
        })
        .collect();
    made.sort_unstable();
    made
}

/// Deletes each rough stand-in whose sheet is on disk and whole.
fn drop_rough_made(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let n = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix("rough-"))
            .and_then(parse_sheet);
        if let Some(n) = n
            && sheet_whole(&sheet_file(dir, n))
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Deletes every rough picture and rough stand-in in a set's directory.
fn drop_rough(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let rough = entry
            .file_name()
            .to_str()
            .is_some_and(|name| {
                name.strip_prefix("rough-")
                    .or_else(|| name.strip_prefix('r'))
                    .and_then(parse_sheet)
                    .is_some()
            });
        if rough {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Composes a rough stand-in for every sheet of a set of `pictures` pictures
/// whose sheet is not on disk: each tile is the rough picture nearest to its
/// place, black past the last picture. Stand-ins of sheets on disk are
/// deleted. Nothing is written with fewer than two rough pictures that
/// decode. Each stand-in is written beside its name and renamed into place.
fn compose_rough(dir: &Path, pictures: u64) -> std::io::Result<()> {
    let mut made: BTreeMap<u64, image::RgbImage> = BTreeMap::new();
    for g in rough_made(dir, pictures) {
        let Ok(bytes) = std::fs::read(rough_file(dir, g)) else {
            continue;
        };
        let Ok(picture) =
            image::load_from_memory_with_format(&bytes, image::ImageFormat::Jpeg)
        else {
            continue;
        };
        let picture = picture.into_rgb8();
        if picture.dimensions() == (WIDTH, HEIGHT) {
            made.insert(g, picture);
        }
    }
    drop_rough_made(dir);
    if made.len() < 2 {
        return Ok(());
    }
    let keys: Vec<u64> = made
        .keys()
        .copied()
        .collect();
    let per_sheet = u64::from(TILE_WIDTH * TILE_HEIGHT);
    for n in 0..=last_sheet(pictures) {
        let Ok(n) = u32::try_from(n) else {
            break;
        };
        if sheet_whole(&sheet_file(dir, n)) {
            continue;
        }
        let mut canvas = image::RgbImage::new(WIDTH * TILE_WIDTH, HEIGHT * TILE_HEIGHT);
        for t in 0..TILE_WIDTH * TILE_HEIGHT {
            let g = u64::from(n) * per_sheet + u64::from(t);
            if g >= pictures {
                break;
            }
            let Some(picture) = nearest(&keys, g).and_then(|k| made.get(&k)) else {
                continue;
            };
            let x = i64::from(t % TILE_WIDTH * WIDTH);
            let y = i64::from(t / TILE_WIDTH * HEIGHT);
            image::imageops::replace(&mut canvas, picture, x, y);
        }
        let mut out = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, ROUGH_QUALITY)
            .encode_image(&canvas)
            .map_err(std::io::Error::other)?;
        let path = rough_sheet_file(dir, n);
        let tmp = dir.join(format!("rough-{n}.jpg.tmp"));
        std::fs::write(&tmp, &out)?;
        std::fs::rename(&tmp, &path)?;
    }
    Ok(())
}

/// [`compose_rough`] off the async threads; a failure is logged and leaves
/// the stand-ins there were.
async fn compose_rough_logged(dir: &Path, pictures: u64) {
    let dir = dir.to_path_buf();
    let composed =
        tokio::task::spawn_blocking(move || compose_rough(&dir, pictures)).await;
    match composed {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "could not compose rough trick play sheets"),
        Err(error) => warn!(%error, "could not compose rough trick play sheets"),
    }
}

/// The number of the last sheet `pictures` fill.
pub fn last_sheet(pictures: u64) -> u64 {
    pictures.saturating_sub(1) / u64::from(TILE_WIDTH * TILE_HEIGHT)
}

/// How many sheets `pictures` fill.
fn expected_sheets(pictures: u64) -> u32 {
    u32::try_from(last_sheet(pictures) + 1).unwrap_or(u32::MAX)
}

/// The sheet holding position `position_s`, at most the last of `expected`.
fn sheet_at(position_s: u64, expected: u32) -> u32 {
    u32::try_from(position_s / SHEET_SECS)
        .unwrap_or(u32::MAX)
        .min(expected.saturating_sub(1))
}

/// What a set's folder holds, as a pass reads it: the whole sheets on disk,
/// and whether a run has reached the end of the file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SetState {
    sheets: BTreeSet<u32>,
    end: bool,
}

/// Whether a sheet is a whole JPEG: not empty, starting with FF D8 and ending
/// with FF D9. A sheet left empty or cut short by a power loss is not, and is
/// made again.
fn sheet_whole(path: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    let mut head = [0u8; 2];
    let mut tail = [0u8; 2];
    file.metadata()
        .is_ok_and(|m| m.len() >= 4)
        && file
            .read_exact(&mut head)
            .is_ok()
        && head == [0xFF, 0xD8]
        && file
            .seek(SeekFrom::End(-2))
            .is_ok()
        && file
            .read_exact(&mut tail)
            .is_ok()
        && tail == [0xFF, 0xD9]
}

/// Reads a set's folder: its whole sheets and the `end` marker.
fn read_set(dir: &Path) -> SetState {
    let mut state = SetState {
        end: dir
            .join(END_MARKER)
            .exists(),
        ..Default::default()
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return state;
    };
    for entry in entries.flatten() {
        let Some(n) = entry
            .file_name()
            .to_str()
            .and_then(parse_sheet)
        else {
            continue;
        };
        if sheet_whole(&entry.path()) {
            state
                .sheets
                .insert(n);
        }
    }
    state
}

/// The sheets a set still needs, lowest first. Once a run has reached the
/// end of the file, every sheet not on disk up to the higher of the highest
/// sheet on disk and the one before the last of `expected`: a run that
/// started at the last sheet and made none reached the end with the sheets
/// before it still to make, and a last sheet may never come. Before that,
/// every sheet of the `expected` that is not on disk, and always the last of
/// them, since the end has not been reached.
fn missing(state: &SetState, expected: u32) -> Vec<u32> {
    let last = expected.saturating_sub(1);
    if state.end {
        let top = state
            .sheets
            .last()
            .copied()
            .unwrap_or(0)
            .max(last.saturating_sub(1));
        return (0..=top)
            .filter(|n| {
                !state
                    .sheets
                    .contains(n)
            })
            .collect();
    }
    (0..=last)
        .filter(|n| {
            *n == last
                || !state
                    .sheets
                    .contains(n)
        })
        .collect()
}

/// One run of ffmpeg in a pass: the sheet it starts at, and how many sheets
/// it makes before it stops, or `None` to read on to the end of the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Run {
    start: u32,
    stop_after: Option<u64>,
}

/// The next run of a pass, or `None` when nothing is missing. A source that
/// cannot jump is read from its start to its end. Otherwise the run starts at
/// the first missing sheet at or after the viewer's, or the first missing
/// sheet when there is none, and covers the unbroken stretch of missing
/// sheets from there, so nothing already on disk is read again; it reads on
/// to the end when that stretch reaches the last expected sheet before any
/// run has reached the end.
fn next_run(
    missing: &[u32],
    viewer: u32,
    can_jump: bool,
    end: bool,
    expected: u32,
) -> Option<Run> {
    if missing.is_empty() {
        return None;
    }
    if !can_jump {
        return Some(Run {
            start: 0,
            stop_after: None,
        });
    }
    let at = missing
        .iter()
        .position(|n| *n >= viewer)
        .unwrap_or(0);
    let start = missing[at];
    let stretch = missing[at..]
        .iter()
        .zip(start..)
        .take_while(|(n, want)| **n == *want)
        .count();
    let stretch = u32::try_from(stretch).unwrap_or(u32::MAX);
    let to_the_end = !end && start.saturating_add(stretch) >= expected;
    Some(Run {
        start,
        stop_after: (!to_the_end).then_some(u64::from(stretch)),
    })
}

/// How a run ended, judged in this order (see [`run_end`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunEnd {
    /// It made the stretch it was given.
    Goal,
    /// Stopped from outside, or by low disk.
    Cancelled,
    /// It read to the end of the file.
    End,
    /// It started part way and made no sheet within its wait.
    NoJump,
    /// It started part way and found the file already over.
    Nothing,
    Failed,
}

/// The exit status ffmpeg 7 gives when its input ended before any picture
/// reached the sheet encoder, as when a run starts past the end of the file:
/// the encoder cannot be opened without a picture, so it ends with
/// AVERROR(EINVAL), -22, which is 234 on Unix (its low byte) and -22 on
/// Windows, and writes no file. An ffmpeg that ends such a run with exit 0
/// is read by its status.
const EMPTY_END_STATUS: [i32; 2] = [234, -22];

/// What ffmpeg 7 writes at `-loglevel error` with [`EMPTY_END_STATUS`] when
/// no picture reached the output before its input ended: the encoder could
/// not open (fftools/ffmpeg_enc.c), or nothing was muxed (fftools/ffmpeg_mux.c
/// of_write_trailer); both end with AVERROR(EINVAL).
const EMPTY_END_LINES: [&str; 2] = [
    "Could not open encoder before EOF",
    "Nothing was written into output file",
];

/// What ffmpeg writes at `-loglevel error` when its input failed rather than
/// ended: a read error it gave up on after its reconnects (which ffmpeg 7
/// then treats as the end of the input, so a run that wrote nothing exits
/// with [`EMPTY_END_STATUS`] all the same), an answer cut short of its
/// length, an HTTP error status, or a connection that failed. "Error reading
/// HTTP response" is not one: a source that closes a request at the file's
/// very end gives it before a clean end.
const SOURCE_ERRORS: [&str; 10] = [
    "Error during demuxing",
    "Read error at pos",
    "Stream ends prematurely",
    "Failed to reconnect",
    "Server returned",
    "Connection refused",
    "Connection reset",
    "Connection timed out",
    "Input/output error",
    "Error opening input",
];

/// The lines of [`SOURCE_ERRORS`] that ffmpeg writes only once it has given
/// up on its input: its reconnects ran out and the read error reached the
/// command line ("Error during demuxing: Input/output error", after the
/// demuxer's own "Read error", which some write as "Read error at pos"), or
/// a reconnect itself failed.
/// "Stream ends prematurely" is not one: ffmpeg writes it before every
/// reconnect, also one that then works. Nor is a bare "Error during
/// demuxing", which a damaged end of a whole file can give too.
const GAVE_UP_ERRORS: [&str; 3] = [
    "Read error at pos",
    "Input/output error",
    "Failed to reconnect",
];

/// Whether a run ended by itself at the end of its input having written no
/// sheet, with no sign that the source failed: exit 0, or
/// [`EMPTY_END_STATUS`] with one of [`EMPTY_END_LINES`], and none of
/// [`SOURCE_ERRORS`] in what ffmpeg wrote.
fn ended_empty(result: &RunResult) -> bool {
    let by_itself = !(result.cancelled || result.reached_goal || result.no_first_sheet);
    let clean_status = result.exit_ok
        || (result
            .exit_code
            .is_some_and(|code| EMPTY_END_STATUS.contains(&code))
            && EMPTY_END_LINES
                .iter()
                .any(|line| {
                    result
                        .stderr
                        .contains(line)
                }));
    by_itself && result.made_sheets == 0 && clean_status && !source_failed(result)
}

/// Whether what ffmpeg wrote has a sign that its source failed (see
/// [`SOURCE_ERRORS`]).
fn source_failed(result: &RunResult) -> bool {
    SOURCE_ERRORS
        .iter()
        .any(|error| {
            result
                .stderr
                .contains(error)
        })
}

/// Whether what ffmpeg wrote shows it gave up on its source (see
/// [`GAVE_UP_ERRORS`]).
fn source_gave_up(result: &RunResult) -> bool {
    GAVE_UP_ERRORS
        .iter()
        .any(|error| {
            result
                .stderr
                .contains(error)
        })
}

/// Judges a run. `End` needs ffmpeg to have ended by itself, and either exit
/// 0 with the count's last sheet or the sheet before it (a last sheet may
/// never come) and no sign that it gave up on its source (a source cut off
/// there exits 0 too, having flushed a partly filled sheet; see
/// [`GAVE_UP_ERRORS`]), or no sheet from a run that started at the last (see
/// [`ended_empty`]). A run that ends short of that by itself is a
/// failure, as a source cut off part way also exits 0, except one that
/// started part way and ended empty: the file ends before that place.
fn run_end(result: &RunResult, run: Run, pictures: u64) -> RunEnd {
    let last = last_sheet(pictures);
    let start = u64::from(run.start);
    if result.reached_goal {
        return RunEnd::Goal;
    }
    if result.cancelled {
        return RunEnd::Cancelled;
    }
    let empty = ended_empty(result);
    let reached = match result.made_sheets {
        0 => empty && start >= last,
        made => {
            let highest = start + made - 1;
            result.exit_ok
                && highest >= last.saturating_sub(1)
                && !source_gave_up(result)
        }
    };
    if reached {
        return RunEnd::End;
    }
    if result.no_first_sheet {
        return RunEnd::NoJump;
    }
    if empty && start > 0 {
        return RunEnd::Nothing;
    }
    RunEnd::Failed
}

/// How long a run that starts part way waits for its first sheet: one
/// sheet's share of the file at [`SEEK_READ_BYTES_PER_SEC`], at least
/// `at_least`.
fn first_sheet_wait(size: Option<u64>, expected: u32, at_least: Duration) -> Duration {
    let share = size.unwrap_or(0) / u64::from(expected.max(1));
    at_least.max(Duration::from_secs(share / SEEK_READ_BYTES_PER_SEC))
}

/// Whether a run should stop so the pass starts again at `viewer`, the sheet
/// the viewer is now on: the source can jump, that sheet is not on disk and
/// is not one of the next two this run makes (`next` is the next), the run
/// is at least `follow_every` old, and the pass has moved fewer than
/// `max_follows` times.
fn follows_to(
    viewer: u32,
    on_disk: bool,
    next: u32,
    run_age: Duration,
    follows: u32,
    can_jump: bool,
    limits: &Limits,
) -> bool {
    can_jump
        && !on_disk
        && !(next..next.saturating_add(2)).contains(&viewer)
        && run_age >= limits.follow_every
        && follows < limits.max_follows
}

/// Estimated disk use of a runtime's pictures (about 330 KB a sheet).
fn estimate_bytes(runtime_s: u64) -> u64 {
    runtime_s * 330 * 1024 / 250
}

/// One sheet line of ffmpeg's `-progress` output.
fn progress_frame(line: &str) -> Option<u64> {
    line.strip_prefix("frame=")?
        .trim()
        .parse()
        .ok()
}

/// Free bytes on the filesystem holding `path`.
#[cfg(unix)]
fn free_bytes(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(
        path.as_os_str()
            .as_bytes(),
    )
    .ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `c_path` is a valid NUL-terminated string and `stat` points to
    // writable memory of the right type; it is read only after success.
    let stat = unsafe {
        if libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) != 0 {
            return None;
        }
        stat.assume_init()
    };
    #[allow(clippy::useless_conversion)]
    let free = u64::from(stat.f_bavail) * u64::from(stat.f_frsize);
    Some(free)
}

#[cfg(not(unix))]
fn free_bytes(_path: &Path) -> Option<u64> {
    None
}

#[cfg(unix)]
fn lower_priority(pid: u32) {
    // SAFETY: plain system call on a process this server just started.
    let ret = unsafe { libc::setpriority(libc::PRIO_PROCESS, pid as libc::id_t, 19) };
    if ret != 0 {
        debug!(
            error = %std::io::Error::last_os_error(),
            "could not lower trick play pass priority"
        );
    }
}

#[cfg(not(unix))]
fn lower_priority(_pid: u32) {}

#[cfg(unix)]
fn kill_pid(pid: u32) {
    if pid == 0 {
        return;
    }
    // SAFETY: plain system call; the pid is that of a pass this server started.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGKILL);
    }
}

#[cfg(not(unix))]
fn kill_pid(_pid: u32) {}

/// Whether a set's folder is marked complete.
pub(crate) fn is_done(dir: &Path) -> bool {
    dir.join(DONE_MARKER)
        .exists()
}

/// Deletes sheet `n`, if it exists.
fn remove_sheet(dir: &Path, n: u32) {
    let _ = std::fs::remove_file(sheet_file(dir, n));
}

/// Deletes `sheets`, which another version made before it was found not to
/// hold the set's file (see [`TrickplayPasses::run_pass`]).
fn drop_on_trial(plan: &PassPlan, dir: &Path, sheets: &[u32]) {
    if sheets.is_empty() {
        return;
    }
    for n in sheets {
        remove_sheet(dir, *n);
    }
    info!(
        set_id = %plan.set_id,
        stream_id = %plan.stream_id,
        sheets = sheets.len(),
        "trick play: another version's read ended short; its pictures are made again from the next"
    );
}

/// Deletes the sheet written last, if it was written at or after `since`,
/// and gives its number.
fn drop_newest_since(dir: &Path, since: SystemTime) -> Option<u32> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return None;
    };
    let newest = entries
        .flatten()
        .filter_map(|e| {
            let n = e
                .file_name()
                .to_str()
                .and_then(parse_sheet)?;
            let written = e
                .metadata()
                .ok()?
                .modified()
                .ok()?;
            (written >= since).then_some((written, n))
        })
        .max();
    let (_, n) = newest?;
    remove_sheet(dir, n);
    Some(n)
}

/// Clears what a pass cut off by a crash may have left in a set's folder,
/// which still holds `busy`: sheets that are not whole, and the sheet written
/// last since the pass began, which may be partly filled if ffmpeg reached
/// the end of its input as it stopped. When that was the highest sheet, the
/// `end` it may have given goes with it, so the next pass reads the end
/// again. A finished set only loses the marker. The folder's time, which
/// records when the set was last shown, is kept.
fn mend_after_crash(dir: &Path) {
    let used = std::fs::metadata(dir).and_then(|m| m.modified());
    let busy = dir.join(BUSY_MARKER);
    if !is_done(dir)
        && let Ok(since) = std::fs::metadata(&busy).and_then(|m| m.modified())
    {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let is_sheet = entry
                    .file_name()
                    .to_str()
                    .and_then(parse_sheet)
                    .is_some();
                if is_sheet && !sheet_whole(&entry.path()) {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        if let Some(n) = drop_newest_since(dir, since)
            && read_set(dir)
                .sheets
                .last()
                .is_none_or(|highest| *highest < n)
        {
            let _ = std::fs::remove_file(dir.join(END_MARKER));
        }
    }
    let _ = std::fs::remove_file(&busy);
    if let (Ok(used), Ok(file)) = (used, std::fs::File::open(dir)) {
        let _ = file.set_modified(used);
    }
}

/// Removes stale temporary files in one stream's directory and returns the
/// size of what is left.
fn sweep_dir(dir: &Path, now: SystemTime) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut size = 0;
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let is_tmp = entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.ends_with(".tmp"));
        let age = meta
            .modified()
            .ok()
            .and_then(|m| {
                now.duration_since(m)
                    .ok()
            })
            .unwrap_or_default();
        if is_tmp && age > TMP_MAX_AGE {
            let _ = std::fs::remove_file(entry.path());
            continue;
        }
        size += meta.len();
    }
    size
}

/// Frees disk under `root`: removes temporary files older than 10 minutes,
/// then sets not in `active` unused for longer than `keep_for` (never, when it
/// is `None`), then the least recently used sets not in `active` until the
/// total is at most `cap_bytes`. Only directories named by an id are touched.
pub fn prune(
    root: &Path,
    cap_bytes: u64,
    keep_for: Option<Duration>,
    active: &HashSet<Uuid>,
    now: SystemTime,
) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let mut total: u64 = 0;
    let mut candidates: Vec<(PathBuf, SystemTime, u64)> = Vec::new();
    for entry in entries.flatten() {
        let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|name| Uuid::parse_str(name).ok())
        else {
            continue;
        };
        // Read before sweeping: removing a file changes the directory's time.
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if !meta.is_dir() {
            continue;
        }
        let used = meta
            .modified()
            .unwrap_or(now);
        let path = entry.path();
        let size = sweep_dir(&path, now);
        if active.contains(&id) {
            total += size;
            continue;
        }
        let age = now
            .duration_since(used)
            .unwrap_or_default();
        if keep_for.is_some_and(|keep| age > keep) {
            let _ = std::fs::remove_dir_all(&path);
            continue;
        }
        total += size;
        candidates.push((path, used, size));
    }
    candidates.sort_by_key(|(_, used, _)| *used);
    for (path, _, size) in candidates {
        if total <= cap_bytes {
            break;
        }
        if std::fs::remove_dir_all(path).is_ok() {
            total = total.saturating_sub(size);
        }
    }
}

/// The prune at server start. No pass can be running then. With the switch
/// off, every picture is deleted.
pub fn prune_on_start(
    root: &Path,
    enabled: bool,
    cap_bytes: u64,
    keep_for: Option<Duration>,
    now: SystemTime,
) {
    if !root.exists() {
        return;
    }
    if !enabled {
        let _ = std::fs::remove_dir_all(root);
        return;
    }
    prune(root, cap_bytes, keep_for, &HashSet::new(), now);
    // Only a set a crash left `busy` is checked; any other set, finished or
    // not, holds whole sheets and is left as it is.
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let dir = entry.path();
        if dir
            .join(BUSY_MARKER)
            .is_file()
        {
            mend_after_crash(&dir);
        }
    }
}

/// Refills the advertising snapshot from the stored addons. Called at startup
/// and whenever an addon is created, changed or deleted.
pub async fn refresh_addon_rules(ctx: &AppContext) {
    match Addon::list(&ctx.db).await {
        Ok(addons) => ctx
            .sessions
            .trickplay
            .set_addon_rules(&addons),
        Err(error) => warn!(%error, "trick play: could not read the addons"),
    }
}

/// [`prune_on_start`] with the saved settings.
pub async fn prune_at_startup(ctx: &AppContext) {
    let cfg = db::Settings::get_config_or_default(&ctx.db).await;
    let root = root_dir(
        &ctx.config
            .data_dir,
    );
    let on = enabled(&cfg);
    let cap = cap_bytes(&cfg);
    let keep = keep_for(&cfg);
    let _ = tokio::task::spawn_blocking(move || {
        prune_on_start(&root, on, cap, keep, SystemTime::now())
    })
    .await;
}

/// What a play-start report hands to [`start_for_play`].
#[derive(Debug, Clone)]
pub struct PlayStart {
    /// The id of the play session as stored, never a client's missing one.
    pub play_session_id: String,
    pub item_id: Uuid,
    pub media_source_id: Option<Uuid>,
    pub device_id: String,
    pub user_id: Uuid,
}

/// Registers the play at once, then resolves its file and queues a pass in
/// the background. Call only with the switch on.
pub fn start_for_play(ctx: &AppContext, play: PlayStart) {
    ctx.sessions
        .trickplay
        .register(&play.play_session_id, play.item_id, &play.device_id);
    let ctx = ctx.clone();
    tokio::spawn(async move {
        prepare(ctx, play).await;
    });
}

async fn prepare(ctx: AppContext, play: PlayStart) {
    let passes = ctx
        .sessions
        .trickplay
        .clone();
    let plan = match resolve_plan(&ctx, &play).await {
        Ok(plan) => plan,
        Err(reason) => {
            debug!(
                item_id = %play.item_id,
                %reason,
                "no trick play pictures for this play"
            );
            passes.release(&play.play_session_id);
            return;
        }
    };
    let sessions = ctx
        .sessions
        .clone();
    passes
        .begin(
            &play.play_session_id,
            plan,
            Arc::new(FfmpegRunner),
            move |device_id: &str, item_id: Uuid| {
                watched_from(sessions.get_by_device(device_id), item_id)
            },
        )
        .await;
}

/// Whether a device's latest reported session is on `item_id`, and if so
/// which play it is, how long ago it last reported and where it was. Progress
/// reports and pings both count as a report.
fn watched_from(
    latest: Option<crate::playback_session::PlaybackSession>,
    item_id: Uuid,
) -> Option<Watched> {
    let session = latest.filter(|s| s.item_id == item_id)?;
    let quiet = (chrono::Utc::now() - session.last_activity)
        .to_std()
        .unwrap_or_default();
    let position_s = u64::try_from(session.position_ticks / TICKS_PER_SEC).unwrap_or(0);
    Some(Watched {
        quiet,
        position_s,
        play_session_id: session.play_session_id,
    })
}

/// The set a stored stream's pictures belong to (see [`set_for`]); the id
/// itself when no stream row has it. With it, the picture count the set is
/// keyed by, when it is keyed by one. One row read by primary key.
pub async fn set_for_stream(ctx: &AppContext, stream_id: Uuid) -> (Uuid, Option<u64>) {
    let root = root_dir(
        &ctx.config
            .data_dir,
    );
    let Some(stream) = stored_stream(ctx, stream_id).await else {
        return (stream_id, None);
    };
    let set_id = set_for(&root, &stream);
    let pictures = measured_ticks(&stream)
        .filter(|_| set_id != stream.id)
        .map(picture_count);
    (set_id, pictures)
}

/// The stream row with this id, if it is one.
async fn stored_stream(ctx: &AppContext, id: Uuid) -> Option<db::Media> {
    db::Media::get_by_id(&ctx.db, &id)
        .await
        .ok()
        .flatten()
        .filter(|m| m.kind == db::MediaKind::Stream)
}

/// Finds the file a play reads the way the stream route does, and checks it.
async fn resolve_plan(
    ctx: &AppContext,
    play: &PlayStart,
) -> Result<PassPlan, Ineligible> {
    let (owner, source) = StreamService::version_target(
        ctx,
        Some(UserId(play.user_id)),
        play.item_id,
        play.media_source_id,
    )
    .await;
    let selected = StreamService::selected_stream_id(
        ctx,
        Some(
            play.device_id
                .as_str(),
        ),
        owner,
        source,
        Some(
            play.play_session_id
                .as_str(),
        ),
    )
    // A client that sent no play session id was given one by the session
    // start, which PlaybackInfo never saw: follow the device's pick, as the
    // stream route does without one.
    .or_else(|| {
        StreamService::selected_stream_id(
            ctx,
            Some(
                play.device_id
                    .as_str(),
            ),
            owner,
            source,
            None,
        )
    });
    let requested = selected.or(source);
    let stream = match stored_stream(ctx, requested.unwrap_or(owner)).await {
        Some(stream) => stream,
        None => StreamService::lookup(
            ctx,
            owner,
            requested,
            Some(
                play.device_id
                    .as_str(),
            ),
            Some(play.user_id),
        )
        .await
        .map_err(|_| Ineligible::NoStream)?,
    };
    let item = db::Media::get_by_id(&ctx.db, &owner)
        .await
        .ok()
        .flatten();
    if item
        .as_ref()
        .is_some_and(db::Media::is_live)
    {
        return Err(Ineligible::LiveTv);
    }
    let info = stream
        .stream_info
        .as_ref()
        .ok_or(Ineligible::NoStream)?;
    let runtime = runtime_s(
        &stream,
        item.as_ref()
            .and_then(|i| i.runtime),
    );
    eligible_row(&stream, info, runtime)?;
    let addon = match info.addon_id {
        Some(id) => Addon::get(&ctx.db, id)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    let refused = eligible_pass(addon.as_ref(), info).err();
    let runtime_s = runtime.ok_or(Ineligible::NoRuntime)?;
    let cfg = db::Settings::get_config_or_default(&ctx.db).await;
    if let Some(reason) = refused
        && !other_version(&cfg)
    {
        return Err(reason);
    }
    let root = root_dir(
        &ctx.config
            .data_dir,
    );
    // Counted from the same length a media source lists: the probed ticks,
    // else the runtime in whole seconds.
    let ticks = stream
        .probe_data
        .as_ref()
        .and_then(|p| p.run_time_ticks)
        .filter(|t| *t > 0)
        .unwrap_or_else(|| {
            i64::try_from(runtime_s)
                .unwrap_or(i64::MAX)
                .saturating_mul(TICKS_PER_SEC)
        });
    let pictures = picture_count(ticks);
    let rough = rough_first(&cfg);
    let estimate = estimate_bytes(runtime_s);
    // Rough stand-ins add about half again while the set is unfinished.
    let estimate = if rough {
        estimate + estimate / 2
    } else {
        estimate
    };
    let set_id = set_for(&root, &stream);
    let others = if other_version(&cfg) && set_id != stream.id {
        other_sources(ctx, &root, &stream, play.user_id).await
    } else {
        Vec::new()
    };
    let plan = PassPlan {
        stream_id: stream.id,
        set_id,
        input: pass_input(
            info,
            stream.id,
            ctx.config
                .port,
        ),
        root,
        pictures,
        size: info
            .size
            .and_then(|s| u64::try_from(s).ok())
            .filter(|s| *s > 0),
        cap_bytes: cap_bytes(&cfg).saturating_sub(estimate),
        keep_for: keep_for(&cfg),
        rough,
        db: Some(
            ctx.db
                .clone(),
        ),
        others,
        other_version: None,
    };
    read_other_if_refused(plan, refused)
}

/// `plan` as it reads the version played, or, when its addon says not to read
/// it (`refused`), as it reads the first of its other versions instead: that
/// version is never opened. With no other version, there is no pass.
fn read_other_if_refused(
    mut plan: PassPlan,
    refused: Option<Ineligible>,
) -> Result<PassPlan, Ineligible> {
    let Some(reason) = refused else {
        return Ok(plan);
    };
    if plan
        .others
        .is_empty()
    {
        return Err(reason);
    }
    let first = plan
        .others
        .remove(0);
    info!(
        set_id = %plan.set_id,
        stream_id = %first.stream_id,
        %reason,
        "trick play: this version is not read; reading another version of this title for its pictures"
    );
    plan.read_other(first);
    Ok(plan)
}

/// [`other_versions`] of a stored stream, from its title's stream rows as
/// last listed, ready to be read. A user limited to some addons is never read
/// another addon's file for.
async fn other_sources(
    ctx: &AppContext,
    root: &Path,
    playing: &db::Media,
    user_id: Uuid,
) -> Vec<PassSource> {
    let Some(parent) = playing.parent_id else {
        return Vec::new();
    };
    let Ok(limited_to) = crate::addons::user_addon_override(&ctx.db, user_id).await
    else {
        return Vec::new();
    };
    let Some(mut title) = db::Media::get_by_id(&ctx.db, &parent)
        .await
        .ok()
        .flatten()
    else {
        return Vec::new();
    };
    let versions = match title
        .streams(&ctx.db)
        .await
    {
        Ok(versions) => versions,
        Err(error) => {
            warn!(%error, "trick play: could not list the other versions");
            return Vec::new();
        }
    };
    let rules = ctx
        .sessions
        .trickplay
        .addon_rules();
    other_versions(playing, &versions, title.runtime, &rules, |id| {
        set_dir(root, id).is_dir()
    })
    .into_iter()
    .filter_map(|row| {
        let info = row
            .stream_info
            .as_ref()?;
        if let Some(ids) = &limited_to
            && !info
                .addon_id
                .is_some_and(|id| ids.contains(&id))
        {
            return None;
        }
        Some(PassSource {
            stream_id: row.id,
            input: pass_input(
                info,
                row.id,
                ctx.config
                    .port,
            ),
            info: info.clone(),
        })
    })
    .collect()
}

/// Another version of a title, as a pass reads it (see [`other_versions`]).
#[derive(Debug, Clone)]
pub(crate) struct PassSource {
    pub stream_id: Uuid,
    pub input: String,
    /// Its row as listed: its addon, and its size for the size check.
    pub info: StreamInfo,
}

/// Everything a queued pass needs.
#[derive(Debug, Clone)]
pub(crate) struct PassPlan {
    /// The stream read, for its proxy input, the logs and the failure wait:
    /// the version played, or another (see `other_version`).
    pub stream_id: Uuid,
    /// The set its pictures belong to (see [`set_for`]).
    pub set_id: Uuid,
    pub input: String,
    pub root: PathBuf,
    /// The picture count the set is keyed by.
    pub pictures: u64,
    /// The file's size in bytes, when the source listed it.
    pub size: Option<u64>,
    /// Disk allowance left for other sets once this one is made.
    pub cap_bytes: u64,
    /// See [`keep_for`].
    pub keep_for: Option<Duration>,
    /// Make rough pictures first (see [`rough_first`]).
    pub rough: bool,
    /// Where the switches are read again just before the pass starts, since a
    /// queued pass may wait long after its play started. `None` in tests.
    pub db: Option<sqlx::SqlitePool>,
    /// Other versions still to try, in order, when the stream read cannot be
    /// (see [`other_versions`]). Empty with `TrickplayOtherVersion` off.
    pub others: Vec<PassSource>,
    /// The listing of the stream in `stream_id` while it is another version
    /// read in place of the one played; `None` while that one is read.
    pub other_version: Option<StreamInfo>,
}

impl PassPlan {
    /// Reads `source`, another version of the title, from now on. The set and
    /// its picture count stay the played version's.
    fn read_other(&mut self, source: PassSource) {
        self.stream_id = source.stream_id;
        self.input = source.input;
        self.size = source
            .info
            .size
            .and_then(|s| u64::try_from(s).ok())
            .filter(|s| *s > 0);
        self.other_version = Some(source.info);
    }
}

/// What a runner is given: one run of a pass.
#[derive(Debug, Clone)]
pub(crate) struct PassJob {
    pub input: String,
    pub out_dir: PathBuf,
    /// Set to the child's pid once it runs.
    pub pid: Arc<AtomicU32>,
    pub min_free_bytes: u64,
    /// The sheet the run starts at (see [`pass_args`]).
    pub start_sheet: u32,
    /// Stop once this many sheets are made, if set.
    pub stop_after: Option<u64>,
    /// Give up when no sheet is made within this long, if set: only for a
    /// run that starts part way.
    pub first_sheet_within: Option<Duration>,
    /// The sheets made so far, kept up to date while it runs.
    pub made: Arc<AtomicU32>,
}

/// How a runner ended.
#[derive(Debug, Clone, Default)]
pub(crate) struct RunResult {
    pub exit_ok: bool,
    /// ffmpeg's exit status, when it exited by itself (see [`ended_empty`]).
    pub exit_code: Option<i32>,
    /// The last `frame=` value of `-progress`, which counts sheets.
    pub made_sheets: u64,
    /// Stopped by a cancel or by low disk, not by its own end.
    pub cancelled: bool,
    /// With `cancelled`: ffmpeg's output had already ended before the stop
    /// came, so it may have flushed a partly filled last sheet.
    pub ended_itself: bool,
    /// Stopped after making `stop_after` sheets, every one of them whole.
    pub reached_goal: bool,
    /// Stopped because `first_sheet_within` passed with no sheet.
    pub no_first_sheet: bool,
    pub stderr: String,
}

/// What a runner is given for one rough picture.
#[derive(Debug, Clone)]
pub(crate) struct GrabJob {
    pub input: String,
    /// Where in the file, in seconds.
    pub at_s: u64,
    /// The picture's file (see [`rough_file`]).
    pub out: PathBuf,
    /// Given up after this long.
    pub wait: Duration,
    /// Set to the child's pid once it runs.
    pub pid: Arc<AtomicU32>,
}

/// How one rough picture ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Grab {
    /// The picture is on disk and whole.
    Made,
    /// ffmpeg ended by itself without a whole picture.
    Failed,
    /// Killed at the end of its wait.
    TimedOut,
    Cancelled,
}

/// Makes the pictures of one run, and single rough pictures; ffmpeg in
/// production, a fake in tests. Each must stop, and wait for its process to
/// exit, once `cancel` resolves (a stop sent or the sender dropped).
pub(crate) trait PassRunner: Send + Sync + 'static {
    fn run(
        &self,
        job: PassJob,
        cancel: oneshot::Receiver<()>,
    ) -> impl Future<Output = RunResult> + Send;

    fn grab(
        &self,
        job: GrabJob,
        cancel: oneshot::Receiver<()>,
    ) -> impl Future<Output = Grab> + Send;

    /// The size in bytes of the file at `input`, as its source gives it now,
    /// or `None` when it gives none.
    fn size(&self, input: &str) -> impl Future<Output = Option<u64>> + Send;
}

/// Runs ffmpeg at low priority with [`pass_args`] and [`grab_args`].
pub(crate) struct FfmpegRunner;

impl PassRunner for FfmpegRunner {
    fn run(
        &self,
        job: PassJob,
        cancel: oneshot::Receiver<()>,
    ) -> impl Future<Output = RunResult> + Send {
        run_ffmpeg(job, cancel)
    }

    fn grab(
        &self,
        job: GrabJob,
        cancel: oneshot::Receiver<()>,
    ) -> impl Future<Output = Grab> + Send {
        run_grab(job, cancel)
    }

    fn size(&self, input: &str) -> impl Future<Output = Option<u64>> + Send {
        source_size(input.to_string())
    }
}

/// The client of [`source_size`], for this server's own stream proxy.
static SIZE_CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(|| {
        reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(5))
            .timeout(SIZE_WAIT)
            .build()
            .expect("failed to build the trick play size client")
    });

/// The size of the file at a pass input (see [`pass_input`]): a local file's
/// own, or the total a request for its first byte through the stream proxy
/// is answered with. Only the answer's head is read.
async fn source_size(input: String) -> Option<u64> {
    if !input.starts_with("http://") && !input.starts_with("https://") {
        return tokio::fs::metadata(&input)
            .await
            .ok()
            .filter(|m| m.is_file())
            .map(|m| m.len());
    }
    let response = SIZE_CLIENT
        .get(&input)
        .header(http::header::RANGE, "bytes=0-0")
        .send()
        .await
        .ok()?;
    let status = response.status();
    if status == http::StatusCode::PARTIAL_CONTENT {
        response
            .headers()
            .get(http::header::CONTENT_RANGE)?
            .to_str()
            .ok()?
            .rsplit_once('/')?
            .1
            .trim()
            .parse()
            .ok()
    } else if status == http::StatusCode::OK {
        response.content_length()
    } else {
        None
    }
}

/// One rough picture by ffmpeg. Judged by its file alone: whole once ffmpeg
/// has ended means made, whatever the exit status.
async fn run_grab(job: GrabJob, mut cancel: oneshot::Receiver<()>) -> Grab {
    use tokio::io::AsyncReadExt;

    let mut cmd = tokio::process::Command::new(crate::tools::ffmpeg());
    cmd.hide_console();
    cmd.kill_on_drop(true);
    cmd.args(grab_args(&job.input, job.at_s, &job.out));
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            debug!(%error, "could not start ffmpeg for a rough trick play picture");
            return Grab::Failed;
        }
    };
    if let Some(pid) = child.id() {
        job.pid
            .store(pid, Ordering::SeqCst);
        lower_priority(pid);
    }
    let stderr_task = child
        .stderr
        .take()
        .map(|mut stderr| {
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let _ = stderr
                    .read_to_end(&mut buf)
                    .await;
                String::from_utf8_lossy(&buf).into_owned()
            })
        });
    let mut cancelled = false;
    let exited = tokio::select! {
        status = child.wait() => Some(status.ok().and_then(|s| s.code())),
        _ = tokio::time::sleep(job.wait) => None,
        _ = &mut cancel => {
            cancelled = true;
            None
        }
    };
    if exited.is_none() {
        let _ = child.start_kill();
        let _ = child
            .wait()
            .await;
    }
    job.pid
        .store(0, Ordering::SeqCst);
    let stderr = match stderr_task {
        Some(task) => task
            .await
            .unwrap_or_default(),
        None => String::new(),
    };
    match exited {
        _ if cancelled => Grab::Cancelled,
        None if sheet_whole(&job.out) => Grab::Made,
        None => {
            debug!(at_s = job.at_s, "rough trick play picture timed out");
            Grab::TimedOut
        }
        Some(_) if sheet_whole(&job.out) => Grab::Made,
        Some(code) => {
            debug!(
                at_s = job.at_s,
                exit_code = ?code,
                stderr = %crate::playback::probe::redact_urls_in(&stderr),
                "no rough trick play picture"
            );
            Grab::Failed
        }
    }
}

async fn run_ffmpeg(job: PassJob, mut cancel: oneshot::Receiver<()>) -> RunResult {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};

    let mut cmd = tokio::process::Command::new(crate::tools::ffmpeg());
    cmd.hide_console();
    cmd.kill_on_drop(true);
    cmd.args(pass_args(&job.input, &job.out_dir, job.start_sheet));
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            return RunResult {
                stderr: format!("failed to start ffmpeg: {e}"),
                ..Default::default()
            };
        }
    };
    if let Some(pid) = child.id() {
        job.pid
            .store(pid, Ordering::SeqCst);
        lower_priority(pid);
    }
    let stderr_task = child
        .stderr
        .take()
        .map(|mut stderr| {
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let _ = stderr
                    .read_to_end(&mut buf)
                    .await;
                String::from_utf8_lossy(&buf).into_owned()
            })
        });

    let mut made_sheets = 0;
    let mut next_check = 10;
    let mut cancelled = false;
    let mut low_disk = false;
    let mut reached_goal = false;
    let mut no_first_sheet = false;
    let first_sheet_by = job
        .first_sheet_within
        .map(|wait| tokio::time::Instant::now() + wait);
    if let Some(stdout) = child
        .stdout
        .take()
    {
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        loop {
            tokio::select! {
                _ = &mut cancel => {
                    cancelled = true;
                    break;
                }
                _ = tokio::time::sleep_until(
                    first_sheet_by.unwrap_or_else(tokio::time::Instant::now)
                ), if first_sheet_by.is_some() && made_sheets == 0 => {
                    no_first_sheet = true;
                    break;
                }
                line = lines.next_line() => {
                    let Ok(Some(line)) = line else {
                        break;
                    };
                    // Each block of progress lines ends with `progress=continue`
                    // or `progress=end`. A sheet counted in a `continue` block
                    // was a full one: a partly filled sheet is only written at
                    // the end of the input.
                    if line.trim() == "progress=continue"
                        && job
                            .stop_after
                            .is_some_and(|goal| made_sheets >= goal)
                    {
                        reached_goal = true;
                        break;
                    }
                    let Some(n) = progress_frame(&line) else {
                        continue;
                    };
                    made_sheets = n;
                    job.made
                        .store(u32::try_from(n).unwrap_or(u32::MAX), Ordering::SeqCst);
                    if made_sheets >= next_check {
                        next_check = made_sheets + 10;
                        let free = free_bytes(&job.out_dir);
                        if free.is_some_and(|b| b < job.min_free_bytes) {
                            low_disk = true;
                            break;
                        }
                    }
                }
            }
        }
    }

    let mut ended_itself = false;
    let mut exit_code = None;
    let exit_ok = if cancelled || low_disk || reached_goal || no_first_sheet {
        let _ = child.start_kill();
        let _ = child
            .wait()
            .await;
        false
    } else {
        tokio::select! {
            status = child.wait() => {
                exit_code = status
                    .as_ref()
                    .ok()
                    .and_then(|s| s.code());
                status.is_ok_and(|s| s.success())
            }
            _ = &mut cancel => {
                cancelled = true;
                ended_itself = true;
                let _ = child.start_kill();
                let _ = child
                    .wait()
                    .await;
                false
            }
        }
    };
    job.pid
        .store(0, Ordering::SeqCst);
    if low_disk {
        warn!(
            "stopping trick play pictures: less than {} GB of disk is free",
            job.min_free_bytes / (1024 * 1024 * 1024)
        );
    }
    let stderr = match stderr_task {
        Some(task) => task
            .await
            .unwrap_or_default(),
        None => String::new(),
    };
    RunResult {
        exit_ok,
        exit_code,
        made_sheets,
        cancelled: cancelled || low_disk,
        ended_itself,
        reached_goal,
        no_first_sheet,
        stderr,
    }
}

/// Durations and limits, fixed in production and shortened in tests.
#[derive(Debug, Clone, Copy)]
struct Limits {
    grace: Duration,
    start_delay: Duration,
    failure_wait: Duration,
    other_failure_wait: Duration,
    min_free_bytes: u64,
    watch_check: Duration,
    quiet_waiting: Duration,
    quiet_alone: Duration,
    seek_wait: Duration,
    follow_every: Duration,
    max_follows: u32,
    rough_budget: Duration,
    rough_seek_wait: Duration,
    rough_max_fails: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            grace: GRACE,
            start_delay: START_DELAY,
            failure_wait: FAILURE_WAIT,
            other_failure_wait: OTHER_FAILURE_WAIT,
            min_free_bytes: MIN_FREE_BYTES,
            watch_check: WATCH_CHECK,
            quiet_waiting: QUIET_WAITING,
            quiet_alone: QUIET_ALONE,
            seek_wait: SEEK_WAIT,
            follow_every: FOLLOW_EVERY,
            max_follows: MAX_FOLLOWS,
            rough_budget: ROUGH_BUDGET,
            rough_seek_wait: ROUGH_SEEK_WAIT,
            rough_max_fails: ROUGH_MAX_FAILS,
        }
    }
}

/// What a play's device last said about it: how long ago it reported, the
/// position it reported, and the play session that reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Watched {
    pub quiet: Duration,
    pub position_s: u64,
    pub play_session_id: String,
}

/// One play session registered for pictures.
struct Registration {
    item_id: Uuid,
    device_id: String,
    /// `None` until the file is resolved.
    set_id: Option<Uuid>,
    /// When the play registered.
    at: Instant,
}

/// The pass of one set, shared by every play of it.
#[derive(Default)]
struct Pass {
    sessions: HashSet<String>,
    /// Present while a pass is queued or running; resolving it stops that.
    cancel: Option<oneshot::Sender<()>>,
    job: Option<JoinHandle<()>>,
    job_id: u64,
    grace: Option<JoinHandle<()>>,
    /// The running child's pid, 0 when none.
    pid: Arc<AtomicU32>,
    /// Its job is waiting for the slot on behalf of a watched play.
    waiting: bool,
    /// The picture count the set is keyed by, once a play of it is resolved;
    /// `None` for a stream's own set.
    pictures: Option<u64>,
}

#[derive(Default)]
struct State {
    by_session: HashMap<String, Registration>,
    by_set: HashMap<Uuid, Pass>,
    /// (item id, device id) to the set whose pictures that device gets.
    selection: HashMap<(Uuid, String), Uuid>,
    /// When each stream that failed may be read again. By stream, not set: a
    /// dead link on one version does not hold back another version of the
    /// same set. Kept in memory only.
    failed_until: HashMap<Uuid, Instant>,
    /// Streams whose source gave no sheet in time after a jump: read from
    /// their start until the server restarts. By stream, as `failed_until`.
    no_jump: HashSet<Uuid>,
    /// Streams whose rough picture at the end of the file timed out: no rough
    /// stage until the server restarts. Runs still follow `no_jump` alone.
    no_rough: HashSet<Uuid>,
    touched_at: HashMap<Uuid, Instant>,
    next_job: u64,
}

struct Inner {
    state: Mutex<State>,
    slots: tokio::sync::Semaphore,
    limits: Limits,
    /// See [`AddonRule`].
    addon_rules: Mutex<Arc<AddonRules>>,
}

/// What [`TrickplayPasses::begin`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Begin {
    /// The play ended, or was never registered: nothing queued.
    Released,
    /// The set is already complete.
    Done,
    /// The file read failed within its wait ([`FAILURE_WAIT`], or
    /// [`OTHER_FAILURE_WAIT`] for another version), and no other version can
    /// be read in its place.
    FailedRecently,
    /// A new pass was queued.
    Queued,
    /// The play joined a pass already queued or running.
    Joined,
}

/// How a job's wait for the slot ended.
enum Turn<P> {
    Slot(P),
    Cancelled,
    Unwatched,
}

/// Why a run was stopped from outside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stopped {
    Cancelled,
    StoodDown,
    /// The viewer moved to this sheet, which has no picture yet.
    Moved(u32),
}

/// How the rough stage of a pass ended (see [`TrickplayPasses::rough_stage`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoughEnd {
    /// The pass gets no rough stage (see [`rough_stage_wanted`]).
    Skipped,
    /// Every picture of [`rough_order`] was tried or already on disk.
    Through,
    /// Its time ran out.
    Budget,
    /// Too many pictures failed in a row.
    Failures,
    /// The picture at the end of the file timed out: no rough stage for this
    /// source again.
    NoJump,
    /// Nobody has watched it lately: the pass gives way.
    StoodDown,
    Cancelled,
}

/// How a job's turn in the slot ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotEnd {
    /// The pass ran to its end or had nothing to do.
    Over,
    Cancelled,
    /// Nobody has watched it lately: it waits to be watched again.
    StoodDown,
    /// Its source was opened and could not be read.
    Failed,
    /// Its source failed recently and was not opened.
    Skipped,
}

/// The passes of this server, held by the playback session manager.
#[derive(Clone)]
pub struct TrickplayPasses {
    inner: Arc<Inner>,
}

impl Default for TrickplayPasses {
    fn default() -> Self {
        Self::new()
    }
}

impl TrickplayPasses {
    pub fn new() -> Self {
        Self::with_limits(Limits::default())
    }

    fn with_limits(limits: Limits) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                slots: tokio::sync::Semaphore::new(MAX_PASSES),
                limits,
                addon_rules: Mutex::new(Arc::default()),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Replaces the advertising snapshot with one rule per addon in `addons`.
    pub fn set_addon_rules(&self, addons: &[Addon]) {
        let rules: AddonRules = addons
            .iter()
            .map(|addon| (addon.id, AddonRule::new(addon)))
            .collect();
        *self
            .inner
            .addon_rules
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Arc::new(rules);
    }

    /// The advertising snapshot: one [`AddonRule`] per stored addon.
    pub fn addon_rules(&self) -> Arc<AddonRules> {
        self.inner
            .addon_rules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Whether the addon of `info`, as its rule is now, allows its stream to
    /// be read. A stream with no addon, or one not in the snapshot, is not.
    fn addon_still_allows(&self, info: &StreamInfo) -> bool {
        let rules = self.addon_rules();
        info.addon_id
            .and_then(|id| rules.get(&id))
            .is_some_and(|rule| rule.allows(info))
    }

    /// Records a play session, before its file is known. The set an earlier
    /// play of the same item on the same device was given is forgotten, so
    /// the sheet route follows this play's pick, which may be a version of
    /// another length, until this play is resolved.
    pub fn register(&self, play_session_id: &str, item_id: Uuid, device_id: &str) {
        // A play session id reused for another item starts over.
        let other_item = self
            .lock()
            .by_session
            .get(play_session_id)
            .is_some_and(|reg| reg.item_id != item_id);
        if other_item {
            self.release(play_session_id);
        }
        let mut st = self.lock();
        st.selection
            .remove(&(item_id, device_id.to_string()));
        let reg = st
            .by_session
            .entry(play_session_id.to_string())
            .or_insert_with(|| Registration {
                item_id,
                device_id: device_id.to_string(),
                set_id: None,
                at: Instant::now(),
            });
        reg.item_id = item_id;
        reg.device_id = device_id.to_string();
    }

    /// The device and item of a registered play session.
    fn play_of(&self, play_session_id: &str) -> Option<(String, Uuid)> {
        self.lock()
            .by_session
            .get(play_session_id)
            .map(|reg| {
                (
                    reg.device_id
                        .clone(),
                    reg.item_id,
                )
            })
    }

    /// Ties a registered play session to its set and records that set for
    /// the session's item and device. False when the session was released
    /// before its file was resolved.
    pub fn attach(&self, play_session_id: &str, set_id: Uuid) -> bool {
        let mut st = self.lock();
        let (old, key) = match st
            .by_session
            .get_mut(play_session_id)
        {
            Some(reg) => (
                reg.set_id
                    .replace(set_id),
                (
                    reg.item_id,
                    reg.device_id
                        .clone(),
                ),
            ),
            None => return false,
        };
        if let Some(old) = old.filter(|old| *old != set_id) {
            self.leave(&mut st, play_session_id, old);
        }
        st.selection
            .insert(key, set_id);
        let pass = st
            .by_set
            .entry(set_id)
            .or_default();
        pass.sessions
            .insert(play_session_id.to_string());
        if let Some(grace) = pass
            .grace
            .take()
        {
            grace.abort();
        }
        true
    }

    /// Ends a play session's part. When it was the last play of its set, the
    /// pass stops after the grace period unless another play joins. The set
    /// it recorded for its device goes with it, so a quick switch to a version
    /// of another length is never answered from this one.
    pub fn release(&self, play_session_id: &str) {
        let mut st = self.lock();
        let Some(reg) = st
            .by_session
            .remove(play_session_id)
        else {
            return;
        };
        if let Some(set_id) = reg.set_id {
            // A later play of the device may have recorded its own set since,
            // or still be on this one.
            let others = st
                .by_session
                .values()
                .any(|r| {
                    r.item_id == reg.item_id
                        && r.device_id == reg.device_id
                        && r.set_id == Some(set_id)
                });
            let key = (reg.item_id, reg.device_id);
            if !others
                && st
                    .selection
                    .get(&key)
                    == Some(&set_id)
            {
                st.selection
                    .remove(&key);
            }
            self.leave(&mut st, play_session_id, set_id);
        }
    }

    fn leave(&self, st: &mut State, play_session_id: &str, set_id: Uuid) {
        let Some(pass) = st
            .by_set
            .get_mut(&set_id)
        else {
            return;
        };
        pass.sessions
            .remove(play_session_id);
        if pass
            .sessions
            .is_empty()
            && pass
                .grace
                .is_none()
        {
            let passes = self.clone();
            let grace = self
                .inner
                .limits
                .grace;
            pass.grace = Some(tokio::spawn(async move {
                tokio::time::sleep(grace).await;
                passes.expire(set_id);
            }));
        }
    }

    /// The grace period ended: drops the pass if nobody rejoined, which
    /// cancels it.
    fn expire(&self, set_id: Uuid) {
        let pass = {
            let mut st = self.lock();
            let unused = st
                .by_set
                .get(&set_id)
                .is_some_and(|p| {
                    p.sessions
                        .is_empty()
                });
            if !unused {
                return;
            }
            st.selection
                .retain(|_, s| *s != set_id);
            st.by_set
                .remove(&set_id)
        };
        if let Some(mut pass) = pass
            && let Some(cancel) = pass
                .cancel
                .take()
        {
            let _ = cancel.send(());
        }
    }

    /// The set whose pictures this device gets for this item, recorded when
    /// its play was resolved.
    pub fn selected_set(&self, item_id: Uuid, device_id: &str) -> Option<Uuid> {
        self.lock()
            .selection
            .get(&(item_id, device_id.to_string()))
            .copied()
    }

    /// The picture count of a set that has a pass, so the sheet route can
    /// tell a sheet past it from one not made yet.
    pub fn pictures_of(&self, set_id: Uuid) -> Option<u64> {
        self.lock()
            .by_set
            .get(&set_id)
            .and_then(|pass| pass.pictures)
    }

    /// Whether `device_id` has a play tied to `set_id` that is at least the
    /// start delay old. Until then a set with a missing sheet is served only
    /// below the gap, so an app that fetches every sheet when play starts and
    /// strings them together in order gets a clean run from sheet 0.
    pub fn open_for(&self, set_id: Uuid, device_id: &str) -> bool {
        let delay = self
            .inner
            .limits
            .start_delay;
        self.lock()
            .by_session
            .values()
            .any(|reg| {
                reg.set_id == Some(set_id)
                    && reg.device_id == device_id
                    && reg
                        .at
                        .elapsed()
                        >= delay
            })
    }

    /// Notes that a set was just shown, so the cap and the keep time delete
    /// it last. Updates the directory's time at most once a minute.
    pub fn mark_used(&self, set_id: Uuid, dir: &Path) {
        let now = Instant::now();
        {
            let mut st = self.lock();
            st.touched_at
                .retain(|_, at| now.saturating_duration_since(*at) < TOUCH_EVERY);
            if st
                .touched_at
                .contains_key(&set_id)
            {
                return;
            }
            st.touched_at
                .insert(set_id, now);
        }
        if let Ok(file) = std::fs::File::open(dir) {
            let _ = file.set_modified(SystemTime::now());
        }
    }

    /// Records that a stream is not to be read again before `until`.
    fn mark_failed(&self, stream_id: Uuid, until: Instant) {
        self.lock()
            .failed_until
            .insert(stream_id, until);
    }

    /// Records that the stream `plan` reads could not be read: the version
    /// played waits [`FAILURE_WAIT`], another version [`OTHER_FAILURE_WAIT`].
    fn mark_plan_failed(&self, plan: &PassPlan) {
        let limits = self
            .inner
            .limits;
        let wait = if plan
            .other_version
            .is_some()
        {
            limits.other_failure_wait
        } else {
            limits.failure_wait
        };
        self.mark_failed(plan.stream_id, Instant::now() + wait);
    }

    fn failed_recently(&self, stream_id: Uuid, now: Instant) -> bool {
        let mut st = self.lock();
        st.failed_until
            .retain(|_, until| *until > now);
        st.failed_until
            .contains_key(&stream_id)
    }

    /// Moves `plan` on to the next of its other versions that has not failed
    /// recently and has no folder of its own, dropping those passed over. The
    /// list was made when the play started: a version given a folder of its
    /// own since then (see [`moved_if_longer`]) would be read into it. False
    /// when none is left.
    fn next_source(&self, plan: &mut PassPlan) -> bool {
        let now = Instant::now();
        while !plan
            .others
            .is_empty()
        {
            let source = plan
                .others
                .remove(0);
            if self.failed_recently(source.stream_id, now)
                || set_dir(&plan.root, source.stream_id).is_dir()
            {
                continue;
            }
            info!(
                set_id = %plan.set_id,
                stream_id = %source.stream_id,
                "trick play: reading another version of this title for its pictures"
            );
            plan.read_other(source);
            return true;
        }
        false
    }

    /// Whether a run of this stream may start part way into the file.
    fn can_jump(&self, stream_id: Uuid) -> bool {
        !self
            .lock()
            .no_jump
            .contains(&stream_id)
    }

    fn mark_no_jump(&self, stream_id: Uuid) {
        self.lock()
            .no_jump
            .insert(stream_id);
    }

    /// Whether a pass over this stream may have a rough stage.
    fn can_rough(&self, stream_id: Uuid) -> bool {
        !self
            .lock()
            .no_rough
            .contains(&stream_id)
    }

    fn mark_no_rough(&self, stream_id: Uuid) {
        self.lock()
            .no_rough
            .insert(stream_id);
    }

    /// The least quiet of a set's plays, each asked of `watch` outside the
    /// lock. A play counts as quiet since it registered when its device has no
    /// reported session on its item, or when the session reporting is another
    /// play registered here on another set (a version of another length,
    /// played after a stop this play never got). `None` for a set with no
    /// plays, which leaves the decision to the grace period.
    fn quiet<W>(&self, set_id: Uuid, watch: &W) -> Option<Watched>
    where
        W: Fn(&str, Uuid) -> Option<Watched>,
    {
        let plays: Vec<(String, Uuid, Instant)> = {
            let st = self.lock();
            let pass = st
                .by_set
                .get(&set_id)?;
            pass.sessions
                .iter()
                .filter_map(|id| {
                    st.by_session
                        .get(id)
                })
                .map(|reg| {
                    (
                        reg.device_id
                            .clone(),
                        reg.item_id,
                        reg.at,
                    )
                })
                .collect()
        };
        let heard: Vec<(Instant, Option<Watched>)> = plays
            .into_iter()
            .map(|(device_id, item_id, at)| (at, watch(&device_id, item_id)))
            .collect();
        let st = self.lock();
        heard
            .into_iter()
            .map(|(at, heard)| {
                let elsewhere = |w: &Watched| {
                    st.by_session
                        .get(&w.play_session_id)
                        .and_then(|reg| reg.set_id)
                        .is_some_and(|other| other != set_id)
                };
                match heard {
                    Some(w) if !elsewhere(&w) => w,
                    _ => Watched {
                        quiet: at.elapsed(),
                        position_s: 0,
                        play_session_id: String::new(),
                    },
                }
            })
            .min_by_key(|heard| heard.quiet)
    }

    /// Whether some play of a set was heard from within `limit`. A set with
    /// no plays is not: a job that stood down, or is waiting for the slot,
    /// stays down until a play reports, or the grace period ends it.
    fn heard_within<W>(&self, set_id: Uuid, watch: &W, limit: Duration) -> bool
    where
        W: Fn(&str, Uuid) -> Option<Watched>,
    {
        self.quiet(set_id, watch)
            .is_some_and(|heard| heard.quiet < limit)
    }

    /// Whether a running pass should give way: its plays have all been quiet
    /// for five minutes, or for a minute while another pass is waiting. Gives
    /// what was last heard, for the log.
    fn gives_way<W>(&self, set_id: Uuid, watch: &W) -> Option<Watched>
    where
        W: Fn(&str, Uuid) -> Option<Watched>,
    {
        let limits = self
            .inner
            .limits;
        let heard = self.quiet(set_id, watch)?;
        let give_way = heard.quiet >= limits.quiet_alone
            || (heard.quiet >= limits.quiet_waiting && self.other_waiting(set_id));
        give_way.then_some(heard)
    }

    fn set_waiting(&self, set_id: Uuid, job_id: u64, waiting: bool) {
        if let Some(pass) = self
            .lock()
            .by_set
            .get_mut(&set_id)
            && pass.job_id == job_id
        {
            pass.waiting = waiting;
        }
    }

    /// Whether a pass other than this set's is waiting for the slot.
    fn other_waiting(&self, set_id: Uuid) -> bool {
        self.lock()
            .by_set
            .iter()
            .any(|(id, pass)| *id != set_id && pass.waiting)
    }

    /// From a resolved file to a queued pass: attaches the play, waits the
    /// start delay, then queues a pass unless the play ended or is no longer
    /// on this item, the set is complete or the file failed recently and no
    /// other version can be read in its place.
    ///
    /// `watch` tells, for a device and an item, whether that device's latest
    /// reported session is on that item, how long ago it reported and where
    /// it was. The pass asks it while it waits and while it runs.
    pub(crate) async fn begin<R, W>(
        &self,
        play_session_id: &str,
        mut plan: PassPlan,
        runner: Arc<R>,
        watch: W,
    ) -> Begin
    where
        R: PassRunner,
        W: Fn(&str, Uuid) -> Option<Watched> + Send + Sync + 'static,
    {
        if !self.attach(play_session_id, plan.set_id) {
            return Begin::Released;
        }
        if let Some(pass) = self
            .lock()
            .by_set
            .get_mut(&plan.set_id)
        {
            // Only a set keyed by its count knows it, as in `set_for_stream`;
            // a stream's own set may run past a count guessed from its runtime.
            pass.pictures = (plan.set_id != plan.stream_id).then_some(plan.pictures);
        }
        tokio::time::sleep(
            self.inner
                .limits
                .start_delay,
        )
        .await;
        let Some((device_id, item_id)) = self.play_of(play_session_id) else {
            return Begin::Released;
        };
        if watch(&device_id, item_id).is_none() {
            self.release(play_session_id);
            return Begin::Released;
        }
        if is_done(&set_dir(&plan.root, plan.set_id)) {
            return Begin::Done;
        }
        // Nothing is opened here, so a version passed over does not count
        // towards `MAX_SOURCES`.
        if self.failed_recently(plan.stream_id, Instant::now())
            && !self.next_source(&mut plan)
        {
            return Begin::FailedRecently;
        }
        self.queue(plan, runner, watch)
    }

    fn queue<R, W>(&self, plan: PassPlan, runner: Arc<R>, watch: W) -> Begin
    where
        R: PassRunner,
        W: Fn(&str, Uuid) -> Option<Watched> + Send + Sync + 'static,
    {
        let mut st = self.lock();
        let job_id = st.next_job;
        st.next_job += 1;
        let Some(pass) = st
            .by_set
            .get_mut(&plan.set_id)
        else {
            return Begin::Released;
        };
        if pass
            .cancel
            .is_some()
        {
            return Begin::Joined;
        }
        let (cancel_tx, cancel_rx) = oneshot::channel();
        pass.cancel = Some(cancel_tx);
        pass.job_id = job_id;
        let pid = pass
            .pid
            .clone();
        let passes = self.clone();
        pass.job = Some(tokio::spawn(async move {
            passes
                .run_job(plan, runner, cancel_rx, pid, job_id, watch)
                .await;
        }));
        Begin::Queued
    }

    /// A queued pass, until it ends: it waits for the slot while one of its
    /// plays is watched, runs, gives way when nobody has watched it lately,
    /// and waits to be watched again. A cancel (the grace period, `expire`,
    /// `kill_all`) ends it at any point, and so does a run that ended by
    /// itself. A source that could not be read is followed by the next of the
    /// plan's other versions, which waits for the slot like any pass, until
    /// [`MAX_SOURCES`] were read.
    async fn run_job<R, W>(
        &self,
        mut plan: PassPlan,
        runner: Arc<R>,
        mut cancel: oneshot::Receiver<()>,
        pid: Arc<AtomicU32>,
        job_id: u64,
        watch: W,
    ) where
        R: PassRunner,
        W: Fn(&str, Uuid) -> Option<Watched> + Send + Sync + 'static,
    {
        let set_id = plan.set_id;
        let limits = self
            .inner
            .limits;
        let mut stood_down = false;
        let mut rough_tried = false;
        let mut size_checked = None;
        let mut read = 0;
        loop {
            if stood_down {
                // Stood down: look again now and then until a play reports.
                let cancelled = loop {
                    tokio::select! {
                        _ = &mut cancel => break true,
                        _ = tokio::time::sleep(limits.watch_check) => {}
                    }
                    if self.heard_within(set_id, &watch, limits.quiet_waiting) {
                        break false;
                    }
                };
                if cancelled {
                    break;
                }
            }
            // Waiting for the slot, only while watched, keeping its place.
            if !self.heard_within(set_id, &watch, limits.quiet_waiting) {
                stood_down = true;
                continue;
            }
            self.set_waiting(set_id, job_id, true);
            let turn = {
                let acquire = self
                    .inner
                    .slots
                    .acquire();
                tokio::pin!(acquire);
                loop {
                    tokio::select! {
                        permit = &mut acquire => break Turn::Slot(permit),
                        _ = &mut cancel => break Turn::Cancelled,
                        _ = tokio::time::sleep(limits.watch_check) => {
                            if !self.heard_within(set_id, &watch, limits.quiet_waiting) {
                                break Turn::Unwatched;
                            }
                        }
                    }
                }
            };
            self.set_waiting(set_id, job_id, false);
            let permit = match turn {
                Turn::Slot(Ok(permit)) => permit,
                Turn::Slot(Err(_)) | Turn::Cancelled => break,
                Turn::Unwatched => {
                    stood_down = true;
                    continue;
                }
            };
            let end = self
                .run_in_slot(
                    &plan,
                    runner.as_ref(),
                    &mut cancel,
                    pid.clone(),
                    &watch,
                    &mut rough_tried,
                    &mut size_checked,
                )
                .await;
            drop(permit);
            match end {
                SlotEnd::StoodDown => {
                    stood_down = true;
                    continue;
                }
                SlotEnd::Failed => {
                    read += 1;
                    // The key check moved the version played to a folder of
                    // its own: its plays no longer show the set.
                    let moved = plan
                        .other_version
                        .is_none()
                        && plan.set_id != plan.stream_id
                        && set_dir(&plan.root, plan.stream_id).is_dir();
                    if !moved && read < MAX_SOURCES && self.next_source(&mut plan) {
                        continue;
                    }
                }
                SlotEnd::Skipped => {
                    if self.next_source(&mut plan) {
                        continue;
                    }
                }
                SlotEnd::Over | SlotEnd::Cancelled => {}
            }
            break;
        }
        self.job_ended(set_id, job_id);
    }

    /// Runs a pass while holding a slot, so prune never races a pass, with
    /// `busy` in the set's folder until it ends (see [`Self::run_pass`]).
    /// `rough_tried` keeps a job to one rough stage, whichever source it
    /// reads, unless that stage gave way before it was through. Another
    /// version's size is checked once a job before it is read (see
    /// [`is_listed_file`]); `size_checked` holds the stream last checked.
    #[allow(clippy::too_many_arguments)]
    async fn run_in_slot<R, W>(
        &self,
        plan: &PassPlan,
        runner: &R,
        cancel: &mut oneshot::Receiver<()>,
        pid: Arc<AtomicU32>,
        watch: &W,
        rough_tried: &mut bool,
        size_checked: &mut Option<Uuid>,
    ) -> SlotEnd
    where
        R: PassRunner,
        W: Fn(&str, Uuid) -> Option<Watched>,
    {
        // After the key check moved the set to the stream's own folder, a pass
        // that stood down and comes back carries on there. That folder exists
        // only after a move, or `set_for` would have chosen it.
        let own = set_dir(&plan.root, plan.stream_id);
        let dir = if plan.set_id != plan.stream_id && own.is_dir() {
            own
        } else {
            set_dir(&plan.root, plan.set_id)
        };
        if is_done(&dir) {
            return SlotEnd::Over;
        }
        if self.failed_recently(plan.stream_id, Instant::now()) {
            return SlotEnd::Skipped;
        }
        // Switched off while this pass waited: start nothing. The rough stage
        // and another version follow their own switches as they are now, too.
        let rough = match &plan.db {
            Some(pool) => {
                let cfg = db::Settings::get_config_or_default(pool).await;
                let other_off = plan
                    .other_version
                    .is_some()
                    && !other_version(&cfg);
                if !enabled(&cfg) || other_off {
                    return SlotEnd::Over;
                }
                if let Some(info) = &plan.other_version
                    && !self.addon_still_allows(info)
                {
                    info!(
                        set_id = %plan.set_id,
                        stream_id = %plan.stream_id,
                        "trick play: another version's addon no longer allows previews"
                    );
                    return SlotEnd::Skipped;
                }
                plan.rough && rough_first(&cfg)
            }
            None => plan.rough,
        };
        let plan = &PassPlan {
            rough,
            ..plan.clone()
        };
        let mut active: HashSet<Uuid> = self
            .lock()
            .by_set
            .keys()
            .copied()
            .collect();
        // Where the key check may have moved this set.
        active.insert(plan.stream_id);
        let root = plan
            .root
            .clone();
        let cap = plan.cap_bytes;
        let keep = plan.keep_for;
        let _ = tokio::task::spawn_blocking(move || {
            prune(&root, cap, keep, &active, SystemTime::now())
        })
        .await;
        if let Err(e) = tokio::fs::create_dir_all(&dir).await {
            warn!(error = %e, "could not create the trick play directory");
            self.mark_failed(
                plan.stream_id,
                Instant::now()
                    + self
                        .inner
                        .limits
                        .failure_wait,
            );
            return SlotEnd::Over;
        }
        let min_free_bytes = self
            .inner
            .limits
            .min_free_bytes;
        if free_bytes(&dir).is_some_and(|b| b < min_free_bytes) {
            warn!(
                "not making trick play pictures: less than {} GB of disk is free",
                min_free_bytes / (1024 * 1024 * 1024)
            );
            return SlotEnd::Over;
        }
        // Another version is read only if its source still holds the file it
        // listed: a resolver can hand over another release, which no later
        // check would tell from the right one.
        if let Some(listed) = plan
            .other_version
            .as_ref()
            && *size_checked != Some(plan.stream_id)
        {
            *size_checked = Some(plan.stream_id);
            let found = tokio::select! {
                found = runner.size(&plan.input) => found,
                _ = &mut *cancel => return SlotEnd::Cancelled,
            };
            if !is_listed_file(listed, found) {
                self.mark_plan_failed(plan);
                info!(
                    set_id = %plan.set_id,
                    stream_id = %plan.stream_id,
                    listed = ?listed.size,
                    found = ?found,
                    "trick play: another version's source does not hold the file it listed"
                );
                return SlotEnd::Failed;
            }
        }
        // `busy` tells the start-up prune that a pass was at work here, should
        // the server stop without removing it.
        if let Err(e) = std::fs::write(dir.join(BUSY_MARKER), b"") {
            warn!(error = %e, "could not mark trick play pictures as being made");
        }
        info!(
            set_id = %plan.set_id,
            stream_id = %plan.stream_id,
            "making trick play pictures"
        );
        let stage = if plan.rough && !*rough_tried {
            self.rough_stage(plan, runner, cancel, pid.clone(), watch, &dir)
                .await
        } else {
            RoughEnd::Skipped
        };
        if stage != RoughEnd::StoodDown {
            *rough_tried = true;
        }
        let mut dir = dir;
        let end = match stage {
            RoughEnd::Cancelled => SlotEnd::Cancelled,
            RoughEnd::StoodDown => SlotEnd::StoodDown,
            _ => {
                self.run_pass(plan, runner, cancel, pid, watch, &mut dir)
                    .await
            }
        };
        let _ = std::fs::remove_file(dir.join(BUSY_MARKER));
        end
    }

    /// The rough stage, before a pass's first run: each picture of
    /// [`rough_order`] not on disk, one short jump each, composing the
    /// stand-ins as they come (see [`ROUGH_COMPOSE_AT`]). It ends with every
    /// picture made, after [`ROUGH_BUDGET`], after [`ROUGH_MAX_FAILS`]
    /// failures in a row, when the picture at the end of the file times out
    /// (the source then gets no rough stage again), when nobody has
    /// watched the pass lately, or on a cancel. Rough pictures on disk are
    /// kept whichever way it ends.
    async fn rough_stage<R, W>(
        &self,
        plan: &PassPlan,
        runner: &R,
        cancel: &mut oneshot::Receiver<()>,
        pid: Arc<AtomicU32>,
        watch: &W,
        dir: &Path,
    ) -> RoughEnd
    where
        R: PassRunner,
        W: Fn(&str, Uuid) -> Option<Watched>,
    {
        let limits = self
            .inner
            .limits;
        let to_make = missing(&read_set(dir), expected_sheets(plan.pictures)).len();
        if !rough_stage_wanted(
            self.can_jump(plan.stream_id) && self.can_rough(plan.stream_id),
            plan.size,
            plan.pictures,
            to_make,
        ) {
            return RoughEnd::Skipped;
        }
        let order = rough_order(plan.pictures, ROUGH_PICTURES);
        let mut on_disk = rough_made(dir, plan.pictures).len();
        let mut composed_at = None;
        let mut fails = 0;
        let mut made_now = 0;
        let mut end = RoughEnd::Through;
        let began = Instant::now();
        info!(
            set_id = %plan.set_id,
            stream_id = %plan.stream_id,
            on_disk,
            "making rough trick play pictures first"
        );
        for (place, picture) in order
            .iter()
            .copied()
            .enumerate()
        {
            let out = rough_file(dir, picture);
            if sheet_whole(&out) {
                continue;
            }
            if !matches!(cancel.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
                return RoughEnd::Cancelled;
            }
            let left = limits
                .rough_budget
                .saturating_sub(began.elapsed());
            if left.is_zero() {
                end = RoughEnd::Budget;
                break;
            }
            if let Some(heard) = self.gives_way(plan.set_id, watch) {
                info!(
                    set_id = %plan.set_id,
                    stream_id = %plan.stream_id,
                    quiet_s = heard.quiet.as_secs(),
                    "rough trick play pictures giving way: nobody has watched them lately"
                );
                end = RoughEnd::StoodDown;
                break;
            }
            let wait = limits
                .rough_seek_wait
                .min(left);
            let job = GrabJob {
                input: plan
                    .input
                    .clone(),
                at_s: picture * INTERVAL_SECS,
                out,
                wait,
                pid: pid.clone(),
            };
            let (stop_tx, stop_rx) = oneshot::channel();
            let mut stop = Some(stop_tx);
            let grabbed = runner.grab(job, stop_rx);
            tokio::pin!(grabbed);
            let grab = loop {
                tokio::select! {
                    grab = &mut grabbed => break grab,
                    _ = &mut *cancel, if stop.is_some() => {
                        if let Some(stop) = stop.take() {
                            let _ = stop.send(());
                        }
                    }
                }
            };
            // The pass was stopped while this picture was being made.
            if stop.is_none() {
                return RoughEnd::Cancelled;
            }
            match grab {
                Grab::Made => {
                    fails = 0;
                    made_now += 1;
                    on_disk += 1;
                    if ROUGH_COMPOSE_AT.contains(&on_disk) {
                        compose_rough_logged(dir, plan.pictures).await;
                        composed_at = Some(on_disk);
                    }
                }
                // The end of the file is the farthest jump: a source that
                // cannot make it in a whole wait is too slow for rough
                // pictures. Whether runs may jump is left to their own check.
                Grab::TimedOut if place == 1 && wait == limits.rough_seek_wait => {
                    self.mark_no_rough(plan.stream_id);
                    info!(
                        set_id = %plan.set_id,
                        stream_id = %plan.stream_id,
                        "trick play: no rough picture in time at the end of the file; \
                         no rough pictures for this source"
                    );
                    end = RoughEnd::NoJump;
                    break;
                }
                Grab::TimedOut | Grab::Failed | Grab::Cancelled => {
                    fails += 1;
                    if fails >= limits.rough_max_fails {
                        end = RoughEnd::Failures;
                        break;
                    }
                }
            }
        }
        if made_now > 0 && on_disk >= 2 && composed_at != Some(on_disk) {
            compose_rough_logged(dir, plan.pictures).await;
        }
        info!(
            set_id = %plan.set_id,
            stream_id = %plan.stream_id,
            pictures = on_disk,
            ended = ?end,
            secs = began
                .elapsed()
                .as_secs(),
            "rough trick play pictures made"
        );
        end
    }

    /// The runs of one pass (see [`next_run`]), the first at the sheet the
    /// viewer is on, until nothing is missing, a run fails, the pass is
    /// stopped or [`MAX_RUNS`] runs were made. The set is marked `done` once a
    /// run has reached the end of the file and no sheet is missing, so never
    /// before its beginning is made. `dir` follows the set if the key check
    /// moves it.
    async fn run_pass<R, W>(
        &self,
        plan: &PassPlan,
        runner: &R,
        cancel: &mut oneshot::Receiver<()>,
        pid: Arc<AtomicU32>,
        watch: &W,
        dir: &mut PathBuf,
    ) -> SlotEnd
    where
        R: PassRunner,
        W: Fn(&str, Uuid) -> Option<Watched>,
    {
        let expected = expected_sheets(plan.pictures);
        let mut viewer = self
            .quiet(plan.set_id, watch)
            .map_or(0, |heard| sheet_at(heard.position_s, expected));
        let mut follows = 0;
        let mut nothing = 0;
        // The sheets another version made in this slot. They are on trial: a
        // read that ends by itself short of the end, with no sign that the
        // source failed, is the sign of a shorter file, and they are deleted
        // for the next version to make (see `drop_on_trial`). Sheets it made
        // before standing down in an earlier slot are not counted.
        let mut on_trial: Vec<u32> = Vec::new();
        for _ in 0..MAX_RUNS {
            let state = read_set(dir);
            let can_jump = self.can_jump(plan.stream_id);
            let needed = missing(&state, expected);
            let Some(run) = next_run(&needed, viewer, can_jump, state.end, expected)
            else {
                break;
            };
            // A stop that came between two runs: start no ffmpeg for it.
            if !matches!(cancel.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
                return SlotEnd::Cancelled;
            }
            let (result, stopped) = self
                .one_run(plan, runner, cancel, pid.clone(), watch, dir, run, follows)
                .await;
            let end = run_end(&result, run, plan.pictures);
            let moved = self.after_run(plan, dir, run, &result, end);
            if stopped == Some(Stopped::Cancelled) {
                return SlotEnd::Cancelled;
            }
            // Another version longer than its stored length: its pictures are
            // in a folder of its own, which nobody plays, and the set is made
            // from the next version.
            if moved
                && plan
                    .other_version
                    .is_some()
            {
                self.mark_plan_failed(plan);
                return SlotEnd::Failed;
            }
            if plan
                .other_version
                .is_some()
            {
                let made = u32::try_from(result.made_sheets).unwrap_or(u32::MAX);
                on_trial.extend(
                    run.start
                        ..run
                            .start
                            .saturating_add(made),
                );
            }
            match (end, stopped) {
                (RunEnd::Failed, _) => {
                    if result.exit_ok && !source_failed(&result) {
                        drop_on_trial(plan, dir, &on_trial);
                    }
                    return SlotEnd::Failed;
                }
                (_, Some(Stopped::StoodDown)) => return SlotEnd::StoodDown,
                (_, Some(Stopped::Moved(to))) => {
                    viewer = to;
                    follows += 1;
                }
                // Stopped by low disk.
                (RunEnd::Cancelled, _) => return SlotEnd::Over,
                // Twice: the file ends before its stored length, or its source
                // ends every request at once. Another version's file then
                // cannot be the set's; the version played goes on to another
                // version when there is one.
                (RunEnd::Nothing, _) => {
                    nothing += 1;
                    if nothing >= 2 {
                        if plan
                            .other_version
                            .is_none()
                            && plan
                                .others
                                .is_empty()
                        {
                            return SlotEnd::Over;
                        }
                        self.mark_plan_failed(plan);
                        drop_on_trial(plan, dir, &on_trial);
                        info!(
                            set_id = %plan.set_id,
                            stream_id = %plan.stream_id,
                            "trick play: the file ends before its stored length"
                        );
                        return SlotEnd::Failed;
                    }
                    viewer = 0;
                }
                (RunEnd::Goal | RunEnd::End | RunEnd::NoJump, _) => {}
            }
        }
        let state = read_set(dir);
        let needed = missing(&state, expected);
        // The loop ends early only with nothing missing.
        if !needed.is_empty() {
            debug!(
                set_id = %plan.set_id,
                sheets_missing = needed.len(),
                "trick play pass reached its run limit"
            );
        }
        let reached_last = state
            .sheets
            .last()
            .is_some_and(|h| u64::from(*h) + 1 >= last_sheet(plan.pictures));
        if state.end && reached_last && needed.is_empty() {
            if let Err(e) = std::fs::write(dir.join(DONE_MARKER), b"") {
                warn!(error = %e, "could not mark trick play pictures complete");
            }
            // Also any left from a time the rough stage was switched on.
            drop_rough(dir);
            info!(
                set_id = %plan.set_id,
                stream_id = %plan.stream_id,
                sheets = state
                    .sheets
                    .len(),
                "trick play pictures complete"
            );
        }
        SlotEnd::Over
    }

    /// One run of a pass. Every `watch_check` it asks whether to give way
    /// (see [`Self::gives_way`]), then whether to follow the viewer to a part
    /// with no pictures (see [`follows_to`]); either stops the run, which
    /// then ends as cancelled.
    #[allow(clippy::too_many_arguments)]
    async fn one_run<R, W>(
        &self,
        plan: &PassPlan,
        runner: &R,
        cancel: &mut oneshot::Receiver<()>,
        pid: Arc<AtomicU32>,
        watch: &W,
        dir: &Path,
        run: Run,
        follows: u32,
    ) -> (RunResult, Option<Stopped>)
    where
        R: PassRunner,
        W: Fn(&str, Uuid) -> Option<Watched>,
    {
        let limits = self
            .inner
            .limits;
        let expected = expected_sheets(plan.pictures);
        let can_jump = self.can_jump(plan.stream_id);
        let made = Arc::new(AtomicU32::new(0));
        let job = PassJob {
            input: plan
                .input
                .clone(),
            out_dir: dir.to_path_buf(),
            pid,
            min_free_bytes: limits.min_free_bytes,
            start_sheet: run.start,
            stop_after: run.stop_after,
            first_sheet_within: (run.start > 0)
                .then(|| first_sheet_wait(plan.size, expected, limits.seek_wait)),
            made: made.clone(),
        };
        let began = Instant::now();
        let (stop_tx, stop_rx) = oneshot::channel();
        let mut stop = Some(stop_tx);
        let mut stopped = None;
        let ran = runner.run(job, stop_rx);
        tokio::pin!(ran);
        let result = loop {
            tokio::select! {
                result = &mut ran => break result,
                _ = &mut *cancel, if stop.is_some() => {
                    stopped = Some(Stopped::Cancelled);
                    if let Some(stop) = stop.take() {
                        let _ = stop.send(());
                    }
                }
                _ = tokio::time::sleep(limits.watch_check), if stop.is_some() => {
                    let why = if let Some(heard) = self.gives_way(plan.set_id, watch) {
                        info!(
                            set_id = %plan.set_id,
                            stream_id = %plan.stream_id,
                            quiet_s = heard.quiet.as_secs(),
                            position_s = heard.position_s,
                            "trick play pass giving way: nobody has watched it lately"
                        );
                        Some(Stopped::StoodDown)
                    } else {
                        // Only a viewer heard from lately is followed.
                        self.quiet(plan.set_id, watch)
                            .filter(|heard| heard.quiet < limits.quiet_waiting)
                            .map(|heard| sheet_at(heard.position_s, expected))
                            .filter(|to| {
                                let next = run
                                    .start
                                    .saturating_add(made.load(Ordering::SeqCst));
                                // With `end` set, the last sheet is one a run
                                // found beyond the end of the file: it never
                                // comes, so it is not followed to.
                                let on_disk = sheet_whole(&sheet_file(dir, *to))
                                    || (*to >= expected.saturating_sub(1)
                                        && dir
                                            .join(END_MARKER)
                                            .exists());
                                follows_to(
                                    *to,
                                    on_disk,
                                    next,
                                    began.elapsed(),
                                    follows,
                                    can_jump,
                                    &limits,
                                )
                            })
                            .map(|to| {
                                info!(
                                    set_id = %plan.set_id,
                                    stream_id = %plan.stream_id,
                                    sheet = to,
                                    "trick play pass moving to the part being watched"
                                );
                                Stopped::Moved(to)
                            })
                    };
                    if why.is_some() {
                        stopped = why;
                        if let Some(stop) = stop.take() {
                            let _ = stop.send(());
                        }
                    }
                }
            }
        };
        (result, stopped)
    }

    /// Acts on how a run ended (see [`run_end`]). Sheets on disk are kept and
    /// built on; a run deletes only its own last sheet, and only when that
    /// may be partly filled: after a failure, or when it was stopped after
    /// its output had already ended. No failure or stop marks a set finished
    /// or deletes it. True when the key check moved the set (see
    /// [`moved_if_longer`]).
    fn after_run(
        &self,
        plan: &PassPlan,
        dir: &mut PathBuf,
        run: Run,
        result: &RunResult,
        end: RunEnd,
    ) -> bool {
        let last_made = (result.made_sheets > 0)
            .then(|| u64::from(run.start) + result.made_sheets - 1);
        let last_made_n = last_made.and_then(|n| u32::try_from(n).ok());
        let mut moved_set = false;
        match end {
            RunEnd::Goal => debug!(
                set_id = %plan.set_id,
                start = run.start,
                sheets = result.made_sheets,
                "trick play run made the sheets it was after"
            ),
            RunEnd::End => {
                if let Some(highest) = last_made {
                    let moved = moved_if_longer(plan, dir.as_path(), highest);
                    if moved != *dir {
                        // The set's folder is gone: its plays follow their
                        // own pick, which finds this stream's folder.
                        self.lock()
                            .selection
                            .retain(|_, s| *s != plan.set_id);
                        // Sheets below this run's start may be another
                        // version's: this pass makes them again from this file.
                        for n in 0..run.start {
                            remove_sheet(&moved, n);
                        }
                        *dir = moved;
                        moved_set = true;
                    }
                }
                if let Err(e) = std::fs::write(dir.join(END_MARKER), b"") {
                    warn!(error = %e, "could not mark the end of trick play pictures");
                }
                debug!(
                    set_id = %plan.set_id,
                    start = run.start,
                    sheets = result.made_sheets,
                    exit_code = ?result.exit_code,
                    stderr = %crate::playback::probe::redact_urls_in(&result.stderr),
                    "trick play run reached the end of the file"
                );
            }
            RunEnd::Cancelled => {
                // A stopped run is killed, and each sheet is renamed into
                // place once whole, so every sheet on disk is good, unless
                // ffmpeg had already reached its end and flushed a partly
                // filled one.
                if result.ended_itself
                    && let Some(n) = last_made_n
                {
                    remove_sheet(dir, n);
                }
                debug!(
                    set_id = %plan.set_id,
                    stream_id = %plan.stream_id,
                    sheets = result.made_sheets,
                    "trick play pass stopped"
                );
            }
            RunEnd::NoJump => {
                self.mark_no_jump(plan.stream_id);
                info!(
                    set_id = %plan.set_id,
                    stream_id = %plan.stream_id,
                    start = run.start,
                    "trick play: no picture in time after a jump; reading this source from its start"
                );
            }
            RunEnd::Nothing => debug!(
                set_id = %plan.set_id,
                start = run.start,
                exit_code = ?result.exit_code,
                "trick play: the file ends before that place"
            ),
            RunEnd::Failed => {
                // The last sheet this run wrote may be a partly filled one
                // flushed at a false end of input.
                if let Some(n) = last_made_n {
                    remove_sheet(dir, n);
                }
                self.mark_plan_failed(plan);
                if plan
                    .other_version
                    .is_some()
                {
                    info!(
                        set_id = %plan.set_id,
                        stream_id = %plan.stream_id,
                        start = run.start,
                        exit_ok = result.exit_ok,
                        exit_code = ?result.exit_code,
                        sheets = result.made_sheets,
                        stderr = %crate::playback::probe::redact_urls_in(&result.stderr),
                        "trick play: another version could not be read"
                    );
                } else {
                    warn!(
                        set_id = %plan.set_id,
                        stream_id = %plan.stream_id,
                        start = run.start,
                        exit_ok = result.exit_ok,
                        exit_code = ?result.exit_code,
                        sheets = result.made_sheets,
                        stderr = %crate::playback::probe::redact_urls_in(&result.stderr),
                        "trick play pass did not finish; not retrying for 10 minutes"
                    );
                }
            }
        }
        moved_set
    }

    fn job_ended(&self, set_id: Uuid, job_id: u64) {
        let mut st = self.lock();
        if let Some(pass) = st
            .by_set
            .get_mut(&set_id)
            && pass.job_id == job_id
        {
            pass.cancel = None;
            pass.job = None;
            pass.waiting = false;
            pass.pid
                .store(0, Ordering::SeqCst);
        }
    }

    /// Server shutdown: cancels every pass with no grace period and waits for
    /// each process to exit, killing it if it does not. Exiting runs no
    /// destructors, so this is what keeps ffmpeg from outliving the server.
    pub async fn kill_all(&self) {
        let passes: Vec<Pass> = {
            let mut st = self.lock();
            st.by_session
                .clear();
            st.selection
                .clear();
            st.by_set
                .drain()
                .map(|(_, pass)| pass)
                .collect()
        };
        let mut jobs = Vec::new();
        for mut pass in passes {
            if let Some(grace) = pass
                .grace
                .take()
            {
                grace.abort();
            }
            if let Some(cancel) = pass
                .cancel
                .take()
            {
                let _ = cancel.send(());
            }
            if let Some(job) = pass
                .job
                .take()
            {
                jobs.push((job, pass.pid));
            }
        }
        for (job, pid) in jobs {
            if tokio::time::timeout(KILL_WAIT, job)
                .await
                .is_err()
            {
                kill_pid(pid.load(Ordering::SeqCst));
            }
        }
    }
}

/// The key check: a run that read to the end of the file and wrote a sheet,
/// `highest`, past the last one its picture count fills read a file longer
/// than its stored length, so its pictures are not the set's. They move to a
/// folder named by the stream itself, which [`set_for`] then keeps that
/// stream on. Gives the folder the pictures are in afterwards.
fn moved_if_longer(plan: &PassPlan, dir: &Path, highest: u64) -> PathBuf {
    let longer = highest > last_sheet(plan.pictures);
    if !longer || plan.set_id == plan.stream_id {
        return dir.to_path_buf();
    }
    let own = set_dir(&plan.root, plan.stream_id);
    if own.exists() {
        warn!(
            set_id = %plan.set_id,
            stream_id = %plan.stream_id,
            "trick play: the file is longer than its stored length, but its own folder exists"
        );
        return dir.to_path_buf();
    }
    match std::fs::rename(dir, &own) {
        Ok(()) => {
            info!(
                set_id = %plan.set_id,
                stream_id = %plan.stream_id,
                highest_sheet = highest,
                "trick play: the file is longer than its stored length; its pictures moved to a set of its own"
            );
            own
        }
        Err(e) => {
            warn!(error = %e, "could not move trick play pictures to their own folder");
            dir.to_path_buf()
        }
    }
}

#[cfg(test)]
impl TrickplayPasses {
    fn registration_count(&self) -> usize {
        self.lock()
            .by_session
            .len()
    }

    fn has_pass(&self, set_id: Uuid) -> bool {
        self.lock()
            .by_set
            .contains_key(&set_id)
    }

    fn is_queued(&self, set_id: Uuid) -> bool {
        self.lock()
            .by_set
            .get(&set_id)
            .is_some_and(|p| {
                p.cancel
                    .is_some()
            })
    }

    fn is_waiting(&self, set_id: Uuid) -> bool {
        self.lock()
            .by_set
            .get(&set_id)
            .is_some_and(|p| p.waiting)
    }

    /// Makes a registered play look `by` older, as if it had started then.
    pub(crate) fn backdate(&self, play_session_id: &str, by: Duration) {
        if let Some(reg) = self
            .lock()
            .by_session
            .get_mut(play_session_id)
            && let Some(at) = Instant::now().checked_sub(by)
        {
            reg.at = at;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    const RUNTIME: u64 = 7_200;
    const URL: &str = "http://example.invalid/a.mkv";

    fn fast() -> Limits {
        Limits {
            grace: Duration::from_millis(50),
            start_delay: Duration::from_millis(10),
            failure_wait: FAILURE_WAIT,
            other_failure_wait: OTHER_FAILURE_WAIT,
            min_free_bytes: 0,
            watch_check: Duration::from_millis(10),
            quiet_waiting: Duration::from_millis(100),
            quiet_alone: Duration::from_secs(10),
            seek_wait: Duration::from_secs(1),
            follow_every: Duration::from_millis(30),
            max_follows: MAX_FOLLOWS,
            rough_budget: Duration::from_secs(5),
            rough_seek_wait: Duration::from_millis(200),
            rough_max_fails: ROUGH_MAX_FAILS,
        }
    }

    /// A pass over a 7,200 s stream with a set of its own.
    fn plan(root: &Path, stream_id: Uuid) -> PassPlan {
        set_plan(root, stream_id, stream_id)
    }

    /// A pass over a 7,200 s stream (720 pictures) into `set_id`.
    fn set_plan(root: &Path, stream_id: Uuid, set_id: Uuid) -> PassPlan {
        PassPlan {
            stream_id,
            set_id,
            input: "/media/invented-film.mkv".to_string(),
            root: root.to_path_buf(),
            pictures: RUNTIME / INTERVAL_SECS,
            size: None,
            cap_bytes: u64::MAX,
            keep_for: Some(Duration::from_secs(30 * 24 * 60 * 60)),
            rough: false,
            db: None,
            others: Vec::new(),
            other_version: None,
        }
    }

    /// A play that reported just now, whatever the device and item.
    fn always() -> impl Fn(&str, Uuid) -> Option<Watched> + Send + Sync + 'static {
        |_: &str, _: Uuid| {
            Some(Watched {
                quiet: Duration::ZERO,
                position_s: 0,
                play_session_id: String::new(),
            })
        }
    }

    /// No device has a reported session on the item.
    fn never() -> impl Fn(&str, Uuid) -> Option<Watched> + Send + Sync + 'static {
        |_: &str, _: Uuid| None
    }

    /// What one device last reported: when (`None` is "just now", whenever
    /// asked), for which play session, and at which position.
    #[derive(Clone, Default)]
    struct Report {
        last: Option<Instant>,
        play_session_id: String,
        position_s: u64,
    }

    /// Each device's last report. A device not listed has no reported
    /// session on the item.
    #[derive(Clone, Default)]
    struct Reports(Arc<Mutex<HashMap<String, Report>>>);

    impl Reports {
        fn set(&self, device_id: &str, last: Option<Instant>) {
            self.0
                .lock()
                .unwrap()
                .entry(device_id.to_string())
                .or_default()
                .last = last;
        }

        /// Reporting all the time.
        fn fresh(&self, device_id: &str) {
            self.set(device_id, None);
        }

        /// Reporting all the time, for `play_session_id`.
        fn playing(&self, device_id: &str, play_session_id: &str) {
            let mut reports = self
                .0
                .lock()
                .unwrap();
            let report = reports
                .entry(device_id.to_string())
                .or_default();
            report.last = None;
            report.play_session_id = play_session_id.to_string();
        }

        /// Reporting all the time, at `position_s`.
        fn at(&self, device_id: &str, position_s: u64) {
            let mut reports = self
                .0
                .lock()
                .unwrap();
            let report = reports
                .entry(device_id.to_string())
                .or_default();
            report.last = None;
            report.position_s = position_s;
        }

        /// Silent since `ago` ago.
        fn silent(&self, device_id: &str, ago: Duration) {
            self.set(device_id, Instant::now().checked_sub(ago));
        }

        fn watch(
            &self,
        ) -> impl Fn(&str, Uuid) -> Option<Watched> + Send + Sync + 'static {
            let reports = self.clone();
            move |device_id: &str, _: Uuid| {
                reports
                    .0
                    .lock()
                    .unwrap()
                    .get(device_id)
                    .map(|report| Watched {
                        quiet: report
                            .last
                            .map_or(Duration::ZERO, |at| at.elapsed()),
                        position_s: report.position_s,
                        play_session_id: report
                            .play_session_id
                            .clone(),
                    })
            }
        }
    }

    /// A sheet as [`sheet_whole`] sees one: JPEG start and end markers.
    const JPEG: &[u8] = &[0xFF, 0xD8, b'j', b'p', b'g', 0xFF, 0xD9];

    /// The size, in bytes, the other versions in these tests list.
    const LISTED: u64 = 6 * 1024 * 1024 * 1024;

    fn write_sheets(dir: &Path, sheets: impl IntoIterator<Item = u32>) {
        std::fs::create_dir_all(dir).unwrap();
        for n in sheets {
            std::fs::write(dir.join(format!("{n}.jpg")), JPEG).unwrap();
        }
    }

    fn sheets_in(dir: &Path) -> Vec<u32> {
        let mut found: Vec<u32> = std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter_map(|e| {
                        e.file_name()
                            .to_str()
                            .and_then(parse_sheet)
                    })
                    .collect()
            })
            .unwrap_or_default();
        found.sort_unstable();
        found
    }

    /// A file of `sheets` sheets. A run makes them from its start sheet. It
    /// stops once it has made `stop_after` (its goal), and ends by itself with
    /// exit 0 after `per_run` sheets (a source cut off part way) or at the end
    /// of the file. On reaching `stall_at` the first time, or instead of
    /// ending when `wait_for_cancel`, it waits to be cancelled. With
    /// `no_jump`, a run that starts part way makes no first sheet in time.
    /// With `empty_exit`, a run that ends by itself having made no sheet
    /// exits with that status and writes that to stderr, as ffmpeg 7 does
    /// (see [`PAST_END`]); without it, exit 0 as ffmpeg 6 does. Each sheet
    /// takes `pace`.
    ///
    /// A rough picture takes `grab_pace` and is written as [`JPEG`], or as a
    /// real 320x180 picture with `real_pictures`. One in `grab_fails` ends
    /// without a picture, one in `grab_timeouts` runs out its wait, and the
    /// one at `grab_hang` waits to be cancelled.
    ///
    /// A run or rough picture of an input in `dead` makes nothing and exits
    /// with an error, as a link that answers 404 does. An input in `empty`
    /// makes nothing and exits 0, as a source that ends every request at once
    /// does. An input in `long` holds one sheet more than `sheets`, one in
    /// `short` half of them; one in `cut` is cut off half way, exiting 0 with
    /// [`SOURCE_CUT`]. Asked its size, an input answers [`LISTED`], or
    /// three times that when it is in `wrong_size`.
    #[derive(Default)]
    struct FakeRunner {
        sheets: u64,
        per_run: Option<u64>,
        stall_at: Option<u64>,
        wait_for_cancel: bool,
        no_jump: bool,
        empty_exit: Option<(i32, &'static str)>,
        pace: Duration,
        runs: AtomicUsize,
        killed: AtomicBool,
        /// Some run has stalled at `stall_at`; later runs pass it.
        stalled_once: AtomicBool,
        starts: Mutex<Vec<u32>>,
        grab_pace: Duration,
        real_pictures: bool,
        grab_fails: Vec<u64>,
        grab_timeouts: Vec<u64>,
        grab_hang: Option<u64>,
        grabs: Mutex<Vec<u64>>,
        /// The wait each rough picture was given.
        grab_waits: Mutex<Vec<Duration>>,
        /// The rough files in the set's folder as each run started.
        rough_at_runs: Mutex<Vec<Vec<String>>>,
        dead: Vec<String>,
        empty: Vec<String>,
        long: Vec<String>,
        short: Vec<String>,
        cut: Vec<String>,
        wrong_size: Vec<String>,
        /// The input of each run so far.
        inputs: Mutex<Vec<String>>,
        /// The inputs whose size was asked so far.
        sized: Mutex<Vec<String>>,
    }

    impl FakeRunner {
        /// The start sheet of each run so far.
        fn starts(&self) -> Vec<u32> {
            self.starts
                .lock()
                .unwrap()
                .clone()
        }

        /// The rough pictures asked for so far, in order.
        fn grabs(&self) -> Vec<u64> {
            self.grabs
                .lock()
                .unwrap()
                .clone()
        }

        /// The input of each run so far.
        fn inputs(&self) -> Vec<String> {
            self.inputs
                .lock()
                .unwrap()
                .clone()
        }

        /// The inputs whose size was asked so far.
        fn sized(&self) -> Vec<String> {
            self.sized
                .lock()
                .unwrap()
                .clone()
        }
    }

    /// The names of the rough files in a set's folder, sorted.
    fn rough_files(dir: &Path) -> Vec<String> {
        let mut found: Vec<String> = std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter_map(|e| {
                        e.file_name()
                            .to_str()
                            .map(str::to_string)
                    })
                    .filter(|name| name.starts_with('r'))
                    .collect()
            })
            .unwrap_or_default();
        found.sort_unstable();
        found
    }

    /// A flat 320x180 picture of one colour, as ffmpeg writes a rough one.
    fn flat_picture(rgb: [u8; 3]) -> Vec<u8> {
        let picture = image::RgbImage::from_pixel(WIDTH, HEIGHT, image::Rgb(rgb));
        let mut out = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 90)
            .encode_image(&picture)
            .unwrap();
        out
    }

    impl PassRunner for FakeRunner {
        fn run(
            &self,
            job: PassJob,
            cancel: oneshot::Receiver<()>,
        ) -> impl Future<Output = RunResult> + Send {
            self.runs
                .fetch_add(1, Ordering::SeqCst);
            self.starts
                .lock()
                .unwrap()
                .push(job.start_sheet);
            self.rough_at_runs
                .lock()
                .unwrap()
                .push(rough_files(&job.out_dir));
            self.inputs
                .lock()
                .unwrap()
                .push(
                    job.input
                        .clone(),
                );
            let dead = self
                .dead
                .contains(&job.input);
            let empty = self
                .empty
                .contains(&job.input);
            let cut = self
                .cut
                .contains(&job.input);
            let sheets = if self
                .long
                .contains(&job.input)
            {
                self.sheets + 1
            } else if cut
                || self
                    .short
                    .contains(&job.input)
            {
                self.sheets / 2
            } else {
                self.sheets
            };
            async move {
                let mut cancel = cancel;
                if dead {
                    return RunResult {
                        exit_code: Some(1),
                        stderr: "[http @ 0x6] HTTP error 404 Not Found\n\
                                 Error opening input: Server returned 404 Not Found\n"
                            .to_string(),
                        ..Default::default()
                    };
                }
                if empty {
                    return RunResult {
                        exit_ok: true,
                        exit_code: Some(0),
                        ..Default::default()
                    };
                }
                let start = u64::from(job.start_sheet);
                if self.no_jump && start > 0 {
                    return RunResult {
                        no_first_sheet: true,
                        ..Default::default()
                    };
                }
                let mut made = 0;
                let mut stalled = false;
                while start + made < sheets
                    && self
                        .per_run
                        .is_none_or(|most| made < most)
                {
                    if job
                        .stop_after
                        .is_some_and(|goal| made >= goal)
                    {
                        return RunResult {
                            made_sheets: made,
                            reached_goal: true,
                            ..Default::default()
                        };
                    }
                    if self.stall_at == Some(start + made)
                        && !self
                            .stalled_once
                            .swap(true, Ordering::SeqCst)
                    {
                        stalled = true;
                        break;
                    }
                    std::fs::write(
                        job.out_dir
                            .join(format!("{}.jpg", start + made)),
                        JPEG,
                    )
                    .unwrap();
                    made += 1;
                    job.made
                        .store(u32::try_from(made).unwrap(), Ordering::SeqCst);
                    if !self
                        .pace
                        .is_zero()
                    {
                        tokio::select! {
                            _ = &mut cancel => {
                                self.killed
                                    .store(true, Ordering::SeqCst);
                                return RunResult {
                                    made_sheets: made,
                                    cancelled: true,
                                    ..Default::default()
                                };
                            }
                            _ = tokio::time::sleep(self.pace) => {}
                        }
                    }
                }
                if stalled || self.wait_for_cancel {
                    let _ = cancel.await;
                    self.killed
                        .store(true, Ordering::SeqCst);
                    return RunResult {
                        made_sheets: made,
                        cancelled: true,
                        ..Default::default()
                    };
                }
                if made == 0
                    && let Some((code, stderr)) = self.empty_exit
                {
                    return RunResult {
                        exit_ok: code == 0,
                        exit_code: Some(code),
                        stderr: stderr.to_string(),
                        ..Default::default()
                    };
                }
                RunResult {
                    exit_ok: true,
                    exit_code: Some(0),
                    made_sheets: made,
                    stderr: if cut {
                        SOURCE_CUT.to_string()
                    } else {
                        String::new()
                    },
                    ..Default::default()
                }
            }
        }

        fn grab(
            &self,
            job: GrabJob,
            cancel: oneshot::Receiver<()>,
        ) -> impl Future<Output = Grab> + Send {
            let picture = job.at_s / INTERVAL_SECS;
            self.grabs
                .lock()
                .unwrap()
                .push(picture);
            self.grab_waits
                .lock()
                .unwrap()
                .push(job.wait);
            let dead = self
                .dead
                .contains(&job.input);
            async move {
                let mut cancel = cancel;
                if dead {
                    return Grab::Failed;
                }
                if self.grab_hang == Some(picture) {
                    let _ = cancel.await;
                    return Grab::Cancelled;
                }
                if self
                    .grab_timeouts
                    .contains(&picture)
                {
                    tokio::select! {
                        _ = &mut cancel => return Grab::Cancelled,
                        _ = tokio::time::sleep(job.wait) => return Grab::TimedOut,
                    }
                }
                if !self
                    .grab_pace
                    .is_zero()
                {
                    tokio::select! {
                        _ = &mut cancel => return Grab::Cancelled,
                        _ = tokio::time::sleep(self.grab_pace) => {}
                    }
                }
                if self
                    .grab_fails
                    .contains(&picture)
                {
                    return Grab::Failed;
                }
                let bytes = if self.real_pictures {
                    flat_picture([0, 0, 0])
                } else {
                    JPEG.to_vec()
                };
                std::fs::write(&job.out, bytes).unwrap();
                Grab::Made
            }
        }

        fn size(&self, input: &str) -> impl Future<Output = Option<u64>> + Send {
            self.sized
                .lock()
                .unwrap()
                .push(input.to_string());
            let size = if self
                .wrong_size
                .iter()
                .any(|i| i == input)
            {
                LISTED * 3
            } else {
                LISTED
            };
            async move { Some(size) }
        }
    }

    /// What this server's ffmpeg 7 wrote at `-loglevel error` for a run with
    /// the pass's arguments started past the end of a file, exit 234 and no
    /// file written (the end-to-end pre-check, from the file itself).
    const PAST_END: &str = "\
        [mjpeg @ 0x8] Non full-range YUV is non-standard, set strict_std_compliance to at \
        most unofficial to use it.\n\
        [mjpeg @ 0x1] ff_frame_thread_encoder_init failed\n\
        [vost#0:0/mjpeg @ 0x2] [enc:mjpeg @ 0x3] Error while opening encoder - maybe incorrect \
        parameters such as bit_rate, rate, width or height.\n\
        [vf#0:0 @ 0x4] Error sending frames to consumers: Invalid argument\n\
        [vf#0:0 @ 0x4] Task finished with error code: -22 (Invalid argument)\n\
        [vf#0:0 @ 0x4] Terminating thread with return code -22 (Invalid argument)\n\
        [vost#0:0/mjpeg @ 0x2] [enc:mjpeg @ 0x3] Could not open encoder before EOF\n\
        [vost#0:0/mjpeg @ 0x2] Task finished with error code: -22 (Invalid argument)\n\
        [vost#0:0/mjpeg @ 0x2] Terminating thread with return code -22 (Invalid argument)\n\
        [out#0/image2 @ 0x5] Nothing was written into output file, because at least one of \
        its streams received no packets.\n";

    /// The same over HTTP from a source that closed a request at the file's
    /// very end (the pre-check over HTTP), before the lines of [`PAST_END`].
    const PAST_END_HTTP: &str = "\
        [http @ 0x6] Error reading HTTP response: End of file\n\
        [in#0/matroska,webm @ 0x7] File ended prematurely at pos. 70027876 (0x42c8a64)\n";

    /// A source that dropped after the seek and did not come back, before the
    /// lines of [`PAST_END`]: ffmpeg 7 then ends its input as at the end of
    /// the file, so the status is the same.
    const SOURCE_DROPPED: &str = "\
        [http @ 0x6] Stream ends prematurely at 52428800, should be 70027876\n\
        [in#0/matroska,webm @ 0x7] Read error at pos. 52428800 (0x3200000)\n\
        [in#0/matroska,webm @ 0x7] Error during demuxing: Input/output error\n";

    /// What this server's ffmpeg wrote, with exit 0 and sheets made, for a
    /// source that cut every answer off part way (the end-to-end check).
    const SOURCE_CUT: &str = "\
        [http @ 0x6] Stream ends prematurely at 49019513, should be 70027876\n\
        [http @ 0x6] Error reading HTTP response: End of file\n\
        [http @ 0x6] Stream ends prematurely at 49019513, should be 18446744073709551615\n\
        [in#0/matroska,webm @ 0x7] Read error\n\
        [http @ 0x6] Stream ends prematurely at 49019513, should be 70027876\n\
        [in#0/matroska,webm @ 0x8] Error during demuxing: Input/output error\n";

    async fn until(check: impl FnMut() -> bool) {
        until_named("condition", check).await;
    }

    /// As [`until`], naming the wait that gave up.
    async fn until_named(label: &str, mut check: impl FnMut() -> bool) {
        for _ in 0..400 {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("{label} not reached");
    }

    fn row(descriptor: StreamDescriptor) -> (db::Media, StreamInfo) {
        let info = StreamInfo {
            descriptor,
            ..Default::default()
        };
        let media = db::Media {
            title: "Invented Film".to_string(),
            kind: db::MediaKind::Stream,
            stream_info: Some(info.clone()),
            ..Default::default()
        };
        (media, info)
    }

    fn addon(kind: &str, redirect: bool) -> Addon {
        let now = chrono::Utc::now().naive_utc();
        Addon {
            id: Uuid::new_v4(),
            name: "Invented Addon".to_string(),
            preset: crate::addons::AddonPresetRef {
                kind: kind.to_string(),
                ..Default::default()
            },
            resources: vec![],
            types: vec![],
            enabled: true,
            priority: 0,
            created_at: now,
            updated_at: now,
            system: false,
            is_default: true,
            http_redirect_stream: redirect,
            subtitle_extraction: false,
            probe_on_scan: false,
            service_filter: vec![],
            trickplay: true,
            trickplay_service_filter: vec![],
        }
    }

    fn item(
        type_: api::MediaType,
        runtime_s: Option<i64>,
        ids: &[Uuid],
    ) -> api::BaseItemDto {
        api::BaseItemDto {
            id: ids
                .first()
                .copied()
                .unwrap_or_else(Uuid::new_v4),
            type_,
            run_time_ticks: runtime_s.map(|s| s * TICKS_PER_SEC),
            media_sources: Some(
                ids.iter()
                    .map(|id| api::MediaSourceInfo {
                        id: *id,
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn off_by_default() {
        let cfg = api::ServerConfiguration::default();
        assert_eq!(cfg.trickplay_while_streaming, Some(false));
        assert_eq!(cfg.trickplay_cache_size_mb, Some(1024));
        assert_eq!(cfg.trickplay_keep_days, Some(30));
        assert!(!enabled(&cfg));
        assert_eq!(cfg.trickplay_rough_first, Some(false));
        assert!(!rough_first(&cfg));
        let switches = |main: bool, rough: bool| api::ServerConfiguration {
            trickplay_while_streaming: Some(main),
            trickplay_rough_first: Some(rough),
            ..Default::default()
        };
        assert!(!rough_first(&switches(true, false)));
        assert!(
            !rough_first(&switches(false, true)),
            "read only while the main switch is on"
        );
        assert!(rough_first(&switches(true, true)));
        assert_eq!(cfg.trickplay_other_version, Some(false));
        assert!(!other_version(&cfg));
        let other = |main: bool, other: bool| api::ServerConfiguration {
            trickplay_while_streaming: Some(main),
            trickplay_other_version: Some(other),
            ..Default::default()
        };
        assert!(!other_version(&other(true, false)));
        assert!(
            !other_version(&other(false, true)),
            "read only while the main switch is on"
        );
        assert!(other_version(&other(true, true)));
        let saved: api::ServerConfiguration = serde_json::from_value(
            serde_json::json!({ "TrickplayWhileStreaming": true }),
        )
        .unwrap();
        assert!(
            !other_version(&saved),
            "a saved document without it reads as off"
        );
        let small = api::ServerConfiguration {
            trickplay_cache_size_mb: Some(1),
            ..Default::default()
        };
        assert_eq!(cap_bytes(&small), 100 * 1024 * 1024);
    }

    #[test]
    fn keep_for_reads_days_and_zero_means_never() {
        let day = Duration::from_secs(24 * 60 * 60);
        let with = |days: Option<i64>| {
            keep_for(&api::ServerConfiguration {
                trickplay_keep_days: days,
                ..Default::default()
            })
        };
        assert_eq!(with(None), Some(30 * day));
        assert_eq!(with(Some(7)), Some(7 * day));
        assert_eq!(with(Some(0)), None);
        assert_eq!(with(Some(-5)), None);
        assert_eq!(with(Some(99_999)), Some(3_650 * day));

        // A saved document from before the setting reads as 30 days.
        let saved: api::ServerConfiguration = serde_json::from_value(
            serde_json::json!({ "TrickplayWhileStreaming": true }),
        )
        .unwrap();
        assert_eq!(saved.trickplay_keep_days, Some(30));
    }

    /// A stream row of `secs` seconds under `parent`, measured by `origin`.
    fn measured(
        parent: Option<Uuid>,
        secs: f64,
        origin: Option<api::ProbeOrigin>,
    ) -> db::Media {
        db::Media {
            title: "Invented.Film.1080p.mkv".to_string(),
            kind: db::MediaKind::Stream,
            parent_id: parent,
            probe_data: Some(api::MediaSourceInfo {
                run_time_ticks: Some((secs * TICKS_PER_SEC as f64).round() as i64),
                remux: Some(api::MediaSourceRemuxInfo {
                    provider_info: None,
                    source: origin,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn picture_count_rounds_up_per_ten_seconds() {
        assert_eq!(picture_count(7_200 * TICKS_PER_SEC), 720);
        assert_eq!(picture_count(7_205 * TICKS_PER_SEC), 721);
        assert_eq!(picture_count(1), 1);
        assert_eq!(picture_count(0), 0);
        assert_eq!(picture_count(-5), 0);
    }

    #[test]
    fn set_id_shares_one_count_under_one_title() {
        let film = Uuid::new_v4();
        let ffprobe = Some(api::ProbeOrigin::Ffprobe);
        let remuxdb = Some(api::ProbeOrigin::RemuxDb);
        let a = measured(Some(film), 9_391.756, ffprobe);
        let b = measured(Some(film), 9_391.776, remuxdb);
        assert_ne!(a.id, b.id);
        assert_eq!(set_id(&a), set_id(&b), "same picture count, same title");
        assert_ne!(set_id(&a), a.id);

        let shorter = measured(Some(film), 9_382.4, ffprobe);
        assert_ne!(set_id(&a), set_id(&shorter), "939 pictures, not 940");

        let other_title = measured(Some(Uuid::new_v4()), 9_391.756, ffprobe);
        assert_ne!(set_id(&a), set_id(&other_title));

        // Anything not measured, or not a stream under a title, keeps its own.
        let guessed =
            measured(Some(film), 9_391.756, Some(api::ProbeOrigin::FilenameGuess));
        assert_eq!(set_id(&guessed), guessed.id);
        let unknown_origin = measured(Some(film), 9_391.756, None);
        assert_eq!(set_id(&unknown_origin), unknown_origin.id);
        let no_probe = db::Media {
            probe_data: None,
            ..measured(Some(film), 9_391.756, ffprobe)
        };
        assert_eq!(set_id(&no_probe), no_probe.id);
        let no_parent = measured(None, 9_391.756, ffprobe);
        assert_eq!(set_id(&no_parent), no_parent.id);
        let not_a_stream = db::Media {
            kind: db::MediaKind::Movie,
            ..measured(Some(film), 9_391.756, ffprobe)
        };
        assert_eq!(set_id(&not_a_stream), not_a_stream.id);
        let zero = measured(Some(film), 0.0, ffprobe);
        assert_eq!(set_id(&zero), zero.id);

        // One episode, played from its own file and from a file holding two.
        let episode = Uuid::new_v4();
        let single = measured(Some(episode), 720.0, ffprobe);
        let combined = measured(Some(episode), 1_407.0, ffprobe);
        assert_ne!(set_id(&single), set_id(&combined));
    }

    #[test]
    fn a_streams_own_folder_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let stream = measured(
            Some(Uuid::new_v4()),
            9_391.756,
            Some(api::ProbeOrigin::Ffprobe),
        );
        assert_eq!(set_for(tmp.path(), &stream), set_id(&stream));
        std::fs::create_dir_all(set_dir(tmp.path(), stream.id)).unwrap();
        assert_eq!(set_for(tmp.path(), &stream), stream.id);
    }

    #[test]
    fn manifest_counts_each_source_by_its_own_length() {
        let item_id = Uuid::new_v4();
        let combined = Uuid::new_v4();
        let unmeasured = Uuid::new_v4();
        let key = |id: Uuid| {
            id.as_simple()
                .to_string()
        };
        let source = |id: Uuid, secs: Option<i64>| api::MediaSourceInfo {
            id,
            run_time_ticks: secs.map(|s| s * TICKS_PER_SEC),
            ..Default::default()
        };

        // A title listed at 720 s, with a file holding two episodes and a
        // source of no known length.
        let dto = api::BaseItemDto {
            id: item_id,
            type_: api::MediaType::Episode,
            run_time_ticks: Some(720 * TICKS_PER_SEC),
            media_sources: Some(vec![
                source(combined, Some(1_407)),
                source(unmeasured, None),
            ]),
            ..Default::default()
        };
        let out = manifest(&dto, |_| true);
        assert_eq!(out[&key(combined)]["320"].thumbnail_count, 141);
        assert_eq!(out[&key(unmeasured)]["320"].thumbnail_count, 72);

        // No listed runtime: a source of known length is still advertised.
        let dto = api::BaseItemDto {
            run_time_ticks: None,
            media_sources: Some(vec![source(combined, Some(1_407))]),
            ..dto
        };
        let out = manifest(&dto, |_| true);
        assert_eq!(out[&key(combined)]["320"].thumbnail_count, 141);

        // The first source carries the item id and the first row's length;
        // another source is told its own.
        let second = Uuid::new_v4();
        let dto = api::BaseItemDto {
            id: item_id,
            type_: api::MediaType::Movie,
            run_time_ticks: Some(9_391 * TICKS_PER_SEC),
            media_sources: Some(vec![
                source(item_id, Some(9_391)),
                source(second, Some(7_800)),
            ]),
            ..Default::default()
        };
        let out = manifest(&dto, |_| true);
        assert_eq!(out[&key(item_id)]["320"].thumbnail_count, 940);
        assert_eq!(out[&key(second)]["320"].thumbnail_count, 780);

        // Each source is judged by its own length: under a title listed at
        // 4 min, a source of no known length gets nothing, a 2 h file does.
        let short = Uuid::new_v4();
        let dto = api::BaseItemDto {
            id: item_id,
            type_: api::MediaType::Movie,
            run_time_ticks: Some(240 * TICKS_PER_SEC),
            media_sources: Some(vec![source(short, None), source(second, Some(7_200))]),
            ..Default::default()
        };
        let out = manifest(&dto, |_| true);
        assert!(!out.contains_key(&key(short)));
        assert_eq!(out[&key(second)]["320"].thumbnail_count, 720);
    }

    #[test]
    fn manifest_counts_from_runtime() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let refused = Uuid::new_v4();
        let dto = item(
            api::MediaType::Movie,
            Some(7_200),
            &[first, second, refused],
        );
        let refused_key = refused
            .as_simple()
            .to_string();
        let out = manifest(&dto, |id| id != refused_key);
        assert_eq!(out.len(), 2);
        let entry = &out[&first
            .as_simple()
            .to_string()]["320"];
        assert_eq!(entry.thumbnail_count, 720);
        assert_eq!((entry.width, entry.height), (320, 180));
        assert_eq!((entry.tile_width, entry.tile_height), (5, 5));
        assert_eq!(entry.interval, 10_000);
        assert!(
            out.contains_key(
                &second
                    .as_simple()
                    .to_string()
            )
        );
        assert!(!out.contains_key(&refused_key));
        assert_eq!(
            dto.id
                .as_simple()
                .to_string(),
            first
                .as_simple()
                .to_string()
        );

        let longer = item(api::MediaType::Episode, Some(7_205), &[first]);
        let out = manifest(&longer, |_| true);
        assert_eq!(
            out[&first
                .as_simple()
                .to_string()]["320"]
                .thumbnail_count,
            721
        );

        let json = serde_json::to_value(&out).unwrap();
        let value = &json[first
            .as_simple()
            .to_string()]["320"];
        assert_eq!(value["ThumbnailCount"], 721);
        assert_eq!(value["TileWidth"], 5);
        assert_eq!(value["Bandwidth"], 10_400);
    }

    #[test]
    fn manifest_empty_for_live_short_long_unknown() {
        let id = Uuid::new_v4();
        for dto in [
            item(api::MediaType::TvChannel, Some(7_200), &[id]),
            item(api::MediaType::Movie, None, &[id]),
            item(api::MediaType::Movie, Some(0), &[id]),
            item(api::MediaType::Movie, Some(299), &[id]),
            item(api::MediaType::Movie, Some(4 * 3_600 + 1), &[id]),
            item(api::MediaType::Audio, Some(7_200), &[id]),
        ] {
            assert!(manifest(&dto, |_| true).is_empty(), "{:?}", dto.type_);
        }
        let mut infinite = item(api::MediaType::Video, Some(7_200), &[id]);
        if let Some(sources) = infinite
            .media_sources
            .as_mut()
        {
            sources[0].is_infinite_stream = true;
        }
        assert!(manifest(&infinite, |_| true).is_empty());
        let stream = item(api::MediaType::Video, Some(300), &[id]);
        assert!(!manifest(&stream, |_| true).is_empty());
    }

    #[test]
    fn manifest_without_sources_is_keyed_by_the_item() {
        let id = Uuid::new_v4();
        let dto = api::BaseItemDto {
            media_sources: None,
            ..item(api::MediaType::Episode, Some(1_800), &[id])
        };
        let out = manifest(&dto, |_| true);
        let keys: Vec<&String> = out
            .keys()
            .collect();
        assert_eq!(
            keys,
            vec![
                &id.as_simple()
                    .to_string()
            ]
        );
        assert!(manifest(&dto, |_| false).is_empty());
    }

    #[test]
    fn eligible_row_rejects_each_reason() {
        let ok = Some(RUNTIME);
        let (mut live, info) = row(StreamDescriptor::http(URL));
        live.kind = db::MediaKind::TvChannel;
        assert_eq!(eligible_row(&live, &info, ok), Err(Ineligible::LiveTv));

        let (mut under_channel, info) = row(StreamDescriptor::http(URL));
        under_channel.parent = Some(Arc::new(db::Media {
            kind: db::MediaKind::TvChannel,
            ..Default::default()
        }));
        assert_eq!(
            eligible_row(&under_channel, &info, ok),
            Err(Ineligible::LiveTv)
        );

        let (media, mut info) = row(StreamDescriptor::http(URL));
        info.catchup_source = Some("http://example.invalid/{utc}".to_string());
        assert_eq!(eligible_row(&media, &info, ok), Err(Ineligible::Catchup));

        let (media, info) = row(StreamDescriptor::rtsp("rtsp://example.invalid/live"));
        assert_eq!(eligible_row(&media, &info, ok), Err(Ineligible::Rtsp));

        let (media, info) = row(StreamDescriptor::Torrent {
            info_hash: "0123456789abcdef0123456789abcdef01234567".to_string(),
            file_hint: None,
            file_idx: None,
            trackers: vec![],
        });
        assert_eq!(eligible_row(&media, &info, ok), Err(Ineligible::Torrent));

        let (mut media, info) = row(StreamDescriptor::http("http://example.invalid/a"));
        media.probe_data = Some(api::MediaSourceInfo {
            container: Some(api::VideoContainer::Other("hls".to_string())),
            ..Default::default()
        });
        assert_eq!(eligible_row(&media, &info, ok), Err(Ineligible::Hls));

        let (media, info) =
            row(StreamDescriptor::http("http://example.invalid/list.m3u8"));
        assert_eq!(eligible_row(&media, &info, ok), Err(Ineligible::Hls));

        let (media, info) = row(StreamDescriptor::http(URL));
        assert_eq!(
            eligible_row(&media, &info, None),
            Err(Ineligible::NoRuntime)
        );
        assert_eq!(
            eligible_row(&media, &info, Some(299)),
            Err(Ineligible::TooShort)
        );
        assert_eq!(
            eligible_row(&media, &info, Some(4 * 3_600 + 1)),
            Err(Ineligible::TooLong)
        );
    }

    #[test]
    fn eligible_pass_rejects_iptv_kind_and_redirect() {
        let (_, info) = row(StreamDescriptor::http(URL));
        assert_eq!(
            eligible_pass(Some(&addon("iptv", false)), &info),
            Err(Ineligible::Iptv)
        );
        assert_eq!(
            eligible_pass(Some(&addon("iptv-m3u", false)), &info),
            Err(Ineligible::Iptv)
        );
        assert_eq!(
            eligible_pass(Some(&addon("iptv-xtream", false)), &info),
            Err(Ineligible::Iptv)
        );
        assert_eq!(
            eligible_pass(Some(&addon("stremio", true)), &info),
            Err(Ineligible::Redirect)
        );
    }

    #[test]
    fn addon_allows_follows_switch_and_service_list() {
        let filter = ["realdebrid".to_string(), "Inner Addon".to_string()];
        // Off.
        assert!(!addon_allows(false, &[], Some("realdebrid"), None));
        assert!(!addon_allows(false, &filter, Some("realdebrid"), None));
        // On with an empty filter.
        assert!(addon_allows(true, &[], None, None));
        assert!(addon_allows(true, &[], Some("torbox"), None));
        // A filter matching the service.
        assert!(addon_allows(true, &filter, Some("realdebrid"), None));
        // A filter matching the inner addon name.
        assert!(addon_allows(true, &filter, None, Some("Inner Addon")));
        // Ignoring case.
        assert!(addon_allows(true, &filter, Some("RealDebrid"), None));
        assert!(addon_allows(true, &filter, None, Some("inner addon")));
        // Neither in the list.
        assert!(!addon_allows(
            true,
            &filter,
            Some("torbox"),
            Some("Other Addon")
        ));
        // No service and no addon name against a non-empty filter.
        assert!(!addon_allows(true, &filter, None, None));
    }

    #[test]
    fn eligible_pass_rejects_addon_switched_off_and_filtered_service() {
        let (_, mut info) = row(StreamDescriptor::http(URL));
        info.service_id = Some("realdebrid".to_string());

        let mut off = addon("stremio", false);
        off.trickplay = false;
        assert_eq!(
            eligible_pass(Some(&off), &info),
            Err(Ineligible::AddonExcluded)
        );

        let mut filtered = addon("stremio", false);
        filtered.trickplay_service_filter = vec!["torbox".to_string()];
        assert_eq!(
            eligible_pass(Some(&filtered), &info),
            Err(Ineligible::AddonExcluded)
        );
        let (_, no_service) = row(StreamDescriptor::http(URL));
        assert_eq!(
            eligible_pass(Some(&filtered), &no_service),
            Err(Ineligible::AddonExcluded)
        );

        filtered.trickplay_service_filter = vec!["RealDebrid".to_string()];
        assert_eq!(eligible_pass(Some(&filtered), &info), Ok(()));
        assert_eq!(
            eligible_pass(Some(&filtered), &no_service),
            Err(Ineligible::AddonExcluded)
        );
    }

    #[test]
    fn addon_ok_follows_the_snapshot() {
        let passes = TrickplayPasses::new();
        let mut off = addon("stremio", false);
        off.trickplay = false;
        let iptv = addon("iptv-m3u", false);
        let redirect = addon("stremio", true);
        let on = addon("stremio", false);
        passes.set_addon_rules(&[
            off.clone(),
            iptv.clone(),
            redirect.clone(),
            on.clone(),
        ]);
        let rules = passes.addon_rules();
        let from = |addon_id: Option<Uuid>| StreamInfo {
            descriptor: StreamDescriptor::http(URL),
            addon_id,
            ..Default::default()
        };
        assert!(!addon_ok(&rules, &from(Some(off.id))));
        assert!(!addon_ok(&rules, &from(Some(iptv.id))));
        assert!(!addon_ok(&rules, &from(Some(redirect.id))));
        assert!(addon_ok(&rules, &from(Some(on.id))));
        // Not in the snapshot, or no addon: judged by the row alone.
        assert!(addon_ok(&rules, &from(Some(Uuid::new_v4()))));
        assert!(addon_ok(&rules, &from(None)));

        passes.set_addon_rules(&[on]);
        assert!(addon_ok(&passes.addon_rules(), &from(Some(off.id))));
    }

    #[test]
    fn eligible_accepts_http_local_opendal() {
        for descriptor in [
            StreamDescriptor::http(URL),
            StreamDescriptor::Local("/media/invented-film.mkv".into()),
            StreamDescriptor::Opendal {
                addon_id: Uuid::new_v4(),
                path: "films/invented-film.mkv".to_string(),
            },
        ] {
            let (media, info) = row(descriptor);
            assert_eq!(eligible_row(&media, &info, Some(RUNTIME)), Ok(()));
        }
        let (_, info) = row(StreamDescriptor::http(URL));
        assert_eq!(eligible_pass(None, &info), Ok(()));
        assert_eq!(eligible_pass(Some(&addon("stremio", false)), &info), Ok(()));
    }

    #[test]
    fn runtime_prefers_probe_then_item_then_listed() {
        let (mut media, _) = row(StreamDescriptor::http(URL));
        if let Some(info) = media
            .stream_info
            .as_mut()
        {
            info.duration = Some(1_000);
        }
        assert_eq!(runtime_s(&media, None), Some(1_000));
        assert_eq!(runtime_s(&media, Some(2_000)), Some(2_000));
        media.probe_data = Some(api::MediaSourceInfo {
            run_time_ticks: Some(3_000 * TICKS_PER_SEC),
            ..Default::default()
        });
        assert_eq!(runtime_s(&media, Some(2_000)), Some(3_000));
    }

    #[test]
    fn parse_sheet_accepts_only_plain_numbers() {
        assert_eq!(parse_sheet("0.jpg"), Some(0));
        assert_eq!(parse_sheet("12.jpg"), Some(12));
        assert_eq!(parse_sheet("999999.jpg"), Some(999_999));
        for bad in [
            "x.jpg",
            "-1.jpg",
            "+1.jpg",
            "01.jpg",
            "1234567.jpg",
            "1.png",
            "1.jpg.tmp",
            "../0.jpg",
            ".jpg",
            "",
        ] {
            assert_eq!(parse_sheet(bad), None, "{bad}");
        }
    }

    #[test]
    fn pass_args_reads_key_frames_from_the_proxy() {
        let stream_id = Uuid::new_v4();
        let info = StreamInfo {
            descriptor: StreamDescriptor::http("https://source.invalid/secret/a.mkv"),
            ..Default::default()
        };
        let input = pass_input(&info, stream_id, 8096);
        assert_eq!(
            input,
            format!(
                "http://127.0.0.1:8096/stream/{stream_id}?token={}",
                crate::stream::stream_link_token(stream_id)
            )
        );
        assert!(!input.contains("source.invalid"));

        let out = Path::new("/data/trickplay/abc");
        let args = pass_args(&input, out, 0);
        let at = |flag: &str| {
            args.iter()
                .position(|a| a == flag)
                .unwrap_or_else(|| panic!("{flag} missing"))
        };
        assert!(
            !args
                .iter()
                .any(|a| a == "-ss"),
            "a run from the start does not seek"
        );
        assert_eq!(args[at("-skip_frame") + 1], "nokey");
        assert!(at("-skip_frame") < at("-i"));
        assert_eq!(args[at("-i") + 1], input);
        assert_eq!(args[at("-threads") + 1], "1");
        assert!(at("-reconnect") < at("-i"));
        let filter = &args[at("-vf") + 1];
        assert!(filter.contains("fps=1/10:start_time=0"));
        assert!(filter.contains("pad=320:180"));
        assert!(filter.contains("tile=5x5"));
        assert_eq!(args[at("-atomic_writing") + 1], "1");
        assert_eq!(args[at("-start_number") + 1], "0");
        assert_eq!(args[at("-progress") + 1], "pipe:1");
        assert!(
            args.iter()
                .any(|a| a.ends_with("%d.jpg"))
        );

        let local = StreamInfo {
            descriptor: StreamDescriptor::Local("/media/invented-film.mkv".into()),
            ..Default::default()
        };
        assert_eq!(
            pass_input(&local, stream_id, 8096),
            "/media/invented-film.mkv"
        );
    }

    #[test]
    fn pass_args_part_way_seek_before_the_input_and_keep_the_grid() {
        let args = pass_args("/media/invented-film.mkv", Path::new("/data/t/abc"), 3);
        let at = |flag: &str| {
            args.iter()
                .position(|a| a == flag)
                .unwrap_or_else(|| panic!("{flag} missing"))
        };
        assert_eq!(args[at("-ss") + 1], "750");
        assert!(at("-skip_frame") < at("-ss"));
        assert!(at("-ss") < at("-i"));
        assert_eq!(args[at("-start_number") + 1], "3");
        assert!(args[at("-vf") + 1].starts_with("fps=1/10:start_time=0,"));
        assert!(
            !args
                .iter()
                .any(|a| a == "-frames:v")
        );
    }

    #[test]
    fn last_sheet_counts_sheets() {
        // 7,200 s: 720 pictures, 29 sheets.
        let pictures = picture_count(RUNTIME as i64 * TICKS_PER_SEC);
        assert_eq!(pictures, 720);
        assert_eq!(last_sheet(pictures), 28);
        assert_eq!(expected_sheets(pictures), 29);
        // 940 pictures fill 38 sheets.
        assert_eq!(expected_sheets(940), 38);
        assert_eq!(last_sheet(940), 37);
        assert_eq!(last_sheet(950), 37);
        assert_eq!(last_sheet(951), 38);
        assert_eq!(last_sheet(0), 0);
        assert_eq!(progress_frame("frame=28"), Some(28));
        assert_eq!(progress_frame("fps=0.00"), None);
    }

    #[tokio::test]
    async fn short_pass_is_failure_and_drops_last_sheet() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let stream_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 10,
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin(
                "play-a",
                plan(tmp.path(), stream_id),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        // A stream's own set: its count is a guess, so the route is not told it.
        assert_eq!(passes.pictures_of(stream_id), None);
        until(|| !passes.is_queued(stream_id)).await;

        let dir = set_dir(tmp.path(), stream_id);
        assert!(
            !dir.join(DONE_MARKER)
                .exists()
        );
        assert!(
            !dir.join("9.jpg")
                .exists()
        );
        assert!(
            dir.join("8.jpg")
                .exists()
        );
        assert!(passes.failed_recently(stream_id, Instant::now()));
    }

    #[tokio::test]
    async fn complete_pass_writes_done() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let stream_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                plan(tmp.path(), stream_id),
                runner.clone(),
                always(),
            )
            .await;
        until(|| !passes.is_queued(stream_id)).await;
        let dir = set_dir(tmp.path(), stream_id);
        assert!(
            dir.join(DONE_MARKER)
                .exists()
        );
        assert!(
            dir.join("28.jpg")
                .exists()
        );

        // A later play of the same file starts nothing.
        passes.register("play-b", Uuid::new_v4(), "device-b");
        let again = passes
            .begin(
                "play-b",
                plan(tmp.path(), stream_id),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(again, Begin::Done);
        assert_eq!(
            runner
                .runs
                .load(Ordering::SeqCst),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn prune_removes_tmp_old_then_lru_and_spares_active() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp
            .path()
            .join(ROOT_DIR);
        let now = SystemTime::now();
        let day = Duration::from_secs(24 * 60 * 60);
        let set_age = |path: &Path, age: Duration| {
            std::fs::File::open(path)
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(now - age))
                .unwrap();
        };
        let make = |files: &[(&str, usize)]| {
            let id = Uuid::new_v4();
            let dir = set_dir(&root, id);
            std::fs::create_dir_all(&dir).unwrap();
            for (name, len) in files {
                std::fs::write(dir.join(name), vec![0u8; *len]).unwrap();
            }
            (id, dir)
        };
        let (active_id, active) = make(&[("0.jpg", 100)]);
        let (_, old) = make(&[("0.jpg", 100)]);
        let (_, lru) = make(&[("0.jpg", 600)]);
        let (_, recent) = make(&[("0.jpg", 600), ("1.jpg.tmp", 10), ("2.jpg.tmp", 10)]);
        set_age(&recent.join("2.jpg.tmp"), Duration::from_secs(20 * 60));
        set_age(&active, 40 * day);
        set_age(&old, 40 * day);
        set_age(&lru, 2 * day);
        set_age(&recent, day);

        let keep = HashSet::from([active_id]);
        prune(&root, 1_000, Some(30 * day), &keep, now);
        assert!(active.exists(), "an active set is never deleted");
        assert!(!old.exists(), "unused for 30 days");
        assert!(!lru.exists(), "least recently used, over the cap");
        assert!(recent.exists());
        assert!(
            recent
                .join("1.jpg.tmp")
                .exists(),
            "a fresh temporary file is being written"
        );
        assert!(
            !recent
                .join("2.jpg.tmp")
                .exists()
        );

        let missing = tmp
            .path()
            .join("missing");
        prune_on_start(&missing, true, 0, None, now);
        assert!(!missing.exists());

        prune_on_start(&root, false, u64::MAX, None, now);
        assert!(!root.exists(), "switched off: every picture goes at start");
    }

    #[cfg(unix)]
    #[test]
    fn prune_on_start_mends_only_sets_a_crash_left_busy() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp
            .path()
            .join(ROOT_DIR);
        let now = SystemTime::now();
        let aged = |path: &Path, age: Duration| {
            std::fs::File::open(path)
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(now - age))
                .unwrap();
        };
        let minute = Duration::from_secs(60);

        // Unfinished, not busy: a stopped pass left only whole sheets.
        let unfinished = set_dir(&root, Uuid::new_v4());
        write_sheets(&unfinished, 0..3);

        // Busy: a crash. 2.jpg was written after the pass began, 3.jpg is
        // empty, 4.jpg is cut short.
        let crashed = set_dir(&root, Uuid::new_v4());
        write_sheets(&crashed, 0..3);
        std::fs::write(crashed.join("3.jpg"), b"").unwrap();
        std::fs::write(crashed.join("4.jpg"), [0xFF_u8, 0xD8, 0x00, 0x00]).unwrap();
        std::fs::write(crashed.join(BUSY_MARKER), b"").unwrap();
        for n in [0, 1, 3, 4] {
            aged(&crashed.join(format!("{n}.jpg")), 10 * minute);
        }
        aged(&crashed.join(BUSY_MARKER), 5 * minute);
        aged(&crashed.join("2.jpg"), minute);
        aged(&crashed, 3 * 24 * 60 * minute);
        let shown = std::fs::metadata(&crashed)
            .unwrap()
            .modified()
            .unwrap();

        // Finished and busy: its sheets are final.
        let finished = set_dir(&root, Uuid::new_v4());
        write_sheets(&finished, 0..3);
        std::fs::write(finished.join(DONE_MARKER), b"").unwrap();
        std::fs::write(finished.join(BUSY_MARKER), b"").unwrap();

        // Busy with `end`, its highest sheet written last: that sheet may be
        // partly filled, and the end it gave goes with it.
        let ended = set_dir(&root, Uuid::new_v4());
        write_sheets(&ended, 0..6);
        std::fs::write(ended.join(END_MARKER), b"").unwrap();
        std::fs::write(ended.join(BUSY_MARKER), b"").unwrap();
        for n in 0..5 {
            aged(&ended.join(format!("{n}.jpg")), 10 * minute);
        }
        aged(&ended.join(BUSY_MARKER), 5 * minute);
        aged(&ended.join("5.jpg"), minute);

        // Busy with `end`, a sheet below its highest written last: the end
        // stays.
        let resumed = set_dir(&root, Uuid::new_v4());
        write_sheets(&resumed, (0..3).chain(19..22));
        std::fs::write(resumed.join(END_MARKER), b"").unwrap();
        std::fs::write(resumed.join(BUSY_MARKER), b"").unwrap();
        for n in [0, 1] {
            aged(&resumed.join(format!("{n}.jpg")), 10 * minute);
        }
        aged(&resumed.join(BUSY_MARKER), 5 * minute);
        for n in 19..22 {
            aged(&resumed.join(format!("{n}.jpg")), 3 * minute);
        }
        aged(&resumed.join("2.jpg"), minute);

        for _ in 0..2 {
            prune_on_start(&root, true, u64::MAX, None, now);
        }
        assert_eq!(
            sheets_in(&unfinished),
            vec![0, 1, 2],
            "a set that is not busy keeps every sheet"
        );
        assert_eq!(sheets_in(&crashed), vec![0, 1]);
        assert!(
            !crashed
                .join(BUSY_MARKER)
                .exists()
        );
        assert_eq!(
            std::fs::metadata(&crashed)
                .unwrap()
                .modified()
                .unwrap(),
            shown,
            "when it was last shown is kept"
        );
        assert_eq!(sheets_in(&finished), vec![0, 1, 2]);
        assert!(
            !finished
                .join(BUSY_MARKER)
                .exists()
        );
        assert_eq!(sheets_in(&ended), vec![0, 1, 2, 3, 4]);
        assert!(
            !ended
                .join(END_MARKER)
                .exists(),
            "the next pass reads the end again"
        );
        assert_eq!(sheets_in(&resumed), vec![0, 1, 19, 20, 21]);
        assert!(
            resumed
                .join(END_MARKER)
                .exists()
        );
    }

    #[tokio::test]
    async fn release_with_grace() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let stream_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            wait_for_cancel: true,
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                plan(tmp.path(), stream_id),
                runner.clone(),
                always(),
            )
            .await;
        until(|| {
            runner
                .runs
                .load(Ordering::SeqCst)
                == 1
        })
        .await;

        // A retry within the grace period keeps the pass.
        passes.release("play-a");
        tokio::time::sleep(Duration::from_millis(10)).await;
        passes.register("play-b", Uuid::new_v4(), "device-a");
        assert!(passes.attach("play-b", stream_id));
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(passes.has_pass(stream_id));
        assert!(
            !runner
                .killed
                .load(Ordering::SeqCst)
        );

        // Once the grace period passes with nobody watching, it is cancelled.
        passes.release("play-b");
        until(|| {
            runner
                .killed
                .load(Ordering::SeqCst)
        })
        .await;
        assert!(!passes.has_pass(stream_id));
    }

    #[tokio::test]
    async fn release_before_resolve() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let stream_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner::default());
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes.release("play-a");
        assert!(!passes.attach("play-a", stream_id));
        let begun = passes
            .begin(
                "play-a",
                plan(tmp.path(), stream_id),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(begun, Begin::Released);
        assert!(!passes.has_pass(stream_id));
        assert_eq!(
            runner
                .runs
                .load(Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn queued_pass_cancelled_before_start() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let stream_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner::default());
        let held = passes
            .inner
            .slots
            .acquire()
            .await
            .unwrap();
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin(
                "play-a",
                plan(tmp.path(), stream_id),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        passes.release("play-a");
        until(|| !passes.has_pass(stream_id)).await;
        drop(held);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            runner
                .runs
                .load(Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn start_delay_and_failure_wait() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let stream_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner::default());

        // The play ended during the start delay: nothing starts.
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin(
                "play-a",
                plan(tmp.path(), stream_id),
                runner.clone(),
                never(),
            )
            .await;
        assert_eq!(begun, Begin::Released);
        assert!(
            passes
                .play_of("play-a")
                .is_none()
        );

        // A failure blocks the file for 10 minutes.
        passes.mark_failed(stream_id, Instant::now() + FAILURE_WAIT);
        passes.register("play-b", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin(
                "play-b",
                plan(tmp.path(), stream_id),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(begun, Begin::FailedRecently);
        assert_eq!(
            runner
                .runs
                .load(Ordering::SeqCst),
            0
        );
        assert!(!passes.failed_recently(
            stream_id,
            Instant::now() + FAILURE_WAIT + Duration::from_secs(1)
        ));
    }

    #[tokio::test]
    async fn selection_follows_registration() {
        let passes = TrickplayPasses::with_limits(fast());
        let item_id = Uuid::new_v4();
        let stream_id = Uuid::new_v4();
        passes.register("play-a", item_id, "device-a");
        assert!(passes.attach("play-a", stream_id));
        assert_eq!(passes.selected_set(item_id, "device-a"), Some(stream_id));
        assert_eq!(passes.selected_set(item_id, "device-b"), None);
        passes.release("play-a");
        until(|| !passes.has_pass(stream_id)).await;
        assert_eq!(passes.selected_set(item_id, "device-a"), None);
    }

    #[tokio::test]
    async fn stop_all_kills_every_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sessions = crate::playback_session::PlaybackSessionManager::new(
            tmp.path()
                .join("t"),
        );
        sessions.trickplay = TrickplayPasses::with_limits(fast());
        let stream_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            wait_for_cancel: true,
            ..Default::default()
        });
        sessions
            .trickplay
            .register("play-a", Uuid::new_v4(), "device-a");
        sessions
            .trickplay
            .begin(
                "play-a",
                plan(tmp.path(), stream_id),
                runner.clone(),
                always(),
            )
            .await;
        until(|| {
            runner
                .runs
                .load(Ordering::SeqCst)
                == 1
        })
        .await;

        sessions
            .stop_all()
            .await;
        assert_eq!(
            sessions
                .trickplay
                .registration_count(),
            0
        );
        assert!(
            !sessions
                .trickplay
                .has_pass(stream_id)
        );
        assert!(
            runner
                .killed
                .load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn start_without_play_session_id_is_released() {
        use crate::integration_test::{auth_header_with_token, authenticated_server};
        use http::{StatusCode, header::HeaderValue};
        use serde_json::json;

        let (server, guard, token) = authenticated_server().await;
        let ctx = &guard.0;
        let cfg = api::ServerConfiguration {
            trickplay_while_streaming: Some(true),
            ..db::Settings::get_config_or_default(&ctx.db).await
        };
        db::Settings::set_config(&ctx.db, &cfg)
            .await
            .unwrap();
        let mut stream = db::Media {
            title: "Invented Film".to_string(),
            kind: db::MediaKind::Stream,
            stream_info: Some(StreamInfo {
                descriptor: StreamDescriptor::http(URL),
                ..Default::default()
            }),
            probe_data: Some(api::MediaSourceInfo {
                run_time_ticks: Some(RUNTIME as i64 * TICKS_PER_SEC),
                ..Default::default()
            }),
            ..Default::default()
        };
        stream
            .save(&ctx.db)
            .await
            .unwrap();
        let auth = auth_header_with_token(&token);
        let item_id = stream
            .id
            .simple()
            .to_string();

        server
            .post("/sessions/playing")
            .add_header(
                http::header::AUTHORIZATION,
                HeaderValue::from_str(&auth).unwrap(),
            )
            .json(&json!({ "ItemId": item_id, "PositionTicks": 0 }))
            .await
            .assert_status(StatusCode::NO_CONTENT);
        assert_eq!(
            ctx.sessions
                .trickplay
                .registration_count(),
            1
        );

        server
            .post("/sessions/playing/stopped")
            .add_header(
                http::header::AUTHORIZATION,
                HeaderValue::from_str(&auth).unwrap(),
            )
            .json(&json!({ "ItemId": item_id, "PositionTicks": 0 }))
            .await
            .assert_status(StatusCode::NO_CONTENT);
        assert_eq!(
            ctx.sessions
                .trickplay
                .registration_count(),
            0
        );
    }

    #[tokio::test]
    async fn versions_of_one_set_share_one_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let (stream_a, stream_b, stream_c) =
            (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            ..Default::default()
        });
        // The slot is busy, so the first pass is still queued when the second
        // version is played.
        let held = passes
            .inner
            .slots
            .acquire()
            .await
            .unwrap();
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let first = passes
            .begin(
                "play-a",
                set_plan(tmp.path(), stream_a, set_id),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(first, Begin::Queued);
        // The sheet route can tell a sheet past the count from one not made.
        assert_eq!(passes.pictures_of(set_id), Some(RUNTIME / INTERVAL_SECS));
        assert_eq!(passes.pictures_of(Uuid::new_v4()), None);
        passes.register("play-b", Uuid::new_v4(), "device-b");
        let second = passes
            .begin(
                "play-b",
                set_plan(tmp.path(), stream_b, set_id),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(second, Begin::Joined);
        drop(held);
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(
            runner
                .runs
                .load(Ordering::SeqCst),
            1
        );
        assert!(
            set_dir(tmp.path(), set_id)
                .join(DONE_MARKER)
                .exists()
        );
        assert!(!set_dir(tmp.path(), stream_a).exists());

        // A third version of the same count starts nothing.
        passes.register("play-c", Uuid::new_v4(), "device-c");
        let third = passes
            .begin(
                "play-c",
                set_plan(tmp.path(), stream_c, set_id),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(third, Begin::Done);
        assert_eq!(
            runner
                .runs
                .load(Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn a_failure_waits_by_stream_not_by_set() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let (stream_a, stream_b) = (Uuid::new_v4(), Uuid::new_v4());
        // Makes no sheet: a failure.
        let runner = Arc::new(FakeRunner::default());
        passes.mark_failed(stream_a, Instant::now() + FAILURE_WAIT);

        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin(
                "play-a",
                set_plan(tmp.path(), stream_a, set_id),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(begun, Begin::FailedRecently);

        passes.register("play-b", Uuid::new_v4(), "device-b");
        let begun = passes
            .begin(
                "play-b",
                set_plan(tmp.path(), stream_b, set_id),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(
            runner
                .runs
                .load(Ordering::SeqCst),
            1
        );
        assert!(passes.failed_recently(stream_b, Instant::now()));
    }

    #[tokio::test]
    async fn a_cancelled_pass_keeps_every_sheet() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let stream_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), stream_id);
        write_sheets(&dir, 0..10);
        // It makes sheets 10 to 12, then is stopped.
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            per_run: Some(3),
            wait_for_cancel: true,
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                plan(tmp.path(), stream_id),
                runner.clone(),
                always(),
            )
            .await;
        until(|| {
            runner
                .runs
                .load(Ordering::SeqCst)
                == 1
        })
        .await;
        passes.release("play-a");
        until(|| {
            runner
                .killed
                .load(Ordering::SeqCst)
        })
        .await;
        until(|| !passes.has_pass(stream_id)).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(runner.starts(), vec![10], "built on what exists");
        assert_eq!(sheets_in(&dir), (0..13).collect::<Vec<u32>>());
        for marker in [DONE_MARKER, END_MARKER, BUSY_MARKER] {
            assert!(
                !dir.join(marker)
                    .exists(),
                "{marker}"
            );
        }
        assert!(!passes.failed_recently(stream_id, Instant::now()));
    }

    #[tokio::test]
    async fn a_failed_pass_drops_only_the_last_sheet_it_wrote() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let stream_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), stream_id);
        write_sheets(&dir, 0..10);
        // Sheets 10 to 12 of 29, then a false end of input.
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            per_run: Some(3),
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                plan(tmp.path(), stream_id),
                runner.clone(),
                always(),
            )
            .await;
        until(|| !passes.is_queued(stream_id)).await;
        assert_eq!(sheets_in(&dir), (0..12).collect::<Vec<u32>>());
        for marker in [DONE_MARKER, END_MARKER, BUSY_MARKER] {
            assert!(
                !dir.join(marker)
                    .exists(),
                "{marker}"
            );
        }
        assert!(passes.failed_recently(stream_id, Instant::now()));
    }

    #[test]
    fn a_pass_stopped_after_its_own_end_drops_its_last_sheet() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::new();
        let stream_id = Uuid::new_v4();
        let mut dir = set_dir(tmp.path(), stream_id);
        write_sheets(&dir, 0..20);
        let plan = plan(tmp.path(), stream_id);
        let run = Run {
            start: 0,
            stop_after: None,
        };

        // Killed while running: nothing is deleted.
        let killed = RunResult {
            made_sheets: 3,
            cancelled: true,
            ..Default::default()
        };
        let end = run_end(&killed, run, plan.pictures);
        assert_eq!(end, RunEnd::Cancelled);
        passes.after_run(&plan, &mut dir, run, &killed, end);
        assert_eq!(sheets_in(&dir), (0..20).collect::<Vec<u32>>());

        // Its output had ended before the stop came: the last sheet it wrote
        // may be partly filled.
        let ended = RunResult {
            made_sheets: 3,
            cancelled: true,
            ended_itself: true,
            ..Default::default()
        };
        let end = run_end(&ended, run, plan.pictures);
        passes.after_run(&plan, &mut dir, run, &ended, end);
        let expected: Vec<u32> = (0..20)
            .filter(|n| *n != 2)
            .collect();
        assert_eq!(sheets_in(&dir), expected);
        assert!(!passes.failed_recently(stream_id, Instant::now()));
    }

    fn state(sheets: impl IntoIterator<Item = u32>, end: bool) -> SetState {
        SetState {
            sheets: sheets
                .into_iter()
                .collect(),
            end,
        }
    }

    #[test]
    fn missing_and_next_run_follow_the_order() {
        let run = |start: u32, stop_after: Option<u64>| Some(Run { start, stop_after });
        // 940 pictures: sheets 0 to 37.
        let next = |st: &SetState, viewer: u32| {
            next_run(&missing(st, 38), viewer, true, st.end, 38)
        };
        // A new set with the viewer at sheet 19: from there to the end of
        // the file, then the beginning up to where sheets exist, then done.
        assert_eq!(next(&state([], false), 19), run(19, None));
        assert_eq!(next(&state(19..38, true), 19), run(0, Some(19)));
        assert_eq!(next(&state(0..38, true), 19), None);
        // No run has reached the end: the rest, to the end.
        assert_eq!(next(&state(0..10, false), 0), run(10, None));
        // A gap once the end is reached, from a viewer before or after it;
        // a viewer inside it starts there.
        let gap = state((0..11).chain(20..38), true);
        assert_eq!(next(&gap, 0), run(11, Some(9)));
        assert_eq!(next(&gap, 5), run(11, Some(9)));
        assert_eq!(next(&gap, 30), run(11, Some(9)));
        assert_eq!(next(&gap, 15), run(15, Some(5)));
        // Two gaps and no end: the first stretch only, then on to the end,
        // so nothing on disk is read again.
        assert_eq!(
            next(&state((0..5).chain(10..15), false), 0),
            run(5, Some(5))
        );
        assert_eq!(next(&state(0..15, false), 0), run(15, None));
        // A source that cannot jump is read from its start.
        assert_eq!(
            next_run(&missing(&state([], false), 38), 19, false, false, 38),
            run(0, None)
        );
        // Once the end is reached, every sheet up to the one before the last
        // expected is missing until made, even when the run that reached the
        // end started at the last and made none; the last itself may never
        // come. Before the end, the last expected sheet always is missing.
        assert!(missing(&state(0..37, true), 38).is_empty());
        assert_eq!(
            missing(&state(0..30, true), 38),
            (30..37).collect::<Vec<u32>>()
        );
        assert_eq!(missing(&state([], true), 38), (0..37).collect::<Vec<u32>>());
        assert_eq!(missing(&state(0..38, false), 38), vec![37]);
        // The viewer's sheet, at most the last.
        assert_eq!(sheet_at(800, 5), 3);
        assert_eq!(sheet_at(5_000, 5), 4);
        assert_eq!(sheet_at(0, 38), 0);
    }

    #[test]
    fn sheets_that_are_not_whole_count_as_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp
            .path()
            .join("set");
        write_sheets(&dir, 0..7);
        std::fs::write(dir.join("7.jpg"), b"").unwrap();
        std::fs::write(dir.join("8.jpg"), [0xFF_u8, 0xD8, b'j', b'p', b'g']).unwrap();
        let read = read_set(&dir);
        assert_eq!(read, state(0..7, false));
        assert_eq!(missing(&read, 10), vec![7, 8, 9]);
        std::fs::write(dir.join(END_MARKER), b"").unwrap();
        assert!(read_set(&dir).end);
    }

    #[test]
    fn run_end_judges_each_way_a_run_ends() {
        let run = |start: u32| Run {
            start,
            stop_after: None,
        };
        let ended = |made: u64| RunResult {
            exit_ok: true,
            made_sheets: made,
            ..Default::default()
        };
        // 940 pictures: sheets 0 to 37.
        let goal = RunResult {
            reached_goal: true,
            made_sheets: 5,
            ..Default::default()
        };
        assert_eq!(run_end(&goal, run(3), 940), RunEnd::Goal);
        let stopped = RunResult {
            cancelled: true,
            made_sheets: 5,
            ..Default::default()
        };
        assert_eq!(run_end(&stopped, run(3), 940), RunEnd::Cancelled);
        assert_eq!(run_end(&ended(38), run(0), 940), RunEnd::End);
        assert_eq!(run_end(&ended(19), run(19), 940), RunEnd::End);
        // Up to the sheet before the last: a last sheet may never come.
        assert_eq!(run_end(&ended(18), run(19), 940), RunEnd::End);
        // The sheet before the last or the last itself from a source ffmpeg
        // gave up on: a source cut off there (a partly filled sheet flushed),
        // not the end of the file.
        for sign in GAVE_UP_ERRORS {
            let cut = |made: u64| RunResult {
                stderr: format!("[in#0 @ 0x7] {sign}\n"),
                ..ended(made)
            };
            assert_eq!(run_end(&cut(18), run(19), 940), RunEnd::Failed, "{sign}");
            assert_eq!(run_end(&cut(37), run(0), 940), RunEnd::Failed, "{sign}");
            assert_eq!(run_end(&cut(19), run(19), 940), RunEnd::Failed, "{sign}");
            assert_eq!(run_end(&cut(38), run(0), 940), RunEnd::Failed, "{sign}");
        }
        // The end-to-end cut: exit 0 with what ffmpeg wrote when it gave up.
        let dropped = |made: u64| RunResult {
            stderr: SOURCE_DROPPED.to_string(),
            ..ended(made)
        };
        assert_eq!(run_end(&dropped(37), run(0), 940), RunEnd::Failed);
        assert_eq!(run_end(&dropped(38), run(0), 940), RunEnd::Failed);
        let cut_off = |made: u64| RunResult {
            stderr: SOURCE_CUT.to_string(),
            ..ended(made)
        };
        assert_eq!(run_end(&cut_off(37), run(0), 940), RunEnd::Failed);
        assert_eq!(run_end(&cut_off(38), run(0), 940), RunEnd::Failed);
        // A drop ffmpeg's reconnect fixed leaves only this line (its "Will
        // reconnect" is a hidden warning): the end of the file all the same.
        let recovered = |made: u64| RunResult {
            stderr:
                "[http @ 0x6] Stream ends prematurely at 52428800, should be 70027876\n"
                    .to_string(),
            ..ended(made)
        };
        assert_eq!(run_end(&recovered(18), run(19), 940), RunEnd::End);
        assert_eq!(run_end(&recovered(38), run(0), 940), RunEnd::End);
        // A damaged end of the file itself is no give-up either.
        let damaged = RunResult {
            stderr: "[in#0 @ 0x7] Error during demuxing: Invalid data found when processing input\n"
                .to_string(),
            ..ended(37)
        };
        assert_eq!(run_end(&damaged, run(0), 940), RunEnd::End);
        // A source that closed a request at the file's very end is no failure.
        let closed = RunResult {
            stderr: PAST_END_HTTP.to_string(),
            ..ended(18)
        };
        assert_eq!(run_end(&closed, run(19), 940), RunEnd::End);
        // Short of that by itself: a source cut off part way also exits 0.
        assert_eq!(run_end(&ended(17), run(19), 940), RunEnd::Failed);
        assert_eq!(run_end(&ended(10), run(0), 940), RunEnd::Failed);
        // From sheet 36 with no sheet the file is over before it: not its end.
        assert_eq!(run_end(&ended(0), run(36), 940), RunEnd::Nothing);
        // From the last sheet with no sheet: the file ended at a sheet's edge.
        assert_eq!(run_end(&ended(0), run(37), 940), RunEnd::End);
        // Part way, ended with an error.
        let error = RunResult {
            made_sheets: 2,
            ..Default::default()
        };
        assert_eq!(run_end(&error, run(10), 940), RunEnd::Failed);
        assert_eq!(run_end(&RunResult::default(), run(10), 940), RunEnd::Failed);
        // Part way, no sheet within the wait.
        let no_jump = RunResult {
            no_first_sheet: true,
            ..Default::default()
        };
        assert_eq!(run_end(&no_jump, run(10), 940), RunEnd::NoJump);
        // From the start, nothing at all.
        assert_eq!(run_end(&ended(0), run(0), 940), RunEnd::Failed);
    }

    #[test]
    fn a_run_past_the_end_is_told_from_a_failing_source() {
        let run = |start: u32| Run {
            start,
            stop_after: None,
        };
        let exited = |code: i32, stderr: String| RunResult {
            exit_ok: code == 0,
            exit_code: Some(code),
            stderr,
            ..Default::default()
        };
        let past_end = exited(234, PAST_END.to_string());
        let past_end_http = exited(234, format!("{PAST_END_HTTP}{PAST_END}"));
        // 940 pictures: sheets 0 to 37. ffmpeg 7 started past the end: 234
        // (AVERROR(EINVAL) on Unix), -22 on Windows, no sheet.
        assert_eq!(run_end(&past_end, run(36), 940), RunEnd::Nothing);
        assert_eq!(run_end(&past_end_http, run(36), 940), RunEnd::Nothing);
        assert_eq!(
            run_end(&exited(-22, PAST_END.to_string()), run(36), 940),
            RunEnd::Nothing
        );
        // From the last sheet: the file ended at a sheet's edge.
        assert_eq!(run_end(&past_end, run(37), 940), RunEnd::End);
        assert_eq!(run_end(&past_end_http, run(37), 940), RunEnd::End);
        // From the start, nothing at all is a failure whatever the status.
        assert_eq!(run_end(&past_end, run(0), 940), RunEnd::Failed);
        // An encoder that opened but muxed nothing: only the muxer's line.
        assert_eq!(
            run_end(
                &exited(
                    234,
                    "[out#0/image2 @ 0x5] Nothing was written into output file, because \
                     at least one of its streams received no packets.\n"
                        .to_string()
                ),
                run(36),
                940
            ),
            RunEnd::Nothing
        );

        // The same status after the source dropped, or with any other sign
        // of a failing source: a failure, never the end of the file.
        let dropped = exited(234, format!("{SOURCE_DROPPED}{PAST_END}"));
        assert_eq!(run_end(&dropped, run(36), 940), RunEnd::Failed);
        assert_eq!(run_end(&dropped, run(37), 940), RunEnd::Failed);
        for sign in SOURCE_ERRORS {
            let failed = exited(234, format!("[in#0 @ 0x7] {sign}\n{PAST_END}"));
            assert_eq!(run_end(&failed, run(36), 940), RunEnd::Failed, "{sign}");
            assert_eq!(run_end(&failed, run(37), 940), RunEnd::Failed, "{sign}");
        }
        // Exit 0 with no sheet and a source error: a failure too.
        let quiet = exited(
            0,
            "[in#0 @ 0x7] Error during demuxing: Input/output error\n".to_string(),
        );
        assert_eq!(run_end(&quiet, run(36), 940), RunEnd::Failed);
        // 234 without the encoder's line, or another status: a failure.
        assert_eq!(
            run_end(&exited(234, String::new()), run(36), 940),
            RunEnd::Failed
        );
        let refused = exited(
            8,
            "[http @ 0x6] HTTP error 404 Not Found\n\
             [in#0 @ 0x7] Error opening input: Server returned 404 Not Found\n"
                .to_string(),
        );
        assert_eq!(run_end(&refused, run(36), 940), RunEnd::Failed);
        assert_eq!(run_end(&refused, run(37), 940), RunEnd::Failed);
        // A run with sheets is judged by them, whatever the status.
        let some = RunResult {
            made_sheets: 1,
            ..past_end.clone()
        };
        assert_eq!(run_end(&some, run(36), 940), RunEnd::Failed);
    }

    #[test]
    fn the_first_sheet_wait_grows_with_the_file() {
        const GB: u64 = 1024 * 1024 * 1024;
        assert_eq!(first_sheet_wait(None, 38, SEEK_WAIT), SEEK_WAIT);
        assert_eq!(first_sheet_wait(Some(13 * GB), 38, SEEK_WAIT), SEEK_WAIT);
        // 40 GB in 38 sheets is about 1.1 GB a sheet: about 216 s at 5 MB/s.
        let wait = first_sheet_wait(Some(40 * GB), 38, SEEK_WAIT);
        assert!(
            wait > Duration::from_secs(200) && wait < Duration::from_secs(240),
            "{wait:?}"
        );
    }

    #[test]
    fn a_pass_follows_only_to_a_part_with_no_pictures_a_few_times() {
        let limits = Limits::default();
        let old = FOLLOW_EVERY;
        // The viewer moved to sheet 19 while the run is about to make sheet 2.
        assert!(follows_to(19, false, 2, old, 0, true, &limits));
        assert!(
            !follows_to(19, true, 2, old, 0, true, &limits),
            "it has its sheet"
        );
        assert!(!follows_to(2, false, 2, old, 0, true, &limits), "made next");
        assert!(
            !follows_to(3, false, 2, old, 0, true, &limits),
            "made next but one"
        );
        let young = old - Duration::from_secs(1);
        assert!(
            !follows_to(19, false, 2, young, 0, true, &limits),
            "too soon"
        );
        assert!(
            follows_to(19, false, 2, old, 3, true, &limits),
            "a fourth move"
        );
        assert!(
            !follows_to(19, false, 2, old, 4, true, &limits),
            "a fifth move"
        );
        assert!(
            !follows_to(19, false, 2, old, 0, false, &limits),
            "cannot jump"
        );
        // Back to a part not made yet.
        assert!(follows_to(0, false, 25, old, 0, true, &limits));
    }

    #[tokio::test]
    async fn a_play_resumed_part_way_ends_with_the_beginning_made() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            ..Default::default()
        });
        // 1 h 20 into a film of 940 pictures: sheet 19 of 38.
        reports.at("device-a", 80 * 60);
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin(
                "play-a",
                PassPlan {
                    pictures: 940,
                    ..plan(tmp.path(), set_id)
                },
                runner.clone(),
                reports.watch(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| !passes.is_queued(set_id)).await;
        let dir = set_dir(tmp.path(), set_id);
        assert_eq!(
            runner.starts(),
            vec![19, 0],
            "to the end, then the beginning"
        );
        assert_eq!(sheets_in(&dir), (0..38).collect::<Vec<u32>>());
        assert!(
            dir.join(END_MARKER)
                .exists()
        );
        assert!(
            dir.join(DONE_MARKER)
                .exists()
        );
        assert!(
            !dir.join(BUSY_MARKER)
                .exists()
        );
    }

    #[tokio::test]
    async fn a_pass_stopped_after_the_end_leaves_the_beginning_to_the_next() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        let set_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), set_id);
        let plan = PassPlan {
            pictures: 940,
            ..plan(tmp.path(), set_id)
        };
        // Its second run, from the beginning, stalls at sheet 10.
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            stall_at: Some(10),
            ..Default::default()
        });
        reports.at("device-a", 80 * 60);
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin("play-a", plan.clone(), runner.clone(), reports.watch())
            .await;
        until(|| {
            runner
                .runs
                .load(Ordering::SeqCst)
                == 2
                && dir
                    .join("9.jpg")
                    .exists()
        })
        .await;
        passes.release("play-a");
        until(|| {
            runner
                .killed
                .load(Ordering::SeqCst)
        })
        .await;
        until(|| !passes.has_pass(set_id)).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let kept: Vec<u32> = (0..10)
            .chain(19..38)
            .collect();
        assert_eq!(sheets_in(&dir), kept);
        assert!(
            dir.join(END_MARKER)
                .exists()
        );
        assert!(
            !dir.join(DONE_MARKER)
                .exists(),
            "not done before the beginning is made"
        );

        // The next pass makes only what is missing, and finishes the set.
        let rest = Arc::new(FakeRunner {
            sheets: 38,
            ..Default::default()
        });
        passes.register("play-b", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin("play-b", plan, rest.clone(), reports.watch())
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(rest.starts(), vec![10]);
        assert_eq!(sheets_in(&dir), (0..38).collect::<Vec<u32>>());
        assert!(
            dir.join(DONE_MARKER)
                .exists()
        );
    }

    #[tokio::test]
    async fn a_source_that_cannot_jump_is_read_from_its_start() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        let set_id = Uuid::new_v4();
        let (stream_a, stream_b) = (Uuid::new_v4(), Uuid::new_v4());
        let dir = set_dir(tmp.path(), set_id);
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            no_jump: true,
            ..Default::default()
        });
        reports.at("device-a", 80 * 60);
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                PassPlan {
                    pictures: 940,
                    ..set_plan(tmp.path(), stream_a, set_id)
                },
                runner.clone(),
                reports.watch(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.starts(), vec![19, 0]);
        assert_eq!(sheets_in(&dir), (0..38).collect::<Vec<u32>>());
        assert!(
            dir.join(DONE_MARKER)
                .exists()
        );
        assert!(!passes.can_jump(stream_a));
        assert!(
            !passes.failed_recently(stream_a, Instant::now()),
            "not a failure"
        );

        // Another version of the same set, from another source, still starts
        // part way.
        assert!(passes.can_jump(stream_b));
        for name in [DONE_MARKER, END_MARKER] {
            std::fs::remove_file(dir.join(name)).unwrap();
        }
        for n in 19..38 {
            remove_sheet(&dir, n);
        }
        let jumps = Arc::new(FakeRunner {
            sheets: 38,
            ..Default::default()
        });
        reports.at("device-b", 80 * 60);
        passes.register("play-b", Uuid::new_v4(), "device-b");
        passes
            .begin(
                "play-b",
                PassPlan {
                    pictures: 940,
                    ..set_plan(tmp.path(), stream_b, set_id)
                },
                jumps.clone(),
                reports.watch(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(jumps.starts(), vec![19]);
        assert!(
            dir.join(DONE_MARKER)
                .exists()
        );
    }

    #[tokio::test]
    async fn a_pass_follows_the_viewer_to_a_part_with_no_pictures() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        let set_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), set_id);
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            pace: Duration::from_millis(15),
            ..Default::default()
        });
        reports.at("device-a", 0);
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                PassPlan {
                    pictures: 940,
                    ..plan(tmp.path(), set_id)
                },
                runner.clone(),
                reports.watch(),
            )
            .await;
        until(|| {
            dir.join("1.jpg")
                .exists()
        })
        .await;
        // The viewer jumps to 1 h 20.
        reports.at("device-a", 80 * 60);
        until(|| !passes.is_queued(set_id)).await;
        let starts = runner.starts();
        assert_eq!(starts.len(), 3, "{starts:?}");
        assert_eq!(starts[..2], [0_u32, 19], "{starts:?}");
        assert_eq!(sheets_in(&dir), (0..38).collect::<Vec<u32>>());
        assert!(
            dir.join(DONE_MARKER)
                .exists()
        );
    }

    #[tokio::test]
    async fn a_file_longer_than_its_count_moves_to_its_own_folder() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());

        // 940 pictures fill sheets 0 to 37; this file gave sheet 38 too.
        let (set_id, stream_id) = (Uuid::new_v4(), Uuid::new_v4());
        let runner = Arc::new(FakeRunner {
            sheets: 39,
            ..Default::default()
        });
        let item_a = Uuid::new_v4();
        passes.register("play-a", item_a, "device-a");
        passes
            .begin(
                "play-a",
                PassPlan {
                    pictures: 940,
                    ..set_plan(tmp.path(), stream_id, set_id)
                },
                runner.clone(),
                always(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert!(!set_dir(tmp.path(), set_id).exists());
        let own = set_dir(tmp.path(), stream_id);
        assert!(
            own.join(DONE_MARKER)
                .exists()
        );
        assert!(
            own.join("38.jpg")
                .exists()
        );
        // The play no longer names the moved set, so the sheet route
        // follows its pick to the stream's own folder.
        assert_eq!(passes.selected_set(item_a, "device-a"), None);

        // Up to the count's last sheet, the pictures stay in the set.
        let (set_id, stream_id) = (Uuid::new_v4(), Uuid::new_v4());
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            ..Default::default()
        });
        let item_b = Uuid::new_v4();
        passes.register("play-b", item_b, "device-b");
        passes
            .begin(
                "play-b",
                PassPlan {
                    pictures: 940,
                    ..set_plan(tmp.path(), stream_id, set_id)
                },
                runner.clone(),
                always(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert!(
            set_dir(tmp.path(), set_id)
                .join(DONE_MARKER)
                .exists()
        );
        assert!(!set_dir(tmp.path(), stream_id).exists());
        assert_eq!(passes.selected_set(item_b, "device-b"), Some(set_id));
    }

    #[tokio::test]
    async fn a_moved_set_stays_in_its_own_folder_and_makes_its_beginning_again() {
        let tmp = tempfile::tempdir().unwrap();
        let quiet_alone = Duration::from_millis(400);
        let passes = TrickplayPasses::with_limits(Limits {
            quiet_alone,
            ..fast()
        });
        let reports = Reports::default();
        let (set_id, stream_id) = (Uuid::new_v4(), Uuid::new_v4());
        // Another version of the same count made sheets 0 to 18.
        const OTHER: &[u8] = &[0xFF, 0xD8, b'o', 0xFF, 0xD9];
        let set = set_dir(tmp.path(), set_id);
        std::fs::create_dir_all(&set).unwrap();
        for n in 0..19 {
            std::fs::write(set.join(format!("{n}.jpg")), OTHER).unwrap();
        }
        // This file gives sheet 38 too, past the count's last. Its run from
        // the beginning stalls at sheet 5, once.
        let runner = Arc::new(FakeRunner {
            sheets: 39,
            stall_at: Some(5),
            ..Default::default()
        });
        reports.at("device-a", 80 * 60);
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                PassPlan {
                    pictures: 940,
                    ..set_plan(tmp.path(), stream_id, set_id)
                },
                runner.clone(),
                reports.watch(),
            )
            .await;
        let own = set_dir(tmp.path(), stream_id);
        until_named("run 2 made sheet 4 in the stream's own folder", || {
            runner
                .runs
                .load(Ordering::SeqCst)
                == 2
                && own
                    .join("4.jpg")
                    .exists()
        })
        .await;
        assert!(!set.exists());

        // Its play goes quiet: the pass stands down.
        reports.silent("device-a", Duration::ZERO);
        until_named("the stalled run 2 stopped by standing down", || {
            runner
                .killed
                .load(Ordering::SeqCst)
        })
        .await;
        assert!(passes.is_queued(set_id));

        // The play reports again: the pass carries on in the stream's own
        // folder from its first missing sheet, keeping the sheets 0 to 4 it
        // made there, and the count's folder is not made again.
        reports.at("device-a", 80 * 60);
        until_named("the pass ended after the play reported again", || {
            !passes.is_queued(set_id)
        })
        .await;
        assert_eq!(runner.starts(), vec![19, 0, 5]);
        assert!(!set.exists(), "the count's folder is not made again");
        assert!(
            own.join(DONE_MARKER)
                .exists()
        );
        assert_eq!(sheets_in(&own), (0..39).collect::<Vec<u32>>());
        for n in 0..19 {
            assert_eq!(
                std::fs::read(own.join(format!("{n}.jpg"))).unwrap(),
                JPEG,
                "sheet {n} is made from this file"
            );
        }
    }

    #[tokio::test]
    async fn a_run_from_the_last_sheet_that_makes_none_leaves_the_rest_to_make() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        let set_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), set_id);
        write_sheets(&dir, 0..11);
        // 940 pictures fill sheets 0 to 37; this file ends with sheet 36.
        // The run from sheet 37 ends as ffmpeg 7 does there: 234, no sheet.
        let runner = Arc::new(FakeRunner {
            sheets: 37,
            empty_exit: Some((234, PAST_END)),
            ..Default::default()
        });
        // The viewer is in the last sheet.
        reports.at("device-a", 37 * 250);
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                PassPlan {
                    pictures: 940,
                    ..plan(tmp.path(), set_id)
                },
                runner.clone(),
                reports.watch(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(
            runner.starts(),
            vec![37, 11],
            "the end, then the sheets before it"
        );
        assert_eq!(sheets_in(&dir), (0..37).collect::<Vec<u32>>());
        assert!(
            dir.join(DONE_MARKER)
                .exists()
        );
        assert!(
            !passes.failed_recently(set_id, Instant::now()),
            "a run past the end is not a failure"
        );
    }

    #[tokio::test]
    async fn a_run_does_not_follow_to_a_last_sheet_past_the_end() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        let set_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), set_id);
        write_sheets(&dir, 0..11);
        // As above, but the run from sheet 11 takes longer than a follow
        // wait, and the viewer is reported in sheet 37 all the while:
        // that sheet never comes, so the run is not stopped to make it.
        let runner = Arc::new(FakeRunner {
            sheets: 37,
            empty_exit: Some((234, PAST_END)),
            pace: Duration::from_millis(20),
            ..Default::default()
        });
        reports.at("device-a", 37 * 250);
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                PassPlan {
                    pictures: 940,
                    ..plan(tmp.path(), set_id)
                },
                runner.clone(),
                reports.watch(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.starts(), vec![37, 11], "no follow to sheet 37");
        assert_eq!(sheets_in(&dir), (0..37).collect::<Vec<u32>>());
        assert!(
            dir.join(DONE_MARKER)
                .exists()
        );
    }

    #[tokio::test]
    async fn a_run_started_past_the_end_is_not_a_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        let set_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), set_id);
        write_sheets(&dir, 0..36);
        // 940 pictures fill sheets 0 to 37; this file ends with sheet 35, so
        // a run from 36 finds it over: 234 and no sheet, from ffmpeg 7.
        let runner = Arc::new(FakeRunner {
            sheets: 36,
            empty_exit: Some((234, PAST_END)),
            ..Default::default()
        });
        reports.at("device-a", 36 * 250);
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                PassPlan {
                    pictures: 940,
                    ..plan(tmp.path(), set_id)
                },
                runner.clone(),
                reports.watch(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(
            runner.starts(),
            vec![36, 36],
            "over twice: the stored length is longer than the file"
        );
        assert_eq!(sheets_in(&dir), (0..36).collect::<Vec<u32>>());
        assert!(
            !passes.failed_recently(set_id, Instant::now()),
            "not held back for the failure wait"
        );
        assert!(
            !dir.join(END_MARKER)
                .exists()
        );
        assert!(
            !dir.join(DONE_MARKER)
                .exists()
        );
        assert!(
            !dir.join(BUSY_MARKER)
                .exists()
        );
    }

    #[tokio::test]
    async fn a_run_whose_source_drops_after_the_seek_is_a_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        let set_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), set_id);
        write_sheets(&dir, 0..11);
        // The same status as a run past the end, after a source error.
        const DROPPED: &str = "\
            [in#0/matroska,webm @ 0x7] Error during demuxing: Input/output error\n\
            [vost#0:0/mjpeg @ 0x2] [enc:mjpeg @ 0x3] Could not open encoder before EOF\n";
        let runner = Arc::new(FakeRunner {
            sheets: 0,
            empty_exit: Some((234, DROPPED)),
            ..Default::default()
        });
        reports.at("device-a", 37 * 250);
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                PassPlan {
                    pictures: 940,
                    ..plan(tmp.path(), set_id)
                },
                runner.clone(),
                reports.watch(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.starts(), vec![37], "one run, then the failure wait");
        assert!(passes.failed_recently(set_id, Instant::now()));
        assert_eq!(sheets_in(&dir), (0..11).collect::<Vec<u32>>());
        assert!(
            !dir.join(END_MARKER)
                .exists(),
            "no sheet is marked as never coming"
        );
    }

    #[tokio::test]
    async fn the_recorded_set_goes_with_its_play() {
        let passes = TrickplayPasses::with_limits(fast());
        let item_id = Uuid::new_v4();
        let (long_set, short_set) = (Uuid::new_v4(), Uuid::new_v4());
        passes.register("play-a", item_id, "device-a");
        assert!(passes.attach("play-a", long_set));
        assert_eq!(passes.selected_set(item_id, "device-a"), Some(long_set));
        passes.release("play-a");
        assert_eq!(
            passes.selected_set(item_id, "device-a"),
            None,
            "forgotten with the play, not after the grace period"
        );

        // A new play forgets the last play's set until its own is known.
        passes.register("play-b", item_id, "device-a");
        assert!(passes.attach("play-b", long_set));
        passes.register("play-c", item_id, "device-a");
        assert_eq!(passes.selected_set(item_id, "device-a"), None);
        assert!(passes.attach("play-c", short_set));
        // The older play's stop leaves the newer play's set alone.
        passes.release("play-b");
        assert_eq!(passes.selected_set(item_id, "device-a"), Some(short_set));

        // So does an older play's stop on the same set.
        passes.register("play-d", item_id, "device-a");
        assert!(passes.attach("play-d", short_set));
        passes.release("play-c");
        assert_eq!(passes.selected_set(item_id, "device-a"), Some(short_set));
        passes.release("play-d");
        assert_eq!(passes.selected_set(item_id, "device-a"), None);
    }

    #[tokio::test]
    async fn open_for_needs_the_devices_own_play_past_the_start_delay() {
        let passes = TrickplayPasses::new();
        let set_id = Uuid::new_v4();
        assert!(!passes.open_for(set_id, "device-a"), "no play");
        passes.register("play-a", Uuid::new_v4(), "device-a");
        assert!(passes.attach("play-a", set_id));
        assert!(
            !passes.open_for(set_id, "device-a"),
            "younger than the start delay"
        );
        passes.backdate("play-a", START_DELAY);
        assert!(passes.open_for(set_id, "device-a"));
        assert!(!passes.open_for(set_id, "device-b"), "another device");
        assert!(!passes.open_for(Uuid::new_v4(), "device-a"), "another set");

        // The same play session id reused for another item starts over.
        passes.register("play-a", Uuid::new_v4(), "device-a");
        assert!(!passes.open_for(set_id, "device-a"), "left the old set");
        let other_set = Uuid::new_v4();
        assert!(passes.attach("play-a", other_set));
        assert!(
            !passes.open_for(other_set, "device-a"),
            "younger than the start delay again"
        );
    }

    #[tokio::test]
    async fn an_unwatched_pass_gives_way_to_a_watched_one() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        let (set_a, set_b) = (Uuid::new_v4(), Uuid::new_v4());
        let slow = Arc::new(FakeRunner {
            sheets: 2,
            wait_for_cancel: true,
            ..Default::default()
        });
        let quick = Arc::new(FakeRunner {
            sheets: 29,
            ..Default::default()
        });
        reports.fresh("device-a");
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin(
                "play-a",
                plan(tmp.path(), set_a),
                slow.clone(),
                reports.watch(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| {
            slow.runs
                .load(Ordering::SeqCst)
                == 1
        })
        .await;

        // A goes silent. With nobody waiting it keeps the slot.
        reports.silent("device-a", Duration::from_secs(1));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !slow
                .killed
                .load(Ordering::SeqCst)
        );

        // B is watched and waiting: A gives way and B runs.
        reports.fresh("device-b");
        passes.register("play-b", Uuid::new_v4(), "device-b");
        let begun = passes
            .begin(
                "play-b",
                plan(tmp.path(), set_b),
                quick.clone(),
                reports.watch(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| {
            slow.killed
                .load(Ordering::SeqCst)
        })
        .await;
        until(|| {
            set_dir(tmp.path(), set_b)
                .join(DONE_MARKER)
                .exists()
        })
        .await;
        // A stood down: its job and its sheets are still there.
        assert!(passes.is_queued(set_a));
        assert_eq!(sheets_in(&set_dir(tmp.path(), set_a)), vec![0, 1]);
        assert_eq!(
            slow.runs
                .load(Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn an_unwatched_waiter_does_not_take_the_slot() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        let (set_a, set_b) = (Uuid::new_v4(), Uuid::new_v4());
        let slow = Arc::new(FakeRunner {
            sheets: 2,
            wait_for_cancel: true,
            ..Default::default()
        });
        let quick = Arc::new(FakeRunner {
            sheets: 29,
            ..Default::default()
        });
        reports.fresh("device-a");
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                plan(tmp.path(), set_a),
                slow.clone(),
                reports.watch(),
            )
            .await;
        until(|| {
            slow.runs
                .load(Ordering::SeqCst)
                == 1
        })
        .await;

        // Both silent: B's device still has a session on the item, so B is
        // queued, but it does not wait for the slot and A keeps it.
        reports.silent("device-a", Duration::from_secs(1));
        reports.silent("device-b", Duration::from_secs(1));
        passes.register("play-b", Uuid::new_v4(), "device-b");
        let begun = passes
            .begin(
                "play-b",
                plan(tmp.path(), set_b),
                quick.clone(),
                reports.watch(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !slow
                .killed
                .load(Ordering::SeqCst)
        );
        assert!(!passes.is_waiting(set_b));
        assert_eq!(
            quick
                .runs
                .load(Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn alone_a_pass_stands_down_late_and_comes_back_when_watched() {
        let tmp = tempfile::tempdir().unwrap();
        let quiet_alone = Duration::from_millis(400);
        let passes = TrickplayPasses::with_limits(Limits {
            quiet_alone,
            ..fast()
        });
        let reports = Reports::default();
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 3,
            wait_for_cancel: true,
            ..Default::default()
        });
        reports.fresh("device-a");
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                plan(tmp.path(), set_id),
                runner.clone(),
                reports.watch(),
            )
            .await;
        until(|| {
            runner
                .runs
                .load(Ordering::SeqCst)
                == 1
        })
        .await;

        let silent_from = Instant::now();
        reports.silent("device-a", Duration::ZERO);
        // Past the one-minute rule, but nobody is waiting.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !runner
                .killed
                .load(Ordering::SeqCst)
        );
        until(|| {
            runner
                .killed
                .load(Ordering::SeqCst)
        })
        .await;
        assert!(silent_from.elapsed() >= quiet_alone);
        assert_eq!(sheets_in(&set_dir(tmp.path(), set_id)), vec![0, 1, 2]);
        assert!(passes.is_queued(set_id));

        // Its play reports again: the same job runs again, with no new begin.
        reports.fresh("device-a");
        until(|| {
            runner
                .runs
                .load(Ordering::SeqCst)
                == 2
        })
        .await;
    }

    #[tokio::test]
    async fn a_cancel_ends_a_stood_down_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(Limits {
            quiet_alone: Duration::from_millis(150),
            ..fast()
        });
        let reports = Reports::default();
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 1,
            wait_for_cancel: true,
            ..Default::default()
        });
        reports.fresh("device-a");
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                plan(tmp.path(), set_id),
                runner.clone(),
                reports.watch(),
            )
            .await;
        until(|| {
            runner
                .runs
                .load(Ordering::SeqCst)
                == 1
        })
        .await;
        reports.silent("device-a", Duration::ZERO);
        until(|| {
            runner
                .killed
                .load(Ordering::SeqCst)
        })
        .await;

        // The play stops: after the grace period the stood-down job ends, so
        // a report from the device no longer brings it back.
        passes.release("play-a");
        until(|| !passes.has_pass(set_id)).await;
        reports.fresh("device-a");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            runner
                .runs
                .load(Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn a_play_whose_device_moved_to_another_set_gives_way() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        let item_id = Uuid::new_v4();
        let (set_a, set_b) = (Uuid::new_v4(), Uuid::new_v4());
        let slow = Arc::new(FakeRunner {
            sheets: 2,
            wait_for_cancel: true,
            ..Default::default()
        });
        let quick = Arc::new(FakeRunner {
            sheets: 29,
            ..Default::default()
        });
        reports.playing("device-a", "play-a");
        passes.register("play-a", item_id, "device-a");
        passes
            .begin(
                "play-a",
                plan(tmp.path(), set_a),
                slow.clone(),
                reports.watch(),
            )
            .await;
        until(|| {
            slow.runs
                .load(Ordering::SeqCst)
                == 1
        })
        .await;

        // The device plays a version of another length of the same title;
        // play-a's stop never came. The device's reports are play-b's.
        reports.playing("device-a", "play-b");
        passes.register("play-b", item_id, "device-a");
        let begun = passes
            .begin(
                "play-b",
                plan(tmp.path(), set_b),
                quick.clone(),
                reports.watch(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| {
            slow.killed
                .load(Ordering::SeqCst)
        })
        .await;
        until(|| {
            set_dir(tmp.path(), set_b)
                .join(DONE_MARKER)
                .exists()
        })
        .await;
        assert_eq!(sheets_in(&set_dir(tmp.path(), set_a)), vec![0, 1]);
    }

    #[tokio::test]
    async fn a_waiter_with_no_plays_left_does_not_take_the_slot() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(Limits {
            grace: Duration::from_secs(5),
            ..fast()
        });
        let reports = Reports::default();
        let (set_a, set_b) = (Uuid::new_v4(), Uuid::new_v4());
        let slow = Arc::new(FakeRunner {
            sheets: 2,
            wait_for_cancel: true,
            ..Default::default()
        });
        let quick = Arc::new(FakeRunner {
            sheets: 29,
            ..Default::default()
        });
        reports.fresh("device-a");
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                plan(tmp.path(), set_a),
                slow.clone(),
                reports.watch(),
            )
            .await;
        until(|| {
            slow.runs
                .load(Ordering::SeqCst)
                == 1
        })
        .await;
        reports.fresh("device-b");
        passes.register("play-b", Uuid::new_v4(), "device-b");
        let begun = passes
            .begin(
                "play-b",
                plan(tmp.path(), set_b),
                quick.clone(),
                reports.watch(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| passes.is_waiting(set_b)).await;

        // B's only play stops while it waits: it stops waiting, so A, quiet
        // for a minute, keeps the slot through B's grace period.
        passes.release("play-b");
        until(|| !passes.is_waiting(set_b)).await;
        reports.silent("device-a", Duration::from_secs(1));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !slow
                .killed
                .load(Ordering::SeqCst)
        );
        assert_eq!(
            quick
                .runs
                .load(Ordering::SeqCst),
            0
        );
    }

    #[cfg(unix)]
    #[test]
    fn prune_with_no_keep_time_goes_by_size_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp
            .path()
            .join(ROOT_DIR);
        let now = SystemTime::now();
        let old = set_dir(&root, Uuid::new_v4());
        let recent = set_dir(&root, Uuid::new_v4());
        for dir in [&old, &recent] {
            std::fs::create_dir_all(dir).unwrap();
            std::fs::write(dir.join("0.jpg"), vec![0u8; 600]).unwrap();
            std::fs::write(dir.join(DONE_MARKER), b"").unwrap();
        }
        let set_age = |path: &Path, age: Duration| {
            std::fs::File::open(path)
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(now - age))
                .unwrap();
        };
        let day = Duration::from_secs(24 * 60 * 60);
        set_age(&old, 400 * day);
        set_age(&recent, day);

        // Under the size limit: a set not shown for 400 days stays.
        prune(&root, u64::MAX, None, &HashSet::new(), now);
        assert!(old.exists());
        assert!(recent.exists());
        prune_on_start(&root, true, u64::MAX, None, now);
        assert!(old.exists(), "a restart keeps it too");

        // Over it: the one shown longest ago goes first.
        prune(&root, 1_000, None, &HashSet::new(), now);
        assert!(!old.exists());
        assert!(recent.exists());
    }

    const GIB: u64 = 1024 * 1024 * 1024;

    /// As [`until`], for up to `limit`.
    async fn until_within(limit: Duration, mut check: impl FnMut() -> bool) {
        let began = Instant::now();
        while !check() {
            assert!(
                began.elapsed() < limit,
                "condition not reached in {limit:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// A pass over a 5 GB file of `pictures` pictures into a set of its own,
    /// with rough pictures first.
    fn rough_plan(root: &Path, set_id: Uuid, pictures: u64) -> PassPlan {
        PassPlan {
            pictures,
            size: Some(5 * GIB),
            rough: true,
            ..plan(root, set_id)
        }
    }

    /// Registers `play` on device-a, begins `plan` and waits for its pass to
    /// be over.
    async fn play_through<W>(
        passes: &TrickplayPasses,
        play: &str,
        plan: PassPlan,
        runner: Arc<FakeRunner>,
        watch: W,
        limit: Duration,
    ) where
        W: Fn(&str, Uuid) -> Option<Watched> + Send + Sync + 'static,
    {
        let set_id = plan.set_id;
        passes.register(play, Uuid::new_v4(), "device-a");
        let begun = passes
            .begin(play, plan, runner, watch)
            .await;
        assert_eq!(begun, Begin::Queued);
        until_within(limit, || !passes.is_queued(set_id)).await;
    }

    #[test]
    fn rough_order_starts_at_both_ends_then_halves_each_gap() {
        assert_eq!(
            rough_order(940, ROUGH_PICTURES),
            vec![
                0, 939, 469, 234, 704, 117, 351, 586, 821, 58, 175, 292, 410, 527, 645,
                762, 880
            ]
        );
        assert_eq!(rough_order(120, ROUGH_PICTURES)[..5], [0, 119, 59, 29, 89]);
        assert_eq!(
            rough_order(720, ROUGH_PICTURES)[..5],
            [0, 719, 359, 179, 539]
        );
        assert_eq!(rough_order(940, 2), vec![0, 939]);
        assert_eq!(rough_order(3, ROUGH_PICTURES), vec![0, 2, 1]);
        assert_eq!(rough_order(2, ROUGH_PICTURES), vec![0, 1]);
        assert_eq!(rough_order(1, ROUGH_PICTURES), vec![0]);
        assert!(rough_order(0, ROUGH_PICTURES).is_empty());
        for count in 2..=40_u64 {
            let order = rough_order(count, ROUGH_PICTURES);
            let unique: BTreeSet<u64> = order
                .iter()
                .copied()
                .collect();
            assert_eq!(unique.len(), order.len(), "{count}: {order:?}");
            assert!(
                order
                    .iter()
                    .all(|g| *g < count),
                "{count}"
            );
            assert_eq!(
                order.len(),
                usize::try_from(count)
                    .unwrap()
                    .min(ROUGH_PICTURES),
                "{count}"
            );
        }
    }

    #[test]
    fn nearest_takes_the_closest_picture_and_the_earlier_on_a_tie() {
        assert_eq!(nearest(&[0, 939], 469), Some(0));
        assert_eq!(nearest(&[0, 939], 470), Some(939));
        assert_eq!(nearest(&[0, 939], 939), Some(939));
        assert_eq!(nearest(&[0, 469, 939], 234), Some(0));
        assert_eq!(nearest(&[0, 468], 234), Some(0), "a tie");
        assert_eq!(nearest(&[], 5), None);
    }

    #[test]
    fn a_rough_stage_only_for_a_large_file_with_much_to_make_that_can_jump() {
        assert!(rough_stage_wanted(true, Some(4 * GIB), 100, 4));
        assert!(!rough_stage_wanted(true, Some(4 * GIB - 1), 940, 38));
        assert!(
            rough_stage_wanted(true, None, 360, 15),
            "60 minutes, size unknown"
        );
        assert!(!rough_stage_wanted(true, None, 359, 15));
        assert!(
            !rough_stage_wanted(true, Some(GIB), 1_440, 58),
            "a known size wins over the length"
        );
        assert!(
            !rough_stage_wanted(true, Some(5 * GIB), 940, 3),
            "nearly whole"
        );
        assert!(
            !rough_stage_wanted(false, Some(5 * GIB), 940, 38),
            "cannot jump"
        );
    }

    #[test]
    fn grab_args_jump_then_take_one_key_frame() {
        let out = Path::new("/data/trickplay/abc/r469.jpg");
        let input = "http://127.0.0.1:8096/stream/abc?token=t";
        let args = grab_args(input, 4_690, out);
        let at = |flag: &str| {
            args.iter()
                .position(|a| a == flag)
                .unwrap_or_else(|| panic!("{flag} missing"))
        };
        assert_eq!(args[at("-ss") + 1], "4690");
        assert!(at("-skip_frame") < at("-i"));
        assert!(at("-ss") < at("-i"));
        assert!(at("-noaccurate_seek") < at("-i"));
        assert!(at("-reconnect") < at("-i"));
        assert_eq!(args[at("-i") + 1], input);
        assert_eq!(args[at("-frames:v") + 1], "1");
        assert_eq!(args[at("-threads") + 1], "1");
        assert_eq!(args[at("-atomic_writing") + 1], "1");
        assert_eq!(args[at("-update") + 1], "1");
        let filter = &args[at("-vf") + 1];
        assert!(filter.contains("pad=320:180"));
        assert!(!filter.contains("tile"));
        assert!(!filter.contains("fps"));
        assert_eq!(
            args.last()
                .map(String::as_str),
            Some("/data/trickplay/abc/r469.jpg")
        );

        let first = grab_args("/media/invented-film.mkv", 0, out);
        assert!(
            !first
                .iter()
                .any(|a| a == "-ss"),
            "the first picture needs no jump"
        );
    }

    #[test]
    fn compose_rough_puts_the_nearest_picture_in_each_tile() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp
            .path()
            .join("set");
        // Every sheet but 0, 18 and 37 is made: only those three get one.
        write_sheets(&dir, (1..37).filter(|n| *n != 18));
        std::fs::write(rough_sheet_file(&dir, 5), JPEG).unwrap();
        std::fs::write(rough_file(&dir, 0), flat_picture([255, 0, 0])).unwrap();
        compose_rough(&dir, 940).unwrap();
        assert!(
            !rough_sheet_file(&dir, 0).exists(),
            "one rough picture composes nothing"
        );
        assert!(
            !rough_sheet_file(&dir, 5).exists(),
            "the stand-in of a sheet made goes"
        );

        std::fs::write(rough_file(&dir, 939), flat_picture([0, 0, 255])).unwrap();
        compose_rough(&dir, 940).unwrap();
        let read = |n: u32| {
            let bytes = std::fs::read(rough_sheet_file(&dir, n)).unwrap();
            assert!(jpeg_whole(&bytes));
            image::load_from_memory(&bytes)
                .unwrap()
                .into_rgb8()
        };
        let tile = |sheet: &image::RgbImage, t: u32| {
            sheet
                .get_pixel(
                    t % TILE_WIDTH * WIDTH + WIDTH / 2,
                    t / TILE_WIDTH * HEIGHT + HEIGHT / 2,
                )
                .0
        };
        let red = |p: [u8; 3]| p[0] > 200 && p[1] < 60 && p[2] < 60;
        let blue = |p: [u8; 3]| p[2] > 200 && p[0] < 60 && p[1] < 60;
        let black = |p: [u8; 3]| {
            p.iter()
                .all(|c| *c < 30)
        };
        let first = read(0);
        let middle = read(18);
        let last = read(37);
        assert_eq!(first.dimensions(), (1_600, 900));
        assert!(red(tile(&first, 0)));
        assert!(red(tile(&middle, 19)), "picture 469 is nearer the start");
        assert!(blue(tile(&middle, 20)), "picture 470 is nearer the end");
        assert!(blue(tile(&last, 14)), "picture 939");
        assert!(black(tile(&last, 15)), "past the last picture");
        assert!(black(tile(&last, 24)));
        for n in (1..37).filter(|n| *n != 18) {
            assert!(
                !rough_sheet_file(&dir, n).exists(),
                "{n}: its sheet is made"
            );
        }

        // A sheet made since: its stand-in goes at the next compose.
        write_sheets(&dir, [18]);
        compose_rough(&dir, 940).unwrap();
        assert!(!rough_sheet_file(&dir, 18).exists());
        assert!(rough_sheet_file(&dir, 0).exists());
        assert_eq!(sheets_in(&dir), (1..37).collect::<Vec<u32>>());
    }

    #[cfg(unix)]
    #[test]
    fn rough_files_are_never_sheets_even_after_a_crash() {
        assert_eq!(parse_sheet("rough-3.jpg"), None);
        assert_eq!(parse_sheet("r3.jpg"), None);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp
            .path()
            .join(ROOT_DIR);
        let dir = set_dir(&root, Uuid::new_v4());
        std::fs::create_dir_all(&dir).unwrap();
        for name in ["r0.jpg", "r939.jpg", "rough-0.jpg", "rough-1.jpg"] {
            std::fs::write(dir.join(name), JPEG).unwrap();
        }
        std::fs::write(dir.join("rough-2.jpg.tmp"), b"cut short").unwrap();
        // A crash during the rough stage: `busy` and no sheet yet.
        std::fs::write(dir.join(BUSY_MARKER), b"").unwrap();
        prune_on_start(&root, true, u64::MAX, None, SystemTime::now());

        let state = read_set(&dir);
        assert!(
            state
                .sheets
                .is_empty()
        );
        assert!(!state.end);
        assert_eq!(
            missing(&state, 38).len(),
            38,
            "every sheet is still to make"
        );
        assert!(!is_done(&dir));
        assert!(
            !dir.join(BUSY_MARKER)
                .exists()
        );
        assert_eq!(
            rough_made(&dir, 940),
            vec![0, 939],
            "kept for the next play"
        );
        assert!(sheets_in(&dir).is_empty());
    }

    #[tokio::test]
    async fn a_rough_stage_asks_in_the_spread_order_before_the_first_run() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            ..Default::default()
        });
        play_through(
            &passes,
            "play-a",
            rough_plan(tmp.path(), set_id, 940),
            runner.clone(),
            always(),
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(runner.grabs(), rough_order(940, ROUGH_PICTURES));
        assert!(
            runner
                .grab_waits
                .lock()
                .unwrap()
                .iter()
                .all(|wait| *wait == fast().rough_seek_wait),
            "each picture is given the same short wait"
        );
        assert_eq!(runner.starts(), vec![0]);
        let at_first_run = runner
            .rough_at_runs
            .lock()
            .unwrap()[0]
            .clone();
        assert_eq!(
            at_first_run
                .iter()
                .filter(|name| !name.starts_with("rough-"))
                .count(),
            ROUGH_PICTURES,
            "{at_first_run:?}"
        );
        let dir = set_dir(tmp.path(), set_id);
        assert_eq!(sheets_in(&dir), (0..38).collect::<Vec<u32>>());
        assert!(is_done(&dir));
        assert!(
            rough_files(&dir).is_empty(),
            "a finished set keeps no rough file"
        );
    }

    #[tokio::test]
    async fn rough_stand_ins_exist_before_the_first_run_and_go_once_done() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        // 100 pictures: 4 sheets, all missing. Two pictures, then three
        // failures, so one compose.
        let runner = Arc::new(FakeRunner {
            sheets: 4,
            real_pictures: true,
            grab_fails: vec![49, 24, 74],
            ..Default::default()
        });
        play_through(
            &passes,
            "play-a",
            rough_plan(tmp.path(), set_id, 100),
            runner.clone(),
            always(),
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(runner.grabs(), vec![0, 99, 49, 24, 74]);
        let at_first_run = runner
            .rough_at_runs
            .lock()
            .unwrap()[0]
            .clone();
        for n in 0..4 {
            assert!(
                at_first_run.contains(&format!("rough-{n}.jpg")),
                "{n}: {at_first_run:?}"
            );
        }
        let dir = set_dir(tmp.path(), set_id);
        assert_eq!(sheets_in(&dir), vec![0, 1, 2, 3]);
        assert!(is_done(&dir));
        assert!(rough_files(&dir).is_empty());
    }

    #[tokio::test]
    async fn three_failed_rough_pictures_in_a_row_end_the_stage_and_the_pass_goes_on() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            grab_fails: vec![0, 939, 469],
            ..Default::default()
        });
        play_through(
            &passes,
            "play-a",
            rough_plan(tmp.path(), set_id, 940),
            runner.clone(),
            always(),
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(runner.grabs(), vec![0, 939, 469]);
        assert!(
            passes.can_jump(set_id),
            "a failure is not a source that cannot jump"
        );
        assert!(!passes.failed_recently(set_id, Instant::now()));
        assert_eq!(runner.starts(), vec![0]);
        let dir = set_dir(tmp.path(), set_id);
        assert!(is_done(&dir));
        assert_eq!(sheets_in(&dir), (0..38).collect::<Vec<u32>>());
    }

    #[tokio::test]
    async fn a_rough_picture_at_the_end_that_times_out_means_no_rough_stage() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        reports.at("device-a", 80 * 60);
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            grab_timeouts: vec![939],
            ..Default::default()
        });
        play_through(
            &passes,
            "play-a",
            rough_plan(tmp.path(), set_id, 940),
            runner.clone(),
            reports.watch(),
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(runner.grabs(), vec![0, 939], "no more after it");
        assert!(!passes.can_rough(set_id));
        assert!(
            passes.can_jump(set_id),
            "the runs decide that for themselves"
        );
        assert_eq!(runner.starts(), vec![19, 0]);
        assert!(is_done(&set_dir(tmp.path(), set_id)));

        // A time-out anywhere else is one failure; the source still jumps.
        let other = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            grab_timeouts: vec![469],
            ..Default::default()
        });
        play_through(
            &passes,
            "play-b",
            rough_plan(tmp.path(), other, 940),
            runner.clone(),
            reports.watch(),
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(runner.grabs(), rough_order(940, ROUGH_PICTURES));
        assert!(passes.can_rough(other));
        assert!(passes.can_jump(other));
        assert_eq!(runner.starts(), vec![19, 0]);
    }

    #[tokio::test]
    async fn the_rough_stage_ends_with_its_time() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(Limits {
            rough_budget: Duration::from_millis(120),
            ..fast()
        });
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            grab_pace: Duration::from_millis(50),
            ..Default::default()
        });
        play_through(
            &passes,
            "play-a",
            rough_plan(tmp.path(), set_id, 940),
            runner.clone(),
            always(),
            Duration::from_secs(10),
        )
        .await;
        let grabs = runner.grabs();
        assert!((1..=4).contains(&grabs.len()), "{grabs:?}");
        assert_eq!(grabs[..], rough_order(940, ROUGH_PICTURES)[..grabs.len()]);
        assert_eq!(runner.starts(), vec![0]);
        assert!(is_done(&set_dir(tmp.path(), set_id)));
    }

    #[tokio::test]
    async fn the_rough_stage_gives_way_and_later_makes_only_what_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(Limits {
            quiet_alone: Duration::from_millis(150),
            ..fast()
        });
        let reports = Reports::default();
        reports.fresh("device-a");
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            grab_pace: Duration::from_millis(30),
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin(
                "play-a",
                rough_plan(tmp.path(), set_id, 940),
                runner.clone(),
                reports.watch(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| {
            runner
                .grabs()
                .len()
                >= 2
        })
        .await;
        // The viewer has gone.
        reports.silent("device-a", Duration::from_secs(1));
        tokio::time::sleep(Duration::from_millis(300)).await;
        let before = runner.grabs();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(runner.grabs(), before, "stood down");
        assert!(before.len() < ROUGH_PICTURES, "{before:?}");
        assert_eq!(
            runner
                .runs
                .load(Ordering::SeqCst),
            0
        );
        assert!(passes.is_queued(set_id));

        // Back: the rest of the rough pictures, none twice, then the runs.
        reports.fresh("device-a");
        until_within(Duration::from_secs(10), || !passes.is_queued(set_id)).await;
        let grabs = runner.grabs();
        assert_eq!(grabs, rough_order(940, ROUGH_PICTURES), "none asked twice");
        assert!(is_done(&set_dir(tmp.path(), set_id)));
    }

    #[tokio::test]
    async fn a_cancel_during_the_rough_stage_keeps_its_pictures() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            grab_hang: Some(469),
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin(
                "play-a",
                rough_plan(tmp.path(), set_id, 940),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| {
            runner
                .grabs()
                .len()
                == 3
        })
        .await;
        let dir = set_dir(tmp.path(), set_id);
        assert!(
            dir.join(BUSY_MARKER)
                .exists()
        );
        passes.release("play-a");
        until(|| {
            !dir.join(BUSY_MARKER)
                .exists()
        })
        .await;

        assert_eq!(rough_made(&dir, 940), vec![0, 939]);
        assert!(sheets_in(&dir).is_empty());
        assert_eq!(
            runner
                .runs
                .load(Ordering::SeqCst),
            0
        );
        assert!(!is_done(&dir));
    }

    #[tokio::test]
    async fn no_rough_stage_switched_off_small_nearly_whole_or_unable_to_jump() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let off = Uuid::new_v4();
        let small = Uuid::new_v4();
        let nearly_whole = Uuid::new_v4();
        let no_jump = Uuid::new_v4();
        write_sheets(&set_dir(tmp.path(), nearly_whole), 0..35);
        passes.mark_no_jump(no_jump);
        let cases = [
            (
                off,
                PassPlan {
                    rough: false,
                    ..rough_plan(tmp.path(), off, 940)
                },
            ),
            (
                small,
                PassPlan {
                    size: Some(2 * GIB),
                    ..rough_plan(tmp.path(), small, 940)
                },
            ),
            (nearly_whole, rough_plan(tmp.path(), nearly_whole, 940)),
            (no_jump, rough_plan(tmp.path(), no_jump, 940)),
        ];
        for (i, (set_id, plan)) in cases
            .into_iter()
            .enumerate()
        {
            let runner = Arc::new(FakeRunner {
                sheets: 38,
                ..Default::default()
            });
            play_through(
                &passes,
                &format!("play-{i}"),
                plan,
                runner.clone(),
                always(),
                Duration::from_secs(10),
            )
            .await;
            assert!(
                runner
                    .grabs()
                    .is_empty(),
                "case {i}"
            );
            let dir = set_dir(tmp.path(), set_id);
            assert!(is_done(&dir), "case {i}");
            assert!(rough_files(&dir).is_empty(), "case {i}");
        }
    }

    /// What the pass of the version played reads, in [`set_plan`].
    const PLAYED: &str = "/media/invented-film.mkv";

    /// A version of `title` measured at `secs`, cached at a named service
    /// and listed at [`LISTED`] bytes, from the addon `addon_id`, listed at
    /// `idx`.
    fn version(title: Uuid, secs: f64, addon_id: Uuid, idx: i64) -> db::Media {
        db::Media {
            idx: Some(idx),
            stream_info: Some(StreamInfo {
                descriptor: StreamDescriptor::http(URL),
                addon_id: Some(addon_id),
                service_id: Some("svc-cached".to_string()),
                service_cached: Some(true),
                size: Some(LISTED as i64),
                ..Default::default()
            }),
            ..measured(Some(title), secs, Some(api::ProbeOrigin::Ffprobe))
        }
    }

    fn rules_of(addons: &[&Addon]) -> AddonRules {
        addons
            .iter()
            .map(|addon| (addon.id, AddonRule::new(addon)))
            .collect()
    }

    fn info_of(row: &mut db::Media) -> &mut StreamInfo {
        row.stream_info
            .as_mut()
            .unwrap()
    }

    #[test]
    fn other_versions_are_only_those_certain_to_fit_and_cheap_to_ask_for() {
        let title = Uuid::new_v4();
        let on = addon("stremio", false);
        let rules = rules_of(&[&on]);
        let none_own = |_: Uuid| false;
        let playing = version(title, 7_200.0, on.id, 0);
        let fits = version(title, 7_195.0, on.id, 1);
        let ids = |found: Vec<&db::Media>| -> Vec<Uuid> {
            found
                .iter()
                .map(|m| m.id)
                .collect()
        };
        assert_eq!(
            ids(other_versions(
                &playing,
                &[playing.clone(), fits.clone()],
                None,
                &rules,
                none_own
            )),
            vec![fits.id],
            "the same count, cached at a named service; never the version played"
        );

        // Each row below breaks one rule.
        let mut refused: Vec<(&str, db::Media)> = vec![
            ("another count", version(title, 7_300.0, on.id, 2)),
            (
                "a length from the file name",
                db::Media {
                    probe_data: measured(
                        Some(title),
                        7_200.0,
                        Some(api::ProbeOrigin::FilenameGuess),
                    )
                    .probe_data,
                    ..version(title, 7_200.0, on.id, 2)
                },
            ),
            (
                "no probe data",
                db::Media {
                    probe_data: None,
                    ..version(title, 7_200.0, on.id, 2)
                },
            ),
        ];
        let mut row = version(title, 7_200.0, on.id, 2);
        info_of(&mut row).service_cached = None;
        refused.push(("not marked cached", row));
        let mut row = version(title, 7_200.0, on.id, 2);
        info_of(&mut row).service_cached = Some(false);
        refused.push(("marked not cached", row));
        let mut row = version(title, 7_200.0, on.id, 2);
        info_of(&mut row).service_id = None;
        refused.push(("cached with no service, as IPTV on demand is listed", row));
        let mut row = version(title, 7_200.0, on.id, 2);
        info_of(&mut row).descriptor = StreamDescriptor::Torrent {
            info_hash: "0123456789abcdef0123456789abcdef01234567".to_string(),
            file_hint: None,
            file_idx: None,
            trackers: vec![],
        };
        refused.push(("a torrent", row));
        let mut row = version(title, 7_200.0, on.id, 2);
        info_of(&mut row).size = None;
        refused.push(("a service's file that lists no size", row));
        let mut row = version(title, 7_200.0, on.id, 2);
        row.probe_data
            .as_mut()
            .unwrap()
            .size = Some(LISTED as i64 * 2);
        refused.push(("measured from another file than it lists", row));
        let iptv = addon("iptv-m3u", false);
        let redirect = addon("stremio", true);
        let mut off = addon("stremio", false);
        off.trickplay = false;
        let mut filtered = addon("stremio", false);
        filtered.trickplay_service_filter = vec!["svc-other".to_string()];
        let all = rules_of(&[&on, &iptv, &redirect, &off, &filtered]);
        for (why, from) in [
            ("an IPTV addon", iptv.id),
            ("an addon that bypasses the proxy", redirect.id),
            ("an addon with previews off", off.id),
            ("a service outside the addon's list", filtered.id),
            ("an addon not in the rules", Uuid::new_v4()),
        ] {
            refused.push((why, version(title, 7_200.0, from, 2)));
        }
        let mut row = version(title, 7_200.0, on.id, 2);
        info_of(&mut row).addon_id = None;
        refused.push(("no addon", row));
        for (why, row) in &refused {
            assert!(
                other_versions(
                    &playing,
                    std::slice::from_ref(row),
                    None,
                    &all,
                    none_own
                )
                .is_empty(),
                "{why}"
            );
        }
        let owned = version(title, 7_200.0, on.id, 2);
        let owned_id = owned.id;
        assert!(
            other_versions(&playing, &[owned], None, &all, |id| id == owned_id)
                .is_empty(),
            "a version with a folder of its own"
        );

        // A version played with no measured length has none.
        let unmeasured = db::Media {
            probe_data: None,
            ..version(title, 7_200.0, on.id, 0)
        };
        assert!(
            other_versions(&unmeasured, &[fits.clone()], None, &rules, none_own)
                .is_empty()
        );

        // One episode from its own file and from a file holding two.
        let episode = Uuid::new_v4();
        let single = version(episode, 720.0, on.id, 0);
        let combined = version(episode, 1_407.0, on.id, 1);
        let both = [single.clone(), combined.clone()];
        assert!(other_versions(&single, &both, None, &rules, none_own).is_empty());
        assert!(other_versions(&combined, &both, None, &rules, none_own).is_empty());
    }

    #[test]
    fn other_versions_cheapest_first_then_smallest_then_listed_first() {
        let title = Uuid::new_v4();
        let on = addon("stremio", false);
        let rules = rules_of(&[&on]);
        let playing = version(title, 7_200.0, on.id, 0);
        let mut usenet = version(title, 7_200.0, on.id, 1);
        info_of(&mut usenet).usenet_guid = Some("guid".to_string());
        let mut cached_big = version(title, 7_200.0, on.id, 2);
        info_of(&mut cached_big).size = Some(LISTED as i64 * 4);
        let cached_late = version(title, 7_200.0, on.id, 4);
        let cached_early = version(title, 7_200.0, on.id, 3);
        let mut webdav = version(title, 7_200.0, on.id, 5);
        info_of(&mut webdav).descriptor = StreamDescriptor::Opendal {
            addon_id: on.id,
            path: "films/invented-film.mkv".to_string(),
        };
        let mut local_unsized = version(title, 7_200.0, on.id, 6);
        info_of(&mut local_unsized).descriptor =
            StreamDescriptor::Local("/media/invented-film-a.mkv".into());
        info_of(&mut local_unsized).size = None;
        let mut local = version(title, 7_200.0, on.id, 7);
        info_of(&mut local).descriptor =
            StreamDescriptor::Local("/media/invented-film-b.mkv".into());
        let rows = [
            playing.clone(),
            usenet.clone(),
            cached_big.clone(),
            cached_late.clone(),
            cached_early.clone(),
            webdav.clone(),
            local_unsized.clone(),
            local.clone(),
        ];
        let order: Vec<Uuid> = other_versions(&playing, &rows, None, &rules, |_| false)
            .iter()
            .map(|m| m.id)
            .collect();
        assert_eq!(
            order,
            vec![
                local.id,
                local_unsized.id,
                webdav.id,
                cached_early.id,
                cached_late.id,
                cached_big.id,
                usenet.id,
            ]
        );
    }

    #[test]
    fn the_size_check_reads_only_the_file_listed() {
        let listed = StreamInfo {
            size: Some(LISTED as i64),
            ..Default::default()
        };
        assert!(is_listed_file(&listed, Some(LISTED)));
        assert!(!is_listed_file(&listed, Some(LISTED * 3)), "another file");
        assert!(
            !is_listed_file(&listed, None),
            "a source that gives no size"
        );
        assert!(!is_listed_file(&listed, Some(0)));
        let no_size = StreamInfo::default();
        assert!(
            is_listed_file(&no_size, Some(LISTED)),
            "nothing to compare with"
        );
    }

    /// Another version listed at [`LISTED`] bytes, cached at a named
    /// service, read from `input`.
    fn other_source(input: &str) -> PassSource {
        PassSource {
            stream_id: Uuid::new_v4(),
            input: input.to_string(),
            info: StreamInfo {
                descriptor: StreamDescriptor::http(URL),
                service_id: Some("svc-cached".to_string()),
                service_cached: Some(true),
                size: Some(LISTED as i64),
                ..Default::default()
            },
        }
    }

    /// A pass over the version played into `set_id`, with `others` behind it.
    fn plan_with(root: &Path, set_id: Uuid, others: &[PassSource]) -> PassPlan {
        PassPlan {
            others: others.to_vec(),
            ..set_plan(root, Uuid::new_v4(), set_id)
        }
    }

    fn strings(inputs: &[&str]) -> Vec<String> {
        inputs
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[tokio::test]
    async fn a_failed_pass_goes_on_with_another_version() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let (a, b) = (other_source("other-a"), other_source("other-b"));
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            dead: strings(&[PLAYED, "other-a"]),
            ..Default::default()
        });
        let plan = plan_with(tmp.path(), set_id, &[a.clone(), b.clone()]);
        let played = plan.stream_id;
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin("play-a", plan, runner.clone(), always())
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| !passes.is_queued(set_id)).await;

        assert_eq!(runner.inputs(), strings(&[PLAYED, "other-a", "other-b"]));
        assert_eq!(
            runner.sized(),
            strings(&["other-a", "other-b"]),
            "never the version played"
        );
        let dir = set_dir(tmp.path(), set_id);
        assert!(is_done(&dir));
        assert_eq!(sheets_in(&dir), (0..29).collect::<Vec<u32>>());
        // The waits are in memory, by stream; the folder holds no failure.
        let kept: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter_map(|e| {
                e.file_name()
                    .to_str()
                    .map(str::to_string)
            })
            .filter(|name| {
                parse_sheet(name).is_none() && name != DONE_MARKER && name != END_MARKER
            })
            .collect();
        assert!(kept.is_empty(), "{kept:?}");
        let now = Instant::now();
        assert!(passes.failed_recently(played, now));
        assert!(
            !passes
                .failed_recently(played, now + FAILURE_WAIT + Duration::from_secs(1))
        );
        assert!(
            passes.failed_recently(
                a.stream_id,
                now + FAILURE_WAIT + Duration::from_secs(1)
            )
        );
        assert!(!passes.failed_recently(
            a.stream_id,
            now + OTHER_FAILURE_WAIT + Duration::from_secs(1)
        ));
        assert!(!passes.failed_recently(b.stream_id, now));
    }

    #[tokio::test]
    async fn a_pass_reads_three_sources_then_gives_up() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), set_id);
        write_sheets(&dir, 0..5);
        let others = [
            other_source("other-a"),
            other_source("other-b"),
            other_source("other-c"),
        ];
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            dead: strings(&[PLAYED, "other-a", "other-b", "other-c"]),
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin(
                "play-a",
                plan_with(tmp.path(), set_id, &others),
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.inputs(), strings(&[PLAYED, "other-a", "other-b"]));
        assert!(
            !passes.failed_recently(others[2].stream_id, Instant::now()),
            "never opened"
        );
        assert!(!is_done(&dir));
        assert_eq!(
            sheets_in(&dir),
            (0..5).collect::<Vec<u32>>(),
            "kept as they were"
        );
    }

    #[tokio::test]
    async fn the_next_play_goes_to_the_next_version() {
        let tmp = tempfile::tempdir().unwrap();
        let failure_wait = Duration::from_millis(300);
        let passes = TrickplayPasses::with_limits(Limits {
            failure_wait,
            ..fast()
        });
        let set_id = Uuid::new_v4();
        let others = [
            other_source("other-a"),
            other_source("other-b"),
            other_source("other-c"),
        ];
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            dead: strings(&[PLAYED, "other-a", "other-b"]),
            ..Default::default()
        });
        let first = plan_with(tmp.path(), set_id, &others);
        let played = first.stream_id;
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin("play-a", first, runner.clone(), always())
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.inputs(), strings(&[PLAYED, "other-a", "other-b"]));

        // Once the version played may be read again, it is read first, then
        // the next version not resting: never the two that failed.
        tokio::time::sleep(failure_wait + Duration::from_millis(100)).await;
        passes.register("play-b", Uuid::new_v4(), "device-b");
        let begun = passes
            .begin(
                "play-b",
                PassPlan {
                    stream_id: played,
                    ..plan_with(tmp.path(), set_id, &others)
                },
                runner.clone(),
                always(),
            )
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(
            runner.inputs(),
            strings(&[PLAYED, "other-a", "other-b", PLAYED, "other-c"])
        );
        assert!(is_done(&set_dir(tmp.path(), set_id)));
    }

    #[tokio::test]
    async fn a_recent_failure_starts_with_another_version() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            ..Default::default()
        });
        let plan = plan_with(tmp.path(), set_id, &[other_source("other-a")]);
        passes.mark_failed(plan.stream_id, Instant::now() + FAILURE_WAIT);
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin("play-a", plan, runner.clone(), always())
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.inputs(), strings(&["other-a"]));
        assert!(is_done(&set_dir(tmp.path(), set_id)));

        // With none to read in its place, nothing is queued, as before.
        let other_set = Uuid::new_v4();
        let plan = plan_with(tmp.path(), other_set, &[]);
        passes.mark_failed(plan.stream_id, Instant::now() + FAILURE_WAIT);
        passes.register("play-b", Uuid::new_v4(), "device-b");
        let begun = passes
            .begin("play-b", plan, runner.clone(), always())
            .await;
        assert_eq!(begun, Begin::FailedRecently);
    }

    #[tokio::test]
    async fn another_version_builds_on_what_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), set_id);
        write_sheets(&dir, 0..10);
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            dead: strings(&[PLAYED]),
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                plan_with(tmp.path(), set_id, &[other_source("other-a")]),
                runner.clone(),
                always(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.inputs(), strings(&[PLAYED, "other-a"]));
        assert_eq!(runner.starts(), vec![10, 10]);
        assert!(is_done(&dir));
        assert_eq!(sheets_in(&dir), (0..29).collect::<Vec<u32>>());
    }

    #[tokio::test]
    async fn another_version_stops_with_its_viewer() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(Limits {
            quiet_alone: Duration::from_millis(150),
            ..fast()
        });
        let reports = Reports::default();
        reports.fresh("device-a");
        let set_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), set_id);
        let other = other_source("other-a");
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            pace: Duration::from_millis(20),
            dead: strings(&[PLAYED]),
            ..Default::default()
        });
        let plan = plan_with(tmp.path(), set_id, std::slice::from_ref(&other));
        let played = plan.stream_id;
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin("play-a", plan, runner.clone(), reports.watch())
            .await;
        until(|| sheets_in(&dir).len() >= 2).await;

        // The viewer goes silent: the other version's run gives way.
        reports.silent("device-a", Duration::from_secs(1));
        until(|| {
            runner
                .killed
                .load(Ordering::SeqCst)
        })
        .await;
        assert!(passes.is_queued(set_id), "waiting to be watched again");
        // The play ends: the pass ends with it.
        passes.release("play-a");
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.inputs(), strings(&[PLAYED, "other-a"]));
        let made = sheets_in(&dir);
        assert!(!made.is_empty() && made.len() < 29, "{made:?}");
        assert!(!is_done(&dir));
        assert!(passes.failed_recently(played, Instant::now()));
        assert!(
            !passes.failed_recently(other.stream_id, Instant::now()),
            "giving way is not failing"
        );
    }

    #[tokio::test]
    async fn another_version_of_another_size_is_not_read() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let others = [other_source("other-a"), other_source("other-b")];
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            dead: strings(&[PLAYED]),
            wrong_size: strings(&["other-a"]),
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                plan_with(tmp.path(), set_id, &others),
                runner.clone(),
                always(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.sized(), strings(&["other-a", "other-b"]));
        assert_eq!(runner.inputs(), strings(&[PLAYED, "other-b"]));
        assert!(
            passes.failed_recently(others[0].stream_id, Instant::now() + FAILURE_WAIT)
        );
        assert!(is_done(&set_dir(tmp.path(), set_id)));
    }

    #[tokio::test]
    async fn another_version_longer_than_its_count_is_dropped() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        // 940 pictures fill sheets 0 to 37; other-a holds sheet 38 too.
        let set_id = Uuid::new_v4();
        let others = [other_source("other-a"), other_source("other-b")];
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            dead: strings(&[PLAYED]),
            long: strings(&["other-a"]),
            ..Default::default()
        });
        let plan = PassPlan {
            pictures: 940,
            ..plan_with(tmp.path(), set_id, &others)
        };
        let played = plan.stream_id;
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin("play-a", plan, runner.clone(), always())
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.inputs(), strings(&[PLAYED, "other-a", "other-b"]));
        let own = set_dir(tmp.path(), others[0].stream_id);
        assert!(
            own.join("38.jpg")
                .exists(),
            "other-a's pictures are in a folder of its own"
        );
        assert!(
            passes.failed_recently(others[0].stream_id, Instant::now() + FAILURE_WAIT)
        );
        let dir = set_dir(tmp.path(), set_id);
        assert!(is_done(&dir));
        assert_eq!(
            sheets_in(&dir),
            (0..38).collect::<Vec<u32>>(),
            "made again from other-b"
        );

        // After a restart forgets every wait, other-a's own folder still
        // keeps it from being read in the version played's place.
        let restarted = TrickplayPasses::with_limits(fast());
        let other_set = Uuid::new_v4();
        let plan = PassPlan {
            stream_id: played,
            pictures: 940,
            ..plan_with(tmp.path(), other_set, &others[..1])
        };
        restarted.mark_failed(played, Instant::now() + FAILURE_WAIT);
        restarted.register("play-b", Uuid::new_v4(), "device-b");
        let begun = restarted
            .begin("play-b", plan, runner.clone(), always())
            .await;
        assert_eq!(begun, Begin::FailedRecently);
    }

    #[tokio::test]
    async fn a_version_that_is_not_read_is_never_opened() {
        let tmp = tempfile::tempdir().unwrap();
        let set_id = Uuid::new_v4();
        let (a, b) = (other_source("other-a"), other_source("other-b"));

        // Read: the plan is left as it was.
        let plan =
            read_other_if_refused(plan_with(tmp.path(), set_id, &[a.clone()]), None)
                .unwrap();
        assert_eq!(plan.input, PLAYED);
        assert!(
            plan.other_version
                .is_none()
        );
        assert_eq!(
            plan.others
                .len(),
            1
        );

        // Not read, with no other version: no pass.
        let refused = Some(Ineligible::AddonExcluded);
        let none = read_other_if_refused(plan_with(tmp.path(), set_id, &[]), refused);
        assert_eq!(none.err(), refused);

        // Not read, with other versions: the first is read, the rest follow.
        let plan = read_other_if_refused(
            plan_with(tmp.path(), set_id, &[a.clone(), b.clone()]),
            Some(Ineligible::Iptv),
        )
        .unwrap();
        assert_eq!(plan.stream_id, a.stream_id);
        assert_eq!(plan.input, "other-a");
        assert_eq!(plan.size, Some(LISTED));
        assert!(
            plan.other_version
                .is_some()
        );
        assert_eq!(plan.set_id, set_id, "the set stays the version played's");
        assert_eq!(
            plan.others
                .iter()
                .map(|s| s.stream_id)
                .collect::<Vec<_>>(),
            vec![b.stream_id]
        );

        let passes = TrickplayPasses::with_limits(fast());
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            dead: strings(&["other-a"]),
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin("play-a", plan, runner.clone(), always())
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.inputs(), strings(&["other-a", "other-b"]));
        assert!(
            !runner
                .sized()
                .contains(&PLAYED.to_string())
        );
        assert!(is_done(&set_dir(tmp.path(), set_id)));
    }

    #[tokio::test]
    async fn a_resumed_version_whose_source_ends_at_once_goes_on_to_another() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let reports = Reports::default();
        reports.at("device-a", 20 * SHEET_SECS);
        let set_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), set_id);
        write_sheets(&dir, 0..10);
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            empty: strings(&[PLAYED]),
            ..Default::default()
        });
        let plan = plan_with(tmp.path(), set_id, &[other_source("other-a")]);
        let played = plan.stream_id;
        passes.register("play-a", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin("play-a", plan, runner.clone(), reports.watch())
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| !passes.is_queued(set_id)).await;
        // Both runs of the version played start above sheet 0 and end empty.
        let inputs = runner.inputs();
        assert_eq!(
            inputs[..2].to_vec(),
            strings(&[PLAYED, PLAYED]),
            "{inputs:?}"
        );
        assert_eq!(runner.starts()[..2].to_vec(), vec![20, 10]);
        assert!(
            inputs.len() > 2
                && inputs[2..]
                    .iter()
                    .all(|i| i == "other-a"),
            "{inputs:?}"
        );
        assert!(passes.failed_recently(played, Instant::now()));
        assert!(is_done(&dir));
        assert_eq!(sheets_in(&dir), (0..29).collect::<Vec<u32>>());

        // With no other version, the pass ends as it did, and the version
        // played does not rest.
        let set_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), set_id);
        write_sheets(&dir, 0..10);
        let plan = plan_with(tmp.path(), set_id, &[]);
        let played = plan.stream_id;
        passes.register("play-b", Uuid::new_v4(), "device-a");
        let begun = passes
            .begin("play-b", plan, runner.clone(), reports.watch())
            .await;
        assert_eq!(begun, Begin::Queued);
        until(|| !passes.is_queued(set_id)).await;
        assert!(!passes.failed_recently(played, Instant::now()));
        assert!(!is_done(&dir));
        assert_eq!(sheets_in(&dir), (0..10).collect::<Vec<u32>>());
    }

    #[tokio::test]
    async fn another_version_that_ends_short_leaves_none_of_its_pictures() {
        // A clean end short of the count: a shorter file. Its sheets are
        // deleted and the next version makes every one of them.
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let others = [other_source("other-a"), other_source("other-b")];
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            dead: strings(&[PLAYED]),
            short: strings(&["other-a"]),
            ..Default::default()
        });
        passes.register("play-a", Uuid::new_v4(), "device-a");
        passes
            .begin(
                "play-a",
                plan_with(tmp.path(), set_id, &others),
                runner.clone(),
                always(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.inputs(), strings(&[PLAYED, "other-a", "other-b"]));
        assert_eq!(runner.starts(), vec![0, 0, 0], "other-b made every sheet");
        assert!(
            passes.failed_recently(others[0].stream_id, Instant::now() + FAILURE_WAIT)
        );
        let dir = set_dir(tmp.path(), set_id);
        assert!(is_done(&dir));
        assert_eq!(sheets_in(&dir), (0..29).collect::<Vec<u32>>());

        // A source that fails part way: its sheets are kept and built on.
        let set_id = Uuid::new_v4();
        let others = [other_source("other-c"), other_source("other-d")];
        let runner = Arc::new(FakeRunner {
            sheets: 29,
            dead: strings(&[PLAYED]),
            cut: strings(&["other-c"]),
            ..Default::default()
        });
        passes.register("play-b", Uuid::new_v4(), "device-b");
        passes
            .begin(
                "play-b",
                plan_with(tmp.path(), set_id, &others),
                runner.clone(),
                always(),
            )
            .await;
        until(|| !passes.is_queued(set_id)).await;
        assert_eq!(runner.inputs(), strings(&[PLAYED, "other-c", "other-d"]));
        // other-c made sheets 0 to 13; the last, maybe partly filled, went.
        assert_eq!(runner.starts(), vec![0, 0, 13]);
        assert!(is_done(&set_dir(tmp.path(), set_id)));
    }

    #[tokio::test]
    async fn only_the_first_source_of_a_pass_gets_a_rough_stage() {
        // Another version read first, here inside the version played's failure
        // wait: it gets the rough stage, under the same rules as any source.
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            ..Default::default()
        });
        let plan = PassPlan {
            pictures: 940,
            rough: true,
            ..plan_with(tmp.path(), set_id, &[other_source("other-a")])
        };
        passes.mark_failed(plan.stream_id, Instant::now() + FAILURE_WAIT);
        play_through(
            &passes,
            "play-a",
            plan,
            runner.clone(),
            always(),
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(runner.grabs(), rough_order(940, ROUGH_PICTURES));
        assert_eq!(runner.inputs(), strings(&["other-a"]));
        assert!(is_done(&set_dir(tmp.path(), set_id)));

        // After the version played failed, its stage was the pass's one:
        // another version read next makes none.
        let set_id = Uuid::new_v4();
        let runner = Arc::new(FakeRunner {
            sheets: 38,
            dead: strings(&[PLAYED]),
            ..Default::default()
        });
        let plan = PassPlan {
            pictures: 940,
            size: Some(5 * GIB),
            rough: true,
            ..plan_with(tmp.path(), set_id, &[other_source("other-a")])
        };
        play_through(
            &passes,
            "play-b",
            plan,
            runner.clone(),
            always(),
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(
            runner.grabs(),
            vec![0, 939, 469],
            "three failed, all of the version played"
        );
        assert_eq!(runner.inputs(), strings(&[PLAYED, "other-a"]));
        let dir = set_dir(tmp.path(), set_id);
        assert!(is_done(&dir));
        assert_eq!(sheets_in(&dir), (0..38).collect::<Vec<u32>>());
    }
}
