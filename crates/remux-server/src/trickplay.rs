//! Jellyfin trick play pictures (seek previews), made in the background while
//! a video plays.
//!
//! Off by default (`TrickplayWhileStreaming`). When on, a play-start report
//! queues one low-priority ffmpeg that reads the file from its start through
//! this server's own stream proxy, decodes key frames only and writes one
//! 320x180 picture every 10 seconds, 25 to a sheet, under
//! `{data_dir}/trickplay/{set id}/{n}.jpg`.
//!
//! A set belongs to a title and a picture count (see [`set_id`]), so every
//! version of a title whose measured length gives the same number of pictures
//! shares one set, and a version of another length gets its own. One pass runs
//! at a time, a pass is shared by every play of the same set, it gives way
//! while nobody is watching it and carries on when someone reports again, and
//! it stops a grace period after the last of those plays ends.

use std::{
    collections::{HashMap, HashSet},
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
/// Share of the expected sheets a pass must make to count as complete.
pub const COMPLETE_PERCENT: u64 = 95;
/// Directory under `data_dir` that holds every set of pictures.
pub const ROOT_DIR: &str = "trickplay";

/// Estimated bits per second of pictures (about 13 KB a picture), as
/// Jellyfin's `Bandwidth`. Clients ignore it.
const BANDWIDTH: i32 = 10_400;
/// Written into a stream's directory once its pass completed.
const DONE_MARKER: &str = "done";
/// Temporary files of an unfinished write older than this are deleted.
const TMP_MAX_AGE: Duration = Duration::from_secs(10 * 60);
/// A served directory's use time is updated at most this often.
const TOUCH_EVERY: Duration = Duration::from_secs(60);
/// Smallest disk allowance, in MB.
const MIN_CACHE_MB: i64 = 100;
/// How long `kill_all` waits for each pass before killing its process.
const KILL_WAIT: Duration = Duration::from_secs(5);
const TICKS_PER_SEC: i64 = 10_000_000;

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

/// ffmpeg's arguments for one pass.
pub fn pass_args(input: &str, out_dir: &Path) -> Vec<String> {
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
        "fps=1/{INTERVAL_SECS},\
         scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=decrease,\
         pad={WIDTH}:{HEIGHT}:(ow-iw)/2:(oh-ih)/2,\
         tile={TILE_WIDTH}x{TILE_HEIGHT}"
    );
    let output = out_dir
        .join("%d.jpg")
        .to_string_lossy()
        .into_owned();
    args.extend([
        "-skip_frame".to_string(),
        "nokey".to_string(),
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
        "0".to_string(),
        output,
        "-progress".to_string(),
        "pipe:1".to_string(),
        "-nostats".to_string(),
    ]);
    args
}

/// Whether `made_sheets` (ffmpeg's last `frame=` value, which counts sheets)
/// is at least 95% of the sheets `pictures` fill.
pub fn is_complete(made_sheets: u64, pictures: u64) -> bool {
    let per_sheet = u64::from(TILE_WIDTH * TILE_HEIGHT);
    let needed = (COMPLETE_PERCENT * pictures).div_ceil(100 * per_sheet);
    made_sheets >= needed
}

/// The number of the last sheet `pictures` fill.
fn last_sheet(pictures: u64) -> u64 {
    pictures.saturating_sub(1) / u64::from(TILE_WIDTH * TILE_HEIGHT)
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

/// Deletes the highest-numbered sheet: a pass cut off by a crash may have
/// flushed a partly filled one.
fn remove_last_sheet(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let last = entries
        .flatten()
        .filter_map(|e| {
            e.file_name()
                .to_str()
                .and_then(parse_sheet)
        })
        .max();
    if let Some(n) = last {
        let _ = std::fs::remove_file(dir.join(format!("{n}.jpg")));
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
    // A pass cut off by a crash may have flushed a partly filled last sheet.
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let dir = entry.path();
        if dir.is_dir() && !is_done(&dir) {
            remove_last_sheet_keeping_time(&dir);
        }
    }
}

/// [`remove_last_sheet`] without changing the directory's time, which records
/// when the set was last shown.
fn remove_last_sheet_keeping_time(dir: &Path) {
    let used = std::fs::metadata(dir).and_then(|m| m.modified());
    remove_last_sheet(dir);
    if let (Ok(used), Ok(file)) = (used, std::fs::File::open(dir)) {
        let _ = file.set_modified(used);
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
/// itself when no stream row has it. One row read by primary key.
pub async fn set_for_stream(ctx: &AppContext, stream_id: Uuid) -> Uuid {
    let root = root_dir(
        &ctx.config
            .data_dir,
    );
    match stored_stream(ctx, stream_id).await {
        Some(stream) => set_for(&root, &stream),
        None => stream_id,
    }
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
    eligible_pass(addon.as_ref(), info)?;
    let runtime_s = runtime.ok_or(Ineligible::NoRuntime)?;
    let cfg = db::Settings::get_config_or_default(&ctx.db).await;
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
    Ok(PassPlan {
        stream_id: stream.id,
        set_id: set_for(&root, &stream),
        input: pass_input(
            info,
            stream.id,
            ctx.config
                .port,
        ),
        root,
        pictures,
        cap_bytes: cap_bytes(&cfg).saturating_sub(estimate_bytes(runtime_s)),
        keep_for: keep_for(&cfg),
        db: Some(
            ctx.db
                .clone(),
        ),
    })
}

/// Everything a queued pass needs.
#[derive(Debug, Clone)]
pub(crate) struct PassPlan {
    /// The stream read, for its proxy input, the logs and the failure wait.
    pub stream_id: Uuid,
    /// The set its pictures belong to (see [`set_for`]).
    pub set_id: Uuid,
    pub input: String,
    pub root: PathBuf,
    /// The picture count the set is keyed by.
    pub pictures: u64,
    /// Disk allowance left for other sets once this one is made.
    pub cap_bytes: u64,
    /// See [`keep_for`].
    pub keep_for: Option<Duration>,
    /// Where the switch is read again just before the pass starts, since a
    /// queued pass may wait long after its play started. `None` in tests.
    pub db: Option<sqlx::SqlitePool>,
}

/// What a runner is given.
#[derive(Debug, Clone)]
pub(crate) struct PassJob {
    pub input: String,
    pub out_dir: PathBuf,
    /// Set to the child's pid once it runs.
    pub pid: Arc<AtomicU32>,
    pub min_free_bytes: u64,
}

/// How a runner ended.
#[derive(Debug, Clone, Default)]
pub(crate) struct RunResult {
    pub exit_ok: bool,
    /// The last `frame=` value of `-progress`, which counts sheets.
    pub made_sheets: u64,
    /// Stopped by a cancel or by low disk, not by its own end.
    pub cancelled: bool,
    /// With `cancelled`: ffmpeg's output had already ended before the stop
    /// came, so it may have flushed a partly filled last sheet.
    pub ended_itself: bool,
    pub stderr: String,
}

/// Makes the pictures of one pass; ffmpeg in production, a fake in tests.
/// It must stop, and wait for its process to exit, once `cancel` resolves
/// (a stop sent or the sender dropped).
pub(crate) trait PassRunner: Send + Sync + 'static {
    fn run(
        &self,
        job: PassJob,
        cancel: oneshot::Receiver<()>,
    ) -> impl Future<Output = RunResult> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Complete,
    Failed,
    Cancelled,
}

fn outcome(result: &RunResult, pictures: u64) -> Outcome {
    if result.cancelled {
        Outcome::Cancelled
    } else if result.exit_ok && is_complete(result.made_sheets, pictures) {
        Outcome::Complete
    } else {
        Outcome::Failed
    }
}

/// Runs ffmpeg at low priority with [`pass_args`].
pub(crate) struct FfmpegRunner;

impl PassRunner for FfmpegRunner {
    fn run(
        &self,
        job: PassJob,
        cancel: oneshot::Receiver<()>,
    ) -> impl Future<Output = RunResult> + Send {
        run_ffmpeg(job, cancel)
    }
}

async fn run_ffmpeg(job: PassJob, mut cancel: oneshot::Receiver<()>) -> RunResult {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};

    let mut cmd = tokio::process::Command::new(crate::tools::ffmpeg());
    cmd.hide_console();
    cmd.kill_on_drop(true);
    cmd.args(pass_args(&job.input, &job.out_dir));
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
                line = lines.next_line() => {
                    let Ok(Some(line)) = line else {
                        break;
                    };
                    let Some(n) = progress_frame(&line) else {
                        continue;
                    };
                    made_sheets = n;
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
    let exit_ok = if cancelled || low_disk {
        let _ = child.start_kill();
        let _ = child
            .wait()
            .await;
        false
    } else {
        tokio::select! {
            status = child.wait() => status.is_ok_and(|s| s.success()),
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
        made_sheets,
        cancelled: cancelled || low_disk,
        ended_itself,
        stderr,
    }
}

/// Durations and limits, fixed in production and shortened in tests.
#[derive(Debug, Clone, Copy)]
struct Limits {
    grace: Duration,
    start_delay: Duration,
    failure_wait: Duration,
    min_free_bytes: u64,
    watch_check: Duration,
    quiet_waiting: Duration,
    quiet_alone: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            grace: GRACE,
            start_delay: START_DELAY,
            failure_wait: FAILURE_WAIT,
            min_free_bytes: MIN_FREE_BYTES,
            watch_check: WATCH_CHECK,
            quiet_waiting: QUIET_WAITING,
            quiet_alone: QUIET_ALONE,
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
}

#[derive(Default)]
struct State {
    by_session: HashMap<String, Registration>,
    by_set: HashMap<Uuid, Pass>,
    /// (item id, device id) to the set whose pictures that device gets.
    selection: HashMap<(Uuid, String), Uuid>,
    /// By stream, not set: a dead link on one version does not hold back
    /// another version of the same set.
    failed_at: HashMap<Uuid, Instant>,
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
    /// The file's last pass failed less than 10 minutes ago.
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

/// How a job's turn in the slot ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotEnd {
    /// The pass ran to its end, failed, or had nothing to do.
    Over,
    Cancelled,
    /// Nobody has watched it lately: it waits to be watched again.
    StoodDown,
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

    fn mark_failed(&self, stream_id: Uuid, at: Instant) {
        self.lock()
            .failed_at
            .insert(stream_id, at);
    }

    fn failed_recently(&self, stream_id: Uuid, now: Instant) -> bool {
        let wait = self
            .inner
            .limits
            .failure_wait;
        let mut st = self.lock();
        st.failed_at
            .retain(|_, at| now.saturating_duration_since(*at) < wait);
        st.failed_at
            .contains_key(&stream_id)
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
    /// on this item, the set is complete or the file failed recently.
    ///
    /// `watch` tells, for a device and an item, whether that device's latest
    /// reported session is on that item, how long ago it reported and where
    /// it was. The pass asks it while it waits and while it runs.
    pub(crate) async fn begin<R, W>(
        &self,
        play_session_id: &str,
        plan: PassPlan,
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
        if self.failed_recently(plan.stream_id, Instant::now()) {
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
    /// itself.
    async fn run_job<R, W>(
        &self,
        plan: PassPlan,
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
                .run_in_slot(&plan, runner.as_ref(), &mut cancel, pid.clone(), &watch)
                .await;
            drop(permit);
            if end != SlotEnd::StoodDown {
                break;
            }
            stood_down = true;
        }
        self.job_ended(set_id, job_id);
    }

    /// Runs a pass while holding a slot, so prune never races a pass. Every
    /// `watch_check` it asks whether to give way (see [`Self::gives_way`]);
    /// if so it stops the run, which then counts as cancelled.
    async fn run_in_slot<R, W>(
        &self,
        plan: &PassPlan,
        runner: &R,
        cancel: &mut oneshot::Receiver<()>,
        pid: Arc<AtomicU32>,
        watch: &W,
    ) -> SlotEnd
    where
        R: PassRunner,
        W: Fn(&str, Uuid) -> Option<Watched>,
    {
        let dir = set_dir(&plan.root, plan.set_id);
        if is_done(&dir) || self.failed_recently(plan.stream_id, Instant::now()) {
            return SlotEnd::Over;
        }
        // Switched off while this pass waited: start nothing.
        if let Some(pool) = &plan.db
            && !enabled(&db::Settings::get_config_or_default(pool).await)
        {
            return SlotEnd::Over;
        }
        let active: HashSet<Uuid> = self
            .lock()
            .by_set
            .keys()
            .copied()
            .collect();
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
            self.mark_failed(plan.stream_id, Instant::now());
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
        info!(
            set_id = %plan.set_id,
            stream_id = %plan.stream_id,
            "making trick play pictures"
        );
        let job = PassJob {
            input: plan
                .input
                .clone(),
            out_dir: dir.clone(),
            pid,
            min_free_bytes,
        };
        let watch_check = self
            .inner
            .limits
            .watch_check;
        let (stop_tx, stop_rx) = oneshot::channel();
        let mut stop = Some(stop_tx);
        let mut end = SlotEnd::Over;
        let run = runner.run(job, stop_rx);
        tokio::pin!(run);
        let result = loop {
            tokio::select! {
                result = &mut run => break result,
                _ = &mut *cancel, if stop.is_some() => {
                    end = SlotEnd::Cancelled;
                    if let Some(stop) = stop.take() {
                        let _ = stop.send(());
                    }
                }
                _ = tokio::time::sleep(watch_check), if stop.is_some() => {
                    if let Some(heard) = self.gives_way(plan.set_id, watch) {
                        info!(
                            set_id = %plan.set_id,
                            stream_id = %plan.stream_id,
                            quiet_s = heard.quiet.as_secs(),
                            position_s = heard.position_s,
                            "trick play pass giving way: nobody has watched it lately"
                        );
                        end = SlotEnd::StoodDown;
                        if let Some(stop) = stop.take() {
                            let _ = stop.send(());
                        }
                    }
                }
            }
        };
        self.finish(plan, &dir, &result);
        if result.cancelled { end } else { SlotEnd::Over }
    }

    fn finish(&self, plan: &PassPlan, dir: &Path, result: &RunResult) {
        let last_made = result
            .made_sheets
            .checked_sub(1)
            .and_then(|n| u32::try_from(n).ok());
        match outcome(result, plan.pictures) {
            Outcome::Complete => {
                let moved = moved_if_longer(plan, dir, result.made_sheets);
                if moved.as_path() != dir {
                    // The set's folder is gone: its plays follow their own
                    // pick, which finds this stream's folder.
                    self.lock()
                        .selection
                        .retain(|_, s| *s != plan.set_id);
                }
                let dir = moved;
                if let Err(e) = std::fs::write(dir.join(DONE_MARKER), b"") {
                    warn!(error = %e, "could not mark trick play pictures complete");
                }
                info!(
                    set_id = %plan.set_id,
                    stream_id = %plan.stream_id,
                    sheets = result.made_sheets,
                    "trick play pictures complete"
                );
            }
            Outcome::Cancelled => {
                // A stopped pass is killed, and each sheet is renamed into
                // place once whole, so every sheet on disk is good, unless
                // ffmpeg had already reached its end and flushed a partly
                // filled one.
                if result.ended_itself
                    && let Some(n) = last_made
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
            Outcome::Failed => {
                // The last sheet this pass wrote may be a partly filled one
                // flushed at a false end of input.
                if let Some(n) = last_made {
                    remove_sheet(dir, n);
                }
                self.mark_failed(plan.stream_id, Instant::now());
                warn!(
                    set_id = %plan.set_id,
                    stream_id = %plan.stream_id,
                    exit_ok = result.exit_ok,
                    sheets = result.made_sheets,
                    stderr = %crate::playback::probe::redact_urls_in(&result.stderr),
                    "trick play pass did not finish; not retrying for 10 minutes"
                );
            }
        }
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

/// The key check: a pass that ran to its end and wrote a sheet past the
/// last one its picture count fills read a file longer than its stored
/// length, so its pictures are not the set's. They move to a folder named by
/// the stream itself, which [`set_for`] then keeps that stream on. Gives the
/// folder the pictures are in afterwards.
fn moved_if_longer(plan: &PassPlan, dir: &Path, made_sheets: u64) -> PathBuf {
    let longer = made_sheets
        .checked_sub(1)
        .is_some_and(|highest| highest > last_sheet(plan.pictures));
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
                sheets = made_sheets,
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
            min_free_bytes: 0,
            watch_check: Duration::from_millis(10),
            quiet_waiting: Duration::from_millis(100),
            quiet_alone: Duration::from_secs(10),
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
            cap_bytes: u64::MAX,
            keep_for: Some(Duration::from_secs(30 * 24 * 60 * 60)),
            db: None,
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

    /// When each device last reported, and for which play session; `None` is
    /// "just now", whenever asked. A device not listed has no reported
    /// session on the item.
    #[derive(Clone, Default)]
    struct Reports(Arc<Mutex<HashMap<String, (Option<Instant>, String)>>>);

    impl Reports {
        fn set(&self, device_id: &str, last: Option<Instant>) {
            self.0
                .lock()
                .unwrap()
                .entry(device_id.to_string())
                .or_default()
                .0 = last;
        }

        /// Reporting all the time.
        fn fresh(&self, device_id: &str) {
            self.set(device_id, None);
        }

        /// Reporting all the time, for `play_session_id`.
        fn playing(&self, device_id: &str, play_session_id: &str) {
            self.0
                .lock()
                .unwrap()
                .insert(device_id.to_string(), (None, play_session_id.to_string()));
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
                    .map(|(last, play_session_id)| Watched {
                        quiet: last.map_or(Duration::ZERO, |at| at.elapsed()),
                        position_s: 0,
                        play_session_id: play_session_id.clone(),
                    })
            }
        }
    }

    fn write_sheets(dir: &Path, sheets: impl IntoIterator<Item = u32>) {
        std::fs::create_dir_all(dir).unwrap();
        for n in sheets {
            std::fs::write(dir.join(format!("{n}.jpg")), b"jpg").unwrap();
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

    /// Writes `sheets` sheets and exits 0, or waits to be cancelled.
    #[derive(Default)]
    struct FakeRunner {
        sheets: u64,
        wait_for_cancel: bool,
        runs: AtomicUsize,
        killed: AtomicBool,
    }

    impl PassRunner for FakeRunner {
        fn run(
            &self,
            job: PassJob,
            cancel: oneshot::Receiver<()>,
        ) -> impl Future<Output = RunResult> + Send {
            self.runs
                .fetch_add(1, Ordering::SeqCst);
            async move {
                for n in 0..self.sheets {
                    std::fs::write(
                        job.out_dir
                            .join(format!("{n}.jpg")),
                        b"jpg",
                    )
                    .unwrap();
                }
                if self.wait_for_cancel {
                    let _ = cancel.await;
                    self.killed
                        .store(true, Ordering::SeqCst);
                    return RunResult {
                        made_sheets: self.sheets,
                        cancelled: true,
                        ..Default::default()
                    };
                }
                RunResult {
                    exit_ok: true,
                    made_sheets: self.sheets,
                    ..Default::default()
                }
            }
        }
    }

    async fn until(mut check: impl FnMut() -> bool) {
        for _ in 0..400 {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("condition not reached");
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
        let args = pass_args(&input, out);
        let at = |flag: &str| {
            args.iter()
                .position(|a| a == flag)
                .unwrap_or_else(|| panic!("{flag} missing"))
        };
        assert_eq!(args[at("-skip_frame") + 1], "nokey");
        assert!(at("-skip_frame") < at("-i"));
        assert_eq!(args[at("-i") + 1], input);
        assert_eq!(args[at("-threads") + 1], "1");
        assert!(at("-reconnect") < at("-i"));
        let filter = &args[at("-vf") + 1];
        assert!(filter.contains("fps=1/10"));
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
    fn is_complete_counts_sheets() {
        // 7,200 s: 720 pictures, 29 sheets; 95% needs 28.
        let pictures = picture_count(RUNTIME as i64 * TICKS_PER_SEC);
        assert_eq!(pictures, 720);
        assert!(is_complete(29, pictures));
        assert!(is_complete(28, pictures));
        assert!(!is_complete(27, pictures));
        // 940 pictures fill 38 sheets; 95% of them needs 36.
        assert!(is_complete(36, 940));
        assert!(!is_complete(35, 940));
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

    #[test]
    fn prune_on_start_drops_last_sheet_of_unfinished_sets() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp
            .path()
            .join(ROOT_DIR);
        let unfinished = set_dir(&root, Uuid::new_v4());
        let finished = set_dir(&root, Uuid::new_v4());
        for dir in [&unfinished, &finished] {
            std::fs::create_dir_all(dir).unwrap();
            std::fs::write(dir.join("0.jpg"), b"jpeg").unwrap();
            std::fs::write(dir.join("1.jpg"), b"jpeg").unwrap();
        }
        std::fs::write(finished.join(DONE_MARKER), b"").unwrap();

        prune_on_start(&root, true, u64::MAX, None, SystemTime::now());
        assert!(
            unfinished
                .join("0.jpg")
                .exists()
        );
        assert!(
            !unfinished
                .join("1.jpg")
                .exists(),
            "a crash may have left it partly filled"
        );
        assert!(
            finished
                .join("1.jpg")
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
        passes.mark_failed(stream_id, Instant::now());
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
        passes.mark_failed(stream_a, Instant::now());

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
        write_sheets(&dir, 0..20);
        let runner = Arc::new(FakeRunner {
            sheets: 3,
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
        assert_eq!(sheets_in(&dir), (0..20).collect::<Vec<u32>>());
        assert!(!passes.failed_recently(stream_id, Instant::now()));
    }

    #[tokio::test]
    async fn a_failed_pass_drops_only_the_last_sheet_it_wrote() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::with_limits(fast());
        let stream_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), stream_id);
        write_sheets(&dir, 0..20);
        // Three sheets of 29, then a false end of input.
        let runner = Arc::new(FakeRunner {
            sheets: 3,
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
        let expected: Vec<u32> = (0..20)
            .filter(|n| *n != 2)
            .collect();
        assert_eq!(sheets_in(&dir), expected);
        assert!(
            !dir.join(DONE_MARKER)
                .exists()
        );
        assert!(passes.failed_recently(stream_id, Instant::now()));
    }

    #[test]
    fn a_pass_stopped_after_its_own_end_drops_its_last_sheet() {
        let tmp = tempfile::tempdir().unwrap();
        let passes = TrickplayPasses::new();
        let stream_id = Uuid::new_v4();
        let dir = set_dir(tmp.path(), stream_id);
        write_sheets(&dir, 0..20);
        let plan = plan(tmp.path(), stream_id);

        // Killed while running: nothing is deleted.
        passes.finish(
            &plan,
            &dir,
            &RunResult {
                made_sheets: 3,
                cancelled: true,
                ..Default::default()
            },
        );
        assert_eq!(sheets_in(&dir), (0..20).collect::<Vec<u32>>());

        // Its output had ended before the stop came: the last sheet it wrote
        // may be partly filled.
        passes.finish(
            &plan,
            &dir,
            &RunResult {
                made_sheets: 3,
                cancelled: true,
                ended_itself: true,
                ..Default::default()
            },
        );
        let expected: Vec<u32> = (0..20)
            .filter(|n| *n != 2)
            .collect();
        assert_eq!(sheets_in(&dir), expected);
        assert!(!passes.failed_recently(stream_id, Instant::now()));
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
}
