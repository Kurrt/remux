use std::{collections::HashSet, io::Cursor, path::PathBuf};

use ab_glyph::{Font, FontRef, PxScale, ScaleFont};
use futures::StreamExt;
use image::{DynamicImage, ImageFormat, Rgb, RgbImage, Rgba, RgbaImage};
use imageproc::drawing::draw_text_mut;
use resvg::usvg::roxmltree;
use uuid::Uuid;

use crate::{
    api::image::detect_content_type,
    db,
    db::ImageKind,
    sdks::remux::{
        CollectionFilter, CollectionFontFamily, CollectionFontWeight,
        CollectionImageConfig, CollectionOverlay, CollectionPosterLayout,
    },
};

/// Width/height of generated library placeholder images (16:9).
const OUT_W: u32 = 960;
const OUT_H: u32 = 540;

/// An SVG item image larger than this is served as it is, never rendered.
/// Title logos are a few kilobytes.
const SVG_MAX_BYTES: usize = 256 * 1024;
/// Longest side of a PNG rendered from an SVG. Every SVG is fitted into a
/// square of this size, wide enough for a title logo across a TV screen; the
/// size a client asks for is not used, as it is not for other images.
const SVG_MAX_SIDE: u32 = 1280;
/// Most elements an SVG may hold, counting each `<use>` as the elements it
/// copies. The text a `<use>` copies also counts towards `SVG_MAX_BYTES`,
/// because a copied path is built again from its data.
const SVG_MAX_ELEMENTS: usize = 10_000;
/// Deepest element nesting an SVG may have. The parser and renderer recurse
/// into each level.
const SVG_MAX_DEPTH: usize = 24;
/// Most namespace declarations an SVG may hold. The parser copies an
/// element's inherited namespaces into each child that declares one of its
/// own, so many of both cost far more than the file's size suggests.
const SVG_MAX_NAMESPACES: usize = 64;
/// Most `<clipPath>` elements an SVG may hold.
const SVG_MAX_CLIPS: usize = 32;
/// Most elements the clip paths of an SVG may hold between them. A clip path
/// is drawn again for each element it clips.
const SVG_MAX_CLIP_ELEMENTS: usize = 64;
/// Most `<stop>` elements a gradient may hold. A gradient fill looks through
/// its stops for every pixel.
const SVG_MAX_STOPS: usize = 64;
/// Longest chain of `href` references from one element to the next, as a
/// gradient takes what it lacks from another.
const SVG_MAX_HREF_HOPS: usize = 8;
/// Most text the `<style>` elements of an SVG may hold between them. Every
/// rule in it is tested against every element.
const SVG_MAX_STYLE_BYTES: usize = 16 * 1024;
/// Deepest element nesting an SVG with a style sheet may have. A rule is
/// tested against an element by trying its ancestors in turn for each part
/// of the rule, so the work for one rule grows steeply with the depth.
const SVG_MAX_STYLED_DEPTH: usize = 6;
/// Longest `class` value an element of an SVG with a style sheet may have.
/// A rule naming a class reads the whole value, for the element and again
/// for each ancestor it tries.
const SVG_MAX_CLASS_BYTES: usize = 64;
/// Most attributes an element of an SVG with a style sheet may have. Each
/// declaration applied to an element is looked up among its attributes.
const SVG_MAX_STYLED_ATTRIBUTES: usize = 64;
/// Most declarations a style sheet may come to once each rule has its own
/// copy of its block, counted from the text as its selectors times its
/// colons, which is never less.
const SVG_MAX_STYLE_DECLARATIONS: usize = 20_000;
/// Most a style sheet may be given to do: its bytes, or its declarations if
/// they are more, times the elements it is applied to, the ones each `<use>`
/// copies included.
const SVG_MAX_STYLE_WORK: usize = 1_000_000;
/// Most canvas the layers of one render may add up to, counted in canvases
/// of the output size. A group with opacity or a clip path is drawn on a
/// layer of its own, which can be larger than the output and stays in memory
/// while the layers inside it are drawn.
const SVG_MAX_LAYER_CANVASES: f64 = 16.0;
/// Elements whose cost the limits above do not bound (filters, masks, markers
/// and patterns, whose tile is drawn at whatever size it names), or that
/// would be dropped from the PNG (text, embedded images and documents). An
/// SVG holding one is served as it is.
const SVG_REFUSED_ELEMENTS: &[&str] = &[
    "filter",
    "feImage",
    "mask",
    "marker",
    "pattern",
    "text",
    "tref",
    "image",
    "foreignObject",
];
/// SVGs rendered at once. A render cannot be stopped once it has started, and
/// the limits above keep its cost finite, not short: an SVG built for it (a
/// long path drawn many times through `<use>`, or wide strokes) can hold a
/// slot for minutes. This bounds how many do, and a request that gets no slot
/// serves the SVG as it is.
const SVG_MAX_RENDERS: usize = 2;
/// How long a request waits for a render slot before serving the SVG as it is.
const SVG_RENDER_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

static SVG_RENDERS: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(SVG_MAX_RENDERS));

/// Black overlay opacity — matches Jellyfin's `0x78` (≈ 47%).
const OVERLAY_ALPHA: f32 = 0x78 as f32 / 255.0;

/// Maximum text width as a fraction of image width before scaling down.
const MAX_TEXT_FRACTION: f32 = 0.90;

/// Standard poster dimensions (2:3 ratio) used in fan layout.
const POSTER_W: u32 = 190;
const POSTER_H: u32 = 285;

/// A three-by-three poster sheet gives the grid enough depth to overflow the frame.
const GRID_POSTER_LIMIT: usize = 9;
/// A grid cell should use its own source poster whenever the collection has
/// enough artwork. Repeating posters makes the projected sheet look synthetic.
const GRID_SOURCE_POSTER_LIMIT: usize = GRID_POSTER_LIMIT;
const GRID_COLUMNS: usize = 3;
const GRID_POSTER_W: u32 = 260;
const GRID_POSTER_H: u32 = 390;
const GRID_GUTTER: u32 = 10;

/// Inset for text overlays, so copy does not sit flush with the image edge.
const TEXT_LEFT_MARGIN: i32 = 115;

/// Side of the square buffer used for rotation (must exceed sqrt(POSTER_W²+POSTER_H²) ≈ 342).
const ROTATION_CANVAS: u32 = 380;

/// Rounded-corner radius on each poster (pixels).
const CORNER_RADIUS: u32 = 10;

/// Left text area: left edge of posters starts here (fan occupies right ~55% of canvas).
const TEXT_AREA_END: u32 = (OUT_W as f32 * 0.42) as u32;

static FONT_BOLD: &[u8] = include_bytes!("../../assets/fonts/LiberationSans-Bold.ttf");
static FONT_ROBOTO: &[u8] = include_bytes!("../../assets/fonts/Roboto.ttf");
static FONT_OPEN_SANS: &[u8] = include_bytes!("../../assets/fonts/OpenSans.ttf");
static FONT_LATO: &[u8] = include_bytes!("../../assets/fonts/Lato.ttf");
static FONT_MONTSERRAT: &[u8] = include_bytes!("../../assets/fonts/Montserrat.ttf");
static FONT_POPPINS: &[u8] = include_bytes!("../../assets/fonts/Poppins.ttf");
static FONT_OSWALD: &[u8] = include_bytes!("../../assets/fonts/Oswald.ttf");
static FONT_RALEWAY: &[u8] = include_bytes!("../../assets/fonts/Raleway.ttf");
static FONT_MERRIWEATHER: &[u8] = include_bytes!("../../assets/fonts/Merriweather.ttf");
static FONT_PLAYFAIR_DISPLAY: &[u8] =
    include_bytes!("../../assets/fonts/PlayfairDisplay.ttf");
static FONT_BEBAS_NEUE: &[u8] = include_bytes!("../../assets/fonts/BebasNeue.ttf");

/// Glyph-coverage fallback chain for text overlays: `ab_glyph` renders a
/// whole string with one font, so a codepoint missing from the user's chosen
/// font (a symbol like ♥ that not every display font includes) draws as
/// nothing rather than falling back to a font that has it. Tried in this
/// order for any character the primary font lacks; Merriweather is first
/// since it happens to carry the widest symbol coverage of the bundled set.
/// This does not add support for color/pictographic emoji — `ab_glyph` can
/// only rasterize monochrome vector outlines, so a true emoji codepoint has
/// no glyph in any of these fonts either and still renders as nothing.
const FALLBACK_FONT_BYTES: &[&[u8]] = &[
    FONT_MERRIWEATHER,
    FONT_ROBOTO,
    FONT_OPEN_SANS,
    FONT_LATO,
    FONT_MONTSERRAT,
    FONT_POPPINS,
    FONT_OSWALD,
    FONT_RALEWAY,
    FONT_PLAYFAIR_DISPLAY,
    FONT_BEBAS_NEUE,
    FONT_BOLD,
];
static PROVIDER_LOGO_NETFLIX: &[u8] =
    include_bytes!("../../assets/provider-logos/netflix.png");
static PROVIDER_LOGO_PRIME_VIDEO: &[u8] =
    include_bytes!("../../assets/provider-logos/prime-video.png");
static PROVIDER_LOGO_DISNEY_PLUS: &[u8] =
    include_bytes!("../../assets/provider-logos/disney-plus.png");
static PROVIDER_LOGO_MAX: &[u8] = include_bytes!("../../assets/provider-logos/max.png");
static PROVIDER_LOGO_HULU: &[u8] =
    include_bytes!("../../assets/provider-logos/hulu.png");
static PROVIDER_LOGO_PARAMOUNT_PLUS: &[u8] =
    include_bytes!("../../assets/provider-logos/paramount-plus.png");
static PROVIDER_LOGO_CRUNCHYROLL: &[u8] =
    include_bytes!("../../assets/provider-logos/crunchyroll.png");
static PROVIDER_LOGO_APPLE_TV: &[u8] =
    include_bytes!("../../assets/provider-logos/apple-tv.png");
static PROVIDER_LOGO_VIAPLAY: &[u8] =
    include_bytes!("../../assets/provider-logos/viaplay.png");
static PROVIDER_LOGO_SKYSHOWTIME: &[u8] =
    include_bytes!("../../assets/provider-logos/skyshowtime.png");
static PROVIDER_LOGO_PEACOCK: &[u8] =
    include_bytes!("../../assets/provider-logos/peacock.png");
static PROVIDER_LOGO_MUBI: &[u8] =
    include_bytes!("../../assets/provider-logos/mubi.png");
static PROVIDER_LOGO_PLUTO_TV: &[u8] =
    include_bytes!("../../assets/provider-logos/pluto-tv.png");
static PROVIDER_LOGO_YOUTUBE: &[u8] =
    include_bytes!("../../assets/provider-logos/youtube.png");
// Transparent wordmarks sourced from Wikimedia Commons (see
// assets/provider-logos/commons/original/ for the full downloaded catalog
// and its raw imageinfo). Only assets that are genuinely alpha-transparent —
// verified by inspection, not just declared PNG — are curated here; TMDB's
// own logo tiles are opaque and are never used as a fallback.
static PROVIDER_LOGO_AMC_PLUS: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/amc-plus.png");
static PROVIDER_LOGO_ADN: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/adn.png");
static PROVIDER_LOGO_FUNIMATION_NOW: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/funimation-now.png");
static PROVIDER_LOGO_ILLICO_PLUS: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/illico-plus.png");
static PROVIDER_LOGO_SKY_MAIS: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/sky-mais.png");
static PROVIDER_LOGO_C_MORE: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/c-more.png");
static PROVIDER_LOGO_FOXTEL_NOW: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/foxtel-now.png");
static PROVIDER_LOGO_JIOHOTSTAR: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/jiohotstar.png");
static PROVIDER_LOGO_WETV: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/wetv.png");
static PROVIDER_LOGO_CHORKI: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/chorki.png");
static PROVIDER_LOGO_TVP_VOD: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/tvp-vod.png");
static PROVIDER_LOGO_KWELITV: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/kwelitv.png");
static PROVIDER_LOGO_VIDIO: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/vidio.png");
static PROVIDER_LOGO_VIVAMAX: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/vivamax.png");
static PROVIDER_LOGO_RUUTU: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/ruutu.png");
static PROVIDER_LOGO_SAMSUNG_TV_PLUS: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/samsung-tv-plus.png");
static PROVIDER_LOGO_CRAFTSY: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/craftsy.png");
static PROVIDER_LOGO_MX_PLAYER: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/mx-player.png");
static PROVIDER_LOGO_AXN_NOW: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/axn-now.png");
static PROVIDER_LOGO_CLARO_VIDEO: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/claro-video.png");
static PROVIDER_LOGO_DIMSUM: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/dimsum.png");
static PROVIDER_LOGO_CATCHPLAY: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/catchplay.png");
static PROVIDER_LOGO_FANATIZ: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/fanatiz.png");
static PROVIDER_LOGO_MOVISTAR_PLUS: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/movistar-plus.png");
static PROVIDER_LOGO_DISCOVERY_PLUS: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/discovery-plus.png");
static PROVIDER_LOGO_KOCOWA: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/kocowa.png");
static PROVIDER_LOGO_TOKU: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/toku.png");
static PROVIDER_LOGO_WATCHA: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/watcha.png");
static PROVIDER_LOGO_WAKANIM: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/wakanim.png");
static PROVIDER_LOGO_DPLAY: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/dplay.png");
static PROVIDER_LOGO_FILMDOO: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/filmdoo.png");
static PROVIDER_LOGO_GLOBOPLAY: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/globoplay.png");
static PROVIDER_LOGO_MTV_KATSOMO: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/mtv-katsomo.png");
static PROVIDER_LOGO_U_NEXT: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/u-next.png");
static PROVIDER_LOGO_RCTI_PLUS: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/rcti-plus.png");
static PROVIDER_LOGO_NFL_PLUS: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/nfl-plus.png");
static PROVIDER_LOGO_NASA_PLUS: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/nasa-plus.png");
static PROVIDER_LOGO_IWANTTFC: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/iwanttfc.png");
static PROVIDER_LOGO_KAPAMILYA_ONLINE_LIVE: &[u8] = include_bytes!(
    "../../assets/provider-logos/commons/curated/kapamilya-online-live.png"
);
static PROVIDER_LOGO_HAYU: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/hayu.png");
static PROVIDER_LOGO_WATCH_IT: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/watch-it.png");
static PROVIDER_LOGO_STAN: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/stan.png");
static PROVIDER_LOGO_SHUDDER: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/shudder.png");
static PROVIDER_LOGO_ROXI: &[u8] =
    include_bytes!("../../assets/provider-logos/commons/curated/roxi.png");

#[allow(clippy::incompatible_msrv)]
static HTTP_CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(|| {
        reqwest::Client::builder()
            .user_agent("remux-server/1.0")
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .expect("failed to build image http client")
    });

fn collection_font_bytes(font: CollectionFontFamily) -> &'static [u8] {
    match font {
        CollectionFontFamily::Roboto => FONT_ROBOTO,
        CollectionFontFamily::OpenSans => FONT_OPEN_SANS,
        CollectionFontFamily::Lato => FONT_LATO,
        CollectionFontFamily::Montserrat => FONT_MONTSERRAT,
        CollectionFontFamily::Poppins => FONT_POPPINS,
        CollectionFontFamily::Oswald => FONT_OSWALD,
        CollectionFontFamily::Raleway => FONT_RALEWAY,
        CollectionFontFamily::Merriweather => FONT_MERRIWEATHER,
        CollectionFontFamily::PlayfairDisplay => FONT_PLAYFAIR_DISPLAY,
        CollectionFontFamily::BebasNeue => FONT_BEBAS_NEUE,
    }
}

// ---------------------------------------------------------------------------
// Image processing options
// ---------------------------------------------------------------------------

/// Parameters controlling server-side image transformation.
#[derive(Debug, Clone, Default)]
pub struct ImageProcessOptions {
    pub fill_width: Option<u32>,
    pub fill_height: Option<u32>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub max_width: Option<u32>,
    pub max_height: Option<u32>,
    /// JPEG encode quality (0–100). `None` → default 90.
    pub quality: Option<u8>,
    /// Gaussian blur sigma in pixels.
    pub blur: Option<u32>,
    pub background_color: Option<String>,
    /// "jpg" / "jpeg" / "png". `None` → jpeg.
    pub format: Option<String>,
}

impl ImageProcessOptions {
    /// Returns true when any transformation is requested.
    ///
    /// Disabled below (always `false`): different clients request wildly
    /// different exact dimensions for the same item, and `apply_sizing`'s
    /// crop-to-fill can cut into a generated collection image's text overlay
    /// depending on which aspect ratio a given client happens to ask for
    /// (#435). Until sizing is reworked to never crop into overlay content,
    /// not resizing at all is safer than resizing inconsistently per client.
    /// The real logic is kept below, dead, for when that rework happens.
    #[allow(unreachable_code)]
    pub fn needs_processing(&self) -> bool {
        return false;
        self.fill_width
            .is_some()
            || self
                .fill_height
                .is_some()
            || self
                .width
                .is_some()
            || self
                .height
                .is_some()
            || self
                .max_width
                .is_some()
            || self
                .max_height
                .is_some()
            || self
                .quality
                .is_some()
            || self
                .blur
                .is_some()
            || self
                .background_color
                .is_some()
            || self
                .format
                .is_some()
    }

    pub fn content_type(&self) -> &'static str {
        match self
            .format
            .as_deref()
        {
            Some("png") => "image/png",
            _ => "image/jpeg",
        }
    }

    /// Stable cache key derived from source identifier + all transform params.
    fn cache_key(&self, source: &str) -> String {
        let key_data = format!(
            "v2|{}|fw={:?}|fh={:?}|w={:?}|h={:?}|mw={:?}|mh={:?}|q={:?}|bl={:?}|bg={:?}|fmt={:?}",
            source,
            self.fill_width,
            self.fill_height,
            self.width,
            self.height,
            self.max_width,
            self.max_height,
            self.quality,
            self.blur,
            self.background_color,
            self.format,
        );
        Uuid::new_v5(&Uuid::NAMESPACE_URL, key_data.as_bytes()).to_string()
    }
}

// ---------------------------------------------------------------------------
// ImageService
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ImageService;

impl ImageService {
    /// Returns the directory for a library item's local images.
    pub fn image_dir(data_dir: &std::path::Path, id: Uuid) -> PathBuf {
        data_dir
            .join("meta")
            .join("library")
            .join(id.to_string())
    }

    /// Returns the local path for a specific image type with the given extension.
    pub fn image_path(
        data_dir: &std::path::Path,
        id: Uuid,
        image_type: &str,
        ext: &str,
    ) -> PathBuf {
        Self::image_dir(data_dir, id).join(format!(
            "{}.{}",
            image_type.to_lowercase(),
            ext
        ))
    }

    /// Generate the library placeholder image, write it to disk, save the path
    /// to `media_images` in the DB, and return the bytes.
    pub async fn library_image(
        data_dir: &std::path::Path,
        id: Uuid,
        name: &str,
        db: &sqlx::SqlitePool,
    ) -> anyhow::Result<Vec<u8>> {
        // Check for an existing generated file (either jpg or png).
        for ext in ["jpg", "png"] {
            let p = Self::image_path(data_dir, id, "primary", ext);
            if p.exists() {
                return Ok(tokio::fs::read(&p).await?);
            }
        }

        let bytes = Self::generate(id, name, db, None, None, None).await?;
        let ct = detect_content_type(&bytes);
        let ext = ext_for_content_type(ct);
        let path = Self::image_path(data_dir, id, "primary", ext);
        Self::write_image_to_disk(&path, &bytes).await?;
        // INSERT OR IGNORE — don't replace if already exists (stable UUID for cache)
        sqlx::query(
            "INSERT OR IGNORE INTO media_images (id, media_id, image_type, image_index, path, width, height) VALUES (?, ?, 'primary', 0, ?, ?, ?)"
        )
        .bind(Uuid::new_v4())
        .bind(id)
        .bind(path.to_string_lossy().as_ref())
        .bind(OUT_W as i64)
        .bind(OUT_H as i64)
        .execute(db)
        .await?;

        Ok(bytes)
    }

    /// Render a collection image from draft, unsaved state — no disk write, no
    /// DB write, no cache row. `config`/`smart_filter` override whatever is
    /// currently stored on the collection, so the caller can preview an
    /// in-progress edit before it's saved. Used by the dashboard's "Update
    /// preview" action, which must never mutate the live collection.
    pub async fn generate_preview(
        id: Uuid,
        name: &str,
        db: &sqlx::SqlitePool,
        config: &CollectionImageConfig,
        smart_filter: Option<&CollectionFilter>,
    ) -> anyhow::Result<(Vec<u8>, &'static str)> {
        let bytes =
            Self::generate(id, name, db, Some(config), smart_filter, None).await?;
        let ct = detect_content_type(&bytes);
        Ok((bytes, ct))
    }

    /// Disk-cached wrapper around `synthetic_thumb`, keyed on the *source*
    /// Backdrop/Logo row ids rather than the media id: `sync_from_media`
    /// regenerates an image's id whenever a refresh replaces it, so keying
    /// on those ids means a stale composite is never served — a changed
    /// Backdrop or Logo naturally misses the old cache file and regenerates.
    /// Never registered in `media_images` — purely a cache entry, wiped like
    /// any other by the Clear Cache and Clean Cache Directory tasks. Returns the derived cache key
    /// alongside the bytes so the caller can reuse it as `process_image`'s
    /// `source_key`, giving per-size variants the same change-detection.
    pub async fn cached_synthetic_thumb(
        data_dir: &std::path::Path,
        backdrop_id: Uuid,
        logo_id: Option<Uuid>,
        backdrop_path: &str,
        logo_path: Option<&str>,
        title: &str,
    ) -> anyhow::Result<(Vec<u8>, String)> {
        let cache_key = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("synthetic-thumb|{backdrop_id}|{logo_id:?}").as_bytes(),
        )
        .to_string();
        let path = Self::cache_dir(data_dir).join(format!("{cache_key}.jpg"));
        if path.exists() {
            return Ok((tokio::fs::read(&path).await?, cache_key));
        }
        let bytes = Self::synthetic_thumb(backdrop_path, logo_path, title).await?;
        Self::write_image_to_disk(&path, &bytes).await?;
        Ok((bytes, cache_key))
    }

    /// Composite a landscape Thumb from a Backdrop and, if available, a Logo —
    /// for items with no dedicated Thumb (TMDB has no matching English
    /// title-card backdrop for them). Runs entirely in memory: no disk write,
    /// no `media_images` row. A later refresh that adds a real Thumb, or a
    /// better Backdrop/Logo, is picked up on the very next request with
    /// nothing to invalidate.
    pub async fn synthetic_thumb(
        backdrop_path: &str,
        logo_path: Option<&str>,
        title: &str,
    ) -> anyhow::Result<Vec<u8>> {
        let backdrop = if backdrop_path.contains("://") {
            Self::fetch_rgb(backdrop_path).await?
        } else {
            Self::read_rgb(backdrop_path).await?
        };
        let logo = match logo_path {
            Some(path) if path.contains("://") => Self::fetch_rgba(path)
                .await
                .ok(),
            Some(path) => Self::read_rgba(path)
                .await
                .ok(),
            None => None,
        };

        let title = title.to_string();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
            let resized = image::imageops::resize(
                &backdrop,
                OUT_W,
                OUT_H,
                image::imageops::FilterType::Lanczos3,
            );
            let dimmed = apply_dark_overlay(resized);

            match logo {
                Some(logo) => {
                    let mut canvas = DynamicImage::ImageRgb8(dimmed).into_rgba8();
                    let max_logo_h = OUT_H * 3 / 4;
                    let max_logo_w = (OUT_W as f32 * 0.9) as u32;
                    let scale = (max_logo_h as f32 / logo.height() as f32)
                        .min(max_logo_w as f32 / logo.width() as f32);
                    let lw = (logo.width() as f32 * scale) as u32;
                    let lh = (logo.height() as f32 * scale) as u32;
                    let scaled = image::imageops::resize(
                        &logo,
                        lw,
                        lh,
                        image::imageops::FilterType::Lanczos3,
                    );
                    let lx = (OUT_W.saturating_sub(lw) / 2) as i64;
                    let ly = (OUT_H.saturating_sub(lh) / 2) as i64;
                    image::imageops::overlay(&mut canvas, &scaled, lx, ly);
                    encode_jpeg_rgba(canvas)
                }
                None => encode_jpeg(draw_label(dimmed, &title)?),
            }
        })
        .await?
    }

    /// Save an uploaded image for `id`/`image_type`, write to disk, update DB.
    pub async fn save_image(
        data_dir: &std::path::Path,
        id: Uuid,
        kind: ImageKind,
        bytes: &[u8],
        db: &sqlx::SqlitePool,
    ) -> anyhow::Result<()> {
        let ext = ext_for_content_type(detect_content_type(bytes));
        let path = Self::image_path(data_dir, id, &kind.to_string(), ext);
        Self::write_image_to_disk(&path, bytes).await?;
        let (img_w, img_h) = image::load_from_memory(bytes)
            .map(|img| (img.width() as i64, img.height() as i64))
            .ok()
            .unzip();
        db::MediaImage::save(
            db,
            id,
            kind,
            path.to_string_lossy()
                .as_ref(),
            img_w,
            img_h,
        )
        .await
        .map_err(anyhow::Error::from)?;
        Ok(())
    }

    /// Delete the local image for `id`/`kind` and remove from media_images.
    pub async fn delete_image(
        data_dir: &std::path::Path,
        id: Uuid,
        kind: ImageKind,
        db: &sqlx::SqlitePool,
    ) -> anyhow::Result<()> {
        // Look up the stored path from the DB rather than reconstructing it —
        // the extension varies by format (gif, png, webp, jpg).
        if let Ok(images) = db::MediaImage::get_for_media(db, &id).await {
            if let Some(img) = images.get(kind) {
                if img
                    .path
                    .starts_with('/')
                {
                    let _ =
                        tokio::fs::remove_file(std::path::Path::new(&img.path)).await;
                }
            }
        }
        // Sweep all known extensions for this kind — previous uploads of different
        // formats leave orphan files behind, and the generated placeholder is always
        // primary.jpg regardless of what the DB row points to.
        for ext in ["jpg", "png", "gif", "webp"] {
            let _ = tokio::fs::remove_file(Self::image_path(
                data_dir,
                id,
                &kind.to_string(),
                ext,
            ))
            .await;
        }
        db::MediaImage::delete_for_type(db, id, kind)
            .await
            .map_err(anyhow::Error::from)?;
        // Touch updated_at so the synthetic image tag (derived from it) changes,
        // busting client caches that keyed on the old tag.
        let _ = sqlx::query("UPDATE media SET updated_at = ? WHERE id = ?")
            .bind(chrono::Utc::now().naive_utc())
            .bind(id)
            .execute(db)
            .await;
        Ok(())
    }

    /// Serve a locally-stored image file, returning (bytes, content_type).
    pub async fn serve_local(
        path: &PathBuf,
    ) -> anyhow::Result<(Vec<u8>, &'static str)> {
        let bytes = tokio::fs::read(path).await?;
        let ct = detect_content_type(&bytes);
        Ok((bytes, ct))
    }

    /// Directory for processed image cache.
    pub fn cache_dir(data_dir: &std::path::Path) -> PathBuf {
        data_dir
            .join("cache")
            .join("images")
    }

    /// Apply image transformations described by `opts`, returning (bytes, content_type).
    ///
    /// * If no processing is needed, the raw bytes are returned as-is.
    /// * Processed results are cached at `cache_dir()/{uuid_key}.{ext}`.
    pub async fn process_image(
        data_dir: &std::path::Path,
        bytes: Vec<u8>,
        opts: &ImageProcessOptions,
        source_key: &str,
    ) -> anyhow::Result<(Vec<u8>, &'static str)> {
        if !opts.needs_processing() {
            let ct = detect_content_type(&bytes);
            return Ok((bytes, ct));
        }

        // GIFs are always served as-is to preserve animation.
        if detect_content_type(&bytes) == "image/gif" {
            return Ok((bytes, "image/gif"));
        }

        let cache_key = opts.cache_key(source_key);
        let cache_dir = Self::cache_dir(data_dir);

        // Check both extensions — alpha auto-detection may produce PNG even when opts say JPEG.
        for (ext, ct) in [("png", "image/png"), ("jpg", "image/jpeg")] {
            let path = cache_dir.join(format!("{cache_key}.{ext}"));
            if path.exists() {
                let cached = tokio::fs::read(&path).await?;
                return Ok((cached, ct));
            }
        }

        let opts_clone = opts.clone();
        let (processed, content_type) = tokio::task::spawn_blocking(move || {
            process_image_sync(&bytes, &opts_clone)
        })
        .await??;

        let ext = if content_type == "image/png" {
            "png"
        } else {
            "jpg"
        };
        tokio::fs::create_dir_all(&cache_dir).await?;
        tokio::fs::write(cache_dir.join(format!("{cache_key}.{ext}")), &processed)
            .await?;

        Ok((processed, content_type))
    }

    /// Render an SVG item image as a PNG, for the many clients that cannot
    /// draw SVG. The image is fitted, never cropped, inside `SVG_MAX_SIDE`
    /// each way, and cached under `cache_dir()` keyed on the SVG's bytes, so
    /// a repeat request skips the render and a changed image misses the old
    /// file.
    ///
    /// Errors when the SVG is too large, is refused by `check_svg`, is
    /// malformed or draws nothing, and when no render slot frees up in time;
    /// the caller then serves it as it is. All but the last are remembered
    /// beside the cache, and `Ok(None)` means this SVG was refused before, so
    /// it is neither tried nor reported again. The last is the only error
    /// that is a `tokio::time::error::Elapsed`.
    pub async fn svg_as_png(
        data_dir: &std::path::Path,
        svg: &[u8],
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let svg_key = Uuid::new_v5(&Uuid::NAMESPACE_URL, svg);
        let dir = Self::cache_dir(data_dir);
        let path = dir.join(format!("{svg_key}.png"));
        let refused = dir.join(format!("{svg_key}.svg-refused"));
        if let Some(known) = Self::svg_seen_before(&path, &refused).await {
            return Ok(known);
        }
        if svg.len() > SVG_MAX_BYTES {
            let _ = Self::write_image_to_disk(&refused, &[]).await;
            anyhow::bail!(
                "SVG is {} bytes, over the {SVG_MAX_BYTES} byte limit",
                svg.len()
            );
        }

        let permit =
            tokio::time::timeout(SVG_RENDER_WAIT, SVG_RENDERS.acquire()).await??;
        // Another request may have rendered this SVG while this one waited.
        if let Some(known) = Self::svg_seen_before(&path, &refused).await {
            return Ok(known);
        }
        let svg = svg.to_vec();
        // A task of its own: a render cannot be stopped, so what comes of it
        // is stored even when the request has gone.
        tokio::spawn(async move {
            let rendered = tokio::task::spawn_blocking(move || render_svg_png(&svg))
                .await
                .map_err(anyhow::Error::from)
                .and_then(|r| r);
            let stored = match rendered {
                Ok(png) => {
                    if let Err(e) = Self::write_cache_file(&path, &png).await {
                        tracing::warn!(error = %e, "could not cache a PNG rendered from an SVG");
                    }
                    Ok(Some(png))
                }
                Err(e) => {
                    // Removed with the rest of the cache, so the SVG is tried
                    // again in time.
                    let _ = Self::write_image_to_disk(&refused, &[]).await;
                    Err(e)
                }
            };
            // The slot is kept until the result is stored, so a request that
            // waited for it finds that result.
            drop(permit);
            stored
        })
        .await?
    }

    /// What an earlier request left for an SVG: its PNG, or `Some(None)` when
    /// it was refused.
    async fn svg_seen_before(
        path: &std::path::Path,
        refused: &std::path::Path,
    ) -> Option<Option<Vec<u8>>> {
        if let Ok(png) = tokio::fs::read(path).await {
            return Some(Some(png));
        }
        tokio::fs::try_exists(refused)
            .await
            .unwrap_or(false)
            .then_some(None)
    }

    async fn write_image_to_disk(path: &PathBuf, bytes: &[u8]) -> anyhow::Result<()> {
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        tokio::fs::write(path, bytes).await?;
        Ok(())
    }

    /// Write a cache file under a temporary name and rename it into place, so
    /// a reader never sees a partly written file and a failed write leaves
    /// none.
    async fn write_cache_file(
        path: &std::path::Path,
        bytes: &[u8],
    ) -> anyhow::Result<()> {
        if let Some(dir) = path.parent() {
            tokio::fs::create_dir_all(dir).await?;
        }
        let mut name = path
            .file_name()
            .unwrap_or_default()
            .to_os_string();
        // Ends in `.tmp`, so the cache cleaner removes one a crash left.
        name.push(format!(".{}.tmp", Uuid::new_v4()));
        let tmp = path.with_file_name(name);
        let mut written = tokio::fs::write(&tmp, bytes).await;
        if written.is_ok() {
            written = tokio::fs::rename(&tmp, path).await;
        }
        if written.is_err() {
            let _ = tokio::fs::remove_file(&tmp).await;
        }
        Ok(written?)
    }

    /// Regenerate the collection image: delete cached file and DB row, then
    /// re-generate on next request. Call after `collection_image_config` changes.
    pub async fn invalidate_collection_image(
        data_dir: &std::path::Path,
        id: Uuid,
        db: &sqlx::SqlitePool,
    ) -> anyhow::Result<()> {
        Self::delete_image(data_dir, id, ImageKind::Primary, db).await
    }

    /// `config_override`/`smart_filter_override` take precedence over whatever
    /// is stored on the collection row when present — the preview path
    /// (`generate_preview`) supplies unsaved draft state this way, while the
    /// persisting path (`library_image`) passes `None` for both and reads the
    /// stored config as before. `background_override` similarly stands in for
    /// a locally-picked-but-not-yet-uploaded custom background.
    async fn generate(
        id: Uuid,
        name: &str,
        db: &sqlx::SqlitePool,
        config_override: Option<&CollectionImageConfig>,
        smart_filter_override: Option<&CollectionFilter>,
        background_override: Option<Vec<u8>>,
    ) -> anyhow::Result<Vec<u8>> {
        let collection = db::Media::get_by_id(db, &id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("collection {id} not found"))?;
        let config = config_override
            .cloned()
            .or_else(|| {
                collection
                    .collection_image_config
                    .clone()
            });
        let layout = config
            .as_ref()
            .map(|c| c.layout)
            .unwrap_or_default();
        let background_color = config
            .as_ref()
            .and_then(|config| {
                config
                    .background_color
                    .as_ref()
                    .and_then(|color| parse_hex_color(color.as_ref()))
            })
            .or_else(|| {
                config
                    .as_ref()
                    .and_then(|config| match &config.overlay {
                        CollectionOverlay::StreamingLogo { provider_name, .. } => {
                            provider_name
                                .as_deref()
                                .and_then(crate::services::stream_provider::StreamProvider::brand_color)
                        }
                        _ => None,
                    })
                    .and_then(parse_hex_color)
            })
            .unwrap_or([0, 0, 0]);

        // Resolve a custom background before looking up the poster grid. It is
        // the entire composition when present, so poster downloads would be
        // wasted work.
        let custom_background = if let Some(bytes) = background_override {
            image::load_from_memory(&bytes)
                .ok()
                .map(|img| img.into_rgba8())
        } else {
            let custom_background_path = config
                .as_ref()
                .and_then(|_| {
                    collection
                        .images
                        .get(ImageKind::Backdrop)
                        .map(|image| {
                            image
                                .path
                                .clone()
                        })
                });
            if let Some(path) = custom_background_path {
                let result = if path.contains("://") {
                    Self::fetch_rgba(&path).await
                } else {
                    Self::read_rgba(&path).await
                };
                result.ok()
            } else {
                None
            }
        };

        let poster_limit = if layout == CollectionPosterLayout::Grid {
            GRID_SOURCE_POSTER_LIMIT as u32
        } else {
            4u32
        };
        let poster_urls =
            if custom_background.is_none() && layout != CollectionPosterLayout::None {
                collect_poster_urls(
                    id,
                    &collection,
                    db,
                    poster_limit,
                    smart_filter_override,
                )
                .await
            } else {
                Vec::new()
            };

        if poster_urls.is_empty() && config.is_none() {
            // Fallback: single backdrop with label (original behaviour).
            let bg = match find_backdrop_url_from_collection(&collection, db).await {
                Ok(src) => {
                    let result = if src.contains("://") {
                        Self::fetch_rgb(&src).await
                    } else {
                        Self::read_rgb(&src).await
                    };
                    match result {
                        Ok(img) => {
                            let resized = image::imageops::resize(
                                &img,
                                OUT_W,
                                OUT_H,
                                image::imageops::FilterType::Lanczos3,
                            );
                            apply_dark_overlay(resized)
                        }
                        Err(_) => solid_background(),
                    }
                }
                Err(_) => solid_background(),
            };
            let img = draw_label(bg, name)?;
            return encode_jpeg(img);
        }

        let posters =
            Self::load_poster_images(&poster_urls, poster_limit as usize).await;

        // Bundled provider wordmarks are transparent and avoid TMDB's opaque app tiles.
        let logo_image: Option<RgbaImage> = match config
            .as_ref()
            .map(|c| &c.overlay)
        {
            Some(CollectionOverlay::StreamingLogo { provider_name, .. }) => {
                provider_wordmark(provider_name.as_deref())
            }
            _ => None,
        };

        // CPU-bound fan composition + PNG encode runs on the blocking pool.
        let name_owned = name.to_string();
        let config_owned = config;
        let bytes = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
            let has_custom_background = custom_background.is_some();
            let mut canvas = custom_background
                .map(|image| {
                    DynamicImage::ImageRgba8(image)
                        .resize_to_fill(
                            OUT_W,
                            OUT_H,
                            image::imageops::FilterType::Lanczos3,
                        )
                        .into_rgba8()
                })
                .unwrap_or_else(|| gradient_background(background_color));

            let max_n = if layout == CollectionPosterLayout::Grid {
                GRID_POSTER_LIMIT
            } else {
                4
            };
            let n = posters
                .len()
                .min(max_n);
            if layout != CollectionPosterLayout::None && n > 0 && !has_custom_background
            {
                if layout == CollectionPosterLayout::Grid {
                    stamp_grid(&mut canvas, &posters[..n]);
                } else {
                    let positions = layout_positions(layout, n);
                    for idx in (0..n).rev() {
                        let (cx, cy, angle_deg) = positions[idx];
                        let resized = image::imageops::resize(
                            &posters[idx],
                            POSTER_W,
                            POSTER_H,
                            image::imageops::FilterType::Lanczos3,
                        );
                        rotate_and_stamp(&mut canvas, resized, cx, cy, angle_deg);
                    }
                }
            }

            let overlay = config_owned
                .as_ref()
                .map(|c| &c.overlay)
                .unwrap_or(&CollectionOverlay::None);
            apply_overlay_sync(
                &mut canvas,
                overlay,
                &name_owned,
                logo_image,
                layout == CollectionPosterLayout::None,
            )?;

            encode_jpeg_rgba(canvas)
        })
        .await??;

        Ok(bytes)
    }

    async fn load_poster_images(urls: &[String], limit: usize) -> Vec<RgbaImage> {
        futures::stream::iter(
            urls.iter()
                .take(limit)
                .cloned()
                .map(|url| async move {
                    let img = if url.contains("://") {
                        Self::fetch_rgba(&url).await
                    } else {
                        Self::read_rgba(&url).await
                    };
                    img
                }),
        )
        // Keep source order stable while downloading up to eight images at
        // once. The old serial loop made a single generated image wait for
        // every remote poster in sequence.
        .buffered(8)
        .filter_map(|image| async move { image.ok() })
        .collect()
        .await
    }

    async fn fetch_rgb(url: &str) -> anyhow::Result<RgbImage> {
        let bytes = HTTP_CLIENT
            .get(url)
            .send()
            .await?
            .bytes()
            .await?;
        Ok(image::load_from_memory(&bytes)?.into_rgb8())
    }

    async fn read_rgb(path: &str) -> anyhow::Result<RgbImage> {
        let bytes = tokio::fs::read(path).await?;
        Ok(image::load_from_memory(&bytes)?.into_rgb8())
    }

    async fn fetch_rgba(url: &str) -> anyhow::Result<RgbaImage> {
        let bytes = HTTP_CLIENT
            .get(url)
            .send()
            .await?
            .bytes()
            .await?;
        Ok(image::load_from_memory(&bytes)?.into_rgba8())
    }

    async fn read_rgba(path: &str) -> anyhow::Result<RgbaImage> {
        let bytes = tokio::fs::read(path).await?;
        Ok(image::load_from_memory(&bytes)?.into_rgba8())
    }
}

/// Builds the query that finds a collection's own member items, for poster
/// selection. Regression: this used to filter only by `collection_media_kind`
/// with no membership scoping at all (no `parent_id`, no `filter_rules`), so
/// the generated collage pulled arbitrary movies/shows matching that kind from
/// the whole library rather than this collection's actual members. Mirrors the
/// scoping `/items?parentId=` already uses (`api/items.rs`):
/// - A "collection of collections" group container holds children via
///   `parent_id`, regardless of its own `collection_kind` — same as
///   `group_container_has_visible_content`'s walk above.
/// - A Manual collection holds members via `media_relations`; `parent` (in
///   addition to `parent_id`) is what makes `get_by_filter_inner` take that
///   join path instead of a literal `parent_id` scan.
/// - A Smart collection floats freely (no `parent_id`) and is scoped by
///   `filter_rules` alone. `smart_filter_override` lets a caller preview an
///   unsaved filter edit without it ever being persisted.
fn collection_item_filter(
    collection: &db::Media,
    smart_filter_override: Option<&CollectionFilter>,
) -> db::MediaFilter {
    match &collection.kind {
        db::MediaKind::Collection => {
            if collection.collection_media_kind
                == Some(db::CollectionMediaKind::Collection)
            {
                return db::MediaFilter {
                    kind: Some(vec![db::MediaKind::Collection]),
                    parent_id: Some(collection.id),
                    limit: Some(8),
                    ..Default::default()
                };
            }
            let kinds = match &collection.collection_media_kind {
                Some(db::CollectionMediaKind::Movie) => vec![db::MediaKind::Movie],
                Some(db::CollectionMediaKind::Series) => vec![db::MediaKind::Series],
                Some(db::CollectionMediaKind::Episode) => vec![db::MediaKind::Episode],
                Some(db::CollectionMediaKind::Mixed) => {
                    vec![db::MediaKind::Movie, db::MediaKind::Series]
                }
                Some(db::CollectionMediaKind::Music) => {
                    vec![db::MediaKind::Album, db::MediaKind::Artist]
                }
                Some(db::CollectionMediaKind::Playlist) => {
                    vec![db::MediaKind::Playlist]
                }
                Some(db::CollectionMediaKind::Collection) => {
                    unreachable!("handled above")
                }
                None => vec![db::MediaKind::Movie, db::MediaKind::Series],
            };
            if collection.collection_kind == Some(db::CollectionKind::Manual) {
                db::MediaFilter {
                    kind: Some(kinds),
                    parent_id: Some(collection.id),
                    parent: Some(collection.clone()),
                    limit: Some(8),
                    ..Default::default()
                }
            } else {
                db::MediaFilter {
                    kind: Some(kinds),
                    filter_rules: smart_filter_override
                        .cloned()
                        .or_else(|| {
                            collection
                                .parse_smart_filter()
                                .cloned()
                        }),
                    limit: Some(8),
                    ..Default::default()
                }
            }
        }
        _ => db::MediaFilter {
            parent_id: Some(collection.id),
            limit: Some(8),
            ..Default::default()
        },
    }
}

/// Whether the collection's items differ per user (favourites or watchlist
/// rule). Its image is shared by everyone, so it must not show any one user's
/// items.
fn is_per_user(
    collection: &db::Media,
    smart_filter_override: Option<&CollectionFilter>,
) -> bool {
    smart_filter_override
        .or_else(|| collection.parse_smart_filter())
        .is_some_and(CollectionFilter::is_per_user)
}

/// Collect distinct poster image paths/URLs from collection items.
async fn collect_poster_urls(
    _id: Uuid,
    collection: &db::Media,
    db: &sqlx::SqlitePool,
    limit: u32,
    smart_filter_override: Option<&CollectionFilter>,
) -> Vec<String> {
    if is_per_user(collection, smart_filter_override) {
        return Vec::new();
    }
    // Inspect extra records so duplicate artwork does not consume a grid cell.
    let filter = db::MediaFilter {
        limit: Some(limit.saturating_mul(2)),
        ..collection_item_filter(collection, smart_filter_override)
    };
    let Ok(result) = db::Media::get_by_filter(db, &filter).await else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    result
        .records
        .iter()
        .filter_map(|m| {
            m.images
                .get(ImageKind::Primary)
                .map(|i| {
                    i.path
                        .clone()
                })
                .or_else(|| {
                    m.images
                        .get(ImageKind::Backdrop)
                        .map(|i| {
                            i.path
                                .clone()
                        })
                })
        })
        .filter(|path| seen.insert(path.clone()))
        .take(limit as usize)
        .collect()
}

/// Find a backdrop or poster URL from collection items (for the legacy single-image path).
async fn find_backdrop_url_from_collection(
    collection: &db::Media,
    db: &sqlx::SqlitePool,
) -> anyhow::Result<String> {
    if is_per_user(collection, None) {
        anyhow::bail!(
            "collection {} lists different items per user",
            collection.id
        );
    }
    let filter = db::MediaFilter {
        limit: Some(8),
        ..collection_item_filter(collection, None)
    };
    let items = db::Media::get_by_filter(db, &filter)
        .await?
        .records;

    items
        .iter()
        .find_map(|m| {
            m.images
                .get(ImageKind::Backdrop)
                .map(|i| {
                    i.path
                        .clone()
                })
        })
        .or_else(|| {
            items
                .iter()
                .find_map(|m| {
                    m.images
                        .get(ImageKind::Primary)
                        .map(|i| {
                            i.path
                                .clone()
                        })
                })
        })
        .ok_or_else(|| {
            anyhow::anyhow!("no image found for collection {}", collection.id)
        })
}

/// Poster positions (canvas center_x, center_y, clockwise angle°) for a given layout and n posters.
fn layout_positions(layout: CollectionPosterLayout, n: usize) -> Vec<(i64, i64, f32)> {
    let positions = match layout {
        // No posters are stamped for either layout — None skips them entirely,
        // and Grid is rendered as one transformed 3×3 plane by `stamp_grid`.
        CollectionPosterLayout::None | CollectionPosterLayout::Grid => Vec::new(),
        // Clean horizontal shelf, barely overlapping.
        CollectionPosterLayout::Row => match n {
            1 => vec![(720, 270, 0.0)],
            2 => vec![(645, 270, 0.0), (795, 270, 0.0)],
            3 => vec![(570, 270, 0.0), (715, 270, 0.0), (860, 270, 0.0)],
            _ => vec![
                (515, 270, 0.0),
                (638, 270, 0.0),
                (762, 270, 0.0),
                (885, 270, 0.0),
            ],
        },
        // Wide artistic scatter with bold angles and height variation.
        CollectionPosterLayout::Scatter => match n {
            1 => vec![(720, 270, 0.0)],
            2 => vec![(628, 288, -14.0), (808, 248, 16.0)],
            3 => vec![(570, 298, -17.0), (718, 245, 10.0), (850, 285, -11.0)],
            _ => vec![
                (545, 302, -19.0),
                (668, 240, 13.0),
                (778, 296, -9.0),
                (882, 244, 17.0),
            ],
        },
    };

    if matches!(
        layout,
        CollectionPosterLayout::Row | CollectionPosterLayout::Scatter
    ) {
        positions
            .into_iter()
            .map(|(x, y, angle)| (x + 50, y, angle))
            .collect()
    } else {
        positions
    }
}

/// Apply rounded corners (CORNER_RADIUS px) to an RGBA image in-place.
fn apply_rounded_corners(img: &mut RgbaImage) {
    let w = img.width();
    let h = img.height();
    let r = CORNER_RADIUS as f32;
    for py in 0..h {
        for px in 0..w {
            let fx = px as f32;
            let fy = py as f32;
            let in_corner = (fx < r && fy < r)
                || (fx > w as f32 - r - 1.0 && fy < r)
                || (fx < r && fy > h as f32 - r - 1.0)
                || (fx > w as f32 - r - 1.0 && fy > h as f32 - r - 1.0);
            if in_corner {
                let cx = if fx < r { r } else { w as f32 - r - 1.0 };
                let cy = if fy < r { r } else { h as f32 - r - 1.0 };
                if ((fx - cx).powi(2) + (fy - cy).powi(2)).sqrt() > r {
                    img.get_pixel_mut(px, py)[3] = 0;
                }
            }
        }
    }
}

/// Build a non-overlapping 3×3 poster sheet, then project it as a single
/// skewed plane. The plane starts beyond the text area and is intentionally
/// larger than the output canvas, so it reads as a background element.
fn stamp_grid(canvas: &mut RgbaImage, posters: &[RgbaImage]) {
    let (grid_w, grid_h) = grid_dimensions();
    let mut grid = RgbaImage::new(grid_w, grid_h);

    let prepared_posters: Vec<RgbaImage> = posters
        .iter()
        .map(|poster| {
            let mut resized = image::imageops::resize(
                poster,
                GRID_POSTER_W,
                GRID_POSTER_H,
                image::imageops::FilterType::Lanczos3,
            );
            apply_rounded_corners(&mut resized);
            resized
        })
        .collect();

    // Smaller collections still need a complete background plane. Reuse their
    // prepared posters cyclically instead of resizing the same image per cell.
    for idx in 0..GRID_POSTER_LIMIT {
        let poster = &prepared_posters[idx % prepared_posters.len()];
        let col = idx % GRID_COLUMNS;
        let row = idx / GRID_COLUMNS;
        let x = col as u32 * (GRID_POSTER_W + GRID_GUTTER);
        let y = row as u32 * (GRID_POSTER_H + GRID_GUTTER);
        image::imageops::overlay(&mut grid, poster, x.into(), y.into());
    }

    // An affine projection with a subtle clockwise rotation and horizontal
    // shear. Its centre sits off the right edge; most of the 3×3 sheet is
    // therefore clipped, leaving roughly the right third of the image visible.
    let (a, b, c, d) = (0.74f32, -0.23f32, 0.23f32, 0.71f32);
    let determinant = a * d - b * c;
    let centre_x = 835.0;
    let centre_y = 320.0;
    let half_w = grid_w as f32 / 2.0;
    let half_h = grid_h as f32 / 2.0;
    let mut projected = RgbaImage::new(OUT_W, OUT_H);

    for y in 0..OUT_H {
        for x in 0..OUT_W {
            let dx = x as f32 - centre_x;
            let dy = y as f32 - centre_y;
            let src_x = (d * dx - b * dy) / determinant + half_w;
            let src_y = (-c * dx + a * dy) / determinant + half_h;
            if src_x < 0.0
                || src_y < 0.0
                || src_x >= (grid_w - 1) as f32
                || src_y >= (grid_h - 1) as f32
            {
                continue;
            }

            let x0 = src_x as u32;
            let y0 = src_y as u32;
            let x1 = x0 + 1;
            let y1 = y0 + 1;
            let tx = src_x.fract();
            let ty = src_y.fract();
            let interpolate = |p00: u8, p10: u8, p01: u8, p11: u8| {
                let top = p00 as f32 + (p10 as f32 - p00 as f32) * tx;
                let bottom = p01 as f32 + (p11 as f32 - p01 as f32) * tx;
                (top + (bottom - top) * ty) as u8
            };
            let p00 = grid
                .get_pixel(x0, y0)
                .0;
            let p10 = grid
                .get_pixel(x1, y0)
                .0;
            let p01 = grid
                .get_pixel(x0, y1)
                .0;
            let p11 = grid
                .get_pixel(x1, y1)
                .0;
            projected.put_pixel(
                x,
                y,
                Rgba([
                    interpolate(p00[0], p10[0], p01[0], p11[0]),
                    interpolate(p00[1], p10[1], p01[1], p11[1]),
                    interpolate(p00[2], p10[2], p01[2], p11[2]),
                    interpolate(p00[3], p10[3], p01[3], p11[3]),
                ]),
            );
        }
    }

    image::imageops::overlay(canvas, &projected, 0, 0);
}

fn grid_dimensions() -> (u32, u32) {
    let rows = GRID_POSTER_LIMIT.div_ceil(GRID_COLUMNS) as u32;
    (
        GRID_COLUMNS as u32 * GRID_POSTER_W + (GRID_COLUMNS as u32 - 1) * GRID_GUTTER,
        rows * GRID_POSTER_H + (rows - 1) * GRID_GUTTER,
    )
}

/// Rotate `poster` by `angle_deg` (clockwise) and stamp it onto `canvas`
/// so its centre lands at (`cx`, `cy`).
fn rotate_and_stamp(
    canvas: &mut RgbaImage,
    mut poster: RgbaImage,
    cx: i64,
    cy: i64,
    angle_deg: f32,
) {
    apply_rounded_corners(&mut poster);

    let pw = poster.width();
    let ph = poster.height();

    // Embed the poster centred in a square big enough that rotation never clips it.
    let buf_size = ROTATION_CANVAS;
    let ox = ((buf_size - pw) / 2) as i64;
    let oy = ((buf_size - ph) / 2) as i64;
    let mut buf = RgbaImage::new(buf_size, buf_size);
    image::imageops::overlay(&mut buf, &poster, ox, oy);

    // Bilinear inverse-mapping rotation (no external imageproc dep needed).
    let bx = buf_size as i32;
    let by = buf_size as i32;
    let half_x = buf_size as f32 / 2.0;
    let half_y = buf_size as f32 / 2.0;
    let theta = angle_deg * std::f32::consts::PI / 180.0;
    let cos_a = theta.cos();
    let sin_a = theta.sin();
    let mut rotated = RgbaImage::new(buf_size, buf_size);
    for oy in 0..buf_size {
        for ox in 0..buf_size {
            let fx = ox as f32 - half_x;
            let fy = oy as f32 - half_y;
            // Inverse rotation (CW angle → CCW inverse)
            let src_x = fx * cos_a + fy * sin_a + half_x;
            let src_y = -fx * sin_a + fy * cos_a + half_y;
            if src_x >= 0.0
                && src_x < (bx - 1) as f32
                && src_y >= 0.0
                && src_y < (by - 1) as f32
            {
                let x0 = src_x as u32;
                let y0 = src_y as u32;
                let x1 = (x0 + 1).min(buf_size - 1);
                let y1 = (y0 + 1).min(buf_size - 1);
                let fx2 = src_x.fract();
                let fy2 = src_y.fract();
                let p00 = buf
                    .get_pixel(x0, y0)
                    .0;
                let p10 = buf
                    .get_pixel(x1, y0)
                    .0;
                let p01 = buf
                    .get_pixel(x0, y1)
                    .0;
                let p11 = buf
                    .get_pixel(x1, y1)
                    .0;
                let interp = |a: u8, b: u8, c: u8, d: u8| -> u8 {
                    let top = a as f32 + (b as f32 - a as f32) * fx2;
                    let bot = c as f32 + (d as f32 - c as f32) * fx2;
                    (top + (bot - top) * fy2) as u8
                };
                rotated.put_pixel(
                    ox,
                    oy,
                    Rgba([
                        interp(p00[0], p10[0], p01[0], p11[0]),
                        interp(p00[1], p10[1], p01[1], p11[1]),
                        interp(p00[2], p10[2], p01[2], p11[2]),
                        interp(p00[3], p10[3], p01[3], p11[3]),
                    ]),
                );
            }
        }
    }

    let stamp_x = cx - buf_size as i64 / 2;
    let stamp_y = cy - buf_size as i64 / 2;
    image::imageops::overlay(canvas, &rotated, stamp_x, stamp_y);
}

/// Sync overlay renderer — called from inside `spawn_blocking`.
/// `logo_image` is the already-downloaded streaming logo (if any). `centered`
/// is set for [`CollectionPosterLayout::None`], where there is no poster grid
/// reserving space on one side, so the overlay is centered on the full canvas
/// instead of left-aligned in the text box next to it.
fn apply_overlay_sync(
    canvas: &mut RgbaImage,
    overlay: &CollectionOverlay,
    fallback_name: &str,
    logo_image: Option<RgbaImage>,
    centered: bool,
) -> anyhow::Result<()> {
    match overlay {
        CollectionOverlay::None => {}
        CollectionOverlay::Text {
            text,
            font_size,
            font_family,
            font_weight,
        } => {
            let label = text
                .as_deref()
                .unwrap_or(fallback_name);
            let size = (*font_size).unwrap_or(90) as f32;
            let font_bytes = collection_font_bytes(font_family.unwrap_or_default());
            let mut font = FontRef::try_from_slice(font_bytes)
                .map_err(|e| anyhow::anyhow!("font load: {e:?}"))?;
            use ab_glyph::VariableFont;
            let weight = match font_weight.unwrap_or_default() {
                CollectionFontWeight::Regular => 400.0,
                CollectionFontWeight::Bold => 700.0,
            };
            let _ = font.set_variation(b"wght", weight);
            let fallbacks = fallback_fonts(weight);

            let max_w = if centered {
                OUT_W as f32 * 0.85
            } else {
                TEXT_AREA_END as f32 - TEXT_LEFT_MARGIN as f32
            };
            let scale = PxScale::from(size);

            let lines = wrap_words(&font, scale, label, max_w);

            let sf = font.as_scaled(scale);
            let line_h = sf.ascent() - sf.descent();
            let line_gap = line_h * 0.18;
            let step = line_h + line_gap;
            let total_h = step * lines.len() as f32 - line_gap;
            let start_y = ((OUT_H as f32 - total_h) / 2.0) as i32;

            for (i, line) in lines
                .iter()
                .enumerate()
            {
                let y = start_y + (i as f32 * step) as i32;
                let x = if centered {
                    ((OUT_W as f32 - measure_text_width(&font, scale, line)) / 2.0)
                        as i32
                } else {
                    TEXT_LEFT_MARGIN
                };
                draw_text_with_fallback(
                    canvas,
                    Rgba([255, 255, 255, 255]),
                    x,
                    y,
                    scale,
                    &font,
                    &fallbacks,
                    line,
                );
            }
        }
        CollectionOverlay::StreamingLogo { .. } => {
            if let Some(logo) = logo_image {
                let max_logo_h = if centered {
                    OUT_H * 9 / 25
                } else {
                    OUT_H * 3 / 10
                };
                let max_logo_w = if centered {
                    (OUT_W as f32 * 0.7) as u32
                } else {
                    (OUT_W as f32 * 0.34) as u32
                };
                let scale = (max_logo_h as f32 / logo.height() as f32)
                    .min(max_logo_w as f32 / logo.width() as f32);
                let lw = (logo.width() as f32 * scale) as u32;
                let lh = (logo.height() as f32 * scale) as u32;
                let scaled = image::imageops::resize(
                    &logo,
                    lw,
                    lh,
                    image::imageops::FilterType::Lanczos3,
                );
                let lx = if centered {
                    (OUT_W.saturating_sub(lw) / 2) as i64
                } else {
                    TEXT_LEFT_MARGIN as i64
                };
                let ly = (OUT_H.saturating_sub(lh) / 2) as i64;
                let mut shadow = scaled.clone();
                for pixel in shadow.pixels_mut() {
                    if pixel[3] > 0 {
                        pixel[0] = 0;
                        pixel[1] = 0;
                        pixel[2] = 0;
                        pixel[3] = (pixel[3] as f32 * 0.35) as u8;
                    }
                }
                image::imageops::overlay(canvas, &shadow, lx + 3, ly + 3);
                image::imageops::overlay(canvas, &scaled, lx, ly);
            }
        }
    }
    Ok(())
}

fn provider_wordmark(provider_name: Option<&str>) -> Option<RgbaImage> {
    let bytes = match provider_name?
        .to_ascii_lowercase()
        .as_str()
    {
        "netflix" => PROVIDER_LOGO_NETFLIX,
        "amazon prime video" | "prime video" => PROVIDER_LOGO_PRIME_VIDEO,
        "disney plus" | "disney+" => PROVIDER_LOGO_DISNEY_PLUS,
        "max" | "hbo max" => PROVIDER_LOGO_MAX,
        "hulu" => PROVIDER_LOGO_HULU,
        "paramount plus" | "paramount+" => PROVIDER_LOGO_PARAMOUNT_PLUS,
        "crunchyroll" => PROVIDER_LOGO_CRUNCHYROLL,
        "apple tv plus" | "apple tv+" | "apple tv" => PROVIDER_LOGO_APPLE_TV,
        "viaplay" => PROVIDER_LOGO_VIAPLAY,
        "skyshowtime" => PROVIDER_LOGO_SKYSHOWTIME,
        "peacock" => PROVIDER_LOGO_PEACOCK,
        "mubi" => PROVIDER_LOGO_MUBI,
        "pluto tv" => PROVIDER_LOGO_PLUTO_TV,
        "youtube" | "youtube premium" => PROVIDER_LOGO_YOUTUBE,
        "amc+" | "amc plus" => PROVIDER_LOGO_AMC_PLUS,
        "adn" | "animation digital network" => PROVIDER_LOGO_ADN,
        "funimation now" | "funimation" => PROVIDER_LOGO_FUNIMATION_NOW,
        "illico+" | "illico plus" => PROVIDER_LOGO_ILLICO_PLUS,
        "sky mais" | "skymais" => PROVIDER_LOGO_SKY_MAIS,
        "c more" | "cmore" => PROVIDER_LOGO_C_MORE,
        "foxtel now" => PROVIDER_LOGO_FOXTEL_NOW,
        "jiohotstar" => PROVIDER_LOGO_JIOHOTSTAR,
        "wetv" => PROVIDER_LOGO_WETV,
        "chorki" => PROVIDER_LOGO_CHORKI,
        "tvp vod" => PROVIDER_LOGO_TVP_VOD,
        "kwelitv" => PROVIDER_LOGO_KWELITV,
        "vidio" => PROVIDER_LOGO_VIDIO,
        "vivamax" => PROVIDER_LOGO_VIVAMAX,
        "ruutu" => PROVIDER_LOGO_RUUTU,
        "samsung tv plus" => PROVIDER_LOGO_SAMSUNG_TV_PLUS,
        "craftsy" => PROVIDER_LOGO_CRAFTSY,
        "mx player" => PROVIDER_LOGO_MX_PLAYER,
        "axn now" | "axnnow" => PROVIDER_LOGO_AXN_NOW,
        "claro video" => PROVIDER_LOGO_CLARO_VIDEO,
        "dimsum" => PROVIDER_LOGO_DIMSUM,
        "catchplay+" | "catchplay" => PROVIDER_LOGO_CATCHPLAY,
        "fanatiz" => PROVIDER_LOGO_FANATIZ,
        "movistar plus+" | "movistar+" | "movistar plus" => PROVIDER_LOGO_MOVISTAR_PLUS,
        "discovery+" | "discovery plus" => PROVIDER_LOGO_DISCOVERY_PLUS,
        "kocowa" => PROVIDER_LOGO_KOCOWA,
        "toku" => PROVIDER_LOGO_TOKU,
        "watcha" => PROVIDER_LOGO_WATCHA,
        "wakanim" => PROVIDER_LOGO_WAKANIM,
        "dplay" => PROVIDER_LOGO_DPLAY,
        "filmdoo" => PROVIDER_LOGO_FILMDOO,
        "globoplay" => PROVIDER_LOGO_GLOBOPLAY,
        "mtv katsomo" => PROVIDER_LOGO_MTV_KATSOMO,
        "u-next" | "unext" => PROVIDER_LOGO_U_NEXT,
        "rcti+" | "rcti plus" => PROVIDER_LOGO_RCTI_PLUS,
        "nfl+" | "nfl plus" => PROVIDER_LOGO_NFL_PLUS,
        "nasa+" | "nasa plus" => PROVIDER_LOGO_NASA_PLUS,
        "iwanttfc" | "iwant" => PROVIDER_LOGO_IWANTTFC,
        "kapamilya online live" => PROVIDER_LOGO_KAPAMILYA_ONLINE_LIVE,
        "hayu" => PROVIDER_LOGO_HAYU,
        "watch it" => PROVIDER_LOGO_WATCH_IT,
        "stan" => PROVIDER_LOGO_STAN,
        "shudder" => PROVIDER_LOGO_SHUDDER,
        "roxi" => PROVIDER_LOGO_ROXI,
        _ => return None,
    };
    image::load_from_memory(bytes)
        .ok()
        .map(|image| image.into_rgba8())
}

/// Break `text` into lines where each line fits within `max_w` pixels at `scale`.
fn wrap_words(
    font: &FontRef<'_>,
    scale: PxScale,
    text: &str,
    max_w: f32,
) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        let candidate = if current.is_empty() {
            word.to_string()
        } else {
            format!("{current} {word}")
        };
        if !current.is_empty() && measure_text_width(font, scale, &candidate) > max_w {
            lines.push(current);
            current = word.to_string();
        } else {
            current = candidate;
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    if lines.is_empty() {
        lines.push(text.to_string());
    }
    lines
}

fn solid_background() -> RgbImage {
    RgbImage::from_pixel(OUT_W, OUT_H, Rgb([0, 0, 0]))
}

fn parse_hex_color(value: &str) -> Option<[u8; 3]> {
    let value = value.strip_prefix('#')?;
    if value.len() != 6 {
        return None;
    }
    Some([
        u8::from_str_radix(&value[0..2], 16).ok()?,
        u8::from_str_radix(&value[2..4], 16).ok()?,
        u8::from_str_radix(&value[4..6], 16).ok()?,
    ])
}

fn gradient_background(color: [u8; 3]) -> RgbaImage {
    let mut image = RgbaImage::new(OUT_W, OUT_H);
    for y in 0..OUT_H {
        let t = y as f32 / (OUT_H.saturating_sub(1)) as f32;
        let factor = 0.7 + t * 0.3;
        let pixel = Rgba([
            (color[0] as f32 * factor) as u8,
            (color[1] as f32 * factor) as u8,
            (color[2] as f32 * factor) as u8,
            255,
        ]);
        for x in 0..OUT_W {
            image.put_pixel(x, y, pixel);
        }
    }
    image
}

/// Blend a semi-transparent black overlay over the image to darken it,
/// matching Jellyfin's `SKColors.Black.WithAlpha(0x78)` step.
fn apply_dark_overlay(mut img: RgbImage) -> RgbImage {
    let inv = 1.0 - OVERLAY_ALPHA;
    for pixel in img.pixels_mut() {
        pixel[0] = (pixel[0] as f32 * inv) as u8;
        pixel[1] = (pixel[1] as f32 * inv) as u8;
        pixel[2] = (pixel[2] as f32 * inv) as u8;
    }
    img
}

/// Render the library name centered on the image in white text.
fn draw_label(mut img: RgbImage, name: &str) -> anyhow::Result<RgbImage> {
    let font = FontRef::try_from_slice(FONT_BOLD)
        .map_err(|e| anyhow::anyhow!("font load failed: {e:?}"))?;

    // Start at ~20% of image height and scale down until it fits within 90% width.
    let mut scale = PxScale::from(OUT_H as f32 * 0.20);
    let max_width = OUT_W as f32 * MAX_TEXT_FRACTION;
    let mut tw = measure_text_width(&font, scale, name);
    if tw > max_width {
        scale = PxScale::from(scale.x * max_width / tw);
        tw = measure_text_width(&font, scale, name);
    }

    let text_height = {
        let sf = font.as_scaled(scale);
        sf.ascent() - sf.descent()
    };

    let x = ((OUT_W as f32 - tw) / 2.0) as i32;
    let y = ((OUT_H as f32 - text_height) / 2.0) as i32;

    draw_text_mut(&mut img, Rgb([255, 255, 255]), x, y, scale, &font, name);

    Ok(img)
}

fn measure_text_width(font: &FontRef<'_>, scale: PxScale, text: &str) -> f32 {
    let scaled = font.as_scaled(scale);
    let mut width = 0.0f32;
    let mut prev: Option<ab_glyph::GlyphId> = None;
    for c in text.chars() {
        let glyph_id = scaled.glyph_id(c);
        if let Some(p) = prev {
            width += scaled.kern(p, glyph_id);
        }
        width += scaled.h_advance(glyph_id);
        prev = Some(glyph_id);
    }
    width
}

/// Loads the fallback chain (see `FALLBACK_FONT_BYTES`) at the given variable
/// weight, matching whatever the primary font was set to. Built fresh per
/// call — cheap, since this only parses font headers, not glyph outlines.
fn fallback_fonts(weight: f32) -> Vec<FontRef<'static>> {
    use ab_glyph::VariableFont;
    FALLBACK_FONT_BYTES
        .iter()
        .filter_map(|bytes| FontRef::try_from_slice(bytes).ok())
        .map(|mut font| {
            let _ = font.set_variation(b"wght", weight);
            font
        })
        .collect()
}

/// Whether `font` has a real glyph for `c`. `ab_glyph` returns glyph id 0
/// (`.notdef`) for anything outside the font's coverage, which is otherwise
/// indistinguishable from a legitimately empty glyph.
fn font_covers(font: &FontRef<'_>, c: char) -> bool {
    c.is_whitespace()
        || font
            .glyph_id(c)
            .0
            != 0
}

/// Draws `text` at `(x, y)`, substituting the first fallback font that has a
/// given character instead of leaving it blank when `primary` doesn't cover
/// it — e.g. a symbol like ♥ that only some of the bundled fonts include.
/// Splits `text` into runs by resolved font and draws each run separately;
/// kerning across a font switch is not applied, a minor cosmetic gap next to
/// an already-uncommon fallback character. This does not add support for
/// color/pictographic emoji: `ab_glyph` only rasterizes monochrome vector
/// outlines, so a true emoji codepoint has no glyph in any of these fonts
/// either and still renders as nothing.
fn draw_text_with_fallback<'f>(
    canvas: &mut RgbaImage,
    color: Rgba<u8>,
    x: i32,
    y: i32,
    scale: PxScale,
    primary: &'f FontRef<'f>,
    fallbacks: &'f [FontRef<'f>],
    text: &str,
) {
    let font_for = |c: char| -> &'f FontRef<'f> {
        if font_covers(primary, c) {
            return primary;
        }
        fallbacks
            .iter()
            .find(|f| font_covers(f, c))
            .unwrap_or(primary)
    };

    let mut cursor_x = x as f32;
    let mut run = String::new();
    let mut run_font: Option<&'f FontRef<'f>> = None;

    for c in text.chars() {
        let f = font_for(c);
        let same = run_font.is_some_and(|rf| std::ptr::eq(rf, f));
        if !same && !run.is_empty() {
            let font = run_font.unwrap();
            draw_text_mut(canvas, color, cursor_x as i32, y, scale, font, &run);
            cursor_x += measure_text_width(font, scale, &run);
            run.clear();
        }
        run_font = Some(f);
        run.push(c);
    }
    if !run.is_empty() {
        let font = run_font.unwrap();
        draw_text_mut(canvas, color, cursor_x as i32, y, scale, font, &run);
    }
}

fn ext_for_content_type(ct: &str) -> &'static str {
    match ct {
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        _ => "jpg",
    }
}

fn encode_jpeg(img: RgbImage) -> anyhow::Result<Vec<u8>> {
    let mut buf = Cursor::new(Vec::new());
    img.write_to(&mut buf, ImageFormat::Jpeg)?;
    Ok(buf.into_inner())
}

/// Generated collection art is fully opaque photographic content. JPEG avoids
/// the disproportionately expensive lossless PNG compression of a poster grid.
fn encode_jpeg_rgba(img: RgbaImage) -> anyhow::Result<Vec<u8>> {
    let mut buf = Cursor::new(Vec::new());
    let rgb = DynamicImage::ImageRgba8(img).into_rgb8();
    use image::codecs::jpeg::JpegEncoder;
    JpegEncoder::new_with_quality(&mut buf, 90)
        .encode_image(&DynamicImage::ImageRgb8(rgb))?;
    Ok(buf.into_inner())
}

// ---------------------------------------------------------------------------
// Image processing helpers (sync — run in spawn_blocking)
// ---------------------------------------------------------------------------

fn process_image_sync(
    bytes: &[u8],
    opts: &ImageProcessOptions,
) -> anyhow::Result<(Vec<u8>, &'static str)> {
    let img = image::load_from_memory(bytes)?;
    let has_alpha = img
        .color()
        .has_alpha();
    let img = apply_sizing(img, opts);
    let img = if let Some(sigma) = opts.blur {
        img.blur(sigma as f32)
    } else {
        img
    };

    // Auto-preserve transparency: use PNG when the source has alpha and the caller
    // didn't explicitly request a lossy format.
    let use_png = matches!(
        opts.format
            .as_deref(),
        Some("png")
    ) || (has_alpha
        && !matches!(
            opts.format
                .as_deref(),
            Some("jpeg" | "jpg")
        ));

    let quality = opts
        .quality
        .unwrap_or(90);
    let mut buf = Cursor::new(Vec::<u8>::new());
    if use_png {
        img.write_to(&mut buf, ImageFormat::Png)?;
        Ok((buf.into_inner(), "image/png"))
    } else {
        use image::codecs::jpeg::JpegEncoder;
        img.write_with_encoder(JpegEncoder::new_with_quality(&mut buf, quality))?;
        Ok((buf.into_inner(), "image/jpeg"))
    }
}

/// Refuse an SVG whose cost to parse and draw is not bounded by its size in
/// bytes, and return the parsed document, which is the one that is drawn.
/// Refused is an SVG that:
///
/// * has a document type with declarations of its own (the entities one can
///   declare expand without limit), or nests deeper than `SVG_MAX_DEPTH`;
/// * declares more than `SVG_MAX_NAMESPACES` namespaces;
/// * has a root that is not `<svg>`, or an element in `SVG_REFUSED_ELEMENTS`;
/// * has a filter or a dashed stroke, given by an attribute or by a style;
/// * has more than `SVG_MAX_STYLE_BYTES` of style sheet, or a style sheet
///   together with a selector that reads an attribute by name, nesting
///   deeper than `SVG_MAX_STYLED_DEPTH`, a class value or an element's
///   attributes past their limits, or more than `SVG_MAX_STYLE_DECLARATIONS`
///   or `SVG_MAX_STYLE_WORK` for it to do;
/// * has a gradient with more than `SVG_MAX_STOPS` stops;
/// * has more than `SVG_MAX_CLIPS` clip paths, more than
///   `SVG_MAX_CLIP_ELEMENTS` elements in them, one sized by the element it
///   clips, or one whose contents could be clipped in turn: they hold a
///   `<use>` or a `url(...)` reference, an element round the clip path holds
///   such a reference, or a style sheet sets a clip path;
/// * has two elements with one id;
/// * chains `href` references in a loop, or more than `SVG_MAX_HREF_HOPS`
///   long;
/// * has a `<use>` whose target cannot be told for sure, or holds a `<use>`;
/// * has more than `SVG_MAX_ELEMENTS` elements or `SVG_MAX_BYTES` bytes once
///   each `<use>` is expanded.
fn check_svg(text: &str) -> anyhow::Result<roxmltree::Document<'_>> {
    // The XML parser recurses once per level of nesting, so the depth is
    // measured first, without it. This also refuses a document type with
    // declarations, the only place entities can be declared; a plain
    // doctype, common in exported logos, passes.
    svg_depth_within(text, SVG_MAX_DEPTH)?;
    // Counted in the text: a declaration needs the attribute name itself.
    let namespaces = text
        .matches("xmlns")
        .count();
    anyhow::ensure!(
        namespaces <= SVG_MAX_NAMESPACES,
        "SVG declares too many namespaces"
    );
    let mut parsing = roxmltree::ParsingOptions::default();
    parsing.allow_dtd = true;
    // Elements, plus the text and comments between them.
    parsing.nodes_limit = 2 * SVG_MAX_ELEMENTS as u32;
    let doc = roxmltree::Document::parse_with_options(text, parsing)?;
    anyhow::ensure!(
        doc.root_element()
            .has_tag_name(("http://www.w3.org/2000/svg", "svg")),
        "not an SVG document"
    );
    check_svg_elements(&doc, text.len())?;
    Ok(doc)
}

/// The rules of `check_svg` that are read from the parsed elements. `bytes`
/// is the length of the SVG's text.
fn check_svg_elements(doc: &roxmltree::Document, bytes: usize) -> anyhow::Result<()> {
    let mut by_id = std::collections::HashMap::new();
    // Elements that refer to another by `href`: their ids, and the ids they
    // name.
    let mut chained: Vec<(Vec<&str>, Vec<&str>)> = Vec::new();
    let (mut elements, mut clips, mut clip_elements) = (0, 0, 0);
    let (mut depth, mut deepest, mut in_clip) = (0, 0, 0);
    let (mut style_bytes, mut css_clips) = (0, false);
    // What a style sheet's cost goes by: its selectors and colons, whether it
    // holds a selector that reads attributes by name, and the longest class
    // value and most attributes it meets on an element.
    let (mut style_selectors, mut style_colons, mut style_reads_attributes) =
        (0, 0, false);
    let (mut longest_class, mut most_attributes) = (0, 0);
    // Walked with a list of nodes still to visit, not by recursion: each
    // element is seen once on the way in and once on the way out.
    let mut pending = vec![(doc.root(), false)];
    while let Some((node, leaving)) = pending.pop() {
        if leaving {
            if node.is_element() {
                depth -= 1;
                if is_clip(node) {
                    in_clip -= 1;
                }
            }
            continue;
        }
        pending.push((node, true));
        pending.extend(
            node.children()
                .rev()
                .map(|child| (child, false)),
        );
        if !node.is_element() {
            continue;
        }
        depth += 1;
        deepest = deepest.max(depth);
        elements += 1;
        most_attributes = most_attributes.max(
            node.attributes()
                .len(),
        );
        longest_class = node
            .attributes()
            .filter(|a| a.name() == "class")
            .map(|a| {
                a.value()
                    .len()
            })
            .fold(longest_class, usize::max);
        // The count made before the parse saw the same levels. This one is a
        // second guard for the renderer, which recurses into them too.
        anyhow::ensure!(depth <= SVG_MAX_DEPTH, "SVG nests too deep");
        let name = node
            .tag_name()
            .name();
        anyhow::ensure!(!SVG_REFUSED_ELEMENTS.contains(&name), "SVG has a <{name}>");

        // What sets styles here: a `style` attribute, or the sheets a
        // `<style>` element holds.
        let mut styles: Vec<&str> = node
            .attribute("style")
            .into_iter()
            .collect();
        if name == "style" {
            for sheet in node
                .children()
                .filter_map(|c| c.text())
            {
                style_bytes += sheet.len();
                style_selectors += sheet
                    .matches([',', '{'])
                    .count();
                style_colons += sheet
                    .matches(':')
                    .count();
                style_reads_attributes |= sheet.contains(['[', '\\']);
                css_clips |= mentions(sheet, "clip-path");
                styles.push(sheet);
            }
            anyhow::ensure!(
                style_bytes <= SVG_MAX_STYLE_BYTES,
                "SVG has too much style text"
            );
        }
        // A filter can also be given by a function, with no element. The
        // renderer knows an attribute by its name in any of a few namespaces,
        // so the name is matched here in all of them.
        let filtered = node
            .attributes()
            .any(|a| a.name() == "filter")
            || styles
                .iter()
                .any(|s| mentions(s, "filter"));
        anyhow::ensure!(!filtered, "SVG has a filter");
        // The renderer cuts a dashed stroke into as many as a million pieces
        // a path.
        let dashed = node
            .attributes()
            .any(|a| a.name() == "stroke-dasharray" && !no_dashes(a.value()))
            || styles
                .iter()
                .any(|s| sets_dashes(s));
        anyhow::ensure!(!dashed, "SVG has a dashed stroke");
        let stops = node
            .children()
            .filter(|c| c.has_tag_name("stop"))
            .count();
        anyhow::ensure!(
            stops <= SVG_MAX_STOPS,
            "SVG has a gradient with too many stops"
        );

        if is_clip(node) {
            in_clip += 1;
            clips += 1;
            anyhow::ensure!(clips <= SVG_MAX_CLIPS, "SVG has too many clip paths");
            // One sized by the element it clips is built again for each.
            let by_target = node
                .attributes()
                .any(|a| a.name() == "clipPathUnits" && a.value() != "userSpaceOnUse");
            anyhow::ensure!(!by_target, "SVG has a clip path sized by its target");
            // With `inherit`, a clip path or what it holds takes a clip path
            // from the elements round it.
            let around = node
                .ancestors()
                .skip(1)
                .any(refers);
            anyhow::ensure!(!around, "SVG chains clip paths");
        } else if in_clip > 0 {
            clip_elements += 1;
            anyhow::ensure!(
                clip_elements <= SVG_MAX_CLIP_ELEMENTS,
                "SVG has too much in its clip paths"
            );
            // A copy could bring in an element that is clipped itself.
            anyhow::ensure!(name != "use", "SVG has a <use> in a clip path");
        }
        if in_clip > 0 {
            anyhow::ensure!(!refers(node), "SVG chains clip paths");
        }

        if let Some(id) = node.attribute("id") {
            // The renderer follows a `<use>` to the first element with its
            // id, so with two the copy could not be told for sure.
            let unique = by_id
                .insert(id, node)
                .is_none();
            anyhow::ensure!(unique, "SVG has two elements with one id");
        }
        let targets: Vec<&str> = node
            .attributes()
            .filter(|a| a.name() == "href")
            .filter_map(|a| href_id(a.value()))
            .collect();
        if !targets.is_empty() {
            let ids = node
                .attributes()
                .filter(|a| a.name() == "id")
                .map(|a| a.value())
                .collect();
            chained.push((ids, targets));
        }
    }

    // A style sheet can give a clip path, or what it holds, a clip path.
    anyhow::ensure!(
        !(css_clips && clips > 0),
        "SVG sets a clip path from a style sheet"
    );

    // The renderer follows `href` from one element to the next with no end
    // when they form a loop. Each round here drops the elements whose
    // reference ends at one with no `href`, so what is left after the last
    // is on a loop, leads into one, or is a longer chain than allowed. Every
    // id and `href` an element has is counted, in any namespace, so no chain
    // the renderer follows is missed.
    for _ in 0..SVG_MAX_HREF_HOPS {
        let linked: HashSet<&str> = chained
            .iter()
            .flat_map(|(ids, _)| ids)
            .copied()
            .collect();
        chained.retain(|(_, targets)| {
            targets
                .iter()
                .any(|id| linked.contains(id))
        });
    }
    anyhow::ensure!(
        chained.is_empty(),
        "SVG chains references in a loop or too far"
    );

    let mut bytes = bytes;
    for node in doc
        .descendants()
        .filter(|n| n.has_tag_name("use"))
    {
        let plain = node.attribute("href");
        let xlink = node.attribute(("http://www.w3.org/1999/xlink", "href"));
        anyhow::ensure!(
            plain.is_none() || xlink.is_none() || plain == xlink,
            "SVG has a <use> with two targets"
        );
        // The renderer reads `xlink:href` first.
        let Some(href) = xlink.or(plain) else {
            continue;
        };
        // It also skips spaces round a reference and ends the id at one, so
        // only a plain `#id` is sure to name the same element there as here.
        let target = href
            .strip_prefix('#')
            .filter(|id| !id.contains(char::is_whitespace))
            .and_then(|id| by_id.get(id));
        let Some(target) = target else {
            anyhow::bail!("SVG has a <use> that cannot be followed");
        };
        anyhow::ensure!(
            !target
                .descendants()
                .any(|n| n.has_tag_name("use")),
            "SVG uses a <use> inside a <use>"
        );
        elements += target
            .descendants()
            .filter(|n| n.is_element())
            .count();
        anyhow::ensure!(elements <= SVG_MAX_ELEMENTS, "SVG has too many elements");
        // A copied path is built again from its data.
        bytes += target
            .range()
            .len();
        anyhow::ensure!(
            bytes <= SVG_MAX_BYTES,
            "SVG is too large once each <use> is expanded"
        );
    }
    anyhow::ensure!(elements <= SVG_MAX_ELEMENTS, "SVG has too many elements");

    // Every rule of a style sheet is tested against every element, the
    // copies a `<use>` makes included, and through its ancestors; what
    // matches is then applied one declaration at a time. A selector that
    // reads an attribute by name could read one of any length, so a sheet
    // holding one is refused.
    if style_bytes > 0 {
        let declarations = style_selectors * style_colons;
        anyhow::ensure!(
            !style_reads_attributes
                && deepest <= SVG_MAX_STYLED_DEPTH
                && longest_class <= SVG_MAX_CLASS_BYTES
                && most_attributes <= SVG_MAX_STYLED_ATTRIBUTES
                && declarations <= SVG_MAX_STYLE_DECLARATIONS
                && style_bytes.max(declarations) * elements <= SVG_MAX_STYLE_WORK,
            "SVG's style sheet has too much to match"
        );
    }
    Ok(())
}

/// Errors when an element of `text` is nested deeper than `limit`, found by
/// one pass over the bytes with no recursion. It splits the text into
/// comments, CDATA sections, processing instructions and tags as XML does, so
/// for any text the parser accepts it counts the same levels. A document type
/// with declarations of its own is refused: they could define text that holds
/// elements.
fn svg_depth_within(text: &str, limit: usize) -> anyhow::Result<()> {
    let bytes = text.as_bytes();
    let find = |from: usize, what: &[u8]| {
        bytes
            .get(from..)
            .and_then(|rest| {
                rest.windows(what.len())
                    .position(|w| w == what)
            })
            .map(|at| from + at + what.len())
    };
    let mut depth = 0usize;
    let mut at = 0;
    while let Some(open) = find(at, b"<") {
        let rest = &bytes[open..];
        // Where this piece of markup ends. Text that stops inside one is not
        // XML, and the parser refuses it before going deeper.
        let end = if rest.starts_with(b"!--") {
            find(open + 3, b"-->")
        } else if rest.starts_with(b"![CDATA[") {
            find(open + 8, b"]]>")
        } else if rest.starts_with(b"?") {
            find(open + 1, b"?>")
        } else {
            let declaration = rest.starts_with(b"!");
            let closing = rest.starts_with(b"/");
            let mut quote = None;
            let mut end = None;
            for (offset, &byte) in rest
                .iter()
                .enumerate()
            {
                match quote {
                    Some(q) if byte == q => quote = None,
                    Some(_) => {}
                    None if byte == b'"' || byte == b'\'' => quote = Some(byte),
                    None if byte == b'[' && declaration => {
                        anyhow::bail!("SVG has a document type with declarations")
                    }
                    None if byte == b'>' => {
                        end = Some(open + offset + 1);
                        break;
                    }
                    None => {}
                }
            }
            if closing {
                depth = depth.saturating_sub(1);
            } else if !declaration {
                // An empty element is a level too, though nothing nests in
                // it.
                anyhow::ensure!(depth < limit, "SVG nests too deep");
                if !end.is_some_and(|end| bytes[end - 2] == b'/') {
                    depth += 1;
                }
            }
            end
        };
        let Some(end) = end else {
            break;
        };
        at = end;
    }
    Ok(())
}

fn is_clip(node: roxmltree::Node) -> bool {
    node.has_tag_name("clipPath")
}

/// Whether an attribute of `node` refers to another element by `url(...)`.
fn refers(node: roxmltree::Node) -> bool {
    node.attributes()
        .any(|a| {
            a.value()
                .contains("url(")
        })
}

/// The id an `href` value names in this document, read as the renderer reads
/// it: after any spaces and the `#`, up to the next space.
fn href_id(href: &str) -> Option<&str> {
    let id = href
        .trim_start_matches([' ', '\t', '\n', '\r'])
        .strip_prefix('#')?;
    id.split(' ')
        .next()
}

fn mentions(text: &str, word: &str) -> bool {
    text.to_ascii_lowercase()
        .contains(word)
}

/// Whether a `stroke-dasharray` value asks for no dashes.
fn no_dashes(value: &str) -> bool {
    value.trim() == "none"
}

/// Whether style text sets a dash pattern other than `none`.
fn sets_dashes(style: &str) -> bool {
    // Lower case keeps every byte where it was, so a place found in one is
    // the same place in the other.
    style
        .to_ascii_lowercase()
        .match_indices("stroke-dasharray")
        .any(|(at, word)| {
            let value = style[at + word.len()..]
                .split([';', '}'])
                .next()
                .unwrap_or("")
                .trim_start()
                .trim_start_matches(':');
            !no_dashes(value)
        })
}

/// Pixel size of a `width`×`height` image scaled to fit inside the box.
fn fit_inside(width: f32, height: f32, box_w: u32, box_h: u32) -> (u32, u32) {
    let scale = (box_w as f32 / width).min(box_h as f32 / height);
    let side = |v: f32| ((v * scale).round() as u32).clamp(1, SVG_MAX_SIDE);
    (side(width), side(height))
}

/// Rasterize an SVG as a PNG with its transparency, fitted inside
/// `SVG_MAX_SIDE` each way, if `check_svg` accepts it.
fn render_svg_png(svg: &[u8]) -> anyhow::Result<Vec<u8>> {
    // Read as text, so compressed SVG is refused rather than inflated.
    let text = std::str::from_utf8(svg)?;
    let doc = check_svg(text)?;
    draw_svg(&doc)
}

/// Draw a checked SVG. No `<image>` reference is followed even so, so an SVG
/// cannot make the server read a file or load another document. Errors when
/// its layers would take more canvas than `SVG_MAX_LAYER_CANVASES`, and when
/// nothing at all was drawn, rather than serve an empty PNG.
fn draw_svg(doc: &roxmltree::Document) -> anyhow::Result<Vec<u8>> {
    use resvg::{tiny_skia, usvg};

    let mut options = usvg::Options::default();
    options.image_href_resolver = usvg::ImageHrefResolver {
        resolve_data: Box::new(|_, _, _| None),
        resolve_string: Box::new(|_, _| None),
    };
    let tree = usvg::Tree::from_xmltree(doc, &options)?;
    let size = tree.size();
    let (width, height) =
        fit_inside(size.width(), size.height(), SVG_MAX_SIDE, SVG_MAX_SIDE);
    let scale_x = width as f32 / size.width();
    let scale_y = height as f32 / size.height();
    svg_layers_within(
        tree.root(),
        (f64::from(scale_x), f64::from(scale_y)),
        SVG_MAX_LAYER_CANVASES * f64::from(width) * f64::from(height),
    )?;
    let mut pixmap = tiny_skia::Pixmap::new(width, height)
        .ok_or_else(|| anyhow::anyhow!("no {width}x{height} canvas"))?;
    resvg::render(
        &tree,
        tiny_skia::Transform::from_scale(scale_x, scale_y),
        &mut pixmap.as_mut(),
    );

    anyhow::ensure!(
        pixmap
            .pixels()
            .iter()
            .any(|px| px.alpha() > 0),
        "SVG drew nothing"
    );
    // The canvas holds premultiplied alpha; PNG stores it straight.
    let rgba = pixmap
        .pixels()
        .iter()
        .flat_map(|px| {
            let c = px.demultiply();
            [c.red(), c.green(), c.blue(), c.alpha()]
        })
        .collect();
    let img = RgbaImage::from_raw(width, height, rgba)
        .ok_or_else(|| anyhow::anyhow!("canvas is not {width}x{height}"))?;
    let mut buf = Cursor::new(Vec::new());
    img.write_to(&mut buf, ImageFormat::Png)?;
    Ok(buf.into_inner())
}

/// Errors when the layers a render would draw add up to more than `budget`
/// pixels. The renderer draws a group with opacity or a clip path on a layer
/// the size of what the group holds, however far that reaches past the
/// output, and a clipped layer takes a second canvas and a mask for its clip
/// path. `scale` is the output pixels to one unit of the SVG, each way.
fn svg_layers_within(
    root: &resvg::usvg::Group,
    scale: (f64, f64),
    budget: f64,
) -> anyhow::Result<()> {
    let mut layers = 0.0;
    // Walked with a list of groups still to visit, not by recursion.
    let mut pending = vec![(root, false)];
    while let Some((group, in_clip)) = pending.pop() {
        if group.should_isolate() {
            // Its size there is not in the output's units, and it is drawn
            // for each element the clip path clips.
            anyhow::ensure!(!in_clip, "SVG has a layer inside a clip path");
            let bounds = group.abs_layer_bounding_box();
            // The renderer adds two pixels on each side.
            let across = (f64::from(bounds.width()) * scale.0).ceil() + 4.0;
            let down = (f64::from(bounds.height()) * scale.1).ceil() + 4.0;
            let clipped = group
                .clip_path()
                .is_some();
            // A clipped layer also takes a canvas for its clip path and a
            // mask.
            let canvases = if clipped { 3.0 } else { 1.0 };
            layers += canvases * across * down;
            anyhow::ensure!(layers <= budget, "SVG draws too many layers");
        }
        let mut clip = group.clip_path();
        while let Some(clipped_by) = clip {
            pending.push((clipped_by.root(), true));
            clip = clipped_by.clip_path();
        }
        for child in group.children() {
            if let resvg::usvg::Node::Group(child) = child {
                pending.push((&**child, in_clip));
            }
        }
    }
    Ok(())
}

/// Resize `img` according to sizing params (fill → exact → max priority order).
fn apply_sizing(img: DynamicImage, opts: &ImageProcessOptions) -> DynamicImage {
    let orig_w = img.width();
    let orig_h = img.height();

    // Priority 1: fill — cover the requested box, including upscaling local
    // generated art when the client asks for a larger rendition. With both
    // dimensions supplied this deliberately crops the excess so the response
    // has the exact dimensions requested by Jellyfin clients.
    if opts
        .fill_width
        .is_some()
        || opts
            .fill_height
            .is_some()
    {
        return match (opts.fill_width, opts.fill_height) {
            (Some(width), Some(height)) => {
                img.resize_to_fill(width, height, image::imageops::FilterType::Lanczos3)
            }
            (Some(width), None) => {
                img.resize(width, u32::MAX, image::imageops::FilterType::Lanczos3)
            }
            (None, Some(height)) => {
                img.resize(u32::MAX, height, image::imageops::FilterType::Lanczos3)
            }
            (None, None) => unreachable!("fill sizing requires a dimension"),
        };
    }

    // Priority 2: exact width / height (missing dimension maintains AR).
    if opts
        .width
        .is_some()
        || opts
            .height
            .is_some()
    {
        let nw = opts
            .width
            .unwrap_or(u32::MAX);
        let nh = opts
            .height
            .unwrap_or(u32::MAX);
        return img.resize(nw, nh, image::imageops::FilterType::Lanczos3);
    }

    // Priority 3: max — cap size, scale down only.
    let cap_w = opts
        .max_width
        .unwrap_or(u32::MAX);
    let cap_h = opts
        .max_height
        .unwrap_or(u32::MAX);
    if orig_w > cap_w || orig_h > cap_h {
        return img.resize(cap_w, cap_h, image::imageops::FilterType::Lanczos3);
    }

    img
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_watchlist_collection_is_not_drawn_from_anyones_items() {
        use remux_sdks::remux::{CollectionFilter, FilterGroup, FilterRule};
        let filter = |rule| CollectionFilter {
            groups: vec![FilterGroup {
                rules: vec![rule],
                ..Default::default()
            }],
            ..Default::default()
        };
        let collection = db::Media {
            collection_smart_filter: Some(filter(FilterRule::Watchlist {
                value: true,
            })),
            ..Default::default()
        };
        assert!(super::is_per_user(&collection, None));
        // A preview of an edited filter is judged by that filter instead.
        let edited = filter(FilterRule::HasTrailer { value: true });
        assert!(!super::is_per_user(&collection, Some(&edited)));
    }

    use super::*;

    #[test]
    fn grid_is_four_by_four_with_gutters() {
        assert_eq!(
            grid_dimensions(),
            (
                3 * GRID_POSTER_W + 2 * GRID_GUTTER,
                3 * GRID_POSTER_H + 2 * GRID_GUTTER,
            )
        );
    }

    #[test]
    fn fill_dimensions_upscale_and_crop_to_the_requested_size() {
        let source = DynamicImage::new_rgba8(960, 540);
        let opts = ImageProcessOptions {
            fill_width: Some(1170),
            fill_height: Some(657),
            ..Default::default()
        };

        let result = apply_sizing(source, &opts);

        assert_eq!((result.width(), result.height()), (1170, 657));
    }

    #[test]
    fn missing_streaming_logo_does_not_render_placeholder_text() {
        let mut canvas = RgbaImage::from_pixel(OUT_W, OUT_H, Rgba([18, 18, 22, 255]));
        let expected = canvas.clone();

        apply_overlay_sync(
            &mut canvas,
            &CollectionOverlay::StreamingLogo {
                provider_id: 0,
                provider_name: None,
                logo_path: None,
            },
            "Collection",
            None,
            false,
        )
        .unwrap();

        assert_eq!(canvas, expected);
    }

    #[tokio::test]
    async fn synthetic_thumb_composites_backdrop_and_logo_at_thumb_size() {
        let dir = tempfile::tempdir().unwrap();
        let backdrop_path = dir
            .path()
            .join("backdrop.jpg");
        RgbImage::from_pixel(1280, 720, Rgb([40, 60, 80]))
            .save(&backdrop_path)
            .unwrap();
        let logo_path = dir
            .path()
            .join("logo.png");
        RgbaImage::from_pixel(500, 200, Rgba([255, 255, 255, 255]))
            .save(&logo_path)
            .unwrap();

        let bytes = ImageService::synthetic_thumb(
            backdrop_path
                .to_str()
                .unwrap(),
            Some(
                logo_path
                    .to_str()
                    .unwrap(),
            ),
            "Some Title",
        )
        .await
        .unwrap();

        let decoded = image::load_from_memory(&bytes).unwrap();
        assert_eq!((decoded.width(), decoded.height()), (OUT_W, OUT_H));
        assert_eq!(detect_content_type(&bytes), "image/jpeg");
    }

    #[tokio::test]
    async fn synthetic_thumb_falls_back_to_title_text_without_a_logo() {
        let dir = tempfile::tempdir().unwrap();
        let backdrop_path = dir
            .path()
            .join("backdrop.jpg");
        RgbImage::from_pixel(1280, 720, Rgb([40, 60, 80]))
            .save(&backdrop_path)
            .unwrap();

        let bytes = ImageService::synthetic_thumb(
            backdrop_path
                .to_str()
                .unwrap(),
            None,
            "Some Title",
        )
        .await
        .unwrap();

        let decoded = image::load_from_memory(&bytes).unwrap();
        assert_eq!((decoded.width(), decoded.height()), (OUT_W, OUT_H));
    }

    #[tokio::test]
    async fn cached_synthetic_thumb_persists_under_the_image_cache_dir() {
        let source_dir = tempfile::tempdir().unwrap();
        let backdrop_path = source_dir
            .path()
            .join("backdrop.jpg");
        RgbImage::from_pixel(1280, 720, Rgb([40, 60, 80]))
            .save(&backdrop_path)
            .unwrap();

        let data_dir = tempfile::tempdir().unwrap();
        let backdrop_id = Uuid::new_v4();

        let (first, cache_key) = ImageService::cached_synthetic_thumb(
            data_dir.path(),
            backdrop_id,
            None,
            backdrop_path
                .to_str()
                .unwrap(),
            None,
            "Some Title",
        )
        .await
        .unwrap();
        let cache_path =
            ImageService::cache_dir(data_dir.path()).join(format!("{cache_key}.jpg"));
        assert!(cache_path.exists(), "composite should be written to disk");

        // Deleting the source backdrop proves a second call for the same
        // backdrop/logo ids is served from the cache file, not regenerated.
        std::fs::remove_file(&backdrop_path).unwrap();
        let (second, _) = ImageService::cached_synthetic_thumb(
            data_dir.path(),
            backdrop_id,
            None,
            backdrop_path
                .to_str()
                .unwrap(),
            None,
            "Some Title",
        )
        .await
        .unwrap();
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn cached_synthetic_thumb_regenerates_when_the_backdrop_changes() {
        let source_dir = tempfile::tempdir().unwrap();
        let backdrop_path = source_dir
            .path()
            .join("backdrop.jpg");
        RgbImage::from_pixel(1280, 720, Rgb([40, 60, 80]))
            .save(&backdrop_path)
            .unwrap();

        let data_dir = tempfile::tempdir().unwrap();

        let (_, first_key) = ImageService::cached_synthetic_thumb(
            data_dir.path(),
            Uuid::new_v4(),
            None,
            backdrop_path
                .to_str()
                .unwrap(),
            None,
            "Some Title",
        )
        .await
        .unwrap();

        // A refresh that replaces the Backdrop gets a new `media_images` row
        // id (see `sync_from_media`) — the cache key must follow it so the
        // stale composite is never served for the new artwork.
        let (_, second_key) = ImageService::cached_synthetic_thumb(
            data_dir.path(),
            Uuid::new_v4(),
            None,
            backdrop_path
                .to_str()
                .unwrap(),
            None,
            "Some Title",
        )
        .await
        .unwrap();

        assert_ne!(first_key, second_key);
    }

    /// Half filled, half transparent, twice as wide as it is tall.
    const HALF_FILLED_SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" width="200" height="100"><rect width="100" height="100" fill="#ff0000"/></svg>"##;

    const SVG_OPEN: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="10" height="10">"#;

    fn cached_pngs(data_dir: &std::path::Path) -> Vec<PathBuf> {
        std::fs::read_dir(ImageService::cache_dir(data_dir))
            .map(|dir| {
                dir.map(|e| {
                    e.unwrap()
                        .path()
                })
                .filter(|p| {
                    p.extension()
                        .is_some_and(|e| e == "png")
                })
                .collect()
            })
            .unwrap_or_default()
    }

    async fn svg_png(data_dir: &std::path::Path, svg: &str) -> Vec<u8> {
        ImageService::svg_as_png(data_dir, svg.as_bytes())
            .await
            .unwrap()
            .unwrap()
    }

    /// Clients that cannot draw SVG showed no logo when a provider sent one.
    #[tokio::test]
    async fn an_svg_is_rendered_as_a_transparent_png_fitted_to_the_size_limit() {
        let data_dir = tempfile::tempdir().unwrap();

        let png = svg_png(data_dir.path(), HALF_FILLED_SVG).await;

        assert_eq!(detect_content_type(&png), "image/png");
        let img = image::load_from_memory(&png)
            .unwrap()
            .into_rgba8();
        // The 2:1 shape is kept, never cropped, and fills the size limit.
        assert_eq!(img.dimensions(), (SVG_MAX_SIDE, SVG_MAX_SIDE / 2));
        assert_eq!(*img.get_pixel(40, 320), Rgba([255, 0, 0, 255]));
        assert_eq!(
            img.get_pixel(1240, 320)[3],
            0,
            "the empty half stays transparent"
        );
    }

    /// The size an SVG declares cannot make the server allocate a huge
    /// canvas.
    #[tokio::test]
    async fn an_svg_render_never_exceeds_the_size_limit() {
        let data_dir = tempfile::tempdir().unwrap();
        let svg = r#"<svg xmlns="http://www.w3.org/2000/svg" width="1000000" height="500000"><rect width="1000000" height="500000"/></svg>"#;

        let png = svg_png(data_dir.path(), svg).await;

        let img = image::load_from_memory(&png).unwrap();
        assert_eq!(
            (img.width(), img.height()),
            (SVG_MAX_SIDE, SVG_MAX_SIDE / 2)
        );
    }

    /// A refused SVG is remembered, so it is not parsed and reported again on
    /// every request.
    #[tokio::test]
    async fn an_oversized_or_malformed_svg_is_refused_once() {
        let data_dir = tempfile::tempdir().unwrap();
        let oversized = format!(
            "<svg xmlns=\"http://www.w3.org/2000/svg\"><!--{}--></svg>",
            " ".repeat(SVG_MAX_BYTES)
        );

        for svg in [
            oversized.as_str(),
            "<svg xmlns=\"http://www.w3.org/2000/svg\"><rect",
            "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"10\" height=\"10\"/>",
        ] {
            assert!(
                ImageService::svg_as_png(data_dir.path(), svg.as_bytes())
                    .await
                    .is_err()
            );
            assert!(
                ImageService::svg_as_png(data_dir.path(), svg.as_bytes())
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        assert!(cached_pngs(data_dir.path()).is_empty());
    }

    /// What a logo editor writes passes the check: styles, gradients, a clip
    /// path and reused shapes.
    #[test]
    fn an_ordinary_logo_passes_the_check() {
        check_svg(HALF_FILLED_SVG).unwrap();
        check_svg(&format!(
            "<?xml version=\"1.0\"?><!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \"x.dtd\">{HALF_FILLED_SVG}"
        ))
        .unwrap();
        check_svg(&format!(
            r##"{SVG_OPEN}<defs><style>.a{{fill:url(#g);}}</style><linearGradient id="g"><stop offset="0" stop-color="#fff"/><stop offset="1" stop-color="#000"/></linearGradient><clipPath id="c"><rect width="10" height="10"/></clipPath><path id="p" d="M0 0h5v5H0z"/></defs><g clip-path="url(#c)"><use xlink:href="#p" class="a"/><use href="#p" x="5"/></g></svg>"##
        ))
        .unwrap();

        // What an editor leaves in also passes, and is drawn: namespaces of
        // its own, both forms of one reference, a gradient that takes its
        // stops from another by way of a third, and a stroke said to have no
        // dashes.
        let exported = r##"<svg xmlns="http://www.w3.org/2000/svg" xmlns:svg="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:sodipodi="http://sodipodi.sourceforge.net/DTD/sodipodi-0.dtd" xmlns:inkscape="http://www.inkscape.org/namespaces/inkscape" width="10" height="10"><defs><linearGradient id="a"><stop offset="0" stop-color="#fff"/><stop offset="1" stop-color="#000"/></linearGradient><linearGradient id="b" xlink:href="#a"/><linearGradient id="c" xlink:href="#b" x2="0" y2="1"/><path id="p" d="M0 0h5v5H0z"/></defs><use href="#p" xlink:href="#p" fill="url(#c)" style="stroke-dasharray:none"/></svg>"##;
        let doc = check_svg(exported).unwrap();
        draw_svg(&doc).unwrap();
    }

    /// The nesting count sees the same levels as the XML parser, so ordinary
    /// markup and nesting right at the limit still pass the check.
    #[test]
    fn the_depth_count_reads_markup_as_xml_does() {
        // Empty elements, closing tags, quoted '>' and a plain document type
        // leave the count where the parser has it.
        let flat = format!(
            "<?xml version=\"1.0\"?><!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\">{SVG_OPEN}<!-- <g><g><g> -->{}</svg>",
            "<g class='a>b'><rect width=\"1\" height=\"1\"/><path d=\"M0 0\" /></g>"
                .repeat(500)
        );
        assert!(svg_depth_within(&flat, 3).is_ok());
        assert!(svg_depth_within(&flat, 2).is_err());
        check_svg(&flat).unwrap();

        // An empty element is a level like any other.
        let at_limit = format!(
            "{SVG_OPEN}{}<rect width=\"1\" height=\"1\"/>{}</svg>",
            "<g>".repeat(SVG_MAX_DEPTH - 2),
            "</g>".repeat(SVG_MAX_DEPTH - 2)
        );
        check_svg(&at_limit).unwrap();
        let over_limit = format!(
            "{SVG_OPEN}{}<rect width=\"1\" height=\"1\"/>{}</svg>",
            "<g>".repeat(SVG_MAX_DEPTH - 1),
            "</g>".repeat(SVG_MAX_DEPTH - 1)
        );
        assert!(check_svg(&over_limit).is_err());
        // The same groups with no leaf are exactly at the limit.
        let groups_only = format!(
            "{SVG_OPEN}{}{}</svg>",
            "<g>".repeat(SVG_MAX_DEPTH - 1),
            "</g>".repeat(SVG_MAX_DEPTH - 1)
        );
        check_svg(&groups_only).unwrap();

        // Markup that only looks like a way back up a level is not counted
        // as one.
        for level in [
            "<g><!--</g>-->",
            "<g><![CDATA[</g>]]>",
            "<g><?a </g>?>",
            "<g>a/>",
            "<g id=\"/>\">",
        ] {
            let deep = level.repeat(SVG_MAX_DEPTH + 1);
            assert!(
                svg_depth_within(&deep, SVG_MAX_DEPTH + 1).is_ok(),
                "{level}"
            );
            assert!(svg_depth_within(&deep, SVG_MAX_DEPTH).is_err(), "{level}");
        }

        // Text cut off inside a tag is left to the parser to refuse.
        assert!(svg_depth_within("<svg><g", 5).is_ok());
        assert!(check_svg("<svg xmlns=\"http://www.w3.org/2000/svg\"><g").is_err());
    }

    /// An SVG is untrusted input: one whose parse or render cost is not
    /// bounded by its size, or that would lose parts in the PNG, is refused
    /// before it is parsed for drawing, each for its own reason.
    #[test]
    fn a_hostile_or_lossy_svg_is_refused_by_the_check() {
        let nested = |levels: usize| {
            format!(
                "{SVG_OPEN}{}<rect width=\"1\" height=\"1\"/>{}</svg>",
                "<g>".repeat(levels),
                "</g>".repeat(levels)
            )
        };

        let mut use_chain =
            format!("{SVG_OPEN}<defs><rect id=\"a0\" width=\"1\" height=\"1\"/>");
        for level in 1..8 {
            use_chain += &format!("<g id=\"a{level}\">");
            for _ in 0..10 {
                use_chain += &format!("<use xlink:href=\"#a{}\"/>", level - 1);
            }
            use_chain += "</g>";
        }
        use_chain += "</defs><use href=\"#a7\"/></svg>";

        let use_fan_out = format!(
            "{SVG_OPEN}<defs><g id=\"a\">{}</g></defs>{}</svg>",
            "<rect width=\"1\" height=\"1\"/>".repeat(2_000),
            "<use href=\"#a\"/>".repeat(10)
        );

        let mut entities =
            String::from("<?xml version=\"1.0\"?><!DOCTYPE svg [<!ENTITY e0 \"lol\">");
        for level in 1..10 {
            entities += &format!(
                "<!ENTITY e{level} \"{}\">",
                format!("&e{};", level - 1).repeat(10)
            );
        }
        entities += &format!("]>{SVG_OPEN}<desc>&e9;</desc></svg>");

        // One more gradient in a row than may take from the next.
        let long_chain = (0..=SVG_MAX_HREF_HOPS)
            .map(|n| format!("<linearGradient id=\"g{n}\" href=\"#g{}\"/>", n + 1))
            .collect::<String>();
        let last_in_chain = SVG_MAX_HREF_HOPS + 1;
        // A group, and a group that holds a `<use>` of it.
        let holder = "<g id=\"r\"/><g id=\"g\"><use href=\"#r\"/></g>";

        let cases = [
            ("deep nesting", "nests too deep", nested(1_000)),
            // The XML parser would overflow the stack on this, so it also
            // holds the depth count in front of the parser.
            (
                "nesting as deep as the size limit allows",
                "nests too deep",
                format!(
                    "{SVG_OPEN}{}",
                    "<g>".repeat((SVG_MAX_BYTES - SVG_OPEN.len()) / 3)
                ),
            ),
            ("an entity bomb", "document type with declarations", entities),
            (
                "a document type with declarations",
                "document type with declarations",
                format!(
                    "<!DOCTYPE svg [<!ELEMENT svg ANY>]>{SVG_OPEN}<rect width=\"1\" height=\"1\"/></svg>"
                ),
            ),
            (
                "inherited namespaces",
                "too many namespaces",
                format!(
                    "<svg xmlns=\"http://www.w3.org/2000/svg\"{}>{}</svg>",
                    (0..300)
                        .map(|n| format!(" xmlns:p{n}=\"\""))
                        .collect::<String>(),
                    "<a xmlns:b=\"\"/>".repeat(300)
                ),
            ),
            (
                "not an SVG",
                "not an SVG document",
                "<html xmlns=\"http://www.w3.org/1999/xhtml\"/>".to_string(),
            ),
            (
                "a filter",
                "<filter>",
                format!(
                    "{SVG_OPEN}<filter id=\"f\"><feGaussianBlur stdDeviation=\"5\"/></filter><rect width=\"9\" height=\"9\" filter=\"url(#f)\"/></svg>"
                ),
            ),
            (
                "a filter function",
                "has a filter",
                format!(
                    "{SVG_OPEN}<rect width=\"9\" height=\"9\" style=\"filter: blur(5px)\"/></svg>"
                ),
            ),
            (
                "a filter in another namespace",
                "has a filter",
                format!(
                    "{SVG_OPEN}<rect width=\"9\" height=\"9\" xml:filter=\"blur(5)\"/></svg>"
                ),
            ),
            (
                "text",
                "<text>",
                format!(
                    "{SVG_OPEN}<rect width=\"9\" height=\"2\"/><text y=\"8\">Made up</text></svg>"
                ),
            ),
            (
                "an embedded image",
                "<image>",
                format!(
                    "{SVG_OPEN}<image width=\"9\" height=\"9\" href=\"data:image/png;base64,AAAA\"/></svg>"
                ),
            ),
            (
                "a pattern with a huge tile",
                "<pattern>",
                format!(
                    "{SVG_OPEN}<pattern id=\"p\" patternUnits=\"userSpaceOnUse\" width=\"100000000\" height=\"100000000\"><rect width=\"9\" height=\"9\"/></pattern><rect width=\"9\" height=\"9\" fill=\"url(#p)\"/></svg>"
                ),
            ),
            (
                "a pattern that fills itself from the element round it",
                "<pattern>",
                "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"10\" height=\"10\" fill=\"url(#p)\"><pattern id=\"p\" width=\"1\" height=\"1\"><rect width=\"1\" height=\"1\"/></pattern><rect width=\"10\" height=\"10\"/></svg>".to_string(),
            ),
            (
                "a pattern that fills itself from a style sheet",
                "<pattern>",
                format!(
                    "{SVG_OPEN}<style>svg{{fill:url(#p)}}</style><pattern id=\"p\" width=\"1\" height=\"1\"><rect width=\"1\" height=\"1\"/></pattern><rect width=\"10\" height=\"10\"/></svg>"
                ),
            ),
            (
                "a dashed stroke",
                "dashed stroke",
                format!(
                    "{SVG_OPEN}<path d=\"M0 0h9\" stroke=\"red\" stroke-dasharray=\"1\"/></svg>"
                ),
            ),
            (
                "a dashed stroke in another namespace",
                "dashed stroke",
                format!(
                    "{SVG_OPEN}<path d=\"M0 0h9\" stroke=\"red\" xml:stroke-dasharray=\"1\"/></svg>"
                ),
            ),
            (
                "a dashed stroke in a style attribute",
                "dashed stroke",
                format!(
                    "{SVG_OPEN}<path d=\"M0 0h9\" stroke=\"red\" style=\"stroke-dasharray: 1 2\"/></svg>"
                ),
            ),
            (
                "a dashed stroke in a style sheet",
                "dashed stroke",
                format!(
                    "{SVG_OPEN}<style>path{{stroke-dasharray:1}}</style><path d=\"M0 0h9\" stroke=\"red\"/></svg>"
                ),
            ),
            (
                "a long style sheet",
                "too much style text",
                format!(
                    "{SVG_OPEN}<style>{}</style></svg>",
                    ".a{}".repeat(SVG_MAX_STYLE_BYTES / 4 + 1)
                ),
            ),
            (
                "a style sheet over deep nesting",
                "too much to match",
                format!(
                    "{SVG_OPEN}<style>g g g g g g rect{{fill:red}}</style>{}<rect width=\"1\" height=\"1\"/>{}</svg>",
                    "<g>".repeat(SVG_MAX_STYLED_DEPTH),
                    "</g>".repeat(SVG_MAX_STYLED_DEPTH)
                ),
            ),
            (
                "a style sheet over many elements",
                "too much to match",
                format!(
                    "{SVG_OPEN}<style>{}</style>{}</svg>",
                    ".a{}".repeat(250),
                    "<rect width=\"1\" height=\"1\"/>".repeat(1_000)
                ),
            ),
            (
                "a style sheet matched against what each <use> copies",
                "too much to match",
                format!(
                    "{SVG_OPEN}<style>{}</style><g id=\"t\">{}</g>{}</svg>",
                    ".a{}".repeat(250),
                    "<rect width=\"1\" height=\"1\"/>".repeat(30),
                    "<use href=\"#t\"/>".repeat(40)
                ),
            ),
            (
                "a style sheet that reads an attribute by name",
                "too much to match",
                format!(
                    "{SVG_OPEN}<style>[d~=z]{{fill:red}}</style><path d=\"M0 0h9v9z\"/></svg>"
                ),
            ),
            (
                "a style sheet over a long class value",
                "too much to match",
                format!(
                    "{SVG_OPEN}<style>.z{{fill:red}}</style><rect class=\"{}\" width=\"1\" height=\"1\"/></svg>",
                    "y ".repeat(SVG_MAX_CLASS_BYTES)
                ),
            ),
            (
                "a style sheet whose rules each copy a long block",
                "too much to match",
                format!(
                    "{SVG_OPEN}<style>{}a{{{}}}</style><rect width=\"1\" height=\"1\"/></svg>",
                    "a,".repeat(200),
                    "x:1;".repeat(200)
                ),
            ),
            (
                "a gradient with many stops",
                "too many stops",
                format!(
                    "{SVG_OPEN}<linearGradient id=\"g\">{}</linearGradient></svg>",
                    "<stop offset=\"0\"/>".repeat(SVG_MAX_STOPS + 1)
                ),
            ),
            (
                "a chain of clip paths",
                "chains clip paths",
                format!(
                    "{SVG_OPEN}<clipPath id=\"a\" clip-path=\"url(#b)\"><rect width=\"9\" height=\"9\"/></clipPath><clipPath id=\"b\"><rect width=\"5\" height=\"5\"/></clipPath><rect width=\"9\" height=\"9\" clip-path=\"url(#a)\"/></svg>"
                ),
            ),
            (
                "a clip path that inherits a clip path",
                "chains clip paths",
                format!(
                    "{SVG_OPEN}<g clip-path=\"url(#b)\"><clipPath id=\"a\" clip-path=\"inherit\"><rect width=\"9\" height=\"9\"/></clipPath></g><clipPath id=\"b\"><rect width=\"5\" height=\"5\"/></clipPath><rect width=\"9\" height=\"9\" clip-path=\"url(#a)\"/></svg>"
                ),
            ),
            (
                "clip paths that reach each other through uses",
                "<use> in a clip path",
                format!(
                    "{SVG_OPEN}<rect id=\"x\" width=\"9\" height=\"9\" clip-path=\"url(#b)\"/><rect id=\"y\" width=\"9\" height=\"9\" clip-path=\"url(#c)\"/><rect id=\"z\" width=\"9\" height=\"9\" clip-path=\"url(#a)\"/><clipPath id=\"a\"><use href=\"#x\"/></clipPath><clipPath id=\"b\"><use href=\"#y\"/></clipPath><clipPath id=\"c\"><use href=\"#z\"/></clipPath></svg>"
                ),
            ),
            (
                "clip paths that reach each other through a style sheet",
                "clip path from a style sheet",
                format!(
                    "{SVG_OPEN}<style>.p{{clip-path:url(#b)}}.q{{clip-path:url(#a)}}</style><clipPath id=\"a\" class=\"p\"><rect width=\"9\" height=\"9\"/></clipPath><clipPath id=\"b\" class=\"q\"><rect width=\"9\" height=\"9\"/></clipPath><rect width=\"9\" height=\"9\" clip-path=\"url(#a)\"/></svg>"
                ),
            ),
            (
                "a clip path sized by its target",
                "sized by its target",
                format!(
                    "{SVG_OPEN}<clipPath id=\"c\" clipPathUnits=\"objectBoundingBox\"><rect width=\"1\" height=\"1\"/></clipPath></svg>"
                ),
            ),
            (
                "a clip path sized by its target, in another namespace",
                "sized by its target",
                format!(
                    "{SVG_OPEN}<clipPath id=\"c\" xlink:clipPathUnits=\"objectBoundingBox\"><rect width=\"1\" height=\"1\"/></clipPath></svg>"
                ),
            ),
            (
                "a clip path that holds a lot",
                "too much in its clip paths",
                format!(
                    "{SVG_OPEN}<clipPath id=\"c\">{}</clipPath></svg>",
                    "<rect width=\"1\" height=\"1\"/>".repeat(SVG_MAX_CLIP_ELEMENTS + 1)
                ),
            ),
            (
                "gradients that refer to each other in a loop",
                "in a loop or too far",
                format!(
                    "{SVG_OPEN}<linearGradient id=\"a\" href=\"#b\"/><linearGradient id=\"b\" href=\"#c\"/><linearGradient id=\"c\" href=\"#b\"/><rect width=\"10\" height=\"10\" fill=\"url(#a)\"/></svg>"
                ),
            ),
            (
                "a loop of gradients with ids in another namespace",
                "in a loop or too far",
                format!(
                    "{SVG_OPEN}<linearGradient id=\"a\" href=\"#b\"/><linearGradient xml:id=\"b\" href=\"#c\"/><linearGradient xml:id=\"c\" href=\"#b\"/><rect width=\"10\" height=\"10\" fill=\"url(#a)\"/></svg>"
                ),
            ),
            (
                "a gradient with stops that leads into a loop",
                "in a loop or too far",
                format!(
                    "{SVG_OPEN}<linearGradient id=\"a\" href=\"#b\"><stop offset=\"0\"/><stop offset=\"1\"/></linearGradient><linearGradient id=\"b\" href=\"#c\"/><linearGradient id=\"c\" href=\"#b\"/><rect width=\"10\" height=\"10\" fill=\"url(#a)\"/></svg>"
                ),
            ),
            (
                "a long chain of gradients",
                "in a loop or too far",
                format!(
                    "{SVG_OPEN}{long_chain}<linearGradient id=\"g{last_in_chain}\"><stop offset=\"0\"/></linearGradient></svg>"
                ),
            ),
            ("a chain of uses", "<use> inside a <use>", use_chain),
            ("a fan of uses", "too many elements", use_fan_out),
            (
                "a long path used again and again",
                "too large once each <use> is expanded",
                format!(
                    "{SVG_OPEN}<path id=\"p\" d=\"M0 0{}\"/>{}</svg>",
                    "h1v1".repeat(25_000),
                    "<use href=\"#p\"/>".repeat(2)
                ),
            ),
            (
                "a use with two targets",
                "<use> with two targets",
                format!(
                    "{SVG_OPEN}<defs>{holder}</defs><use href=\"#r\" xlink:href=\"#g\"/></svg>"
                ),
            ),
            (
                "two elements with one id",
                "two elements with one id",
                format!(
                    "{SVG_OPEN}<defs>{holder}<rect id=\"g\" width=\"1\" height=\"1\"/></defs><use href=\"#g\"/></svg>"
                ),
            ),
            (
                "a use with a space before its reference",
                "cannot be followed",
                format!("{SVG_OPEN}<defs>{holder}</defs><use href=\" #g\"/></svg>"),
            ),
            (
                "a use with a space after its reference",
                "cannot be followed",
                format!("{SVG_OPEN}<defs>{holder}</defs><use href=\"#g \"/></svg>"),
            ),
            (
                "a use of an id that ends in a space",
                "cannot be followed",
                format!(
                    "{SVG_OPEN}<defs>{holder}<rect id=\"g \" width=\"1\" height=\"1\"/></defs><use href=\"#g \"/></svg>"
                ),
            ),
        ];
        for (what, why, svg) in cases {
            let refused = check_svg(&svg)
                .expect_err(what)
                .to_string();
            assert!(refused.contains(why), "{what} was refused as: {refused}");
        }
    }

    /// A group with opacity or a clip path is drawn on a layer of its own,
    /// as large as what it holds: an SVG whose layers add up to far more
    /// canvas than its picture is refused before any is made.
    #[test]
    fn an_svg_with_too_much_layer_is_refused_before_it_is_drawn() {
        let reaching = format!(
            "{SVG_OPEN}<rect x=\"-9999\" y=\"-9999\" width=\"99999\" height=\"99999\" opacity=\"0.5\"/></svg>"
        );
        let stacked = format!(
            "{SVG_OPEN}{}<rect width=\"10\" height=\"10\"/>{}</svg>",
            "<g opacity=\"0.9\">".repeat(20),
            "</g>".repeat(20)
        );
        for svg in [reaching, stacked] {
            let doc = check_svg(&svg).unwrap();
            let refused = draw_svg(&doc)
                .unwrap_err()
                .to_string();
            assert!(refused.contains("too many layers"), "{refused}");
        }

        // A few layers the size of the picture are what logos have.
        let ordinary = format!(
            "{SVG_OPEN}<clipPath id=\"c\"><rect width=\"10\" height=\"5\"/></clipPath><g clip-path=\"url(#c)\" opacity=\"0.5\"><rect width=\"10\" height=\"10\" opacity=\"0.5\"/></g></svg>"
        );
        let doc = check_svg(&ordinary).unwrap();
        draw_svg(&doc).unwrap();
    }

    /// Even an SVG that got past the check cannot make the server read a local
    /// file or decode another document it carries.
    #[test]
    fn an_svg_cannot_make_the_server_read_a_file_or_load_a_document() {
        let source_dir = tempfile::tempdir().unwrap();
        let inner = source_dir
            .path()
            .join("inner.svg");
        std::fs::write(
            &inner,
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"><rect width="10" height="10"/></svg>"#,
        )
        .unwrap();
        // The same document as a data URI.
        let inner_data = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxMCIgaGVpZ2h0PSIxMCI+PHJlY3Qgd2lkdGg9IjEwIiBoZWlnaHQ9IjEwIi8+PC9zdmc+";
        // The top half is drawn; the bottom half could only come from the
        // referenced documents.
        let svg = format!(
            r##"<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20"><rect width="20" height="10" fill="#0000ff"/><image x="0" y="10" width="10" height="10" href="{}"/><image x="10" y="10" width="10" height="10" href="{inner_data}"/></svg>"##,
            inner.display(),
        );

        // Drawn without the check, which would refuse the `<image>`.
        let doc = roxmltree::Document::parse(&svg).unwrap();
        let png = draw_svg(&doc).unwrap();

        let img = image::load_from_memory(&png)
            .unwrap()
            .into_rgba8();
        let (w, h) = img.dimensions();
        assert_eq!(img.get_pixel(w / 2, h / 4)[3], 255);
        assert_eq!(
            img.get_pixel(w / 4, h * 3 / 4)[3],
            0,
            "the file was not drawn"
        );
        assert_eq!(
            img.get_pixel(w * 3 / 4, h * 3 / 4)[3],
            0,
            "the data URI was not drawn"
        );
    }

    #[tokio::test]
    async fn a_second_request_for_an_svg_is_served_from_the_cache() {
        let data_dir = tempfile::tempdir().unwrap();
        svg_png(data_dir.path(), HALF_FILLED_SVG).await;
        let cached = cached_pngs(data_dir.path());
        assert_eq!(cached.len(), 1);
        // Nothing is left of the temporary file it was written under.
        let files = std::fs::read_dir(ImageService::cache_dir(data_dir.path()))
            .unwrap()
            .count();
        assert_eq!(files, 1);

        // Replacing the cached file proves the second call reads it rather
        // than rendering again.
        let mut marker = Cursor::new(Vec::new());
        RgbaImage::new(3, 3)
            .write_to(&mut marker, ImageFormat::Png)
            .unwrap();
        std::fs::write(&cached[0], marker.get_ref()).unwrap();

        let second = svg_png(data_dir.path(), HALF_FILLED_SVG).await;
        assert_eq!(&second, marker.get_ref());
    }

    /// A request that waited for a render slot uses what the render before it
    /// stored, instead of rendering the same SVG again.
    #[tokio::test]
    async fn a_request_that_waited_for_a_render_slot_reads_the_cache() {
        let data_dir = tempfile::tempdir().unwrap();
        svg_png(data_dir.path(), HALF_FILLED_SVG).await;
        let cached = cached_pngs(data_dir.path());
        std::fs::remove_file(&cached[0]).unwrap();

        let slots = SVG_RENDERS
            .acquire_many(SVG_MAX_RENDERS as u32)
            .await
            .unwrap();
        let dir = data_dir
            .path()
            .to_path_buf();
        let waiting = tokio::spawn(async move {
            ImageService::svg_as_png(&dir, HALF_FILLED_SVG.as_bytes()).await
        });
        // Long enough for it to miss the cache and start waiting for a slot.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut marker = Cursor::new(Vec::new());
        RgbaImage::new(3, 3)
            .write_to(&mut marker, ImageFormat::Png)
            .unwrap();
        std::fs::write(&cached[0], marker.get_ref()).unwrap();
        drop(slots);

        let png = waiting
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(&png, marker.get_ref());
    }

    /// A cache file that cannot be put in place leaves nothing behind.
    #[tokio::test]
    async fn a_failed_cache_write_leaves_no_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        // A folder in the way: the file cannot be renamed onto it.
        let path = dir
            .path()
            .join("taken.png");
        std::fs::create_dir(&path).unwrap();

        assert!(
            ImageService::write_cache_file(&path, b"png")
                .await
                .is_err()
        );
        let left = std::fs::read_dir(dir.path())
            .unwrap()
            .count();
        assert_eq!(left, 1);
    }
}
