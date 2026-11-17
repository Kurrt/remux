use crate::ResultExt;
/// Axum extractor for Jellyfin-style image uploads.
///
/// Jellyfin clients POST images as a **base64-encoded** body (optionally
/// prefixed with a `data:<mime>;base64,` data-URI header).  This extractor
/// transparently decodes the body and sniffs the content-type from the magic
/// bytes, giving handlers ready-to-use raw image bytes.
///
/// # Usage
/// ```rust,ignore
/// #[post("/some/image/endpoint")]
/// async fn handler(image: JellyfinImage) -> impl IntoResponse {
///     let (bytes, content_type) = image.into_parts();
///     // bytes is decoded raw image data, content_type is e.g. "image/jpeg"
/// }
/// ```
use axum::body::Bytes;
use axum::extract::{FromRequest, Request};
use axum_anyhow::ApiError;
use base64::Engine;

pub struct JellyfinImage {
    pub bytes: Bytes,
    pub content_type: &'static str,
}

impl JellyfinImage {
    pub fn into_parts(self) -> (Bytes, &'static str) {
        (self.bytes, self.content_type)
    }
}

/// Decode a Jellyfin image body: strip optional data-URI prefix, then
/// base64-decode. Falls back to raw bytes if decoding fails (e.g. a future
/// client sending raw binary directly).
fn decode_body(body: &[u8]) -> Vec<u8> {
    // Strip "data:<mime>;base64," prefix if present
    let src = if let Some(pos) = body
        .iter()
        .position(|&b| b == b',')
    {
        &body[pos + 1..]
    } else {
        body
    };

    base64::engine::general_purpose::STANDARD
        .decode(src)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(src))
        .unwrap_or_else(|_| body.to_vec())
}

pub fn detect_content_type(bytes: &[u8]) -> &'static str {
    match bytes {
        [0xff, 0xd8, 0xff, ..] => "image/jpeg",
        [0x89, b'P', b'N', b'G', ..] => "image/png",
        [b'G', b'I', b'F', ..] => "image/gif",
        [
            b'R',
            b'I',
            b'F',
            b'F',
            _,
            _,
            _,
            _,
            b'W',
            b'E',
            b'B',
            b'P',
            ..,
        ] => "image/webp",
        _ => "image/jpeg",
    }
}

/// Whether `bytes` are an SVG document: an optional byte order mark, XML
/// declaration, comments and doctype, then an `<svg>` root. Raster formats
/// never start with `<`, so none is taken for SVG. Compressed SVG is not
/// recognised, and only the first 4 KiB are looked at, so neither is an SVG
/// whose prolog is longer.
pub fn is_svg(bytes: &[u8]) -> bool {
    let bytes = &bytes[..bytes
        .len()
        .min(4096)];
    let mut rest = bytes
        .strip_prefix(b"\xEF\xBB\xBF")
        .unwrap_or(bytes);
    loop {
        rest = rest.trim_ascii_start();
        if let Some(after) = rest.strip_prefix(b"<svg") {
            return after
                .first()
                .is_some_and(|&b| b.is_ascii_whitespace() || matches!(b, b'>' | b'/'));
        }
        let end: &[u8] = if rest.starts_with(b"<?") {
            b"?>"
        } else if rest.starts_with(b"<!--") {
            b"-->"
        } else if rest.starts_with(b"<!") {
            // A doctype may carry an internal subset in brackets, with `>`
            // inside it.
            let close = rest
                .iter()
                .position(|&b| b == b'>');
            if close.is_some_and(|c| rest[..c].contains(&b'[')) {
                b"]"
            } else {
                b">"
            }
        } else {
            return false;
        };
        let Some(at) = rest
            .windows(end.len())
            .position(|w| w == end)
        else {
            return false;
        };
        rest = &rest[at + end.len()..];
        if end == b"]" {
            rest = rest.trim_ascii_start();
            let Some(after) = rest.strip_prefix(b">") else {
                return false;
            };
            rest = after;
        }
    }
}

impl<S> FromRequest<S> for JellyfinImage
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let body = axum::body::to_bytes(req.into_body(), 50 * 1024 * 1024)
            .await
            .context_internal("body read error")?;

        let decoded = decode_body(&body);
        let content_type = detect_content_type(&decoded);

        Ok(JellyfinImage {
            bytes: Bytes::from(decoded),
            content_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::is_svg;

    /// Item images are sniffed for SVG so they can be served as PNG; the
    /// sniff must see through the prolog editors write and never match a
    /// raster image or an HTML page.
    #[test]
    fn svg_is_recognised_after_its_prolog_and_nothing_else_is() {
        assert!(is_svg(b"<svg xmlns=\"http://www.w3.org/2000/svg\"/>"));
        assert!(is_svg(b"\xEF\xBB\xBF\n  <svg>"));
        assert!(is_svg(
            b"<?xml version=\"1.0\"?>\n<!-- made by hand -->\n\
              <!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \"x.dtd\" [\n\
              <!ENTITY ns \"made up\">\n]>\n<svg width=\"1\">"
        ));

        assert!(!is_svg(b"\x89PNG\r\n\x1a\n"));
        assert!(!is_svg(&[0xff, 0xd8, 0xff, 0xe0]));
        assert!(!is_svg(&[0x1f, 0x8b, 0x08, 0x00]));
        assert!(!is_svg(
            b"<!DOCTYPE html><html><body><svg></svg></body></html>"
        ));
        assert!(!is_svg(b"<svgfoo/>"));
        assert!(!is_svg(b"<?xml version=\"1.0\""));
        assert!(!is_svg(b""));
    }

    /// The sniff runs on every image body: only the first 4 KiB are looked
    /// at, so it costs the same on a body of any size.
    #[test]
    fn only_a_short_prefix_is_sniffed() {
        assert!(is_svg(format!("{}<svg/>", " ".repeat(4000)).as_bytes()));
        assert!(!is_svg(format!("{}<svg/>", " ".repeat(4096)).as_bytes()));
    }
}
