//! Jellyfin's trick play sheets, made in the background by [`crate::trickplay`]
//! while a video plays.

use axum::{
    body::Body,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use axum_extra::extract::Query;
use http::{StatusCode, header};
use remux_macros::{get, query};
use uuid::Uuid;

use crate::{
    AppContext, AppState, db, db::auth, ids::UserId, services::StreamService, trickplay,
};

#[query]
pub struct TrickplaySheetQuery {
    pub media_source_id: Option<Uuid>,
}

/// One sheet of 5x5 pictures, as Jellyfin serves it. Answers 404 until the
/// sheet is made, and whenever the switch is off.
#[get("/videos/{id}/trickplay/{width}/{file}")]
pub async fn trickplay_sheet(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Path((id, width, file)): Path<(Uuid, u32, String)>,
    Query(q): Query<TrickplaySheetQuery>,
) -> Response {
    let root = trickplay::root_dir(
        &state
            .ctx
            .config
            .data_dir,
    );
    serve_sheet(
        &state.ctx,
        &root,
        SheetRequest {
            user_id: session
                .user
                .id,
            device_id: &session
                .device
                .id,
            id,
            media_source_id: q.media_source_id,
            width,
            file: &file,
        },
    )
    .await
}

/// What a sheet request names, with the caller's user and device.
pub(crate) struct SheetRequest<'a> {
    pub user_id: Uuid,
    pub device_id: &'a str,
    pub id: Uuid,
    pub media_source_id: Option<Uuid>,
    pub width: u32,
    pub file: &'a str,
}

/// Resolves the stream from cheap state, then serves its sheet from `root`.
pub(crate) async fn serve_sheet(
    ctx: &AppContext,
    root: &std::path::Path,
    req: SheetRequest<'_>,
) -> Response {
    if req.width != trickplay::WIDTH {
        return not_found();
    }
    let Some(n) = trickplay::parse_sheet(req.file) else {
        return not_found();
    };
    if !trickplay::enabled(&db::Settings::get_config_or_default(&ctx.db).await) {
        return not_found();
    }
    let Some(stream_id) =
        resolve_stream(ctx, req.user_id, req.device_id, req.id, req.media_source_id)
            .await
    else {
        return not_found();
    };
    // Any sheet on disk is whole: each is renamed into place once written, and
    // a pass that stops early deletes its last one.
    let Ok(bytes) = tokio::fs::read(trickplay::sheet_path(root, stream_id, n)).await
    else {
        return not_found();
    };
    ctx.sessions
        .trickplay
        .mark_used(stream_id, &trickplay::stream_dir(root, stream_id));
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/jpeg"),
            (header::CACHE_CONTROL, "private, no-cache"),
        ],
        Body::from(bytes),
    )
        .into_response()
}

/// The stream whose pictures this device gets for `id`: the file its play
/// registered, then the stream its PlaybackInfo picked. Never a stream lookup:
/// that can refresh addon streams, players ask for a sheet on nearly every
/// move of the seek bar, and it could pick another file than the one whose
/// pictures exist.
async fn resolve_stream(
    ctx: &AppContext,
    user_id: Uuid,
    device_id: &str,
    id: Uuid,
    media_source_id: Option<Uuid>,
) -> Option<Uuid> {
    let passes = &ctx
        .sessions
        .trickplay;
    if let Some(stream_id) = passes.selected_stream(id, device_id) {
        return Some(stream_id);
    }
    // A version held as the item id maps back to its owner, the key a play
    // registers under and PlaybackInfo saves its pick under.
    let (owner, source) =
        StreamService::version_target(ctx, Some(UserId(user_id)), id, media_source_id)
            .await;
    passes
        .selected_stream(owner, device_id)
        .or_else(|| {
            StreamService::selected_stream_id(ctx, Some(device_id), owner, source, None)
        })
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, [(header::CACHE_CONTROL, "no-store")]).into_response()
}

#[cfg(test)]
mod tests {
    use axum_test::TestServer;
    use http::header::HeaderValue;
    use serde_json::json;
    use uuid::Uuid;

    use crate::{
        AppContext, Config, api, db,
        integration_test::{AUTH_HEADER, TestGuard, new_test_server_with_config},
        trickplay,
    };

    /// A test server whose data directory is `dir`, signed in as the test
    /// user, so sheets written for it never touch a real data directory.
    async fn server_in(dir: &std::path::Path) -> (TestServer, TestGuard, String) {
        let (server, guard) = new_test_server_with_config(Config {
            data_dir: dir.to_path_buf(),
            database_url: Some("sqlite::memory:".into()),
            torrent_http_port: None,
            torrent_peer_port: None,
            disable_dht: true,
            disable_startup_tasks: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let body: serde_json::Value = server
            .post("/users/authenticatebyname")
            .add_header(
                http::header::AUTHORIZATION,
                HeaderValue::from_static(AUTH_HEADER),
            )
            .json(&json!({ "Username": "test", "Pw": "test" }))
            .await
            .json();
        let token = body["AccessToken"]
            .as_str()
            .unwrap()
            .to_string();
        (server, guard, token)
    }

    async fn set_switch(ctx: &AppContext, on: bool) {
        let cfg = api::ServerConfiguration {
            trickplay_while_streaming: Some(on),
            ..db::Settings::get_config_or_default(&ctx.db).await
        };
        db::Settings::set_config(&ctx.db, &cfg)
            .await
            .unwrap();
    }

    /// A film with no stream rows, so nothing but the recorded selection can
    /// name a stream for it.
    async fn invented_film(ctx: &AppContext) -> db::Media {
        let external_ids = db::ExternalIds {
            imdb: db::NonEmptyString::try_new("tt9900001".to_string()).ok(),
            ..Default::default()
        };
        let mut film = db::Media {
            id: Uuid::from(&db::MediaIdRaw {
                kind: db::MediaKind::Movie,
                external_ids: external_ids.clone(),
                season: None,
                episode: None,
            }),
            title: "The Lantern Keeper".to_string(),
            kind: db::MediaKind::Movie,
            runtime: Some(7_200),
            external_ids,
            ..Default::default()
        };
        film.save(&ctx.db)
            .await
            .unwrap();
        film
    }

    /// Records that the test device's play of `item_id` reads `stream_id`, as
    /// a play-start report does once its file is resolved.
    fn record_selection(ctx: &AppContext, item_id: Uuid, stream_id: Uuid) {
        let passes = &ctx
            .sessions
            .trickplay;
        passes.register("invented-play", item_id, "test-device");
        assert!(passes.attach("invented-play", stream_id));
    }

    fn write_sheet(ctx: &AppContext, stream_id: Uuid, n: u32) {
        let root = trickplay::root_dir(
            &ctx.config
                .data_dir,
        );
        std::fs::create_dir_all(trickplay::stream_dir(&root, stream_id)).unwrap();
        std::fs::write(trickplay::sheet_path(&root, stream_id, n), b"jpeg").unwrap();
    }

    #[tokio::test]
    async fn serves_made_sheets_and_404s_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let (server, guard, token) = server_in(dir.path()).await;
        let ctx = &guard.0;
        set_switch(ctx, true).await;
        let film = invented_film(ctx).await;
        let stream_id = Uuid::new_v4();
        record_selection(ctx, film.id, stream_id);
        write_sheet(ctx, stream_id, 0);
        let id = film
            .id
            .simple();

        let served = server
            .get(&format!("/Videos/{id}/Trickplay/320/0.jpg?ApiKey={token}"))
            .await;
        served.assert_status_ok();
        assert_eq!(served.header("content-type"), "image/jpeg");
        assert_eq!(
            served
                .as_bytes()
                .as_ref(),
            b"jpeg"
        );

        // Not made yet.
        let missing = server
            .get(&format!("/Videos/{id}/Trickplay/320/1.jpg?ApiKey={token}"))
            .expect_failure()
            .await;
        missing.assert_status_not_found();
        assert_eq!(missing.header("cache-control"), "no-store");

        // Only one width is made.
        server
            .get(&format!("/Videos/{id}/Trickplay/480/0.jpg?ApiKey={token}"))
            .expect_failure()
            .await
            .assert_status_not_found();

        // No playlist of sheets.
        server
            .get(&format!(
                "/Videos/{id}/Trickplay/320/tiles.m3u8?ApiKey={token}"
            ))
            .expect_failure()
            .await
            .assert_status_not_found();

        set_switch(ctx, false).await;
        server
            .get(&format!("/Videos/{id}/Trickplay/320/0.jpg?ApiKey={token}"))
            .expect_failure()
            .await
            .assert_status_not_found();
    }

    #[tokio::test]
    async fn no_selection_is_404_without_a_stream_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let (server, guard, token) = server_in(dir.path()).await;
        let ctx = &guard.0;
        set_switch(ctx, true).await;
        let film = invented_film(ctx).await;
        // A fresh stored stream with a made sheet: a stream lookup would find
        // it, so only the route's refusal to look one up gives a 404.
        let mut stream = db::Media {
            title: "Lantern.Keeper.1080p.mkv".to_string(),
            kind: db::MediaKind::Stream,
            parent_id: Some(film.id),
            idx: Some(0),
            stream_info: Some(crate::stream::StreamInfo {
                descriptor: crate::stream::StreamDescriptor::Local(
                    "Lantern.Keeper.1080p.mkv".into(),
                ),
                filename: Some("Lantern.Keeper.1080p.mkv".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        stream
            .save(&ctx.db)
            .await
            .unwrap();
        sqlx::query("UPDATE media SET streams_refreshed_at = ? WHERE id = ?")
            .bind(chrono::Utc::now().naive_utc())
            .bind(film.id)
            .execute(&ctx.db)
            .await
            .unwrap();
        write_sheet(ctx, stream.id, 0);
        let id = film
            .id
            .simple();

        let refused = server
            .get(&format!("/Videos/{id}/Trickplay/320/0.jpg?ApiKey={token}"))
            .expect_failure()
            .await;
        refused.assert_status_not_found();
        assert_eq!(refused.header("cache-control"), "no-store");
    }

    #[tokio::test]
    async fn another_device_gets_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (_server, guard, _token) = server_in(dir.path()).await;
        let ctx = &guard.0;
        set_switch(ctx, true).await;
        let film = invented_film(ctx).await;
        let stream_id = Uuid::new_v4();
        record_selection(ctx, film.id, stream_id);
        write_sheet(ctx, stream_id, 0);
        let root = trickplay::root_dir(
            &ctx.config
                .data_dir,
        );

        let request = |device_id: &'static str| super::SheetRequest {
            user_id: Uuid::new_v4(),
            device_id,
            id: film.id,
            media_source_id: None,
            width: trickplay::WIDTH,
            file: "0.jpg",
        };
        let own = super::serve_sheet(ctx, &root, request("test-device")).await;
        assert_eq!(own.status(), http::StatusCode::OK);
        let other = super::serve_sheet(ctx, &root, request("other-device")).await;
        assert_eq!(other.status(), http::StatusCode::NOT_FOUND);
    }
}
