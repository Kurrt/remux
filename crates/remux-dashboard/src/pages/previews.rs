use crate::{
    components::{Card, ErrorAlert, LoadingText, SuccessAlert, ToggleRow},
    router::Route,
    state::AppState,
};
use dioxus::prelude::*;
use remux_sdks::remux::{
    GetSystemConfiguration, ServerConfiguration, UpdateSystemConfiguration,
};

#[derive(Clone, Copy)]
struct SeekPreviewSettingsState {
    config: Signal<Option<ServerConfiguration>>,
    enabled: Signal<bool>,
    cache_mb: Signal<i64>,
    keep_days: Signal<i64>,
    start_at_position: Signal<bool>,
    rough_first: Signal<bool>,
    other_version: Signal<bool>,
    loading: Signal<bool>,
    saving: Signal<bool>,
    saved: Signal<bool>,
    error: Signal<Option<String>>,
}

#[component]
pub fn SeekPreviewSettingsPage(app_state: AppState) -> Element {
    let mut state = SeekPreviewSettingsState {
        config: use_signal(|| None),
        enabled: use_signal(|| false),
        cache_mb: use_signal(|| 1024),
        keep_days: use_signal(|| 30),
        start_at_position: use_signal(|| false),
        rough_first: use_signal(|| false),
        other_version: use_signal(|| false),
        loading: use_signal(|| true),
        saving: use_signal(|| false),
        saved: use_signal(|| false),
        error: use_signal(|| None),
    };
    use_context_provider(|| state);

    let mut load_state = state;
    let load_client = app_state.clone();
    use_effect(move || {
        let client = load_client.clone();
        spawn(async move {
            match client
                .execute(GetSystemConfiguration)
                .await
            {
                Ok(cfg) => {
                    load_state
                        .enabled
                        .set(
                            cfg.trickplay_while_streaming
                                .unwrap_or(false),
                        );
                    load_state
                        .cache_mb
                        .set(
                            cfg.trickplay_cache_size_mb
                                .unwrap_or(1024),
                        );
                    load_state
                        .keep_days
                        .set(
                            cfg.trickplay_keep_days
                                .unwrap_or(30),
                        );
                    load_state
                        .start_at_position
                        .set(
                            cfg.trickplay_start_at_position
                                .unwrap_or(false),
                        );
                    load_state
                        .rough_first
                        .set(
                            cfg.trickplay_rough_first
                                .unwrap_or(false),
                        );
                    load_state
                        .other_version
                        .set(
                            cfg.trickplay_other_version
                                .unwrap_or(false),
                        );
                    load_state
                        .config
                        .set(Some(cfg));
                }
                Err(error) => load_state
                    .error
                    .set(Some(format!(
                        "Failed to load seek preview settings: {error}"
                    ))),
            }
            load_state
                .loading
                .set(false);
        });
    });

    let on_submit = move |event: Event<FormData>| {
        event.prevent_default();
        let Some(config) = state
            .config
            .peek()
            .clone()
        else {
            return;
        };
        let updated = ServerConfiguration {
            trickplay_while_streaming: Some(
                *state
                    .enabled
                    .peek(),
            ),
            trickplay_cache_size_mb: Some(
                *state
                    .cache_mb
                    .peek(),
            ),
            trickplay_keep_days: Some(
                *state
                    .keep_days
                    .peek(),
            ),
            trickplay_start_at_position: Some(
                *state
                    .start_at_position
                    .peek(),
            ),
            trickplay_rough_first: Some(
                *state
                    .rough_first
                    .peek(),
            ),
            trickplay_other_version: Some(
                *state
                    .other_version
                    .peek(),
            ),
            ..config
        };
        state
            .saving
            .set(true);
        state
            .saved
            .set(false);
        state
            .error
            .set(None);
        let client = app_state.clone();
        spawn(async move {
            match client
                .execute(UpdateSystemConfiguration {
                    config: updated.clone(),
                })
                .await
            {
                Ok(_) => {
                    state
                        .config
                        .set(Some(updated));
                    state
                        .saved
                        .set(true);
                }
                Err(error) => state
                    .error
                    .set(Some(error.user_message())),
            }
            state
                .saving
                .set(false);
        });
    };

    rsx! {
        if *state.loading.read() {
            LoadingText {}
        } else {
            form {
                onsubmit: on_submit,
                style: "display:flex;flex-direction:column;gap:14px",
                p { style: "margin:0;color:var(--text-muted);font-size:.75rem;line-height:1.5",
                    "Seek previews are the small pictures a client shows while you seek through a video."
                }
                PreviewsMainPanel {}
                if !*state.enabled.read() {
                    p { class: "field-hint", "The options below have no effect while seek previews are off." }
                }
                PreviewsStoragePanel {}
                PreviewsStartPanel {}
                PreviewsRoughPanel {}
                PreviewsOtherVersionPanel {}
                PreviewsAddonPanel {}

                if let Some(error) = state.error.read().as_ref() {
                    ErrorAlert { message: error.clone() }
                }
                if *state.saved.read() {
                    SuccessAlert { message: "Seek preview settings saved.".to_string() }
                }
                div { class: "form-actions",
                    button {
                        r#type: "submit",
                        class: "btn btn-primary",
                        disabled: *state.saving.read() || state.config.peek().is_none(),
                        if *state.saving.read() { "Saving…" } else { "Save settings" }
                    }
                }
            }
        }
    }
}

/// What an option costs and what clients do with it, under its control:
/// "Costs", then "Clients", then "Not made for", each only when it has lines.
/// `warn` marks the Clients lines of an option that can make a client show a
/// picture that does not belong to the position.
#[component]
fn Notes(
    costs: Vec<&'static str>,
    #[props(default)] clients: Vec<&'static str>,
    #[props(default)] not_for: Vec<&'static str>,
    #[props(default)] warn: bool,
) -> Element {
    rsx! {
        div { style: "display:flex;flex-direction:column;gap:10px;border-top:1px solid var(--border);padding-top:14px",
            if !costs.is_empty() {
                NoteRow { label: "Costs", warn: false, lines: costs.clone() }
            }
            if !clients.is_empty() {
                NoteRow { label: "Clients", warn, lines: clients.clone() }
            }
            if !not_for.is_empty() {
                NoteRow { label: "Not made for", warn: false, lines: not_for.clone() }
            }
        }
    }
}

/// One row of [`Notes`]: a small heading beside its lines, the heading above
/// them on a narrow screen.
#[component]
fn NoteRow(label: &'static str, warn: bool, lines: Vec<&'static str>) -> Element {
    rsx! {
        div { style: "display:flex;flex-wrap:wrap;gap:4px 16px",
            span { class: "field-label", style: "flex:0 0 7.5rem", "{label}" }
            div {
                style: if warn {
                    "flex:1 1 16rem;min-width:0;display:flex;flex-direction:column;gap:4px;border-left:2px solid var(--warning);padding-left:10px"
                } else {
                    "flex:1 1 16rem;min-width:0;display:flex;flex-direction:column;gap:4px;border-left:2px solid transparent;padding-left:10px"
                },
                for line in lines.iter() {
                    p { key: "{line}", class: "field-hint", "{line}" }
                }
            }
        }
    }
}

#[component]
fn PreviewsMainPanel() -> Element {
    let mut state = use_context::<SeekPreviewSettingsState>();
    rsx! {
        Card { title: "Seek previews",
            ToggleRow {
                label: "Make seek previews while streaming",
                description: "Makes seek previews in the background while a video plays, by reading the file from its source a second time. Disabled by default.",
                checked: *state.enabled.read(),
                disabled: *state.saving.read(),
                on_change: move |value| {
                    state.enabled.set(value);
                    state.saved.set(false);
                }
            }
            Notes {
                costs: vec![
                    "Until a video's previews are complete, each play reads up to the whole file a second time, which counts toward debrid, usenet and data limits.",
                    "Holds a second connection to the source and uses about one CPU core while previews are made.",
                    "A source that allows only one connection may refuse the second or interrupt playback: see Per addon.",
                ],
                clients: vec!["The preview box may be empty until a video's previews are made."],
                not_for: vec![
                    "Live TV, IPTV addons, addons that bypass the Remux proxy, torrents, HLS streams, music, and videos under 5 minutes or over 4 hours.",
                ],
            }
        }
    }
}

#[component]
fn PreviewsStoragePanel() -> Element {
    let mut state = use_context::<SeekPreviewSettingsState>();
    let disabled = *state
        .saving
        .read()
        || !*state
            .enabled
            .read();
    rsx! {
        Card { title: "Storage",
            div { style: "display:flex;flex-wrap:wrap;gap:14px",
                div { class: "field", style: "flex:1 1 260px;min-width:0",
                    label { class: "field-label", r#for: "trickplay-cache", "Preview cache size (MB)" }
                    input {
                        id: "trickplay-cache",
                        r#type: "number",
                        class: "field-input",
                        min: "100",
                        value: "{state.cache_mb}",
                        disabled,
                        oninput: move |event| {
                            if let Ok(value) = event.value().parse::<i64>() {
                                state.cache_mb.set(value);
                                state.saved.set(false);
                            }
                        },
                    }
                    p { class: "field-hint", "When full, the previews shown longest ago are deleted first. Minimum 100. Default: 1024." }
                }
                div { class: "field", style: "flex:1 1 260px;min-width:0",
                    label { class: "field-label", r#for: "trickplay-keep", "Keep previews for (days)" }
                    input {
                        id: "trickplay-keep",
                        r#type: "number",
                        class: "field-input",
                        min: "0",
                        max: "3650",
                        value: "{state.keep_days}",
                        disabled,
                        oninput: move |event| {
                            if let Ok(value) = event.value().parse::<i64>() {
                                state.keep_days.set(value);
                                state.saved.set(false);
                            }
                        },
                    }
                    p { class: "field-hint", "Previews not shown for this many days are deleted. 0 keeps them until the cache is full. Default: 30." }
                }
            }
            Notes {
                costs: vec![
                    "About 9 MB of disk for a two-hour movie.",
                    "Turning seek previews off deletes all previews the next time Remux starts.",
                ],
            }
        }
    }
}

#[component]
fn PreviewsStartPanel() -> Element {
    let mut state = use_context::<SeekPreviewSettingsState>();
    rsx! {
        Card { title: "Start position",
            ToggleRow {
                label: "Start at the playback position",
                description: "Starts previews at the position being watched and follows up to 4 seeks, instead of working in order from the start. Disabled by default.",
                checked: *state.start_at_position.read(),
                disabled: *state.saving.read() || !*state.enabled.read(),
                on_change: move |value| {
                    state.start_at_position.set(value);
                    state.saved.set(false);
                }
            }
            Notes {
                costs: vec![
                    "Starting part way and each followed seek may briefly hold another connection to the source.",
                ],
                clients: vec![
                    "Clients that load every preview when play starts may show previews at the wrong time until a video's previews are complete.",
                ],
                warn: true,
            }
        }
    }
}

#[component]
fn PreviewsRoughPanel() -> Element {
    let mut state = use_context::<SeekPreviewSettingsState>();
    rsx! {
        Card { title: "Rough previews",
            ToggleRow {
                label: "Rough previews first",
                description: "For files of 4 GB or more, first makes up to 17 pictures spread across the video and sends the nearest one until the real preview is made. Disabled by default.",
                checked: *state.rough_first.read(),
                disabled: *state.saving.read() || !*state.enabled.read(),
                on_change: move |value| {
                    state.rough_first.set(value);
                    state.saved.set(false);
                }
            }
            Notes {
                costs: vec![
                    "Up to 17 extra short reads of the source, and the full previews start up to 45 seconds later.",
                ],
                clients: vec![
                    "Shown by the Jellyfin web client and apps built on it; other clients may ignore them.",
                    "A rough preview can be minutes from the seek position.",
                    "Sent as HTTP 503, so a reverse proxy that replaces error pages removes them.",
                ],
                warn: true,
            }
        }
    }
}

#[component]
fn PreviewsOtherVersionPanel() -> Element {
    let mut state = use_context::<SeekPreviewSettingsState>();
    rsx! {
        Card { title: "Another version",
            ToggleRow {
                label: "Use another version for previews",
                description: "When the version being played cannot be read for previews, such as one from an IPTV addon, reads another version of the same movie or episode with the same length. Disabled by default.",
                checked: *state.other_version.read(),
                disabled: *state.saving.read() || !*state.enabled.read(),
                on_change: move |value| {
                    state.other_version.set(value);
                    state.saved.set(false);
                }
            }
            Notes {
                costs: vec![
                    "Reads a file nobody chose to play, up to the whole file, which counts toward that source's limits.",
                    "Only local, WebDAV and cached debrid or usenet versions are read.",
                    "If a file marked cached has left the debrid cache, reading it can start a download in your account.",
                ],
                clients: vec![
                    "Previews can come from a different cut of the same length, or a file listed under the wrong episode.",
                    "If the other version proves to be a different file, its previews are deleted, but clients that cache previews may keep showing them.",
                ],
                warn: true,
            }
        }
    }
}

#[component]
fn PreviewsAddonPanel() -> Element {
    rsx! {
        Card {
            title: "Per addon",
            action: rsx! {
                button {
                    r#type: "button",
                    class: "btn btn-ghost",
                    style: "height:32px;font-size:.68rem",
                    onclick: move |_| {
                        navigator().push(Route::AddonsRoute);
                    },
                    "Open Addons"
                }
            },
            div { style: "display:flex;flex-direction:column;gap:4px",
                p { class: "field-hint",
                    "Each addon that can have seek previews has its own \"Make seek previews\" switch and service filter, under Addons, Edit."
                }
                p { class: "field-hint",
                    "The switch is enabled by default. Turn it off for an addon whose source allows only one connection."
                }
                p { class: "field-hint",
                    "An addon that combines several sources is judged stream by stream. Use its service filter to leave out a source that allows only one connection."
                }
            }
        }
    }
}
