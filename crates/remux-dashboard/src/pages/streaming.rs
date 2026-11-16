use super::{stream_sorting_description, DEVICE_SORTING_HINT};
use crate::{
    components::{Card, ErrorAlert, LoadingText, SuccessAlert, ToggleRow},
    state::AppState,
};
use dioxus::prelude::*;
use remux_sdks::remux::{
    GetSystemConfiguration, ServerConfiguration, SortMediaSourcesMode,
    UpdateSystemConfiguration,
};

#[derive(Clone, Copy)]
struct StreamingSettingsState {
    config: Signal<Option<ServerConfiguration>>,
    sort_mode: Signal<SortMediaSourcesMode>,
    show_decision: Signal<bool>,
    probe_timeout: Signal<i64>,
    probe_timeout_p2p: Signal<i64>,
    auto_next_stream: Signal<bool>,
    max_fallback_streams: Signal<i64>,
    p2p_enabled: Signal<bool>,
    p2p_upload_speed: Signal<i64>,
    p2p_download_speed: Signal<i64>,
    trickplay_enabled: Signal<bool>,
    trickplay_cache_mb: Signal<i64>,
    trickplay_keep_days: Signal<i64>,
    loading: Signal<bool>,
    saving: Signal<bool>,
    saved: Signal<bool>,
    error: Signal<Option<String>>,
}

#[component]
pub fn StreamingGeneralSettingsPage(app_state: AppState) -> Element {
    let mut state = StreamingSettingsState {
        config: use_signal(|| None),
        sort_mode: use_signal(|| SortMediaSourcesMode::Best),
        show_decision: use_signal(|| true),
        probe_timeout: use_signal(|| 20),
        probe_timeout_p2p: use_signal(|| 60),
        auto_next_stream: use_signal(|| true),
        max_fallback_streams: use_signal(|| 3),
        p2p_enabled: use_signal(|| true),
        p2p_upload_speed: use_signal(|| 0),
        p2p_download_speed: use_signal(|| 0),
        trickplay_enabled: use_signal(|| false),
        trickplay_cache_mb: use_signal(|| 1024),
        trickplay_keep_days: use_signal(|| 30),
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
                        .sort_mode
                        .set(
                            cfg.sort_media_sources
                                .unwrap_or_default(),
                        );
                    load_state
                        .show_decision
                        .set(
                            cfg.show_playback_decision_in_title
                                .unwrap_or(true),
                        );
                    load_state
                        .probe_timeout
                        .set(
                            cfg.probe_timeout_secs
                                .unwrap_or(20),
                        );
                    load_state
                        .probe_timeout_p2p
                        .set(
                            cfg.probe_timeout_p2p_secs
                                .unwrap_or(60),
                        );
                    load_state
                        .auto_next_stream
                        .set(
                            cfg.auto_next_stream_on_probe_fail
                                .unwrap_or(true),
                        );
                    load_state
                        .max_fallback_streams
                        .set(
                            cfg.max_probe_fallback_streams
                                .unwrap_or(3),
                        );
                    load_state
                        .p2p_enabled
                        .set(
                            cfg.p2p_enabled
                                .unwrap_or(true),
                        );
                    load_state
                        .p2p_upload_speed
                        .set(
                            cfg.p2p_upload_speed_kbps
                                .unwrap_or(0),
                        );
                    load_state
                        .p2p_download_speed
                        .set(
                            cfg.p2p_download_speed_kbps
                                .unwrap_or(0),
                        );
                    load_state
                        .trickplay_enabled
                        .set(
                            cfg.trickplay_while_streaming
                                .unwrap_or(false),
                        );
                    load_state
                        .trickplay_cache_mb
                        .set(
                            cfg.trickplay_cache_size_mb
                                .unwrap_or(1024),
                        );
                    load_state
                        .trickplay_keep_days
                        .set(
                            cfg.trickplay_keep_days
                                .unwrap_or(30),
                        );
                    load_state
                        .config
                        .set(Some(cfg));
                }
                Err(error) => load_state
                    .error
                    .set(Some(format!("Failed to load streaming settings: {error}"))),
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
            sort_media_sources: Some(
                *state
                    .sort_mode
                    .peek(),
            ),
            show_playback_decision_in_title: Some(
                *state
                    .show_decision
                    .peek(),
            ),
            probe_timeout_secs: Some(
                *state
                    .probe_timeout
                    .peek(),
            ),
            probe_timeout_p2p_secs: Some(
                *state
                    .probe_timeout_p2p
                    .peek(),
            ),
            auto_next_stream_on_probe_fail: Some(
                *state
                    .auto_next_stream
                    .peek(),
            ),
            max_probe_fallback_streams: Some(
                *state
                    .max_fallback_streams
                    .peek(),
            ),
            p2p_enabled: Some(
                *state
                    .p2p_enabled
                    .peek(),
            ),
            p2p_upload_speed_kbps: Some(
                *state
                    .p2p_upload_speed
                    .peek(),
            ),
            p2p_download_speed_kbps: Some(
                *state
                    .p2p_download_speed
                    .peek(),
            ),
            trickplay_while_streaming: Some(
                *state
                    .trickplay_enabled
                    .peek(),
            ),
            trickplay_cache_size_mb: Some(
                *state
                    .trickplay_cache_mb
                    .peek(),
            ),
            trickplay_keep_days: Some(
                *state
                    .trickplay_keep_days
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
                StreamingSortPanel {}
                StreamingProbePanel {}
                StreamingP2pPanel {}
                StreamingTrickplayPanel {}

                if let Some(error) = state.error.read().as_ref() {
                    ErrorAlert { message: error.clone() }
                }
                if *state.saved.read() {
                    SuccessAlert { message: "Streaming settings saved.".to_string() }
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

#[component]
fn StreamingSortPanel() -> Element {
    let mut state = use_context::<StreamingSettingsState>();
    let description = stream_sorting_description(
        *state
            .sort_mode
            .read(),
    );

    rsx! {
        Card { title: "Stream selection",
            div { style: "display:flex;flex-direction:column;gap:14px",
                div { class: "field",
                    label { class: "field-label", r#for: "sort-media-sources", "Sort streams by device capability" }
                    div { class: "field-hint", "{description}" }
                    if matches!(*state.sort_mode.read(), SortMediaSourcesMode::Best | SortMediaSourcesMode::Compatibility) {
                        div { class: "field-hint", "{DEVICE_SORTING_HINT}" }
                    }
                    select {
                        id: "sort-media-sources",
                        class: "select-input",
                        disabled: *state.saving.read(),
                        value: "{state.sort_mode.read().to_string()}",
                        onchange: move |event: Event<FormData>| {
                            if let Ok(mode) = event.value().parse::<SortMediaSourcesMode>() {
                                state.sort_mode.set(mode);
                                state.saved.set(false);
                            }
                        },
                        option { value: "Disabled", selected: *state.sort_mode.read() == SortMediaSourcesMode::Disabled, "Disabled" }
                        option { value: "Best", selected: *state.sort_mode.read() == SortMediaSourcesMode::Best, "Best" }
                        option { value: "Compatibility", selected: *state.sort_mode.read() == SortMediaSourcesMode::Compatibility, "Compatibility" }
                        option { value: "Quality", selected: *state.sort_mode.read() == SortMediaSourcesMode::Quality, "Quality" }
                    }
                }
                ToggleRow {
                    label: "Show playback decision in title",
                    description: "Append the playback decision to each stream title.",
                    checked: *state.show_decision.read(),
                    disabled: *state.saving.read(),
                    on_change: move |value| {
                        state.show_decision.set(value);
                        state.saved.set(false);
                    }
                }
            }
        }
    }
}

#[component]
fn StreamingProbePanel() -> Element {
    let mut state = use_context::<StreamingSettingsState>();
    rsx! {
        Card { title: "Stream probing",
            div { style: "display:flex;flex-direction:column;gap:14px",
                div { class: "field",
                    label { class: "field-label", r#for: "probe-timeout", "Probe timeout (seconds)" }
                    div { class: "field-hint", "Seconds to wait for HTTP/local stream probes." }
                    input {
                        id: "probe-timeout",
                        r#type: "number",
                        class: "text-input",
                        min: "1",
                        max: "300",
                        value: "{state.probe_timeout}",
                        disabled: *state.saving.read(),
                        oninput: move |event| {
                            if let Ok(value) = event.value().parse::<i64>() {
                                state.probe_timeout.set(value);
                                state.saved.set(false);
                            }
                        },
                    }
                }
                div { class: "field",
                    label { class: "field-label", r#for: "probe-timeout-p2p", "P2P probe timeout (seconds)" }
                    div { class: "field-hint", "Seconds to wait for torrent/P2P stream probes." }
                    input {
                        id: "probe-timeout-p2p",
                        r#type: "number",
                        class: "text-input",
                        min: "1",
                        max: "600",
                        value: "{state.probe_timeout_p2p}",
                        disabled: *state.saving.read(),
                        oninput: move |event| {
                            if let Ok(value) = event.value().parse::<i64>() {
                                state.probe_timeout_p2p.set(value);
                                state.saved.set(false);
                            }
                        },
                    }
                }
                ToggleRow {
                    label: "Auto next stream on probe failure",
                    description: "Try the next stream with matching resolution and type when probing fails.",
                    checked: *state.auto_next_stream.read(),
                    disabled: *state.saving.read(),
                    on_change: move |value| {
                        state.auto_next_stream.set(value);
                        state.saved.set(false);
                    }
                }
                div { class: "field",
                    label { class: "field-label", r#for: "max-fallback", "Max stream retries" }
                    div { class: "field-hint", "How many alternative streams to try before returning an error." }
                    input {
                        id: "max-fallback",
                        r#type: "number",
                        class: "text-input",
                        min: "0",
                        max: "20",
                        value: "{state.max_fallback_streams}",
                        disabled: *state.saving.read(),
                        oninput: move |event| {
                            if let Ok(value) = event.value().parse::<i64>() {
                                state.max_fallback_streams.set(value);
                                state.saved.set(false);
                            }
                        },
                    }
                }
            }
        }
    }
}

#[component]
fn StreamingP2pPanel() -> Element {
    let mut state = use_context::<StreamingSettingsState>();
    rsx! {
        Card { title: "P2P / torrent streams",
            div { style: "display:flex;flex-direction:column;gap:14px",
                ToggleRow {
                    label: "Enable P2P streams",
                    description: "Allow torrent/magnet streams from AIO sources.",
                    checked: *state.p2p_enabled.read(),
                    disabled: *state.saving.read(),
                    on_change: move |value| {
                        state.p2p_enabled.set(value);
                        state.saved.set(false);
                    }
                }
                if *state.p2p_enabled.read() {
                    div { class: "field",
                        label { class: "field-label", r#for: "p2p-up", "Upload speed limit (KB/s)" }
                        input {
                            id: "p2p-up",
                            r#type: "number",
                            class: "field-input",
                            min: "0",
                            value: "{state.p2p_upload_speed}",
                            disabled: *state.saving.read(),
                            oninput: move |event| {
                                if let Ok(value) = event.value().parse::<i64>() {
                                    state.p2p_upload_speed.set(value);
                                    state.saved.set(false);
                                }
                            },
                        }
                        p { class: "field-hint", "0 = no uploading (seeding disabled)." }
                    }
                    div { class: "field",
                        label { class: "field-label", r#for: "p2p-down", "Download speed limit (KB/s)" }
                        input {
                            id: "p2p-down",
                            r#type: "number",
                            class: "field-input",
                            min: "0",
                            value: "{state.p2p_download_speed}",
                            disabled: *state.saving.read(),
                            oninput: move |event| {
                                if let Ok(value) = event.value().parse::<i64>() {
                                    state.p2p_download_speed.set(value);
                                    state.saved.set(false);
                                }
                            },
                        }
                        p { class: "field-hint", "0 = unlimited." }
                    }
                }
            }
        }
    }
}

#[component]
fn StreamingTrickplayPanel() -> Element {
    let mut state = use_context::<StreamingSettingsState>();
    rsx! {
        Card { title: "Trick play while streaming",
            div { style: "display:flex;flex-direction:column;gap:14px",
                ToggleRow {
                    label: "Make seek previews while streaming",
                    description: "Off by default. Shows the small pictures that Jellyfin apps which support trick play display while you skip through a film or episode. Remux has no local copy to prepare them from in advance, so it makes them in the background while someone is watching, by reading the whole file from its source a second time.",
                    checked: *state.trickplay_enabled.read(),
                    disabled: *state.saving.read(),
                    on_change: move |value| {
                        state.trickplay_enabled.set(value);
                        state.saved.set(false);
                    }
                }
                div { style: "border-left:3px solid var(--warning);padding-left:10px",
                    p { class: "field-label", "Before you turn this on:" }
                    ol { class: "field-hint", style: "margin:0;padding-left:18px",
                        li { "The first play of a film or episode can read the whole file a second time from its source, even if only a few minutes are watched. Reading stops a minute after an app reports that play has stopped, and within about five minutes when an app goes silent without reporting it; the pictures made so far are kept, shown on the next play, and the rest are made then. This counts against debrid fair-use limits, usenet quotas and metered connections." }
                        li { "Each play opens a second connection to the source and reads as fast as the source allows. A source that allows one connection at a time may refuse it or interrupt the video being watched. You can turn previews off for one addon, or limit them to some services, where you edit that addon. A play that starts part way into a file, or jumps to a part that has no pictures yet, makes extra short requests to get there, and for a moment each jump holds one more connection to the source." }
                        li { "It uses processor time (about one CPU core while pictures are being made, at low priority) and disk space (about 9 MB for a two-hour film, up to the limit below)." }
                        li { "Pictures arrive gradually, usually several times faster than playing speed, starting at the place being watched, going on to the end and then coming back for the beginning. After a jump to a part that has no pictures yet, they are made there next. Looking far ahead of what has been read shows no picture yet. Once made, they show at once on every later play, in any version whose length gives the same number of pictures (lengths that round up to the same 10 seconds). A version of another length (an extended cut, a sped-up copy, a file holding two episodes) gets its own pictures. Two different cuts of the same length share pictures, so a preview can show the other cut." }
                        li { "While this is on, apps show a preview box for every video. Where no picture has been made (not made yet, an excluded source, or a failed attempt) the box is empty or grey, and some apps may show an older picture." }
                        li { "One video is processed at a time. Other plays wait their turn and may end before their pictures are made. A video whose apps have all been silent for a minute gives way to one that is waiting, and carries on when its app reports again." }
                        li { "Not made for Live TV, IPTV, torrents, sources set to bypass the Remux proxy, videos shorter than 5 minutes or longer than 4 hours. IPTV reached through another addon is not detected: limit that addon's previews to its other services where you edit it." }
                    }
                }
                if *state.trickplay_enabled.read() {
                    div { class: "field",
                        label { class: "field-label", r#for: "trickplay-cache", "Disk space for pictures (MB)" }
                        input {
                            id: "trickplay-cache",
                            r#type: "number",
                            class: "field-input",
                            min: "100",
                            value: "{state.trickplay_cache_mb}",
                            disabled: *state.saving.read(),
                            oninput: move |event| {
                                if let Ok(value) = event.value().parse::<i64>() {
                                    state.trickplay_cache_mb.set(value);
                                    state.saved.set(false);
                                }
                            },
                        }
                        p { class: "field-hint", "When full, the pictures shown longest ago are deleted first. Remux also stops making pictures when less than 2 GB of disk is free. Turning this off stops new pictures; the ones already made are deleted the next time Remux starts." }
                    }
                    div { class: "field",
                        label { class: "field-label", r#for: "trickplay-keep", "Keep pictures for (days)" }
                        input {
                            id: "trickplay-keep",
                            r#type: "number",
                            class: "field-input",
                            min: "0",
                            max: "3650",
                            value: "{state.trickplay_keep_days}",
                            disabled: *state.saving.read(),
                            oninput: move |event| {
                                if let Ok(value) = event.value().parse::<i64>() {
                                    state.trickplay_keep_days.set(value);
                                    state.saved.set(false);
                                }
                            },
                        }
                        p { class: "field-hint", "Pictures not shown for this many days are deleted. 0 never deletes them by age: they then go only when the disk space above is full." }
                    }
                }
            }
        }
    }
}
