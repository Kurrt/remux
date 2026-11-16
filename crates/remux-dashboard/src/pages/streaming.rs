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
    trickplay_rough_first: Signal<bool>,
    trickplay_other_version: Signal<bool>,
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
        trickplay_rough_first: use_signal(|| false),
        trickplay_other_version: use_signal(|| false),
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
                        .trickplay_rough_first
                        .set(
                            cfg.trickplay_rough_first
                                .unwrap_or(false),
                        );
                    load_state
                        .trickplay_other_version
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
            trickplay_rough_first: Some(
                *state
                    .trickplay_rough_first
                    .peek(),
            ),
            trickplay_other_version: Some(
                *state
                    .trickplay_other_version
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
                        li { "While this is on, apps show a preview box for every video. Where no picture has been made (not made yet, an excluded source that no other version stands in for, or a failed attempt) the box is empty or grey, and some apps may show an older picture. With \"Rough previews first\" on, web-based apps can show a rough picture instead while a large video's pictures are still being made." }
                        li { "One video is processed at a time. Other plays wait their turn and may end before their pictures are made. A video whose apps have all been silent for a minute gives way to one that is waiting, and carries on when its app reports again." }
                        li { "Not made for Live TV, IPTV, torrents, sources set to bypass the Remux proxy, videos shorter than 5 minutes or longer than 4 hours. IPTV reached through another addon is not detected: limit that addon's previews to its other services where you edit it. With \"Use another version when the one playing cannot be read\" on, a video from an IPTV addon or one set to bypass the proxy can still get pictures made from another version." }
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
                    ToggleRow {
                        label: "Rough previews first",
                        description: "Off by default. For a large file, Remux first makes up to 17 pictures spread across the whole video (the start, the end, the middle, then the points halfway between), each with its own short request. About a minute into a first play, every place on the seek bar then shows the nearest of them. Detailed pictures take their place as they are made.",
                        checked: *state.trickplay_rough_first.read(),
                        disabled: *state.saving.read(),
                        on_change: move |value| {
                            state.trickplay_rough_first.set(value);
                            state.saved.set(false);
                        }
                    }
                    div { style: "border-left:3px solid var(--warning);padding-left:10px",
                        p { class: "field-label", "Before you turn this on:" }
                        ol { class: "field-hint", style: "margin:0;padding-left:18px",
                            li { "Only the Jellyfin web client shows the rough pictures: in a browser, and in the TV, desktop and iOS apps built on it. A few other apps may show them too and keep them for the rest of a play. Apps that store the pictures they get ignore them and wait for the detailed pictures, as with this off, but are still sent them." }
                            li { "A rough picture can be many minutes away from the place you skip to. It shows roughly where you are in the video, not what is at that place." }
                            li { "A web-based app keeps a rough picture it has shown until you move the seek bar to another part of the video and back, and some may keep it a little longer. Closing the app clears it." }
                            li { "Before the detailed pictures of a video that is not finished yet, Remux makes up to 17 short reads of the source, each a few requests, and the detailed pictures start up to about 45 seconds later. Files under 4 GB (under an hour when the source gives no size), videos whose pictures are nearly finished, and sources that cannot jump within a file or reach its end within 10 seconds get no rough pictures." }
                            li { "Rough pictures are sent inside a \"not ready yet\" answer (HTTP 503), so that apps which store pictures for good do not store them. A reverse proxy set to replace error pages removes them, and previews then behave as with this off." }
                        }
                    }
                    ToggleRow {
                        label: "Use another version when the one playing cannot be read",
                        description: "Off by default. When the version being played cannot be read for pictures (its read failed, or Remux does not read it: an IPTV addon, an addon that bypasses the proxy, or seek previews off for its addon or service), Remux reads another version of the same film or episode instead and shows its pictures.",
                        checked: *state.trickplay_other_version.read(),
                        disabled: *state.saving.read(),
                        on_change: move |value| {
                            state.trickplay_other_version.set(value);
                            state.saved.set(false);
                        }
                    }
                    div { style: "border-left:3px solid var(--warning);padding-left:10px",
                        p { class: "field-label", "Before you turn this on:" }
                        ol { class: "field-hint", style: "margin:0;padding-left:18px",
                            li { "It reads a file that nobody chose to play, up to the whole file, from that file's source. This counts against that source's fair-use limits, quotas and metered connections like a play does." }
                            li { "Only a version that is certain to fit is read: one whose own measured length gives the same number of pictures (lengths that round up to the same 10 seconds), and that is either a file of a library addon (a local folder or a WebDAV server; turn seek previews off for that addon if reading it costs you) or marked as cached by a debrid or usenet service that the addon names for each stream (only some aggregating addons send this). A cached file must also list its size, and the length Remux measured must be of a file of that size. A version that was never played or measured, one not marked cached, a plain link with no service, a torrent, and any addon or service with seek previews off are never read. For many titles no other version qualifies and nothing changes." }
                            li { "The cached mark is as old as the last stream listing. If the file has left the service's cache since, asking for it can start a download in your account before Remux gives up on it. Before reading it, Remux asks its source once for the file's size and leaves it alone when that is not the size listed (for usenet and releases of several files, when it is larger than listed). That one request can itself start the download. If the read then ends short of the end, the pictures it made are deleted." }
                            li { "The other version can be a different cut of the same length, or a file listed under the wrong episode that happens to have the same length, so a preview can show a scene that is not in the version being watched." }
                            li { "At most three versions are read for one play, counting the one being played: files on this server or a WebDAV server first, then cached files, usenet last, and the smallest first within each. Another version that fails is left alone for a day. Reading stops when the viewer stops, as for any video. A user limited to some addons only gets pictures from those addons' versions." }
                            li { "Remux cannot tell by itself that a source allows only one connection. Turn seek previews off for that addon where you edit it: its files are then never opened a second time, and with this setting on their pictures come from another version. Such a version is then listed with seek previews whenever another version can stand in for it; its preview box stays empty if that other version cannot be read." }
                        }
                    }
                }
            }
        }
    }
}
