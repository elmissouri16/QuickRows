// Application bootstrap and global GPUI registration.
fn open_workspace_window(
    cx: &mut App,
    initial_path: Option<OpenTarget>,
    requests: Arc<Mutex<VecDeque<RuntimeRequest>>>,
    diagnostics_error: Option<String>,
    active_window: Arc<Mutex<Option<gpui::AnyWindowHandle>>>,
) {
    let bounds = Bounds::centered(None, size(px(800.0), px(600.0)), cx);
    cx.open_window(
        WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(bounds)),
            app_id: Some("com.el.csv-viewer".to_string()),
            window_min_size: Some(size(px(640.0), px(420.0))),
            ..Default::default()
        },
        move |window, cx| {
            window.set_window_title(BASE_TITLE);
            let view = cx.new(|cx| QuickRowsView::new(initial_path, window, cx));
            let weak_view = view.downgrade();
            let window_handle = window.window_handle();
            if let Ok(mut active_window) = active_window.lock() {
                *active_window = Some(window_handle);
            }
            view.update(cx, |view, cx| {
                view.runtime.self_weak = Some(weak_view.clone());
                if let Some(diagnostics_error) = diagnostics_error.clone() {
                    view.feedback.error = Some(match view.feedback.error.take() {
                        Some(settings_error) => {
                            format!("{settings_error}; {diagnostics_error}").into()
                        }
                        None => diagnostics_error.into(),
                    });
                }
                view.track_runtime_requests(requests.clone(), window_handle, cx);
                view.track_external_changes(cx);
            });
            window.on_window_should_close(cx, move |_, cx| {
                weak_view
                    .update(cx, |view, cx| {
                        if view.is_dirty() {
                            view.overlay.modal =
                                Modal::Destructive(PendingDestructiveAction::Close);
                            cx.notify();
                            false
                        } else {
                            true
                        }
                    })
                    .unwrap_or(true)
            });
            cx.new(|cx| Root::new(view, window, cx))
        },
    )
    .expect("Failed to open QuickRows window");
}

fn track_windowless_runtime_requests(
    requests: Arc<Mutex<VecDeque<RuntimeRequest>>>,
    diagnostics_error: Option<String>,
    active_window: Arc<Mutex<Option<gpui::AnyWindowHandle>>>,
    cx: &mut App,
) {
    cx.spawn(async move |cx| {
        loop {
            cx.background_executor()
                .timer(Duration::from_millis(150))
                .await;
            let has_requests = requests
                .lock()
                .is_ok_and(|requests| !requests.is_empty());
            if !has_requests {
                continue;
            }
            if cx
                .update(|cx| {
                    let current_window = active_window
                        .lock()
                        .ok()
                        .and_then(|active_window| *active_window);
                    let window_is_alive = current_window.is_some_and(|window_handle| {
                        window_handle.update(cx, |_, _, _| {}).is_ok()
                    });
                    if window_is_alive {
                        return;
                    }

                    let mut initial_path = None;
                    let mut received_request = false;
                    if let Ok(mut pending) = requests.lock() {
                        while let Some(request) = pending.pop_front() {
                            received_request = true;
                            if let RuntimeRequest::Open(path) = request {
                                initial_path = Some(path);
                                break;
                            }
                        }
                    }
                    if !received_request {
                        return;
                    }
                    open_workspace_window(
                        cx,
                        initial_path,
                        requests.clone(),
                        diagnostics_error.clone(),
                        active_window.clone(),
                    );
                })
                .is_err()
            {
                return anyhow::Ok(());
            }
        }
    })
    .detach();
}

pub fn run() {
    let paths = initial_paths();
    let requests = match coordinate_instance(&paths) {
        Ok(Some(requests)) => requests,
        Ok(None) => return,
        Err(error) => {
            eprintln!("{error}");
            return;
        }
    };
    let initial_path = paths.first().cloned();
    if let Ok(mut pending) = requests.lock() {
        pending.extend(paths.into_iter().skip(1).map(RuntimeRequest::Open));
    }
    migrate_legacy_settings();
    let initial_settings = match SettingsStore::new(settings_path()).load() {
        Ok(settings) => settings,
        Err(error) => {
            eprintln!("Unable to load QuickRows settings: {error}");
            AppSettings::default()
        }
    };
    let diagnostics_directory = diagnostics_path();
    let (diagnostics, diagnostics_error) =
        match Diagnostics::new(diagnostics_directory.clone(), false) {
            Ok(diagnostics) => (diagnostics, None),
            Err(error) => {
                eprintln!("QuickRows diagnostics are unavailable: {error}");
                (
                    Diagnostics::disabled(diagnostics_directory),
                    Some(format!("Diagnostics are unavailable: {error}")),
                )
            }
        };
    diagnostics.install_panic_hook();
    let application = Application::new().with_assets(Assets);
    let url_requests = requests.clone();
    application.on_open_urls(move |values| {
        if let Ok(mut requests) = url_requests.lock() {
            requests.push_back(RuntimeRequest::Activate);
            requests.extend(
                values
                    .into_iter()
                    .filter_map(|value| open_target_from_value(&value))
                    .map(RuntimeRequest::Open),
            );
        }
    });
    let active_window: Arc<Mutex<Option<gpui::AnyWindowHandle>>> =
        Arc::new(Mutex::new(None));
    let reopen_requests = requests.clone();
    let reopen_active_window = active_window.clone();
    let reopen_diagnostics_error = diagnostics_error.clone();
    application.on_reopen(move |cx| {
        cx.activate(true);
        let current_window = reopen_active_window
            .lock()
            .ok()
            .and_then(|active_window| *active_window);
        let activated = current_window.is_some_and(|window_handle| {
            window_handle
                .update(cx, |_, window, cx| {
                    window.activate_window();
                    cx.activate(true);
                })
                .is_ok()
        });
        if activated {
            return;
        }
        open_workspace_window(
            cx,
            None,
            reopen_requests.clone(),
            reopen_diagnostics_error.clone(),
            reopen_active_window.clone(),
        );
    });
    application.run(move |cx: &mut App| {
        gpui_component::init(cx);
        let initial_mode = match initial_settings.theme {
            ThemePreference::Light => ThemeMode::Light,
            ThemePreference::Dark => ThemeMode::Dark,
            ThemePreference::System => ThemeMode::from(cx.window_appearance()),
        };
        Theme::change(initial_mode, None, cx);
        cx.activate(true);
        #[cfg(target_os = "macos")]
        let primary = "cmd";
        #[cfg(not(target_os = "macos"))]
        let primary = "ctrl";
        track_windowless_runtime_requests(
            requests.clone(),
            diagnostics_error.clone(),
            active_window.clone(),
            cx,
        );
        cx.bind_keys([
            KeyBinding::new(&format!("{primary}-o"), OpenFile, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-s"), SaveFile, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-shift-s"), SaveFileAs, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-r"), ReloadFile, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-shift-k"), ClearFile, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-w"), CloseWindow, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-q"), QuitApp, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-f"), Find, Some("QuickRows")),
            KeyBinding::new(
                &format!("{primary}-shift-f"),
                ClearSearch,
                Some("QuickRows"),
            ),
            KeyBinding::new("f3", NextMatch, Some("QuickRows")),
            KeyBinding::new("shift-f3", PreviousMatch, Some("QuickRows")),
            KeyBinding::new(
                &format!("{primary}-shift-d"),
                CheckDuplicates,
                Some("QuickRows"),
            ),
            KeyBinding::new(
                &format!("{primary}-shift-t"),
                ToggleTheme,
                Some("QuickRows"),
            ),
            KeyBinding::new(&format!("{primary}-,"), OpenSettings, Some("QuickRows")),
            KeyBinding::new(
                &format!("{primary}-shift-p"),
                OpenParseSettings,
                Some("QuickRows"),
            ),
            KeyBinding::new(&format!("{primary}-i"), ToggleIndex, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-a"), SelectAllRows, Some("QuickRows")),
            KeyBinding::new("escape", ClearRowSelection, Some("QuickRows")),
            KeyBinding::new("enter", ActivateContextMenu, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-c"), CopySelected, Some("QuickRows")),
            KeyBinding::new("delete", DeleteSelectedRows, Some("QuickRows")),
            KeyBinding::new("backspace", DeleteSelectedRows, Some("QuickRows")),
            KeyBinding::new("up", SelectPreviousRow, Some("QuickRows")),
            KeyBinding::new("down", SelectNextRow, Some("QuickRows")),
            KeyBinding::new("left", SelectPreviousColumn, Some("QuickRows")),
            KeyBinding::new("right", SelectNextColumn, Some("QuickRows")),
            KeyBinding::new("shift-left", ExtendPreviousColumn, Some("QuickRows")),
            KeyBinding::new("shift-right", ExtendNextColumn, Some("QuickRows")),
            KeyBinding::new("home", SelectFirstRow, Some("QuickRows")),
            KeyBinding::new("end", SelectLastRow, Some("QuickRows")),
            KeyBinding::new("pageup", SelectPageUp, Some("QuickRows")),
            KeyBinding::new("pagedown", SelectPageDown, Some("QuickRows")),
            KeyBinding::new("shift-up", ExtendPreviousRow, Some("QuickRows")),
            KeyBinding::new("shift-down", ExtendNextRow, Some("QuickRows")),
            KeyBinding::new("shift-home", ExtendFirstRow, Some("QuickRows")),
            KeyBinding::new("shift-end", ExtendLastRow, Some("QuickRows")),
            KeyBinding::new("shift-pageup", ExtendPageUp, Some("QuickRows")),
            KeyBinding::new("shift-pagedown", ExtendPageDown, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-alt-1"), CompactRows, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-alt-2"), DefaultRows, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-alt-3"), SpaciousRows, Some("QuickRows")),
        ]);

        open_workspace_window(
            cx,
            initial_path,
            requests.clone(),
            diagnostics_error.clone(),
            active_window.clone(),
        );
    });
}
