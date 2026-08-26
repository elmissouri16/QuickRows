//! Rendered GPUI interaction tests for critical workspace flows.

use crate::{Find, Modal, OpenFile, OpenSettings, PendingDestructiveAction, QuickRowsView};
use gpui::AppContext as _;

fn open_test_workspace(
    cx: &mut gpui::TestAppContext,
    settings_path: &std::path::Path,
) -> (
    gpui::WindowHandle<gpui_component::Root>,
    gpui::Entity<QuickRowsView>,
) {
    use std::{cell::RefCell, rc::Rc};

    let captured_view = Rc::new(RefCell::new(None));
    let capture = captured_view.clone();
    let settings_path = settings_path.to_path_buf();
    let window = cx.update(|cx| {
        gpui_component::init(cx);
        gpui_component::Theme::change(gpui_component::ThemeMode::Light, None, cx);
        cx.open_window(Default::default(), move |window, cx| {
            let view = cx.new(|cx| QuickRowsView::new(None, window, cx));
            let weak_view = view.downgrade();
            view.update(cx, |view, _| {
                view.runtime.self_weak = Some(weak_view);
                view.preferences.settings = quickrows_core::AppSettings::default();
                view.preferences.settings.parse_overrides.has_headers = Some(true);
                view.preferences.settings_store =
                    quickrows_core::SettingsStore::new(settings_path.clone());
            });
            *capture.borrow_mut() = Some(view.clone());
            cx.new(|cx| gpui_component::Root::new(view, window, cx))
        })
        .unwrap()
    });
    cx.refresh().unwrap();
    cx.run_until_parked();
    let view = captured_view
        .borrow_mut()
        .take()
        .expect("QuickRows view should be captured");
    (window, view)
}

fn select_path(
    window: gpui::AnyWindowHandle,
    path: Option<std::path::PathBuf>,
    cx: &mut gpui::TestAppContext,
) {
    cx.dispatch_action(window, OpenFile);
    assert!(cx.did_prompt_for_new_path());
    cx.simulate_new_path_selection(|_| path);
    cx.run_until_parked();
    cx.refresh().unwrap();
    cx.run_until_parked();
}

fn click(window: gpui::AnyWindowHandle, selector: &'static str, cx: &mut gpui::TestAppContext) {
    cx.refresh().unwrap();
    cx.run_until_parked();
    let mut visual = gpui::VisualTestContext::from_window(window, cx);
    let bounds = visual
        .debug_bounds(selector)
        .unwrap_or_else(|| panic!("{selector} should be rendered"));
    visual.simulate_click(bounds.center(), gpui::Modifiers::default());
    cx.run_until_parked();
}

#[gpui::test]
fn open_button_cancel_is_safe_and_can_be_repeated(cx: &mut gpui::TestAppContext) {
    let dir = tempfile::tempdir().unwrap();
    let (window, view) = open_test_workspace(cx, &dir.path().join("settings.json"));

    click(*window, "open-csv", cx);
    assert!(cx.did_prompt_for_new_path());
    cx.simulate_new_path_selection(|_| None);
    cx.run_until_parked();
    view.update(cx, |view, _| {
        assert!(view.document.loaded.is_none());
        assert!(view.feedback.error.is_none());
    });

    select_path(*window, None, cx);
    view.update(cx, |view, _| {
        assert!(view.document.loaded.is_none());
        assert!(view.feedback.error.is_none());
    });
}

#[gpui::test]
fn open_action_loads_selected_csv(cx: &mut gpui::TestAppContext) {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("selected.csv");
    std::fs::write(&csv_path, "name,value\nalpha,1\nbeta,2\n").unwrap();
    let (window, view) = open_test_workspace(cx, &dir.path().join("settings.json"));

    select_path(*window, Some(csv_path.clone()), cx);
    view.update(cx, |view, _| {
        let loaded = view
            .document
            .loaded
            .as_ref()
            .expect("selected CSV should be loaded");
        assert_eq!(loaded.path, csv_path);
        assert_eq!(loaded.row_count, 2);
        assert!(view.feedback.error.is_none());
    });
    let mut visual = gpui::VisualTestContext::from_window(*window, cx);
    assert_eq!(
        visual.window_title().as_deref(),
        Some("selected.csv - QuickRows")
    );
}

#[gpui::test]
fn picker_result_still_rejects_non_csv_paths(cx: &mut gpui::TestAppContext) {
    let dir = tempfile::tempdir().unwrap();
    let text_path = dir.path().join("not-csv.txt");
    std::fs::write(&text_path, "name,value\nalpha,1\n").unwrap();
    let (window, view) = open_test_workspace(cx, &dir.path().join("settings.json"));

    select_path(*window, Some(text_path), cx);
    view.update(cx, |view, _| {
        assert!(view.document.loaded.is_none());
        assert_eq!(
            view.feedback.error.as_ref().map(ToString::to_string),
            Some("QuickRows can only open .csv files.".to_string())
        );
    });

    select_path(*window, None, cx);
}

#[gpui::test]
fn picker_errors_are_reported_without_poisoning_the_next_open(cx: &mut gpui::TestAppContext) {
    let dir = tempfile::tempdir().unwrap();
    let (window, view) = open_test_workspace(cx, &dir.path().join("settings.json"));

    view.update(cx, |view, cx| {
        view.finish_open_dialog(Err("Unable to open file dialog: test failure".into()), cx)
    });
    view.update(cx, |view, _| {
        assert_eq!(
            view.feedback.error.as_ref().map(ToString::to_string),
            Some("Unable to open file dialog: test failure".to_string())
        );
    });

    select_path(*window, None, cx);
}

#[gpui::test]
fn settings_switch_and_done_button_update_the_workspace(cx: &mut gpui::TestAppContext) {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("settings.csv");
    let settings_path = dir.path().join("settings.json");
    std::fs::write(&csv_path, "name,value\nalpha,1\n").unwrap();
    let (window, view) = open_test_workspace(cx, &settings_path);
    select_path(*window, Some(csv_path), cx);

    cx.dispatch_action(*window, OpenSettings);
    cx.refresh().unwrap();
    cx.run_until_parked();
    let before = view.update(cx, |view, _| {
        assert!(matches!(view.overlay.modal, Modal::Settings));
        view.preferences.settings.show_index
    });

    click(*window, "settings-index-switch", cx);
    view.update(cx, |view, _| {
        assert_eq!(view.preferences.settings.show_index, !before)
    });
    assert!(settings_path.exists());

    click(*window, "settings-done", cx);
    view.update(cx, |view, _| {
        assert!(matches!(view.overlay.modal, Modal::None))
    });
}

#[gpui::test]
fn find_controls_toggle_options_and_close(cx: &mut gpui::TestAppContext) {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("find.csv");
    std::fs::write(&csv_path, "name,value\nalpha,1\n").unwrap();
    let (window, view) = open_test_workspace(cx, &dir.path().join("settings.json"));
    select_path(*window, Some(csv_path), cx);

    cx.dispatch_action(*window, Find);
    cx.refresh().unwrap();
    cx.run_until_parked();
    view.update(cx, |view, _| {
        assert!(view.queries.show_find);
        assert!(!view.queries.search.match_case);
    });

    click(*window, "search-match-case", cx);
    view.update(cx, |view, _| assert!(view.queries.search.match_case));

    click(*window, "close-find", cx);
    view.update(cx, |view, _| assert!(!view.queries.show_find));
}

#[gpui::test]
fn unsaved_modal_cancel_is_routed_through_the_rendered_button(cx: &mut gpui::TestAppContext) {
    let dir = tempfile::tempdir().unwrap();
    let (window, view) = open_test_workspace(cx, &dir.path().join("settings.json"));
    view.update(cx, |view, cx| {
        view.overlay.modal = Modal::Destructive(PendingDestructiveAction::Clear);
        cx.notify();
    });

    cx.refresh().unwrap();
    cx.run_until_parked();
    let mut visual = gpui::VisualTestContext::from_window(*window, cx);
    assert!(visual.debug_bounds("unsaved-backdrop").is_some());
    drop(visual);

    click(*window, "unsaved-cancel", cx);
    view.update(cx, |view, _| {
        assert!(matches!(view.overlay.modal, Modal::None))
    });
}
