//! Focused parsing settings tests.

use crate::{
    LoadedDocument, Modal, QuickRowsView, cache_header_labels, is_valid_syntax_character,
    load_settings_for_window, parse_diagnostic_rows, parse_effective_changes, parse_summary,
};

#[gpui::test]
fn parsing_diagnostics_surface_renders_detected_values_and_override_changes(
    cx: &mut gpui::TestAppContext,
) {
    use gpui::AppContext as _;
    use std::sync::{Arc, Mutex};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("settings-diagnostics.csv");
    std::fs::write(&path, "name,value\nalpha,1\n").unwrap();
    let document = quickrows_core::CsvDocument::open(&path, None, None).unwrap();
    let headers = document.metadata().headers.clone();
    let header_labels = cache_header_labels(&headers);
    let headers = Arc::from(headers);
    let row_count = document.row_count();
    let mut detected = document.metadata().detected.clone();
    detected.has_headers = false;
    let mut effective = detected.clone();
    effective.has_headers = true;

    let window = cx.update(|cx| {
        gpui_component::init(cx);
        cx.open_window(Default::default(), |window, cx| {
            let view = cx.new(|cx| QuickRowsView::new(None, window, cx));
            let weak_view = view.downgrade();
            view.update(cx, |view, _| view.runtime.self_weak = Some(weak_view));
            view
        })
        .unwrap()
    });
    window
        .update(cx, |view, _, cx| {
            view.document.loaded = Some(LoadedDocument {
                document: Arc::new(Mutex::new(document)),
                path: path.clone(),
                headers,
                header_labels,
                row_count,
                detected_parse_info: detected,
                parse_info: effective,
                warnings: Vec::new(),
                file_fingerprint: crate::file_fingerprint(&path),
                dirty: false,
            });
            view.overlay.modal = Modal::Settings;
            let rendered = view.render_settings(cx);
            drop(rendered);
            assert!(matches!(view.overlay.modal, Modal::Settings));
        })
        .unwrap();
}

#[test]
fn corrupt_settings_are_reported_to_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("settings.json");
    std::fs::write(&path, "{broken").unwrap();
    let store = quickrows_core::SettingsStore::new(path);

    let (settings, error) = load_settings_for_window(&store);

    assert_eq!(
        settings.column_width,
        quickrows_core::AppSettings::default().column_width
    );
    assert!(error.unwrap().contains("Unable to load settings"));
}

#[test]
fn matching_parse_diagnostics_render_once_and_overrides_render_as_comparison() {
    assert_eq!(
        parse_diagnostic_rows("detected".into(), vec![]),
        vec![("Detected and effective settings", "detected".into())]
    );
    assert_eq!(
        parse_diagnostic_rows(
            "detected".into(),
            vec!["Headers: First row".into(), "Malformed rows: Repair".into()],
        ),
        vec![
            ("Detected from file", "detected".into()),
            (
                "Overrides in effect",
                "Headers: First row · Malformed rows: Repair".into(),
            ),
        ]
    );
}

#[test]
fn parse_summaries_use_readable_labels_and_group_related_details() {
    let info = quickrows_core::ParseInfo {
        delimiter: ",".to_string(),
        quote: "\"".to_string(),
        escape: None,
        comment: None,
        excel_sep: false,
        line_ending: "crlf".to_string(),
        encoding: "UTF-8".to_string(),
        has_headers: false,
        malformed: "skip".to_string(),
        max_field_size: usize::MAX,
        max_record_size: usize::MAX,
    };

    assert_eq!(
        parse_summary(&info),
        "Comma delimiter · Double quote · UTF-8 · CRLF\nNo headers · No comments · Excel sep= off · Skip malformed rows"
    );

    let mut effective = info.clone();
    effective.has_headers = true;
    effective.max_record_size = 8 << 20;
    assert_eq!(
        parse_effective_changes(&info, &effective),
        vec![
            "Headers: First row".to_string(),
            "Record limit: 8 MiB".to_string(),
        ]
    );
}

#[test]
fn custom_csv_syntax_accepts_one_unicode_scalar_only() {
    assert!(is_valid_syntax_character("§"));
    assert!(is_valid_syntax_character("🧊"));
    assert!(is_valid_syntax_character("\t"));
    assert!(!is_valid_syntax_character(""));
    assert!(!is_valid_syntax_character("ab"));
    assert!(!is_valid_syntax_character("\n"));
    assert!(!is_valid_syntax_character("\r"));
    assert!(!is_valid_syntax_character("\0"));
}

#[test]
fn syntax_overrides_reject_conflicting_characters_before_reload() {
    use quickrows_core::ParseOverrides;

    assert!(
        quickrows_core::validate_parse_overrides_for_info(
            &ParseOverrides {
                delimiter: Some("|".to_string()),
                quote: Some("|".to_string()),
                ..Default::default()
            },
            None,
        )
        .is_err()
    );
    assert!(
        quickrows_core::validate_parse_overrides_for_info(
            &ParseOverrides {
                delimiter: Some("§".to_string()),
                comment: Some("§".to_string()),
                ..Default::default()
            },
            None,
        )
        .is_err()
    );
    assert!(
        quickrows_core::validate_parse_overrides_for_info(
            &ParseOverrides {
                delimiter: Some("§".to_string()),
                quote: Some("«".to_string()),
                ..Default::default()
            },
            None,
        )
        .is_ok()
    );
}
