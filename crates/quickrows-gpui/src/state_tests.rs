#[cfg(test)]
mod tests {
    use super::super::{
        ContextMenuCommand, EditingCell, LoadedDocument, OpenTarget, OperationKind,
        PendingEditAction, QuickRowsView, RuntimeRequest, ShowShortcuts, TableContextMenuKind,
        context_menu_command, context_menu_item_count, counted_noun, display_header_label,
        file_fingerprint, format_count, fragment_regions_to_selection, is_valid_syntax_character,
        load_settings_for_window, open_target_from_value, parse_diagnostic_rows,
        parse_effective_changes, parse_summary, path_from_open_value, query_result_label,
        requeue_deferred_runtime_requests, toolbar_shows_labels, validate_syntax_overrides,
    };

    #[gpui::test]
    fn focused_workspace_dispatches_contextual_keyboard_shortcuts(cx: &mut gpui::TestAppContext) {
        use gpui::{AppContext as _, KeyBinding, Keystroke};

        let window = cx.update(|cx| {
            gpui_component::init(cx);
            cx.open_window(Default::default(), |window, cx| {
                let view = cx.new(|cx| QuickRowsView::new(None, window, cx));
                let weak_view = view.downgrade();
                view.update(cx, |view, _| view.self_weak = Some(weak_view));
                view
            })
            .unwrap()
        });
        cx.update(|cx| {
            cx.bind_keys([KeyBinding::new("ctrl-g", ShowShortcuts, Some("QuickRows"))]);
        });
        cx.dispatch_keystroke(*window, Keystroke::parse("ctrl-g").unwrap());
        window
            .update(cx, |view, _, _| assert!(view.show_shortcuts))
            .unwrap();
    }

    #[gpui::test]
    fn stale_query_completion_does_not_clear_the_current_operation(cx: &mut gpui::TestAppContext) {
        use gpui::AppContext as _;

        let window = cx.update(|cx| {
            gpui_component::init(cx);
            cx.open_window(Default::default(), |window, cx| {
                cx.new(|cx| QuickRowsView::new(None, window, cx))
            })
            .unwrap()
        });
        window
            .update(cx, |view, _, _| {
                let cancellation = quickrows_core::CancellationToken::new();
                view.loading = true;
                view.search_request_id = 2;
                view.operation_kind = Some(OperationKind::Search);
                view.operation_cancellation = Some(cancellation.clone());

                assert!(!view.finish_query_operation(OperationKind::Search, 1));
                assert!(view.loading);
                assert!(view.operation_cancellation.is_some());
                assert!(!cancellation.is_cancelled());

                assert!(view.finish_query_operation(OperationKind::Search, 2));
                assert!(!view.loading);
                assert!(view.operation_cancellation.is_none());

                let cancellation = quickrows_core::CancellationToken::new();
                view.loading = true;
                view.search_request_id = 3;
                view.operation_kind = Some(OperationKind::Search);
                view.operation_cancellation = Some(cancellation.clone());
                view.search_request_id = view.search_request_id.wrapping_add(1);
                view.cancel_query_operation(OperationKind::Search);
                assert!(cancellation.is_cancelled());
                assert!(!view.loading);
                assert!(view.operation_cancellation.is_none());
            })
            .unwrap();
    }

    #[gpui::test]
    fn sorting_commits_an_active_cell_edit_before_reordering(cx: &mut gpui::TestAppContext) {
        use gpui::AppContext as _;
        use std::sync::{Arc, Mutex};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sort-edit.csv");
        std::fs::write(&path, "name,value\nb,2\na,1\n").unwrap();
        let document = quickrows_core::CsvDocument::open(&path, None, None).unwrap();
        let headers = document.metadata().headers.clone();
        let row_count = document.row_count();
        let parse_info = document.metadata().effective.clone();
        let detected_parse_info = document.metadata().detected.clone();
        let document = Arc::new(Mutex::new(document));

        let window = cx.update(|cx| {
            gpui_component::init(cx);
            cx.open_window(Default::default(), |window, cx| {
                cx.new(|cx| QuickRowsView::new(None, window, cx))
            })
            .unwrap()
        });
        window
            .update(cx, |view, window, cx| {
                view.loaded = Some(LoadedDocument {
                    document: document.clone(),
                    path: path.clone(),
                    headers,
                    row_count,
                    detected_parse_info,
                    parse_info,
                    warnings: Vec::new(),
                    file_fingerprint: super::super::file_fingerprint(&path),
                    dirty: false,
                });
                view.editing_cell = Some(EditingCell {
                    display_row: 0,
                    source_row: 0,
                    column: 1,
                    initial_value: "2".to_string(),
                });
                view.edit_input.update(cx, |input, cx| {
                    input.set_value("edited".to_string(), window, cx)
                });

                view.sort_column(0, cx);

                assert!(view.editing_cell.is_none());
                assert_eq!(view.pending_cell_commits, 1);
                assert!(matches!(
                    view.pending_edit_action,
                    Some(PendingEditAction::SortColumn(0))
                ));
                assert!(document.lock().unwrap().sort_spec().is_none());
            })
            .unwrap();
    }

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
                view.update(cx, |view, _| view.self_weak = Some(weak_view));
                view
            })
            .unwrap()
        });
        window
            .update(cx, |view, _, cx| {
                view.loaded = Some(LoadedDocument {
                    document: Arc::new(Mutex::new(document)),
                    path: path.clone(),
                    headers,
                    row_count,
                    detected_parse_info: detected,
                    parse_info: effective,
                    warnings: Vec::new(),
                    file_fingerprint: super::super::file_fingerprint(&path),
                    dirty: false,
                });
                view.show_settings = true;
                let rendered = view.render_settings(cx);
                drop(rendered);
                assert!(view.show_settings);
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
    fn deferred_runtime_request_preserves_the_remaining_batch() {
        use std::collections::VecDeque;
        use std::sync::Mutex;

        let current = OpenTarget::from(std::path::PathBuf::from("first.csv"));
        let remaining_open = OpenTarget::from(std::path::PathBuf::from("second.csv"));
        let later_open = OpenTarget::from(std::path::PathBuf::from("third.csv"));
        let requests = Mutex::new(VecDeque::from([RuntimeRequest::Open(later_open)]));
        requeue_deferred_runtime_requests(
            &requests,
            RuntimeRequest::Open(current),
            VecDeque::from([
                RuntimeRequest::Activate,
                RuntimeRequest::Open(remaining_open),
            ]),
        );

        let mut requests = requests.into_inner().unwrap();
        assert!(
            matches!(requests.pop_front(), Some(RuntimeRequest::Open(target)) if target.path == std::path::Path::new("first.csv"))
        );
        assert!(matches!(
            requests.pop_front(),
            Some(RuntimeRequest::Activate)
        ));
        assert!(
            matches!(requests.pop_front(), Some(RuntimeRequest::Open(target)) if target.path == std::path::Path::new("second.csv"))
        );
        assert!(
            matches!(requests.pop_front(), Some(RuntimeRequest::Open(target)) if target.path == std::path::Path::new("third.csv"))
        );
        assert!(requests.is_empty());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn non_utf8_paths_survive_cli_and_instance_encoding() {
        use super::super::{decode_open_target, encode_open_target, open_target_from_os_value};
        use std::os::unix::ffi::OsStringExt;

        let dir = tempfile::tempdir().unwrap();
        let mut name = b"rows-".to_vec();
        name.push(0xff);
        name.extend_from_slice(b".csv");
        let path = dir.path().join(std::ffi::OsString::from_vec(name));
        std::fs::write(&path, "a,b\n1,2\n").unwrap();

        let target = open_target_from_os_value(path.as_os_str()).unwrap();
        assert_eq!(target.path, path);
        let encoded = encode_open_target(&target);
        assert_eq!(decode_open_target(&encoded).unwrap().path, target.path);
    }

    #[test]
    fn file_urls_preserve_unicode_paths() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("résumé-日本語.csv");
        std::fs::write(&path, "a,b\n1,2\n").unwrap();
        let url = url::Url::from_file_path(&path).unwrap();
        assert_eq!(path_from_open_value(url.as_str()), Some(path));
    }

    #[test]
    fn file_url_fragments_are_percent_decoded_and_retained() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fragment.csv");
        std::fs::write(&path, "a,b\n1,2\n3,4\n").unwrap();
        let mut url = url::Url::from_file_path(&path).unwrap().to_string();
        url.push_str("#row%3D2-3");
        let target = open_target_from_value(&url).unwrap();
        assert_eq!(target.path, path);
        assert_eq!(target.fragment.unwrap().to_string(), "row=2-3");
    }

    #[test]
    fn rfc7111_regions_map_header_coordinates_to_visible_selection() {
        use quickrows_core::ResolvedFragmentRegion;

        let regions = vec![
            ResolvedFragmentRegion::Rows(0..=2),
            ResolvedFragmentRegion::Cells {
                rows: 2..=3,
                columns: 1..=2,
            },
        ];
        let (rows, cells) = fragment_regions_to_selection(&regions, 4, 3, true);
        assert_eq!(rows, vec![0..=1, 1..=2]);
        assert_eq!(cells, Some((1, 1, 2, 2)));

        let (rows, cells) =
            fragment_regions_to_selection(&[ResolvedFragmentRegion::Columns(1..=2)], 4, 3, true);
        assert_eq!(rows, vec![0..=3]);
        assert_eq!(cells, Some((0, 1, 3, 2)));
    }

    #[test]
    fn context_menu_keyboard_order_skips_unavailable_editing() {
        let editable = TableContextMenuKind::Cell { can_edit: true };
        let readonly = TableContextMenuKind::Cell { can_edit: false };
        let row = TableContextMenuKind::Row;
        assert_eq!(context_menu_item_count(editable), 6);
        assert_eq!(context_menu_item_count(readonly), 5);
        assert_eq!(context_menu_item_count(row), 3);
        assert_eq!(
            context_menu_command(editable, 2),
            ContextMenuCommand::EditCell
        );
        assert_eq!(
            context_menu_command(readonly, 2),
            ContextMenuCommand::DeleteRows
        );
        assert_eq!(
            context_menu_command(row, 1),
            ContextMenuCommand::RestoreRows
        );
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
    fn display_headers_collapse_embedded_and_trailing_line_breaks() {
        assert_eq!(display_header_label("Image图片\n", 2), "Image图片");
        assert_eq!(
            display_header_label("Photos Link网页链接\r\n", 4),
            "Photos Link网页链接"
        );
        assert_eq!(
            display_header_label("Moxa product\nrequirements\n", 5),
            "Moxa product requirements"
        );
        assert_eq!(display_header_label(" \n\t", 6), "Column 7");
    }

    #[test]
    fn counted_nouns_use_singular_only_for_one() {
        assert_eq!(counted_noun(0, "row", "rows"), "rows");
        assert_eq!(counted_noun(1, "row", "rows"), "row");
        assert_eq!(counted_noun(2, "row", "rows"), "rows");
    }

    #[test]
    fn large_counts_use_grouping_separators() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_000), "1,000");
        assert_eq!(format_count(1_000_000), "1,000,000");
        assert_eq!(format_count(12_345_678), "12,345,678");
    }

    #[test]
    fn toolbar_labels_follow_the_desktop_breakpoint() {
        assert!(!toolbar_shows_labels(899.0));
        assert!(toolbar_shows_labels(900.0));
        assert!(toolbar_shows_labels(1_200.0));
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
            validate_syntax_overrides(
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
            validate_syntax_overrides(
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
            validate_syntax_overrides(
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

    #[test]
    fn query_result_labels_distinguish_pending_and_completed_empty_states() {
        assert_eq!(query_result_label(0, 0, false, "No matches"), None);
        assert_eq!(
            query_result_label(0, 0, true, "No matches"),
            Some("No matches".to_string())
        );
        assert_eq!(
            query_result_label(1, 3, false, "No matches"),
            Some("2 of 3".to_string())
        );
        assert_eq!(
            query_result_label(999_999, 1_000_000, true, "No matches"),
            Some("1,000,000 of 1,000,000".to_string())
        );
    }

    #[test]
    fn fingerprints_change_after_external_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("external.csv");
        std::fs::write(&path, "a\n1\n").unwrap();
        let before = file_fingerprint(&path).unwrap();
        std::fs::write(&path, "a\n123456\n").unwrap();
        let after = file_fingerprint(&path).unwrap();
        assert_ne!(before, after);
    }
}
