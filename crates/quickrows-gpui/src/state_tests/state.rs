//! Focused state tests.

use crate::{
    EditingCell, LoadedDocument, Modal, OperationKind, PendingEditAction, QuickRowsView,
    cache_header_labels, merge_sorted_unique,
};

#[test]
fn foreground_operation_has_one_consistent_state() {
    let mut operation = crate::ForegroundOperation::default();
    assert!(!operation.is_running());
    assert_eq!(operation.kind(), None);
    assert!(operation.cancellation().is_none());

    let cancellation = operation.start(OperationKind::Search);
    assert!(operation.is_running());
    assert_eq!(operation.kind(), Some(OperationKind::Search));
    assert!(!cancellation.is_cancelled());
    assert!(operation.cancel());
    assert!(cancellation.is_cancelled());

    operation.finish();
    assert!(!operation.is_running());
    assert_eq!(operation.kind(), None);
    assert!(operation.cancellation().is_none());
}

#[test]
fn modal_state_replaces_instead_of_stacking_overlays() {
    let mut modal = Modal::Settings;
    assert!(modal.is_active());
    assert!(matches!(modal, Modal::Settings));

    modal = Modal::Destructive(crate::PendingDestructiveAction::Clear);
    assert!(matches!(
        modal,
        Modal::Destructive(crate::PendingDestructiveAction::Clear)
    ));

    modal = Modal::None;
    assert!(!modal.is_active());
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
            let cancellation = view.operation.start(OperationKind::Search);
            view.queries.search.request_id = 2;

            assert!(!view.finish_query_operation(OperationKind::Search, 1));
            assert!(view.operation.is_running());
            assert!(view.operation.cancellation().is_some());
            assert!(!cancellation.is_cancelled());

            assert!(view.finish_query_operation(OperationKind::Search, 2));
            assert!(!view.operation.is_running());
            assert!(view.operation.cancellation().is_none());

            let cancellation = view.operation.start(OperationKind::Search);
            view.queries.search.request_id = 3;
            view.queries.search.request_id = view.queries.search.request_id.wrapping_add(1);
            view.cancel_query_operation(OperationKind::Search);
            assert!(cancellation.is_cancelled());
            assert!(!view.operation.is_running());
            assert!(view.operation.cancellation().is_none());
        })
        .unwrap();
}

#[gpui::test]
fn operation_generation_rejects_a_stale_same_kind_completion(cx: &mut gpui::TestAppContext) {
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
            view.begin_cancellable_operation(OperationKind::Save);
            let first_generation = view.runtime.operation_generation;
            assert!(view.operation_is_current(OperationKind::Save, first_generation));

            view.finish_cancellable_operation();
            view.begin_cancellable_operation(OperationKind::Save);
            let second_generation = view.runtime.operation_generation;
            assert_ne!(first_generation, second_generation);
            assert!(!view.operation_is_current(OperationKind::Save, first_generation));
            assert!(view.operation_is_current(OperationKind::Save, second_generation));
        })
        .unwrap();
}

#[gpui::test]
fn pending_row_mutations_require_close_confirmation(cx: &mut gpui::TestAppContext) {
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
            view.operation.start(OperationKind::Rows);
            assert!(view.is_dirty());
        })
        .unwrap();
}

#[gpui::test]
fn destructive_discard_is_ignored_while_save_is_running(cx: &mut gpui::TestAppContext) {
    use gpui::AppContext as _;

    let window = cx.update(|cx| {
        gpui_component::init(cx);
        cx.open_window(Default::default(), |window, cx| {
            cx.new(|cx| QuickRowsView::new(None, window, cx))
        })
        .unwrap()
    });
    window
        .update(cx, |view, window, cx| {
            view.overlay.modal = Modal::Destructive(crate::PendingDestructiveAction::Clear);
            view.operation.start(OperationKind::Save);

            view.discard_pending_destructive(window, cx);

            assert!(matches!(
                view.overlay.modal,
                Modal::Destructive(crate::PendingDestructiveAction::Clear)
            ));
            assert_eq!(view.operation.kind(), Some(OperationKind::Save));
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
    let header_labels = cache_header_labels(&headers);
    let headers = Arc::from(headers);
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
            view.document.loaded = Some(LoadedDocument {
                document: document.clone(),
                path: path.clone(),
                headers,
                header_labels,
                row_count,
                detected_parse_info,
                parse_info,
                warnings: Vec::new(),
                file_fingerprint: crate::file_fingerprint(&path),
                dirty: false,
            });
            view.editor.editing_cell = Some(EditingCell {
                display_row: 0,
                source_row: 0,
                column: 1,
                initial_value: "2".to_string(),
            });
            view.inputs.edit_input.update(cx, |input, cx| {
                input.set_value("edited".to_string(), window, cx)
            });

            view.sort_column(0, cx);

            assert!(view.editor.editing_cell.is_none());
            assert_eq!(view.editor.pending_cell_commits, 1);
            assert!(matches!(
                view.editor.pending_edit_action,
                Some(PendingEditAction::SortColumn(0))
            ));
            assert!(document.lock().unwrap().sort_spec().is_none());
        })
        .unwrap();
}

#[test]
fn incremental_query_results_merge_in_source_order_without_duplicates() {
    let mut results = vec![1, 4, 8];
    merge_sorted_unique(&mut results, vec![7, 4, 2, 7]);
    assert_eq!(results, vec![1, 2, 4, 7, 8]);

    merge_sorted_unique(&mut results, vec![10, 9]);
    assert_eq!(results, vec![1, 2, 4, 7, 8, 9, 10]);
    merge_sorted_unique(&mut results, Vec::new());
    assert_eq!(results, vec![1, 2, 4, 7, 8, 9, 10]);
}
