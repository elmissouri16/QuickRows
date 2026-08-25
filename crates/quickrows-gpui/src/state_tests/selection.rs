//! Focused selection tests.

use crate::{
    ContextMenuCommand, OperationKind, QuickRowsView, TableContextMenuKind, context_menu_command,
    context_menu_item_count, fragment_regions_to_selection,
};

#[gpui::test]
fn query_scope_pickers_select_columns_directly(cx: &mut gpui::TestAppContext) {
    use gpui::AppContext as _;

    let window = cx.update(|cx| {
        gpui_component::init(cx);
        cx.open_window(Default::default(), |window, cx| {
            cx.new(|cx| QuickRowsView::new(None, window, cx))
        })
        .unwrap()
    });
    window
        .update(cx, |view, _, cx| {
            view.queries.search.results = vec![4];
            view.queries.search.completed = true;
            let initial_refresh = view.queries.search.refresh_token;

            view.select_search_column(Some(7), cx);

            assert_eq!(view.queries.search.column, Some(7));
            assert_eq!(
                view.queries.search.refresh_token,
                initial_refresh.wrapping_add(1)
            );
            assert!(view.queries.search.stale);
            assert!(!view.queries.search.completed);

            view.queries.search.completed = true;
            view.queries.search.stale = false;
            let unchanged_refresh = view.queries.search.refresh_token;
            view.select_search_column(Some(7), cx);
            assert_eq!(view.queries.search.refresh_token, unchanged_refresh);
            assert!(!view.queries.search.stale);
            assert!(view.queries.search.completed);

            view.select_search_column(None, cx);
            assert_eq!(view.queries.search.column, None);
            assert_eq!(
                view.queries.search.refresh_token,
                unchanged_refresh.wrapping_add(1)
            );

            view.queries.duplicates.results = vec![9];
            view.queries.duplicates.completed = true;
            view.select_duplicate_column(Some(6), cx);
            assert_eq!(view.queries.duplicates.column, Some(6));
            assert!(view.queries.duplicates.stale);
            assert!(!view.queries.duplicates.completed);

            view.queries.duplicates.stale = false;
            view.queries.duplicates.completed = true;
            view.select_duplicate_column(Some(6), cx);
            assert!(!view.queries.duplicates.stale);
            assert!(view.queries.duplicates.completed);

            view.select_duplicate_column(None, cx);
            assert_eq!(view.queries.duplicates.column, None);
            assert!(view.queries.duplicates.stale);
            assert!(!view.queries.duplicates.completed);

            view.operation.start(OperationKind::Open);
            let blocked_refresh = view.queries.search.refresh_token;
            view.select_search_column(Some(2), cx);
            view.select_duplicate_column(Some(3), cx);
            assert_eq!(view.queries.search.column, None);
            assert_eq!(view.queries.search.refresh_token, blocked_refresh);
            assert_eq!(view.queries.duplicates.column, None);
        })
        .unwrap();
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
