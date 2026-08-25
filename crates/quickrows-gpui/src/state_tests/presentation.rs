//! Focused presentation tests.

use crate::{
    CachedRow, ColumnLayout, column_render_plan, counted_noun, display_header_label, format_count,
    query_result_label, toolbar_shows_labels,
};

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
fn column_layout_caches_clamped_widths_and_prefix_offsets() {
    let mut settings = quickrows_core::AppSettings {
        column_width: 100.0,
        column_widths: vec![80.0, 150.0, 200.0],
        ..Default::default()
    };
    let layout = ColumnLayout::from_settings(4, &settings);

    assert_eq!(&*layout.widths, &[120.0, 150.0, 200.0, 120.0]);
    assert_eq!(&*layout.offsets, &[0.0, 120.0, 270.0, 470.0, 590.0]);
    assert_eq!(layout.total_width(), 590.0);

    let empty = ColumnLayout::from_settings(0, &settings);
    assert!(empty.widths.is_empty());
    assert_eq!(&*empty.offsets, &[0.0]);

    settings.column_widths = vec![f32::MAX, f32::MAX];
    let saturated = ColumnLayout::from_settings(2, &settings);
    assert_eq!(saturated.total_width(), f32::MAX);
}

#[test]
fn column_render_plan_windows_columns_and_keeps_pins_global() {
    let settings = quickrows_core::AppSettings {
        column_width: 120.0,
        column_widths: Vec::new(),
        ..Default::default()
    };
    let layout = ColumnLayout::from_settings(10, &settings);

    assert_eq!(
        column_render_plan(&layout, 0.0, 120.0, 0.0, []).runs,
        vec![0..3]
    );
    assert_eq!(
        column_render_plan(&layout, 360.0, 120.0, 0.0, []).runs,
        vec![1..6]
    );
    assert_eq!(
        column_render_plan(&layout, 1_080.0, 120.0, 0.0, []).runs,
        vec![7..10]
    );
    assert_eq!(
        column_render_plan(&layout, 0.0, 120.0, 72.0, []).runs,
        vec![0..3]
    );

    let pinned = column_render_plan(&layout, 480.0, 120.0, 0.0, [0, 7, 9]);
    assert_eq!(pinned.runs, vec![0..1, 2..8, 9..10]);
    assert_eq!(
        pinned
            .runs
            .iter()
            .flat_map(|run| run.clone())
            .collect::<Vec<_>>(),
        vec![0, 2, 3, 4, 5, 6, 7, 9]
    );
}

#[test]
fn cached_row_clones_share_cells_until_an_optimistic_edit() {
    use gpui::SharedString;
    use std::sync::Arc;

    let mut cached = CachedRow {
        source_row: 4,
        cells: Arc::from([SharedString::from("alpha"), SharedString::from("beta")]),
        deleted: false,
    };
    let rendered = cached.clone();
    assert!(Arc::ptr_eq(&cached.cells, &rendered.cells));

    Arc::make_mut(&mut cached.cells)[1] = SharedString::from("edited");
    assert!(!Arc::ptr_eq(&cached.cells, &rendered.cells));
    assert_eq!(cached.cells[1].as_ref(), "edited");
    assert_eq!(rendered.cells[1].as_ref(), "beta");
}
