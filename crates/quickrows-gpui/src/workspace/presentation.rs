// Shared labels, parse diagnostics, formatting, and table presentation helpers.
fn settings_section_title(label: &'static str, cx: &App) -> gpui::AnyElement {
    h_flex()
        .pt_1()
        .gap_2()
        .child(
            div()
                .w(px(4.0))
                .h(px(14.0))
                .rounded_full()
                .bg(cx.theme().accent),
        )
        .child(
            div()
                .text_xs()
                .font_weight(gpui::FontWeight::BOLD)
                .text_color(cx.theme().muted_foreground)
                .child(label),
        )
        .child(div().flex_1().h(px(1.0)).bg(cx.theme().border))
        .into_any_element()
}

fn query_scope_dropdown(
    id: &'static str,
    label: SharedString,
    headers: Arc<[SharedString]>,
    selected: Option<usize>,
    disabled: bool,
    kind: QueryScopeKind,
    view: WeakEntity<QuickRowsView>,
) -> impl IntoElement {
    Button::new(id)
        .compact()
        .ghost()
        .label(format!("{label}  ▾"))
        .disabled(disabled)
        .dropdown_menu(move |menu, _, _| {
            let entire_row_view = view.clone();
            let menu = menu.item(
                PopupMenuItem::new("Entire row")
                    .checked(selected.is_none())
                    .on_click(move |_, _, cx| {
                        let _ = entire_row_view.update(cx, |this, cx| match kind {
                            QueryScopeKind::Search => this.select_search_column(None, cx),
                            QueryScopeKind::Duplicates => this.select_duplicate_column(None, cx),
                        });
                    }),
            );
            headers
                .iter()
                .enumerate()
                .fold(menu, |menu, (column, label)| {
                    let view = view.clone();
                    menu.item(
                        PopupMenuItem::new(label.clone())
                            .checked(selected == Some(column))
                            .on_click(move |_, _, cx| {
                                let _ = view.update(cx, |this, cx| match kind {
                                    QueryScopeKind::Search => {
                                        this.select_search_column(Some(column), cx)
                                    }
                                    QueryScopeKind::Duplicates => {
                                        this.select_duplicate_column(Some(column), cx)
                                    }
                                });
                            }),
                    )
                })
        })
}

fn settings_dropdown(
    id: &'static str,
    label: impl Into<SharedString>,
    width: Pixels,
    view: WeakEntity<QuickRowsView>,
    choices: Vec<(&'static str, bool, SettingsChoice)>,
) -> impl IntoElement {
    Button::new(id)
        .w(width)
        .label(label)
        .dropdown_menu(move |menu, _, _| {
            choices.iter().fold(menu, |menu, (label, checked, choice)| {
                let view = view.clone();
                let choice = *choice;
                menu.item(PopupMenuItem::new(*label).checked(*checked).on_click(
                    move |_, window, cx| {
                        let _ = view.update(cx, |this, cx| {
                            this.apply_settings_choice(choice, window, cx)
                        });
                    },
                ))
            })
        })
}

fn settings_row(label: &'static str, control: impl IntoElement, cx: &App) -> gpui::AnyElement {
    h_flex()
        .min_h(px(48.0))
        .p_3()
        .gap_4()
        .rounded(px(9.0))
        .border_1()
        .border_color(cx.theme().border)
        .bg(cx.theme().background)
        .shadow_sm()
        .child(
            div()
                .flex_1()
                .min_w_0()
                .text_sm()
                .font_weight(gpui::FontWeight::MEDIUM)
                .text_color(cx.theme().foreground)
                .child(label),
        )
        .child(control)
        .into_any_element()
}

fn settings_description(text: impl Into<SharedString>, cx: &App) -> gpui::AnyElement {
    div()
        .mt(px(-12.0))
        .px_3()
        .max_w(px(430.0))
        .text_sm()
        .line_height(relative(1.45))
        .text_color(cx.theme().muted_foreground)
        .child(text.into())
        .into_any_element()
}

fn settings_parse_diagnostic(label: &'static str, summary: String, cx: &App) -> gpui::AnyElement {
    v_flex()
        .gap_1()
        .child(
            div()
                .text_xs()
                .font_weight(gpui::FontWeight::SEMIBOLD)
                .text_color(cx.theme().muted_foreground)
                .child(label),
        )
        .child(
            div()
                .text_sm()
                .line_height(relative(1.45))
                .text_color(cx.theme().foreground)
                .child(summary),
        )
        .into_any_element()
}

fn parse_diagnostic_rows(
    detected: String,
    effective_changes: Vec<String>,
) -> Vec<(&'static str, String)> {
    if effective_changes.is_empty() {
        vec![("Detected and effective settings", detected)]
    } else {
        vec![
            ("Detected from file", detected),
            ("Overrides in effect", effective_changes.join(" · ")),
        ]
    }
}

fn settings_parse_diagnostics(
    detected: String,
    effective_changes: Vec<String>,
    cx: &App,
) -> gpui::AnyElement {
    let rows = parse_diagnostic_rows(detected, effective_changes);
    let mut panel = v_flex()
        .gap_3()
        .p_3()
        .rounded(px(9.0))
        .border_1()
        .border_color(cx.theme().border)
        .bg(cx.theme().background)
        .shadow_sm();

    for (index, (label, summary)) in rows.into_iter().enumerate() {
        if index > 0 {
            panel = panel.child(div().h(px(1.0)).w_full().bg(cx.theme().border));
        }
        panel = panel.child(settings_parse_diagnostic(label, summary, cx));
    }
    panel.into_any_element()
}

fn is_named_delimiter(value: &str) -> bool {
    matches!(
        value.to_lowercase().as_str(),
        "comma" | "tab" | "semicolon" | "pipe" | "space"
    )
}

fn is_named_quote(value: &str) -> bool {
    matches!(value.to_lowercase().as_str(), "double" | "single")
}

fn is_named_escape(value: &str) -> bool {
    matches!(value.to_lowercase().as_str(), "none" | "off" | "backslash")
}

fn is_named_comment(value: &str) -> bool {
    matches!(value.to_lowercase().as_str(), "none" | "off" | "hash" | "#")
}

fn is_valid_syntax_character(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(ch) if !matches!(ch, '\0' | '\r' | '\n')) && chars.next().is_none()
}

fn cell_matches_search(value: &str, query: &str, match_case: bool, whole_word: bool) -> bool {
    if query.is_empty() {
        return false;
    }
    if match_case {
        if whole_word {
            value == query
        } else {
            value.contains(query)
        }
    } else {
        let value = value.to_lowercase();
        let query = query.to_lowercase();
        if whole_word {
            value == query
        } else {
            value.contains(&query)
        }
    }
}

const TOOLBAR_LABEL_BREAKPOINT: f32 = 900.0;

fn toolbar_shows_labels(viewport_width: f32) -> bool {
    viewport_width >= TOOLBAR_LABEL_BREAKPOINT
}

fn toolbar_divider(cx: &App) -> gpui::Div {
    div()
        .flex_none()
        .w(px(1.0))
        .h(px(24.0))
        .ml_1()
        .mr_1()
        .bg(cx.theme().border)
}

fn format_count(count: usize) -> String {
    let digits = count.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len().saturating_sub(1) / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            formatted.push(',');
        }
        formatted.push(digit);
    }
    formatted
}

fn counted_noun(count: usize, singular: &'static str, plural: &'static str) -> &'static str {
    if count == 1 { singular } else { plural }
}

fn merge_sorted_unique(target: &mut Vec<usize>, mut incoming: Vec<usize>) {
    if incoming.is_empty() {
        return;
    }
    incoming.sort_unstable();
    incoming.dedup();
    if target.is_empty() {
        *target = incoming;
        return;
    }
    if target.last() < incoming.first() {
        target.extend(incoming);
        return;
    }

    let mut merged = Vec::with_capacity(target.len().saturating_add(incoming.len()));
    let (mut left, mut right) = (0, 0);
    while left < target.len() || right < incoming.len() {
        let value = match (target.get(left), incoming.get(right)) {
            (Some(left_value), Some(right_value)) if left_value <= right_value => {
                left += 1;
                *left_value
            }
            (Some(_), Some(right_value)) => {
                right += 1;
                *right_value
            }
            (Some(left_value), None) => {
                left += 1;
                *left_value
            }
            (None, Some(right_value)) => {
                right += 1;
                *right_value
            }
            (None, None) => break,
        };
        if merged.last().copied() != Some(value) {
            merged.push(value);
        }
    }
    *target = merged;
}

fn query_result_label(
    current: usize,
    result_count: usize,
    has_completed: bool,
    empty_label: &str,
) -> Option<String> {
    if result_count > 0 {
        Some(format!(
            "{} of {}",
            format_count(current + 1),
            format_count(result_count)
        ))
    } else if has_completed {
        Some(empty_label.to_string())
    } else {
        None
    }
}

fn display_header_label(header: &str, column: usize) -> String {
    let label = header.split_whitespace().collect::<Vec<_>>().join(" ");
    if label.is_empty() {
        format!("Column {}", column + 1)
    } else {
        label
    }
}

fn cache_header_labels(headers: &[String]) -> Arc<[SharedString]> {
    Arc::from(
        headers
            .iter()
            .enumerate()
            .map(|(column, header)| SharedString::from(display_header_label(header, column)))
            .collect::<Vec<_>>(),
    )
}

fn column_scope_label(column: Option<usize>, headers: &[SharedString]) -> SharedString {
    match column {
        Some(column) => headers
            .get(column)
            .cloned()
            .unwrap_or_else(|| SharedString::from(format!("Column {}", column + 1))),
        None => SharedString::from("Entire row"),
    }
}

fn override_label(value: Option<&str>) -> String {
    value
        .map(|value| value.replace('-', " "))
        .unwrap_or_else(|| "Automatic".to_string())
}

fn visible_control(value: &str) -> &str {
    match value {
        "\t" => "Tab",
        " " => "Space",
        value => value,
    }
}

fn size_override_label(value: Option<usize>) -> String {
    value
        .map(|value| format!("{} MiB", value / (1 << 20)))
        .unwrap_or_else(|| "Default".to_string())
}

fn delimiter_label(value: &str) -> String {
    match value {
        "," => "Comma".to_string(),
        "\t" => "Tab".to_string(),
        ";" => "Semicolon".to_string(),
        "|" => "Pipe".to_string(),
        " " => "Space".to_string(),
        value => visible_control(value).to_string(),
    }
}

fn quote_label(value: &str) -> String {
    match value {
        "\"" => "Double quote".to_string(),
        "'" => "Single quote".to_string(),
        value => format!("{} quote", visible_control(value)),
    }
}

fn optional_character_label(value: Option<&str>) -> String {
    match value {
        None => "None".to_string(),
        Some("\\") => "Backslash".to_string(),
        Some(value) => visible_control(value).to_string(),
    }
}

fn malformed_rows_label(value: &str) -> String {
    match value {
        "strict" => "Reject".to_string(),
        "skip" => "Skip".to_string(),
        "repair" => "Repair".to_string(),
        value => value.to_string(),
    }
}

fn parse_limit_label(value: usize) -> String {
    if value == usize::MAX {
        "Default".to_string()
    } else {
        format!("{} MiB", value / (1 << 20))
    }
}

fn parse_summary(info: &ParseInfo) -> String {
    let comments = info
        .comment
        .as_deref()
        .map(|value| format!("{} comments", visible_control(value)))
        .unwrap_or_else(|| "No comments".to_string());
    format!(
        "{} delimiter · {} · {} · {}\n{} · {comments} · Excel sep= {} · {} malformed rows",
        delimiter_label(&info.delimiter),
        quote_label(&info.quote),
        info.encoding,
        info.line_ending.to_uppercase(),
        if info.has_headers {
            "First row is header"
        } else {
            "No headers"
        },
        if info.excel_sep { "on" } else { "off" },
        malformed_rows_label(&info.malformed),
    )
}

fn parse_effective_changes(detected: &ParseInfo, effective: &ParseInfo) -> Vec<String> {
    let mut changes = Vec::new();
    if detected.delimiter != effective.delimiter {
        changes.push(format!(
            "Delimiter: {}",
            delimiter_label(&effective.delimiter)
        ));
    }
    if detected.quote != effective.quote {
        changes.push(format!("Quote: {}", quote_label(&effective.quote)));
    }
    if detected.escape != effective.escape {
        changes.push(format!(
            "Escape: {}",
            optional_character_label(effective.escape.as_deref())
        ));
    }
    if detected.comment != effective.comment {
        changes.push(format!(
            "Comments: {}",
            optional_character_label(effective.comment.as_deref())
        ));
    }
    if detected.encoding != effective.encoding {
        changes.push(format!("Encoding: {}", effective.encoding));
    }
    if detected.line_ending != effective.line_ending {
        changes.push(format!(
            "Line ending: {}",
            effective.line_ending.to_uppercase()
        ));
    }
    if detected.has_headers != effective.has_headers {
        changes.push(format!(
            "Headers: {}",
            if effective.has_headers {
                "First row"
            } else {
                "None"
            }
        ));
    }
    if detected.excel_sep != effective.excel_sep {
        changes.push(format!(
            "Excel sep=: {}",
            if effective.excel_sep { "On" } else { "Off" }
        ));
    }
    if detected.malformed != effective.malformed {
        changes.push(format!(
            "Malformed rows: {}",
            malformed_rows_label(&effective.malformed)
        ));
    }
    if detected.max_field_size != effective.max_field_size {
        changes.push(format!(
            "Field limit: {}",
            parse_limit_label(effective.max_field_size)
        ));
    }
    if detected.max_record_size != effective.max_record_size {
        changes.push(format!(
            "Record limit: {}",
            parse_limit_label(effective.max_record_size)
        ));
    }
    changes
}

fn parse_warning_location(warning: &ParseWarning) -> String {
    let mut parts = Vec::new();
    if let Some(record) = warning.record {
        parts.push(format!("record {record}"));
    }
    if let Some(line) = warning.line {
        parts.push(format!("line {line}"));
    }
    if let Some(field) = warning.field {
        parts.push(format!("field {field}"));
    }
    if let Some(byte) = warning.byte {
        parts.push(format!("byte {byte}"));
    }
    if let (Some(expected), Some(actual)) = (warning.expected_len, warning.len) {
        parts.push(format!("expected {expected} fields, found {actual}"));
    }
    parts.push(warning.kind.clone());
    parts.join(" · ")
}

fn column_render_plan(
    layout: &ColumnLayout,
    scroll_left: f32,
    viewport_width: f32,
    leading_width: f32,
    pinned_columns: impl IntoIterator<Item = usize>,
) -> ColumnRenderPlan {
    let column_count = layout.widths.len();
    if column_count == 0 {
        return ColumnRenderPlan::default();
    }

    let scroll_left = if scroll_left.is_finite() {
        scroll_left.max(0.0)
    } else {
        0.0
    };
    let viewport_width = if viewport_width.is_finite() {
        viewport_width.max(0.0)
    } else {
        0.0
    };
    let leading_width = if leading_width.is_finite() {
        leading_width.max(0.0)
    } else {
        0.0
    };
    let total_width = layout.total_width();
    let left = (scroll_left - leading_width - COLUMN_OVERSCAN_WIDTH)
        .max(0.0)
        .min(total_width);
    let right = (scroll_left + viewport_width - leading_width + COLUMN_OVERSCAN_WIDTH)
        .max(0.0)
        .min(total_width);

    let first = layout.offsets[1..].partition_point(|end| *end <= left);
    let end = layout.offsets[..column_count].partition_point(|start| *start < right);
    let mut runs = Vec::new();
    if first < end {
        runs.push(first..end);
    }
    runs.extend(
        pinned_columns
            .into_iter()
            .filter(|column| *column < column_count)
            .map(|column| column..column + 1),
    );
    runs.sort_unstable_by_key(|run| run.start);

    let mut merged: Vec<std::ops::Range<usize>> = Vec::with_capacity(runs.len());
    for run in runs {
        if let Some(previous) = merged.last_mut()
            && run.start <= previous.end
        {
            previous.end = previous.end.max(run.end);
        } else {
            merged.push(run);
        }
    }
    ColumnRenderPlan { runs: merged }
}

fn column_spacer(width: f32) -> gpui::Div {
    div()
        .w(px(width))
        .min_w(px(width))
        .max_w(px(width))
        .h_full()
        .flex_none()
}

fn virtual_column_children(
    layout: &ColumnLayout,
    plan: &ColumnRenderPlan,
    mut render_column: impl FnMut(usize) -> gpui::AnyElement,
) -> Vec<gpui::AnyElement> {
    let mut children = Vec::new();
    let mut cursor = 0;
    for run in &plan.runs {
        let start = run.start.min(layout.widths.len()).max(cursor);
        let end = run.end.min(layout.widths.len()).max(start);
        if start > cursor {
            children.push(
                column_spacer(layout.offsets[start] - layout.offsets[cursor]).into_any_element(),
            );
        }
        children.extend((start..end).map(&mut render_column));
        cursor = end;
    }
    if cursor < layout.widths.len() {
        children
            .push(column_spacer(layout.total_width() - layout.offsets[cursor]).into_any_element());
    }
    children
}

fn header_cell(label: impl Into<SharedString>, width: f32, cx: &App) -> gpui::Div {
    div()
        .w(px(width))
        .min_w(px(width))
        .max_w(px(width))
        .flex_none()
        .h_full()
        .px_2()
        .flex()
        .items_center()
        .font_weight(gpui::FontWeight::SEMIBOLD)
        .text_color(cx.theme().secondary_foreground)
        .overflow_hidden()
        .whitespace_nowrap()
        .text_ellipsis()
        .child(label.into())
}

fn body_cell_frame(width: f32, cx: &App) -> gpui::Div {
    div()
        .w(px(width))
        .min_w(px(width))
        .max_w(px(width))
        .flex_none()
        .h_full()
        .flex()
        .items_center()
        .border_r_1()
        .border_color(cx.theme().border)
        .overflow_hidden()
}

fn body_cell(value: impl Into<SharedString>, width: f32, cx: &App) -> gpui::Div {
    body_cell_frame(width, cx)
        .px_2()
        .text_ellipsis()
        .child(value.into())
}
