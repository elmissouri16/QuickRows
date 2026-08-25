// Settings surface rendering.
impl QuickRowsView {
    fn render_settings(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let theme = self.preferences.settings.theme;
        let density = self.preferences.settings.row_density;
        let parse = &self.preferences.settings.parse_overrides;
        let theme_label = match theme {
            ThemePreference::System => "System",
            ThemePreference::Light => "Light",
            ThemePreference::Dark => "Dark",
        };
        let density_label = match density {
            RowDensity::Compact => "Compact",
            RowDensity::Default => "Default",
            RowDensity::Spacious => "Spacious",
        };
        let delimiter = override_label(parse.delimiter.as_deref());
        let quote = override_label(parse.quote.as_deref());
        let escape = override_label(parse.escape.as_deref());
        let comment = override_label(parse.comment.as_deref());
        let excel_sep = match parse.excel_sep {
            None => "Automatic",
            Some(true) => "Enabled",
            Some(false) => "Disabled",
        };
        let line_ending = override_label(parse.line_ending.as_deref());
        let encoding = override_label(parse.encoding.as_deref());
        let headers = match parse.has_headers {
            None => "Automatic",
            Some(true) => "First row",
            Some(false) => "No header",
        };
        let malformed = override_label(parse.malformed.as_deref());
        let max_field = size_override_label(parse.max_field_size);
        let max_record = size_override_label(parse.max_record_size);
        let show_index = self.preferences.settings.show_index;
        let enable_indexing = self.preferences.settings.enable_indexing;
        let parse_diagnostics = self.document.loaded.as_ref().map(|loaded| {
            (
                parse_summary(&loaded.detected_parse_info),
                parse_effective_changes(&loaded.detected_parse_info, &loaded.parse_info),
                loaded.warnings.clone(),
            )
        });
        let show_warning_details = self.preferences.show_warning_details;

        let select_width = px(142.0);
        let view = self
            .runtime.self_weak
            .clone()
            .expect("QuickRows view identity is initialized before rendering settings");
        let body = v_flex()
            .gap_5()
            .child(settings_section_title("APPEARANCE", cx))
            .child(settings_row(
                "Theme",
                settings_dropdown(
                    "settings-theme-select",
                    format!("{theme_label}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("System", theme == ThemePreference::System, SettingsChoice::Theme(ThemePreference::System)),
                        ("Light", theme == ThemePreference::Light, SettingsChoice::Theme(ThemePreference::Light)),
                        ("Dark", theme == ThemePreference::Dark, SettingsChoice::Theme(ThemePreference::Dark)),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Row Height",
                settings_dropdown(
                    "settings-density-select",
                    format!("{density_label}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Compact", density == RowDensity::Compact, SettingsChoice::Density(RowDensity::Compact)),
                        ("Default", density == RowDensity::Default, SettingsChoice::Density(RowDensity::Default)),
                        ("Spacious", density == RowDensity::Spacious, SettingsChoice::Density(RowDensity::Spacious)),
                    ],
                ),
                cx,
            ))
            .child(settings_section_title("VIEW", cx))
            .child(settings_row(
                "Show Row Numbers",
                Switch::new("settings-index-switch")
                    .checked(show_index)
                    .tooltip("Show row numbers")
                    .on_click({
                        let view = view.clone();
                        move |_, window, cx| {
                            let _ = view.update(cx, |this, cx| {
                                this.toggle_index(&ToggleIndex, window, cx)
                            });
                        }
                    }),
                cx,
            ))
            .child(settings_section_title("SEARCH & PARSING", cx))
            .child(settings_row(
                "Enable Search Indexing",
                Switch::new("settings-indexing-switch")
                    .checked(enable_indexing)
                    .tooltip("Enable search indexing")
                    .on_click({
                        let view = view.clone();
                        move |_, _, cx| {
                            let _ = view.update(cx, |this, cx| this.toggle_search_indexing(cx));
                        }
                    }),
                cx,
            ))
            .child(settings_description(
                "Indexes only the selected column when it is searched and retains one column at a time. Disabled by default to minimize RAM use.",
                cx,
            ))
            .child(settings_row(
                "Delimiter",
                settings_dropdown(
                    "settings-delimiter-select",
                    format!("{delimiter}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.delimiter.is_none(), SettingsChoice::Delimiter(None)),
                        ("Comma", parse.delimiter.as_deref() == Some("comma"), SettingsChoice::Delimiter(Some("comma"))),
                        ("Tab", parse.delimiter.as_deref() == Some("tab"), SettingsChoice::Delimiter(Some("tab"))),
                        ("Semicolon", parse.delimiter.as_deref() == Some("semicolon"), SettingsChoice::Delimiter(Some("semicolon"))),
                        ("Pipe", parse.delimiter.as_deref() == Some("pipe"), SettingsChoice::Delimiter(Some("pipe"))),
                        ("Space", parse.delimiter.as_deref() == Some("space"), SettingsChoice::Delimiter(Some("space"))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Quote Character",
                settings_dropdown(
                    "settings-quote-select",
                    format!("{quote}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.quote.is_none(), SettingsChoice::Quote(None)),
                        ("Double quote", parse.quote.as_deref() == Some("double"), SettingsChoice::Quote(Some("double"))),
                        ("Single quote", parse.quote.as_deref() == Some("single"), SettingsChoice::Quote(Some("single"))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Escape Character",
                settings_dropdown(
                    "settings-escape-select",
                    format!("{escape}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.escape.is_none(), SettingsChoice::Escape(None)),
                        ("None", parse.escape.as_deref() == Some("none"), SettingsChoice::Escape(Some("none"))),
                        ("Backslash", parse.escape.as_deref() == Some("backslash"), SettingsChoice::Escape(Some("backslash"))),
                    ],
                ),
                cx,
            ))
            .child(
                h_flex()
                    .gap_2()
                    .child(Input::new(&self.inputs.custom_delimiter_input).flex_1())
                    .child(
                        Button::new("apply-custom-delimiter")
                            .label("Use delimiter")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.apply_custom_delimiter(cx)
                            })),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(Input::new(&self.inputs.custom_quote_input).flex_1())
                    .child(
                        Button::new("apply-custom-quote")
                            .label("Use quote")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.apply_custom_quote(cx)
                            })),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(Input::new(&self.inputs.custom_escape_input).flex_1())
                    .child(
                        Button::new("apply-custom-escape")
                            .label("Use escape")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.apply_custom_escape(cx)
                            })),
                    ),
            )
            .child(settings_row(
                "Comment Character",
                settings_dropdown(
                    "settings-comment-select",
                    format!("{comment}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.comment.is_none(), SettingsChoice::Comment(None)),
                        ("None", parse.comment.as_deref() == Some("none"), SettingsChoice::Comment(Some("none"))),
                        ("Hash (#)", parse.comment.as_deref() == Some("#"), SettingsChoice::Comment(Some("#"))),
                    ],
                ),
                cx,
            ))
            .child(
                h_flex()
                    .gap_2()
                    .child(Input::new(&self.inputs.custom_comment_input).flex_1())
                    .child(
                        Button::new("apply-custom-comment")
                            .label("Use comment")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.apply_custom_comment(cx)
                            })),
                    ),
            )
            .child(settings_row(
                "Excel sep= Directive",
                settings_dropdown(
                    "settings-excel-sep-select",
                    format!("{excel_sep}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.excel_sep.is_none(), SettingsChoice::ExcelSep(None)),
                        ("Enabled", parse.excel_sep == Some(true), SettingsChoice::ExcelSep(Some(true))),
                        ("Disabled", parse.excel_sep == Some(false), SettingsChoice::ExcelSep(Some(false))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Line Ending",
                settings_dropdown(
                    "settings-lines-select",
                    format!("{line_ending}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.line_ending.is_none(), SettingsChoice::LineEnding(None)),
                        ("LF", parse.line_ending.as_deref() == Some("lf"), SettingsChoice::LineEnding(Some("lf"))),
                        ("CRLF", parse.line_ending.as_deref() == Some("crlf"), SettingsChoice::LineEnding(Some("crlf"))),
                        ("CR", parse.line_ending.as_deref() == Some("cr"), SettingsChoice::LineEnding(Some("cr"))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Encoding",
                settings_dropdown(
                    "settings-encoding-select",
                    format!("{encoding}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.encoding.is_none(), SettingsChoice::Encoding(None)),
                        ("UTF-8", parse.encoding.as_deref() == Some("utf-8"), SettingsChoice::Encoding(Some("utf-8"))),
                        ("UTF-16 LE", parse.encoding.as_deref() == Some("utf-16le"), SettingsChoice::Encoding(Some("utf-16le"))),
                        ("UTF-16 BE", parse.encoding.as_deref() == Some("utf-16be"), SettingsChoice::Encoding(Some("utf-16be"))),
                        ("Windows-1252", parse.encoding.as_deref() == Some("windows-1252"), SettingsChoice::Encoding(Some("windows-1252"))),
                        ("ISO-8859-1", parse.encoding.as_deref() == Some("iso-8859-1"), SettingsChoice::Encoding(Some("iso-8859-1"))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Header Row",
                settings_dropdown(
                    "settings-headers-select",
                    format!("{headers}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.has_headers.is_none(), SettingsChoice::Headers(None)),
                        ("First row", parse.has_headers == Some(true), SettingsChoice::Headers(Some(true))),
                        ("No header", parse.has_headers == Some(false), SettingsChoice::Headers(Some(false))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Malformed Rows",
                settings_dropdown(
                    "settings-malformed-select",
                    format!("{malformed}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.malformed.is_none(), SettingsChoice::Malformed(None)),
                        ("Strict", parse.malformed.as_deref() == Some("strict"), SettingsChoice::Malformed(Some("strict"))),
                        ("Skip", parse.malformed.as_deref() == Some("skip"), SettingsChoice::Malformed(Some("skip"))),
                        ("Repair", parse.malformed.as_deref() == Some("repair"), SettingsChoice::Malformed(Some("repair"))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Field Size Limit",
                settings_dropdown(
                    "settings-field-limit-select",
                    format!("{max_field}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Default", parse.max_field_size.is_none(), SettingsChoice::MaxFieldSize(None)),
                        ("1 MiB", parse.max_field_size == Some(1 << 20), SettingsChoice::MaxFieldSize(Some(1 << 20))),
                        ("8 MiB", parse.max_field_size == Some(8 << 20), SettingsChoice::MaxFieldSize(Some(8 << 20))),
                        ("64 MiB", parse.max_field_size == Some(64 << 20), SettingsChoice::MaxFieldSize(Some(64 << 20))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Record Size Limit",
                settings_dropdown(
                    "settings-record-limit-select",
                    format!("{max_record}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Default", parse.max_record_size.is_none(), SettingsChoice::MaxRecordSize(None)),
                        ("8 MiB", parse.max_record_size == Some(8 << 20), SettingsChoice::MaxRecordSize(Some(8 << 20))),
                        ("64 MiB", parse.max_record_size == Some(64 << 20), SettingsChoice::MaxRecordSize(Some(64 << 20))),
                        ("256 MiB", parse.max_record_size == Some(256 << 20), SettingsChoice::MaxRecordSize(Some(256 << 20))),
                    ],
                ),
                cx,
            ))
            .when_some(
                parse_diagnostics,
                |body, (detected, effective_changes, warnings)| {
                    let warning_count = warnings.len();
                    body.child(settings_parse_diagnostics(
                        detected,
                        effective_changes,
                        cx,
                    ))
                        .when(warning_count > 0, |body| {
                            body.child(
                                Button::new("parse-warning-details")
                                    .ghost()
                                    .label(if show_warning_details {
                                        format!(
                                            "Hide {} {}",
                                            format_count(warning_count),
                                            counted_noun(
                                                warning_count,
                                                "parse warning",
                                                "parse warnings",
                                            )
                                        )
                                    } else {
                                        format!(
                                            "View {} {}",
                                            format_count(warning_count),
                                            counted_noun(
                                                warning_count,
                                                "parse warning",
                                                "parse warnings",
                                            )
                                        )
                                    })
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.preferences.show_warning_details = !this.preferences.show_warning_details;
                                        cx.notify();
                                    })),
                            )
                        })
                        .when(show_warning_details && warning_count > 0, |body| {
                            body.child(
                                v_flex()
                                    .id("parse-warning-list")
                                    .max_h(px(260.0))
                                    .overflow_y_scroll()
                                    .gap_2()
                                    .p_2()
                                    .rounded(px(8.0))
                                    .border_1()
                                    .border_color(cx.theme().warning)
                                    .children(warnings.into_iter().enumerate().map(
                                        |(index, warning)| {
                                            v_flex()
                                                .p_2()
                                                .gap_1()
                                                .rounded(px(6.0))
                                                .bg(cx.theme().warning.opacity(0.1))
                                                .child(
                                                    div()
                                                        .text_sm()
                                                        .font_weight(gpui::FontWeight::SEMIBOLD)
                                                        .child(format!(
                                                            "{}. {}",
                                                            index + 1,
                                                            warning.message
                                                        )),
                                                )
                                                .child(
                                                    div()
                                                        .text_xs()
                                                        .text_color(cx.theme().muted_foreground)
                                                        .child(parse_warning_location(&warning)),
                                                )
                                        },
                                    )),
                            )
                        })
                },
            )
            .child(
                h_flex()
                    .flex_wrap()
                    .justify_end()
                    .gap_2()
                    .child(
                        Button::new("parse-reset")
                            .ghost()
                            .label("Reset overrides")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.reset_parse_settings(window, cx)
                            })),
                    )
                    .child(
                        Button::new("parse-reload")
                            .primary()
                            .label("Apply and Reload")
                            .disabled(self.document.loaded.is_none() || self.operation.is_running())
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.reload_with_parse_settings(cx)
                            })),
                    ),
            );

        div()
            .id("settings-backdrop")
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .p_4()
            .bg(gpui::black().opacity(0.6))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w_full()
                    .max_w(px(520.0))
                    .h_full()
                    .max_h(px(720.0))
                    .overflow_hidden()
                    .rounded(px(12.0))
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .shadow_2xl()
                    .child(
                        h_flex()
                            .flex_none()
                            .h(px(58.0))
                            .px_5()
                            .border_b_1()
                            .border_color(cx.theme().border)
                            .bg(cx.theme().secondary.opacity(0.55))
                            .gap_3()
                            .child(
                                div()
                                    .w(px(32.0))
                                    .h(px(32.0))
                                    .rounded(px(9.0))
                                    .bg(cx.theme().accent)
                                    .text_color(cx.theme().accent_foreground)
                                    .font_weight(gpui::FontWeight::BOLD)
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .child("Q"),
                            )
                            .child(
                                v_flex()
                                    .child(
                                        div()
                                            .text_lg()
                                            .font_weight(gpui::FontWeight::SEMIBOLD)
                                            .child("QuickRows Settings"),
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(cx.theme().muted_foreground)
                                            .child("Tune your data workspace"),
                                    ),
                            )
                            .child(div().flex_1())
                            .child(
                                Button::new("settings-close").ghost().label("×").on_click(
                                    cx.listener(|this, _, _, cx| this.close_settings(cx)),
                                ),
                            ),
                    )
                    .child(
                        div()
                            .id("settings-body")
                            .flex_1()
                            .min_h_0()
                            .overflow_y_scrollbar()
                            .p_5()
                            .bg(cx.theme().secondary.opacity(0.24))
                            .child(body),
                    )
                    .child(
                        h_flex()
                            .flex_none()
                            .h(px(64.0))
                            .px_5()
                            .justify_end()
                            .border_t_1()
                            .border_color(cx.theme().border)
                            .child(
                                Button::new("settings-done")
                                    .primary()
                                    .label("Done")
                                    .on_click(
                                        cx.listener(|this, _, _, cx| this.close_settings(cx)),
                                    ),
                            ),
                    ),
            )
            .into_any_element()
    }
}
