// Empty workspace and modal surfaces.
impl QuickRowsView {
    fn render_empty(&self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let recent = self.preferences.settings.recent_files.clone();
        div()
            .size_full()
            .p_6()
            .bg(cx.theme().secondary.opacity(0.35))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w_full()
                    .max_w(px(560.0))
                    .gap_5()
                    .child(
                        Button::new("open-csv")
                            .icon(IconName::FolderOpen)
                            .label(if self.operation.is_running() {
                                "Opening…"
                            } else {
                                "Choose a CSV file"
                            })
                            .disabled(self.operation.is_running())
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.open_dialog(&OpenFile, window, cx)
                            })),
                    )
                    .when(!recent.is_empty(), |this| {
                        this.child(
                            v_flex()
                                .gap_2()
                                .child(
                                    div()
                                        .text_xs()
                                        .font_weight(gpui::FontWeight::SEMIBOLD)
                                        .text_color(cx.theme().muted_foreground)
                                        .child("RECENT FILES"),
                                )
                                .children(recent.into_iter().take(6).map(|path| {
                                    let title = display_name(&path);
                                    let open_path = path.clone();
                                    let remove_path = path.clone();
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            Button::new(SharedString::from(format!(
                                                "recent:{}",
                                                path.display()
                                            )))
                                            .flex_1()
                                            .icon(IconName::File)
                                            .label(title)
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.open_path(open_path.clone().into(), cx)
                                            })),
                                        )
                                        .child(
                                            Button::new(SharedString::from(format!(
                                                "remove-recent:{}",
                                                path.display()
                                            )))
                                            .ghost()
                                            .icon(IconName::Delete)
                                            .tooltip("Remove from recent files")
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.remove_recent_file(&remove_path, cx)
                                            })),
                                        )
                                })),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_unsaved_confirmation(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let action = match &self.overlay.modal {
            Modal::Destructive(PendingDestructiveAction::Open(_)) => "opening another file",
            Modal::Destructive(PendingDestructiveAction::Reload) => "reloading with new parse settings",
            Modal::Destructive(PendingDestructiveAction::Clear) => "clearing this file",
            Modal::Destructive(PendingDestructiveAction::Close) => "closing this window",
            Modal::Destructive(PendingDestructiveAction::Quit) => "quitting QuickRows",
            _ => "continuing",
        };
        div()
            .id("unsaved-backdrop")
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .bg(gpui::black().opacity(0.35))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w(px(460.0))
                    .p_5()
                    .gap_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .child(
                        div()
                            .text_xl()
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .child("Save changes?"),
                    )
                    .child(format!(
                        "Your cell edits or deleted rows will be lost before {action}."
                    ))
                    .child(
                        h_flex()
                            .justify_end()
                            .gap_2()
                            .child(
                                Button::new("unsaved-cancel")
                                    .ghost()
                                    .label("Cancel")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.cancel_pending_destructive(cx)
                                    })),
                            )
                            .child(
                                Button::new("unsaved-discard")
                                    .danger()
                                    .label("Discard")
                                    .disabled(self.operation.is_running())
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.discard_pending_destructive(window, cx)
                                    })),
                            )
                            .child(
                                Button::new("unsaved-save")
                                    .primary()
                                    .label("Save")
                                    .disabled(self.operation.is_running())
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.save_pending_destructive(window, cx)
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_external_save_confirmation(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        div()
            .id("external-save-backdrop")
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .bg(gpui::black().opacity(0.45))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w(px(520.0))
                    .p_5()
                    .gap_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().warning)
                    .bg(cx.theme().background)
                    .child(
                        div()
                            .text_xl()
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .child("The CSV changed on disk"),
                    )
                    .child("Saving here would overwrite changes made by another program. Reload, save a copy, or explicitly overwrite the file.")
                    .child(
                        h_flex()
                            .flex_wrap()
                            .justify_end()
                            .gap_2()
                            .child(
                                Button::new("external-save-cancel")
                                    .ghost()
                                    .label("Cancel")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.cancel_external_save(cx)
                                    })),
                            )
                            .child(
                                Button::new("external-save-reload")
                                    .label("Reload")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.reload_external_change(cx)
                                    })),
                            )
                            .child(
                                Button::new("external-save-as")
                                    .label("Save As…")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.save_external_as(cx)
                                    })),
                            )
                            .child(
                                Button::new("external-save-overwrite")
                                    .danger()
                                    .label("Overwrite")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.confirm_external_overwrite(cx)
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_header_prompt(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        div()
            .id("header-prompt-backdrop")
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .bg(gpui::black().opacity(0.35))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w(px(500.0))
                    .p_5()
                    .gap_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .child(
                        div()
                            .text_xl()
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .child("Does the first row contain headers?"),
                    )
                    .child(
                        div()
                            .text_color(cx.theme().muted_foreground)
                            .child("Header detection was uncertain. Choose how this file should be interpreted."),
                    )
                    .child(
                        h_flex()
                            .flex_wrap()
                            .justify_end()
                            .gap_2()
                            .child(
                                Button::new("header-dismiss")
                                    .ghost()
                                    .label("Dismiss")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.resolve_header_prompt(None, cx)
                                    })),
                            )
                            .child(
                                Button::new("header-as-data")
                                    .label("Keep as data")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.resolve_header_prompt(Some(false), cx)
                                    })),
                            )
                            .child(
                                Button::new("header-use-first")
                                    .primary()
                                    .label("Use first row as headers")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.resolve_header_prompt(Some(true), cx)
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_shortcuts(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let primary = if cfg!(target_os = "macos") {
            "⌘"
        } else {
            "Ctrl+"
        };
        let shortcuts = vec![
            (format!("{primary}O"), "Open CSV"),
            (format!("{primary}S"), "Save"),
            (format!("{primary}Shift+S"), "Save As"),
            (format!("{primary}F"), "Find"),
            ("F3 / Shift+F3".to_string(), "Next / previous match"),
            (format!("{primary}A"), "Select all rows"),
            ("Shift+Arrow".to_string(), "Extend row or cell selection"),
            ("Page Up / Page Down".to_string(), "Move by one page"),
            (format!("{primary}C"), "Copy selected rows or cells"),
            ("Delete / Backspace".to_string(), "Delete selected rows"),
            ("Escape".to_string(), "Dismiss menu or clear selection"),
        ];
        self.render_info_modal(
            "Keyboard Shortcuts",
            v_flex()
                .gap_2()
                .children(shortcuts.into_iter().map(|(keys, description)| {
                    h_flex()
                        .gap_4()
                        .child(
                            div()
                                .w(px(170.0))
                                .font_weight(gpui::FontWeight::SEMIBOLD)
                                .child(keys),
                        )
                        .child(description)
                }))
                .into_any_element(),
            cx,
        )
    }

    fn render_about(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        self.render_info_modal(
            "About QuickRows",
            v_flex()
                .gap_3()
                .child(
                    div()
                        .text_lg()
                        .font_weight(gpui::FontWeight::SEMIBOLD)
                        .child(format!("QuickRows {}", env!("CARGO_PKG_VERSION"))),
                )
                .child("A fast, local-first native CSV viewer and editor.")
                .child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().muted_foreground)
                        .child("CSV data stays on this computer. Licensed under MIT."),
                )
                .into_any_element(),
            cx,
        )
    }

    fn render_info_modal(
        &mut self,
        title: &'static str,
        content: gpui::AnyElement,
        cx: &mut Context<Self>,
    ) -> gpui::AnyElement {
        div()
            .id(SharedString::from(format!(
                "{}-backdrop",
                title.replace(' ', "-")
            )))
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .bg(gpui::black().opacity(0.45))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w(px(520.0))
                    .max_h(px(680.0))
                    .p_5()
                    .gap_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .child(
                        div()
                            .text_xl()
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .child(title),
                    )
                    .child(content)
                    .child(
                        h_flex().justify_end().child(
                            Button::new("close-info-modal")
                                .primary()
                                .label("Close")
                                .on_click(cx.listener(|this, _, _, cx| this.close_info_modal(cx))),
                        ),
                    ),
            )
            .into_any_element()
    }

    fn render_bulk_confirmation(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let (title, message, confirm_label) = match &self.overlay.modal {
            Modal::Bulk(PendingBulkAction::Copy { rows }) => (
                "Copy many rows?",
                format!(
                    "Copy {} selected rows to the clipboard?",
                    format_count(rows.len())
                ),
                "Copy",
            ),
            Modal::Bulk(PendingBulkAction::CopyCells {
                row_start,
                row_end,
                column_start,
                column_end,
            }) => (
                "Copy a large cell range?",
                format!(
                    "Copy {} rows × {} columns to the clipboard?",
                    format_count(row_end - row_start + 1),
                    format_count(column_end - column_start + 1)
                ),
                "Copy",
            ),
            Modal::Bulk(PendingBulkAction::Delete { rows }) => (
                "Delete many rows?",
                format!(
                    "Mark {} selected rows as deleted?",
                    format_count(rows.len())
                ),
                "Delete",
            ),
            _ => ("Continue?", String::new(), "Continue"),
        };
        div()
            .id("bulk-confirmation-backdrop")
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .bg(gpui::black().opacity(0.35))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w(px(440.0))
                    .p_5()
                    .gap_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .child(
                        div()
                            .text_xl()
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .child(title),
                    )
                    .child(message)
                    .child(
                        h_flex()
                            .justify_end()
                            .gap_2()
                            .child(Button::new("bulk-cancel").ghost().label("Cancel").on_click(
                                cx.listener(|this, _, _, cx| this.cancel_pending_bulk_action(cx)),
                            ))
                            .child(
                                Button::new("bulk-confirm")
                                    .danger()
                                    .label(confirm_label)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.confirm_pending_bulk_action(cx)
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }
}
