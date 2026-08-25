// Root workspace rendering and context-menu item presentation.
fn table_context_menu_item(
    id: &'static str,
    label: impl Into<SharedString>,
    shortcut: Option<&'static str>,
    focused: bool,
    destructive: bool,
    on_click: impl Fn(&mut QuickRowsView, &ClickEvent, &mut Window, &mut Context<QuickRowsView>)
    + 'static,
    cx: &mut Context<QuickRowsView>,
) -> gpui::AnyElement {
    let label = label.into();
    h_flex()
        .id(id)
        .h(px(36.0))
        .px_3()
        .gap_3()
        .rounded(px(4.0))
        .cursor_pointer()
        .text_sm()
        .when(focused, |item| item.bg(cx.theme().accent))
        .when(destructive, |item| item.text_color(cx.theme().danger))
        .hover(|item| item.bg(cx.theme().accent))
        .child(div().flex_1().child(label))
        .when_some(shortcut, |item, shortcut| {
            item.child(
                div()
                    .text_color(cx.theme().muted_foreground)
                    .child(shortcut),
            )
        })
        .on_click(cx.listener(on_click))
        .into_any_element()
}

impl gpui::Focusable for QuickRowsView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for QuickRowsView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let content = self.render_table(window, cx);
        let settings = matches!(self.overlay.modal, Modal::Settings)
            .then(|| self.render_settings(cx));
        let header_prompt = matches!(self.overlay.modal, Modal::HeaderPrompt)
            .then(|| self.render_header_prompt(cx));
        let shortcuts = matches!(self.overlay.modal, Modal::Shortcuts)
            .then(|| self.render_shortcuts(cx));
        let about = matches!(self.overlay.modal, Modal::About)
            .then(|| self.render_about(cx));
        let external_save = matches!(self.overlay.modal, Modal::ExternalSave(_))
            .then(|| self.render_external_save_confirmation(cx));
        let unsaved = matches!(self.overlay.modal, Modal::Destructive(_))
            .then(|| self.render_unsaved_confirmation(cx));
        let bulk_confirmation = matches!(self.overlay.modal, Modal::Bulk(_))
            .then(|| self.render_bulk_confirmation(cx));
        let modal_active = self.modal_active();
        let table_context_menu = (!modal_active)
            .then(|| self.render_table_context_menu(window, cx))
            .flatten();
        let cell_editing = self.editor.editing_cell.is_some();
        let can_cancel = self.operation.cancellation().is_some();
        let can_retry_rows = self.table.failed_row_range.is_some();
        let can_reload_external = self.document.external_change_detected && !self.operation.is_running();
        div()
            .key_context("QuickRows")
            .track_focus(&self.focus_handle)
            .on_mouse_move(cx.listener(Self::update_column_resize))
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(Self::finish_pointer_interaction),
            )
            .on_action(cx.listener(Self::cancel_cell_edit_action))
            .on_action(cx.listener(Self::clear_row_selection))
            .when(!modal_active && !cell_editing, |this| {
                this.on_action(cx.listener(Self::open_dialog))
                    .on_action(cx.listener(Self::save_file))
                    .on_action(cx.listener(Self::save_file_as))
                    .on_action(cx.listener(Self::reload_file))
                    .on_action(cx.listener(Self::clear_file))
                    .on_action(cx.listener(Self::close_window_action))
                    .on_action(cx.listener(Self::quit_app))
                    .on_action(cx.listener(Self::show_find))
                    .on_action(cx.listener(Self::clear_search))
                    .on_action(cx.listener(Self::next_match))
                    .on_action(cx.listener(Self::previous_match))
                    .on_action(cx.listener(Self::check_duplicates))
                    .on_action(cx.listener(Self::toggle_theme))
                    .on_action(cx.listener(Self::open_settings))
                    .on_action(cx.listener(Self::open_parse_settings))
                    .on_action(cx.listener(Self::show_shortcuts))
                    .on_action(cx.listener(Self::show_about))
                    .on_action(cx.listener(Self::toggle_index))
                    .on_action(cx.listener(Self::compact_rows))
                    .on_action(cx.listener(Self::default_rows))
                    .on_action(cx.listener(Self::spacious_rows))
                    .on_action(cx.listener(Self::copy_selected_action))
                    .on_action(cx.listener(Self::copy_context_cell))
                    .on_action(cx.listener(Self::search_context_cell))
                    .on_action(cx.listener(Self::edit_context_cell))
                    .on_action(cx.listener(Self::activate_context_menu))
                    .on_action(cx.listener(Self::toggle_delete_selected_action))
                    .on_action(cx.listener(Self::delete_selected_rows_action))
                    .on_action(cx.listener(Self::restore_selected_rows_action))
                    .on_action(cx.listener(Self::select_all_rows))
                    .on_action(cx.listener(Self::select_previous_row))
                    .on_action(cx.listener(Self::select_next_row))
                    .on_action(cx.listener(Self::select_previous_column))
                    .on_action(cx.listener(Self::select_next_column))
                    .on_action(cx.listener(Self::extend_previous_column))
                    .on_action(cx.listener(Self::extend_next_column))
                    .on_action(cx.listener(Self::select_first_row))
                    .on_action(cx.listener(Self::select_last_row))
                    .on_action(cx.listener(Self::extend_previous_row))
                    .on_action(cx.listener(Self::extend_next_row))
                    .on_action(cx.listener(Self::extend_first_row))
                    .on_action(cx.listener(Self::extend_last_row))
                    .on_action(cx.listener(Self::select_page_up))
                    .on_action(cx.listener(Self::select_page_down))
                    .on_action(cx.listener(Self::extend_page_up))
                    .on_action(cx.listener(Self::extend_page_down))
            })
            .size_full()
            .bg(cx.theme().background)
            .text_color(cx.theme().foreground)
            .child(content)
            .when_some(table_context_menu, |this, menu| this.child(menu))
            .when_some(settings, |this, settings| this.child(settings))
            .when_some(header_prompt, |this, prompt| this.child(prompt))
            .when_some(shortcuts, |this, shortcuts| this.child(shortcuts))
            .when_some(about, |this, about| this.child(about))
            .when_some(external_save, |this, confirmation| this.child(confirmation))
            .when_some(unsaved, |this, unsaved| this.child(unsaved))
            .when_some(bulk_confirmation, |this, confirmation| {
                this.child(confirmation)
            })
            .when_some(self.feedback.error.clone(), |this, error| {
                this.child(
                    div()
                        .absolute()
                        .top_3()
                        .left_3()
                        .right_3()
                        .p_3()
                        .rounded_md()
                        .bg(cx.theme().danger)
                        .text_color(cx.theme().danger_foreground)
                        .child(
                            h_flex()
                                .gap_3()
                                .child(div().flex_1().child(error))
                                .when(can_retry_rows, |status| {
                                    status.child(
                                        Button::new("retry-row-load").label("Retry").on_click(
                                            cx.listener(|this, _, _, cx| {
                                                this.retry_failed_rows(cx)
                                            }),
                                        ),
                                    )
                                })
                                .child(
                                    Button::new("dismiss-error")
                                        .ghost()
                                        .label("Dismiss")
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.feedback.error = None;
                                            cx.notify();
                                        })),
                                ),
                        ),
                )
            })
            .when_some(self.feedback.notice.clone(), |this, notice| {
                this.child(
                    div()
                        .absolute()
                        .bottom_3()
                        .right_3()
                        .p_3()
                        .rounded_md()
                        .bg(cx.theme().popover)
                        .border_1()
                        .border_color(cx.theme().border)
                        .child(
                            h_flex()
                                .gap_3()
                                .child(div().flex_1().child(notice))
                                .when(can_cancel, |status| {
                                    status.child(
                                        Button::new("cancel-operation")
                                            .danger()
                                            .label("Cancel")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.cancel_current_operation(cx)
                                            })),
                                    )
                                })
                                .when(can_reload_external, |status| {
                                    status.child(
                                        Button::new("reload-external-change")
                                            .label("Reload")
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.reload_file(&ReloadFile, window, cx)
                                            })),
                                    )
                                })
                                .child(
                                    Button::new("dismiss-notice")
                                        .ghost()
                                        .label("Dismiss")
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.feedback.notice = None;
                                            cx.notify();
                                        })),
                                ),
                        ),
                )
            })
    }
}
