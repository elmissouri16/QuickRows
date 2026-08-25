// Pointer and keyboard selection behavior.
impl QuickRowsView {
    fn select_row_from_click(&mut self, row: usize, event: &ClickEvent, cx: &mut Context<Self>) {
        let modifiers = event.modifiers();
        if modifiers.shift {
            let anchor = *self.selection.selection_anchor.get_or_insert(row);
            self.selection.selected_rows.select_only_range(anchor, row);
            self.selection.selected_row = Some(row);
        } else if modifiers.control || modifiers.platform {
            let selected = self.selection.selected_rows.toggle(row);
            self.selection.selected_row = selected
                .then_some(row)
                .or_else(|| self.selection.selected_rows.first());
            self.selection.selection_anchor = Some(row);
        } else {
            self.selection.selected_rows.select_only(row);
            self.selection.selected_row = Some(row);
            self.selection.selection_anchor = Some(row);
        }
        self.selection.cell_selection = None;
        self.selection.cell_dragging = false;
        cx.notify();
    }

    fn begin_cell_selection(
        &mut self,
        row: usize,
        column: usize,
        event: &MouseDownEvent,
        cx: &mut Context<Self>,
    ) {
        if self.modal_active() || self.operation.is_running() {
            return;
        }
        if event.modifiers.shift {
            if let Some(selection) = &mut self.selection.cell_selection {
                selection.set_active(row, column);
            } else {
                self.selection.cell_selection = Some(CellSelection::single(row, column));
            }
        } else {
            self.selection.cell_selection = Some(CellSelection::single(row, column));
        }
        self.selection.cell_dragging = true;
        self.sync_rows_to_cell_selection();
        cx.notify();
    }

    fn drag_cell_selection(&mut self, row: usize, column: usize, cx: &mut Context<Self>) {
        if !self.selection.cell_dragging {
            return;
        }
        if let Some(selection) = &mut self.selection.cell_selection {
            selection.set_active(row, column);
            self.sync_rows_to_cell_selection();
            cx.notify();
        }
    }

    fn sync_rows_to_cell_selection(&mut self) {
        let Some(selection) = self.selection.cell_selection else {
            return;
        };
        let rows = selection.rows();
        let start = *rows.start();
        let end = *rows.end();
        self.selection.selected_rows.select_only_range(start, end);
        self.selection.selected_row = Some(selection.active().row);
        self.selection.selection_anchor = Some(selection.anchor().row);
    }

    fn navigate_cell(
        &mut self,
        row_delta: isize,
        column_delta: isize,
        extend: bool,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(mut selection) = self.selection.cell_selection else {
            return false;
        };
        let Some(loaded) = &self.document.loaded else {
            return false;
        };
        if loaded.row_count == 0 || loaded.headers.is_empty() {
            return false;
        }
        let active = selection.active();
        let row = active
            .row
            .saturating_add_signed(row_delta)
            .min(loaded.row_count - 1);
        let column = active
            .column
            .saturating_add_signed(column_delta)
            .min(loaded.headers.len() - 1);
        selection.move_to(row, column, extend);
        self.selection.cell_selection = Some(selection);
        self.sync_rows_to_cell_selection();
        self.table.row_scroll.scroll_to_item(row, ScrollStrategy::Center);
        cx.notify();
        true
    }

    fn select_row_and_scroll(&mut self, row: usize, cx: &mut Context<Self>) {
        self.navigate_row_and_scroll(row, false, cx);
    }

    fn navigate_row_and_scroll(&mut self, row: usize, extend: bool, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        let Some(row_count) = self.document.loaded.as_ref().map(|loaded| loaded.row_count) else {
            return;
        };
        if row_count == 0 {
            return;
        }
        let row = row.min(row_count - 1);
        self.selection.cell_selection = None;
        self.selection.cell_dragging = false;
        if extend {
            let current = self.selection.selected_row.unwrap_or(row);
            let anchor = *self.selection.selection_anchor.get_or_insert(current);
            self.selection.selected_rows.select_only_range(anchor, row);
        } else {
            self.selection.selected_rows.select_only(row);
            self.selection.selection_anchor = Some(row);
        }
        self.selection.selected_row = Some(row);
        self.table.row_scroll.scroll_to_item(row, ScrollStrategy::Center);
        cx.notify();
    }

    fn page_step(&self) -> usize {
        self.table.desired_row_range
            .as_ref()
            .map(|range| range.end.saturating_sub(range.start).saturating_sub(1))
            .unwrap_or(20)
            .clamp(1, 100)
    }

    fn move_context_menu_focus(&mut self, delta: isize, cx: &mut Context<Self>) -> bool {
        let Some(menu) = &mut self.table.table_context_menu else {
            return false;
        };
        let count = context_menu_item_count(menu.kind);
        menu.focused_item = menu
            .focused_item
            .saturating_add_signed(delta)
            .min(count - 1);
        cx.notify();
        true
    }

    fn activate_context_menu(
        &mut self,
        _: &ActivateContextMenu,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(menu) = self.table.table_context_menu.take() else {
            return;
        };
        match context_menu_command(menu.kind, menu.focused_item) {
            ContextMenuCommand::CopyCell => self.copy_context_cell(&CopyContextCell, window, cx),
            ContextMenuCommand::SearchCell => {
                self.search_context_cell(&SearchContextCell, window, cx)
            }
            ContextMenuCommand::EditCell => self.edit_context_cell(&EditContextCell, window, cx),
            ContextMenuCommand::DeleteRows => {
                self.table.context_cell = None;
                self.mutate_selected_rows(RowMutation::Delete, cx);
            }
            ContextMenuCommand::RestoreRows => {
                self.table.context_cell = None;
                self.mutate_selected_rows(RowMutation::Restore, cx);
            }
            ContextMenuCommand::CopySelection => {
                self.table.context_cell = None;
                self.copy_selected(window, cx);
            }
        }
        cx.notify();
    }

    fn select_previous_row(
        &mut self,
        _: &SelectPreviousRow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.move_context_menu_focus(-1, cx) {
            return;
        }
        if !self.navigate_cell(-1, 0, false, cx) {
            self.select_row_and_scroll(self.selection.selected_row.unwrap_or(0).saturating_sub(1), cx);
        }
    }

    fn select_next_row(&mut self, _: &SelectNextRow, _window: &mut Window, cx: &mut Context<Self>) {
        if self.move_context_menu_focus(1, cx) {
            return;
        }
        if !self.navigate_cell(1, 0, false, cx) {
            self.select_row_and_scroll(
                self.selection.selected_row.map_or(0, |row| row.saturating_add(1)),
                cx,
            );
        }
    }

    fn select_previous_column(
        &mut self,
        _: &SelectPreviousColumn,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_cell(0, -1, false, cx);
    }

    fn select_next_column(
        &mut self,
        _: &SelectNextColumn,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_cell(0, 1, false, cx);
    }

    fn extend_previous_column(
        &mut self,
        _: &ExtendPreviousColumn,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_cell(0, -1, true, cx);
    }

    fn extend_next_column(
        &mut self,
        _: &ExtendNextColumn,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_cell(0, 1, true, cx);
    }

    fn select_first_row(
        &mut self,
        _: &SelectFirstRow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.select_row_and_scroll(0, cx);
    }

    fn select_last_row(&mut self, _: &SelectLastRow, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(last) = self
            .document.loaded
            .as_ref()
            .and_then(|loaded| loaded.row_count.checked_sub(1))
        {
            self.select_row_and_scroll(last, cx);
        }
    }

    fn extend_previous_row(
        &mut self,
        _: &ExtendPreviousRow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.navigate_cell(-1, 0, true, cx) {
            let row = self.selection.selected_row.unwrap_or(0).saturating_sub(1);
            self.navigate_row_and_scroll(row, true, cx);
        }
    }

    fn extend_next_row(&mut self, _: &ExtendNextRow, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.navigate_cell(1, 0, true, cx) {
            let row = self.selection.selected_row.map_or(0, |row| row.saturating_add(1));
            self.navigate_row_and_scroll(row, true, cx);
        }
    }

    fn extend_first_row(
        &mut self,
        _: &ExtendFirstRow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_row_and_scroll(0, true, cx);
    }

    fn extend_last_row(&mut self, _: &ExtendLastRow, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(last) = self
            .document.loaded
            .as_ref()
            .and_then(|loaded| loaded.row_count.checked_sub(1))
        {
            self.navigate_row_and_scroll(last, true, cx);
        }
    }

    fn select_page_up(&mut self, _: &SelectPageUp, _window: &mut Window, cx: &mut Context<Self>) {
        let row = self
            .selection.selected_row
            .unwrap_or(0)
            .saturating_sub(self.page_step());
        self.navigate_row_and_scroll(row, false, cx);
    }

    fn select_page_down(
        &mut self,
        _: &SelectPageDown,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let row = self
            .selection.selected_row
            .unwrap_or(0)
            .saturating_add(self.page_step());
        self.navigate_row_and_scroll(row, false, cx);
    }

    fn extend_page_up(&mut self, _: &ExtendPageUp, _window: &mut Window, cx: &mut Context<Self>) {
        let row = self
            .selection.selected_row
            .unwrap_or(0)
            .saturating_sub(self.page_step());
        self.navigate_row_and_scroll(row, true, cx);
    }

    fn extend_page_down(
        &mut self,
        _: &ExtendPageDown,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let row = self
            .selection.selected_row
            .unwrap_or(0)
            .saturating_add(self.page_step());
        self.navigate_row_and_scroll(row, true, cx);
    }

    fn select_all_rows(&mut self, _: &SelectAllRows, _window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        let Some(row_count) = self.document.loaded.as_ref().map(|loaded| loaded.row_count) else {
            return;
        };
        self.selection.cell_selection = None;
        self.selection.cell_dragging = false;
        self.selection.selected_rows.select_all(row_count);
        self.selection.selected_row = (row_count > 0).then_some(0);
        self.selection.selection_anchor = self.selection.selected_row;
        cx.notify();
    }

    fn clear_row_selection(
        &mut self,
        _: &ClearRowSelection,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if matches!(self.overlay.modal, Modal::Settings) {
            self.close_settings(cx);
            return;
        }
        if matches!(self.overlay.modal, Modal::Shortcuts | Modal::About) {
            self.close_info_modal(cx);
            return;
        }
        if matches!(self.overlay.modal, Modal::HeaderPrompt) {
            self.resolve_header_prompt(None, cx);
            return;
        }
        if matches!(self.overlay.modal, Modal::ExternalSave(_)) {
            self.cancel_external_save(cx);
            return;
        }
        if matches!(self.overlay.modal, Modal::Bulk(_)) {
            self.cancel_pending_bulk_action(cx);
            return;
        }
        if matches!(self.overlay.modal, Modal::Destructive(_)) {
            self.cancel_pending_destructive(cx);
            return;
        }
        if self.table.table_context_menu.take().is_some() {
            cx.notify();
            return;
        }
        self.clear_selection();
        cx.notify();
    }
}
