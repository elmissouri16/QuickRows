// Table context-menu rendering and interaction.
impl QuickRowsView {
    fn render_table_context_menu(
        &self,
        window: &Window,
        cx: &mut Context<Self>,
    ) -> Option<gpui::AnyElement> {
        let context_menu = self.table.table_context_menu?;
        let can_edit = matches!(
            context_menu.kind,
            TableContextMenuKind::Cell { can_edit: true }
        );
        let is_cell_menu = matches!(context_menu.kind, TableContextMenuKind::Cell { .. });
        let selected_count = self
            .selection.selected_rows
            .len()
            .max(usize::from(self.selection.selected_row.is_some()))
            .max(1);
        let delete_label = if selected_count == 1 {
            "Delete row".to_string()
        } else {
            format!("Delete {} rows", format_count(selected_count))
        };
        let restore_label = if selected_count == 1 {
            "Restore row".to_string()
        } else {
            format!("Restore {} rows", format_count(selected_count))
        };
        let copy_shortcut = if cfg!(target_os = "macos") {
            "⌘C"
        } else {
            "Ctrl+C"
        };
        let copy_rows_label = if self.selection.cell_selection.is_some() {
            "Copy selected cells".to_string()
        } else if selected_count == 1 {
            "Copy row".to_string()
        } else {
            format!("Copy {} rows", format_count(selected_count))
        };
        let menu_width = px(220.0);
        let item_count = if is_cell_menu {
            if can_edit { 6.0 } else { 5.0 }
        } else {
            3.0
        };
        let menu_height = px(item_count * 36.0 + 8.0);
        let viewport = window.viewport_size();
        let max_x = (viewport.width - menu_width - px(8.0)).max(px(8.0));
        let max_y = (viewport.height - menu_height - px(8.0)).max(px(8.0));
        let left = context_menu.position.x.max(px(8.0)).min(max_x);
        let top = context_menu.position.y.max(px(8.0)).min(max_y);

        let mut items = Vec::new();
        let row_action_start = if is_cell_menu {
            2 + usize::from(can_edit)
        } else {
            0
        };
        if is_cell_menu {
            items.push(table_context_menu_item(
                "context-copy-cell",
                "Copy cell",
                Some(copy_shortcut),
                context_menu.focused_item == 0,
                false,
                |this, _, window, cx| {
                    this.table.table_context_menu = None;
                    this.copy_context_cell(&CopyContextCell, window, cx);
                },
                cx,
            ));
            items.push(table_context_menu_item(
                "context-search-cell",
                "Search for this",
                None,
                context_menu.focused_item == 1,
                false,
                |this, _, window, cx| {
                    this.table.table_context_menu = None;
                    this.search_context_cell(&SearchContextCell, window, cx);
                },
                cx,
            ));
            if can_edit {
                items.push(table_context_menu_item(
                    "context-edit-cell",
                    "Edit cell",
                    None,
                    context_menu.focused_item == 2,
                    false,
                    |this, _, window, cx| {
                        this.table.table_context_menu = None;
                        this.edit_context_cell(&EditContextCell, window, cx);
                    },
                    cx,
                ));
            }
        }
        items.push(table_context_menu_item(
            "context-delete-row",
            delete_label,
            None,
            context_menu.focused_item == row_action_start,
            true,
            |this, _, _, cx| {
                this.table.table_context_menu = None;
                this.table.context_cell = None;
                this.mutate_selected_rows(RowMutation::Delete, cx);
            },
            cx,
        ));
        items.push(table_context_menu_item(
            "context-restore-row",
            restore_label,
            None,
            context_menu.focused_item == row_action_start + 1,
            false,
            |this, _, _, cx| {
                this.table.table_context_menu = None;
                this.table.context_cell = None;
                this.mutate_selected_rows(RowMutation::Restore, cx);
            },
            cx,
        ));
        items.push(table_context_menu_item(
            "context-copy-row",
            copy_rows_label,
            Some(copy_shortcut),
            context_menu.focused_item == row_action_start + 2,
            false,
            |this, _, window, cx| {
                this.table.table_context_menu = None;
                this.table.context_cell = None;
                this.copy_selected(window, cx);
            },
            cx,
        ));

        Some(
            v_flex()
                .id("table-context-menu")
                .absolute()
                .left(left)
                .top(top)
                .w(menu_width)
                .p_1()
                .rounded(px(6.0))
                .border_1()
                .border_color(cx.theme().border)
                .bg(cx.theme().popover)
                .text_color(cx.theme().popover_foreground)
                .occlude()
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.table.table_context_menu = None;
                    this.table.context_cell = None;
                    cx.notify();
                }))
                .children(items)
                .into_any_element(),
        )
    }
}
