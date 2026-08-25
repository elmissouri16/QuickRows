// Inline cell editing and commit queue behavior.
impl QuickRowsView {
    fn begin_cell_edit(
        &mut self,
        display_row: usize,
        source_row: usize,
        column: usize,
        value: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.operation.is_running()
            || self
                .table.row_cache
                .get(&display_row)
                .is_none_or(|row| row.deleted || row.source_row != source_row)
        {
            return;
        }
        self.selection.selected_row = Some(display_row);
        self.selection.selected_rows.select_only(display_row);
        self.selection.selection_anchor = Some(display_row);
        self.editor.editing_cell = Some(EditingCell {
            display_row,
            source_row,
            column,
            initial_value: value.clone(),
        });
        self.editor.editing_draft_dirty = false;
        self.inputs.edit_input
            .update(cx, |input, cx| input.set_value(value, window, cx));
        cx.on_next_frame(window, |this, window, cx| {
            if this.editor.editing_cell.is_none() {
                return;
            }
            this.inputs.edit_input
                .update(cx, |input, cx| input.focus(window, cx));
            window.dispatch_action(Box::new(InputSelectAll), cx);
        });
        cx.notify();
    }

    fn cancel_cell_edit(&mut self, cx: &mut Context<Self>) {
        if !self.operation.is_running() {
            self.editor.editing_cell = None;
            self.editor.editing_draft_dirty = false;
            self.editor.pending_edit_action = None;
            cx.notify();
        }
    }

    fn continue_pending_edit_action(&mut self, cx: &mut Context<Self>) {
        match self.editor.pending_edit_action.take() {
            Some(PendingEditAction::OpenDialog) => self.prompt_open_dialog(cx),
            Some(PendingEditAction::Save) => {
                if let Some(path) = self
                    .document.loaded
                    .as_ref()
                    .filter(|loaded| loaded.dirty)
                    .map(|loaded| loaded.path.clone())
                {
                    self.save_to(path, cx);
                }
            }
            Some(PendingEditAction::SaveAs) => self.prompt_save_as(cx),
            Some(PendingEditAction::SortColumn(column)) => self.sort_column(column, cx),
            Some(PendingEditAction::MutateRows { rows, mutation }) => {
                self.mutate_rows(rows, mutation, cx)
            }
            None => {}
        }
    }

    fn commit_cell_edit(&mut self, cx: &mut Context<Self>) {
        if self.operation.is_running() {
            return;
        }
        let (Some(loaded), Some(editing)) = (&self.document.loaded, self.editor.editing_cell.clone()) else {
            return;
        };
        let value = self.inputs.edit_input.read(cx).value().to_string();
        self.editor.editing_cell = None;
        self.editor.editing_draft_dirty = false;
        if value == editing.initial_value {
            cx.notify();
            if self.editor.pending_cell_commits == 0 {
                self.continue_pending_edit_action(cx);
            }
            return;
        }

        if let Some(row) = self
            .table.row_cache
            .get_mut(&editing.display_row)
            .filter(|row| row.source_row == editing.source_row)
            && let Some(cell) = Arc::make_mut(&mut row.cells).get_mut(editing.column)
        {
            *cell = SharedString::from(value.clone());
        }
        self.editor.cell_commit_queue.push_back(CellCommit {
            editing,
            value,
            document: loaded.document.clone(),
        });
        self.editor.pending_cell_commits = self.editor.pending_cell_commits.saturating_add(1);
        self.feedback.error = None;
        self.feedback.notice = Some("Applying cell edit…".into());
        self.start_next_cell_commit(cx);
        cx.notify();
    }

    fn start_next_cell_commit(&mut self, cx: &mut Context<Self>) {
        if self.editor.cell_commit_in_flight {
            return;
        }
        let Some(commit) = self.editor.cell_commit_queue.pop_front() else {
            if self.editor.pending_cell_commits == 0 {
                self.continue_pending_edit_action(cx);
            }
            return;
        };
        self.editor.cell_commit_in_flight = true;
        let CellCommit {
            editing,
            value,
            document,
        } = commit;
        let source_row = editing.source_row;
        let display_row = editing.display_row;
        let column = editing.column;
        let initial_value = editing.initial_value.clone();
        let committed_value = value.clone();
        let task = cx.background_spawn({
            let document = document.clone();
            async move {
                let mut document = document
                    .lock()
                    .map_err(|_| QuickRowsError::other("CSV document lock was poisoned"))?;
                document.edit_source_cell(source_row, column, value)?;
                Ok::<_, QuickRowsError>(document.is_dirty())
            }
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                this.editor.cell_commit_in_flight = false;
                this.editor.pending_cell_commits = this.editor.pending_cell_commits.saturating_sub(1);
                let is_current_document = this
                    .document.loaded
                    .as_ref()
                    .is_some_and(|loaded| Arc::ptr_eq(&loaded.document, &document));
                if is_current_document {
                    match result {
                        Ok(dirty) => {
                            if let Some(loaded) = &mut this.document.loaded {
                                loaded.dirty = dirty;
                            }
                            this.invalidate_row_cache();
                            this.mark_results_stale();
                            this.feedback.notice = Some(
                                if dirty {
                                    "Cell updated. Save to write changes to disk."
                                } else {
                                    "Cell value restored; there are no unsaved row changes."
                                }
                                .into(),
                            );
                        }
                        Err(error) => {
                            if let Some(row) = this
                                .table.row_cache
                                .get_mut(&display_row)
                                .filter(|row| row.source_row == source_row)
                                && let Some(cell) = Arc::make_mut(&mut row.cells).get_mut(column)
                                && cell.as_ref() == committed_value
                            {
                                *cell = SharedString::from(initial_value);
                            }
                            if this.editor.cell_commit_queue.is_empty() && this.editor.editing_cell.is_none() {
                                this.editor.editing_cell = Some(editing);
                            }
                            this.editor.pending_edit_action = None;
                            this.feedback.error = Some(format!("Unable to edit cell: {error}").into());
                            this.feedback.notice = None;
                        }
                    }
                }
                this.start_next_cell_commit(cx);
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn copy_selected_action(
        &mut self,
        _: &CopySelected,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.copy_selected(window, cx);
    }

    fn copy_context_cell(
        &mut self,
        _: &CopyContextCell,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some((_, _, _, value)) = self.table.context_cell.take() {
            cx.write_to_clipboard(ClipboardItem::new_string(value));
            self.feedback.notice = Some("Cell copied.".into());
            cx.notify();
        }
    }

    fn search_context_cell(
        &mut self,
        _: &SearchContextCell,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some((_, _, column, value)) = self.table.context_cell.take() {
            self.queries.show_find = true;
            self.queries.search.column = Some(column);
            self.inputs.search_input
                .update(cx, |input, cx| input.set_value(value, window, cx));
            self.run_search(cx);
        }
    }

    fn edit_context_cell(
        &mut self,
        _: &EditContextCell,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some((display_row, source_row, column, value)) = self.table.context_cell.take() {
            self.begin_cell_edit(display_row, source_row, column, value, window, cx);
        }
    }

    fn cancel_cell_edit_action(
        &mut self,
        _: &InputEscape,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.cancel_cell_edit(cx);
    }

    fn toggle_delete_selected_action(
        &mut self,
        _: &ToggleDeleteSelected,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_delete_selected(cx);
    }
}
