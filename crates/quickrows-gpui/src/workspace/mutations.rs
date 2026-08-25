// Sort and bulk row mutation orchestration.
impl QuickRowsView {
    fn hide_duplicates(&mut self, cx: &mut Context<Self>) {
        self.queries.show_duplicates = false;
        cx.notify();
    }

    fn sort_column(&mut self, column: usize, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        if self.editor.editing_cell.is_some() {
            self.editor.pending_edit_action = Some(PendingEditAction::SortColumn(column));
            self.commit_cell_edit(cx);
            return;
        }
        if self.editor.pending_cell_commits > 0 {
            self.editor.pending_edit_action = Some(PendingEditAction::SortColumn(column));
            return;
        }
        let Some(loaded) = &self.document.loaded else { return };
        if self.operation.is_running() {
            return;
        }
        let document = loaded.document.clone();
        let row_count = loaded.row_count;
        let next = document.try_lock().ok().map(|doc| match doc.sort_spec() {
            Some(spec) if spec.column == column && spec.direction == SortDirection::Ascending => {
                Some(SortSpec {
                    column,
                    direction: SortDirection::Descending,
                })
            }
            Some(spec) if spec.column == column && spec.direction == SortDirection::Descending => {
                None
            }
            _ => Some(SortSpec {
                column,
                direction: SortDirection::Ascending,
            }),
        });
        let Some(next) = next else { return };
        let cancellation = self.begin_cancellable_operation(OperationKind::Sort);
        self.feedback.error = None;
        self.feedback.notice = Some("Sorting…".into());
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_row_progress(
            progress.clone(),
            row_count,
            OperationKind::Sort,
            "Sorting",
            cx,
        );
        cx.notify();
        let task = cx.background_spawn(async move {
            let update_progress = |processed, _| progress.store(processed, Ordering::Relaxed);
            document
                .lock()
                .map_err(|_| QuickRowsError::other("CSV document lock was poisoned"))?
                .sort_cancellable_with_progress(next, &cancellation, &update_progress)
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                this.finish_cancellable_operation();
                if let Err(error) = result {
                    if error.kind() == ErrorKind::Cancelled {
                        this.feedback.notice = Some("Sort cancelled.".into());
                    } else {
                        this.feedback.error = Some(format!("Sort failed: {error}").into());
                        this.feedback.notice = None;
                    }
                } else {
                    this.invalidate_row_cache();
                    this.feedback.notice = Some("Sort complete.".into());
                }
                this.clear_selection();
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn selected_display_rows(&self) -> Vec<usize> {
        let mut rows = self.selection.selected_rows.iter().collect::<Vec<_>>();
        if rows.is_empty() {
            rows.extend(self.selection.selected_row);
        }
        rows
    }

    fn toggle_delete_selected(&mut self, cx: &mut Context<Self>) {
        let Some(primary_row) = self.selection.selected_row else {
            return;
        };
        let Some(loaded) = &self.document.loaded else { return };
        let mutation = match loaded.document.try_lock() {
            Ok(document) if document.is_display_row_deleted(primary_row) => RowMutation::Restore,
            Ok(_) => RowMutation::Delete,
            Err(_) => {
                self.feedback.notice = Some("Rows are still loading; try the row action again.".into());
                cx.notify();
                return;
            }
        };
        self.mutate_selected_rows(mutation, cx);
    }

    fn delete_selected_rows_action(
        &mut self,
        _: &DeleteSelectedRows,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selected_rows(RowMutation::Delete, cx);
    }

    fn restore_selected_rows_action(
        &mut self,
        _: &RestoreSelectedRows,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selected_rows(RowMutation::Restore, cx);
    }

    fn mutate_selected_rows(&mut self, mutation: RowMutation, cx: &mut Context<Self>) {
        if self.modal_active() || self.operation.is_running() || self.selection.selected_row.is_none() {
            return;
        }
        let rows = self.selected_display_rows();
        if self.editor.pending_cell_commits > 0 {
            self.editor.pending_edit_action = Some(PendingEditAction::MutateRows { rows, mutation });
            return;
        }
        self.mutate_rows(rows, mutation, cx);
    }

    fn mutate_rows(&mut self, rows: Vec<usize>, mutation: RowMutation, cx: &mut Context<Self>) {
        if mutation == RowMutation::Delete && rows.len() >= DELETE_CONFIRM_THRESHOLD {
            self.overlay.modal = Modal::Bulk(PendingBulkAction::Delete { rows });
            cx.notify();
            return;
        }
        if rows.len() >= DELETE_CONFIRM_THRESHOLD {
            self.mutate_rows_background(rows, mutation, cx);
            return;
        }
        let Some(loaded) = &self.document.loaded else { return };
        let Ok(mut document) = loaded.document.try_lock() else {
            self.feedback.notice = Some("Rows are still loading; try the row action again.".into());
            cx.notify();
            return;
        };
        match document.set_display_rows_deleted(&rows, mutation.deleted()) {
            Err(error) => self.feedback.error = Some(error.to_string().into()),
            Ok(changed) => {
                let dirty = document.is_dirty();
                drop(document);
                if let Some(loaded) = &mut self.document.loaded {
                    loaded.dirty = dirty;
                }
                self.mark_results_stale();
                self.invalidate_row_cache();
                self.feedback.notice = Some(
                    format!(
                        "{} {} {}.{}",
                        mutation.past_tense(),
                        format_count(changed),
                        counted_noun(changed, "row", "rows"),
                        if dirty {
                            " Save to write changes to disk."
                        } else {
                            " All row changes are reverted."
                        }
                    )
                    .into(),
                );
            }
        }
        cx.notify();
    }

    fn mutate_rows_background(
        &mut self,
        rows: Vec<usize>,
        mutation: RowMutation,
        cx: &mut Context<Self>,
    ) {
        let Some(document) = self.document.loaded.as_ref().map(|loaded| loaded.document.clone()) else {
            return;
        };
        let completion_document = document.clone();
        let row_count = rows.len();
        let cancellation = self.begin_cancellable_operation(OperationKind::Rows);
        let operation_generation = self.runtime.operation_generation;
        self.feedback.notice = Some(
            format!(
                "{} {} rows…",
                mutation.past_tense().trim_end_matches('d'),
                format_count(row_count)
            )
            .into(),
        );
        cx.notify();
        let task = cx.background_spawn(async move {
            let mut document = document
                .lock()
                .map_err(|_| QuickRowsError::other("CSV document lock was poisoned"))?;
            let changed = document.set_display_rows_deleted_cancellable(
                &rows,
                mutation.deleted(),
                &cancellation,
            )?;
            Ok::<_, QuickRowsError>((document.is_dirty(), changed))
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                let mutation_is_current = this
                    .operation_is_current(OperationKind::Rows, operation_generation)
                    && this
                        .document
                        .loaded
                        .as_ref()
                        .is_some_and(|loaded| Arc::ptr_eq(&loaded.document, &completion_document));
                if !mutation_is_current {
                    return;
                }
                this.finish_cancellable_operation();
                match result {
                    Ok((dirty, changed)) => {
                        if let Some(loaded) = &mut this.document.loaded {
                            loaded.dirty = dirty;
                        }
                        this.mark_results_stale();
                        this.invalidate_row_cache();
                        this.feedback.notice = Some(
                            format!(
                                "{} {} {}. Save to write changes to disk.",
                                mutation.past_tense(),
                                format_count(changed),
                                counted_noun(changed, "row", "rows")
                            )
                            .into(),
                        );
                    }
                    Err(error) if error.kind() == ErrorKind::Cancelled => {
                        this.feedback.notice = Some(format!("{} cancelled.", mutation.past_tense()).into());
                    }
                    Err(error) => {
                        this.feedback.error = Some(error.to_string().into());
                        this.feedback.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }
}
