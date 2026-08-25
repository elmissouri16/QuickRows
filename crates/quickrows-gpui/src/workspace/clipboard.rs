// Clipboard serialization and copy operations.
impl QuickRowsView {
    fn copy_selected(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() || self.operation.is_running() {
            return;
        }
        if let Some(selection) = self.selection.cell_selection {
            let rows = selection.rows();
            let columns = selection.columns();
            let row_start = *rows.start();
            let row_end = *rows.end();
            let column_start = *columns.start();
            let column_end = *columns.end();
            if row_end - row_start + 1 >= COPY_CONFIRM_THRESHOLD {
                self.overlay.modal = Modal::Bulk(PendingBulkAction::CopyCells {
                    row_start,
                    row_end,
                    column_start,
                    column_end,
                });
                cx.notify();
            } else {
                self.copy_cell_range(row_start, row_end, column_start, column_end, cx);
            }
            return;
        }
        let mut rows = self.selection.selected_rows.iter().collect::<Vec<_>>();
        if rows.is_empty() {
            rows.extend(self.selection.selected_row);
        }
        if rows.is_empty() {
            return;
        }
        rows.sort_unstable();
        if rows.len() >= COPY_CONFIRM_THRESHOLD {
            self.overlay.modal = Modal::Bulk(PendingBulkAction::Copy { rows });
            cx.notify();
            return;
        }
        self.copy_rows(rows, cx);
    }

    fn copy_rows(&mut self, rows: Vec<usize>, cx: &mut Context<Self>) {
        let Some(document) = self.document.loaded.as_ref().map(|loaded| loaded.document.clone()) else {
            return;
        };
        let cancellation = self.begin_cancellable_operation(OperationKind::Copy);
        self.feedback.notice = Some(
            format!(
                "Copying {} {}…",
                format_count(rows.len()),
                counted_noun(rows.len(), "row", "rows")
            )
            .into(),
        );
        let row_count = rows.len();
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_row_progress(
            progress.clone(),
            row_count,
            OperationKind::Copy,
            "Copying",
            cx,
        );
        cx.notify();
        let task = cx.background_spawn(async move {
            let update_progress = |processed, _| progress.store(processed, Ordering::Relaxed);
            document
                .lock()
                .map_err(|_| QuickRowsError::other("CSV document lock was poisoned"))?
                .serialize_display_rows_cancellable_with_progress(
                    &rows,
                    &cancellation,
                    &update_progress,
                )
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                this.finish_cancellable_operation();
                match result {
                    Ok(text) => {
                        cx.write_to_clipboard(ClipboardItem::new_string(text));
                        this.feedback.notice = Some(
                            format!(
                                "Copied {} {} to clipboard.",
                                format_count(row_count),
                                counted_noun(row_count, "row", "rows")
                            )
                            .into(),
                        );
                    }
                    Err(error) if error.kind() == ErrorKind::Cancelled => {
                        this.feedback.notice = Some("Copy cancelled.".into());
                    }
                    Err(error) => {
                        this.feedback.error = Some(format!("Unable to copy rows: {error}").into());
                        this.feedback.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn copy_cell_range(
        &mut self,
        row_start: usize,
        row_end: usize,
        column_start: usize,
        column_end: usize,
        cx: &mut Context<Self>,
    ) {
        let Some(document) = self.document.loaded.as_ref().map(|loaded| loaded.document.clone()) else {
            return;
        };
        let row_count = row_end - row_start + 1;
        let cancellation = self.begin_cancellable_operation(OperationKind::Copy);
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_row_progress(
            progress.clone(),
            row_count,
            OperationKind::Copy,
            "Copying cell range",
            cx,
        );
        self.feedback.notice = Some(
            format!(
                "Copying {} rows × {} columns…",
                format_count(row_count),
                format_count(column_end - column_start + 1)
            )
            .into(),
        );
        cx.notify();
        let task = cx.background_spawn(async move {
            let update_progress = |processed, _| progress.store(processed, Ordering::Relaxed);
            document
                .lock()
                .map_err(|_| QuickRowsError::other("CSV document lock was poisoned"))?
                .serialize_display_cell_range_cancellable_with_progress(
                    row_start,
                    row_end,
                    column_start,
                    column_end,
                    &cancellation,
                    &update_progress,
                )
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                this.finish_cancellable_operation();
                match result {
                    Ok(text) => {
                        cx.write_to_clipboard(ClipboardItem::new_string(text));
                        this.feedback.notice = Some(
                            format!(
                                "Copied {} rows × {} columns.",
                                format_count(row_count),
                                format_count(column_end - column_start + 1)
                            )
                            .into(),
                        );
                    }
                    Err(error) if error.kind() == ErrorKind::Cancelled => {
                        this.feedback.notice = Some("Cell-range copy cancelled.".into());
                    }
                    Err(error) => {
                        this.feedback.error = Some(format!("Unable to copy cell range: {error}").into());
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
