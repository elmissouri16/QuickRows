// Document open, reload, close, save, and conflict handling.
impl QuickRowsView {
    fn open_path(&mut self, target: OpenTarget, cx: &mut Context<Self>) {
        if self.operation.is_running() {
            return;
        }
        if self.is_dirty() {
            self.overlay.modal = Modal::Destructive(PendingDestructiveAction::Open(target));
            cx.notify();
            return;
        }
        let OpenTarget { path, fragment } = target;
        self.document.open_request_id = self.document.open_request_id.wrapping_add(1);
        let request_id = self.document.open_request_id;
        let label = format!("Opening {}", display_name(&path));
        self.feedback.error = None;
        self.feedback.notice = Some(format!("{label}… 0 rows scanned").into());
        cx.notify();

        let cancellation = self.begin_cancellable_operation(OperationKind::Open);
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_open_progress(
            progress.clone(),
            request_id,
            OperationKind::Open,
            label,
            cx,
        );
        let overrides = self.preferences.settings.parse_overrides.clone();
        let prompt_if_no_headers = overrides.has_headers.is_none();
        let cache_root = cache_path();
        let task = cx.background_spawn(async move {
            let update_progress = |rows| progress.store(rows, Ordering::Relaxed);
            let document = CsvDocument::open_cancellable_cached(
                &path,
                Some(overrides),
                Some(&update_progress),
                &cancellation,
                &cache_root,
            )?;
            Ok::<_, QuickRowsError>((path, document, fragment))
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.document.open_request_id != request_id {
                    return;
                }
                this.finish_cancellable_operation();
                match result {
                    Ok((path, document, fragment)) => {
                        let fragment_regions = fragment
                            .as_ref()
                            .map(|fragment| document.resolve_fragment(fragment))
                            .unwrap_or_default();
                        let show_header_prompt =
                            prompt_if_no_headers && !document.metadata().detected.has_headers;
                        let headers = document.metadata().headers.clone();
                        let header_labels = cache_header_labels(&headers);
                        let headers = Arc::from(headers);
                        let row_count = document.row_count();
                        let detected_parse_info = document.metadata().detected.clone();
                        let parse_info = document.metadata().effective.clone();
                        let warnings = document.metadata().warnings.clone();
                        let file_fingerprint = Some(document_file_fingerprint(&document));
                        this.preferences.settings.remember_file(path.clone());
                        this.persist_settings();
                        this.document.loaded = Some(LoadedDocument {
                            document: Arc::new(Mutex::new(document)),
                            path,
                            headers,
                            header_labels,
                            row_count,
                            detected_parse_info,
                            parse_info,
                            warnings,
                            file_fingerprint,
                            dirty: false,
                        });
                        this.overlay.modal = if show_header_prompt { Modal::HeaderPrompt } else { Modal::None };
                        this.document.external_change_detected = false;
                        this.clear_cell_editor();
                        this.invalidate_row_cache();
                        this.clear_selection();
                        this.apply_fragment_regions(&fragment_regions);
                        this.queries.search.results.clear();
                        this.queries.search.stale = false;
                        this.queries.search.completed = false;
                        this.queries.duplicates.results.clear();
                        this.queries.duplicates.stale = false;
                        this.queries.duplicates.completed = false;
                        this.queries.search.current_match = 0;
                        this.queries.search.last_query = None;
                        this.queries.duplicates.current_match = 0;
                        this.queries.active_highlight = None;
                        this.queries.show_find = false;
                        this.queries.show_duplicates = false;
                        this.feedback.notice = None;
                    }
                    Err(error) if error.kind() == ErrorKind::Cancelled => {
                        this.feedback.notice = Some("Open cancelled.".into());
                    }
                    Err(error) => {
                        this.feedback.error = Some(format!("Unable to open CSV: {error}").into());
                        this.feedback.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn reload_path(&mut self, path: PathBuf, cx: &mut Context<Self>) {
        if self.operation.is_running() {
            return;
        }
        self.document.open_request_id = self.document.open_request_id.wrapping_add(1);
        let request_id = self.document.open_request_id;
        let label = format!("Reloading {}", display_name(&path));
        self.feedback.error = None;
        self.feedback.notice = Some(format!("{label}… 0 rows scanned").into());
        cx.notify();

        let cancellation = self.begin_cancellable_operation(OperationKind::Reload);
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_open_progress(
            progress.clone(),
            request_id,
            OperationKind::Reload,
            label,
            cx,
        );
        let overrides = self.preferences.settings.parse_overrides.clone();
        let prompt_if_no_headers = overrides.has_headers.is_none();
        let cache_root = cache_path();
        let task = cx.background_spawn(async move {
            let update_progress = |rows| progress.store(rows, Ordering::Relaxed);
            let document = CsvDocument::open_cancellable_cached(
                &path,
                Some(overrides),
                Some(&update_progress),
                &cancellation,
                &cache_root,
            )?;
            Ok::<_, QuickRowsError>((path, document))
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.document.open_request_id != request_id {
                    return;
                }
                this.finish_cancellable_operation();
                match result {
                    Ok((path, document)) => {
                        let show_header_prompt =
                            prompt_if_no_headers && !document.metadata().detected.has_headers;
                        let headers = document.metadata().headers.clone();
                        let header_labels = cache_header_labels(&headers);
                        let headers = Arc::from(headers);
                        let row_count = document.row_count();
                        let detected_parse_info = document.metadata().detected.clone();
                        let parse_info = document.metadata().effective.clone();
                        let warnings = document.metadata().warnings.clone();
                        let file_fingerprint = Some(document_file_fingerprint(&document));
                        this.document.loaded = Some(LoadedDocument {
                            document: Arc::new(Mutex::new(document)),
                            path,
                            headers,
                            header_labels,
                            row_count,
                            detected_parse_info,
                            parse_info,
                            warnings,
                            file_fingerprint,
                            dirty: false,
                        });
                        this.overlay.modal = if show_header_prompt { Modal::HeaderPrompt } else { Modal::None };
                        this.document.external_change_detected = false;
                        this.clear_cell_editor();
                        this.invalidate_row_cache();
                        this.clear_selection();
                        this.queries.search.results.clear();
                        this.queries.search.stale = false;
                        this.queries.search.completed = false;
                        this.queries.duplicates.results.clear();
                        this.queries.duplicates.stale = false;
                        this.queries.duplicates.completed = false;
                        this.queries.search.current_match = 0;
                        this.queries.search.last_query = None;
                        this.queries.duplicates.current_match = 0;
                        this.queries.active_highlight = None;
                        this.queries.show_find = false;
                        this.queries.show_duplicates = false;
                        this.feedback.notice = None;
                    }
                    Err(error) if error.kind() == ErrorKind::Cancelled => {
                        this.feedback.notice =
                            Some("Reload cancelled; the previous document is still open.".into());
                    }
                    Err(error) => {
                        this.feedback.error = Some(format!("Unable to reload CSV: {error}").into());
                        this.feedback.notice = Some("The previous document is still open.".into());
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn clear_file(&mut self, _: &ClearFile, window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        if self.operation.is_running() {
            self.feedback.notice = Some("Cancel the current operation before clearing the document.".into());
            cx.notify();
            return;
        }
        if self.is_dirty() {
            self.overlay.modal = Modal::Destructive(PendingDestructiveAction::Clear);
            cx.notify();
            return;
        }
        self.clear_file_unchecked(window, cx);
    }

    fn reload_file(&mut self, _: &ReloadFile, _window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() || self.operation.is_running() {
            return;
        }
        let Some(path) = self.document.loaded.as_ref().map(|loaded| loaded.path.clone()) else {
            return;
        };
        if self.is_dirty() {
            self.overlay.modal = Modal::Destructive(PendingDestructiveAction::Reload);
            cx.notify();
        } else {
            self.reload_path(path, cx);
        }
    }

    fn close_window_action(
        &mut self,
        _: &CloseWindow,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.is_dirty() {
            self.overlay.modal = Modal::Destructive(PendingDestructiveAction::Close);
            cx.notify();
        } else {
            window.remove_window();
        }
    }

    fn quit_app(&mut self, _: &QuitApp, _window: &mut Window, cx: &mut Context<Self>) {
        if self.is_dirty() {
            self.overlay.modal = Modal::Destructive(PendingDestructiveAction::Quit);
            cx.notify();
        } else {
            cx.quit();
        }
    }

    fn clear_file_unchecked(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.operation.cancel();
        self.operation.finish();
        self.queries.search.request_id = self.queries.search.request_id.wrapping_add(1);
        self.queries.duplicates.request_id = self.queries.duplicates.request_id.wrapping_add(1);
        self.document.loaded = None;
        self.table.column_layout = None;
        self.clear_cell_editor();
        self.invalidate_row_cache();
        self.clear_selection();
        self.queries.search.results.clear();
        self.queries.search.stale = false;
        self.queries.search.completed = false;
        self.queries.duplicates.results.clear();
        self.queries.duplicates.stale = false;
        self.queries.duplicates.completed = false;
        self.queries.search.current_match = 0;
        self.queries.search.last_query = None;
        self.queries.duplicates.current_match = 0;
        self.queries.active_highlight = None;
        self.queries.show_find = false;
        self.queries.show_duplicates = false;
        self.feedback.error = None;
        self.feedback.notice = None;
        window.set_window_title(BASE_TITLE);
        cx.notify();
    }

    fn is_dirty(&self) -> bool {
        self.editor.editing_draft_dirty
            || self.editor.pending_cell_commits > 0
            || self.operation.kind() == Some(OperationKind::Rows)
            || self.document.loaded.as_ref().is_some_and(|loaded| loaded.dirty)
    }

    fn modal_active(&self) -> bool {
        self.overlay.modal.is_active()
    }

    fn begin_cancellable_operation(&mut self, kind: OperationKind) -> CancellationToken {
        self.runtime.operation_generation = self.runtime.operation_generation.wrapping_add(1);
        self.operation.start(kind)
    }

    fn operation_is_current(&self, kind: OperationKind, generation: u64) -> bool {
        self.operation.kind() == Some(kind)
            && self.runtime.operation_generation == generation
    }

    fn finish_cancellable_operation(&mut self) {
        self.operation.finish();
    }

    fn finish_query_operation(&mut self, kind: OperationKind, request_id: u64) -> bool {
        let current_request_id = match kind {
            OperationKind::Search => self.queries.search.request_id,
            OperationKind::Duplicates => self.queries.duplicates.request_id,
            _ => return false,
        };
        if current_request_id != request_id || self.operation.kind() != Some(kind) {
            return false;
        }
        self.finish_cancellable_operation();
        true
    }

    fn cancel_query_operation(&mut self, kind: OperationKind) {
        if self.operation.kind() != Some(kind) {
            return;
        }
        self.operation.cancel();
        self.finish_cancellable_operation();
    }

    fn cancel_current_operation(&mut self, cx: &mut Context<Self>) {
        if self.operation.cancel() {
            self.feedback.notice = Some("Cancelling operation…".into());
            cx.notify();
        }
    }

    fn cancel_pending_destructive(&mut self, cx: &mut Context<Self>) {
        self.overlay.modal = Modal::None;
        self.editor.pending_edit_action = None;
        cx.notify();
    }

    fn cancel_pending_bulk_action(&mut self, cx: &mut Context<Self>) {
        self.overlay.modal = Modal::None;
        cx.notify();
    }

    fn confirm_pending_bulk_action(&mut self, cx: &mut Context<Self>) {
        match std::mem::take(&mut self.overlay.modal) {
            Modal::Bulk(PendingBulkAction::Copy { rows }) => self.copy_rows(rows, cx),
            Modal::Bulk(PendingBulkAction::CopyCells {
                row_start,
                row_end,
                column_start,
                column_end,
            }) => self.copy_cell_range(row_start, row_end, column_start, column_end, cx),
            Modal::Bulk(PendingBulkAction::Delete { rows }) => {
                self.mutate_rows_background(rows, RowMutation::Delete, cx)
            }
            _ => {}
        }
    }

    fn save_pending_destructive(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.editor.editing_cell.is_some() {
            self.editor.pending_edit_action = Some(PendingEditAction::Save);
            self.commit_cell_edit(cx);
        } else {
            self.save_file(&SaveFile, window, cx);
        }
    }

    fn discard_pending_destructive(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.operation.is_running() {
            return;
        }
        self.editor.pending_edit_action = None;
        self.editor.editing_cell = None;
        self.editor.editing_draft_dirty = false;
        match std::mem::take(&mut self.overlay.modal) {
            Modal::Destructive(PendingDestructiveAction::Open(path)) => {
                self.document.loaded = None;
                self.table.column_layout = None;
                self.open_path(path, cx);
            }
            Modal::Destructive(PendingDestructiveAction::Reload) => {
                if let Some(path) = self.document.loaded.as_ref().map(|loaded| loaded.path.clone()) {
                    self.reload_path(path, cx);
                }
            }
            Modal::Destructive(PendingDestructiveAction::Clear) => self.clear_file_unchecked(window, cx),
            Modal::Destructive(PendingDestructiveAction::Close) => window.remove_window(),
            Modal::Destructive(PendingDestructiveAction::Quit) => cx.quit(),
            _ => {}
        }
    }

    fn cancel_external_save(&mut self, cx: &mut Context<Self>) {
        self.overlay.modal = Modal::None;
        cx.notify();
    }

    fn confirm_external_overwrite(&mut self, cx: &mut Context<Self>) {
        if let Modal::ExternalSave(path) = std::mem::take(&mut self.overlay.modal) {
            self.save_to_unchecked(path, true, cx);
        }
    }

    fn save_external_as(&mut self, cx: &mut Context<Self>) {
        self.overlay.modal = Modal::None;
        self.prompt_save_as(cx);
    }

    fn reload_external_change(&mut self, cx: &mut Context<Self>) {
        self.overlay.modal = Modal::None;
        let Some(path) = self.document.loaded.as_ref().map(|loaded| loaded.path.clone()) else {
            return;
        };
        if self.is_dirty() {
            self.overlay.modal = Modal::Destructive(PendingDestructiveAction::Reload);
            cx.notify();
        } else {
            self.reload_path(path, cx);
        }
    }

    fn save_file(&mut self, _: &SaveFile, _window: &mut Window, cx: &mut Context<Self>) {
        if self.editor.editing_cell.is_some() {
            self.editor.pending_edit_action = Some(PendingEditAction::Save);
            self.commit_cell_edit(cx);
            return;
        }
        if self.editor.pending_cell_commits > 0 {
            self.editor.pending_edit_action = Some(PendingEditAction::Save);
            return;
        }
        if self.operation.is_running() || !self.is_dirty() {
            return;
        }
        let Some(loaded) = &self.document.loaded else { return };
        let path = loaded.path.clone();
        self.save_to(path, cx);
    }

    fn save_file_as(&mut self, _: &SaveFileAs, _window: &mut Window, cx: &mut Context<Self>) {
        if self.editor.editing_cell.is_some() {
            self.editor.pending_edit_action = Some(PendingEditAction::SaveAs);
            self.commit_cell_edit(cx);
            return;
        }
        if self.editor.pending_cell_commits > 0 {
            self.editor.pending_edit_action = Some(PendingEditAction::SaveAs);
            return;
        }
        self.prompt_save_as(cx);
    }

    fn prompt_save_as(&mut self, cx: &mut Context<Self>) {
        if self.operation.is_running() {
            return;
        }
        let Some(loaded) = &self.document.loaded else { return };
        let directory = loaded
            .path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let suggested_name = display_name(&loaded.path);
        let receiver = cx.prompt_for_new_path(&directory, Some(&suggested_name));
        let cancellation = self.begin_cancellable_operation(OperationKind::Save);
        let operation_generation = self.runtime.operation_generation;
        self.feedback.notice = Some("Waiting for a Save As location…".into());
        cx.notify();
        cx.spawn(async move |this, cx| {
            let prompt_result = receiver.await;
            this.update(cx, |this, cx| {
                if !this.operation_is_current(OperationKind::Save, operation_generation) {
                    return;
                }
                this.finish_cancellable_operation();
                this.feedback.notice = None;
                if cancellation.is_cancelled() {
                    this.feedback.notice = Some("Save As cancelled".into());
                    cx.notify();
                    return;
                }
                match prompt_result {
                    Ok(Ok(Some(path))) => this.save_to(path, cx),
                    Ok(Ok(None)) => cx.notify(),
                    Ok(Err(error)) => {
                        this.feedback.error =
                            Some(format!("Unable to show Save As dialog: {error}").into());
                        cx.notify();
                    }
                    Err(error) => {
                        this.feedback.error =
                            Some(format!("Save As dialog closed unexpectedly: {error}").into());
                        cx.notify();
                    }
                }
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn save_to(&mut self, path: PathBuf, cx: &mut Context<Self>) {
        if self.operation.is_running() {
            return;
        }
        if self.document.external_change_detected
            && self
                .document.loaded
                .as_ref()
                .is_some_and(|loaded| loaded.path == path)
        {
            self.overlay.modal = Modal::ExternalSave(path);
            cx.notify();
            return;
        }
        self.save_to_unchecked(path, false, cx);
    }

    fn save_to_unchecked(
        &mut self,
        path: PathBuf,
        overwrite_external_changes: bool,
        cx: &mut Context<Self>,
    ) {
        if self.operation.is_running() {
            return;
        }
        let Some(loaded) = &self.document.loaded else { return };
        let document = loaded.document.clone();
        let completion_document = document.clone();
        let row_count = loaded.row_count;
        let cancellation = self.begin_cancellable_operation(OperationKind::Save);
        let operation_generation = self.runtime.operation_generation;
        self.feedback.error = None;
        self.feedback.notice = Some(format!("Saving {}…", display_name(&path)).into());
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_row_progress(
            progress.clone(),
            row_count,
            OperationKind::Save,
            "Saving",
            cx,
        );
        cx.notify();
        let save_path = path.clone();
        let task = cx.background_spawn(async move {
            let update_progress = |processed, _| progress.store(processed, Ordering::Relaxed);
            let mut document = document
                .lock()
                .map_err(|_| QuickRowsError::other("CSV document lock was poisoned"))?;
            if overwrite_external_changes {
                document.save_cancellable_with_progress_overwrite_external(
                    &save_path,
                    &cancellation,
                    &update_progress,
                )?;
            } else {
                document.save_cancellable_with_progress(
                    &save_path,
                    &cancellation,
                    &update_progress,
                )?;
            }
            Ok::<_, QuickRowsError>(())
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                let save_is_current = this
                    .operation_is_current(OperationKind::Save, operation_generation)
                    && this
                        .document
                        .loaded
                        .as_ref()
                        .is_some_and(|loaded| Arc::ptr_eq(&loaded.document, &completion_document));
                if !save_is_current {
                    return;
                }
                this.finish_cancellable_operation();
                match result {
                    Ok(()) => {
                        if let Some(loaded) = &mut this.document.loaded {
                            loaded.path = path.clone();
                            if let Ok(document) = loaded.document.lock() {
                                let headers = document.metadata().headers.clone();
                                loaded.header_labels = cache_header_labels(&headers);
                                loaded.headers = Arc::from(headers);
                                loaded.row_count = document.row_count();
                                loaded.detected_parse_info = document.metadata().detected.clone();
                                loaded.parse_info = document.metadata().effective.clone();
                                loaded.warnings = document.metadata().warnings.clone();
                                loaded.file_fingerprint =
                                    Some(document_file_fingerprint(&document));
                                loaded.dirty = false;
                            }
                        }
                        this.document.external_change_detected = false;
                        this.invalidate_row_cache();
                        this.clear_selection();
                        this.queries.search.results.clear();
                        this.queries.search.stale = false;
                        this.queries.search.completed = false;
                        this.queries.duplicates.results.clear();
                        this.queries.duplicates.stale = false;
                        this.queries.duplicates.completed = false;
                        this.queries.search.current_match = 0;
                        this.preferences.settings.remember_file(path.clone());
                        this.persist_settings();
                        this.feedback.notice = Some(format!("Saved {}", display_name(&path)).into());
                        let pending = std::mem::take(&mut this.overlay.modal);
                        match pending {
                            Modal::Destructive(PendingDestructiveAction::Open(next_path)) => {
                                this.open_path(next_path, cx);
                            }
                            Modal::Destructive(PendingDestructiveAction::Reload) => {
                                if let Some(reload_path) =
                                    this.document.loaded.as_ref().map(|loaded| loaded.path.clone())
                                {
                                    this.reload_path(reload_path, cx);
                                }
                            }
                            Modal::Destructive(PendingDestructiveAction::Clear) => {
                                this.document.loaded = None;
                                this.table.column_layout = None;
                                this.invalidate_row_cache();
                                this.clear_selection();
                                this.queries.show_find = false;
                                this.feedback.notice = None;
                            }
                            Modal::Destructive(PendingDestructiveAction::Close) => {
                                if let Some(window_handle) = this.runtime.window_handle {
                                    let _ = window_handle.update(cx, |_, window, _| {
                                        window.remove_window();
                                    });
                                }
                            }
                            Modal::Destructive(PendingDestructiveAction::Quit) => cx.quit(),
                            _ => {}
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::Cancelled => {
                        this.feedback.notice = Some("Save cancelled.".into());
                    }
                    Err(error) if error.kind() == ErrorKind::DestinationChanged => {
                        this.document.external_change_detected = true;
                        this.feedback.error = Some(format!("Unable to save CSV: {error}").into());
                        this.feedback.notice = None;
                    }
                    Err(error) => {
                        this.feedback.error = Some(format!("Unable to save CSV: {error}").into());
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
