// Find and duplicate-query orchestration.
impl QuickRowsView {
    fn show_find(&mut self, _: &Find, _window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        self.queries.show_find = true;
        self.queries.active_highlight = Some(ActiveHighlight::Search);
        cx.notify();
    }

    fn clear_search(&mut self, _: &ClearSearch, _window: &mut Window, cx: &mut Context<Self>) {
        self.queries.search.refresh_token = self.queries.search.refresh_token.wrapping_add(1);
        self.queries.search.request_id = self.queries.search.request_id.wrapping_add(1);
        self.cancel_query_operation(OperationKind::Search);
        self.queries.search.results.clear();
        self.queries.search.current_match = 0;
        self.queries.search.last_query = None;
        self.queries.search.stale = false;
        self.queries.search.completed = false;
        if self.queries.active_highlight == Some(ActiveHighlight::Search) {
            self.queries.active_highlight = None;
        }
        cx.notify();
    }

    fn hide_find(&mut self, cx: &mut Context<Self>) {
        self.queries.show_find = false;
        cx.notify();
    }

    fn schedule_search(&mut self, cx: &mut Context<Self>) {
        self.queries.search.refresh_token = self.queries.search.refresh_token.wrapping_add(1);
        let token = self.queries.search.refresh_token;
        self.queries.search.stale = !self.queries.search.results.is_empty();
        self.queries.search.completed = false;
        if self.operation.kind() == Some(OperationKind::Search)
            && let Some(cancellation) = self.operation.cancellation()
        {
            cancellation.cancel();
        }
        cx.notify();
        cx.spawn(async move |this, cx| {
            cx.background_executor()
                .timer(Duration::from_millis(450))
                .await;
            loop {
                let wait_for_operation = this.update(cx, |this, cx| {
                    if this.queries.search.refresh_token != token || !this.queries.show_find {
                        return false;
                    }
                    if this.operation.is_running() {
                        return true;
                    }
                    this.run_search(cx);
                    false
                })?;
                if !wait_for_operation {
                    break;
                }
                cx.background_executor()
                    .timer(Duration::from_millis(50))
                    .await;
            }
            anyhow::Ok(())
        })
        .detach();
    }

    fn mark_results_stale(&mut self) {
        self.queries.search.stale = !self.queries.search.results.is_empty();
        self.queries.search.completed = false;
        self.queries.duplicates.stale = !self.queries.duplicates.results.is_empty();
        self.queries.duplicates.completed = false;
    }

    fn select_search_column(&mut self, column: Option<usize>, cx: &mut Context<Self>) {
        if self.operation.is_running() || self.queries.search.column == column {
            return;
        }
        self.queries.search.column = column;
        self.schedule_search(cx);
    }

    fn toggle_search_match_case(&mut self, cx: &mut Context<Self>) {
        self.queries.search.match_case = !self.queries.search.match_case;
        self.schedule_search(cx);
    }

    fn toggle_search_whole_word(&mut self, cx: &mut Context<Self>) {
        self.queries.search.whole_word = !self.queries.search.whole_word;
        self.schedule_search(cx);
    }

    fn run_search(&mut self, cx: &mut Context<Self>) {
        if self.modal_active() || self.operation.is_running() {
            return;
        }
        let Some(loaded) = &self.document.loaded else { return };
        let query = self.inputs.search_input.read(cx).value().to_string();
        if query.trim().is_empty() {
            self.queries.search.results.clear();
            self.queries.search.current_match = 0;
            self.queries.search.last_query = None;
            self.queries.search.stale = false;
            self.queries.search.completed = false;
            cx.notify();
            return;
        }
        let document = loaded.document.clone();
        let requested_query = query.clone();
        self.queries.search.refresh_token = self.queries.search.refresh_token.wrapping_add(1);
        self.queries.search.last_query = Some(query.clone());
        let column = self.queries.search.column;
        let match_case = self.queries.search.match_case;
        let whole_word = self.queries.search.whole_word;
        let enable_indexing = self.preferences.settings.enable_indexing;
        self.queries.search.request_id = self.queries.search.request_id.wrapping_add(1);
        let request_id = self.queries.search.request_id;
        let cancellation = self.begin_cancellable_operation(OperationKind::Search);
        self.queries.active_highlight = Some(ActiveHighlight::Search);
        self.queries.search.results.clear();
        self.queries.search.stale = false;
        self.queries.search.completed = false;
        self.queries.search.current_match = 0;
        self.feedback.error = None;
        self.feedback.notice = Some("Searching…".into());
        let progress = Arc::new(Mutex::new(QueryProgress::default()));
        self.track_query_progress(progress.clone(), OperationKind::Search, request_id, cx);
        cx.notify();
        let task_progress = progress.clone();
        let task = cx.background_spawn(async move {
            let update_progress = |new_results: &[usize], processed: usize, total: usize| {
                if let Ok(mut progress) = task_progress.lock() {
                    progress.processed = processed;
                    progress.total = total;
                    progress.found += new_results.len();
                    progress.pending.extend_from_slice(new_results);
                }
            };
            let mut document = document
                .lock()
                .map_err(|_| QuickRowsError::other("CSV document lock was poisoned"))?;
            if enable_indexing
                && !match_case
                && let Some(column) = column
            {
                let update_index_progress = |processed, total| {
                    update_progress(&[], processed, total);
                };
                document.ensure_search_index_for_column_cancellable(
                    column,
                    &cancellation,
                    Some(&update_index_progress),
                )?;
            }
            document.search_cancellable_streaming(
                &query,
                column,
                match_case,
                whole_word,
                &cancellation,
                &update_progress,
            )
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if !this.finish_query_operation(OperationKind::Search, request_id) {
                    cx.notify();
                    return;
                }
                match result {
                    Ok(matches) => {
                        this.queries.search.results = matches;
                        this.queries.search.current_match = 0;
                        let query_changed =
                            this.inputs.search_input.read(cx).value().as_ref() != requested_query;
                        this.queries.search.stale = query_changed;
                        this.queries.search.completed = !query_changed;
                        if !query_changed {
                            this.select_current_match();
                        }
                        this.feedback.notice = Some(
                            format!("{} search matches", format_count(this.queries.search.results.len()))
                                .into(),
                        );
                        if query_changed {
                            this.schedule_search(cx);
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::Cancelled => {
                        this.feedback.notice = Some("Search cancelled.".into());
                    }
                    Err(error) => {
                        this.feedback.error = Some(format!("Search failed: {error}").into());
                        this.feedback.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn select_current_match(&mut self) {
        let (Some(loaded), Some(source_row)) = (
            &self.document.loaded,
            self.queries.search.results.get(self.queries.search.current_match).copied(),
        ) else {
            return;
        };
        if let Some(row) = loaded
            .document
            .try_lock()
            .ok()
            .and_then(|doc| doc.display_row_for_source(source_row))
        {
            self.table.row_scroll.scroll_to_item(row, ScrollStrategy::Center);
        }
    }

    fn next_search_result(&mut self, cx: &mut Context<Self>) {
        if !self.operation.is_running() && !self.queries.search.stale && !self.queries.search.results.is_empty() {
            self.queries.search.current_match = (self.queries.search.current_match + 1) % self.queries.search.results.len();
            self.select_current_match();
            cx.notify();
        }
    }

    fn previous_search_result(&mut self, cx: &mut Context<Self>) {
        if !self.operation.is_running() && !self.queries.search.stale && !self.queries.search.results.is_empty() {
            self.queries.search.current_match = if self.queries.search.current_match == 0 {
                self.queries.search.results.len() - 1
            } else {
                self.queries.search.current_match - 1
            };
            self.select_current_match();
            cx.notify();
        }
    }

    fn next_match(&mut self, _: &NextMatch, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.modal_active() {
            self.next_search_result(cx);
        }
    }

    fn previous_match(&mut self, _: &PreviousMatch, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.modal_active() {
            self.previous_search_result(cx);
        }
    }

    fn check_duplicates(
        &mut self,
        _: &CheckDuplicates,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.modal_active() {
            return;
        }
        self.queries.show_duplicates = true;
        self.queries.active_highlight = Some(ActiveHighlight::Duplicates);
        cx.notify();
    }

    fn select_duplicate_column(&mut self, column: Option<usize>, cx: &mut Context<Self>) {
        if self.operation.is_running() || self.queries.duplicates.column == column {
            return;
        }
        self.queries.duplicates.column = column;
        self.queries.duplicates.stale = !self.queries.duplicates.results.is_empty();
        self.queries.duplicates.completed = false;
        cx.notify();
    }

    fn run_duplicate_check(&mut self, cx: &mut Context<Self>) {
        if self.modal_active() || self.operation.is_running() {
            return;
        }
        let Some(loaded) = &self.document.loaded else { return };
        let document = loaded.document.clone();
        let column = self.queries.duplicates.column;
        self.queries.duplicates.request_id = self.queries.duplicates.request_id.wrapping_add(1);
        let request_id = self.queries.duplicates.request_id;
        let cancellation = self.begin_cancellable_operation(OperationKind::Duplicates);
        self.queries.active_highlight = Some(ActiveHighlight::Duplicates);
        self.queries.duplicates.results.clear();
        self.queries.duplicates.stale = false;
        self.queries.duplicates.completed = false;
        self.queries.duplicates.current_match = 0;
        self.feedback.error = None;
        self.feedback.notice = Some("Checking duplicates…".into());
        let progress = Arc::new(Mutex::new(QueryProgress::default()));
        self.track_query_progress(progress.clone(), OperationKind::Duplicates, request_id, cx);
        cx.notify();
        let task_progress = progress.clone();
        let task = cx.background_spawn(async move {
            let update_progress = |new_results: &[usize], processed: usize, total: usize| {
                if let Ok(mut progress) = task_progress.lock() {
                    progress.processed = processed;
                    progress.total = total;
                    progress.found += new_results.len();
                    progress.pending.extend_from_slice(new_results);
                }
            };
            document
                .lock()
                .map_err(|_| QuickRowsError::other("CSV document lock was poisoned"))?
                .find_duplicates_cancellable_streaming(column, &cancellation, &update_progress)
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if !this.finish_query_operation(OperationKind::Duplicates, request_id) {
                    cx.notify();
                    return;
                }
                match result {
                    Ok(matches) => {
                        this.queries.duplicates.results = matches;
                        this.queries.duplicates.stale = false;
                        this.queries.duplicates.completed = true;
                        this.queries.duplicates.current_match = 0;
                        this.select_current_duplicate();
                        this.feedback.notice = Some(
                            format!(
                                "{} rows are part of duplicate groups",
                                format_count(this.queries.duplicates.results.len())
                            )
                            .into(),
                        );
                    }
                    Err(error) if error.kind() == ErrorKind::Cancelled => {
                        this.feedback.notice = Some("Duplicate check cancelled.".into());
                    }
                    Err(error) => {
                        this.feedback.error = Some(format!("Duplicate check failed: {error}").into());
                        this.feedback.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn select_current_duplicate(&mut self) {
        let (Some(loaded), Some(source_row)) = (
            &self.document.loaded,
            self.queries.duplicates.results
                .get(self.queries.duplicates.current_match)
                .copied(),
        ) else {
            return;
        };
        if let Some(row) = loaded
            .document
            .try_lock()
            .ok()
            .and_then(|doc| doc.display_row_for_source(source_row))
        {
            self.table.row_scroll.scroll_to_item(row, ScrollStrategy::Center);
        }
    }

    fn previous_duplicate(&mut self, cx: &mut Context<Self>) {
        if self.operation.is_running() || self.queries.duplicates.stale || self.queries.duplicates.results.is_empty() {
            return;
        }
        self.queries.duplicates.current_match = if self.queries.duplicates.current_match == 0 {
            self.queries.duplicates.results.len() - 1
        } else {
            self.queries.duplicates.current_match - 1
        };
        self.select_current_duplicate();
        cx.notify();
    }

    fn next_duplicate(&mut self, cx: &mut Context<Self>) {
        if self.operation.is_running() || self.queries.duplicates.stale || self.queries.duplicates.results.is_empty() {
            return;
        }
        self.queries.duplicates.current_match =
            (self.queries.duplicates.current_match + 1) % self.queries.duplicates.results.len();
        self.select_current_duplicate();
        cx.notify();
    }

    fn clear_duplicates(&mut self, cx: &mut Context<Self>) {
        self.queries.duplicates.request_id = self.queries.duplicates.request_id.wrapping_add(1);
        self.cancel_query_operation(OperationKind::Duplicates);
        self.queries.duplicates.results.clear();
        self.queries.duplicates.current_match = 0;
        self.queries.duplicates.stale = false;
        self.queries.duplicates.completed = false;
        if self.queries.active_highlight == Some(ActiveHighlight::Duplicates) {
            self.queries.active_highlight = None;
        }
        cx.notify();
    }
}
