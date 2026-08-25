// Viewport row caching, background loading, progress, and runtime watchers.
impl QuickRowsView {
    fn invalidate_row_cache(&mut self) {
        self.table.document_generation = self.table.document_generation.checked_add(1).unwrap_or_default();
        self.table.row_cache.clear();
        self.table.desired_row_range = None;
        self.table.failed_row_range = None;
        // Keep an obsolete in-flight request registered. Its completion will
        // release this slot and start only the latest requested viewport.
    }

    fn cache_rows(&mut self, start: usize, rows: Vec<(usize, Vec<String>, bool)>) {
        self.table.row_cache.clear();
        self.table.row_cache.extend(rows.into_iter().enumerate().map(
            |(offset, (source_row, cells, deleted))| {
                (
                    start + offset,
                    CachedRow {
                        source_row,
                        cells: Arc::from(
                            cells
                                .into_iter()
                                .map(SharedString::from)
                                .collect::<Vec<_>>(),
                        ),
                        deleted,
                    },
                )
            },
        ));
    }

    fn retry_failed_rows(&mut self, cx: &mut Context<Self>) {
        self.table.failed_row_range = None;
        self.feedback.error = None;
        self.start_row_request(cx);
        cx.notify();
    }

    fn request_visible_rows(&mut self, visible: std::ops::Range<usize>, cx: &mut App) {
        let Some(loaded) = &self.document.loaded else { return };
        if loaded.row_count == 0 || visible.start >= loaded.row_count {
            return;
        }
        let visible = visible.start
            ..visible
                .end
                .min(visible.start.saturating_add(MAX_CACHED_ROWS))
                .min(loaded.row_count);
        if self.table.desired_row_range.as_ref() != Some(&visible) {
            self.table.desired_row_range = Some(visible);
            self.table.failed_row_range = None;
        }
        self.start_row_request(cx);
    }

    fn start_row_request(&mut self, cx: &mut App) {
        if self.operation.is_running() || self.table.row_request_in_flight.is_some() {
            return;
        }
        let Some(visible) = self.table.desired_row_range.clone() else {
            return;
        };
        if (visible.clone()).all(|row| self.table.row_cache.contains_key(&row))
            || self
                .table.failed_row_range
                .as_ref()
                .is_some_and(|(generation, range)| {
                    *generation == self.table.document_generation && range == &visible
                })
        {
            return;
        }
        let Some(loaded) = &self.document.loaded else { return };
        let Some(weak) = self.runtime.self_weak.clone() else {
            return;
        };
        let row_count = loaded.row_count;
        let document = loaded.document.clone();
        let document_identity = loaded.document.clone();
        let visible_len = visible.len().min(MAX_CACHED_ROWS);
        let padding = (MAX_CACHED_ROWS - visible_len) / 2;
        let mut start = visible.start.saturating_sub(padding);
        let mut end = start.saturating_add(MAX_CACHED_ROWS).min(row_count);
        start = end.saturating_sub(MAX_CACHED_ROWS);
        end = end.max(visible.end.min(row_count));
        let count = end - start;
        let generation = self.table.document_generation;
        self.table.next_row_request_id = self.table.next_row_request_id.checked_add(1).unwrap_or_default();
        let request_id = self.table.next_row_request_id;
        self.table.row_request_in_flight = Some((generation, request_id));
        let requested_visible = visible.clone();

        let task = cx.background_spawn(async move {
            let document = document
                .lock()
                .map_err(|_| QuickRowsError::other("CSV document lock was poisoned"))?;
            let rows = document.display_rows(start, count)?;
            Ok::<_, QuickRowsError>(
                rows.into_iter()
                    .enumerate()
                    .map(|(offset, (source_row, cells))| {
                        let deleted = document.is_display_row_deleted(start + offset);
                        (source_row, cells, deleted)
                    })
                    .collect::<Vec<_>>(),
            )
        });
        cx.spawn(async move |cx| {
            let result = task.await;
            let _ = weak.update(cx, |view, cx| {
                if view.table.row_request_in_flight == Some((generation, request_id)) {
                    view.table.row_request_in_flight = None;
                }
                let result_is_current = view.table.document_generation == generation
                    && view.table.desired_row_range.as_ref() == Some(&requested_visible)
                    && view
                        .document.loaded
                        .as_ref()
                        .is_some_and(|loaded| Arc::ptr_eq(&loaded.document, &document_identity));
                if result_is_current {
                    match result {
                        Ok(rows) => view.cache_rows(start, rows),
                        Err(error) => {
                            view.table.failed_row_range = Some((generation, requested_visible));
                            view.feedback.error = Some(format!("Unable to load rows: {error}").into());
                        }
                    }
                }
                view.start_row_request(cx);
                cx.notify();
            });
        })
        .detach();
    }

    fn remove_recent_file(&mut self, path: &Path, cx: &mut Context<Self>) {
        self.preferences.settings.recent_files.retain(|recent| recent != path);
        self.persist_settings();
        cx.notify();
    }

    fn open_dialog(&mut self, _: &OpenFile, _window: &mut Window, cx: &mut Context<Self>) {
        if self.editor.editing_cell.is_some() {
            self.editor.pending_edit_action = Some(PendingEditAction::OpenDialog);
            self.commit_cell_edit(cx);
            return;
        }
        self.prompt_open_dialog(cx);
    }

    fn prompt_open_dialog(&mut self, cx: &mut Context<Self>) {
        if self.operation.is_running() || self.modal_active() {
            return;
        }
        let receiver = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some("Open CSV".into()),
        });
        cx.spawn(async move |this, cx| {
            match receiver.await {
                Ok(Ok(Some(paths))) => {
                    if let Some(path) = paths.into_iter().next() {
                        this.update(cx, |this, cx| this.open_path(path.into(), cx))?;
                    }
                }
                Ok(Ok(None)) => {}
                Ok(Err(error)) => {
                    this.update(cx, |this, cx| {
                        this.feedback.error = Some(format!("Unable to open file dialog: {error}").into());
                        cx.notify();
                    })?;
                }
                Err(error) => {
                    this.update(cx, |this, cx| {
                        this.feedback.error =
                            Some(format!("File dialog closed unexpectedly: {error}").into());
                        cx.notify();
                    })?;
                }
            }
            anyhow::Ok(())
        })
        .detach();
    }

    fn track_open_progress(
        &mut self,
        progress: Arc<AtomicUsize>,
        request_id: u64,
        operation_kind: OperationKind,
        label: String,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(Duration::from_millis(100))
                    .await;
                let rows = progress.load(Ordering::Relaxed);
                let keep_tracking = this.update(cx, |this, cx| {
                    if this.operation.kind() == Some(operation_kind)
                        && this.document.open_request_id == request_id
                    {
                        this.feedback.notice =
                            Some(format!("{label}… {} rows scanned", format_count(rows)).into());
                        cx.notify();
                        true
                    } else {
                        false
                    }
                })?;
                if !keep_tracking {
                    break;
                }
            }
            anyhow::Ok(())
        })
        .detach();
    }

    fn track_row_progress(
        &mut self,
        progress: Arc<AtomicUsize>,
        total: usize,
        kind: OperationKind,
        label: &'static str,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(Duration::from_millis(100))
                    .await;
                let processed = progress.load(Ordering::Relaxed).min(total);
                let keep_tracking = this.update(cx, |this, cx| {
                    if this.operation.is_running() && this.operation.kind() == Some(kind) {
                        let percent = processed
                            .saturating_mul(100)
                            .checked_div(total)
                            .unwrap_or(100);
                        this.feedback.notice = Some(
                            format!(
                                "{label}… {}/{} rows ({percent}%)",
                                format_count(processed),
                                format_count(total)
                            )
                            .into(),
                        );
                        cx.notify();
                        true
                    } else {
                        false
                    }
                })?;
                if !keep_tracking {
                    break;
                }
            }
            anyhow::Ok(())
        })
        .detach();
    }

    fn track_query_progress(
        &mut self,
        progress: Arc<Mutex<QueryProgress>>,
        kind: OperationKind,
        request_id: u64,
        cx: &mut Context<Self>,
    ) {
        cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(Duration::from_millis(100))
                    .await;
                let (processed, total, found, pending) = match progress.lock() {
                    Ok(mut progress) => (
                        progress.processed,
                        progress.total,
                        progress.found,
                        std::mem::take(&mut progress.pending),
                    ),
                    Err(_) => break,
                };
                let keep_tracking = this.update(cx, |this, cx| {
                    let request_is_current = match kind {
                        OperationKind::Search => this.queries.search.request_id == request_id,
                        OperationKind::Duplicates => this.queries.duplicates.request_id == request_id,
                        _ => false,
                    };
                    if this.operation.is_running() && this.operation.kind() == Some(kind) && request_is_current {
                        match kind {
                            OperationKind::Search => {
                                merge_sorted_unique(&mut this.queries.search.results, pending);
                            }
                            OperationKind::Duplicates => {
                                merge_sorted_unique(&mut this.queries.duplicates.results, pending);
                            }
                            _ => {}
                        }
                        let percent = processed
                            .min(total)
                            .saturating_mul(100)
                            .checked_div(total)
                            .unwrap_or(100);
                        let label = if kind == OperationKind::Search {
                            "Searching"
                        } else {
                            "Checking duplicates"
                        };
                        this.feedback.notice = Some(
                            format!(
                                "{label}… {}/{} rows ({percent}%), {} matches",
                                format_count(processed.min(total)),
                                format_count(total),
                                format_count(found)
                            )
                            .into(),
                        );
                        cx.notify();
                        true
                    } else {
                        false
                    }
                })?;
                if !keep_tracking {
                    break;
                }
            }
            anyhow::Ok(())
        })
        .detach();
    }

    fn track_external_changes(&mut self, cx: &mut Context<Self>) {
        cx.spawn(async move |this, cx| {
            let mut unchanged_metadata_polls = 0usize;
            loop {
                cx.background_executor()
                    .timer(Duration::from_secs(2))
                    .await;
                let probe = match this.update(cx, |this, _| {
                    let loaded = this.document.loaded.as_ref()?;
                    if this.operation.is_running() || this.document.external_change_detected {
                        return None;
                    }
                    Some((loaded.path.clone(), loaded.file_fingerprint))
                }) {
                    Ok(probe) => probe,
                    Err(_) => break,
                };
                let Some((path, expected)) = probe else {
                    unchanged_metadata_polls = 0;
                    continue;
                };

                let changed = if !file_metadata_matches(&path, expected) {
                    true
                } else {
                    unchanged_metadata_polls = unchanged_metadata_polls.saturating_add(1);
                    let strong_check_interval = if expected
                        .is_some_and(|fingerprint| fingerprint.len > 1024 * 1024 * 1024)
                    {
                        300
                    } else {
                        30
                    };
                    if unchanged_metadata_polls < strong_check_interval {
                        continue;
                    }
                    unchanged_metadata_polls = 0;
                    let probe_path = path.clone();
                    let current = cx
                        .background_executor()
                        .spawn(async move { file_fingerprint(&probe_path) })
                        .await;
                    current != expected
                };
                if !changed {
                    continue;
                }
                if this
                    .update(cx, |this, cx| {
                        let Some(loaded) = &this.document.loaded else { return true };
                        if loaded.path != path
                            || loaded.file_fingerprint != expected
                            || this.operation.is_running()
                            || this.document.external_change_detected
                        {
                            return true;
                        }
                        this.document.external_change_detected = true;
                        this.feedback.notice = Some(
                            if this.is_dirty() {
                                "The CSV changed on disk. Save As or reload to avoid overwriting external changes."
                            } else {
                                "The CSV changed on disk. Reload to view the latest version."
                            }
                            .into(),
                        );
                        cx.notify();
                        true
                    })
                    .is_err()
                {
                    break;
                }
            }
            anyhow::Ok(())
        })
        .detach();
    }

    fn track_runtime_requests(
        &mut self,
        requests: Arc<Mutex<VecDeque<RuntimeRequest>>>,
        window_handle: gpui::AnyWindowHandle,
        cx: &mut Context<Self>,
    ) {
        self.runtime.window_handle = Some(window_handle);
        cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(Duration::from_millis(150))
                    .await;
                if this.update(cx, |_, _| ()).is_err() {
                    return anyhow::Ok(());
                }
                let mut pending = match requests.lock() {
                    Ok(mut requests) => requests.drain(..).collect::<VecDeque<_>>(),
                    Err(_) => VecDeque::new(),
                };
                while let Some(request) = pending.pop_front() {
                    match request {
                        RuntimeRequest::Activate => {
                            if window_handle
                                .update(cx, |_, window, cx| {
                                    window.activate_window();
                                    cx.activate(true);
                                })
                                .is_err()
                            {
                                requeue_deferred_runtime_requests(
                                    &requests,
                                    RuntimeRequest::Activate,
                                    pending,
                                );
                                return anyhow::Ok(());
                            }
                        }
                        RuntimeRequest::Open(path) => {
                            let deferred = match this.update(cx, |this, cx| {
                                if this.operation.is_running() || this.modal_active() {
                                    true
                                } else {
                                    this.open_path(path.clone(), cx);
                                    false
                                }
                            }) {
                                Ok(deferred) => deferred,
                                Err(_) => {
                                    requeue_deferred_runtime_requests(
                                        &requests,
                                        RuntimeRequest::Open(path),
                                        pending,
                                    );
                                    return anyhow::Ok(());
                                }
                            };
                            if deferred {
                                requeue_deferred_runtime_requests(
                                    &requests,
                                    RuntimeRequest::Open(path),
                                    pending,
                                );
                                break;
                            }
                        }
                    }
                }
            }
        })
        .detach();
    }
}
