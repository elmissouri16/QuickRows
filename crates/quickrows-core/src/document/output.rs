struct CellRangeRequest<'a> {
    row_start: usize,
    row_end: usize,
    column_start: usize,
    column_end: usize,
    operation: RowOperationContext<'a>,
}

struct SaveRequest<'a> {
    path: &'a Path,
    operation: RowOperationContext<'a>,
}

impl CsvDocument {
    pub fn sort(&mut self, spec: Option<SortSpec>) -> QuickRowsResult<()> {
        self.sort_with_context(spec, OperationContext::new(None, None))
    }

    pub fn sort_cancellable(
        &mut self,
        spec: Option<SortSpec>,
        cancellation: &CancellationToken,
    ) -> QuickRowsResult<()> {
        self.sort_with_context(spec, OperationContext::new(Some(cancellation), None))
    }

    pub fn sort_cancellable_with_progress(
        &mut self,
        spec: Option<SortSpec>,
        cancellation: &CancellationToken,
        progress: &dyn Fn(usize, usize),
    ) -> QuickRowsResult<()> {
        self.sort_with_context(
            spec,
            OperationContext::new(Some(cancellation), Some(progress)),
        )
    }

    fn sort_with_context(
        &mut self,
        spec: Option<SortSpec>,
        operation: RowOperationContext<'_>,
    ) -> QuickRowsResult<()> {
        operation.check()?;
        let cancellation = operation.cancellation;
        let progress = operation.progress;
        let Some(spec) = spec else {
            self.sorted_order = None;
            self.sorted_inverse = None;
            self.sort_spec = None;
            if let Some(progress) = progress {
                progress(self.row_count(), self.row_count());
            }
            return Ok(());
        };
        if spec.column >= self.metadata.headers.len() {
            return Err(QuickRowsError::out_of_range("Sort column is out of range"));
        }
        let ascending = spec.direction == SortDirection::Ascending;
        if !self.edits.is_dirty()
            && let (Some(dir), Some(key)) = (&self.disk_cache_dir, self.disk_cache_key)
        {
            let path = order_cache_path(dir, key, spec.column, ascending);
            if let Some(order) = read_order_cache(&path, key, spec.column, ascending)
                .ok()
                .flatten()
                .filter(|order| cached_order_is_valid(order, self.row_count()))
            {
                if let Some(progress) = progress {
                    progress(self.row_count(), self.row_count());
                }
                operation.check()?;
                self.install_sorted_order(order, spec);
                return Ok(());
            }
        }

        let mut values = Vec::with_capacity(self.row_count());
        for start in (0..self.row_count()).step_by(SORT_CHUNK_SIZE) {
            operation.check()?;
            let end = start.saturating_add(SORT_CHUNK_SIZE).min(self.row_count());
            let projected = self.read_source_column_range(start, end, spec.column)?;
            for (offset, source_value) in projected.into_iter().enumerate() {
                let source_row = start + offset;
                let value = self
                    .edits
                    .edited_cell(source_row, spec.column)
                    .map(str::to_owned)
                    .or(source_value)
                    .unwrap_or_default();
                values.push(value);
            }
            if let Some(progress) = progress {
                progress(end, self.row_count());
            }
            operation.check()?;
        }

        // Rayon cannot interrupt a single monolithic parallel sort. Sort
        // bounded chunks instead, then merge them in cancellable stages so an
        // in-flight request observes cancellation without waiting for all
        // O(n log n) comparisons to finish.
        let ascending = spec.direction == SortDirection::Ascending;
        let check_cancellation = || -> QuickRowsResult<()> {
            if let Some(cancellation) = cancellation {
                cancellation.check()?;
            }
            Ok(())
        };
        let order = sort_projected_order(
            &values,
            ascending,
            SORT_MERGE_CHUNK_SIZE,
            &check_cancellation,
            &|| {},
        )?;
        if !self.edits.is_dirty()
            && let (Some(dir), Some(key)) = (&self.disk_cache_dir, self.disk_cache_key)
        {
            check_cancellation()?;
            let path = order_cache_path(dir, key, spec.column, ascending);
            let _ = write_order_cache(&path, key, spec.column, ascending, &order);
        }
        check_cancellation()?;
        self.install_sorted_order(order, spec);
        Ok(())
    }

    pub fn serialize_display_rows(&self, display_rows: &[usize]) -> QuickRowsResult<String> {
        self.serialize_display_rows_with_context(display_rows, OperationContext::new(None, None))
    }

    pub fn serialize_display_rows_cancellable(
        &self,
        display_rows: &[usize],
        cancellation: &CancellationToken,
    ) -> QuickRowsResult<String> {
        self.serialize_display_rows_with_context(
            display_rows,
            OperationContext::new(Some(cancellation), None),
        )
    }

    pub fn serialize_display_rows_cancellable_with_progress(
        &self,
        display_rows: &[usize],
        cancellation: &CancellationToken,
        progress: &dyn Fn(usize, usize),
    ) -> QuickRowsResult<String> {
        self.serialize_display_rows_with_context(
            display_rows,
            OperationContext::new(Some(cancellation), Some(progress)),
        )
    }

    fn serialize_display_rows_with_context(
        &self,
        display_rows: &[usize],
        operation: RowOperationContext<'_>,
    ) -> QuickRowsResult<String> {
        operation.check()?;
        let progress = operation.progress;
        let mut output = String::new();
        let mut wrote_record = false;
        let mut processed = 0;
        for display_chunk in display_rows.chunks(INDEX_CHUNK_SIZE) {
            operation.check()?;
            let mut source_rows = Vec::with_capacity(display_chunk.len());
            for &display_row in display_chunk {
                let source_row = self
                    .source_row_for_display(display_row)
                    .ok_or_else(|| format!("Display row {display_row} is out of range"))?;
                if !self.edits.is_deleted(source_row) {
                    source_rows.push(source_row);
                }
            }
            let rows = self.read_source_rows(&source_rows)?;
            for (source_row, mut row) in source_rows.iter().copied().zip(rows) {
                operation.check()?;
                self.edits.apply(source_row, &mut row);
                if wrote_record {
                    output.push_str(line_ending(&self.settings));
                }
                push_csv_record(&mut output, &row, &self.settings);
                wrote_record = true;
            }
            processed += display_chunk.len();
            if let Some(progress) = progress {
                progress(processed, display_rows.len());
            }
        }

        Ok(output)
    }

    pub fn serialize_display_cell_range_cancellable_with_progress(
        &self,
        row_start: usize,
        row_end: usize,
        column_start: usize,
        column_end: usize,
        cancellation: &CancellationToken,
        progress: &dyn Fn(usize, usize),
    ) -> QuickRowsResult<String> {
        self.serialize_display_cell_range_request(CellRangeRequest {
            row_start,
            row_end,
            column_start,
            column_end,
            operation: OperationContext::new(Some(cancellation), Some(progress)),
        })
    }

    fn serialize_display_cell_range_request(
        &self,
        request: CellRangeRequest<'_>,
    ) -> QuickRowsResult<String> {
        let CellRangeRequest {
            row_start,
            row_end,
            column_start,
            column_end,
            operation,
        } = request;
        operation.check()?;
        let progress = operation
            .progress
            .expect("cell-range serialization always supplies progress");
        if row_start > row_end || row_end >= self.row_count() {
            return Err(QuickRowsError::out_of_range(
                "Cell selection row range is out of bounds",
            ));
        }
        if column_start > column_end || column_end >= self.metadata.headers.len() {
            return Err(QuickRowsError::out_of_range(
                "Cell selection column range is out of bounds",
            ));
        }
        let mut output = String::new();
        let mut wrote_record = false;
        let total = row_end - row_start + 1;
        let mut processed = 0;
        let mut chunk_start = row_start;
        while chunk_start <= row_end {
            operation.check()?;
            let chunk_end = chunk_start
                .saturating_add(INDEX_CHUNK_SIZE - 1)
                .min(row_end);
            let mut source_rows = Vec::with_capacity(chunk_end - chunk_start + 1);
            for display_row in chunk_start..=chunk_end {
                let source_row = self
                    .source_row_for_display(display_row)
                    .ok_or_else(|| format!("Display row {display_row} is out of range"))?;
                if !self.edits.is_deleted(source_row) {
                    source_rows.push(source_row);
                }
            }
            let rows = self.read_source_rows(&source_rows)?;
            for (source_row, mut row) in source_rows.iter().copied().zip(rows) {
                operation.check()?;
                self.edits.apply(source_row, &mut row);
                let selected = (column_start..=column_end)
                    .map(|column| row.get(column).cloned().unwrap_or_default())
                    .collect::<Vec<_>>();
                if wrote_record {
                    output.push_str(line_ending(&self.settings));
                }
                push_csv_record(&mut output, &selected, &self.settings);
                wrote_record = true;
            }
            processed += chunk_end - chunk_start + 1;
            progress(processed, total);
            if chunk_end == usize::MAX {
                break;
            }
            chunk_start = chunk_end + 1;
        }
        Ok(output)
    }

    pub fn save(&mut self, path: impl AsRef<Path>) -> QuickRowsResult<()> {
        self.save_request(SaveRequest {
            path: path.as_ref(),
            operation: OperationContext::new(None, None),
        })
    }

    pub fn save_cancellable(
        &mut self,
        path: impl AsRef<Path>,
        cancellation: &CancellationToken,
    ) -> QuickRowsResult<()> {
        self.save_request(SaveRequest {
            path: path.as_ref(),
            operation: OperationContext::new(Some(cancellation), None),
        })
    }

    pub fn save_cancellable_with_progress(
        &mut self,
        path: impl AsRef<Path>,
        cancellation: &CancellationToken,
        progress: &dyn Fn(usize, usize),
    ) -> QuickRowsResult<()> {
        self.save_request(SaveRequest {
            path: path.as_ref(),
            operation: OperationContext::new(Some(cancellation), Some(progress)),
        })
    }

    /// Compatibility entry point for callers that previously requested an
    /// unsafe overwrite after an external source change.
    ///
    /// QuickRows now applies the same optimistic-concurrency check as a normal
    /// save and returns [`ErrorKind::SourceChanged`] until the file is reloaded.
    #[deprecated(
        since = "0.1.1",
        note = "external changes must be reloaded before saving"
    )]
    pub fn save_cancellable_with_progress_overwrite_external(
        &mut self,
        path: impl AsRef<Path>,
        cancellation: &CancellationToken,
        progress: &dyn Fn(usize, usize),
    ) -> QuickRowsResult<()> {
        self.save_cancellable_with_progress(path, cancellation, progress)
    }

    fn save_request(&mut self, request: SaveRequest<'_>) -> QuickRowsResult<()> {
        let SaveRequest {
            path: target,
            operation,
        } = request;
        operation.check()?;
        let cancellation = operation.cancellation;
        let progress = operation.progress;
        let is_cancelled = || cancellation.is_some_and(CancellationToken::is_cancelled);
        let live_source = file_fingerprint_cancellable(&self.path, &is_cancelled).map_err(|error| {
            if error.kind() == ErrorKind::Io {
                QuickRowsError::source_changed(format!(
                    "CSV changed or became inaccessible; reload before saving: {error}"
                ))
            } else {
                error
            }
        })?;
        if live_source != self.source_fingerprint {
            return Err(QuickRowsError::source_changed(
                "CSV changed on disk; reload before saving",
            ));
        }
        let commit_target = resolve_save_target(target)?;
        let expected_destination = if target == self.path {
            let expected = DestinationState::Existing(self.source_fingerprint);
            ensure_destination_unchanged(&commit_target, expected)?;
            expected
        } else {
            destination_state(&commit_target)?
        };
        let cache_root = self.cache_root.clone();
        let effective = self.metadata.effective.clone();
        let original_malformed = self.settings.malformed;
        let validation_overrides = ParseOverrides {
            delimiter: Some(effective.delimiter),
            quote: Some(effective.quote),
            escape: Some(effective.escape.unwrap_or_else(|| "none".to_string())),
            comment: Some(effective.comment.unwrap_or_else(|| "none".to_string())),
            excel_sep: Some(effective.excel_sep),
            line_ending: Some(effective.line_ending),
            encoding: Some(effective.encoding),
            has_headers: Some(effective.has_headers),
            malformed: Some("strict".to_string()),
            max_field_size: Some(effective.max_field_size),
            max_record_size: Some(effective.max_record_size),
        };
        let parent = commit_target.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent).map_err(QuickRowsError::from)?;
        let mut temporary_builder = tempfile::Builder::new();
        temporary_builder.prefix(".quickrows-").suffix(".tmp");
        #[cfg(unix)]
        if expected_destination == DestinationState::Missing {
            use std::os::unix::fs::PermissionsExt;
            temporary_builder.permissions(std::fs::Permissions::from_mode(0o666));
        }
        let mut temporary = temporary_builder
            .tempfile_in(parent)
            .map_err(QuickRowsError::from)?;
        let mut record_settings = self.settings.clone();
        record_settings.source_bom = false;
        let terminator = line_ending(&self.settings);
        let mut comments = self.comments.iter().peekable();
        let (serialized_len, serialized_hash) = {
            let buffered = BufWriter::with_capacity(SAVE_IO_BUFFER_BYTES, temporary.as_file_mut());
            let mut output = FingerprintingWriter::new(buffered);
            if self.settings.source_bom {
                let bom: &[u8] = if self.settings.encoding == encoding_rs::UTF_16LE {
                    &[0xff, 0xfe]
                } else if self.settings.encoding == encoding_rs::UTF_16BE {
                    &[0xfe, 0xff]
                } else if self.settings.encoding == encoding_rs::UTF_8 {
                    &[0xef, 0xbb, 0xbf]
                } else {
                    &[]
                };
                output.write_all(bom).map_err(QuickRowsError::from)?;
            }

            let mut record = String::new();
            let mut encoded = Vec::new();
            let mut comment_text = String::new();
            if self.settings.excel_sep {
                comment_text.push_str("sep=");
                comment_text.push(self.settings.delimiter);
                comment_text.push_str(terminator);
                write_encoded_csv_text(&mut output, &comment_text, &record_settings, &mut encoded)?;
                comment_text.clear();
            }

            let mut write_comments = |output: &mut dyn Write,
                                      before_record: usize,
                                      text: &mut String,
                                      encoded: &mut Vec<u8>|
             -> QuickRowsResult<()> {
                while comments
                    .peek()
                    .is_some_and(|comment| comment.before_record <= before_record)
                {
                    let comment = comments.next().expect("peeked comment must exist");
                    text.clear();
                    text.push_str(&comment.text);
                    text.push_str(terminator);
                    write_encoded_csv_text(output, text, &record_settings, encoded)?;
                }
                Ok(())
            };

            write_comments(&mut output, 0, &mut comment_text, &mut encoded)?;
            if self.settings.has_headers && !self.metadata.headers.is_empty() {
                write_csv_record(
                    &mut output,
                    &self.metadata.headers,
                    &self.settings,
                    &record_settings,
                    terminator,
                    &mut record,
                    &mut encoded,
                )?;
            }
            let mut source_rows = Vec::with_capacity(INDEX_CHUNK_SIZE);
            for start in (0..self.row_count()).step_by(INDEX_CHUNK_SIZE) {
                operation.check()?;
                let end = start.saturating_add(INDEX_CHUNK_SIZE).min(self.row_count());
                source_rows.clear();
                source_rows.extend(start..end);
                let rows = self.read_source_rows(&source_rows).map_err(|error| {
                    prefer_source_changed(
                        &self.path,
                        self.source_fingerprint,
                        &is_cancelled,
                        error,
                        "CSV changed on disk while it was being saved",
                    )
                })?;
                for (source_row, mut row) in source_rows.iter().copied().zip(rows) {
                    operation.check()?;
                    let source_record = source_row + usize::from(self.settings.has_headers);
                    write_comments(&mut output, source_record, &mut comment_text, &mut encoded)?;
                    if self.edits.is_deleted(source_row) {
                        continue;
                    }
                    self.edits.apply(source_row, &mut row);
                    write_csv_record(
                        &mut output,
                        &row,
                        &self.settings,
                        &record_settings,
                        terminator,
                        &mut record,
                        &mut encoded,
                    )?;
                }
                if let Some(progress) = progress {
                    progress(end, self.row_count());
                }
            }
            write_comments(&mut output, usize::MAX, &mut comment_text, &mut encoded)?;
            let (buffered, len, hash) = output.finish()?;
            drop(buffered);
            (len, hash)
        };
        operation.check()?;
        let final_source = file_fingerprint_cancellable(&self.path, &is_cancelled).map_err(
            |error| {
                if error.kind() == ErrorKind::Io {
                    QuickRowsError::source_changed(format!(
                        "CSV changed or became inaccessible while saving: {error}"
                    ))
                } else {
                    error
                }
            },
        )?;
        if final_source != self.source_fingerprint {
            return Err(QuickRowsError::source_changed(
                "CSV changed on disk while saving; reload and try again",
            ));
        }

        // Validate the exact bytes to be committed and capture data-row
        // offsets. Directly indexable output stays in the atomic-save file;
        // formats requiring canonical storage use an OS-temporary backing.
        let temporary_path = temporary.path().to_path_buf();
        copy_destination_permissions(&commit_target, &temporary_path)?;
        temporary
            .as_file()
            .sync_all()
            .map_err(QuickRowsError::from)?;
        let serialized_state = capture_open_file_state(temporary.as_file())?;
        verify_path_references_open_file(&temporary_path, temporary.as_file())?;
        validate_parse_overrides(&validation_overrides)?;
        let detected = detect_parse_settings_for_encoding(
            &temporary_path,
            validation_overrides.encoding.as_deref(),
        )?;
        let detected_settings = apply_parse_overrides(&detected, None);
        let mut saved_settings = apply_parse_overrides(&detected, Some(validation_overrides));
        validate_parse_settings(&saved_settings)?;
        let expected_saved_rows = self
            .row_count()
            .saturating_sub(self.edits.deleted_rows.len());
        let mut saved = prepare_saved_csv_source_cancellable(
            &temporary_path,
            &saved_settings,
            &self.metadata.headers,
            expected_saved_rows,
            &is_cancelled,
        )?;
        if saved.raw_len != serialized_len || saved.raw_content_hash != serialized_hash {
            return Err(QuickRowsError::destination_changed(
                "Saved CSV changed while it was being validated",
            ));
        }
        verify_open_file_state(temporary.as_file(), serialized_state)?;
        verify_path_references_open_file(&temporary_path, temporary.as_file())?;
        if (self.settings.has_headers && saved.headers != self.metadata.headers)
            || !saved.prepared.warnings.is_empty()
        {
            return Err(QuickRowsError::invalid_csv(
                "Saved CSV did not round-trip under the active parse settings",
            ));
        }
        operation.check()?;
        let commit_state = serialized_state;
        let source_fingerprint = commit_state.fingerprint(serialized_len, serialized_hash)?;
        saved_settings.malformed = original_malformed;
        saved.prepared.settings.malformed = original_malformed;
        let mut saved_document = CsvDocument::from_saved_build(
            &temporary_path,
            source_fingerprint,
            saved_settings,
            detected_settings,
            saved.prepared,
            saved.headers,
            saved.offsets,
        )?;

        saved_document.rebind_saved_path(&temporary_path, target, cache_root.as_deref());
        verify_open_file_state(temporary.as_file(), commit_state)?;
        verify_path_references_open_file(&temporary_path, temporary.as_file())?;

        // Re-check both the content and symlink route immediately before the
        // atomic replacement. Missing destinations use a no-clobber rename.
        commit_temporary(
            temporary,
            target,
            &commit_target,
            expected_destination,
            saved_document.source_fingerprint,
            commit_state,
        )?;
        *self = saved_document;
        // The rename already committed the save. Some filesystems do not
        // support directory fsync; that must not leave the UI in a dirty state
        // after a successful replacement.
        let _ = sync_directory(parent);
        Ok(())
    }

    fn rebind_saved_path(
        &mut self,
        temporary_path: &Path,
        target: &Path,
        cache_root: Option<&Path>,
    ) {
        if self.data_path == temporary_path {
            self.data_path = target.to_path_buf();
        }
        self.path = target.to_path_buf();
        self.cache_root = cache_root.map(Path::to_path_buf);
        let (disk_cache_dir, disk_cache_key) = match cache_root {
            Some(root) => match ensure_cache_dir(root) {
                Ok(dir) => {
                    let key = cache_key_from_fingerprint(
                        target,
                        Some(settings_cache_hash(&self.settings)),
                        self.source_fingerprint,
                    );
                    (Some(dir), Some(key))
                }
                Err(_) => (None, None),
            },
            None => (None, None),
        };
        self.disk_cache_dir = disk_cache_dir;
        self.disk_cache_key = disk_cache_key;
    }
}
