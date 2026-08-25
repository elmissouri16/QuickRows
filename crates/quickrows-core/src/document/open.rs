struct OpenRequest<'a> {
    path: &'a Path,
    overrides: Option<ParseOverrides>,
    cache_root: Option<&'a Path>,
    operation: OpenOperationContext<'a>,
}

impl CsvDocument {
    pub fn open(
        path: impl AsRef<Path>,
        overrides: Option<ParseOverrides>,
        progress: Option<&dyn Fn(usize)>,
    ) -> QuickRowsResult<Self> {
        Self::open_request(OpenRequest {
            path: path.as_ref(),
            overrides,
            cache_root: None,
            operation: OperationContext::new(None, progress),
        })
    }

    pub fn open_cached(
        path: impl AsRef<Path>,
        overrides: Option<ParseOverrides>,
        progress: Option<&dyn Fn(usize)>,
        cache_root: impl AsRef<Path>,
    ) -> QuickRowsResult<Self> {
        Self::open_request(OpenRequest {
            path: path.as_ref(),
            overrides,
            cache_root: Some(cache_root.as_ref()),
            operation: OperationContext::new(None, progress),
        })
    }

    pub fn open_cancellable(
        path: impl AsRef<Path>,
        overrides: Option<ParseOverrides>,
        progress: Option<&dyn Fn(usize)>,
        cancellation: &CancellationToken,
    ) -> QuickRowsResult<Self> {
        Self::open_request(OpenRequest {
            path: path.as_ref(),
            overrides,
            cache_root: None,
            operation: OperationContext::new(Some(cancellation), progress),
        })
    }

    pub fn open_cancellable_cached(
        path: impl AsRef<Path>,
        overrides: Option<ParseOverrides>,
        progress: Option<&dyn Fn(usize)>,
        cancellation: &CancellationToken,
        cache_root: impl AsRef<Path>,
    ) -> QuickRowsResult<Self> {
        Self::open_request(OpenRequest {
            path: path.as_ref(),
            overrides,
            cache_root: Some(cache_root.as_ref()),
            operation: OperationContext::new(Some(cancellation), progress),
        })
    }

    fn open_request(request: OpenRequest<'_>) -> QuickRowsResult<Self> {
        let OpenRequest {
            path,
            overrides,
            cache_root,
            operation,
        } = request;
        operation.check()?;
        let progress = operation.progress;
        let cancellation = operation.cancellation;
        let is_cancelled = || cancellation.is_some_and(CancellationToken::is_cancelled);
        let path = path.to_path_buf();
        if let Some(overrides) = overrides.as_ref() {
            validate_parse_overrides(overrides)?;
        }
        let SourceSnapshot {
            temporary,
            fingerprint: source_fingerprint,
        } = snapshot_csv_source(&path, &is_cancelled)?;
        // All parsing and cache work below uses the immutable capture. A final
        // cancellable live fingerprint rejects source changes before open returns.
        let immutable_source = Arc::new(temporary);
        let immutable_path = immutable_source.path().to_path_buf();
        let encoding_override = overrides
            .as_ref()
            .and_then(|overrides| overrides.encoding.as_deref());
        let detected = detect_parse_settings_for_encoding(&immutable_path, encoding_override)?;
        let detected_settings = apply_parse_overrides(&detected, None);
        let explicit_headers = overrides.as_ref().and_then(|value| value.has_headers);
        let mut settings = apply_parse_overrides(&detected, overrides);
        validate_parse_settings(&settings)?;
        let mut prepared =
            prepare_csv_source_cancellable(&immutable_path, &settings, progress, &is_cancelled)?;
        if prepared.temporary.is_none() {
            prepared.path = immutable_path;
            prepared.temporary = Some(immutable_source);
        } else {
            drop(immutable_source);
        }
        let data_path = prepared.path.clone();
        let mut storage_settings = prepared.settings.clone();
        if explicit_headers.is_none() {
            let has_headers = detect_headers_for_settings(&data_path, &storage_settings)?;
            settings.has_headers = has_headers;
            storage_settings.has_headers = has_headers;
        }
        let mut warnings = prepared.warnings.clone();
        let headers = get_headers(&data_path, &storage_settings, &mut warnings)?;
        let expected_columns = (!headers.is_empty()).then_some(headers.len());
        let header_warning_count = warnings.len();
        let mmap = if prepared.temporary.is_some() {
            open_immutable_mmap_if_large(&data_path).map_err(QuickRowsError::from)?
        } else {
            None
        };
        let data_len = std::fs::metadata(&data_path)
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let (disk_cache_dir, disk_cache_key, cached_offsets, cached_warnings) = match cache_root {
            Some(root) => match ensure_cache_dir(root) {
                Ok(dir) => {
                    let key = cache_key_from_fingerprint(
                        &path,
                        Some(settings_cache_hash(&settings)),
                        source_fingerprint,
                    );
                    prune_cache_dir(&dir);
                    let offsets = read_offsets_cache(&offsets_cache_path(&dir, key), key)
                        .ok()
                        .flatten()
                        .filter(|offsets| cached_offsets_are_valid(offsets, data_len));
                    let cached_warnings = read_warnings_cache(&warnings_cache_path(&dir, key), key)
                        .ok()
                        .flatten();
                    if offsets.is_some() && cached_warnings.is_some() {
                        (Some(dir), Some(key), offsets, cached_warnings)
                    } else {
                        (Some(dir), Some(key), None, None)
                    }
                }
                _ => (None, None, None, None),
            },
            None => (None, None, None, None),
        };
        let loaded_offsets_from_cache = cached_offsets.is_some();
        let offsets = if let Some(offsets) = cached_offsets {
            if let Some(cached_warnings) = cached_warnings {
                warnings.extend(cached_warnings);
            }
            if let Some(progress) = progress {
                progress(offsets.len());
            }
            offsets
        } else {
            match (mmap.as_deref(), cancellation) {
                (Some(mmap), Some(cancellation)) => build_row_offsets_mmap_cancellable(
                    &mmap[..],
                    &storage_settings,
                    expected_columns,
                    &mut warnings,
                    progress,
                    &|| cancellation.is_cancelled(),
                ),
                (Some(mmap), None) => build_row_offsets_mmap(
                    &mmap[..],
                    &storage_settings,
                    expected_columns,
                    &mut warnings,
                    progress,
                ),
                (None, Some(cancellation)) => build_row_offsets_cancellable(
                    &data_path,
                    &storage_settings,
                    expected_columns,
                    &mut warnings,
                    progress,
                    &|| cancellation.is_cancelled(),
                ),
                (None, None) => build_row_offsets(
                    &data_path,
                    &storage_settings,
                    expected_columns,
                    &mut warnings,
                    progress,
                ),
            }?
        };
        cancellation.map(CancellationToken::check).transpose()?;
        match file_fingerprint_cancellable(&path, &is_cancelled) {
            Ok(final_fingerprint) if final_fingerprint == source_fingerprint => {}
            Ok(_) => {
                return Err(QuickRowsError::source_changed(
                    "CSV changed on disk while it was being opened",
                ));
            }
            Err(error) if error.kind() == ErrorKind::Io => {
                return Err(QuickRowsError::source_changed(format!(
                    "CSV changed or became inaccessible while it was being opened: {error}"
                )));
            }
            Err(error) => return Err(error),
        }
        // A valid cache hit is already complete. Rewriting it would serialize every
        // row offset again and turn warm opens into the slowest path.
        if !loaded_offsets_from_cache
            && let (Some(dir), Some(key)) = (&disk_cache_dir, disk_cache_key)
        {
            let _ = write_offsets_cache(&offsets_cache_path(dir, key), key, &offsets);
            let _ = write_warnings_cache(
                &warnings_cache_path(dir, key),
                key,
                &warnings[header_warning_count..],
            );
        }
        warnings.truncate(MAX_WARNING_COUNT);
        let metadata = CsvMetadata {
            headers,
            detected: parse_info_from_settings(&detected_settings),
            effective: parse_info_from_settings(&settings),
            warnings,
            row_count: offsets.len(),
        };

        Ok(Self {
            path,
            source_fingerprint,
            data_path,
            settings,
            storage_settings,
            metadata,
            offsets,
            mmap,
            _prepared_source: prepared.temporary,
            comments: prepared.comments,
            cache: CsvCache::new(DEFAULT_CACHE_CHUNKS),
            sorted_order: None,
            sorted_inverse: None,
            sort_spec: None,
            search_index: None,
            indexed_search_column: None,
            edits: DocumentEdits::default(),
            cache_root: cache_root.map(Path::to_path_buf),
            disk_cache_dir,
            disk_cache_key,
            generation: next_document_generation(),
            revision: 0,
        })
    }

    fn from_saved_build(
        raw_path: &Path,
        source_fingerprint: FileFingerprint,
        settings: ParseSettings,
        detected_settings: ParseSettings,
        prepared: PreparedCsvSource,
        headers: Vec<String>,
        offsets: Vec<u64>,
    ) -> QuickRowsResult<Self> {
        let PreparedCsvSource {
            path: data_path,
            settings: storage_settings,
            temporary,
            mut warnings,
            comments,
        } = prepared;
        let mmap = if temporary.is_some() {
            open_immutable_mmap_if_large(&data_path).map_err(QuickRowsError::from)?
        } else {
            None
        };
        warnings.truncate(MAX_WARNING_COUNT);
        let metadata = CsvMetadata {
            headers,
            detected: parse_info_from_settings(&detected_settings),
            effective: parse_info_from_settings(&settings),
            warnings,
            row_count: offsets.len(),
        };
        Ok(Self {
            path: raw_path.to_path_buf(),
            source_fingerprint,
            data_path,
            settings,
            storage_settings,
            metadata,
            offsets,
            mmap,
            _prepared_source: temporary,
            comments,
            cache: CsvCache::new(DEFAULT_CACHE_CHUNKS),
            sorted_order: None,
            sorted_inverse: None,
            sort_spec: None,
            search_index: None,
            indexed_search_column: None,
            edits: DocumentEdits::default(),
            cache_root: None,
            disk_cache_dir: None,
            disk_cache_key: None,
            generation: next_document_generation(),
            revision: 0,
        })
    }
}
