impl CsvDocument {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn source_fingerprint(&self) -> FileFingerprint {
        self.source_fingerprint
    }

    pub fn metadata(&self) -> &CsvMetadata {
        &self.metadata
    }

    pub fn row_count(&self) -> usize {
        self.offsets.len()
    }

    pub fn resolve_fragment(&self, fragment: &CsvFragment) -> Vec<ResolvedFragmentRegion> {
        let entity_rows = self
            .row_count()
            .saturating_add(usize::from(self.settings.has_headers));
        fragment.resolve(entity_rows, self.metadata.headers.len())
    }

    pub fn is_dirty(&self) -> bool {
        self.edits.is_dirty()
    }

    pub fn sort_spec(&self) -> Option<SortSpec> {
        self.sort_spec
    }

    pub fn source_row_for_display(&self, display_row: usize) -> Option<usize> {
        match &self.sorted_order {
            Some(order) => order.get(display_row).copied(),
            None => (display_row < self.row_count()).then_some(display_row),
        }
    }

    pub fn display_row_for_source(&self, source_row: usize) -> Option<usize> {
        match &self.sorted_inverse {
            Some(inverse) => inverse
                .get(source_row)
                .copied()
                .filter(|display_row| *display_row != usize::MAX),
            None => (source_row < self.row_count()).then_some(source_row),
        }
    }

    fn install_sorted_order(&mut self, order: Vec<usize>, spec: SortSpec) {
        let mut inverse = vec![usize::MAX; self.row_count()];
        for (display_row, &source_row) in order.iter().enumerate() {
            if let Some(slot) = inverse.get_mut(source_row) {
                *slot = display_row;
            }
        }
        self.sorted_order = Some(order);
        self.sorted_inverse = Some(inverse);
        self.sort_spec = Some(spec);
    }

    fn expected_columns(&self) -> Option<usize> {
        (!self.metadata.headers.is_empty()).then_some(self.metadata.headers.len())
    }

    fn read_source_rows(&self, indices: &[usize]) -> QuickRowsResult<Vec<Vec<String>>> {
        let mut warnings = Vec::new();
        let path = &self.data_path;
        match self.mmap.as_deref() {
            Some(mmap) => read_rows_by_index_mmap(
                &mmap[..],
                &self.offsets,
                indices,
                &self.storage_settings,
                self.expected_columns(),
                &mut warnings,
            ),
            None => read_rows_by_index(
                path,
                &self.offsets,
                indices,
                &self.storage_settings,
                self.expected_columns(),
                &mut warnings,
            ),
        }
    }

    fn read_source_column_range(
        &self,
        start: usize,
        end: usize,
        column: usize,
    ) -> QuickRowsResult<Vec<Option<String>>> {
        let mut warnings = Vec::new();
        match self.mmap.as_deref() {
            Some(mmap) => read_column_range_with_offsets_mmap(
                &mmap[..],
                &self.offsets,
                start,
                end,
                column,
                &self.storage_settings,
                self.expected_columns(),
                &mut warnings,
            ),
            None => read_column_range_with_offsets(
                &self.data_path,
                &self.offsets,
                start,
                end,
                column,
                &self.storage_settings,
                self.expected_columns(),
                &mut warnings,
            ),
        }
    }

    fn search_source_range(
        &self,
        start: usize,
        end: usize,
        column: Option<usize>,
        query: &str,
        match_case: bool,
        whole_word: bool,
    ) -> QuickRowsResult<Vec<usize>> {
        match self.mmap.as_deref() {
            Some(mmap) => search_range_with_offsets_mmap(
                &mmap[..],
                &self.offsets,
                start,
                end,
                column,
                query,
                match_case,
                whole_word,
                &self.storage_settings,
            ),
            None => search_range_with_offsets(
                &self.data_path,
                &self.offsets,
                start,
                end,
                column,
                query,
                match_case,
                whole_word,
                &self.storage_settings,
            ),
        }
    }

    pub fn display_rows(
        &self,
        start: usize,
        count: usize,
    ) -> QuickRowsResult<Vec<(usize, Vec<String>)>> {
        if count == 0 || start >= self.row_count() {
            return Ok(Vec::new());
        }
        let end = start.saturating_add(count).min(self.row_count());
        let source_indices: Vec<usize> = (start..end)
            .filter_map(|row| self.source_row_for_display(row))
            .collect();

        let mut rows = if self.sorted_order.is_none() {
            if let Some(cached) = self.cache.get(start, source_indices.len()) {
                cached
            } else {
                let mut warnings = Vec::new();
                let path = &self.data_path;
                let rows = match self.mmap.as_deref() {
                    Some(mmap) => read_chunk_with_offsets_mmap(
                        &mmap[..],
                        &self.offsets,
                        start,
                        source_indices.len(),
                        &self.storage_settings,
                        self.expected_columns(),
                        &mut warnings,
                    ),
                    None => read_chunk_with_offsets(
                        path,
                        &self.offsets,
                        start,
                        source_indices.len(),
                        &self.storage_settings,
                        self.expected_columns(),
                        &mut warnings,
                    ),
                }?;
                self.cache.put(start, source_indices.len(), rows.clone());
                rows
            }
        } else {
            self.read_source_rows(&source_indices)?
        };

        for (&source_row, row) in source_indices.iter().zip(rows.iter_mut()) {
            self.edits.apply(source_row, row);
        }
        Ok(source_indices.into_iter().zip(rows).collect())
    }

    pub fn edit_cell(
        &mut self,
        display_row: usize,
        column: usize,
        value: String,
    ) -> QuickRowsResult<()> {
        let source_row = self
            .source_row_for_display(display_row)
            .ok_or_else(|| "Row is out of range".to_string())?;
        self.edit_source_cell(source_row, column, value)
    }

    pub fn edit_source_cell(
        &mut self,
        source_row: usize,
        column: usize,
        value: String,
    ) -> QuickRowsResult<()> {
        if source_row >= self.row_count() {
            return Err(QuickRowsError::out_of_range("Row is out of range"));
        }
        let original = self
            .read_source_rows(&[source_row])?
            .into_iter()
            .next()
            .and_then(|row| row.get(column).cloned())
            .ok_or_else(|| "Column is out of range".to_string())?;
        self.edits.set_cell(source_row, column, &original, value);
        self.revision = self.revision.wrapping_add(1);
        // Indexed values describe the source file. Any edit can change query
        // results, so fall back to the current-document scan until rebuilt.
        self.search_index = None;
        self.indexed_search_column = None;
        Ok(())
    }

    pub fn delete_display_row(&mut self, display_row: usize) -> QuickRowsResult<()> {
        let row = self
            .source_row_for_display(display_row)
            .ok_or_else(|| "Row is out of range".to_string())?;
        self.edits.delete_row(row);
        self.revision = self.revision.wrapping_add(1);
        self.search_index = None;
        self.indexed_search_column = None;
        Ok(())
    }

    pub fn restore_display_row(&mut self, display_row: usize) -> QuickRowsResult<()> {
        let row = self
            .source_row_for_display(display_row)
            .ok_or_else(|| "Row is out of range".to_string())?;
        self.edits.restore_row(row);
        self.revision = self.revision.wrapping_add(1);
        self.search_index = None;
        self.indexed_search_column = None;
        Ok(())
    }

    pub fn is_display_row_deleted(&self, display_row: usize) -> bool {
        self.source_row_for_display(display_row)
            .is_some_and(|row| self.edits.is_deleted(row))
    }

    pub fn set_display_rows_deleted(
        &mut self,
        display_rows: &[usize],
        deleted: bool,
    ) -> QuickRowsResult<usize> {
        self.set_display_rows_deleted_with_context(
            display_rows,
            deleted,
            OperationContext::new(None, None),
        )
    }

    pub fn set_display_rows_deleted_cancellable(
        &mut self,
        display_rows: &[usize],
        deleted: bool,
        cancellation: &CancellationToken,
    ) -> QuickRowsResult<usize> {
        self.set_display_rows_deleted_with_context(
            display_rows,
            deleted,
            OperationContext::new(Some(cancellation), None),
        )
    }

    fn set_display_rows_deleted_with_context(
        &mut self,
        display_rows: &[usize],
        deleted: bool,
        operation: CancellationContext<'_>,
    ) -> QuickRowsResult<usize> {
        operation.check()?;
        let mut rows = Vec::with_capacity(display_rows.len());
        let mut seen = HashSet::with_capacity(display_rows.len());
        for &display_row in display_rows {
            operation.check()?;
            let source_row = self
                .source_row_for_display(display_row)
                .ok_or_else(|| format!("Display row {display_row} is out of range"))?;
            if seen.insert(source_row) {
                rows.push((source_row, self.edits.is_deleted(source_row)));
            }
        }

        for (applied, &(source_row, _)) in rows.iter().enumerate() {
            if let Err(error) = operation.check() {
                for &(rollback_row, was_deleted) in rows[..applied].iter().rev() {
                    if was_deleted {
                        self.edits.delete_row(rollback_row);
                    } else {
                        self.edits.restore_row(rollback_row);
                    }
                }
                return Err(error);
            }
            if deleted {
                self.edits.delete_row(source_row);
            } else {
                self.edits.restore_row(source_row);
            }
        }
        self.revision = self.revision.wrapping_add(1);
        self.search_index = None;
        self.indexed_search_column = None;
        Ok(rows
            .into_iter()
            .filter(|(_, was_deleted)| *was_deleted != deleted)
            .count())
    }

    pub fn clear_edits(&mut self) {
        self.edits.clear();
        self.revision = self.revision.wrapping_add(1);
        self.search_index = None;
        self.indexed_search_column = None;
    }
}
