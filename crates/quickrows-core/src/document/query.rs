struct SearchRequest<'a> {
    query: &'a str,
    column: Option<usize>,
    match_case: bool,
    whole_word: bool,
    operation: QueryOperationContext<'a>,
}

struct DuplicateRequest<'a> {
    column: Option<usize>,
    operation: QueryOperationContext<'a>,
}

impl CsvDocument {
    pub fn prepare_search_index_build(&self) -> SearchIndexBuild {
        SearchIndexBuild {
            path: self.data_path.clone(),
            settings: self.storage_settings.clone(),
            offsets: self.offsets.clone(),
            mmap: self.mmap.clone(),
            _prepared_source: self._prepared_source.clone(),
            column_count: self.metadata.headers.len(),
            edits: self.edits.clone(),
            generation: self.generation,
            revision: self.revision,
        }
    }

    pub fn install_search_index(&mut self, index: BuiltSearchIndex) -> bool {
        if index.generation != self.generation || index.revision != self.revision {
            return false;
        }
        self.search_index = Some(index.columns);
        self.indexed_search_column = None;
        true
    }

    pub fn build_search_index(&mut self) -> QuickRowsResult<()> {
        self.build_search_index_with_cancellation(None)
    }

    pub fn build_search_index_cancellable(
        &mut self,
        cancellation: &CancellationToken,
    ) -> QuickRowsResult<()> {
        self.build_search_index_with_cancellation(Some(cancellation))
    }

    fn build_search_index_with_cancellation(
        &mut self,
        cancellation: Option<&CancellationToken>,
    ) -> QuickRowsResult<()> {
        cancellation.map(CancellationToken::check).transpose()?;
        let column_count = self.metadata.headers.len();
        let mut columns = (0..column_count)
            .map(|_| Some(HashMap::new()))
            .collect::<Vec<Option<ColumnSearchIndex>>>();
        for start in (0..self.row_count()).step_by(INDEX_CHUNK_SIZE) {
            cancellation.map(CancellationToken::check).transpose()?;
            let end = (start + INDEX_CHUNK_SIZE).min(self.row_count());
            let indices = (start..end).collect::<Vec<_>>();
            for (source_row, mut row) in (start..end).zip(self.read_source_rows(&indices)?) {
                if self.edits.is_deleted(source_row) {
                    continue;
                }
                self.edits.apply(source_row, &mut row);
                for (column, value) in row.into_iter().enumerate().take(column_count) {
                    let Some(index) = columns[column].as_mut() else {
                        continue;
                    };
                    // Keep complete normalized values. Truncating index keys
                    // changes contains and whole-cell query semantics.
                    index_value(index, value.to_lowercase(), source_row);
                    if index.len() > INDEX_MAX_CARDINALITY {
                        columns[column] = None;
                    }
                }
            }
        }
        cancellation.map(CancellationToken::check).transpose()?;
        for index in columns.iter_mut().flatten() {
            compact_index(index);
        }
        cancellation.map(CancellationToken::check).transpose()?;
        self.search_index = Some(columns);
        self.indexed_search_column = None;
        Ok(())
    }

    pub fn ensure_search_index_for_column_cancellable(
        &mut self,
        column: usize,
        cancellation: &CancellationToken,
        progress: Option<&dyn Fn(usize, usize)>,
    ) -> QuickRowsResult<()> {
        if column >= self.metadata.headers.len() {
            return Err(QuickRowsError::out_of_range(
                "Search column is out of range",
            ));
        }
        let already_ready = self
            .search_index
            .as_ref()
            .and_then(|columns| columns.get(column))
            .is_some_and(Option::is_some);
        if already_ready || self.indexed_search_column == Some(column) {
            return Ok(());
        }

        // Build off to the side so cancellation or a read failure preserves
        // whichever complete index is currently installed. A successful build
        // still retains at most this one lazily-built column.
        let mut index = HashMap::new();
        let mut exceeded_cardinality = false;
        for start in (0..self.row_count()).step_by(INDEX_CHUNK_SIZE) {
            cancellation.check()?;
            let end = (start + INDEX_CHUNK_SIZE).min(self.row_count());
            let projected = self.read_source_column_range(start, end, column)?;
            for (offset, source_value) in projected.into_iter().enumerate() {
                let source_row = start + offset;
                if self.edits.is_deleted(source_row) {
                    continue;
                }
                let value = self
                    .edits
                    .edited_cell(source_row, column)
                    .map(str::to_owned)
                    .or(source_value);
                if let Some(value) = value {
                    index_value(&mut index, value.to_lowercase(), source_row);
                    if index.len() > INDEX_MAX_CARDINALITY {
                        exceeded_cardinality = true;
                        break;
                    }
                }
            }
            if let Some(progress) = progress {
                progress(end, self.row_count());
            }
            cancellation.check()?;
            if exceeded_cardinality {
                break;
            }
        }

        cancellation.check()?;
        let mut columns = (0..self.metadata.headers.len())
            .map(|_| None)
            .collect::<Vec<Option<ColumnSearchIndex>>>();
        if !exceeded_cardinality {
            compact_index(&mut index);
            columns[column] = Some(index);
        }
        cancellation.check()?;
        self.search_index = Some(columns);
        self.indexed_search_column = Some(column);
        Ok(())
    }

    pub fn clear_search_index(&mut self) {
        self.search_index = None;
        self.indexed_search_column = None;
    }

    pub fn has_search_index(&self) -> bool {
        self.search_index
            .as_ref()
            .is_some_and(|columns| columns.iter().any(Option::is_some))
    }

    pub fn has_search_index_for_column(&self, column: usize) -> bool {
        self.search_index
            .as_ref()
            .and_then(|columns| columns.get(column))
            .is_some_and(Option::is_some)
    }

    pub fn search(
        &self,
        query: &str,
        column: Option<usize>,
        match_case: bool,
        whole_word: bool,
    ) -> QuickRowsResult<Vec<usize>> {
        self.search_request(SearchRequest {
            query,
            column,
            match_case,
            whole_word,
            operation: OperationContext::new(None, None),
        })
    }

    pub fn search_cancellable(
        &self,
        query: &str,
        column: Option<usize>,
        match_case: bool,
        whole_word: bool,
        cancellation: &CancellationToken,
    ) -> QuickRowsResult<Vec<usize>> {
        self.search_request(SearchRequest {
            query,
            column,
            match_case,
            whole_word,
            operation: OperationContext::new(Some(cancellation), None),
        })
    }

    pub fn search_cancellable_streaming(
        &self,
        query: &str,
        column: Option<usize>,
        match_case: bool,
        whole_word: bool,
        cancellation: &CancellationToken,
        progress: &dyn Fn(&[usize], usize, usize),
    ) -> QuickRowsResult<Vec<usize>> {
        self.search_request(SearchRequest {
            query,
            column,
            match_case,
            whole_word,
            operation: OperationContext::new(Some(cancellation), Some(progress)),
        })
    }

    fn search_request(&self, request: SearchRequest<'_>) -> QuickRowsResult<Vec<usize>> {
        let SearchRequest {
            query,
            column,
            match_case,
            whole_word,
            operation,
        } = request;
        operation.check()?;
        let progress = operation.progress;
        if query.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(column) = column
            && column >= self.metadata.headers.len()
        {
            return Err(QuickRowsError::out_of_range(
                "Search column is out of range",
            ));
        }
        if !match_case
            && let (Some(column), Some(columns)) = (column, self.search_index.as_ref())
            && let Some(Some(index)) = columns.get(column)
        {
            let query = query.to_lowercase();
            if whole_word {
                let matches = index
                    .get(&query)
                    .map(RowPostings::to_vec)
                    .unwrap_or_default();
                if let Some(progress) = progress {
                    progress(&matches, self.row_count(), self.row_count());
                }
                return Ok(matches);
            }
            let mut matches = Vec::new();
            let mut processed = 0;
            for (value, rows) in index {
                operation.check()?;
                processed += rows.len();
                if value.contains(&query) {
                    matches.extend(rows.as_slice().iter().copied());
                    if let Some(progress) = progress {
                        progress(rows.as_slice(), processed, self.row_count());
                    }
                } else if let Some(progress) = progress {
                    progress(&[], processed, self.row_count());
                }
            }
            matches.par_sort_unstable();
            matches.dedup();
            return Ok(matches);
        }

        if !self.edits.is_dirty() {
            let mut matches = Vec::new();
            for start in (0..self.row_count()).step_by(INDEX_CHUNK_SIZE) {
                operation.check()?;
                let end = (start + INDEX_CHUNK_SIZE).min(self.row_count());
                let chunk_matches =
                    self.search_source_range(start, end, column, query, match_case, whole_word)?;
                if let Some(progress) = progress {
                    progress(&chunk_matches, end, self.row_count());
                }
                matches.extend(chunk_matches);
            }
            return Ok(matches);
        }

        if let Some(column) = column {
            let normalized_query = (!match_case).then(|| query.to_lowercase());
            let query_for_edits = normalized_query.as_deref().unwrap_or(query);
            let mut edited_matches = Vec::new();
            for (index, (&source_row, row_edits)) in self.edits.cells.iter().enumerate() {
                if index % INDEX_CHUNK_SIZE == 0 {
                    operation.check()?;
                }
                if self.edits.is_deleted(source_row) {
                    continue;
                }
                let Some(value) = row_edits.get(&column) else {
                    continue;
                };
                let is_match = if match_case {
                    if whole_word {
                        value == query_for_edits
                    } else {
                        value.contains(query_for_edits)
                    }
                } else {
                    let value = value.to_lowercase();
                    if whole_word {
                        value == query_for_edits
                    } else {
                        value.contains(query_for_edits)
                    }
                };
                if is_match {
                    edited_matches.push(source_row);
                }
            }
            edited_matches.sort_unstable();

            let mut matches = Vec::new();
            for start in (0..self.row_count()).step_by(INDEX_CHUNK_SIZE) {
                operation.check()?;
                let end = (start + INDEX_CHUNK_SIZE).min(self.row_count());
                let mut chunk_matches = self.search_source_range(
                    start,
                    end,
                    Some(column),
                    query,
                    match_case,
                    whole_word,
                )?;
                chunk_matches.retain(|source_row| {
                    !self.edits.is_deleted(*source_row)
                        && self.edits.edited_cell(*source_row, column).is_none()
                });
                let first_edit = edited_matches.partition_point(|source_row| *source_row < start);
                let after_last_edit =
                    edited_matches.partition_point(|source_row| *source_row < end);
                chunk_matches.extend_from_slice(&edited_matches[first_edit..after_last_edit]);
                chunk_matches.sort_unstable();
                if let Some(progress) = progress {
                    progress(&chunk_matches, end, self.row_count());
                }
                matches.extend(chunk_matches);
            }
            return Ok(matches);
        }

        let normalized_query = (!match_case).then(|| query.to_lowercase());
        let query = normalized_query.as_deref().unwrap_or(query);
        let mut matches = Vec::new();
        for start in (0..self.row_count()).step_by(INDEX_CHUNK_SIZE) {
            operation.check()?;
            let end = (start + INDEX_CHUNK_SIZE).min(self.row_count());
            let indices = (start..end).collect::<Vec<_>>();
            let chunk_match_start = matches.len();
            for (source_row, mut row) in (start..end).zip(self.read_source_rows(&indices)?) {
                if self.edits.is_deleted(source_row) {
                    continue;
                }
                self.edits.apply(source_row, &mut row);
                let cells: Box<dyn Iterator<Item = &str> + '_> = match column {
                    Some(column) => Box::new(row.get(column).into_iter().map(String::as_str)),
                    None => Box::new(row.iter().map(String::as_str)),
                };
                let is_match = cells.into_iter().any(|value| {
                    if match_case {
                        if whole_word {
                            value == query
                        } else {
                            value.contains(query)
                        }
                    } else {
                        let value = value.to_lowercase();
                        if whole_word {
                            value == query
                        } else {
                            value.contains(query)
                        }
                    }
                });
                if is_match {
                    matches.push(source_row);
                }
            }
            if let Some(progress) = progress {
                progress(&matches[chunk_match_start..], end, self.row_count());
            }
        }
        Ok(matches)
    }

    pub fn find_duplicates(&self, column: Option<usize>) -> QuickRowsResult<Vec<usize>> {
        self.find_duplicates_request(DuplicateRequest {
            column,
            operation: OperationContext::new(None, None),
        })
    }

    pub fn find_duplicates_cancellable(
        &self,
        column: Option<usize>,
        cancellation: &CancellationToken,
    ) -> QuickRowsResult<Vec<usize>> {
        self.find_duplicates_request(DuplicateRequest {
            column,
            operation: OperationContext::new(Some(cancellation), None),
        })
    }

    pub fn find_duplicates_cancellable_streaming(
        &self,
        column: Option<usize>,
        cancellation: &CancellationToken,
        progress: &dyn Fn(&[usize], usize, usize),
    ) -> QuickRowsResult<Vec<usize>> {
        self.find_duplicates_request(DuplicateRequest {
            column,
            operation: OperationContext::new(Some(cancellation), Some(progress)),
        })
    }

    fn find_duplicates_request(
        &self,
        request: DuplicateRequest<'_>,
    ) -> QuickRowsResult<Vec<usize>> {
        let DuplicateRequest { column, operation } = request;
        operation.check()?;
        let progress = operation.progress;
        if column.is_some_and(|column| column >= self.metadata.headers.len()) {
            return Err(QuickRowsError::out_of_range(
                "Duplicate column is out of range",
            ));
        }
        let mut hashes = Vec::with_capacity(self.row_count());
        for start in (0..self.row_count()).step_by(INDEX_CHUNK_SIZE) {
            operation.check()?;
            let end = (start + INDEX_CHUNK_SIZE).min(self.row_count());
            let indices = (start..end).collect::<Vec<_>>();
            for (source_row, mut row) in (start..end).zip(self.read_source_rows(&indices)?) {
                if self.edits.is_deleted(source_row) {
                    continue;
                }
                self.edits.apply(source_row, &mut row);
                let mut hasher = DefaultHasher::new();
                match column {
                    Some(column) => row[column].hash(&mut hasher),
                    None => row.hash(&mut hasher),
                }
                hashes.push((hasher.finish(), source_row));
            }
            if let Some(progress) = progress {
                progress(&[], end, self.row_count());
            }
        }
        hashes.par_sort_unstable_by_key(|entry| entry.0);
        operation.check()?;

        let mut duplicates = Vec::new();
        let mut start = 0;
        while start < hashes.len() {
            operation.check()?;
            let mut end = start + 1;
            while end < hashes.len() && hashes[end].0 == hashes[start].0 {
                end += 1;
            }
            if end - start > 1 {
                let candidates = hashes[start..end]
                    .iter()
                    .map(|entry| entry.1)
                    .collect::<Vec<_>>();
                let rows = self.read_source_rows(&candidates)?;
                let mut groups: HashMap<Vec<String>, Vec<usize>> = HashMap::new();
                for (source_row, mut row) in candidates.into_iter().zip(rows) {
                    self.edits.apply(source_row, &mut row);
                    let key = match column {
                        Some(column) => vec![row[column].clone()],
                        None => row,
                    };
                    groups.entry(key).or_default().push(source_row);
                }
                let new_duplicates = groups
                    .into_values()
                    .filter(|rows| rows.len() > 1)
                    .flatten()
                    .collect::<Vec<_>>();
                if let Some(progress) = progress {
                    progress(&new_duplicates, self.row_count(), self.row_count());
                }
                duplicates.extend(new_duplicates);
            }
            start = end;
        }
        duplicates.sort_unstable();
        Ok(duplicates)
    }
}
