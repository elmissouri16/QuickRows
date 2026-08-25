use std::collections::{HashMap, hash_map::DefaultHasher};
use std::ops::Range;

#[derive(Clone, Copy)]
struct SearchOptions<'a> {
    column: Option<usize>,
    query: &'a str,
    match_case: bool,
    whole_word: bool,
    settings: &'a ParseSettings,
}

impl SearchOptions<'_> {
    fn normalized_query(&self) -> Option<String> {
        (!self.match_case).then(|| self.query.to_lowercase())
    }

    fn cell_matches(&self, cell: &[u8], normalized_query: Option<&str>) -> bool {
        let (decoded, _, _) = self.settings.encoding.decode(cell);
        let value = decoded.as_ref();
        if self.match_case {
            if self.whole_word {
                value == self.query
            } else {
                value.contains(self.query)
            }
        } else {
            let value = value.to_lowercase();
            let query = normalized_query.expect("case-insensitive search has a normalized query");
            if self.whole_word {
                value == query
            } else {
                value.contains(query)
            }
        }
    }

    fn record_matches(&self, record: &ByteRecord, normalized_query: Option<&str>) -> bool {
        match self.column {
            Some(column) => record
                .get(column)
                .is_some_and(|cell| self.cell_matches(cell, normalized_query)),
            None => record
                .iter()
                .any(|cell| self.cell_matches(cell, normalized_query)),
        }
    }
}

fn seek_to_record<R: Read + Seek>(reader: &mut csv::Reader<R>, offset: u64) -> QuickRowsResult<()> {
    if reader.position().byte() != offset {
        let mut position = Position::new();
        position.set_byte(offset);
        reader
            .seek(position)
            .map_err(map_csv_error)?;
    }
    Ok(())
}

fn search_range_from_reader<R: Read + Seek>(
    mut reader: csv::Reader<R>,
    offsets: &[u64],
    rows: Range<usize>,
    options: SearchOptions<'_>,
) -> QuickRowsResult<Vec<usize>> {
    if rows.start >= offsets.len() {
        return Ok(Vec::new());
    }

    let end = rows.end.min(offsets.len());
    let normalized_query = options.normalized_query();
    let mut record = ByteRecord::new();
    let mut matches = Vec::new();

    seek_to_record(&mut reader, offsets[rows.start])?;
    for (row_index, &row_offset) in offsets.iter().enumerate().take(end).skip(rows.start) {
        seek_to_record(&mut reader, row_offset)?;
        if !reader
            .read_byte_record(&mut record)
            .map_err(map_csv_error)?
        {
            break;
        }
        if options.record_matches(&record, normalized_query.as_deref()) {
            matches.push(row_index);
        }
    }
    Ok(matches)
}

/// Search a logical row range using buffered file I/O.
///
/// The existing argument-oriented API is retained for compatibility; internally
/// the query semantics travel together in the internal search options value.
#[allow(clippy::too_many_arguments)] // compatibility wrapper; SearchOptions groups internals
pub fn search_range_with_offsets(
    path: impl AsRef<Path>,
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: Option<usize>,
    query: &str,
    match_case: bool,
    whole_word: bool,
    settings: &ParseSettings,
) -> QuickRowsResult<Vec<usize>> {
    let file = File::open(path).map_err(QuickRowsError::from)?;
    let reader = build_reader(BufReader::new(file), settings, false);
    search_range_from_reader(
        reader,
        offsets,
        start..end,
        SearchOptions {
            column: column_idx,
            query,
            match_case,
            whole_word,
            settings,
        },
    )
}

/// Search a logical row range backed by an immutable memory map.
#[allow(clippy::too_many_arguments)] // compatibility wrapper; SearchOptions groups internals
pub fn search_range_with_offsets_mmap(
    data: &[u8],
    offsets: &[u64],
    start: usize,
    end: usize,
    column_idx: Option<usize>,
    query: &str,
    match_case: bool,
    whole_word: bool,
    settings: &ParseSettings,
) -> QuickRowsResult<Vec<usize>> {
    let reader = build_reader(Cursor::new(data), settings, false);
    search_range_from_reader(
        reader,
        offsets,
        start..end,
        SearchOptions {
            column: column_idx,
            query,
            match_case,
            whole_word,
            settings,
        },
    )
}

#[derive(Clone, Copy)]
struct DuplicateOptions {
    column: Option<usize>,
}

impl DuplicateOptions {
    fn hash_record(self, record: &ByteRecord) -> u64 {
        let mut hasher = DefaultHasher::new();
        match self.column {
            Some(column) => record.get(column).unwrap_or_default().hash(&mut hasher),
            None => {
                for field in record {
                    field.hash(&mut hasher);
                }
            }
        }
        hasher.finish()
    }

    fn record_key(self, record: &ByteRecord) -> Vec<Vec<u8>> {
        match self.column {
            Some(column) => vec![record.get(column).unwrap_or_default().to_vec()],
            None => record.iter().map(<[u8]>::to_vec).collect(),
        }
    }
}

fn compute_hashes_from_reader<R: Read + Seek>(
    mut reader: csv::Reader<R>,
    offsets: &[u64],
    options: DuplicateOptions,
) -> QuickRowsResult<Vec<(u64, usize)>> {
    let mut hashes = Vec::with_capacity(offsets.len());
    let mut record = ByteRecord::new();
    for (row_index, &offset) in offsets.iter().enumerate() {
        seek_to_record(&mut reader, offset)?;
        if !reader
            .read_byte_record(&mut record)
            .map_err(map_csv_error)?
        {
            break;
        }
        hashes.push((options.hash_record(&record), row_index));
    }
    Ok(hashes)
}

fn compute_hashes_mmap(
    data: &[u8],
    offsets: &[u64],
    settings: &ParseSettings,
    options: DuplicateOptions,
) -> QuickRowsResult<Vec<(u64, usize)>> {
    const PARALLEL_HASH_CHUNK_ROWS: usize = 10_000;

    let chunks = offsets
        .par_chunks(PARALLEL_HASH_CHUNK_ROWS)
        .enumerate()
        .map(|(chunk_index, chunk)| {
            let first_row = chunk_index * PARALLEL_HASH_CHUNK_ROWS;
            let mut reader = build_reader(Cursor::new(data), settings, false);
            let mut record = ByteRecord::new();
            let mut hashes = Vec::with_capacity(chunk.len());
            for (offset_index, &offset) in chunk.iter().enumerate() {
                seek_to_record(&mut reader, offset)?;
                if !reader
                    .read_byte_record(&mut record)
                    .map_err(map_csv_error)?
                {
                    break;
                }
                hashes.push((options.hash_record(&record), first_row + offset_index));
            }
            Ok::<_, QuickRowsError>(hashes)
        })
        .collect::<QuickRowsResult<Vec<_>>>()?;
    Ok(chunks.into_iter().flatten().collect())
}

fn duplicate_hash_runs(hashes: &[(u64, usize)]) -> Vec<Range<usize>> {
    let mut runs = Vec::new();
    let mut start = 0;
    while start < hashes.len() {
        let mut end = start + 1;
        while end < hashes.len() && hashes[end].0 == hashes[start].0 {
            end += 1;
        }
        if end - start > 1 {
            runs.push(start..end);
        }
        start = end;
    }
    runs
}

fn verify_duplicate_run<R: Read + Seek>(
    reader: &mut csv::Reader<R>,
    offsets: &[u64],
    candidates: &[(u64, usize)],
    options: DuplicateOptions,
) -> QuickRowsResult<Vec<usize>> {
    let mut groups: HashMap<Vec<Vec<u8>>, Vec<usize>> = HashMap::with_capacity(candidates.len());
    let mut record = ByteRecord::new();
    for &(_, row_index) in candidates {
        let Some(&offset) = offsets.get(row_index) else {
            return Err(QuickRowsError::out_of_range(
                "Duplicate candidate row is out of range",
            ));
        };
        seek_to_record(reader, offset)?;
        if !reader
            .read_byte_record(&mut record)
            .map_err(map_csv_error)?
        {
            return Err(QuickRowsError::invalid_csv(
                "CSV ended before a duplicate candidate row",
            ));
        }
        groups
            .entry(options.record_key(&record))
            .or_default()
            .push(row_index);
    }

    Ok(groups
        .into_values()
        .filter(|rows| rows.len() > 1)
        .flatten()
        .collect())
}

fn sort_hashes_and_collect_runs(hashes: &mut [(u64, usize)]) -> Vec<Range<usize>> {
    hashes.par_sort_unstable_by_key(|entry| entry.0);
    duplicate_hash_runs(hashes)
}

pub fn find_duplicates_hashed(
    path: impl AsRef<Path>,
    offsets: &[u64],
    settings: &ParseSettings,
    column_idx: Option<usize>,
) -> QuickRowsResult<Vec<usize>> {
    let path = path.as_ref();
    let options = DuplicateOptions { column: column_idx };
    let mut settings = settings.clone();
    settings.has_headers = false;

    let file = File::open(path).map_err(QuickRowsError::from)?;
    let mut hashes = compute_hashes_from_reader(
        build_reader(BufReader::new(file), &settings, false),
        offsets,
        options,
    )?;
    let runs = sort_hashes_and_collect_runs(&mut hashes);

    let file = File::open(path).map_err(QuickRowsError::from)?;
    let mut reader = build_reader(BufReader::new(file), &settings, false);
    let mut duplicates = Vec::new();
    for run in runs {
        duplicates.extend(verify_duplicate_run(
            &mut reader,
            offsets,
            &hashes[run],
            options,
        )?);
    }
    duplicates.sort_unstable();
    Ok(duplicates)
}

pub fn find_duplicates_hashed_mmap(
    data: &[u8],
    offsets: &[u64],
    settings: &ParseSettings,
    column_idx: Option<usize>,
) -> QuickRowsResult<Vec<usize>> {
    let options = DuplicateOptions { column: column_idx };
    let mut settings = settings.clone();
    settings.has_headers = false;

    let mut hashes = compute_hashes_mmap(data, offsets, &settings, options)?;
    let runs = sort_hashes_and_collect_runs(&mut hashes);
    let groups = runs
        .par_iter()
        .map(|run| {
            let mut reader = build_reader(Cursor::new(data), &settings, false);
            verify_duplicate_run(&mut reader, offsets, &hashes[run.clone()], options)
        })
        .collect::<QuickRowsResult<Vec<_>>>()?;

    let mut duplicates = groups.into_iter().flatten().collect::<Vec<_>>();
    duplicates.sort_unstable();
    Ok(duplicates)
}
