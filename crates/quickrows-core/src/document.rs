use crate::cache::CsvCache;
use crate::csv::{
    apply_parse_overrides, build_row_offsets, build_row_offsets_cancellable,
    build_row_offsets_mmap, build_row_offsets_mmap_cancellable, detect_headers_for_settings,
    detect_parse_settings, detect_parse_settings_for_encoding, get_headers,
    parse_info_from_settings, prepare_csv_source_cancellable, read_chunk_with_offsets,
    read_chunk_with_offsets_mmap, read_rows_by_index, read_rows_by_index_mmap,
    search_range_with_offsets, search_range_with_offsets_mmap, settings_cache_hash,
    validate_parse_overrides, validate_parse_settings, ParseInfo, ParseOverrides, ParseSettings,
    ParseWarning, PreservedComment, MAX_WARNING_COUNT,
};
use crate::disk_cache::{
    cache_key_from_fingerprint, ensure_cache_dir, file_fingerprint, offsets_cache_path,
    order_cache_path, prune_cache_dir, read_offsets_cache, read_order_cache, read_warnings_cache,
    warnings_cache_path, write_offsets_cache, write_order_cache, write_warnings_cache, CacheKey,
    FileFingerprint,
};
use crate::fragment::{CsvFragment, ResolvedFragmentRegion};
use crate::mmap::open_immutable_mmap_if_large;
use memmap2::Mmap;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

// The UI keeps the visible range in its own cache. Retain only a small number
// of recently decoded chunks here so scrolling stays responsive without
// accumulating dozens of duplicate String grids.
const DEFAULT_CACHE_CHUNKS: usize = 8;
const SORT_CHUNK_SIZE: usize = 10_000;
const INDEX_CHUNK_SIZE: usize = 10_000;
const INDEX_MAX_CARDINALITY: usize = 500_000;

fn line_ending(settings: &ParseSettings) -> &'static str {
    match settings.line_ending.as_str() {
        "crlf" => "\r\n",
        "cr" => "\r",
        _ => "\n",
    }
}

fn push_csv_record(output: &mut String, fields: &[String], settings: &ParseSettings) {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            output.push(settings.delimiter);
        }
        let needs_quote = field.contains(settings.delimiter)
            || field.contains(settings.quote)
            || field.contains(['\r', '\n'])
            || (index == 0
                && settings
                    .comment
                    .is_some_and(|comment| field.starts_with(comment)));
        if !needs_quote {
            output.push_str(field);
            continue;
        }
        output.push(settings.quote);
        for ch in field.chars() {
            if ch == settings.quote {
                if let Some(escape) = settings.escape {
                    output.push(escape);
                    output.push(ch);
                } else {
                    output.push(ch);
                    output.push(ch);
                }
            } else {
                output.push(ch);
            }
        }
        output.push(settings.quote);
    }
}

fn encode_csv_text(text: &str, settings: &ParseSettings) -> Result<Vec<u8>, String> {
    if settings.encoding == encoding_rs::UTF_16LE || settings.encoding == encoding_rs::UTF_16BE {
        let mut output = Vec::with_capacity(text.len().saturating_mul(2).saturating_add(2));
        if settings.source_bom {
            if settings.encoding == encoding_rs::UTF_16LE {
                output.extend_from_slice(&[0xff, 0xfe]);
            } else {
                output.extend_from_slice(&[0xfe, 0xff]);
            }
        }
        for unit in text.encode_utf16() {
            let bytes = if settings.encoding == encoding_rs::UTF_16LE {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            };
            output.extend_from_slice(&bytes);
        }
        return Ok(output);
    }

    let (encoded, _, had_errors) = settings.encoding.encode(text);
    if had_errors {
        return Err(format!(
            "A value cannot be represented in {}. Change the output encoding or edit the value.",
            settings.encoding_label
        ));
    }
    let mut output = Vec::with_capacity(encoded.len() + 3);
    if settings.source_bom && settings.encoding == encoding_rs::UTF_8 {
        output.extend_from_slice(&[0xef, 0xbb, 0xbf]);
    }
    output.extend_from_slice(encoded.as_ref());
    Ok(output)
}

#[derive(Debug)]
enum RowPostings {
    One(usize),
    Many(Vec<usize>),
}

impl RowPostings {
    fn push(&mut self, row: usize) {
        match self {
            Self::One(first) => {
                let first = *first;
                *self = Self::Many(vec![first, row]);
            }
            Self::Many(rows) => rows.push(row),
        }
    }

    fn as_slice(&self) -> &[usize] {
        match self {
            Self::One(row) => std::slice::from_ref(row),
            Self::Many(rows) => rows,
        }
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn to_vec(&self) -> Vec<usize> {
        self.as_slice().to_vec()
    }

    fn shrink_to_fit(&mut self) {
        if let Self::Many(rows) = self {
            rows.shrink_to_fit();
        }
    }
}

type ColumnSearchIndex = HashMap<String, RowPostings>;

fn index_value(index: &mut ColumnSearchIndex, value: String, source_row: usize) {
    match index.entry(value) {
        std::collections::hash_map::Entry::Occupied(mut entry) => entry.get_mut().push(source_row),
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(RowPostings::One(source_row));
        }
    }
}

fn compact_index(index: &mut ColumnSearchIndex) {
    for postings in index.values_mut() {
        postings.shrink_to_fit();
    }
    index.shrink_to_fit();
}

pub struct SearchIndexBuild {
    path: PathBuf,
    settings: ParseSettings,
    offsets: Vec<u64>,
    mmap: Option<Arc<Mmap>>,
    _prepared_source: Option<Arc<tempfile::NamedTempFile>>,
    column_count: usize,
    edits: DocumentEdits,
    revision: u64,
}

pub struct BuiltSearchIndex {
    columns: Vec<Option<ColumnSearchIndex>>,
    revision: u64,
}

impl SearchIndexBuild {
    pub fn build(
        self,
        cancellation: &CancellationToken,
        progress: Option<&dyn Fn(usize, usize)>,
    ) -> Result<BuiltSearchIndex, String> {
        cancellation.check()?;
        let mut columns = (0..self.column_count)
            .map(|_| Some(HashMap::new()))
            .collect::<Vec<Option<ColumnSearchIndex>>>();
        let row_count = self.offsets.len();
        let path = self
            .path
            .to_str()
            .ok_or_else(|| "CSV paths must be valid UTF-8".to_string())?;
        for start in (0..row_count).step_by(INDEX_CHUNK_SIZE) {
            cancellation.check()?;
            let end = (start + INDEX_CHUNK_SIZE).min(row_count);
            let indices = (start..end).collect::<Vec<_>>();
            let mut warnings = Vec::new();
            let rows = match self.mmap.as_deref() {
                Some(mmap) => read_rows_by_index_mmap(
                    &mmap[..],
                    &self.offsets,
                    &indices,
                    &self.settings,
                    Some(self.column_count),
                    &mut warnings,
                ),
                None => read_rows_by_index(
                    path,
                    &self.offsets,
                    &indices,
                    &self.settings,
                    Some(self.column_count),
                    &mut warnings,
                ),
            }
            .map_err(|error| error.to_string())?;
            for (source_row, mut row) in (start..end).zip(rows) {
                if self.edits.is_deleted(source_row) {
                    continue;
                }
                self.edits.apply(source_row, &mut row);
                for (column, value) in row.into_iter().enumerate().take(self.column_count) {
                    let Some(index) = columns[column].as_mut() else {
                        continue;
                    };
                    index_value(index, value.to_lowercase(), source_row);
                    if index.len() > INDEX_MAX_CARDINALITY {
                        columns[column] = None;
                    }
                }
            }
            if let Some(progress) = progress {
                progress(end, row_count);
            }
        }
        for index in columns.iter_mut().flatten() {
            compact_index(index);
        }
        Ok(BuiltSearchIndex {
            columns,
            revision: self.revision,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    pub fn check(&self) -> Result<(), String> {
        if self.is_cancelled() {
            Err("Operation cancelled".to_string())
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CsvMetadata {
    pub headers: Vec<String>,
    pub detected: ParseInfo,
    pub effective: ParseInfo,
    pub warnings: Vec<ParseWarning>,
    pub row_count: usize,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SortSpec {
    pub column: usize,
    pub direction: SortDirection,
}

#[derive(Clone, Debug, Default)]
pub struct DocumentEdits {
    cells: HashMap<usize, HashMap<usize, String>>,
    deleted_rows: HashSet<usize>,
}

impl DocumentEdits {
    pub fn set_cell(&mut self, row: usize, column: usize, original: &str, value: String) {
        if value == original {
            if let Some(row_edits) = self.cells.get_mut(&row) {
                row_edits.remove(&column);
                if row_edits.is_empty() {
                    self.cells.remove(&row);
                }
            }
        } else {
            self.cells.entry(row).or_default().insert(column, value);
        }
    }

    pub fn delete_row(&mut self, row: usize) {
        self.deleted_rows.insert(row);
    }

    pub fn restore_row(&mut self, row: usize) {
        self.deleted_rows.remove(&row);
    }

    pub fn is_deleted(&self, row: usize) -> bool {
        self.deleted_rows.contains(&row)
    }

    pub fn is_dirty(&self) -> bool {
        !self.cells.is_empty() || !self.deleted_rows.is_empty()
    }

    pub fn clear(&mut self) {
        self.cells.clear();
        self.deleted_rows.clear();
    }

    fn apply(&self, source_row: usize, row: &mut [String]) {
        if let Some(edits) = self.cells.get(&source_row) {
            for (&column, value) in edits {
                if let Some(cell) = row.get_mut(column) {
                    *cell = value.clone();
                }
            }
        }
    }
}

pub struct CsvDocument {
    path: PathBuf,
    source_fingerprint: FileFingerprint,
    data_path: PathBuf,
    settings: ParseSettings,
    storage_settings: ParseSettings,
    metadata: CsvMetadata,
    offsets: Vec<u64>,
    mmap: Option<Arc<Mmap>>,
    _prepared_source: Option<Arc<tempfile::NamedTempFile>>,
    comments: Vec<PreservedComment>,
    cache: CsvCache,
    sorted_order: Option<Vec<usize>>,
    sorted_inverse: Option<Vec<usize>>,
    sort_spec: Option<SortSpec>,
    search_index: Option<Vec<Option<ColumnSearchIndex>>>,
    // `Some(column)` means an on-demand build for that one column has already
    // completed, including the case where its cardinality was too high to keep.
    indexed_search_column: Option<usize>,
    edits: DocumentEdits,
    cache_root: Option<PathBuf>,
    disk_cache_dir: Option<PathBuf>,
    disk_cache_key: Option<CacheKey>,
    revision: u64,
}

impl CsvDocument {
    pub fn open(
        path: impl AsRef<Path>,
        overrides: Option<ParseOverrides>,
        progress: Option<&dyn Fn(usize)>,
    ) -> Result<Self, String> {
        Self::open_with_cancellation(path.as_ref(), overrides, progress, None, None)
    }

    pub fn open_cached(
        path: impl AsRef<Path>,
        overrides: Option<ParseOverrides>,
        progress: Option<&dyn Fn(usize)>,
        cache_root: impl AsRef<Path>,
    ) -> Result<Self, String> {
        Self::open_with_cancellation(
            path.as_ref(),
            overrides,
            progress,
            None,
            Some(cache_root.as_ref()),
        )
    }

    pub fn open_cancellable(
        path: impl AsRef<Path>,
        overrides: Option<ParseOverrides>,
        progress: Option<&dyn Fn(usize)>,
        cancellation: &CancellationToken,
    ) -> Result<Self, String> {
        Self::open_with_cancellation(path.as_ref(), overrides, progress, Some(cancellation), None)
    }

    pub fn open_cancellable_cached(
        path: impl AsRef<Path>,
        overrides: Option<ParseOverrides>,
        progress: Option<&dyn Fn(usize)>,
        cancellation: &CancellationToken,
        cache_root: impl AsRef<Path>,
    ) -> Result<Self, String> {
        Self::open_with_cancellation(
            path.as_ref(),
            overrides,
            progress,
            Some(cancellation),
            Some(cache_root.as_ref()),
        )
    }

    fn open_with_cancellation(
        path: &Path,
        overrides: Option<ParseOverrides>,
        progress: Option<&dyn Fn(usize)>,
        cancellation: Option<&CancellationToken>,
        cache_root: Option<&Path>,
    ) -> Result<Self, String> {
        cancellation.map(CancellationToken::check).transpose()?;
        let path = path.to_path_buf();
        let path_string = path
            .to_str()
            .ok_or_else(|| "CSV paths must be valid UTF-8".to_string())?
            .to_owned();
        let source_fingerprint = file_fingerprint(&path)?;
        cancellation.map(CancellationToken::check).transpose()?;
        let mut detected = detect_parse_settings(&path_string).map_err(|e| e.to_string())?;
        if let Some(overrides) = overrides.as_ref() {
            validate_parse_overrides(overrides)?;
            if let Some(encoding) = overrides.encoding.as_deref() {
                detected = detect_parse_settings_for_encoding(&path_string, Some(encoding))
                    .map_err(|error| error.to_string())?;
            }
        }
        let detected_settings = apply_parse_overrides(&detected, None);
        let explicit_headers = overrides.as_ref().and_then(|value| value.has_headers);
        let mut settings = apply_parse_overrides(&detected, overrides);
        validate_parse_settings(&settings)?;
        let mut prepared = prepare_csv_source_cancellable(&path, &settings, progress, &|| {
            cancellation.is_some_and(CancellationToken::is_cancelled)
        })?;
        if prepared.temporary.is_none() {
            let temporary = snapshot_csv_source(&path, source_fingerprint, cancellation)?;
            prepared.path = temporary.path().to_path_buf();
            prepared.temporary = Some(Arc::new(temporary));
        }
        let data_path = prepared.path.clone();
        let data_path_string = data_path
            .to_str()
            .ok_or_else(|| "CSV paths must be valid UTF-8".to_string())?
            .to_owned();
        let mut storage_settings = prepared.settings.clone();
        if explicit_headers.is_none() {
            let has_headers = detect_headers_for_settings(&data_path_string, &storage_settings)
                .map_err(|error| error.to_string())?;
            settings.has_headers = has_headers;
            storage_settings.has_headers = has_headers;
        }
        let mut warnings = prepared.warnings.clone();
        let headers = get_headers(&data_path_string, &storage_settings, &mut warnings)
            .map_err(|e| e.to_string())?;
        let expected_columns = (!headers.is_empty()).then_some(headers.len());
        let header_warning_count = warnings.len();
        let mmap = if prepared.temporary.is_some() {
            open_immutable_mmap_if_large(&data_path_string).map_err(|e| e.to_string())?
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
                        &path_string,
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
        let offsets = if let Some(offsets) = cached_offsets {
            if let Some(cached_warnings) = cached_warnings {
                warnings.extend(cached_warnings);
            }
            if let Some(progress) = progress {
                progress(offsets.len());
            }
            offsets
        } else {
            let offsets = match (mmap.as_deref(), cancellation) {
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
                    &data_path_string,
                    &storage_settings,
                    expected_columns,
                    &mut warnings,
                    progress,
                    &|| cancellation.is_cancelled(),
                ),
                (None, None) => build_row_offsets(
                    &data_path_string,
                    &storage_settings,
                    expected_columns,
                    &mut warnings,
                    progress,
                ),
            }
            .map_err(|e| e.to_string())?;
            offsets
        };
        cancellation.map(CancellationToken::check).transpose()?;
        if file_fingerprint(&path)? != source_fingerprint {
            return Err("CSV changed on disk while it was being opened".to_string());
        }
        if let (Some(dir), Some(key)) = (&disk_cache_dir, disk_cache_key) {
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
            revision: 0,
        })
    }

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

    fn read_source_rows(&self, indices: &[usize]) -> Result<Vec<Vec<String>>, String> {
        let mut warnings = Vec::new();
        let path = self
            .data_path
            .to_str()
            .ok_or_else(|| "CSV paths must be valid UTF-8".to_string())?;
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
                &path,
                &self.offsets,
                indices,
                &self.storage_settings,
                self.expected_columns(),
                &mut warnings,
            ),
        }
        .map_err(|e| e.to_string())
    }

    pub fn display_rows(
        &self,
        start: usize,
        count: usize,
    ) -> Result<Vec<(usize, Vec<String>)>, String> {
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
                let path = self
                    .data_path
                    .to_str()
                    .ok_or_else(|| "CSV paths must be valid UTF-8".to_string())?;
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
                        &path,
                        &self.offsets,
                        start,
                        source_indices.len(),
                        &self.storage_settings,
                        self.expected_columns(),
                        &mut warnings,
                    ),
                }
                .map_err(|e| e.to_string())?;
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
    ) -> Result<(), String> {
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
    ) -> Result<(), String> {
        if source_row >= self.row_count() {
            return Err("Row is out of range".to_string());
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

    pub fn delete_display_row(&mut self, display_row: usize) -> Result<(), String> {
        let row = self
            .source_row_for_display(display_row)
            .ok_or_else(|| "Row is out of range".to_string())?;
        self.edits.delete_row(row);
        self.revision = self.revision.wrapping_add(1);
        self.search_index = None;
        self.indexed_search_column = None;
        Ok(())
    }

    pub fn restore_display_row(&mut self, display_row: usize) -> Result<(), String> {
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
    ) -> Result<usize, String> {
        self.set_display_rows_deleted_with_cancellation(display_rows, deleted, None)
    }

    pub fn set_display_rows_deleted_cancellable(
        &mut self,
        display_rows: &[usize],
        deleted: bool,
        cancellation: &CancellationToken,
    ) -> Result<usize, String> {
        self.set_display_rows_deleted_with_cancellation(display_rows, deleted, Some(cancellation))
    }

    fn set_display_rows_deleted_with_cancellation(
        &mut self,
        display_rows: &[usize],
        deleted: bool,
        cancellation: Option<&CancellationToken>,
    ) -> Result<usize, String> {
        cancellation.map(CancellationToken::check).transpose()?;
        let mut rows = Vec::with_capacity(display_rows.len());
        for &display_row in display_rows {
            cancellation.map(CancellationToken::check).transpose()?;
            let source_row = self
                .source_row_for_display(display_row)
                .ok_or_else(|| format!("Display row {display_row} is out of range"))?;
            rows.push((source_row, self.edits.is_deleted(source_row)));
        }

        let mut applied = 0;
        for &(source_row, _) in &rows {
            if let Err(error) = cancellation.map(CancellationToken::check).transpose() {
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
            applied += 1;
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

    pub fn prepare_search_index_build(&self) -> SearchIndexBuild {
        SearchIndexBuild {
            path: self.data_path.clone(),
            settings: self.storage_settings.clone(),
            offsets: self.offsets.clone(),
            mmap: self.mmap.clone(),
            _prepared_source: self._prepared_source.clone(),
            column_count: self.metadata.headers.len(),
            edits: self.edits.clone(),
            revision: self.revision,
        }
    }

    pub fn install_search_index(&mut self, index: BuiltSearchIndex) -> bool {
        if index.revision != self.revision {
            return false;
        }
        self.search_index = Some(index.columns);
        self.indexed_search_column = None;
        true
    }

    pub fn build_search_index(&mut self) -> Result<(), String> {
        self.build_search_index_with_cancellation(None)
    }

    pub fn build_search_index_cancellable(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<(), String> {
        self.build_search_index_with_cancellation(Some(cancellation))
    }

    fn build_search_index_with_cancellation(
        &mut self,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(), String> {
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
        for index in columns.iter_mut().flatten() {
            compact_index(index);
        }
        self.search_index = Some(columns);
        self.indexed_search_column = None;
        Ok(())
    }

    pub fn ensure_search_index_for_column_cancellable(
        &mut self,
        column: usize,
        cancellation: &CancellationToken,
        progress: Option<&dyn Fn(usize, usize)>,
    ) -> Result<(), String> {
        if column >= self.metadata.headers.len() {
            return Err("Search column is out of range".to_string());
        }
        let already_ready = self
            .search_index
            .as_ref()
            .and_then(|columns| columns.get(column))
            .is_some_and(Option::is_some);
        if already_ready || self.indexed_search_column == Some(column) {
            return Ok(());
        }

        // Keep at most one lazily-built column. This makes the setting useful
        // for repeated searches without multiplying memory by the CSV width.
        self.search_index = None;
        self.indexed_search_column = None;
        let mut index = HashMap::new();
        let mut exceeded_cardinality = false;
        for start in (0..self.row_count()).step_by(INDEX_CHUNK_SIZE) {
            cancellation.check()?;
            let end = (start + INDEX_CHUNK_SIZE).min(self.row_count());
            let indices = (start..end).collect::<Vec<_>>();
            for (source_row, mut row) in (start..end).zip(self.read_source_rows(&indices)?) {
                if self.edits.is_deleted(source_row) {
                    continue;
                }
                self.edits.apply(source_row, &mut row);
                if let Some(value) = row.into_iter().nth(column) {
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
            if exceeded_cardinality {
                break;
            }
        }

        let mut columns = (0..self.metadata.headers.len())
            .map(|_| None)
            .collect::<Vec<Option<ColumnSearchIndex>>>();
        if !exceeded_cardinality {
            compact_index(&mut index);
            columns[column] = Some(index);
        }
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
    ) -> Result<Vec<usize>, String> {
        self.search_with_cancellation(query, column, match_case, whole_word, None, None)
    }

    pub fn search_cancellable(
        &self,
        query: &str,
        column: Option<usize>,
        match_case: bool,
        whole_word: bool,
        cancellation: &CancellationToken,
    ) -> Result<Vec<usize>, String> {
        self.search_with_cancellation(
            query,
            column,
            match_case,
            whole_word,
            Some(cancellation),
            None,
        )
    }

    pub fn search_cancellable_streaming(
        &self,
        query: &str,
        column: Option<usize>,
        match_case: bool,
        whole_word: bool,
        cancellation: &CancellationToken,
        progress: &dyn Fn(&[usize], usize, usize),
    ) -> Result<Vec<usize>, String> {
        self.search_with_cancellation(
            query,
            column,
            match_case,
            whole_word,
            Some(cancellation),
            Some(progress),
        )
    }

    fn search_with_cancellation(
        &self,
        query: &str,
        column: Option<usize>,
        match_case: bool,
        whole_word: bool,
        cancellation: Option<&CancellationToken>,
        progress: Option<&dyn Fn(&[usize], usize, usize)>,
    ) -> Result<Vec<usize>, String> {
        cancellation.map(CancellationToken::check).transpose()?;
        if query.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(column) = column {
            if column >= self.metadata.headers.len() {
                return Err("Search column is out of range".to_string());
            }
        }
        if !match_case {
            if let (Some(column), Some(columns)) = (column, self.search_index.as_ref()) {
                if let Some(Some(index)) = columns.get(column) {
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
                        cancellation.map(CancellationToken::check).transpose()?;
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
            }
        }

        if !self.edits.is_dirty() {
            let path = self
                .data_path
                .to_str()
                .ok_or_else(|| "CSV paths must be valid UTF-8".to_string())?;
            let mut matches = Vec::new();
            for start in (0..self.row_count()).step_by(INDEX_CHUNK_SIZE) {
                cancellation.map(CancellationToken::check).transpose()?;
                let end = (start + INDEX_CHUNK_SIZE).min(self.row_count());
                let chunk_matches = match self.mmap.as_deref() {
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
                        path,
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
                .map_err(|error| error.to_string())?;
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
            cancellation.map(CancellationToken::check).transpose()?;
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

    pub fn find_duplicates(&self, column: Option<usize>) -> Result<Vec<usize>, String> {
        self.find_duplicates_with_cancellation(column, &CancellationToken::new(), None)
    }

    pub fn find_duplicates_cancellable(
        &self,
        column: Option<usize>,
        cancellation: &CancellationToken,
    ) -> Result<Vec<usize>, String> {
        self.find_duplicates_with_cancellation(column, cancellation, None)
    }

    pub fn find_duplicates_cancellable_streaming(
        &self,
        column: Option<usize>,
        cancellation: &CancellationToken,
        progress: &dyn Fn(&[usize], usize, usize),
    ) -> Result<Vec<usize>, String> {
        self.find_duplicates_with_cancellation(column, cancellation, Some(progress))
    }

    fn find_duplicates_with_cancellation(
        &self,
        column: Option<usize>,
        cancellation: &CancellationToken,
        progress: Option<&dyn Fn(&[usize], usize, usize)>,
    ) -> Result<Vec<usize>, String> {
        cancellation.check()?;
        if column.is_some_and(|column| column >= self.metadata.headers.len()) {
            return Err("Duplicate column is out of range".to_string());
        }
        let mut hashes = Vec::with_capacity(self.row_count());
        for start in (0..self.row_count()).step_by(INDEX_CHUNK_SIZE) {
            cancellation.check()?;
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
        cancellation.check()?;

        let mut duplicates = Vec::new();
        let mut start = 0;
        while start < hashes.len() {
            cancellation.check()?;
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

    pub fn sort(&mut self, spec: Option<SortSpec>) -> Result<(), String> {
        self.sort_with_cancellation(spec, None, None)
    }

    pub fn sort_cancellable(
        &mut self,
        spec: Option<SortSpec>,
        cancellation: &CancellationToken,
    ) -> Result<(), String> {
        self.sort_with_cancellation(spec, Some(cancellation), None)
    }

    pub fn sort_cancellable_with_progress(
        &mut self,
        spec: Option<SortSpec>,
        cancellation: &CancellationToken,
        progress: &dyn Fn(usize, usize),
    ) -> Result<(), String> {
        self.sort_with_cancellation(spec, Some(cancellation), Some(progress))
    }

    fn sort_with_cancellation(
        &mut self,
        spec: Option<SortSpec>,
        cancellation: Option<&CancellationToken>,
        progress: Option<&dyn Fn(usize, usize)>,
    ) -> Result<(), String> {
        cancellation.map(CancellationToken::check).transpose()?;
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
            return Err("Sort column is out of range".to_string());
        }
        let ascending = spec.direction == SortDirection::Ascending;
        if !self.edits.is_dirty() {
            if let (Some(dir), Some(key)) = (&self.disk_cache_dir, self.disk_cache_key) {
                let path = order_cache_path(dir, key, spec.column, ascending);
                if let Some(order) = read_order_cache(&path, key, spec.column, ascending)
                    .ok()
                    .flatten()
                    .filter(|order| cached_order_is_valid(order, self.row_count()))
                {
                    self.install_sorted_order(order, spec);
                    if let Some(progress) = progress {
                        progress(self.row_count(), self.row_count());
                    }
                    return Ok(());
                }
            }
        }

        let mut values: Vec<(usize, String)> = Vec::with_capacity(self.row_count());
        let mut start = 0;
        while start < self.row_count() {
            cancellation.map(CancellationToken::check).transpose()?;
            let rows = self.display_rows(start, SORT_CHUNK_SIZE)?;
            if rows.is_empty() {
                break;
            }
            for (source_row, row) in rows {
                let value = row.get(spec.column).map(String::as_str).unwrap_or_default();
                values.push((source_row, value.to_string()));
            }
            start += SORT_CHUNK_SIZE;
            if let Some(progress) = progress {
                progress(start.min(self.row_count()), self.row_count());
            }
        }
        match spec.direction {
            SortDirection::Ascending => {
                values.par_sort_unstable_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)))
            }
            SortDirection::Descending => {
                values.par_sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)))
            }
        }
        cancellation.map(CancellationToken::check).transpose()?;
        let order = values.into_iter().map(|(row, _)| row).collect::<Vec<_>>();
        if !self.edits.is_dirty() {
            if let (Some(dir), Some(key)) = (&self.disk_cache_dir, self.disk_cache_key) {
                let path = order_cache_path(dir, key, spec.column, ascending);
                let _ = write_order_cache(&path, key, spec.column, ascending, &order);
            }
        }
        self.install_sorted_order(order, spec);
        Ok(())
    }

    pub fn serialize_display_rows(&self, display_rows: &[usize]) -> Result<String, String> {
        self.serialize_display_rows_with_cancellation(display_rows, None, None)
    }

    pub fn serialize_display_rows_cancellable(
        &self,
        display_rows: &[usize],
        cancellation: &CancellationToken,
    ) -> Result<String, String> {
        self.serialize_display_rows_with_cancellation(display_rows, Some(cancellation), None)
    }

    pub fn serialize_display_rows_cancellable_with_progress(
        &self,
        display_rows: &[usize],
        cancellation: &CancellationToken,
        progress: &dyn Fn(usize, usize),
    ) -> Result<String, String> {
        self.serialize_display_rows_with_cancellation(
            display_rows,
            Some(cancellation),
            Some(progress),
        )
    }

    fn serialize_display_rows_with_cancellation(
        &self,
        display_rows: &[usize],
        cancellation: Option<&CancellationToken>,
        progress: Option<&dyn Fn(usize, usize)>,
    ) -> Result<String, String> {
        cancellation.map(CancellationToken::check).transpose()?;
        let mut output = String::new();
        let mut wrote_record = false;
        let mut processed = 0;
        for display_chunk in display_rows.chunks(INDEX_CHUNK_SIZE) {
            cancellation.map(CancellationToken::check).transpose()?;
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
                cancellation.map(CancellationToken::check).transpose()?;
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
    ) -> Result<String, String> {
        cancellation.check()?;
        if row_start > row_end || row_end >= self.row_count() {
            return Err("Cell selection row range is out of bounds".to_string());
        }
        if column_start > column_end || column_end >= self.metadata.headers.len() {
            return Err("Cell selection column range is out of bounds".to_string());
        }
        let mut output = String::new();
        let mut wrote_record = false;
        let total = row_end - row_start + 1;
        let mut processed = 0;
        let mut chunk_start = row_start;
        while chunk_start <= row_end {
            cancellation.check()?;
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
                cancellation.check()?;
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

    pub fn save(&mut self, path: impl AsRef<Path>) -> Result<(), String> {
        self.save_with_cancellation(path.as_ref(), None, None, false)
    }

    pub fn save_cancellable(
        &mut self,
        path: impl AsRef<Path>,
        cancellation: &CancellationToken,
    ) -> Result<(), String> {
        self.save_with_cancellation(path.as_ref(), Some(cancellation), None, false)
    }

    pub fn save_cancellable_with_progress(
        &mut self,
        path: impl AsRef<Path>,
        cancellation: &CancellationToken,
        progress: &dyn Fn(usize, usize),
    ) -> Result<(), String> {
        self.save_with_cancellation(path.as_ref(), Some(cancellation), Some(progress), false)
    }

    pub fn save_cancellable_with_progress_overwrite_external(
        &mut self,
        path: impl AsRef<Path>,
        cancellation: &CancellationToken,
        progress: &dyn Fn(usize, usize),
    ) -> Result<(), String> {
        self.save_with_cancellation(path.as_ref(), Some(cancellation), Some(progress), true)
    }

    fn save_with_cancellation(
        &mut self,
        target: &Path,
        cancellation: Option<&CancellationToken>,
        progress: Option<&dyn Fn(usize, usize)>,
        overwrite_external_changes: bool,
    ) -> Result<(), String> {
        cancellation.map(CancellationToken::check).transpose()?;
        let commit_target = resolve_save_target(target)?;
        let expected_destination = if target == self.path && !overwrite_external_changes {
            let expected = DestinationState::Existing(self.source_fingerprint);
            ensure_destination_unchanged(&commit_target, expected)?;
            expected
        } else {
            destination_state(&commit_target)?
        };
        let cache_root = self.cache_root.clone();
        let effective = self.metadata.effective.clone();
        let original_malformed = self.settings.malformed;
        let original_malformed_label = effective.malformed.clone();
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
        let target_string = target
            .to_str()
            .ok_or_else(|| "CSV paths must be valid UTF-8".to_string())?
            .to_owned();
        let parent = commit_target.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        let mut temporary = tempfile::Builder::new()
            .prefix(".quickrows-")
            .suffix(".tmp")
            .tempfile_in(parent)
            .map_err(|e| e.to_string())?;
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
            temporary
                .as_file_mut()
                .write_all(bom)
                .map_err(|e| e.to_string())?;
        }
        let mut record_settings = self.settings.clone();
        record_settings.source_bom = false;
        let terminator = line_ending(&self.settings);
        if self.settings.excel_sep {
            let directive = format!("sep={}{}", self.settings.delimiter, terminator);
            let encoded = encode_csv_text(&directive, &record_settings)?;
            temporary
                .as_file_mut()
                .write_all(&encoded)
                .map_err(|error| error.to_string())?;
        }
        let write_record = |output: &mut dyn Write, fields: &[String]| -> Result<(), String> {
            let mut record = String::new();
            push_csv_record(&mut record, fields, &self.settings);
            record.push_str(terminator);
            let encoded = encode_csv_text(&record, &record_settings)?;
            output
                .write_all(&encoded)
                .map_err(|error| error.to_string())
        };
        let mut comments = self.comments.iter().peekable();
        let mut write_comments =
            |output: &mut dyn Write, before_record: usize| -> Result<(), String> {
                while comments
                    .peek()
                    .is_some_and(|comment| comment.before_record <= before_record)
                {
                    let comment = comments.next().expect("peeked comment must exist");
                    let text = format!("{}{}", comment.text, terminator);
                    let encoded = encode_csv_text(&text, &record_settings)?;
                    output
                        .write_all(&encoded)
                        .map_err(|error| error.to_string())?;
                }
                Ok(())
            };

        write_comments(temporary.as_file_mut(), 0)?;
        if self.settings.has_headers && !self.metadata.headers.is_empty() {
            write_record(temporary.as_file_mut(), &self.metadata.headers)?;
        }
        for start in (0..self.row_count()).step_by(INDEX_CHUNK_SIZE) {
            cancellation.map(CancellationToken::check).transpose()?;
            let end = start.saturating_add(INDEX_CHUNK_SIZE).min(self.row_count());
            let source_rows = (start..end).collect::<Vec<_>>();
            for (source_row, mut row) in source_rows
                .iter()
                .copied()
                .zip(self.read_source_rows(&source_rows)?)
            {
                cancellation.map(CancellationToken::check).transpose()?;
                let source_record = source_row + usize::from(self.settings.has_headers);
                write_comments(temporary.as_file_mut(), source_record)?;
                if self.edits.is_deleted(source_row) {
                    continue;
                }
                self.edits.apply(source_row, &mut row);
                write_record(temporary.as_file_mut(), &row)?;
            }
            if let Some(progress) = progress {
                progress(end, self.row_count());
            }
        }
        write_comments(temporary.as_file_mut(), usize::MAX)?;
        temporary.as_file().sync_all().map_err(|e| e.to_string())?;
        cancellation.map(CancellationToken::check).transpose()?;

        // Parse the exact bytes to be committed before replacing the user's
        // file. This keeps the destination and the current document intact if
        // an edit exceeds active parse limits or otherwise cannot be reopened.
        let temporary_path = temporary.path().to_path_buf();
        let expected_saved_rows = self
            .row_count()
            .saturating_sub(self.edits.deleted_rows.len());
        let mut saved_document = CsvDocument::open_with_cancellation(
            &temporary_path,
            Some(validation_overrides),
            None,
            cancellation,
            None,
        )?;
        if saved_document.row_count() != expected_saved_rows
            || (self.settings.has_headers
                && saved_document.metadata.headers != self.metadata.headers)
            || !saved_document.metadata.warnings.is_empty()
        {
            return Err("Saved CSV did not round-trip under the active parse settings".to_string());
        }
        saved_document.settings.malformed = original_malformed;
        saved_document.storage_settings.malformed = original_malformed;
        saved_document.metadata.effective.malformed = original_malformed_label;
        cancellation.map(CancellationToken::check).transpose()?;
        copy_destination_permissions(&commit_target, &temporary_path)?;
        temporary.as_file().sync_all().map_err(|e| e.to_string())?;
        saved_document.rebind_saved_path(
            &temporary_path,
            target,
            &target_string,
            cache_root.as_deref(),
        );

        // Re-check both the content and symlink route immediately before the
        // atomic replacement. Missing destinations use a no-clobber rename.
        commit_temporary(
            temporary,
            target,
            &commit_target,
            expected_destination,
            saved_document.source_fingerprint,
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
        target_string: &str,
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
                        target_string,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DestinationState {
    Missing,
    Existing(FileFingerprint),
}

fn resolve_save_target(path: &Path) -> Result<PathBuf, String> {
    fn resolve(path: &Path, remaining_links: usize) -> Result<PathBuf, String> {
        if remaining_links == 0 {
            return Err("CSV destination contains too many symbolic links".to_string());
        }
        if let Ok(canonical) = std::fs::canonicalize(path) {
            return Ok(canonical);
        }
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let link = std::fs::read_link(path).map_err(|error| error.to_string())?;
                let linked_path = if link.is_absolute() {
                    link
                } else {
                    path.parent()
                        .filter(|parent| !parent.as_os_str().is_empty())
                        .unwrap_or_else(|| Path::new("."))
                        .join(link)
                };
                resolve(&linked_path, remaining_links - 1)
            }
            Ok(_) => std::fs::canonicalize(path).map_err(|error| error.to_string()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let file_name = path
                    .file_name()
                    .ok_or_else(|| "CSV destination has no file name".to_string())?;
                let parent = path
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                Ok(resolve(parent, remaining_links - 1)?.join(file_name))
            }
            Err(error) => Err(error.to_string()),
        }
    }

    resolve(path, 40)
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
fn exchange_paths(first: &Path, second: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    #[cfg(target_vendor = "apple")]
    use std::os::raw::{c_char, c_int, c_uint};
    use std::os::unix::ffi::OsStrExt;

    let first = CString::new(first.as_os_str().as_bytes())?;
    let second = CString::new(second.as_os_str().as_bytes())?;
    #[cfg(target_os = "linux")]
    unsafe {
        if libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            first.as_ptr(),
            libc::AT_FDCWD,
            second.as_ptr(),
            libc::RENAME_EXCHANGE,
        ) == 0
        {
            return Ok(());
        }
    }
    #[cfg(target_vendor = "apple")]
    unsafe {
        unsafe extern "C" {
            fn renamex_np(old: *const c_char, new: *const c_char, flags: c_uint) -> c_int;
        }
        const RENAME_SWAP: c_uint = 2;
        if renamex_np(first.as_ptr(), second.as_ptr(), RENAME_SWAP) == 0 {
            return Ok(());
        }
    }
    let error = std::io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(22) | Some(38) | Some(45) | Some(95)
    ) {
        Err(std::io::Error::new(std::io::ErrorKind::Unsupported, error))
    } else {
        Err(error)
    }
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn exchange_paths(_first: &Path, _second: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic file exchange is unavailable on this platform",
    ))
}

fn commit_temporary(
    temporary: tempfile::NamedTempFile,
    logical_target: &Path,
    resolved_target: &Path,
    expected: DestinationState,
    replacement_fingerprint: FileFingerprint,
) -> Result<(), String> {
    let conflict =
        || "CSV changed on disk; save was cancelled to protect the external changes".to_string();
    let route_matches =
        || resolve_save_target(logical_target).is_ok_and(|current| current == resolved_target);
    if !route_matches() {
        return Err(conflict());
    }
    if expected == DestinationState::Missing {
        return temporary
            .persist_noclobber(resolved_target)
            .map(|_| ())
            .map_err(|error| {
                if error.error.kind() == std::io::ErrorKind::AlreadyExists {
                    conflict()
                } else {
                    error.error.to_string()
                }
            });
    }

    ensure_destination_unchanged(resolved_target, expected)?;
    if !route_matches() {
        return Err(conflict());
    }

    match exchange_paths(temporary.path(), resolved_target) {
        Ok(()) => {
            let displaced_matches = destination_state(temporary.path()) == Ok(expected);
            if displaced_matches && route_matches() {
                // `temporary.path()` now names the old destination. Dropping it
                // removes that displaced file while the new target stays live.
                drop(temporary);
                return Ok(());
            }
            if destination_state(resolved_target)
                != Ok(DestinationState::Existing(replacement_fingerprint))
            {
                let recovery = temporary
                    .into_temp_path()
                    .keep()
                    .map_err(|error| error.error.to_string())?;
                return Err(format!(
                    "CSV changed during save; the displaced destination is at {}",
                    recovery.display()
                ));
            }
            if let Err(rollback_error) = exchange_paths(temporary.path(), resolved_target) {
                let recovery = temporary
                    .into_temp_path()
                    .keep()
                    .map_err(|error| error.error.to_string())?;
                return Err(format!(
                    "CSV changed during save and rollback failed ({rollback_error}); the displaced destination is at {}",
                    recovery.display()
                ));
            }
            // Never unlink the exchanged path after a conflict: a writer could
            // have replaced the target immediately before the rollback, in
            // which case its file now occupies this unpredictable temp path.
            let recovery = temporary
                .into_temp_path()
                .keep()
                .map_err(|error| error.error.to_string())?;
            Err(format!(
                "{}; the uncommitted file was preserved at {}",
                conflict(),
                recovery.display()
            ))
        }
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
            // Portable fallback: preserve atomic replacement and perform the
            // strongest available content and route checks immediately before it.
            ensure_destination_unchanged(resolved_target, expected)?;
            if !route_matches() {
                return Err(conflict());
            }
            temporary
                .persist(resolved_target)
                .map(|_| ())
                .map_err(|error| error.error.to_string())
        }
        Err(error) => Err(error.to_string()),
    }
}

fn destination_state(path: &Path) -> Result<DestinationState, String> {
    match path.try_exists().map_err(|error| error.to_string())? {
        true => file_fingerprint(path)
            .map(DestinationState::Existing)
            .map_err(|_| "CSV destination changed while preparing to save".to_string()),
        false => Ok(DestinationState::Missing),
    }
}

fn ensure_destination_unchanged(path: &Path, expected: DestinationState) -> Result<(), String> {
    let conflict =
        || "CSV changed on disk; save was cancelled to protect the external changes".to_string();
    match destination_state(path) {
        Ok(current) if current == expected => Ok(()),
        Ok(_) | Err(_) => Err(conflict()),
    }
}

fn copy_destination_permissions(target: &Path, temporary: &Path) -> Result<(), String> {
    match std::fs::metadata(target) {
        Ok(metadata) => std::fs::set_permissions(temporary, metadata.permissions())
            .map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn file_metadata_matches(path: &Path, expected: FileFingerprint) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    metadata.len() == expected.len && modified == expected.modified
}

fn snapshot_csv_source(
    path: &Path,
    expected: FileFingerprint,
    cancellation: Option<&CancellationToken>,
) -> Result<tempfile::NamedTempFile, String> {
    cancellation.map(CancellationToken::check).transpose()?;
    let temporary = tempfile::Builder::new()
        .prefix("quickrows-source-")
        .suffix(".csv")
        .tempfile()
        .map_err(|error| error.to_string())?;
    let (placeholder, temporary_path) = temporary.into_parts();
    drop(placeholder);
    std::fs::remove_file(&temporary_path).map_err(|error| error.to_string())?;

    let snapshot_matches = if reflink_copy::reflink(path, &temporary_path).is_ok() {
        file_fingerprint(&temporary_path).is_ok_and(|fingerprint| {
            fingerprint.len == expected.len && fingerprint.content_hash == expected.content_hash
        })
    } else {
        let _ = std::fs::remove_file(&temporary_path);
        let mut source = std::fs::File::open(path).map_err(|error| error.to_string())?;
        let mut destination = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(|error| error.to_string())?;
        let mut hasher = blake3::Hasher::new();
        let mut copied = 0u64;
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            cancellation.map(CancellationToken::check).transpose()?;
            let read = source
                .read(&mut buffer)
                .map_err(|error| error.to_string())?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            copied = copied.saturating_add(read as u64);
            destination
                .write_all(&buffer[..read])
                .map_err(|error| error.to_string())?;
        }
        destination.flush().map_err(|error| error.to_string())?;
        copied == expected.len && hasher.finalize().as_bytes() == &expected.content_hash
    };
    cancellation.map(CancellationToken::check).transpose()?;

    if !snapshot_matches || !file_metadata_matches(path, expected) {
        return Err("CSV changed on disk while it was being opened".to_string());
    }
    let file = std::fs::File::open(&temporary_path).map_err(|error| error.to_string())?;
    Ok(tempfile::NamedTempFile::from_parts(file, temporary_path))
}

fn cached_offsets_are_valid(offsets: &[u64], file_len: u64) -> bool {
    offsets.iter().all(|offset| *offset < file_len)
        && offsets.windows(2).all(|pair| pair[0] < pair[1])
}

fn cached_order_is_valid(order: &[usize], row_count: usize) -> bool {
    if order.len() != row_count {
        return false;
    }
    let mut seen = vec![false; row_count];
    for &row in order {
        let Some(slot) = seen.get_mut(row) else {
            return false;
        };
        if *slot {
            return false;
        }
        *slot = true;
    }
    true
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), String> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|e| e.to_string())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document(contents: &str) -> (tempfile::TempDir, CsvDocument) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.csv");
        std::fs::write(&path, contents).unwrap();
        let doc = CsvDocument::open(path, None, None).unwrap();
        (dir, doc)
    }

    fn utf16_bytes(text: &str, little_endian: bool, bom: bool) -> Vec<u8> {
        let mut bytes = if bom {
            if little_endian {
                vec![0xff, 0xfe]
            } else {
                vec![0xfe, 0xff]
            }
        } else {
            Vec::new()
        };
        for unit in text.encode_utf16() {
            let encoded = if little_endian {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            };
            bytes.extend_from_slice(&encoded);
        }
        bytes
    }

    #[test]
    fn cancellable_operations_stop_before_work_starts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cancel.csv");
        std::fs::write(&path, "name,value\na,1\n").unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(
            CsvDocument::open_cancellable(&path, None, None, &cancellation)
                .err()
                .unwrap()
                .contains("cancelled")
        );

        let active_cancellation = CancellationToken::new();
        let cancel_from_progress = active_cancellation.clone();
        let cancel_at_first_row = move |_| cancel_from_progress.cancel();
        assert!(CsvDocument::open_cancellable(
            &path,
            None,
            Some(&cancel_at_first_row),
            &active_cancellation,
        )
        .err()
        .unwrap()
        .contains("cancelled"));

        let (doc_dir, mut doc) = document("name,value\na,1\n");
        assert!(doc
            .search_cancellable("a", Some(0), false, false, &cancellation)
            .unwrap_err()
            .contains("cancelled"));
        assert!(doc
            .build_search_index_cancellable(&cancellation)
            .unwrap_err()
            .contains("cancelled"));
        assert!(doc
            .find_duplicates_cancellable(None, &cancellation)
            .unwrap_err()
            .contains("cancelled"));
        assert!(doc
            .sort_cancellable(
                Some(SortSpec {
                    column: 0,
                    direction: SortDirection::Ascending,
                }),
                &cancellation,
            )
            .unwrap_err()
            .contains("cancelled"));
        assert!(doc
            .serialize_display_rows_cancellable(&[0], &cancellation)
            .unwrap_err()
            .contains("cancelled"));
        let output = doc_dir.path().join("cancelled-save.csv");
        assert!(doc
            .save_cancellable(&output, &cancellation)
            .unwrap_err()
            .contains("cancelled"));
        assert!(!output.exists());
    }

    #[test]
    fn opens_utf16le_with_bom() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("utf16.csv");
        let mut bytes = vec![0xff, 0xfe];
        for unit in "name,value\r\ncafé,2\r\n".encode_utf16() {
            bytes.extend_from_slice(&unit.to_le_bytes());
        }
        std::fs::write(&path, bytes).unwrap();
        let doc = CsvDocument::open(path, None, None).unwrap();
        assert_eq!(doc.metadata().effective.encoding, "UTF-16LE");
        assert_eq!(doc.metadata().headers, vec!["name", "value"]);
        assert_eq!(
            doc.row_count(),
            1,
            "prepared source: {:?}",
            std::fs::read_to_string(&doc.data_path)
        );
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["café", "2"]);
    }

    #[test]
    fn bomless_utf16_uses_explicit_endianness_for_dialect_detection() {
        for (encoding, little_endian) in [("utf-16le", true), ("utf-16be", false)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("bomless-{encoding}.csv"));
            let mut bytes = Vec::new();
            for unit in "name;value\r\nalpha;1\r\n".encode_utf16() {
                let encoded = if little_endian {
                    unit.to_le_bytes()
                } else {
                    unit.to_be_bytes()
                };
                bytes.extend_from_slice(&encoded);
            }
            std::fs::write(&path, bytes).unwrap();
            let overrides = ParseOverrides {
                encoding: Some(encoding.to_string()),
                ..Default::default()
            };
            let doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
            assert_eq!(doc.metadata().effective.delimiter, ";");
            assert_eq!(doc.metadata().effective.line_ending, "crlf");
            assert_eq!(doc.metadata().headers, vec!["name", "value"]);
            assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "1"]);
        }
    }

    #[test]
    fn utf16le_and_utf16be_round_trip_through_save() {
        for (label, bom, little_endian) in [
            ("utf-16le", [0xff, 0xfe], true),
            ("utf-16be", [0xfe, 0xff], false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("{label}.csv"));
            let mut bytes = bom.to_vec();
            for unit in "name,note\r\ncafé,\"line 1\r\nline 2\"\r\n".encode_utf16() {
                let encoded = if little_endian {
                    unit.to_le_bytes()
                } else {
                    unit.to_be_bytes()
                };
                bytes.extend_from_slice(&encoded);
            }
            std::fs::write(&path, bytes).unwrap();

            let mut doc = CsvDocument::open(
                &path,
                Some(ParseOverrides {
                    has_headers: Some(true),
                    ..Default::default()
                }),
                None,
            )
            .unwrap();
            assert_eq!(doc.row_count(), 1);
            assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "café");
            assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[1], "line 1\r\nline 2");
            doc.edit_cell(0, 0, "thé".to_string()).unwrap();
            doc.save(&path).unwrap();

            let saved = std::fs::read(&path).unwrap();
            assert_eq!(&saved[..2], &bom);
            let units = saved[2..]
                .chunks_exact(2)
                .map(|pair| {
                    if little_endian {
                        u16::from_le_bytes([pair[0], pair[1]])
                    } else {
                        u16::from_be_bytes([pair[0], pair[1]])
                    }
                })
                .collect::<Vec<_>>();
            let decoded = String::from_utf16(&units).unwrap();
            assert_eq!(decoded, "name,note\r\nthé,\"line 1\r\nline 2\"\r\n");
            assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "thé");
        }
    }

    #[test]
    fn malformed_utf16_obeys_strict_skip_and_repair_for_both_endiannesses() {
        for (label, little_endian) in [("utf-16le", true), ("utf-16be", false)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("malformed-{label}.csv"));
            let mut bytes = utf16_bytes("name,value\ngood,1\nbad,", little_endian, true);
            let malformed_unit = if little_endian {
                0xd800u16.to_le_bytes()
            } else {
                0xd800u16.to_be_bytes()
            };
            bytes.extend_from_slice(&malformed_unit);
            bytes.extend_from_slice(&utf16_bytes("\nafter,3\n", little_endian, false));
            std::fs::write(&path, bytes).unwrap();

            let overrides = |mode: &str| ParseOverrides {
                encoding: Some(label.to_string()),
                has_headers: Some(true),
                malformed: Some(mode.to_string()),
                ..Default::default()
            };
            assert!(CsvDocument::open(&path, Some(overrides("strict")), None).is_err());

            let skipped = CsvDocument::open(&path, Some(overrides("skip")), None).unwrap();
            assert_eq!(skipped.row_count(), 2);
            assert_eq!(skipped.display_rows(0, 2).unwrap()[0].1, vec!["good", "1"]);
            assert_eq!(skipped.display_rows(0, 2).unwrap()[1].1, vec!["after", "3"]);
            assert!(skipped
                .metadata()
                .warnings
                .iter()
                .any(|warning| warning.kind == "encoding"));

            let repaired = CsvDocument::open(&path, Some(overrides("repair")), None).unwrap();
            assert_eq!(repaired.row_count(), 3);
            assert_eq!(repaired.display_rows(1, 1).unwrap()[0].1, vec!["bad", "�"]);
            assert!(repaired
                .metadata()
                .warnings
                .iter()
                .any(|warning| warning.kind == "encoding"));
        }
    }

    #[test]
    fn odd_trailing_utf16_bytes_obey_all_malformed_policies() {
        for (label, little_endian) in [("utf-16le", true), ("utf-16be", false)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("odd-byte-{label}.csv"));
            let mut bytes = utf16_bytes("name,value\ngood,1\nbad,", little_endian, true);
            bytes.push(0x61);
            std::fs::write(&path, bytes).unwrap();
            let overrides = |mode: &str| ParseOverrides {
                encoding: Some(label.to_string()),
                has_headers: Some(true),
                malformed: Some(mode.to_string()),
                ..Default::default()
            };
            assert!(CsvDocument::open(&path, Some(overrides("strict")), None).is_err());
            let skipped = CsvDocument::open(&path, Some(overrides("skip")), None).unwrap();
            assert_eq!(skipped.row_count(), 1);
            assert_eq!(skipped.display_rows(0, 1).unwrap()[0].1, vec!["good", "1"]);
            let repaired = CsvDocument::open(&path, Some(overrides("repair")), None).unwrap();
            assert_eq!(repaired.row_count(), 2);
            assert_eq!(repaired.display_rows(1, 1).unwrap()[0].1, vec!["bad", "�"]);
        }
    }

    #[test]
    fn utf16_decoder_preserves_a_surrogate_pair_split_across_stream_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("split-surrogate.csv");
        let prefix = "name,value\nrow,";
        let prefix_bytes = prefix.encode_utf16().count() * 2;
        let filler_units = (64 * 1024 - 2 - prefix_bytes) / 2;
        let text = format!("{prefix}{}😀\n", "x".repeat(filler_units));
        let bytes = utf16_bytes(&text, true, true);
        std::fs::write(&path, bytes).unwrap();
        let doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                encoding: Some("utf-16le".to_string()),
                has_headers: Some(true),
                malformed: Some("strict".to_string()),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        let value = &doc.display_rows(0, 1).unwrap()[0].1[1];
        assert!(value.ends_with('😀'));
        assert_eq!(doc.row_count(), 1);
    }

    #[test]
    fn source_preparation_reports_progress_and_honors_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cancel-utf16.csv");
        let text = format!("name,value\n{}\n", "row,value\n".repeat(20_000));
        std::fs::write(&path, utf16_bytes(&text, true, true)).unwrap();
        let cancellation = CancellationToken::new();
        let cancel = cancellation.clone();
        let progress = move |bytes: usize| {
            if bytes >= 64 * 1024 {
                cancel.cancel();
            }
        };
        let error = CsvDocument::open_cancellable(
            &path,
            Some(ParseOverrides {
                encoding: Some("utf-16le".to_string()),
                has_headers: Some(true),
                ..Default::default()
            }),
            Some(&progress),
            &cancellation,
        )
        .err()
        .unwrap();
        assert!(error.contains("cancelled"));
    }

    #[test]
    fn unicode_delimiter_quote_and_escape_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unicode-dialect.csv");
        std::fs::write(&path, "name§note\nalpha§«one§two«\nbeta§«say ※«hello※««\n").unwrap();
        let overrides = ParseOverrides {
            delimiter: Some("§".to_string()),
            quote: Some("«".to_string()),
            escape: Some("※".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
        assert_eq!(doc.row_count(), 2);
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[1], "one§two");
        assert_eq!(doc.display_rows(1, 1).unwrap()[0].1[1], "say «hello«");
        doc.edit_cell(0, 1, "edited § «value«".to_string()).unwrap();
        doc.save(&path).unwrap();
        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(saved.contains("alpha§«edited § ※«value※««"));
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[1], "edited § «value«");
    }

    #[test]
    fn excel_sep_directive_is_detected_skipped_and_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("excel.csv");
        std::fs::write(&path, "sep=;\r\nname;value\r\nalpha;1\r\n").unwrap();
        let mut doc = CsvDocument::open(&path, None, None).unwrap();
        assert!(doc.metadata().effective.excel_sep);
        assert_eq!(doc.metadata().effective.delimiter, ";");
        assert_eq!(doc.metadata().headers, vec!["name", "value"]);
        assert_eq!(doc.row_count(), 1);
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "1"]);
        doc.save(&path).unwrap();
        assert!(std::fs::read_to_string(&path)
            .unwrap()
            .starts_with("sep=;\r\n"));
    }

    #[test]
    fn comment_prefixed_first_fields_are_quoted_on_save() {
        for comment in ["#", "※"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("comment-values.csv");
            std::fs::write(
                &path,
                format!("name,value\n\"{comment}literal\",1\nnormal,2\n"),
            )
            .unwrap();
            let overrides = ParseOverrides {
                comment: Some(comment.to_string()),
                has_headers: Some(true),
                ..Default::default()
            };
            let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
            doc.edit_cell(1, 1, "3".to_string()).unwrap();
            doc.save(&path).unwrap();
            assert_eq!(doc.row_count(), 2);
            assert_eq!(
                doc.display_rows(0, 1).unwrap()[0].1[0],
                format!("{comment}literal")
            );
            let saved = std::fs::read_to_string(&path).unwrap();
            assert!(saved.contains(&format!("\"{comment}literal\",1")));
        }
    }

    #[test]
    fn comment_prefixed_values_round_trip_in_utf16() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comment-utf16.csv");
        std::fs::write(
            &path,
            utf16_bytes("name,value\r\n\"#literal\",1\r\n", true, true),
        )
        .unwrap();
        let mut doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                encoding: Some("utf-16le".to_string()),
                comment: Some("#".to_string()),
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        doc.edit_cell(0, 1, "2".to_string()).unwrap();
        doc.save(&path).unwrap();
        assert_eq!(doc.row_count(), 1);
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["#literal", "2"]);
    }

    #[test]
    fn configured_comments_are_excluded_from_headers_and_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comments.csv");
        std::fs::write(
            &path,
            "# generated file\nname,value\nalpha,1\n# middle comment,ignored\nbeta,2\n",
        )
        .unwrap();
        let overrides = ParseOverrides {
            comment: Some("#".to_string()),
            ..Default::default()
        };
        let doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
        assert_eq!(doc.metadata().headers, vec!["name", "value"]);
        assert_eq!(doc.row_count(), 2);
        assert_eq!(doc.display_rows(1, 1).unwrap()[0].1, vec!["beta", "2"]);
    }

    #[test]
    fn comments_are_preserved_in_their_record_positions_when_saving() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("preserved-comments.csv");
        let contents = concat!(
            "# before header\n",
            "name,value\n",
            "alpha,1\n",
            "# before beta\n",
            "beta,2\n",
            "# trailing\n",
        );
        std::fs::write(&path, contents).unwrap();
        let mut doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                comment: Some("#".to_string()),
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        doc.edit_cell(0, 1, "3".to_string()).unwrap();
        doc.save(&path).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            concat!(
                "# before header\n",
                "name,value\n",
                "alpha,3\n",
                "# before beta\n",
                "beta,2\n",
                "# trailing\n",
            )
        );
    }

    #[test]
    fn standards_compliant_large_fields_have_no_default_rejection_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large-field.csv");
        let value = "x".repeat(300 * 1024);
        std::fs::write(&path, format!("name,value\nalpha,{value}\n")).unwrap();
        let doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        assert_eq!(doc.row_count(), 1);
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[1].len(), value.len());
    }

    #[test]
    fn standards_compliant_large_records_have_no_default_rejection_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large-record.csv");
        let headers = (0..11).map(|index| format!("c{index}")).collect::<Vec<_>>();
        let fields = std::iter::once("row".to_string())
            .chain((0..10).map(|_| "x".repeat(220 * 1024)))
            .collect::<Vec<_>>();
        std::fs::write(
            &path,
            format!("{}\n{}\n", headers.join(","), fields.join(",")),
        )
        .unwrap();
        let doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        assert_eq!(doc.row_count(), 1);
        assert!(
            doc.display_rows(0, 1).unwrap()[0]
                .1
                .iter()
                .map(String::len)
                .sum::<usize>()
                > 2 * 1024 * 1024
        );
    }

    #[test]
    fn utf8_bom_is_preserved_and_does_not_affect_no_header_search() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bom.csv");
        std::fs::write(&path, b"\xef\xbb\xbfalpha,1\nalpha,2\n").unwrap();
        let overrides = ParseOverrides {
            has_headers: Some(false),
            ..Default::default()
        };
        let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
        assert_eq!(
            doc.search("alpha", Some(0), true, true).unwrap(),
            vec![0, 1]
        );
        assert_eq!(doc.find_duplicates(Some(0)).unwrap(), vec![0, 1]);
        doc.edit_cell(0, 1, "3".to_string()).unwrap();
        doc.save(&path).unwrap();
        assert!(std::fs::read(&path).unwrap().starts_with(b"\xef\xbb\xbf"));
    }

    #[test]
    fn a_bom_is_removed_by_detected_length_even_when_encoding_is_overridden() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("forced-encoding.csv");
        std::fs::write(&path, b"\xef\xbb\xbfname,value\nalpha,1\n").unwrap();
        let mut doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                encoding: Some("windows-1252".to_string()),
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        assert_eq!(doc.metadata().headers, vec!["name", "value"]);
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "1"]);
        doc.edit_cell(0, 1, "2".to_string()).unwrap();
        doc.save(&path).unwrap();
        assert!(!std::fs::read(&path).unwrap().starts_with(b"\xef\xbb\xbf"));
    }

    #[test]
    fn bomless_utf16_stays_bomless_after_save() {
        for (label, little_endian) in [("utf-16le", true), ("utf-16be", false)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("bomless-save-{label}.csv"));
            std::fs::write(
                &path,
                utf16_bytes("name,value\nalpha,1\n", little_endian, false),
            )
            .unwrap();
            let mut doc = CsvDocument::open(
                &path,
                Some(ParseOverrides {
                    encoding: Some(label.to_string()),
                    has_headers: Some(true),
                    ..Default::default()
                }),
                None,
            )
            .unwrap();
            doc.edit_cell(0, 1, "2".to_string()).unwrap();
            doc.save(&path).unwrap();
            let saved = std::fs::read(&path).unwrap();
            assert!(!saved.starts_with(b"\xff\xfe"));
            assert!(!saved.starts_with(b"\xfe\xff"));
            assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "2"]);
        }
    }

    #[test]
    fn utf16_excel_sep_and_unicode_syntax_work_together() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("utf16-unicode-sep.csv");
        let text = "sep=§\r\nname§note\r\nalpha§«one§two«\r\n";
        std::fs::write(&path, utf16_bytes(text, true, true)).unwrap();
        let doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                encoding: Some("utf-16le".to_string()),
                delimiter: Some("§".to_string()),
                quote: Some("«".to_string()),
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        assert!(doc.metadata().effective.excel_sep);
        assert_eq!(doc.metadata().headers, vec!["name", "note"]);
        assert_eq!(
            doc.display_rows(0, 1).unwrap()[0].1,
            vec!["alpha", "one§two"]
        );
    }

    #[test]
    fn rfc7111_fragments_resolve_against_header_and_data_rows() {
        let (_dir, doc) = document("name,value\nalpha,1\nbeta,2\n");
        let rows = "row=1;3".parse::<CsvFragment>().unwrap();
        assert_eq!(
            doc.resolve_fragment(&rows),
            vec![
                ResolvedFragmentRegion::Rows(0..=0),
                ResolvedFragmentRegion::Rows(2..=2),
            ]
        );
        let cells = "cell=2,1-3,2".parse::<CsvFragment>().unwrap();
        assert_eq!(
            doc.resolve_fragment(&cells),
            vec![ResolvedFragmentRegion::Cells {
                rows: 1..=2,
                columns: 0..=1,
            }]
        );
    }

    #[test]
    fn invalid_parse_overrides_are_reported_instead_of_silently_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("invalid-overrides.csv");
        std::fs::write(&path, "name,value\nalpha,1\n").unwrap();
        for overrides in [
            ParseOverrides {
                delimiter: Some("two".to_string()),
                ..Default::default()
            },
            ParseOverrides {
                encoding: Some("made-up-encoding".to_string()),
                ..Default::default()
            },
            ParseOverrides {
                malformed: Some("maybe".to_string()),
                ..Default::default()
            },
            ParseOverrides {
                line_ending: Some("vertical".to_string()),
                ..Default::default()
            },
            ParseOverrides {
                delimiter: Some("|".to_string()),
                quote: Some("|".to_string()),
                ..Default::default()
            },
            ParseOverrides {
                delimiter: Some("|".to_string()),
                comment: Some("|".to_string()),
                ..Default::default()
            },
        ] {
            assert!(CsvDocument::open(&path, Some(overrides), None).is_err());
        }
    }

    #[test]
    fn strict_mode_rejects_invalid_quote_grammar() {
        for contents in [
            "name,value\nalpha,ba\"d\n",
            "name,value\nalpha,\"unclosed\n",
            "name,value\nalpha,\"closed\"tail\n",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("strict.csv");
            std::fs::write(&path, contents).unwrap();
            let overrides = ParseOverrides {
                malformed: Some("strict".to_string()),
                has_headers: Some(true),
                ..Default::default()
            };
            assert!(CsvDocument::open(&path, Some(overrides), None).is_err());
        }
    }

    #[test]
    fn empty_physical_records_follow_the_selected_malformed_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blank-record.csv");
        std::fs::write(&path, "name,value\n\nalpha,1\n").unwrap();
        let overrides = |mode: &str| ParseOverrides {
            malformed: Some(mode.to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        assert!(CsvDocument::open(&path, Some(overrides("strict")), None).is_err());
        let skipped = CsvDocument::open(&path, Some(overrides("skip")), None).unwrap();
        assert_eq!(skipped.row_count(), 1);
        assert_eq!(skipped.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "1"]);
        let repaired = CsvDocument::open(&path, Some(overrides("repair")), None).unwrap();
        assert_eq!(repaired.row_count(), 2);
        assert_eq!(repaired.display_rows(0, 1).unwrap()[0].1, vec!["", ""]);
        assert_eq!(
            repaired.display_rows(1, 1).unwrap()[0].1,
            vec!["alpha", "1"]
        );
    }

    #[test]
    fn malformed_width_modes_are_strict_skip_and_repair() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("widths.csv");
        std::fs::write(&path, "name,value\nalpha,1\nmissing\nbeta,2,extra\n").unwrap();

        let strict = ParseOverrides {
            malformed: Some("strict".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        assert!(CsvDocument::open(&path, Some(strict), None).is_err());

        let skip = ParseOverrides {
            malformed: Some("skip".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let skipped = CsvDocument::open(&path, Some(skip), None).unwrap();
        assert_eq!(skipped.row_count(), 1);
        assert_eq!(skipped.display_rows(0, 1).unwrap()[0].1, vec!["alpha", "1"]);

        let repair = ParseOverrides {
            malformed: Some("repair".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let repaired = CsvDocument::open(&path, Some(repair), None).unwrap();
        assert_eq!(repaired.row_count(), 3);
        assert_eq!(
            repaired.display_rows(1, 1).unwrap()[0].1,
            vec!["missing", ""]
        );
        assert_eq!(repaired.display_rows(2, 1).unwrap()[0].1, vec!["beta", "2"]);
    }

    #[test]
    fn invalid_encoding_follows_strict_skip_and_repair_modes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("invalid-utf8.csv");
        std::fs::write(&path, b"name,value\nalpha,\xff\nbeta,ok\n").unwrap();
        let settings = |mode: &str| ParseOverrides {
            encoding: Some("utf-8".to_string()),
            malformed: Some(mode.to_string()),
            has_headers: Some(true),
            ..Default::default()
        };

        assert!(CsvDocument::open(&path, Some(settings("strict")), None).is_err());
        let skipped = CsvDocument::open(&path, Some(settings("skip")), None).unwrap();
        assert_eq!(skipped.row_count(), 1);
        assert_eq!(skipped.display_rows(0, 1).unwrap()[0].1, vec!["beta", "ok"]);
        assert!(skipped
            .metadata()
            .warnings
            .iter()
            .any(|warning| warning.kind == "encoding"));

        let repaired = CsvDocument::open(&path, Some(settings("repair")), None).unwrap();
        assert_eq!(repaired.row_count(), 2);
        assert_eq!(repaired.display_rows(0, 1).unwrap()[0].1[1], "�");
        assert!(repaired
            .metadata()
            .warnings
            .iter()
            .any(|warning| warning.kind == "encoding"));
    }

    #[test]
    fn warning_collection_is_bounded_for_highly_malformed_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("many-warnings.csv");
        let mut contents = String::from("name,value\n");
        for index in 0..(MAX_WARNING_COUNT + 75) {
            contents.push_str(&format!("missing-{index}\n"));
        }
        std::fs::write(&path, contents).unwrap();
        let doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                has_headers: Some(true),
                malformed: Some("skip".to_string()),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        assert_eq!(doc.row_count(), 0);
        assert_eq!(doc.metadata().warnings.len(), MAX_WARNING_COUNT);
    }

    #[test]
    fn spaces_and_nul_bytes_are_preserved_as_field_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("significant-content.csv");
        std::fs::write(&path, b"name,value\n  alpha  ,\0inside\n").unwrap();
        let doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        assert_eq!(
            doc.display_rows(0, 1).unwrap()[0].1,
            vec!["  alpha  ", "\0inside"]
        );
    }

    #[test]
    fn record_size_repair_preserves_the_rectangular_row_shape() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("record-limit.csv");
        std::fs::write(&path, "name,left,right\nrow,1234,5678\n").unwrap();
        let settings = |mode: &str| ParseOverrides {
            malformed: Some(mode.to_string()),
            has_headers: Some(true),
            max_record_size: Some(7),
            ..Default::default()
        };
        assert!(CsvDocument::open(&path, Some(settings("strict")), None).is_err());
        assert_eq!(
            CsvDocument::open(&path, Some(settings("skip")), None)
                .unwrap()
                .row_count(),
            0
        );
        let repaired = CsvDocument::open(&path, Some(settings("repair")), None).unwrap();
        assert_eq!(repaired.row_count(), 1);
        assert_eq!(
            repaired.display_rows(0, 1).unwrap()[0].1,
            vec!["row", "1234", ""]
        );
        assert_eq!(repaired.display_rows(0, 1).unwrap()[0].1.len(), 3);
        assert!(repaired
            .metadata()
            .warnings
            .iter()
            .any(|warning| warning.kind == "repaired"));
    }

    #[test]
    fn repaired_field_truncation_does_not_consume_the_record_budget() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("combined-limits.csv");
        std::fs::write(&path, "aaaa,bb\n").unwrap();
        let doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                has_headers: Some(false),
                malformed: Some("repair".to_string()),
                max_field_size: Some(2),
                max_record_size: Some(4),
                ..Default::default()
            }),
            None,
        )
        .unwrap();

        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["aa", "bb"]);
    }

    #[test]
    fn explicit_size_limits_apply_in_all_malformed_modes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("limits.csv");
        std::fs::write(&path, "name,value\nalpha,abcd\n").unwrap();

        let settings = |mode: &str| ParseOverrides {
            malformed: Some(mode.to_string()),
            has_headers: Some(true),
            max_field_size: Some(3),
            max_record_size: Some(32),
            ..Default::default()
        };
        assert!(CsvDocument::open(&path, Some(settings("strict")), None).is_err());
        let skipped = CsvDocument::open(&path, Some(settings("skip")), None).unwrap();
        assert_eq!(skipped.row_count(), 0);
        let repaired = CsvDocument::open(&path, Some(settings("repair")), None).unwrap();
        assert_eq!(repaired.row_count(), 1);
        assert_eq!(
            repaired.display_rows(0, 1).unwrap()[0].1,
            vec!["alp", "abc"]
        );
    }

    #[test]
    fn malformed_unicode_quote_grammar_obeys_all_policies() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("malformed-unicode.csv");
        std::fs::write(&path, "name§value\ngood§«one«\nbad§«unclosed\n").unwrap();
        let overrides = |mode: &str| ParseOverrides {
            delimiter: Some("§".to_string()),
            quote: Some("«".to_string()),
            has_headers: Some(true),
            malformed: Some(mode.to_string()),
            ..Default::default()
        };
        assert!(CsvDocument::open(&path, Some(overrides("strict")), None).is_err());

        let skipped = CsvDocument::open(&path, Some(overrides("skip")), None).unwrap();
        assert_eq!(skipped.row_count(), 1);
        assert_eq!(
            skipped.display_rows(0, 1).unwrap()[0].1,
            vec!["good", "one"]
        );
        assert!(skipped
            .metadata()
            .warnings
            .iter()
            .any(|warning| warning.kind == "malformed-quote"));

        let repaired = CsvDocument::open(&path, Some(overrides("repair")), None).unwrap();
        assert_eq!(repaired.row_count(), 2);
        assert_eq!(repaired.display_rows(1, 1).unwrap()[0].1[0], "bad");
        assert!(repaired.display_rows(1, 1).unwrap()[0].1[1].contains("unclosed"));
        assert!(repaired
            .metadata()
            .warnings
            .iter()
            .any(|warning| warning.kind == "malformed-quote"));
    }

    #[test]
    fn unicode_comment_character_is_supported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unicode-comments.csv");
        std::fs::write(
            &path,
            "※ generated\nname,value\nalpha,1\n※ ignored\nbeta,2\n",
        )
        .unwrap();
        let overrides = ParseOverrides {
            comment: Some("※".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
        assert_eq!(doc.row_count(), 2);
        assert_eq!(doc.display_rows(1, 1).unwrap()[0].1, vec!["beta", "2"]);
    }

    #[test]
    fn disabling_excel_sep_treats_directive_as_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disabled-sep.csv");
        std::fs::write(&path, "sep=;\nname;value\nalpha;1\n").unwrap();
        let overrides = ParseOverrides {
            excel_sep: Some(false),
            delimiter: Some("semicolon".to_string()),
            has_headers: Some(false),
            malformed: Some("repair".to_string()),
            ..Default::default()
        };
        let doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
        assert!(!doc.metadata().effective.excel_sep);
        assert_eq!(doc.row_count(), 3);
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "sep=");
    }

    #[test]
    fn no_op_edits_and_restored_deletes_are_clean() {
        let (_dir, mut doc) = document("name,value\na,1\n");
        doc.edit_cell(0, 1, "1".to_string()).unwrap();
        assert!(!doc.is_dirty());
        doc.delete_display_row(0).unwrap();
        assert!(doc.is_dirty());
        doc.restore_display_row(0).unwrap();
        assert!(!doc.is_dirty());
    }

    #[test]
    fn opens_reads_searches_and_sorts() {
        let (_dir, mut doc) = document("name,value\nbeta,2\nalpha,1\n");
        assert_eq!(doc.row_count(), 2);
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "beta");
        assert_eq!(doc.search("alpha", None, false, false).unwrap(), vec![1]);
        doc.sort(Some(SortSpec {
            column: 0,
            direction: SortDirection::Ascending,
        }))
        .unwrap();
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "alpha");
    }

    #[test]
    fn source_cell_edits_keep_their_identity_after_sorting() {
        let (_dir, mut doc) = document("name,value\nbeta,2\nalpha,1\n");
        let beta_source_row = doc.source_row_for_display(0).unwrap();
        doc.sort(Some(SortSpec {
            column: 0,
            direction: SortDirection::Ascending,
        }))
        .unwrap();
        doc.edit_source_cell(beta_source_row, 1, "9".to_string())
            .unwrap();
        let beta_display_row = doc.display_row_for_source(beta_source_row).unwrap();
        assert_eq!(doc.display_rows(beta_display_row, 1).unwrap()[0].1[1], "9");
    }

    #[test]
    fn search_index_matches_case_insensitive_column_searches() {
        let (_dir, mut doc) = document("name,value\nAlpha,first\nbeta,second\nalphabet,third\n");
        let contains_before = doc.search("ALPHA", Some(0), false, false).unwrap();
        let exact_before = doc.search("ALPHA", Some(0), false, true).unwrap();
        doc.build_search_index().unwrap();
        assert!(doc.has_search_index());
        assert_eq!(
            doc.search("ALPHA", Some(0), false, false).unwrap(),
            contains_before
        );
        assert_eq!(
            doc.search("ALPHA", Some(0), false, true).unwrap(),
            exact_before
        );
        doc.clear_search_index();
        assert!(!doc.has_search_index());
    }

    #[test]
    fn on_demand_search_index_retains_only_the_selected_column() {
        let (_dir, mut doc) = document("name,value\nAlpha,first\nbeta,second\nalphabet,first\n");
        let cancellation = CancellationToken::new();
        let name_matches = doc.search("ALPHA", Some(0), false, false).unwrap();
        let value_matches = doc.search("FIRST", Some(1), false, true).unwrap();
        let indexed_rows = std::cell::Cell::new(0);
        let report_progress = |processed, _| indexed_rows.set(processed);

        doc.ensure_search_index_for_column_cancellable(0, &cancellation, Some(&report_progress))
            .unwrap();
        assert_eq!(indexed_rows.get(), doc.row_count());
        assert!(doc.has_search_index_for_column(0));
        assert!(!doc.has_search_index_for_column(1));
        assert_eq!(
            doc.search("ALPHA", Some(0), false, false).unwrap(),
            name_matches
        );

        doc.ensure_search_index_for_column_cancellable(1, &cancellation, None)
            .unwrap();
        assert!(!doc.has_search_index_for_column(0));
        assert!(doc.has_search_index_for_column(1));
        assert_eq!(
            doc.search("FIRST", Some(1), false, true).unwrap(),
            value_matches
        );
    }

    #[test]
    fn compact_postings_preserve_single_and_duplicate_rows() {
        let mut postings = RowPostings::One(3);
        assert_eq!(postings.as_slice(), &[3]);
        postings.push(8);
        postings.push(13);
        assert_eq!(postings.as_slice(), &[3, 8, 13]);
    }

    #[test]
    fn sorting_uses_values_beyond_the_first_256_bytes() {
        let prefix = "x".repeat(256);
        let csv = format!("name,value\n{prefix}z,2\n{prefix}a,1\n");
        let (_dir, mut doc) = document(&csv);
        doc.sort(Some(SortSpec {
            column: 0,
            direction: SortDirection::Ascending,
        }))
        .unwrap();
        assert!(doc.display_rows(0, 1).unwrap()[0].1[0].ends_with('a'));
    }

    #[test]
    fn edits_deletes_and_saves() {
        let (dir, mut doc) = document("name,value\na,1\nb,2\n");
        doc.edit_cell(0, 1, "9".to_string()).unwrap();
        doc.delete_display_row(1).unwrap();
        assert!(doc.is_dirty());
        let output = dir.path().join("saved.csv");
        doc.save(&output).unwrap();
        assert_eq!(
            std::fs::read_to_string(output).unwrap(),
            "name,value\na,9\n"
        );
        assert!(!doc.is_dirty());
    }

    #[test]
    fn serializes_display_rows_with_csv_settings_and_without_trailing_terminator() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("semicolon.csv");
        std::fs::write(
            &path,
            "name;note\r\nalpha;'one;two'\r\nbeta;'line\r\nbreak'\r\n",
        )
        .unwrap();
        let overrides = ParseOverrides {
            delimiter: Some("semicolon".to_string()),
            quote: Some("single".to_string()),
            line_ending: Some("crlf".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let doc = CsvDocument::open(path, Some(overrides), None).unwrap();

        assert_eq!(
            doc.serialize_display_rows(&[0, 1]).unwrap(),
            "alpha;'one;two'\r\nbeta;'line\r\nbreak'"
        );
    }

    #[test]
    fn serialization_applies_sort_edits_and_deleted_rows() {
        let (_dir, mut doc) = document("name,value\nbeta,2\nalpha,1\ngamma,3\n");
        doc.sort(Some(SortSpec {
            column: 0,
            direction: SortDirection::Ascending,
        }))
        .unwrap();
        doc.edit_cell(0, 1, "edited, value".to_string()).unwrap();
        doc.delete_display_row(1).unwrap();

        assert_eq!(
            doc.serialize_display_rows(&[2, 0, 1]).unwrap(),
            "gamma,3\nalpha,\"edited, value\""
        );
    }

    #[test]
    fn save_can_atomically_replace_the_open_file() {
        let (_dir, mut doc) = document("name,value\na,1\nb,2\n");
        let path = doc.path().to_path_buf();
        doc.edit_cell(1, 1, "3".to_string()).unwrap();
        doc.save(&path).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "name,value\na,1\nb,3\n"
        );
        assert_eq!(doc.display_rows(1, 1).unwrap()[0].1[1], "3");
    }

    #[cfg(unix)]
    #[test]
    fn save_preserves_existing_destination_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let (dir, mut doc) = document("a,b\nx,1\n");
        let path = dir.path().join("permissions.csv");
        std::fs::write(&path, "a,b\nx,1\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        doc.save(&path).unwrap();

        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }

    #[cfg(unix)]
    #[test]
    fn save_through_symlink_updates_referent_without_replacing_link() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let referent = dir.path().join("actual.csv");
        let link = dir.path().join("linked.csv");
        std::fs::write(&referent, "name,value\na,1\n").unwrap();
        symlink("actual.csv", &link).unwrap();

        let mut doc = CsvDocument::open(&link, None, None).unwrap();
        doc.edit_cell(0, 1, "2".to_string()).unwrap();
        doc.save(&link).unwrap();

        assert!(std::fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            std::fs::read_to_string(&referent).unwrap(),
            "name,value\na,2\n"
        );
        assert_eq!(doc.path(), link.as_path());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_retargeted_during_save_aborts_without_touching_either_referent() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.csv");
        let second = dir.path().join("second.csv");
        let link = dir.path().join("linked.csv");
        std::fs::write(&first, "name,value\na,1\n").unwrap();
        std::fs::write(&second, "name,value\nb,2\n").unwrap();
        symlink("first.csv", &link).unwrap();
        let mut doc = CsvDocument::open(&link, None, None).unwrap();
        doc.edit_cell(0, 1, "edited".to_string()).unwrap();
        let retargeted = std::cell::Cell::new(false);
        let link_for_progress = link.clone();
        let progress = |_, _| {
            if !retargeted.replace(true) {
                std::fs::remove_file(&link_for_progress).unwrap();
                symlink("second.csv", &link_for_progress).unwrap();
            }
        };

        let error = doc
            .save_cancellable_with_progress(&link, &CancellationToken::new(), &progress)
            .unwrap_err();
        assert!(error.contains("changed on disk"));
        assert_eq!(
            std::fs::read_to_string(&first).unwrap(),
            "name,value\na,1\n"
        );
        assert_eq!(
            std::fs::read_to_string(&second).unwrap(),
            "name,value\nb,2\n"
        );
        assert!(doc.is_dirty());
    }

    #[cfg(unix)]
    #[test]
    fn parent_symlink_retargeted_during_save_is_detected() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let first_dir = dir.path().join("first");
        let second_dir = dir.path().join("second");
        std::fs::create_dir_all(&first_dir).unwrap();
        std::fs::create_dir_all(&second_dir).unwrap();
        let first = first_dir.join("data.csv");
        let second = second_dir.join("data.csv");
        std::fs::write(&first, "name,value\na,1\n").unwrap();
        std::fs::write(&second, "name,value\nb,2\n").unwrap();
        let parent_link = dir.path().join("current");
        symlink("first", &parent_link).unwrap();
        let logical_path = parent_link.join("data.csv");
        let mut doc = CsvDocument::open(&logical_path, None, None).unwrap();
        doc.edit_cell(0, 1, "edited".to_string()).unwrap();
        let retargeted = std::cell::Cell::new(false);
        let link_for_progress = parent_link.clone();
        let progress = |_, _| {
            if !retargeted.replace(true) {
                std::fs::remove_file(&link_for_progress).unwrap();
                symlink("second", &link_for_progress).unwrap();
            }
        };

        assert!(doc
            .save_cancellable_with_progress(&logical_path, &CancellationToken::new(), &progress,)
            .unwrap_err()
            .contains("changed on disk"));
        assert_eq!(
            std::fs::read_to_string(&first).unwrap(),
            "name,value\na,1\n"
        );
        assert_eq!(
            std::fs::read_to_string(&second).unwrap(),
            "name,value\nb,2\n"
        );
    }

    #[test]
    fn failed_save_validation_leaves_destination_and_edits_intact() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("strict.csv");
        let original = b"a,b\nx,1\n";
        std::fs::write(&path, original).unwrap();
        let mut doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                has_headers: Some(true),
                malformed: Some("repair".to_string()),
                max_field_size: Some(3),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        doc.edit_cell(0, 0, "too-long".to_string()).unwrap();

        assert!(doc.save(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert!(doc.is_dirty());
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "too-long");
    }

    #[test]
    fn headerless_document_can_save_after_all_rows_are_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("headerless.csv");
        std::fs::write(&path, "1,2\n").unwrap();
        let mut doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                has_headers: Some(false),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        doc.delete_display_row(0).unwrap();

        doc.save(&path).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"");
        assert_eq!(doc.row_count(), 0);
    }

    #[test]
    fn save_validation_preserves_disabled_escape_and_comment_settings() {
        let (dir, mut doc) = document("a,b\nx,1\n");
        doc.edit_cell(0, 0, "#not-a-comment".to_string()).unwrap();
        doc.edit_cell(0, 1, "backslash\\\"quote".to_string())
            .unwrap();
        let output = dir.path().join("syntax.csv");

        doc.save(&output).unwrap();
        assert_eq!(doc.row_count(), 1);
        assert_eq!(
            doc.display_rows(0, 1).unwrap()[0].1,
            vec!["#not-a-comment", "backslash\\\"quote"]
        );
    }

    #[test]
    fn external_change_during_save_is_checked_before_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent.csv");
        std::fs::write(&path, "a,b\nx,1\ny,2\n").unwrap();
        let mut doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        doc.edit_cell(0, 1, "edited".to_string()).unwrap();
        let changed = std::cell::Cell::new(false);
        let external_path = path.clone();
        let progress = |_, _| {
            if !changed.replace(true) {
                std::fs::write(&external_path, "a,b\nexternal,8\ny,2\n").unwrap();
            }
        };
        let cancellation = CancellationToken::new();

        let error = doc
            .save_cancellable_with_progress(&path, &cancellation, &progress)
            .unwrap_err();
        assert!(error.contains("changed on disk"));
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "a,b\nexternal,8\ny,2\n"
        );
        assert!(doc.is_dirty());
    }

    #[test]
    fn concurrent_save_as_destination_change_is_not_overwritten() {
        let (dir, mut doc) = document("a,b\noriginal,1\n");
        doc.edit_cell(0, 1, "edited".to_string()).unwrap();
        let target = dir.path().join("save-as.csv");
        std::fs::write(&target, "a,b\nbefore,2\n").unwrap();
        let changed = std::cell::Cell::new(false);
        let external_target = target.clone();
        let progress = |_, _| {
            if !changed.replace(true) {
                std::fs::write(&external_target, "a,b\nexternal,9\n").unwrap();
            }
        };
        let cancellation = CancellationToken::new();

        let error = doc
            .save_cancellable_with_progress(&target, &cancellation, &progress)
            .unwrap_err();
        assert!(error.contains("changed on disk"));
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "a,b\nexternal,9\n"
        );
    }

    #[test]
    fn explicit_external_overwrite_uses_the_immutable_opened_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overwrite.csv");
        std::fs::write(&path, "a,b\noriginal,1\nsecond,2\n").unwrap();
        let mut doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        doc.edit_cell(0, 1, "edited".to_string()).unwrap();
        std::fs::write(&path, "a,b\nexternal,8\nchanged,9\n").unwrap();
        let cancellation = CancellationToken::new();

        doc.save_cancellable_with_progress_overwrite_external(&path, &cancellation, &|_, _| {})
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "a,b\noriginal,edited\nsecond,2\n"
        );
    }

    #[test]
    fn explicit_external_overwrite_can_restore_a_deleted_source() {
        let (dir, mut doc) = document("a,b\noriginal,1\n");
        let path = dir.path().join("sample.csv");
        doc.edit_cell(0, 1, "restored".to_string()).unwrap();
        std::fs::remove_file(&path).unwrap();
        let cancellation = CancellationToken::new();

        doc.save_cancellable_with_progress_overwrite_external(&path, &cancellation, &|_, _| {})
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "a,b\noriginal,restored\n"
        );
    }

    #[test]
    fn live_source_files_are_read_from_an_immutable_snapshot() {
        let mut contents = String::from("a,b\n");
        for row in 0..256 {
            contents.push_str(&format!("row-{row},value-{row}\n"));
        }
        assert!(contents.len() > 1024);
        let (_dir, doc) = document(&contents);
        assert!(doc._prepared_source.is_some());
        assert_ne!(doc.data_path, doc.path);
        assert!(doc.mmap.is_some());
        let last_row = doc.display_rows(doc.row_count() - 1, 1).unwrap();

        std::fs::write(&doc.path, "a,b\ntruncated,source\n").unwrap();
        assert_eq!(doc.display_rows(doc.row_count() - 1, 1).unwrap(), last_row);
    }

    #[test]
    fn prepared_source_lives_until_a_background_index_build_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prepared-index.csv");
        std::fs::write(&path, "name§value\nalpha§1\nbeta§2\n").unwrap();
        let doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                delimiter: Some("§".to_string()),
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        let prepared_path = doc.data_path.clone();
        let build = doc.prepare_search_index_build();
        drop(doc);
        assert!(prepared_path.exists());
        let index = build.build(&CancellationToken::new(), None).unwrap();
        assert_eq!(index.columns.len(), 2);
        assert!(!prepared_path.exists());
    }

    #[test]
    fn background_index_snapshots_are_rejected_after_edits() {
        let (_dir, mut doc) = document("name,value\na,one\nb,two\n");
        let build = doc.prepare_search_index_build();
        doc.edit_cell(0, 1, "changed".to_string()).unwrap();
        let built = build.build(&CancellationToken::new(), None).unwrap();
        assert!(!doc.install_search_index(built));
        assert!(!doc.has_search_index());
        assert_eq!(
            doc.search("changed", Some(1), false, true).unwrap(),
            vec![0]
        );
    }

    #[test]
    fn indexed_search_uses_complete_values_and_queries() {
        let prefix = "x".repeat(300);
        let contents = format!("name,value\n{prefix}suffix,1\n{prefix},2\n");
        let (_dir, mut doc) = document(&contents);
        let query = format!("{}suffix", "x".repeat(300));
        let expected_contains = doc.search("suffix", Some(0), false, false).unwrap();
        let expected_exact = doc.search(&query, Some(0), false, true).unwrap();
        doc.build_search_index().unwrap();
        assert_eq!(
            doc.search("suffix", Some(0), false, false).unwrap(),
            expected_contains
        );
        assert_eq!(
            doc.search(&query, Some(0), false, true).unwrap(),
            expected_exact
        );
        assert_eq!(expected_contains, vec![0]);
        assert_eq!(expected_exact, vec![0]);
    }

    #[test]
    fn search_and_duplicates_follow_edits_and_deletions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("edited-query.csv");
        std::fs::write(&path, "name,value\na,one\nb,two\nc,two\n").unwrap();
        let mut doc = CsvDocument::open(
            path,
            Some(ParseOverrides {
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        doc.build_search_index().unwrap();
        doc.edit_cell(0, 1, "two".to_string()).unwrap();
        doc.delete_display_row(1).unwrap();

        assert_eq!(
            doc.search("one", Some(1), false, true).unwrap(),
            Vec::<usize>::new()
        );
        assert_eq!(doc.search("two", Some(1), false, true).unwrap(), vec![0, 2]);
        assert_eq!(doc.find_duplicates(Some(1)).unwrap(), vec![0, 2]);

        doc.delete_display_row(2).unwrap();
        assert!(doc.find_duplicates(Some(1)).unwrap().is_empty());
    }

    #[test]
    fn duplicate_and_search_columns_are_validated() {
        let (_dir, doc) = document("name,value\na,1\n");
        assert!(doc.search("a", Some(2), false, false).is_err());
        assert!(doc.find_duplicates(Some(2)).is_err());
    }

    #[test]
    fn save_preserves_custom_quote_and_crlf() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("custom.csv");
        std::fs::write(&path, "name;note\r\nalpha;'one;two'\r\n").unwrap();
        let overrides = ParseOverrides {
            delimiter: Some("semicolon".to_string()),
            quote: Some("single".to_string()),
            line_ending: Some("crlf".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
        doc.edit_cell(0, 1, "updated;note".to_string()).unwrap();
        doc.save(&path).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "name;note\r\nalpha;'updated;note'\r\n"
        );
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[1], "updated;note");
    }

    #[test]
    fn save_preserves_cr_terminators_and_embedded_newlines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("classic-mac.csv");
        std::fs::write(&path, b"name,note\ralpha,\"line one\nline two\"\r").unwrap();
        let overrides = ParseOverrides {
            line_ending: Some("cr".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
        doc.edit_cell(0, 0, "alpha, edited".to_string()).unwrap();
        doc.save(&path).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"name,note\r\"alpha, edited\",\"line one\nline two\"\r"
        );
        assert_eq!(doc.metadata().effective.line_ending, "cr");
    }

    #[test]
    fn unicode_paths_round_trip_through_save() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("données-日本語-😀.csv");
        std::fs::write(&path, "name,value\ncafé,1\n").unwrap();
        let mut doc = CsvDocument::open(
            &path,
            Some(ParseOverrides {
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        doc.edit_cell(0, 1, "2".to_string()).unwrap();
        doc.save(&path).unwrap();
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1, vec!["café", "2"]);
    }

    #[test]
    fn save_preserves_single_byte_source_encoding() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("windows-1252.csv");
        std::fs::write(&path, b"name,value\r\ncaf\xe9,one\r\n").unwrap();
        let overrides = ParseOverrides {
            encoding: Some("windows-1252".to_string()),
            line_ending: Some("crlf".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
        doc.edit_cell(0, 1, "deux".to_string()).unwrap();
        doc.save(&path).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"name,value\r\ncaf\xe9,deux\r\n"
        );
        assert_eq!(doc.display_rows(0, 1).unwrap()[0].1[0], "café");
    }

    #[test]
    fn unrepresentable_legacy_encoding_edits_do_not_replace_the_source() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("windows-1252-atomic.csv");
        let original = b"name,value\r\ncaf\xe9,one\r\n".to_vec();
        std::fs::write(&path, &original).unwrap();
        let overrides = ParseOverrides {
            encoding: Some("windows-1252".to_string()),
            line_ending: Some("crlf".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let mut doc = CsvDocument::open(&path, Some(overrides), None).unwrap();
        doc.edit_cell(0, 1, "🧊".to_string()).unwrap();
        assert!(doc
            .save(&path)
            .unwrap_err()
            .contains("cannot be represented"));
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert!(doc.is_dirty());
    }

    #[test]
    fn bulk_row_mutation_is_transactional_on_validation_failure() {
        let (_dir, mut doc) = document("name,value\na,1\nb,2\n");
        assert!(doc
            .set_display_rows_deleted(&[0, usize::MAX], true)
            .is_err());
        assert!(!doc.is_display_row_deleted(0));
        assert!(!doc.is_dirty());
    }

    #[test]
    fn serializes_rectangular_cell_ranges_with_edits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cells.csv");
        std::fs::write(&path, "name,left,right\na,1,x\nb,2,y\nc,3,z\n").unwrap();
        let mut doc = CsvDocument::open(
            path,
            Some(ParseOverrides {
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        doc.edit_cell(1, 1, "two, edited".to_string()).unwrap();
        let cancellation = CancellationToken::new();
        let progress = std::cell::Cell::new(0);
        let text = doc
            .serialize_display_cell_range_cancellable_with_progress(
                0,
                2,
                1,
                2,
                &cancellation,
                &|processed, _| progress.set(processed),
            )
            .unwrap();
        assert_eq!(text, "1,x\n\"two, edited\",y\n3,z");
        assert_eq!(progress.get(), 3);
    }

    #[test]
    fn cached_open_and_sort_create_reusable_cache_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cached.csv");
        let cache_root = dir.path().join("cache");
        std::fs::write(&path, "name,value\nb,2\na,1\n").unwrap();
        let overrides = ParseOverrides {
            has_headers: Some(true),
            ..Default::default()
        };
        let mut doc =
            CsvDocument::open_cached(&path, Some(overrides.clone()), None, &cache_root).unwrap();
        doc.sort(Some(SortSpec {
            column: 0,
            direction: SortDirection::Ascending,
        }))
        .unwrap();
        let cache_dir = cache_root.join("csv-index-cache");
        let entries = std::fs::read_dir(&cache_dir).unwrap().count();
        assert!(entries >= 2, "offset and sort caches should both exist");

        let progress = std::cell::Cell::new(0);
        let reopened = CsvDocument::open_cached(
            &path,
            Some(overrides),
            Some(&|rows| progress.set(rows)),
            &cache_root,
        )
        .unwrap();
        assert_eq!(progress.get(), reopened.row_count());
    }

    #[test]
    fn prepared_unicode_sources_reuse_offset_and_sort_caches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cached-unicode.csv");
        let cache_root = dir.path().join("cache");
        std::fs::write(&path, "# note\nname§value\nb§2\na§1\n").unwrap();
        let overrides = ParseOverrides {
            delimiter: Some("§".to_string()),
            comment: Some("#".to_string()),
            has_headers: Some(true),
            ..Default::default()
        };
        let mut doc =
            CsvDocument::open_cached(&path, Some(overrides.clone()), None, &cache_root).unwrap();
        doc.sort(Some(SortSpec {
            column: 0,
            direction: SortDirection::Ascending,
        }))
        .unwrap();
        let cache_dir = cache_root.join("csv-index-cache");
        assert!(std::fs::read_dir(&cache_dir).unwrap().count() >= 2);

        let progress = std::cell::Cell::new(0);
        let reopened = CsvDocument::open_cached(
            &path,
            Some(overrides),
            Some(&|rows| progress.set(rows)),
            &cache_root,
        )
        .unwrap();
        assert_eq!(progress.get(), reopened.row_count());
        assert_eq!(reopened.display_rows(0, 2).unwrap().len(), 2);
    }

    #[test]
    fn streaming_search_reports_rows_and_result_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stream.csv");
        std::fs::write(&path, "name,value\na,hit\nb,miss\nc,hit\n").unwrap();
        let doc = CsvDocument::open(
            path,
            Some(ParseOverrides {
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        let streamed = std::cell::RefCell::new(Vec::new());
        let processed = std::cell::Cell::new(0);
        let result = doc
            .search_cancellable_streaming(
                "hit",
                Some(1),
                false,
                true,
                &CancellationToken::new(),
                &|batch, done, _| {
                    streamed.borrow_mut().extend_from_slice(batch);
                    processed.set(done);
                },
            )
            .unwrap();
        assert_eq!(result, vec![0, 2]);
        assert_eq!(*streamed.borrow(), result);
        assert_eq!(processed.get(), doc.row_count());
    }

    #[test]
    fn inverse_sort_lookup_tracks_source_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inverse.csv");
        std::fs::write(&path, "name,value\nc,3\na,1\nb,2\n").unwrap();
        let mut doc = CsvDocument::open(
            path,
            Some(ParseOverrides {
                has_headers: Some(true),
                ..Default::default()
            }),
            None,
        )
        .unwrap();
        doc.sort(Some(SortSpec {
            column: 0,
            direction: SortDirection::Ascending,
        }))
        .unwrap();
        assert_eq!(doc.display_row_for_source(0), Some(2));
        assert_eq!(doc.display_row_for_source(1), Some(0));
        assert_eq!(doc.display_row_for_source(2), Some(1));
        doc.sort(None).unwrap();
        assert_eq!(doc.display_row_for_source(2), Some(2));
    }
}
