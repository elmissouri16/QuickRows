mod csv_cache;
mod csv_handler;
mod csv_mmap;
mod disk_cache;
use csv_cache::CsvCache;
use csv_handler::{
    apply_parse_overrides, build_reader, build_row_offsets, build_row_offsets_mmap, decode_record,
    default_parse_settings, detect_parse_settings, get_headers, parse_info_from_settings,
    read_chunk, read_chunk_mmap, read_chunk_with_offsets, read_chunk_with_offsets_mmap,
    read_rows_by_index, read_rows_by_index_mmap, search_range_with_offsets,
    search_range_with_offsets_mmap, settings_cache_hash, ParseInfo, ParseOverrides, ParseSettings,
    ParseWarning, MAX_WARNING_COUNT,
};
use csv_mmap::open_mmap_if_large;
use disk_cache::{
    cache_key, ensure_cache_dir, offsets_cache_path, order_cache_path, prune_cache_dir,
    read_offsets_cache, read_order_cache, write_offsets_cache, write_order_cache,
};
use memmap2::Mmap;
use rayon::prelude::*;
// use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tauri::{Emitter, Manager, State, WebviewWindowBuilder};

#[cfg(desktop)]
use tauri::menu::{
    CheckMenuItemBuilder, MenuBuilder, MenuItemBuilder, MenuItemKind, SubmenuBuilder,
};

#[derive(Clone, serde::Serialize)]
struct SortedRow {
    index: usize,
    row: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct CsvMetadata {
    pub headers: Vec<String>,
    pub detected: ParseInfo,
    pub effective: ParseInfo,
    pub warnings: Vec<ParseWarning>,
    pub estimated_count: Option<usize>,
}

/// Inverted index for fast search: maps lowercase value → row indices
/// Key: lowercase cell value (truncated to 256 chars for memory efficiency)
/// Value: sorted list of row indices containing that value
type ColumnIndex = std::collections::HashMap<Box<str>, Vec<u32>>;

/// Search index for all columns
struct SearchIndex {
    /// Per-column inverted index (column_idx → ColumnIndex)
    columns: Vec<Option<ColumnIndex>>,
    /// Whether index is ready for use
    ready: bool,
    /// Columns skipped due to high cardinality (>500K unique values)
    skipped_columns: Vec<usize>,
}

impl SearchIndex {
    fn new() -> Self {
        SearchIndex {
            columns: Vec::new(),
            ready: false,
            skipped_columns: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.columns.clear();
        self.ready = false;
        self.skipped_columns.clear();
    }
}

struct AppState {
    file_path: Mutex<Option<String>>,
    total_rows: Mutex<usize>,
    headers: Mutex<Vec<String>>,
    cache: CsvCache,
    sorted_order: Mutex<Option<Vec<usize>>>,
    pending_open: Mutex<Option<String>>,
    row_offsets: Mutex<Option<Vec<u64>>>,
    mmap: Mutex<Option<Arc<Mmap>>>,
    parse_settings: Mutex<ParseSettings>,
    parse_info_detected: Mutex<ParseInfo>,
    parse_info_effective: Mutex<ParseInfo>,
    parse_warnings: Mutex<Vec<ParseWarning>>,
    search_index: Mutex<SearchIndex>,
    enable_indexing: Mutex<bool>,
    debug_logging: Mutex<bool>,
}

const SEARCH_CHUNK_SIZE: usize = 25_000;
const BULK_CHUNK_SIZE: usize = 10_000;
const INDEX_VALUE_MAX_LEN: usize = 256;
const INDEX_MAX_CARDINALITY: usize = 2_000_000; // Skip column if >2M unique values
const RESULT_CHUNK_SIZE: usize = 5_000;

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MatchesChunkPayload {
    request_id: u32,
    matches: Vec<usize>,
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct MatchesCompletePayload {
    request_id: u32,
    total: usize,
}

fn emit_matches_chunk(
    app: &tauri::AppHandle,
    event: &str,
    request_id: u32,
    matches: &[usize],
) -> Result<(), String> {
    if matches.is_empty() {
        return Ok(());
    }
    let payload = MatchesChunkPayload {
        request_id,
        matches: matches.to_vec(),
    };
    app.emit(event, payload).map_err(|err| err.to_string())?;
    Ok(())
}

fn emit_matches_complete(
    app: &tauri::AppHandle,
    event: &str,
    request_id: u32,
    total: usize,
) -> Result<(), String> {
    let payload = MatchesCompletePayload { request_id, total };
    app.emit(event, payload).map_err(|err| err.to_string())?;
    Ok(())
}

const DEBUG_LOG_FILE: &str = "quickrows.log";
const CRASH_LOG_FILE: &str = "quickrows-crash.log";
const LOG_MAX_BYTES: u64 = 10 * 1024 * 1024;

fn now_timestamp() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    let ms = duration.subsec_millis();
    format!("{secs}.{ms:03}")
}

fn debug_log_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    Ok(ensure_cache_dir(app)?.join(DEBUG_LOG_FILE))
}

fn crash_log_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    Ok(ensure_cache_dir(app)?.join(CRASH_LOG_FILE))
}

fn maybe_truncate_log(path: &Path) {
    let Ok(meta) = std::fs::metadata(path) else {
        return;
    };
    if meta.len() <= LOG_MAX_BYTES {
        return;
    }
    let _ = std::fs::write(path, b"");
}

fn append_log_line(path: &Path, line: &str) -> Result<(), String> {
    maybe_truncate_log(path);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| err.to_string())?;
    writeln!(file, "{line}").map_err(|err| err.to_string())?;
    Ok(())
}

fn append_debug_line(app: &tauri::AppHandle, line: &str) -> Result<(), String> {
    append_log_line(&debug_log_path(app)?, line)
}

fn append_crash_line(app: &tauri::AppHandle, line: &str) -> Result<(), String> {
    append_log_line(&crash_log_path(app)?, line)
}

fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn truncate_string_utf8(s: &mut String, max_bytes: usize) {
    if s.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
}

fn install_panic_hook(app: tauri::AppHandle) {
    std::panic::set_hook(Box::new(move |info| {
        let mut message = String::new();
        message.push_str(&format!("[{}] PANIC: {info}\n", now_timestamp()));
        let backtrace = std::backtrace::Backtrace::capture();
        message.push_str(&format!("{backtrace}\n"));
        let _ = append_crash_line(&app, &message);
        let _ = append_debug_line(&app, &message);
    }));
}

/// Build search index for all columns in background
fn build_search_index(
    path: &str,
    settings: &ParseSettings,
    offsets: &[u64],
    mmap: Option<&Mmap>,
    num_columns: usize,
) -> SearchIndex {
    let mut index = SearchIndex::new();
    if num_columns == 0 {
        return index;
    }

    // Initialize column indexes
    index.columns = (0..num_columns)
        .map(|_| Some(std::collections::HashMap::new()))
        .collect();

    let mut start = 0usize;
    let mut warnings = Vec::new();

    loop {
        let chunk: Vec<Vec<String>> = if let Some(mmap) = mmap {
            match read_chunk_with_offsets_mmap(
                mmap,
                offsets,
                start,
                BULK_CHUNK_SIZE,
                settings,
                Some(num_columns),
                &mut warnings,
            ) {
                Ok(c) => c,
                Err(_) => break,
            }
        } else {
            match read_chunk_with_offsets(
                path,
                offsets,
                start,
                BULK_CHUNK_SIZE,
                settings,
                Some(num_columns),
                &mut warnings,
            ) {
                Ok(c) => c,
                Err(_) => break,
            }
        };

        if chunk.is_empty() {
            break;
        }

        // Index each row
        for (idx, row) in chunk.iter().enumerate() {
            let row_index = (start + idx) as u32;

            for (col_idx, cell) in row.iter().enumerate() {
                if col_idx >= index.columns.len() {
                    continue;
                }

                // Skip columns already marked as too high cardinality
                if index.columns[col_idx].is_none() {
                    continue;
                }

                let col_index = index.columns[col_idx].as_mut().unwrap();

                // Create lowercase key, truncated for memory efficiency
                let key: Box<str> = if cell.len() > INDEX_VALUE_MAX_LEN {
                    truncate_utf8(cell, INDEX_VALUE_MAX_LEN)
                        .to_lowercase()
                        .into()
                } else {
                    cell.to_lowercase().into()
                };

                col_index
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push(row_index);

                // Check cardinality limit
                if col_index.len() > INDEX_MAX_CARDINALITY {
                    println!(
                        "[INDEX] Skipping column {} (too many unique values: {})",
                        col_idx,
                        col_index.len()
                    );
                    index.skipped_columns.push(col_idx);
                    index.columns[col_idx] = None;
                }
            }
        }

        if chunk.len() < BULK_CHUNK_SIZE {
            break;
        }
        start += chunk.len();
    }

    index.ready = true;
    index
}

fn initial_open_path() -> Option<String> {
    std::env::args_os().skip(1).find_map(|arg| {
        let path = std::path::PathBuf::from(arg);
        match path.extension().and_then(|ext| ext.to_str()) {
            Some(ext) if ext.eq_ignore_ascii_case("csv") => {
                Some(path.to_string_lossy().to_string())
            }
            _ => None,
        }
    })
}

#[tauri::command]
async fn load_csv_metadata(
    path: String,
    overrides: Option<ParseOverrides>,
    state: State<'_, AppState>,
    app: tauri::AppHandle,
) -> Result<CsvMetadata, String> {
    let detected = detect_parse_settings(&path).map_err(|err| err.to_string())?;
    let settings = apply_parse_overrides(&detected, overrides);
    let detected_settings = apply_parse_overrides(&detected, None);
    let detected_info = parse_info_from_settings(&detected_settings);
    let effective_info = parse_info_from_settings(&settings);

    let mut warnings = Vec::new();
    let headers = get_headers(&path, &settings, &mut warnings).map_err(|err| err.to_string())?;

    *state.file_path.lock().unwrap() = Some(path.clone());
    *state.total_rows.lock().unwrap() = 0;
    *state.headers.lock().unwrap() = headers.clone();
    *state.sorted_order.lock().unwrap() = None;
    *state.row_offsets.lock().unwrap() = None;
    *state.mmap.lock().unwrap() = None;
    *state.parse_settings.lock().unwrap() = settings.clone();
    *state.parse_info_detected.lock().unwrap() = detected_info.clone();
    *state.parse_info_effective.lock().unwrap() = effective_info.clone();
    *state.parse_warnings.lock().unwrap() = warnings.clone();
    state.search_index.lock().unwrap().clear();
    state.cache.clear();

    let expected_columns = if headers.is_empty() {
        None
    } else {
        Some(headers.len())
    };

    let mut estimated_count = None;
    if let Ok(file) = std::fs::File::open(&path) {
        if let Ok(meta) = file.metadata() {
            let len = meta.len();
            if len > 0 {
                use std::io::Read;
                let mut sample = vec![0u8; 64 * 1024];
                let mut take = file.take(sample.len() as u64);
                if let Ok(read) = take.read(&mut sample) {
                    if read > 0 {
                        let sample_slice = &sample[..read];
                        let newlines = sample_slice.iter().filter(|&&b| b == b'\n').count();
                        if newlines > 0 {
                            let avg_len = read as f64 / newlines as f64;
                            if avg_len > 0.0 {
                                estimated_count = Some((len as f64 / avg_len) as usize);
                            }
                        }
                    }
                }
            }
        }
    }

    tauri::async_runtime::spawn_blocking(move || {
        let cache_dir = match ensure_cache_dir(&app) {
            Ok(dir) => dir,
            Err(_) => return,
        };
        prune_cache_dir(&cache_dir);

        let settings_hash = settings_cache_hash(&settings);
        let key = match cache_key(&path, Some(settings_hash)) {
            Ok(key) => key,
            Err(_) => return,
        };
        let cache_path = offsets_cache_path(&cache_dir, key);

        let mmap = match open_mmap_if_large(&path) {
            Ok(mmap) => mmap,
            Err(_) => None,
        };

        let mut offset_warnings = Vec::new();
        let app_for_cb = app.clone();
        let progress_cb = move |rows: usize| {
            let _ = app_for_cb.emit("parse-progress", rows);
        };

        let offsets = match read_offsets_cache(&cache_path, key) {
            Ok(Some(offsets)) => {
                let _ = app.emit("parse-progress", offsets.len());
                offsets
            }
            _ => {
                let offsets = match mmap.as_deref() {
                    Some(mmap) => match build_row_offsets_mmap(
                        &mmap[..],
                        &settings,
                        expected_columns,
                        &mut offset_warnings,
                        Some(&progress_cb),
                    ) {
                        Ok(offsets) => offsets,
                        Err(_) => return,
                    },
                    None => match build_row_offsets(
                        &path,
                        &settings,
                        expected_columns,
                        &mut offset_warnings,
                        Some(&progress_cb),
                    ) {
                        Ok(offsets) => offsets,
                        Err(_) => return,
                    },
                };
                let _ = write_offsets_cache(&cache_path, key, &offsets);
                offsets
            }
        };

        let count = offsets.len();
        let state = app.state::<AppState>();
        let current_path = state.file_path.lock().unwrap().clone();
        if current_path.as_deref() != Some(path.as_str()) {
            return;
        }
        if !offset_warnings.is_empty() {
            let mut warnings = state.parse_warnings.lock().unwrap();
            warnings.extend(offset_warnings);
            warnings.truncate(MAX_WARNING_COUNT);
        }
        *state.row_offsets.lock().unwrap() = Some(offsets.clone());
        *state.total_rows.lock().unwrap() = count;
        *state.mmap.lock().unwrap() = mmap.clone();
        let _ = app.emit("row-count", count);

        // Build search index in background
        let enable_indexing = *state.enable_indexing.lock().unwrap();
        if enable_indexing {
            let num_columns = state.headers.lock().unwrap().len();
            println!(
                "[INDEX] Starting index build: path={}, num_columns={}, offsets_len={}",
                path,
                num_columns,
                offsets.len()
            );
            let index_path = path.clone();
            let index_settings = settings.clone();
            let index_offsets = offsets;
            let index_mmap = mmap;
            let app_for_index = app.clone();

            std::thread::spawn(move || {
                println!("[INDEX] Thread started, building index...");
                let index = if let Some(ref mmap) = index_mmap {
                    build_search_index(
                        &index_path,
                        &index_settings,
                        &index_offsets,
                        Some(mmap.as_ref()),
                        num_columns,
                    )
                } else {
                    build_search_index(
                        &index_path,
                        &index_settings,
                        &index_offsets,
                        None,
                        num_columns,
                    )
                };
                println!(
                    "[INDEX] Index built: ready={}, columns={}",
                    index.ready,
                    index.columns.len()
                );

                let state = app_for_index.state::<AppState>();
                // Check if same file is still loaded
                let current_path = state.file_path.lock().unwrap().clone();
                if current_path.as_deref() == Some(index_path.as_str()) {
                    *state.search_index.lock().unwrap() = index;
                    println!("[INDEX] Index stored in state");
                    let _ = app_for_index.emit("index-ready", true);
                } else {
                    println!("[INDEX] File changed, discarding index");
                }
            });
        }
    });

    Ok(CsvMetadata {
        headers,
        detected: detected_info,
        effective: effective_info,
        warnings,
        estimated_count,
    })
}

#[tauri::command]
async fn get_csv_chunk(
    start: usize,
    count: usize,
    state: State<'_, AppState>,
) -> Result<Vec<Vec<String>>, String> {
    if let Some(cached) = state.cache.get(start, count) {
        return Ok(cached);
    }

    let path = state
        .file_path
        .lock()
        .unwrap()
        .clone()
        .ok_or("No file loaded")?;
    let settings = state.parse_settings.lock().unwrap().clone();
    let expected_columns = {
        let len = state.headers.lock().unwrap().len();
        if len == 0 {
            None
        } else {
            Some(len)
        }
    };
    let mut warnings = Vec::new();

    let mmap = state.mmap.lock().unwrap().clone();
    let offsets_guard = state.row_offsets.lock().unwrap();
    let data = match offsets_guard.as_ref() {
        Some(offsets) => {
            if let Some(mmap) = mmap.as_ref() {
                read_chunk_with_offsets_mmap(
                    &mmap[..],
                    offsets,
                    start,
                    count,
                    &settings,
                    expected_columns,
                    &mut warnings,
                )
                .map_err(|err| err.to_string())?
            } else {
                read_chunk_with_offsets(
                    &path,
                    offsets,
                    start,
                    count,
                    &settings,
                    expected_columns,
                    &mut warnings,
                )
                .map_err(|err| err.to_string())?
            }
        }
        None => {
            if let Some(mmap) = mmap.as_ref() {
                read_chunk_mmap(
                    &mmap[..],
                    start,
                    count,
                    &settings,
                    expected_columns,
                    &mut warnings,
                )
                .map_err(|err| err.to_string())?
            } else {
                read_chunk(
                    &path,
                    start,
                    count,
                    &settings,
                    expected_columns,
                    &mut warnings,
                )
                .map_err(|err| err.to_string())?
            }
        }
    };
    if !warnings.is_empty() {
        let mut stored = state.parse_warnings.lock().unwrap();
        stored.extend(warnings);
        stored.truncate(MAX_WARNING_COUNT);
    }
    state.cache.put(start, count, data.clone());

    Ok(data)
}

#[tauri::command]
async fn search_csv(
    column_idx: Option<usize>,
    query: String,
    match_case: Option<bool>,
    whole_word: Option<bool>,
    state: State<'_, AppState>,
) -> Result<Vec<usize>, String> {
    let path = state
        .file_path
        .lock()
        .unwrap()
        .clone()
        .ok_or("No file loaded")?;

    let match_case = match_case.unwrap_or(false);
    let whole_word = whole_word.unwrap_or(false);

    let query_processed = if match_case {
        query.clone()
    } else {
        query.to_lowercase()
    };

    let settings = state.parse_settings.lock().unwrap().clone();

    // Try index-based search first (for exact or contains matches)
    let search_index = state.search_index.lock().unwrap();
    println!(
        "[SEARCH] index.ready={} match_case={} column_idx={:?} columns_len={}",
        search_index.ready,
        match_case,
        column_idx,
        search_index.columns.len()
    );
    if search_index.ready && !match_case {
        if let Some(col_idx) = column_idx {
            // Search specific column using index
            if col_idx < search_index.columns.len() {
                let has_index = search_index.columns[col_idx].is_some();
                println!(
                    "[SEARCH] Using index for column {}, has_index={}",
                    col_idx, has_index
                );
                if let Some(ref col_index) = search_index.columns[col_idx] {
                    // Truncate query key like we did during indexing
                    let query_key: Box<str> = if query_processed.len() > INDEX_VALUE_MAX_LEN {
                        truncate_utf8(&query_processed, INDEX_VALUE_MAX_LEN).into()
                    } else {
                        query_processed.clone().into()
                    };

                    // For whole_word, do exact match
                    if whole_word {
                        if let Some(rows) = col_index.get(&query_key) {
                            let mut result: Vec<usize> = rows.iter().map(|&r| r as usize).collect();
                            result.sort_unstable();
                            return Ok(result);
                        } else {
                            return Ok(Vec::new());
                        }
                    } else {
                        // Contains search: parallel scan of index keys
                        let matches: Vec<usize> = col_index
                            .par_iter()
                            .filter(|(key, _)| key.contains(&*query_key))
                            .flat_map(|(_, rows)| rows.par_iter().map(|&r| r as usize))
                            .collect();
                        let mut matches: Vec<usize> = matches.into_iter().collect();
                        matches.par_sort_unstable();
                        matches.dedup();
                        return Ok(matches);
                    }
                }
            }
        }
    }
    drop(search_index); // Release lock before sequential scan

    let mmap = state.mmap.lock().unwrap().clone();
    let offsets_guard = state.row_offsets.lock().unwrap();
    if let Some(offsets) = offsets_guard.as_ref() {
        let total = offsets.len();
        let ranges = (0..total)
            .step_by(SEARCH_CHUNK_SIZE)
            .map(|start| (start, usize::min(start + SEARCH_CHUNK_SIZE, total)))
            .collect::<Vec<_>>();

        let mut matches = ranges
            .par_iter()
            .try_fold(Vec::new, |mut acc, (start, end)| {
                let mut found = if let Some(mmap) = mmap.as_ref() {
                    search_range_with_offsets_mmap(
                        &mmap[..],
                        offsets,
                        *start,
                        *end,
                        column_idx,
                        &query_processed,
                        match_case,
                        whole_word,
                        &settings,
                    )
                } else {
                    search_range_with_offsets(
                        &path,
                        offsets,
                        *start,
                        *end,
                        column_idx,
                        &query_processed,
                        match_case,
                        whole_word,
                        &settings,
                    )
                }
                .map_err(|err| err.to_string())?;
                acc.append(&mut found);
                Ok::<Vec<usize>, String>(acc)
            })
            .try_reduce(Vec::new, |mut left, mut right| {
                left.append(&mut right);
                Ok::<Vec<usize>, String>(left)
            })?;

        matches.sort_unstable();
        return Ok(matches);
    }
    drop(offsets_guard);

    let mut matches = Vec::new();
    let mut record = csv::ByteRecord::new();
    let mut idx: usize = 0;

    // Helper closure for matching logic
    let check_match = |cell: &str| -> bool {
        if !match_case {
            let val_lower = cell.to_lowercase();
            if whole_word {
                val_lower == query_processed
            } else {
                val_lower.contains(&query_processed)
            }
        } else {
            if whole_word {
                cell == query_processed
            } else {
                cell.contains(&query_processed)
            }
        }
    };

    if let Some(mmap) = mmap.as_ref() {
        let mut rdr = build_reader(&mmap[..], &settings, settings.has_headers);
        while rdr
            .read_byte_record(&mut record)
            .map_err(|err| err.to_string())?
        {
            let strip_bom = !settings.has_headers && idx == 0;
            let (decoded, _) = decode_record(&record, &settings, strip_bom);
            let is_match = match column_idx {
                Some(index) => decoded
                    .get(index)
                    .map(|cell| check_match(cell))
                    .unwrap_or(false),
                None => decoded.iter().any(|cell| check_match(cell)),
            };
            if is_match {
                matches.push(idx);
            }
            idx += 1;
        }
    } else {
        let file = std::fs::File::open(&path).map_err(|err| err.to_string())?;
        let reader = std::io::BufReader::new(file);
        let mut rdr = build_reader(reader, &settings, settings.has_headers);
        while rdr
            .read_byte_record(&mut record)
            .map_err(|err| err.to_string())?
        {
            let strip_bom = !settings.has_headers && idx == 0;
            let (decoded, _) = decode_record(&record, &settings, strip_bom);
            let is_match = match column_idx {
                Some(index) => decoded
                    .get(index)
                    .map(|cell| check_match(cell))
                    .unwrap_or(false),
                None => decoded.iter().any(|cell| check_match(cell)),
            };
            if is_match {
                matches.push(idx);
            }
            idx += 1;
        }
    }

    Ok(matches)
}

#[tauri::command]
async fn search_csv_stream(
    column_idx: Option<usize>,
    query: String,
    match_case: Option<bool>,
    whole_word: Option<bool>,
    request_id: u32,
    state: State<'_, AppState>,
    app: tauri::AppHandle,
) -> Result<(), String> {
    let path = state
        .file_path
        .lock()
        .unwrap()
        .clone()
        .ok_or("No file loaded")?;

    let match_case = match_case.unwrap_or(false);
    let whole_word = whole_word.unwrap_or(false);
    let query_processed = if match_case {
        query.clone()
    } else {
        query.to_lowercase()
    };

    let settings = state.parse_settings.lock().unwrap().clone();

    // Try index-based search first (for exact or contains matches)
    let search_index = state.search_index.lock().unwrap();
    if search_index.ready && !match_case {
        if let Some(col_idx) = column_idx {
            if col_idx < search_index.columns.len() {
                if let Some(ref col_index) = search_index.columns[col_idx] {
                    let query_key: Box<str> = if query_processed.len() > INDEX_VALUE_MAX_LEN {
                        truncate_utf8(&query_processed, INDEX_VALUE_MAX_LEN).into()
                    } else {
                        query_processed.clone().into()
                    };

                    if whole_word {
                        if let Some(rows) = col_index.get(&query_key) {
                            let matches: Vec<usize> = rows.iter().map(|&r| r as usize).collect();
                            let total = matches.len();
                            for chunk in matches.chunks(RESULT_CHUNK_SIZE) {
                                emit_matches_chunk(&app, "search-chunk", request_id, chunk)?;
                            }
                            emit_matches_complete(&app, "search-complete", request_id, total)?;
                            return Ok(());
                        } else {
                            emit_matches_complete(&app, "search-complete", request_id, 0)?;
                            return Ok(());
                        }
                    } else {
                        let matches: Vec<usize> = col_index
                            .par_iter()
                            .filter(|(key, _)| key.contains(&*query_key))
                            .flat_map(|(_, rows)| rows.par_iter().map(|&r| r as usize))
                            .collect();
                        let mut matches: Vec<usize> = matches.into_iter().collect();
                        matches.par_sort_unstable();
                        matches.dedup();
                        let total = matches.len();
                        for chunk in matches.chunks(RESULT_CHUNK_SIZE) {
                            emit_matches_chunk(&app, "search-chunk", request_id, chunk)?;
                        }
                        emit_matches_complete(&app, "search-complete", request_id, total)?;
                        return Ok(());
                    }
                }
            }
        }
    }
    drop(search_index); // Release lock before sequential scan

    let mmap = state.mmap.lock().unwrap().clone();
    let offsets_guard = state.row_offsets.lock().unwrap();
    if let Some(offsets) = offsets_guard.as_ref() {
        let total = offsets.len();
        let ranges = (0..total)
            .step_by(SEARCH_CHUNK_SIZE)
            .map(|start| (start, usize::min(start + SEARCH_CHUNK_SIZE, total)))
            .collect::<Vec<_>>();

        let mut matches = ranges
            .par_iter()
            .try_fold(Vec::new, |mut acc, (start, end)| {
                let mut found = if let Some(mmap) = mmap.as_ref() {
                    search_range_with_offsets_mmap(
                        &mmap[..],
                        offsets,
                        *start,
                        *end,
                        column_idx,
                        &query_processed,
                        match_case,
                        whole_word,
                        &settings,
                    )
                } else {
                    search_range_with_offsets(
                        &path,
                        offsets,
                        *start,
                        *end,
                        column_idx,
                        &query_processed,
                        match_case,
                        whole_word,
                        &settings,
                    )
                }
                .map_err(|err| err.to_string())?;
                acc.append(&mut found);
                Ok::<Vec<usize>, String>(acc)
            })
            .try_reduce(Vec::new, |mut left, mut right| {
                left.append(&mut right);
                Ok::<Vec<usize>, String>(left)
            })?;

        matches.sort_unstable();
        let total = matches.len();
        for chunk in matches.chunks(RESULT_CHUNK_SIZE) {
            emit_matches_chunk(&app, "search-chunk", request_id, chunk)?;
        }
        emit_matches_complete(&app, "search-complete", request_id, total)?;
        return Ok(());
    }
    drop(offsets_guard);

    let mut matches = Vec::new();
    let mut total = 0usize;
    let mut record = csv::ByteRecord::new();
    let mut idx: usize = 0;

    let check_match = |cell: &str| -> bool {
        if !match_case {
            let val_lower = cell.to_lowercase();
            if whole_word {
                val_lower == query_processed
            } else {
                val_lower.contains(&query_processed)
            }
        } else {
            if whole_word {
                cell == query_processed
            } else {
                cell.contains(&query_processed)
            }
        }
    };

    if let Some(mmap) = mmap.as_ref() {
        let mut rdr = build_reader(&mmap[..], &settings, settings.has_headers);
        while rdr
            .read_byte_record(&mut record)
            .map_err(|err| err.to_string())?
        {
            let strip_bom = !settings.has_headers && idx == 0;
            let (decoded, _) = decode_record(&record, &settings, strip_bom);
            let is_match = match column_idx {
                Some(index) => decoded
                    .get(index)
                    .map(|cell| check_match(cell))
                    .unwrap_or(false),
                None => decoded.iter().any(|cell| check_match(cell)),
            };
            if is_match {
                matches.push(idx);
                if matches.len() >= RESULT_CHUNK_SIZE {
                    total += matches.len();
                    emit_matches_chunk(&app, "search-chunk", request_id, &matches)?;
                    matches.clear();
                }
            }
            idx += 1;
        }
    } else {
        let file = std::fs::File::open(&path).map_err(|err| err.to_string())?;
        let reader = std::io::BufReader::new(file);
        let mut rdr = build_reader(reader, &settings, settings.has_headers);
        while rdr
            .read_byte_record(&mut record)
            .map_err(|err| err.to_string())?
        {
            let strip_bom = !settings.has_headers && idx == 0;
            let (decoded, _) = decode_record(&record, &settings, strip_bom);
            let is_match = match column_idx {
                Some(index) => decoded
                    .get(index)
                    .map(|cell| check_match(cell))
                    .unwrap_or(false),
                None => decoded.iter().any(|cell| check_match(cell)),
            };
            if is_match {
                matches.push(idx);
                if matches.len() >= RESULT_CHUNK_SIZE {
                    total += matches.len();
                    emit_matches_chunk(&app, "search-chunk", request_id, &matches)?;
                    matches.clear();
                }
            }
            idx += 1;
        }
    }

    total += matches.len();
    emit_matches_chunk(&app, "search-chunk", request_id, &matches)?;
    emit_matches_complete(&app, "search-complete", request_id, total)?;
    Ok(())
}

#[tauri::command]
async fn find_duplicates(
    column_idx: Option<usize>,
    state: State<'_, AppState>,
) -> Result<Vec<usize>, String> {
    let path = state
        .file_path
        .lock()
        .unwrap()
        .clone()
        .ok_or("No file loaded")?;
    let settings = state.parse_settings.lock().unwrap().clone();

    let mmap = state.mmap.lock().unwrap().clone();
    let offsets = state.row_offsets.lock().unwrap().clone();
    // Offsets are required for random access verification
    let offsets = offsets.ok_or("File not fully indexed yet")?;

    // Use hashed approach for memory efficiency
    let duplicates = if let Some(mmap) = mmap.as_ref() {
        csv_handler::find_duplicates_hashed_mmap(&mmap[..], &offsets, &settings, column_idx)
            .map_err(|err| err.to_string())?
    } else {
        csv_handler::find_duplicates_hashed(&path, &offsets, &settings, column_idx)
            .map_err(|err| err.to_string())?
    };

    Ok(duplicates)
}

#[tauri::command]
async fn find_duplicates_stream(
    column_idx: Option<usize>,
    request_id: u32,
    state: State<'_, AppState>,
    app: tauri::AppHandle,
) -> Result<(), String> {
    let path = state
        .file_path
        .lock()
        .unwrap()
        .clone()
        .ok_or("No file loaded")?;
    let settings = state.parse_settings.lock().unwrap().clone();

    let mmap = state.mmap.lock().unwrap().clone();
    let offsets = state.row_offsets.lock().unwrap().clone();
    let offsets = offsets.ok_or("File not fully indexed yet")?;

    let duplicates = if let Some(mmap) = mmap.as_ref() {
        csv_handler::find_duplicates_hashed_mmap(&mmap[..], &offsets, &settings, column_idx)
            .map_err(|err| err.to_string())?
    } else {
        csv_handler::find_duplicates_hashed(&path, &offsets, &settings, column_idx)
            .map_err(|err| err.to_string())?
    };

    let total = duplicates.len();
    for chunk in duplicates.chunks(RESULT_CHUNK_SIZE) {
        emit_matches_chunk(&app, "duplicates-chunk", request_id, chunk)?;
    }
    emit_matches_complete(&app, "duplicates-complete", request_id, total)?;
    Ok(())
}

#[tauri::command]
async fn sort_csv(
    column_idx: usize,
    ascending: bool,
    state: State<'_, AppState>,
    app: tauri::AppHandle,
) -> Result<Vec<usize>, String> {
    let path = state
        .file_path
        .lock()
        .unwrap()
        .clone()
        .ok_or("No file loaded")?;
    let settings = state.parse_settings.lock().unwrap().clone();
    let started = std::time::Instant::now();
    let debug_enabled = *state.debug_logging.lock().unwrap();
    if debug_enabled {
        let _ = append_debug_line(
            &app,
            &format!(
                "[{}] INFO sort_csv start column_idx={column_idx} ascending={ascending} path={path}",
                now_timestamp()
            ),
        );
    }

    let cache_dir = ensure_cache_dir(&app)?;
    let settings_hash = settings_cache_hash(&settings);
    let key = cache_key(&path, Some(settings_hash))?;
    let order_path = order_cache_path(&cache_dir, key, column_idx, ascending);
    if let Ok(Some(order)) = read_order_cache(&order_path, key, column_idx, ascending) {
        *state.sorted_order.lock().unwrap() = Some(order.clone());
        if debug_enabled {
            let _ = append_debug_line(
                &app,
                &format!(
                    "[{}] INFO sort_csv cache_hit len={} ms={}",
                    now_timestamp(),
                    order.len(),
                    started.elapsed().as_millis()
                ),
            );
        }
        return Ok(order);
    }
    if !ascending {
        let asc_path = order_cache_path(&cache_dir, key, column_idx, true);
        if let Ok(Some(mut order)) = read_order_cache(&asc_path, key, column_idx, true) {
            order.reverse();
            *state.sorted_order.lock().unwrap() = Some(order.clone());
            if debug_enabled {
                let _ = append_debug_line(
                    &app,
                    &format!(
                        "[{}] INFO sort_csv cache_hit(reversed) len={} ms={}",
                        now_timestamp(),
                        order.len(),
                        started.elapsed().as_millis()
                    ),
                );
            }
            return Ok(order);
        }
    }

    let mmap = state.mmap.lock().unwrap().clone();
    let offsets = state.row_offsets.lock().unwrap().clone();
    let expected_columns = {
        let len = state.headers.lock().unwrap().len();
        if len == 0 {
            None
        } else {
            Some(len)
        }
    };
    // Memory optimization: truncate values to 256 chars max
    // Most sort comparisons differ in first few chars anyway
    const SORT_VALUE_MAX_LEN: usize = 256;
    let mut warnings = Vec::new();
    let mut rows: Vec<(u32, Box<str>)> =
        Vec::with_capacity(offsets.as_ref().map(|o| o.len()).unwrap_or(100_000));
    let mut start = 0usize;

    loop {
        let chunk = match offsets.as_ref() {
            Some(offsets) => {
                if let Some(mmap) = mmap.as_ref() {
                    read_chunk_with_offsets_mmap(
                        &mmap[..],
                        offsets,
                        start,
                        BULK_CHUNK_SIZE,
                        &settings,
                        expected_columns,
                        &mut warnings,
                    )
                    .map_err(|err| err.to_string())?
                } else {
                    read_chunk_with_offsets(
                        &path,
                        offsets,
                        start,
                        BULK_CHUNK_SIZE,
                        &settings,
                        expected_columns,
                        &mut warnings,
                    )
                    .map_err(|err| err.to_string())?
                }
            }
            None => {
                if let Some(mmap) = mmap.as_ref() {
                    read_chunk_mmap(
                        &mmap[..],
                        start,
                        BULK_CHUNK_SIZE,
                        &settings,
                        expected_columns,
                        &mut warnings,
                    )
                    .map_err(|err| err.to_string())?
                } else {
                    read_chunk(
                        &path,
                        start,
                        BULK_CHUNK_SIZE,
                        &settings,
                        expected_columns,
                        &mut warnings,
                    )
                    .map_err(|err| err.to_string())?
                }
            }
        };

        if chunk.is_empty() {
            break;
        }

        for (idx, row) in chunk.iter().enumerate() {
            let row_index = (start + idx) as u32;
            let value = row
                .get(column_idx)
                .map(|s| {
                    // Truncate to reduce memory usage
                    if s.len() > SORT_VALUE_MAX_LEN {
                        truncate_utf8(s, SORT_VALUE_MAX_LEN).into()
                    } else {
                        s.as_str().into()
                    }
                })
                .unwrap_or_else(|| "".into());
            rows.push((row_index, value));
        }

        if chunk.len() < BULK_CHUNK_SIZE {
            break;
        }
        start += chunk.len();
    }

    if !warnings.is_empty() {
        let mut stored = state.parse_warnings.lock().unwrap();
        stored.extend(warnings);
        stored.truncate(MAX_WARNING_COUNT);
    }

    if ascending {
        rows.par_sort_unstable_by(|a, b| a.1.cmp(&b.1));
    } else {
        rows.par_sort_unstable_by(|a, b| b.1.cmp(&a.1));
    }

    // Convert u32 indices back to usize
    let order: Vec<usize> = rows.iter().map(|(idx, _)| *idx as usize).collect();
    *state.sorted_order.lock().unwrap() = Some(order.clone());
    let _ = write_order_cache(&order_path, key, column_idx, ascending, &order);
    if debug_enabled {
        let _ = append_debug_line(
            &app,
            &format!(
                "[{}] INFO sort_csv done len={} ms={}",
                now_timestamp(),
                order.len(),
                started.elapsed().as_millis()
            ),
        );
    }

    Ok(order)
}

#[tauri::command]
async fn get_sorted_chunk(
    start: usize,
    count: usize,
    state: State<'_, AppState>,
) -> Result<Vec<SortedRow>, String> {
    let sorted = state.sorted_order.lock().unwrap();
    let order = sorted.as_ref().ok_or("No sorted data")?;
    if start >= order.len() {
        return Ok(Vec::new());
    }
    let end = usize::min(start + count, order.len());
    let slice = &order[start..end];
    let path = state
        .file_path
        .lock()
        .unwrap()
        .clone()
        .ok_or("No file loaded")?;
    let settings = state.parse_settings.lock().unwrap().clone();
    let expected_columns = {
        let len = state.headers.lock().unwrap().len();
        if len == 0 {
            None
        } else {
            Some(len)
        }
    };
    let mut warnings = Vec::new();
    let mmap = state.mmap.lock().unwrap().clone();
    let offsets_guard = state.row_offsets.lock().unwrap();
    let offsets = offsets_guard.as_ref().ok_or("Row index not ready")?;
    let rows = if let Some(mmap) = mmap.as_ref() {
        read_rows_by_index_mmap(
            &mmap[..],
            offsets,
            slice,
            &settings,
            expected_columns,
            &mut warnings,
        )
        .map_err(|err| err.to_string())?
    } else {
        read_rows_by_index(
            &path,
            offsets,
            slice,
            &settings,
            expected_columns,
            &mut warnings,
        )
        .map_err(|err| err.to_string())?
    };
    if !warnings.is_empty() {
        let mut stored = state.parse_warnings.lock().unwrap();
        stored.extend(warnings);
        stored.truncate(MAX_WARNING_COUNT);
    }
    let sorted_rows = slice
        .iter()
        .zip(rows.into_iter())
        .map(|(idx, row)| SortedRow { index: *idx, row })
        .collect::<Vec<_>>();
    Ok(sorted_rows)
}

#[tauri::command]
async fn get_sorted_indices(
    start: usize,
    count: usize,
    state: State<'_, AppState>,
) -> Result<Vec<usize>, String> {
    let sorted = state.sorted_order.lock().unwrap();
    let order = sorted.as_ref().ok_or("No sorted data")?;
    if start >= order.len() {
        return Ok(Vec::new());
    }
    let end = usize::min(start + count, order.len());
    Ok(order[start..end].to_vec())
}

#[tauri::command]
async fn clear_sort(state: State<'_, AppState>) -> Result<(), String> {
    *state.sorted_order.lock().unwrap() = None;
    Ok(())
}

#[tauri::command]
async fn take_pending_open(state: State<'_, AppState>) -> Result<Option<String>, String> {
    Ok(state.pending_open.lock().unwrap().take())
}

#[tauri::command]
async fn get_row_count(state: State<'_, AppState>) -> Result<usize, String> {
    Ok(*state.total_rows.lock().unwrap())
}

#[tauri::command]
async fn get_parse_warnings(
    clear: bool,
    state: State<'_, AppState>,
) -> Result<Vec<ParseWarning>, String> {
    let mut warnings = state.parse_warnings.lock().unwrap();
    let output = warnings.clone();
    if clear {
        warnings.clear();
    }
    Ok(output)
}

#[tauri::command]
async fn get_debug_log_path(app: tauri::AppHandle) -> Result<String, String> {
    let path = debug_log_path(&app)?;
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path);
    Ok(path.display().to_string())
}

#[tauri::command]
async fn get_crash_log_path(app: tauri::AppHandle) -> Result<String, String> {
    let path = crash_log_path(&app)?;
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path);
    Ok(path.display().to_string())
}

#[tauri::command]
async fn clear_debug_log(app: tauri::AppHandle) -> Result<(), String> {
    std::fs::write(debug_log_path(&app)?, b"").map_err(|err| err.to_string())
}

#[tauri::command]
async fn set_debug_logging(
    enabled: bool,
    state: State<'_, AppState>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    *state.debug_logging.lock().unwrap() = enabled;
    let path = debug_log_path(&app)?;
    let _ = append_debug_line(
        &app,
        &format!("[{}] INFO debug_logging={enabled}", now_timestamp()),
    );
    Ok(path.display().to_string())
}

#[tauri::command]
async fn append_debug_log(
    message: String,
    state: State<'_, AppState>,
    app: tauri::AppHandle,
) -> Result<(), String> {
    if !*state.debug_logging.lock().unwrap() {
        return Ok(());
    }
    let mut msg = message.replace('\n', "\\n");
    truncate_string_utf8(&mut msg, 8_000);
    append_debug_line(&app, &format!("[{}] WEB {msg}", now_timestamp()))
}

#[tauri::command]
async fn write_csv_file(path: String, contents: String) -> Result<(), String> {
    std::fs::write(path, contents).map_err(|err| err.to_string())
}

#[tauri::command]
async fn set_show_index_checked(checked: bool, app: tauri::AppHandle) -> Result<(), String> {
    if let Some(menu) = app.menu() {
        if let Some(item) = menu.get("view") {
            if let MenuItemKind::Submenu(submenu) = item {
                if let Some(item) = submenu.get("show-index") {
                    if let MenuItemKind::Check(check) = item {
                        check.set_checked(checked).map_err(|err| err.to_string())?;
                    }
                }
            }
        }
    }
    Ok(())
}

#[tauri::command]
async fn set_enable_indexing(enabled: bool, state: State<'_, AppState>) -> Result<(), String> {
    *state.enable_indexing.lock().unwrap() = enabled;
    if !enabled {
        state.search_index.lock().unwrap().clear();
    }
    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let default_settings = default_parse_settings();
    let default_info = parse_info_from_settings(&default_settings);
    let builder = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            install_panic_hook(app.handle().clone());
            for window_config in app.config().app.windows.iter().filter(|w| !w.create) {
                WebviewWindowBuilder::from_config(app.handle(), window_config)?
                    .enable_clipboard_access()
                    .build()?;
            }
            Ok(())
        })
        .manage(AppState {
            file_path: Mutex::new(None),
            total_rows: Mutex::new(0),
            headers: Mutex::new(Vec::new()),
            cache: CsvCache::new(64),
            sorted_order: Mutex::new(None),
            pending_open: Mutex::new(initial_open_path()),
            row_offsets: Mutex::new(None),
            mmap: Mutex::new(None),
            parse_settings: Mutex::new(default_settings),
            parse_info_detected: Mutex::new(default_info.clone()),
            parse_info_effective: Mutex::new(default_info),
            parse_warnings: Mutex::new(Vec::new()),
            search_index: Mutex::new(SearchIndex::new()),
            enable_indexing: Mutex::new(true),
            debug_logging: Mutex::new(false),
        })
        .invoke_handler(tauri::generate_handler![
            load_csv_metadata,
            get_csv_chunk,
            search_csv,
            search_csv_stream,
            find_duplicates,
            find_duplicates_stream,
            sort_csv,
            get_sorted_chunk,
            get_sorted_indices,
            clear_sort,
            take_pending_open,
            get_row_count,
            get_parse_warnings,
            get_debug_log_path,
            get_crash_log_path,
            set_debug_logging,
            append_debug_log,
            clear_debug_log,
            write_csv_file,
            set_show_index_checked,
            set_enable_indexing
        ]);

    #[cfg(desktop)]
    let builder = builder
        .menu(build_menu)
        .on_menu_event(|app, event| match event.id().as_ref() {
            "open" => {
                let _ = app.emit("menu-open", ());
            }
            "save" => {
                let _ = app.emit("menu-save", ());
            }
            "save-as" => {
                let _ = app.emit("menu-save-as", ());
            }
            "clear" => {
                let _ = app.emit("menu-clear", ());
            }
            "open-settings" => {
                let _ = app.emit("open-settings", ());
            }
            "find" => {
                let _ = app.emit("menu-find", ());
            }
            "clear-search" => {
                let _ = app.emit("menu-clear-search", ());
            }
            "check-duplicates" => {
                let _ = app.emit("menu-check-duplicates", ());
            }
            "next-match" => {
                let _ = app.emit("menu-next-match", ());
            }
            "prev-match" => {
                let _ = app.emit("menu-prev-match", ());
            }
            "close-find" => {
                let _ = app.emit("menu-close-find", ());
            }
            "toggle-theme" => {
                let _ = app.emit("menu-toggle-theme", ());
            }
            "parse-settings" => {
                let _ = app.emit("menu-parse-settings", ());
            }
            "show-index" => {
                if let Some(menu) = app.menu() {
                    if let Some(item) = menu.get("view") {
                        if let MenuItemKind::Submenu(submenu) = item {
                            if let Some(item) = submenu.get("show-index") {
                                if let MenuItemKind::Check(check) = item {
                                    let checked = check.is_checked().unwrap_or(false);
                                    let _ = app.emit("menu-show-index", checked);
                                }
                            }
                        }
                    }
                }
            }
            "row-compact" => {
                let _ = app.emit("menu-row-height", 28);
            }
            "row-default" => {
                let _ = app.emit("menu-row-height", 36);
            }
            "row-spacious" => {
                let _ = app.emit("menu-row-height", 44);
            }
            "reload" => {
                if let Some(window) = app.get_webview_window("main") {
                    let _ = window.eval("window.location.reload()");
                }
            }
            _ => {}
        });

    builder
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(desktop)]
fn build_menu<R: tauri::Runtime>(app: &tauri::AppHandle<R>) -> tauri::Result<tauri::menu::Menu<R>> {
    let open_item = MenuItemBuilder::with_id("open", "Open...")
        .accelerator("CmdOrCtrl+O")
        .build(app)?;
    let save_item = MenuItemBuilder::with_id("save", "Save")
        .accelerator("CmdOrCtrl+S")
        .build(app)?;
    let save_as_item = MenuItemBuilder::with_id("save-as", "Save As...")
        .accelerator("CmdOrCtrl+Shift+S")
        .build(app)?;
    let clear_item = MenuItemBuilder::with_id("clear", "Clear")
        .accelerator("CmdOrCtrl+Shift+K")
        .build(app)?;
    let find_item = MenuItemBuilder::with_id("find", "Find...")
        .accelerator("CmdOrCtrl+F")
        .build(app)?;
    let clear_search_item = MenuItemBuilder::with_id("clear-search", "Clear Search")
        .accelerator("CmdOrCtrl+Shift+F")
        .build(app)?;
    let next_match_item = MenuItemBuilder::with_id("next-match", "Next Match")
        .accelerator("F3")
        .build(app)?;
    let prev_match_item = MenuItemBuilder::with_id("prev-match", "Previous Match")
        .accelerator("Shift+F3")
        .build(app)?;
    let close_find_item = MenuItemBuilder::with_id("close-find", "Close Find")
        .accelerator("Esc")
        .build(app)?;
    let reload_item = MenuItemBuilder::with_id("reload", "Reload")
        .accelerator("CmdOrCtrl+R")
        .build(app)?;
    let toggle_theme_item = MenuItemBuilder::with_id("toggle-theme", "Toggle Theme")
        .accelerator("CmdOrCtrl+Shift+T")
        .build(app)?;
    let show_index_item = CheckMenuItemBuilder::with_id("show-index", "Show Line Numbers")
        .accelerator("CmdOrCtrl+I")
        .checked(false)
        .build(app)?;
    let row_compact_item = MenuItemBuilder::with_id("row-compact", "Compact")
        .accelerator("CmdOrCtrl+Alt+1")
        .build(app)?;
    let row_default_item = MenuItemBuilder::with_id("row-default", "Default")
        .accelerator("CmdOrCtrl+Alt+2")
        .build(app)?;
    let row_spacious_item = MenuItemBuilder::with_id("row-spacious", "Spacious")
        .accelerator("CmdOrCtrl+Alt+3")
        .build(app)?;
    let row_height_menu = SubmenuBuilder::new(app, "Row Height")
        .item(&row_compact_item)
        .item(&row_default_item)
        .item(&row_spacious_item)
        .build()?;
    let check_duplicates_item = MenuItemBuilder::with_id("check-duplicates", "Check Duplicates...")
        .accelerator("CmdOrCtrl+Shift+D")
        .build(app)?;
    let parse_settings_item = MenuItemBuilder::with_id("parse-settings", "Parse Settings...")
        .accelerator("CmdOrCtrl+Shift+P")
        .build(app)?;

    let file_menu = SubmenuBuilder::new(app, "File")
        .item(&open_item)
        .item(&save_item)
        .item(&save_as_item)
        .separator()
        .item(&clear_item)
        .separator()
        .close_window()
        .quit()
        .build()?;
    let settings_item = MenuItemBuilder::with_id("open-settings", "Open Settings...")
        .accelerator("CmdOrCtrl+,")
        .build(app)?;
    let edit_menu = SubmenuBuilder::new(app, "Settings")
        .item(&settings_item)
        .build()?;

    let view_menu = SubmenuBuilder::with_id(app, "view", "View")
        .item(&reload_item)
        .item(&toggle_theme_item)
        .separator()
        .item(&show_index_item)
        .item(&row_height_menu)
        .build()?;
    let search_menu = SubmenuBuilder::new(app, "Search")
        .item(&find_item)
        .item(&clear_search_item)
        .separator()
        .item(&next_match_item)
        .item(&prev_match_item)
        .item(&close_find_item)
        .build()?;
    let tools_menu = SubmenuBuilder::new(app, "Tools")
        .item(&check_duplicates_item)
        .item(&parse_settings_item)
        .build()?;
    let shortcuts_open = MenuItemBuilder::new("Open File")
        .accelerator("CmdOrCtrl+O")
        .enabled(false)
        .build(app)?;
    let shortcuts_clear = MenuItemBuilder::new("Clear File")
        .accelerator("CmdOrCtrl+Shift+K")
        .enabled(false)
        .build(app)?;
    let shortcuts_find = MenuItemBuilder::new("Find")
        .accelerator("CmdOrCtrl+F")
        .enabled(false)
        .build(app)?;
    let shortcuts_clear_search = MenuItemBuilder::new("Clear Search")
        .accelerator("CmdOrCtrl+Shift+F")
        .enabled(false)
        .build(app)?;
    let shortcuts_reload = MenuItemBuilder::new("Reload")
        .accelerator("CmdOrCtrl+R")
        .enabled(false)
        .build(app)?;
    let shortcuts_toggle_theme = MenuItemBuilder::new("Toggle Theme")
        .accelerator("CmdOrCtrl+Shift+T")
        .enabled(false)
        .build(app)?;
    let shortcuts_show_index = MenuItemBuilder::new("Show Line Numbers")
        .accelerator("CmdOrCtrl+I")
        .enabled(false)
        .build(app)?;
    let shortcuts_row_compact = MenuItemBuilder::new("Row Height: Compact")
        .accelerator("CmdOrCtrl+Alt+1")
        .enabled(false)
        .build(app)?;
    let shortcuts_row_default = MenuItemBuilder::new("Row Height: Default")
        .accelerator("CmdOrCtrl+Alt+2")
        .enabled(false)
        .build(app)?;
    let shortcuts_row_spacious = MenuItemBuilder::new("Row Height: Spacious")
        .accelerator("CmdOrCtrl+Alt+3")
        .enabled(false)
        .build(app)?;
    let shortcuts_check_duplicates = MenuItemBuilder::new("Check Duplicates")
        .accelerator("CmdOrCtrl+Shift+D")
        .enabled(false)
        .build(app)?;
    let shortcuts_parse_settings = MenuItemBuilder::new("Parse Settings")
        .accelerator("CmdOrCtrl+Shift+P")
        .enabled(false)
        .build(app)?;
    let shortcuts_settings = MenuItemBuilder::new("Open Settings")
        .accelerator("CmdOrCtrl+,")
        .enabled(false)
        .build(app)?;
    let shortcuts_next = MenuItemBuilder::new("Next Match")
        .accelerator("F3")
        .enabled(false)
        .build(app)?;
    let shortcuts_prev = MenuItemBuilder::new("Previous Match")
        .accelerator("Shift+F3")
        .enabled(false)
        .build(app)?;
    let shortcuts_enter = MenuItemBuilder::new("Find: Enter = Next")
        .enabled(false)
        .build(app)?;
    let shortcuts_shift_enter = MenuItemBuilder::new("Find: Shift+Enter = Previous")
        .enabled(false)
        .build(app)?;
    let shortcuts_esc = MenuItemBuilder::new("Find: Esc = Close")
        .enabled(false)
        .build(app)?;
    let shortcuts_menu = SubmenuBuilder::new(app, "Shortcuts")
        .item(&shortcuts_open)
        .item(&shortcuts_clear)
        .separator()
        .item(&shortcuts_find)
        .item(&shortcuts_clear_search)
        .item(&shortcuts_next)
        .item(&shortcuts_prev)
        .separator()
        .item(&shortcuts_enter)
        .item(&shortcuts_shift_enter)
        .item(&shortcuts_esc)
        .separator()
        .item(&shortcuts_reload)
        .item(&shortcuts_toggle_theme)
        .item(&shortcuts_show_index)
        .item(&shortcuts_row_compact)
        .item(&shortcuts_row_default)
        .item(&shortcuts_row_spacious)
        .separator()
        .item(&shortcuts_check_duplicates)
        .item(&shortcuts_parse_settings)
        .separator()
        .item(&shortcuts_settings)
        .build()?;
    let package = app.package_info();
    let authors = package
        .authors
        .split(',')
        .map(|author| author.trim().to_string())
        .filter(|author| !author.is_empty())
        .collect::<Vec<_>>();
    let authors = if authors.is_empty() {
        None
    } else {
        Some(authors)
    };
    let about = tauri::menu::AboutMetadataBuilder::new()
        .name(Some(package.name.clone()))
        .version(Some(package.version.to_string()))
        .short_version(Some(package.version.to_string()))
        .authors(authors)
        .comments(Some(package.description.to_string()))
        .credits(Some("Built with Tauri, Rust, and React.".to_string()))
        .icon(app.default_window_icon().cloned())
        .build();
    let help_menu = SubmenuBuilder::new(app, "Help")
        .about(Some(about))
        .build()?;

    MenuBuilder::new(app)
        .item(&file_menu)
        .item(&edit_menu)
        .item(&view_menu)
        .item(&search_menu)
        .item(&tools_menu)
        .item(&shortcuts_menu)
        .item(&help_menu)
        .build()
}
