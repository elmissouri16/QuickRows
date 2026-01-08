mod csv_cache;
mod csv_handler;
mod csv_mmap;
mod disk_cache;
use csv_cache::CsvCache;
use csv_handler::{
    build_row_offsets, build_row_offsets_mmap, get_headers, read_chunk, read_chunk_mmap,
    read_chunk_with_offsets, read_chunk_with_offsets_mmap, read_rows_by_index,
    read_rows_by_index_mmap, search_range_with_offsets, search_range_with_offsets_mmap,
};
use csv_mmap::open_mmap_if_large;
use disk_cache::{
    cache_key, ensure_cache_dir, offsets_cache_path, order_cache_path, prune_cache_dir,
    read_offsets_cache, read_order_cache, write_offsets_cache, write_order_cache,
};
use memmap2::Mmap;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tauri::{Emitter, Manager, State};

#[cfg(desktop)]
use tauri::menu::{CheckMenuItem, MenuBuilder, MenuItemBuilder, MenuItemKind, SubmenuBuilder};

#[derive(Clone, serde::Serialize)]
struct SortedRow {
    index: usize,
    row: Vec<String>,
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
}

const SEARCH_CHUNK_SIZE: usize = 25_000;

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
    state: State<'_, AppState>,
    app: tauri::AppHandle,
) -> Result<Vec<String>, String> {
    let headers = get_headers(&path).map_err(|err| err.to_string())?;

    *state.file_path.lock().unwrap() = Some(path.clone());
    *state.total_rows.lock().unwrap() = 0;
    *state.headers.lock().unwrap() = headers.clone();
    *state.sorted_order.lock().unwrap() = None;
    *state.row_offsets.lock().unwrap() = None;
    *state.mmap.lock().unwrap() = None;
    state.cache.clear();

    tauri::async_runtime::spawn_blocking(move || {
        let cache_dir = match ensure_cache_dir(&app) {
            Ok(dir) => dir,
            Err(_) => return,
        };
        prune_cache_dir(&cache_dir);

        let key = match cache_key(&path) {
            Ok(key) => key,
            Err(_) => return,
        };
        let cache_path = offsets_cache_path(&cache_dir, key);

        let mmap = match open_mmap_if_large(&path) {
            Ok(mmap) => mmap,
            Err(_) => None,
        };

        let offsets = match read_offsets_cache(&cache_path, key) {
            Ok(Some(offsets)) => offsets,
            _ => {
                let offsets = match mmap.as_deref() {
                    Some(mmap) => match build_row_offsets_mmap(&mmap[..]) {
                        Ok(offsets) => offsets,
                        Err(_) => return,
                    },
                    None => match build_row_offsets(&path) {
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
        *state.row_offsets.lock().unwrap() = Some(offsets);
        *state.total_rows.lock().unwrap() = count;
        *state.mmap.lock().unwrap() = mmap;
        let _ = app.emit("row-count", count);
    });

    Ok(headers)
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

    let mmap = state.mmap.lock().unwrap().clone();
    let offsets_guard = state.row_offsets.lock().unwrap();
    let data = match offsets_guard.as_ref() {
        Some(offsets) => {
            if let Some(mmap) = mmap.as_ref() {
                read_chunk_with_offsets_mmap(&mmap[..], offsets, start, count)
                    .map_err(|err| err.to_string())?
            } else {
                read_chunk_with_offsets(&path, offsets, start, count)
                    .map_err(|err| err.to_string())?
            }
        }
        None => {
            if let Some(mmap) = mmap.as_ref() {
                read_chunk_mmap(&mmap[..], start, count).map_err(|err| err.to_string())?
            } else {
                read_chunk(&path, start, count).map_err(|err| err.to_string())?
            }
        }
    };
    state.cache.put(start, count, data.clone());

    Ok(data)
}

#[tauri::command]
async fn search_csv(
    column_idx: Option<usize>,
    query: String,
    state: State<'_, AppState>,
) -> Result<Vec<usize>, String> {
    let path = state
        .file_path
        .lock()
        .unwrap()
        .clone()
        .ok_or("No file loaded")?;
    let query_lower = query.to_lowercase();

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
                        &query_lower,
                    )
                } else {
                    search_range_with_offsets(
                        &path,
                        offsets,
                        *start,
                        *end,
                        column_idx,
                        &query_lower,
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
    if let Some(mmap) = mmap.as_ref() {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(&mmap[..]);
        for (idx, result) in rdr.records().enumerate() {
            let record = result.map_err(|err| err.to_string())?;
            let is_match = match column_idx {
                Some(index) => record
                    .get(index)
                    .unwrap_or("")
                    .to_lowercase()
                    .contains(&query_lower),
                None => record
                    .iter()
                    .any(|cell| cell.to_lowercase().contains(&query_lower)),
            };
            if is_match {
                matches.push(idx);
            }
        }
    } else {
        let file = std::fs::File::open(&path).map_err(|err| err.to_string())?;
        let reader = std::io::BufReader::new(file);
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(reader);
        for (idx, result) in rdr.records().enumerate() {
            let record = result.map_err(|err| err.to_string())?;
            let is_match = match column_idx {
                Some(index) => record
                    .get(index)
                    .unwrap_or("")
                    .to_lowercase()
                    .contains(&query_lower),
                None => record
                    .iter()
                    .any(|cell| cell.to_lowercase().contains(&query_lower)),
            };
            if is_match {
                matches.push(idx);
            }
        }
    }

    Ok(matches)
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

    let mmap = state.mmap.lock().unwrap().clone();
    let mut seen: HashMap<String, usize> = HashMap::new();
    let mut duplicates: Vec<usize> = Vec::new();
    let mut duplicate_set: HashSet<usize> = HashSet::new();

    if let Some(mmap) = mmap.as_ref() {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(&mmap[..]);
        for (idx, result) in rdr.records().enumerate() {
            let record = result.map_err(|err| err.to_string())?;
            let key = match column_idx {
                Some(index) => record.get(index).unwrap_or("").to_string(),
                None => record.iter().collect::<Vec<_>>().join("\u{1f}"),
            };
            if let Some(first_idx) = seen.get(&key).copied() {
                if duplicate_set.insert(first_idx) {
                    duplicates.push(first_idx);
                }
                if duplicate_set.insert(idx) {
                    duplicates.push(idx);
                }
            } else {
                seen.insert(key, idx);
            }
        }
    } else {
        let file = std::fs::File::open(&path).map_err(|err| err.to_string())?;
        let reader = std::io::BufReader::new(file);
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(reader);
        for (idx, result) in rdr.records().enumerate() {
            let record = result.map_err(|err| err.to_string())?;
            let key = match column_idx {
                Some(index) => record.get(index).unwrap_or("").to_string(),
                None => record.iter().collect::<Vec<_>>().join("\u{1f}"),
            };
            if let Some(first_idx) = seen.get(&key).copied() {
                if duplicate_set.insert(first_idx) {
                    duplicates.push(first_idx);
                }
                if duplicate_set.insert(idx) {
                    duplicates.push(idx);
                }
            } else {
                seen.insert(key, idx);
            }
        }
    }

    duplicates.sort_unstable();
    Ok(duplicates)
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

    let cache_dir = ensure_cache_dir(&app)?;
    let key = cache_key(&path)?;
    let order_path = order_cache_path(&cache_dir, key, column_idx, ascending);
    if let Ok(Some(order)) = read_order_cache(&order_path, key, column_idx, ascending) {
        *state.sorted_order.lock().unwrap() = Some(order.clone());
        return Ok(order);
    }
    if !ascending {
        let asc_path = order_cache_path(&cache_dir, key, column_idx, true);
        if let Ok(Some(mut order)) = read_order_cache(&asc_path, key, column_idx, true) {
            order.reverse();
            *state.sorted_order.lock().unwrap() = Some(order.clone());
            return Ok(order);
        }
    }

    let mmap = state.mmap.lock().unwrap().clone();
    let mut rows: Vec<(usize, String)> = Vec::new();

    if let Some(mmap) = mmap.as_ref() {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(&mmap[..]);
        for (idx, result) in rdr.records().enumerate() {
            let record = result.map_err(|err| err.to_string())?;
            let value = record.get(column_idx).unwrap_or("").to_string();
            rows.push((idx, value));
        }
    } else {
        let file = std::fs::File::open(&path).map_err(|err| err.to_string())?;
        let reader = std::io::BufReader::new(file);
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(reader);
        for (idx, result) in rdr.records().enumerate() {
            let record = result.map_err(|err| err.to_string())?;
            let value = record.get(column_idx).unwrap_or("").to_string();
            rows.push((idx, value));
        }
    }

    if ascending {
        rows.par_sort_unstable_by(|a, b| a.1.cmp(&b.1));
    } else {
        rows.par_sort_unstable_by(|a, b| b.1.cmp(&a.1));
    }

    let order = rows.iter().map(|(idx, _)| *idx).collect::<Vec<_>>();
    *state.sorted_order.lock().unwrap() = Some(order.clone());
    let _ = write_order_cache(&order_path, key, column_idx, ascending, &order);

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
    let mmap = state.mmap.lock().unwrap().clone();
    let offsets_guard = state.row_offsets.lock().unwrap();
    let offsets = offsets_guard.as_ref().ok_or("Row index not ready")?;
    let rows = if let Some(mmap) = mmap.as_ref() {
        read_rows_by_index_mmap(&mmap[..], offsets, slice).map_err(|err| err.to_string())?
    } else {
        read_rows_by_index(&path, offsets, slice).map_err(|err| err.to_string())?
    };
    let sorted_rows = slice
        .iter()
        .zip(rows.into_iter())
        .map(|(idx, row)| SortedRow { index: *idx, row })
        .collect::<Vec<_>>();
    Ok(sorted_rows)
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

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let builder = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .manage(AppState {
            file_path: Mutex::new(None),
            total_rows: Mutex::new(0),
            headers: Mutex::new(Vec::new()),
            cache: CsvCache::new(64),
            sorted_order: Mutex::new(None),
            pending_open: Mutex::new(initial_open_path()),
            row_offsets: Mutex::new(None),
            mmap: Mutex::new(None),
        })
        .invoke_handler(tauri::generate_handler![
            load_csv_metadata,
            get_csv_chunk,
            search_csv,
            find_duplicates,
            sort_csv,
            get_sorted_chunk,
            clear_sort,
            take_pending_open,
            get_row_count,
            set_show_index_checked
        ]);

    #[cfg(desktop)]
    let builder = builder
        .menu(build_menu)
        .on_menu_event(|app, event| match event.id().as_ref() {
            "open" => {
                let _ = app.emit("menu-open", ());
            }
            "clear" => {
                let _ = app.emit("menu-clear", ());
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
    let clear_item = MenuItemBuilder::with_id("clear", "Clear").build(app)?;
    let find_item = MenuItemBuilder::with_id("find", "Find...")
        .accelerator("CmdOrCtrl+F")
        .build(app)?;
    let clear_search_item = MenuItemBuilder::with_id("clear-search", "Clear Search").build(app)?;
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
    let toggle_theme_item = MenuItemBuilder::with_id("toggle-theme", "Toggle Theme").build(app)?;
    let check_duplicates_item =
        MenuItemBuilder::with_id("check-duplicates", "Check Duplicates...").build(app)?;
    let show_index_item = CheckMenuItem::with_id(
        app,
        "show-index",
        "Show Row Numbers",
        true,
        false,
        None::<&str>,
    )?;
    let row_compact = MenuItemBuilder::with_id("row-compact", "Compact").build(app)?;
    let row_default = MenuItemBuilder::with_id("row-default", "Default").build(app)?;
    let row_spacious = MenuItemBuilder::with_id("row-spacious", "Spacious").build(app)?;

    let file_menu = SubmenuBuilder::new(app, "File")
        .item(&open_item)
        .item(&clear_item)
        .separator()
        .close_window()
        .quit()
        .build()?;
    let edit_menu = SubmenuBuilder::new(app, "Edit")
        .undo()
        .redo()
        .separator()
        .cut()
        .copy()
        .paste()
        .select_all()
        .build()?;
    let row_menu = SubmenuBuilder::new(app, "Row Height")
        .item(&row_compact)
        .item(&row_default)
        .item(&row_spacious)
        .build()?;
    let view_menu = SubmenuBuilder::with_id(app, "view", "View")
        .item(&reload_item)
        .separator()
        .item(&toggle_theme_item)
        .separator()
        .item(&row_menu)
        .separator()
        .item(&show_index_item)
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
        .build()?;
    let shortcuts_open = MenuItemBuilder::new("Open File")
        .accelerator("CmdOrCtrl+O")
        .enabled(false)
        .build(app)?;
    let shortcuts_find = MenuItemBuilder::new("Find")
        .accelerator("CmdOrCtrl+F")
        .enabled(false)
        .build(app)?;
    let shortcuts_reload = MenuItemBuilder::new("Reload")
        .accelerator("CmdOrCtrl+R")
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
        .item(&shortcuts_find)
        .item(&shortcuts_reload)
        .separator()
        .item(&shortcuts_next)
        .item(&shortcuts_prev)
        .separator()
        .item(&shortcuts_enter)
        .item(&shortcuts_shift_enter)
        .item(&shortcuts_esc)
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
