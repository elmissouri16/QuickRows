mod csv_cache;
mod csv_handler;
mod csv_ops;

use csv_cache::CsvCache;
use csv_handler::{count_rows_fast, get_headers, read_chunk};
use csv_ops::search_parallel;
use std::sync::Mutex;
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
    sorted_rows: Mutex<Option<Vec<SortedRow>>>,
    pending_open: Mutex<Option<String>>,
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
    state: State<'_, AppState>,
) -> Result<(Vec<String>, usize), String> {
    let headers = get_headers(&path).map_err(|err| err.to_string())?;
    let count = count_rows_fast(&path).map_err(|err| err.to_string())?;

    *state.file_path.lock().unwrap() = Some(path);
    *state.total_rows.lock().unwrap() = count;
    *state.headers.lock().unwrap() = headers.clone();
    *state.sorted_rows.lock().unwrap() = None;
    state.cache.clear();

    Ok((headers, count))
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

    let data = read_chunk(&path, start, count).map_err(|err| err.to_string())?;
    state.cache.put(start, count, data.clone());

    Ok(data)
}

#[tauri::command]
async fn search_csv(
    column_idx: usize,
    query: String,
    state: State<'_, AppState>,
) -> Result<Vec<usize>, String> {
    let path = state
        .file_path
        .lock()
        .unwrap()
        .clone()
        .ok_or("No file loaded")?;

    let total = *state.total_rows.lock().unwrap();
    let data = read_chunk(&path, 0, total).map_err(|err| err.to_string())?;
    Ok(search_parallel(&data, column_idx, &query))
}

#[tauri::command]
async fn sort_csv(
    column_idx: usize,
    ascending: bool,
    state: State<'_, AppState>,
) -> Result<Vec<usize>, String> {
    let path = state
        .file_path
        .lock()
        .unwrap()
        .clone()
        .ok_or("No file loaded")?;

    let total = *state.total_rows.lock().unwrap();
    let data = read_chunk(&path, 0, total).map_err(|err| err.to_string())?;
    let mut rows: Vec<(usize, Vec<String>)> = data.into_iter().enumerate().collect();

    rows.sort_by(|a, b| {
        let a_value = a.1.get(column_idx).map(String::as_str).unwrap_or("");
        let b_value = b.1.get(column_idx).map(String::as_str).unwrap_or("");
        let cmp = a_value.cmp(b_value);
        if ascending {
            cmp
        } else {
            cmp.reverse()
        }
    });

    let order = rows.iter().map(|(idx, _)| *idx).collect::<Vec<_>>();
    let sorted = rows
        .into_iter()
        .map(|(idx, row)| SortedRow { index: idx, row })
        .collect::<Vec<_>>();

    *state.sorted_rows.lock().unwrap() = Some(sorted);

    Ok(order)
}

#[tauri::command]
async fn get_sorted_chunk(
    start: usize,
    count: usize,
    state: State<'_, AppState>,
) -> Result<Vec<SortedRow>, String> {
    let sorted = state.sorted_rows.lock().unwrap();
    let rows = sorted.as_ref().ok_or("No sorted data")?;
    let end = usize::min(start + count, rows.len());
    Ok(rows[start..end].to_vec())
}

#[tauri::command]
async fn clear_sort(state: State<'_, AppState>) -> Result<(), String> {
    *state.sorted_rows.lock().unwrap() = None;
    Ok(())
}

#[tauri::command]
async fn take_pending_open(state: State<'_, AppState>) -> Result<Option<String>, String> {
    Ok(state.pending_open.lock().unwrap().take())
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
            sorted_rows: Mutex::new(None),
            pending_open: Mutex::new(initial_open_path()),
        })
        .invoke_handler(tauri::generate_handler![
            load_csv_metadata,
            get_csv_chunk,
            search_csv,
            sort_csv,
            get_sorted_chunk,
            clear_sort,
            take_pending_open,
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
            "next-match" => {
                let _ = app.emit("menu-next-match", ());
            }
            "prev-match" => {
                let _ = app.emit("menu-prev-match", ());
            }
            "close-find" => {
                let _ = app.emit("menu-close-find", ());
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
            "width-120" => {
                let _ = app.emit("menu-column-width", 120);
            }
            "width-160" => {
                let _ = app.emit("menu-column-width", 160);
            }
            "width-200" => {
                let _ = app.emit("menu-column-width", 200);
            }
            "width-240" => {
                let _ = app.emit("menu-column-width", 240);
            }
            "width-280" => {
                let _ = app.emit("menu-column-width", 280);
            }
            "width-320" => {
                let _ = app.emit("menu-column-width", 320);
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
    let no_tools = MenuItemBuilder::new("No tools available")
        .enabled(false)
        .build(app)?;
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
    let width_120 = MenuItemBuilder::with_id("width-120", "120 px").build(app)?;
    let width_160 = MenuItemBuilder::with_id("width-160", "160 px").build(app)?;
    let width_200 = MenuItemBuilder::with_id("width-200", "200 px").build(app)?;
    let width_240 = MenuItemBuilder::with_id("width-240", "240 px").build(app)?;
    let width_280 = MenuItemBuilder::with_id("width-280", "280 px").build(app)?;
    let width_320 = MenuItemBuilder::with_id("width-320", "320 px").build(app)?;

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
    let width_menu = SubmenuBuilder::new(app, "Column Width")
        .item(&width_120)
        .item(&width_160)
        .item(&width_200)
        .item(&width_240)
        .item(&width_280)
        .item(&width_320)
        .build()?;
    let view_menu = SubmenuBuilder::with_id(app, "view", "View")
        .item(&reload_item)
        .separator()
        .item(&row_menu)
        .item(&width_menu)
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
    let tools_menu = SubmenuBuilder::new(app, "Tools").item(&no_tools).build()?;
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
