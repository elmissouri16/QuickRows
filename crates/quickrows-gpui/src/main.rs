#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod actions;
mod cell_selection;
mod selection;
#[cfg(test)]
mod state_tests;

use actions::*;
use cell_selection::CellSelection;
use directories::ProjectDirs;
use gpui::{
    App, Application, Axis, Bounds, ClickEvent, ClipboardItem, Context, FocusHandle, KeyBinding,
    KeyDownEvent, MouseButton, MouseDownEvent, MouseMoveEvent, MouseUpEvent, PathPromptOptions,
    Pixels, Point, ScrollHandle, ScrollStrategy, SharedString, StatefulInteractiveElement,
    UniformListScrollHandle, WeakEntity, Window, WindowBounds, WindowOptions, div, prelude::*, px,
    relative, size, uniform_list,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, IconName, Root, Theme, ThemeMode,
    button::{Button, ButtonVariants as _},
    h_flex,
    input::{Escape as InputEscape, Input, InputEvent, InputState, SelectAll as InputSelectAll},
    menu::{DropdownMenu as _, PopupMenuItem},
    scroll::{ScrollableElement as _, ScrollableMask, Scrollbar, ScrollbarShow},
    switch::Switch,
    v_flex,
};
use gpui_component_assets::Assets;
use quickrows_core::{
    AppSettings, CancellationToken, CsvDocument, CsvFragment, Diagnostics, ParseInfo,
    ParseOverrides, ParseWarning, ResolvedFragmentRegion, RowDensity, SettingsStore, SortDirection,
    SortSpec, ThemePreference,
};
use selection::RowSelection;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime};

const BASE_TITLE: &str = "QuickRows";
const MIN_COLUMN_WIDTH: f32 = 120.0;
const COLUMN_RESIZE_HANDLE_WIDTH: f32 = 8.0;
const DELETE_CONFIRM_THRESHOLD: usize = 1_000;
const COPY_CONFIRM_THRESHOLD: usize = 5_000;
const MAX_CACHED_ROWS: usize = 1_024;
const INSTANCE_PORT: u16 = 47_391;
const INSTANCE_MAGIC: &str = "QUICKROWS-INSTANCE-1";

#[derive(Clone, Debug)]
struct OpenTarget {
    path: PathBuf,
    fragment: Option<CsvFragment>,
}

impl From<PathBuf> for OpenTarget {
    fn from(path: PathBuf) -> Self {
        Self {
            path,
            fragment: None,
        }
    }
}

#[derive(Clone, Debug)]
enum RuntimeRequest {
    Activate,
    Open(OpenTarget),
}

#[derive(Default)]
struct QueryProgress {
    processed: usize,
    total: usize,
    found: usize,
    pending: Vec<usize>,
}

#[derive(Clone)]
struct CachedRow {
    source_row: usize,
    cells: Vec<String>,
    deleted: bool,
}

#[derive(Clone)]
struct EditingCell {
    display_row: usize,
    source_row: usize,
    column: usize,
    initial_value: String,
}

struct CellCommit {
    editing: EditingCell,
    value: String,
    document: Arc<Mutex<CsvDocument>>,
}

struct ColumnResize {
    column: usize,
    start_x: f32,
    start_width: f32,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ActiveHighlight {
    Search,
    Duplicates,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    Open,
    Reload,
    Save,
    Search,
    Duplicates,
    Sort,
    Rows,
    Copy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TableContextMenuKind {
    Cell { can_edit: bool },
    Row,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ContextMenuCommand {
    CopyCell,
    SearchCell,
    EditCell,
    DeleteRows,
    RestoreRows,
    CopySelection,
}

fn context_menu_item_count(kind: TableContextMenuKind) -> usize {
    match kind {
        TableContextMenuKind::Cell { can_edit: true } => 6,
        TableContextMenuKind::Cell { can_edit: false } => 5,
        TableContextMenuKind::Row => 3,
    }
}

fn context_menu_command(kind: TableContextMenuKind, index: usize) -> ContextMenuCommand {
    let command_index = match kind {
        TableContextMenuKind::Cell { can_edit: true } => index,
        TableContextMenuKind::Cell { can_edit: false } => match index {
            0 | 1 => index,
            item => item + 1,
        },
        TableContextMenuKind::Row => index + 3,
    };
    match command_index {
        0 => ContextMenuCommand::CopyCell,
        1 => ContextMenuCommand::SearchCell,
        2 => ContextMenuCommand::EditCell,
        3 => ContextMenuCommand::DeleteRows,
        4 => ContextMenuCommand::RestoreRows,
        _ => ContextMenuCommand::CopySelection,
    }
}

#[derive(Clone, Copy)]
struct TableContextMenu {
    position: Point<Pixels>,
    kind: TableContextMenuKind,
    focused_item: usize,
}

#[derive(Clone)]
enum PendingBulkAction {
    Copy {
        rows: Vec<usize>,
    },
    CopyCells {
        row_start: usize,
        row_end: usize,
        column_start: usize,
        column_end: usize,
    },
    Delete {
        rows: Vec<usize>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RowMutation {
    Delete,
    Restore,
}

#[derive(Clone, Copy)]
enum SettingsChoice {
    Theme(ThemePreference),
    Density(RowDensity),
    Delimiter(Option<&'static str>),
    Quote(Option<&'static str>),
    Escape(Option<&'static str>),
    Comment(Option<&'static str>),
    ExcelSep(Option<bool>),
    LineEnding(Option<&'static str>),
    Encoding(Option<&'static str>),
    Headers(Option<bool>),
    Malformed(Option<&'static str>),
    MaxFieldSize(Option<usize>),
    MaxRecordSize(Option<usize>),
}

impl RowMutation {
    fn deleted(self) -> bool {
        self == Self::Delete
    }

    fn past_tense(self) -> &'static str {
        match self {
            Self::Delete => "Deleted",
            Self::Restore => "Restored",
        }
    }
}

#[derive(Clone)]
enum PendingEditAction {
    OpenDialog,
    Save,
    SaveAs,
    SortColumn(usize),
    MutateRows {
        rows: Vec<usize>,
        mutation: RowMutation,
    },
}

#[derive(Clone)]
enum PendingDestructiveAction {
    Open(OpenTarget),
    Reload,
    Clear,
    Close,
    Quit,
}

struct LoadedDocument {
    document: Arc<Mutex<CsvDocument>>,
    path: PathBuf,
    headers: Vec<String>,
    row_count: usize,
    detected_parse_info: ParseInfo,
    parse_info: ParseInfo,
    warnings: Vec<ParseWarning>,
    file_fingerprint: Option<(u64, SystemTime)>,
    dirty: bool,
}

struct QuickRowsView {
    focus_handle: FocusHandle,
    loaded: Option<LoadedDocument>,
    loading: bool,
    operation_kind: Option<OperationKind>,
    operation_cancellation: Option<CancellationToken>,
    error: Option<SharedString>,
    notice: Option<SharedString>,
    external_change_detected: bool,
    selected_row: Option<usize>,
    selected_rows: RowSelection,
    selection_anchor: Option<usize>,
    cell_selection: Option<CellSelection>,
    cell_dragging: bool,
    show_find: bool,
    show_duplicates: bool,
    show_settings: bool,
    show_warning_details: bool,
    show_header_prompt: bool,
    show_shortcuts: bool,
    show_about: bool,
    search_column: Option<usize>,
    search_match_case: bool,
    search_whole_word: bool,
    duplicate_column: Option<usize>,
    current_duplicate_match: usize,
    active_highlight: Option<ActiveHighlight>,
    search_input: gpui::Entity<InputState>,
    custom_delimiter_input: gpui::Entity<InputState>,
    custom_quote_input: gpui::Entity<InputState>,
    custom_escape_input: gpui::Entity<InputState>,
    custom_comment_input: gpui::Entity<InputState>,
    edit_input: gpui::Entity<InputState>,
    editing_cell: Option<EditingCell>,
    editing_draft_dirty: bool,
    context_cell: Option<(usize, usize, usize, String)>,
    table_context_menu: Option<TableContextMenu>,
    pending_cell_commits: usize,
    cell_commit_in_flight: bool,
    cell_commit_queue: VecDeque<CellCommit>,
    pending_edit_action: Option<PendingEditAction>,
    pending_bulk_action: Option<PendingBulkAction>,
    pending_external_save: Option<PathBuf>,
    resizing_column: Option<ColumnResize>,
    search_results: Vec<usize>,
    current_match: usize,
    last_search_query: Option<String>,
    search_request_id: u64,
    search_refresh_token: u64,
    search_stale: bool,
    search_has_completed: bool,
    duplicate_results: Vec<usize>,
    duplicate_request_id: u64,
    duplicate_stale: bool,
    duplicate_check_has_completed: bool,
    settings: AppSettings,
    settings_store: SettingsStore,
    pending_initial_path: Option<OpenTarget>,
    pending_destructive: Option<PendingDestructiveAction>,
    self_weak: Option<WeakEntity<QuickRowsView>>,
    row_cache: HashMap<usize, CachedRow>,
    desired_row_range: Option<std::ops::Range<usize>>,
    row_request_in_flight: Option<(u64, u64)>,
    failed_row_range: Option<(u64, std::ops::Range<usize>)>,
    next_row_request_id: u64,
    document_generation: u64,
    open_request_id: u64,
    row_scroll: UniformListScrollHandle,
    column_scroll: ScrollHandle,
}

fn fragment_regions_to_selection(
    regions: &[ResolvedFragmentRegion],
    row_count: usize,
    column_count: usize,
    has_headers: bool,
) -> (
    Vec<std::ops::RangeInclusive<usize>>,
    Option<(usize, usize, usize, usize)>,
) {
    if row_count == 0 || column_count == 0 {
        return (Vec::new(), None);
    }
    let header_offset = usize::from(has_headers);
    let data_rows = |rows: &std::ops::RangeInclusive<usize>| {
        let start = (*rows.start())
            .max(header_offset)
            .saturating_sub(header_offset);
        let end = (*rows.end())
            .saturating_sub(header_offset)
            .min(row_count - 1);
        (start <= end).then_some(start..=end)
    };
    let mut selected_rows = Vec::new();
    let mut first_cells = None;
    for region in regions {
        match region {
            ResolvedFragmentRegion::Rows(rows) => {
                if let Some(rows) = data_rows(rows) {
                    selected_rows.push(rows);
                }
            }
            ResolvedFragmentRegion::Columns(columns) => {
                let columns = (*columns.start()).min(column_count - 1)
                    ..=(*columns.end()).min(column_count - 1);
                if columns.start() <= columns.end() {
                    selected_rows.push(0..=row_count - 1);
                    first_cells.get_or_insert((0, *columns.start(), row_count - 1, *columns.end()));
                }
            }
            ResolvedFragmentRegion::Cells { rows, columns } => {
                if let Some(rows) = data_rows(rows) {
                    let columns = (*columns.start()).min(column_count - 1)
                        ..=(*columns.end()).min(column_count - 1);
                    if columns.start() <= columns.end() {
                        selected_rows.push(rows.clone());
                        first_cells.get_or_insert((
                            *rows.start(),
                            *columns.start(),
                            *rows.end(),
                            *columns.end(),
                        ));
                    }
                }
            }
        }
    }
    (selected_rows, first_cells)
}

impl QuickRowsView {
    fn new(initial_path: Option<OpenTarget>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let focus_handle = cx.focus_handle();
        let settings_store = SettingsStore::new(settings_path());
        let settings = settings_store.load().unwrap_or_default();
        let search_input = cx.new(|cx| InputState::new(window, cx).placeholder("Find in CSV"));
        let custom_delimiter_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Custom delimiter"));
        if let Some(delimiter) = settings
            .parse_overrides
            .delimiter
            .as_deref()
            .filter(|delimiter| !is_named_delimiter(delimiter))
        {
            custom_delimiter_input.update(cx, |input, cx| {
                input.set_value(delimiter.to_string(), window, cx)
            });
        }
        let custom_quote_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Custom quote"));
        if let Some(quote) = settings
            .parse_overrides
            .quote
            .as_deref()
            .filter(|quote| !is_named_quote(quote))
        {
            custom_quote_input.update(cx, |input, cx| {
                input.set_value(quote.to_string(), window, cx)
            });
        }
        let custom_escape_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Custom escape"));
        if let Some(escape) = settings
            .parse_overrides
            .escape
            .as_deref()
            .filter(|escape| !is_named_escape(escape))
        {
            custom_escape_input.update(cx, |input, cx| {
                input.set_value(escape.to_string(), window, cx)
            });
        }
        let custom_comment_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Custom comment"));
        if let Some(comment) = settings
            .parse_overrides
            .comment
            .as_deref()
            .filter(|comment| !is_named_comment(comment))
        {
            custom_comment_input.update(cx, |input, cx| {
                input.set_value(comment.to_string(), window, cx)
            });
        }
        let edit_input = cx.new(|cx| InputState::new(window, cx).placeholder("Cell value"));
        cx.subscribe(&edit_input, |view, _, event, cx| match event {
            InputEvent::Change => {
                if let Some(initial_value) = view
                    .editing_cell
                    .as_ref()
                    .map(|editing| editing.initial_value.clone())
                {
                    view.editing_draft_dirty = view.edit_input.read(cx).value() != initial_value;
                    cx.notify();
                }
            }
            InputEvent::PressEnter { secondary: false } | InputEvent::Blur => {
                view.commit_cell_edit(cx);
            }
            _ => {}
        })
        .detach();
        cx.subscribe(&search_input, |view, _, event, cx| match event {
            InputEvent::Change => view.schedule_search(cx),
            InputEvent::PressEnter { secondary } => {
                let query = view.search_input.read(cx).value().to_string();
                if view.last_search_query.as_deref() != Some(query.as_str()) || view.search_stale {
                    view.run_search(cx);
                } else if *secondary {
                    view.previous_search_result(cx);
                } else {
                    view.next_search_result(cx);
                }
            }
            _ => {}
        })
        .detach();
        let mut view = Self {
            focus_handle,
            loaded: None,
            loading: false,
            operation_kind: None,
            operation_cancellation: None,
            error: None,
            notice: None,
            external_change_detected: false,
            selected_row: None,
            selected_rows: RowSelection::default(),
            selection_anchor: None,
            cell_selection: None,
            cell_dragging: false,
            show_find: false,
            show_duplicates: false,
            show_settings: false,
            show_warning_details: false,
            show_header_prompt: false,
            show_shortcuts: false,
            show_about: false,
            search_column: None,
            search_match_case: false,
            search_whole_word: false,
            duplicate_column: None,
            current_duplicate_match: 0,
            active_highlight: None,
            search_input,
            custom_delimiter_input,
            custom_quote_input,
            custom_escape_input,
            custom_comment_input,
            edit_input,
            editing_cell: None,
            editing_draft_dirty: false,
            context_cell: None,
            table_context_menu: None,
            pending_cell_commits: 0,
            cell_commit_in_flight: false,
            cell_commit_queue: VecDeque::new(),
            pending_edit_action: None,
            pending_bulk_action: None,
            pending_external_save: None,
            resizing_column: None,
            search_results: Vec::new(),
            current_match: 0,
            last_search_query: None,
            search_request_id: 0,
            search_refresh_token: 0,
            search_stale: false,
            search_has_completed: false,
            duplicate_results: Vec::new(),
            duplicate_request_id: 0,
            duplicate_stale: false,
            duplicate_check_has_completed: false,
            settings,
            settings_store,
            pending_initial_path: initial_path,
            pending_destructive: None,
            self_weak: None,
            row_cache: HashMap::new(),
            desired_row_range: None,
            row_request_in_flight: None,
            failed_row_range: None,
            next_row_request_id: 0,
            document_generation: 0,
            open_request_id: 0,
            row_scroll: UniformListScrollHandle::new(),
            column_scroll: ScrollHandle::new(),
        };
        if let Some(path) = view.pending_initial_path.take() {
            view.open_path(path, cx);
        }
        view.focus_handle.focus(window);
        view
    }

    fn persist_settings(&mut self) {
        if let Err(error) = self.settings_store.save(&self.settings) {
            self.error = Some(format!("Unable to save settings: {error}").into());
        }
    }

    fn column_width(&self, column: usize) -> f32 {
        self.settings
            .column_widths
            .get(column)
            .copied()
            .unwrap_or(self.settings.column_width)
            .max(MIN_COLUMN_WIDTH)
    }

    fn set_column_width(&mut self, column: usize, width: f32) {
        let default_width = self.settings.column_width.max(MIN_COLUMN_WIDTH);
        if self.settings.column_widths.len() <= column {
            self.settings
                .column_widths
                .resize(column + 1, default_width);
        }
        self.settings.column_widths[column] = width.max(MIN_COLUMN_WIDTH);
    }

    fn begin_column_resize(&mut self, column: usize, start_x: f32, cx: &mut Context<Self>) {
        self.resizing_column = Some(ColumnResize {
            column,
            start_x,
            start_width: self.column_width(column),
        });
        cx.notify();
    }

    fn update_column_resize(
        &mut self,
        event: &MouseMoveEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(resize) = self.resizing_column.as_ref() else {
            return;
        };
        let column = resize.column;
        let width = resize.start_width + f32::from(event.position.x) - resize.start_x;
        self.set_column_width(column, width);
        cx.notify();
    }

    fn finish_pointer_interaction(
        &mut self,
        _: &MouseUpEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let resized = self.resizing_column.take().is_some();
        let dragged_cells = std::mem::take(&mut self.cell_dragging);
        if resized {
            self.persist_settings();
        }
        if resized || dragged_cells {
            cx.notify();
        }
    }

    fn clear_cell_editor(&mut self) {
        self.editing_cell = None;
        self.editing_draft_dirty = false;
        self.context_cell = None;
        self.pending_cell_commits = self
            .pending_cell_commits
            .saturating_sub(self.cell_commit_queue.len());
        self.cell_commit_queue.clear();
        self.pending_edit_action = None;
    }

    fn clear_selection(&mut self) {
        self.selected_row = None;
        self.selected_rows.clear();
        self.selection_anchor = None;
        self.cell_selection = None;
        self.cell_dragging = false;
    }

    fn apply_fragment_regions(&mut self, regions: &[ResolvedFragmentRegion]) {
        let Some(loaded) = self.loaded.as_ref() else {
            return;
        };
        let (rows, cells) = fragment_regions_to_selection(
            regions,
            loaded.row_count,
            loaded.headers.len(),
            loaded.parse_info.has_headers,
        );
        let first_row = rows.first().map(|rows| *rows.start());
        for rows in rows {
            self.selected_rows.insert_range(rows);
        }
        if let Some(row) = first_row {
            self.selected_row = Some(row);
            self.selection_anchor = Some(row);
            self.row_scroll.scroll_to_item(row, ScrollStrategy::Center);
        }
        if let Some((start_row, start_column, end_row, end_column)) = cells {
            let mut selection = CellSelection::single(start_row, start_column);
            selection.set_active(end_row, end_column);
            self.cell_selection = Some(selection);
        }
    }

    fn invalidate_row_cache(&mut self) {
        self.document_generation = self.document_generation.checked_add(1).unwrap_or_default();
        self.row_cache.clear();
        self.desired_row_range = None;
        self.failed_row_range = None;
        // Keep an obsolete in-flight request registered. Its completion will
        // release this slot and start only the latest requested viewport.
    }

    fn cache_rows(&mut self, start: usize, rows: Vec<(usize, Vec<String>, bool)>) {
        self.row_cache.clear();
        self.row_cache.extend(rows.into_iter().enumerate().map(
            |(offset, (source_row, cells, deleted))| {
                (
                    start + offset,
                    CachedRow {
                        source_row,
                        cells,
                        deleted,
                    },
                )
            },
        ));
    }

    fn retry_failed_rows(&mut self, cx: &mut Context<Self>) {
        self.failed_row_range = None;
        self.error = None;
        self.start_row_request(cx);
        cx.notify();
    }

    fn request_visible_rows(&mut self, visible: std::ops::Range<usize>, cx: &mut App) {
        let Some(loaded) = &self.loaded else { return };
        if loaded.row_count == 0 || visible.start >= loaded.row_count {
            return;
        }
        let visible = visible.start
            ..visible
                .end
                .min(visible.start.saturating_add(MAX_CACHED_ROWS))
                .min(loaded.row_count);
        if self.desired_row_range.as_ref() != Some(&visible) {
            self.desired_row_range = Some(visible);
            self.failed_row_range = None;
        }
        self.start_row_request(cx);
    }

    fn start_row_request(&mut self, cx: &mut App) {
        if self.loading || self.row_request_in_flight.is_some() {
            return;
        }
        let Some(visible) = self.desired_row_range.clone() else {
            return;
        };
        if (visible.clone()).all(|row| self.row_cache.contains_key(&row))
            || self
                .failed_row_range
                .as_ref()
                .is_some_and(|(generation, range)| {
                    *generation == self.document_generation && range == &visible
                })
        {
            return;
        }
        let Some(loaded) = &self.loaded else { return };
        let Some(weak) = self.self_weak.clone() else {
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
        let generation = self.document_generation;
        self.next_row_request_id = self.next_row_request_id.checked_add(1).unwrap_or_default();
        let request_id = self.next_row_request_id;
        self.row_request_in_flight = Some((generation, request_id));
        let requested_visible = visible.clone();

        let task = cx.background_spawn(async move {
            let document = document
                .lock()
                .map_err(|_| "CSV document lock was poisoned".to_string())?;
            let rows = document.display_rows(start, count)?;
            Ok::<_, String>(
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
                if view.row_request_in_flight == Some((generation, request_id)) {
                    view.row_request_in_flight = None;
                }
                let result_is_current = view.document_generation == generation
                    && view.desired_row_range.as_ref() == Some(&requested_visible)
                    && view
                        .loaded
                        .as_ref()
                        .is_some_and(|loaded| Arc::ptr_eq(&loaded.document, &document_identity));
                if result_is_current {
                    match result {
                        Ok(rows) => view.cache_rows(start, rows),
                        Err(error) => {
                            view.failed_row_range = Some((generation, requested_visible));
                            view.error = Some(format!("Unable to load rows: {error}").into());
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
        self.settings.recent_files.retain(|recent| recent != path);
        self.persist_settings();
        cx.notify();
    }

    fn open_dialog(&mut self, _: &OpenFile, _window: &mut Window, cx: &mut Context<Self>) {
        if self.editing_cell.is_some() {
            self.pending_edit_action = Some(PendingEditAction::OpenDialog);
            self.commit_cell_edit(cx);
            return;
        }
        self.prompt_open_dialog(cx);
    }

    fn prompt_open_dialog(&mut self, cx: &mut Context<Self>) {
        if self.loading || self.modal_active() {
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
                        this.error = Some(format!("Unable to open file dialog: {error}").into());
                        cx.notify();
                    })?;
                }
                Err(error) => {
                    this.update(cx, |this, cx| {
                        this.error =
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
                    if this.loading && this.open_request_id == request_id {
                        this.notice =
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
                    if this.loading && this.operation_kind == Some(kind) {
                        let percent = if total == 0 {
                            100
                        } else {
                            processed.saturating_mul(100) / total
                        };
                        this.notice = Some(
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
                        OperationKind::Search => this.search_request_id == request_id,
                        OperationKind::Duplicates => this.duplicate_request_id == request_id,
                        _ => false,
                    };
                    if this.loading && this.operation_kind == Some(kind) && request_is_current {
                        match kind {
                            OperationKind::Search => {
                                this.search_results.extend(pending);
                                this.search_results.sort_unstable();
                                this.search_results.dedup();
                            }
                            OperationKind::Duplicates => {
                                this.duplicate_results.extend(pending);
                                this.duplicate_results.sort_unstable();
                                this.duplicate_results.dedup();
                            }
                            _ => {}
                        }
                        let percent = if total == 0 {
                            100
                        } else {
                            processed.min(total).saturating_mul(100) / total
                        };
                        let label = if kind == OperationKind::Search {
                            "Searching"
                        } else {
                            "Checking duplicates"
                        };
                        this.notice = Some(
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
            loop {
                cx.background_executor()
                    .timer(Duration::from_secs(2))
                    .await;
                if this
                    .update(cx, |this, cx| {
                        let Some(loaded) = &this.loaded else { return true };
                        if this.loading || this.external_change_detected {
                            return true;
                        }
                        let current = file_fingerprint(&loaded.path);
                        if current != loaded.file_fingerprint {
                            this.external_change_detected = true;
                            this.notice = Some(
                                if this.is_dirty() {
                                    "The CSV changed on disk. Save As or reload to avoid overwriting external changes."
                                } else {
                                    "The CSV changed on disk. Reload to view the latest version."
                                }
                                .into(),
                            );
                            cx.notify();
                        }
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
        cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(Duration::from_millis(150))
                    .await;
                let pending = match requests.lock() {
                    Ok(mut requests) => requests.drain(..).collect::<Vec<_>>(),
                    Err(_) => Vec::new(),
                };
                for request in pending {
                    match request {
                        RuntimeRequest::Activate => {
                            let _ = window_handle.update(cx, |_, window, cx| {
                                window.activate_window();
                                cx.activate(true);
                            });
                        }
                        RuntimeRequest::Open(path) => {
                            let deferred = match this.update(cx, |this, cx| {
                                if this.loading || this.pending_destructive.is_some() {
                                    true
                                } else {
                                    this.open_path(path.clone(), cx);
                                    false
                                }
                            }) {
                                Ok(deferred) => deferred,
                                Err(_) => return anyhow::Ok(()),
                            };
                            if deferred {
                                if let Ok(mut requests) = requests.lock() {
                                    requests.push_front(RuntimeRequest::Open(path));
                                }
                                break;
                            }
                        }
                    }
                }
            }
        })
        .detach();
    }

    fn open_path(&mut self, target: OpenTarget, cx: &mut Context<Self>) {
        if self.loading {
            return;
        }
        if self.is_dirty() {
            self.pending_destructive = Some(PendingDestructiveAction::Open(target));
            cx.notify();
            return;
        }
        let OpenTarget { path, fragment } = target;
        self.loading = true;
        self.open_request_id = self.open_request_id.wrapping_add(1);
        let request_id = self.open_request_id;
        let label = format!("Opening {}", display_name(&path));
        self.error = None;
        self.notice = Some(format!("{label}… 0 rows scanned").into());
        cx.notify();

        let cancellation = self.begin_cancellable_operation(OperationKind::Open);
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_open_progress(progress.clone(), request_id, label, cx);
        let overrides = self.settings.parse_overrides.clone();
        let prompt_if_no_headers = overrides.has_headers.is_none();
        let cache_root = cache_path();
        let task = cx.background_spawn(async move {
            let update_progress = |rows| progress.store(rows, Ordering::Relaxed);
            let document = CsvDocument::open_cancellable_cached(
                &path,
                Some(overrides),
                Some(&update_progress),
                &cancellation,
                &cache_root,
            )?;
            Ok::<_, String>((path, document, fragment))
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.open_request_id != request_id {
                    return;
                }
                this.finish_cancellable_operation();
                this.loading = false;
                match result {
                    Ok((path, document, fragment)) => {
                        let fragment_regions = fragment
                            .as_ref()
                            .map(|fragment| document.resolve_fragment(fragment))
                            .unwrap_or_default();
                        let show_header_prompt =
                            prompt_if_no_headers && !document.metadata().detected.has_headers;
                        let headers = document.metadata().headers.clone();
                        let row_count = document.row_count();
                        let detected_parse_info = document.metadata().detected.clone();
                        let parse_info = document.metadata().effective.clone();
                        let warnings = document.metadata().warnings.clone();
                        let file_fingerprint = file_fingerprint(&path);
                        this.settings.remember_file(path.clone());
                        this.persist_settings();
                        this.loaded = Some(LoadedDocument {
                            document: Arc::new(Mutex::new(document)),
                            path,
                            headers,
                            row_count,
                            detected_parse_info,
                            parse_info,
                            warnings,
                            file_fingerprint,
                            dirty: false,
                        });
                        this.show_header_prompt = show_header_prompt;
                        this.external_change_detected = false;
                        this.clear_cell_editor();
                        this.invalidate_row_cache();
                        this.clear_selection();
                        this.apply_fragment_regions(&fragment_regions);
                        this.search_results.clear();
                        this.search_stale = false;
                        this.search_has_completed = false;
                        this.duplicate_results.clear();
                        this.duplicate_stale = false;
                        this.duplicate_check_has_completed = false;
                        this.current_match = 0;
                        this.last_search_query = None;
                        this.current_duplicate_match = 0;
                        this.active_highlight = None;
                        this.show_find = false;
                        this.show_duplicates = false;
                        this.notice = None;
                    }
                    Err(error) if error.contains("Operation cancelled") => {
                        this.notice = Some("Open cancelled.".into());
                    }
                    Err(error) => {
                        this.error = Some(format!("Unable to open CSV: {error}").into());
                        this.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn reload_path(&mut self, path: PathBuf, cx: &mut Context<Self>) {
        if self.loading {
            return;
        }
        self.loading = true;
        self.open_request_id = self.open_request_id.wrapping_add(1);
        let request_id = self.open_request_id;
        let label = format!("Reloading {}", display_name(&path));
        self.error = None;
        self.notice = Some(format!("{label}… 0 rows scanned").into());
        cx.notify();

        let cancellation = self.begin_cancellable_operation(OperationKind::Reload);
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_open_progress(progress.clone(), request_id, label, cx);
        let overrides = self.settings.parse_overrides.clone();
        let prompt_if_no_headers = overrides.has_headers.is_none();
        let cache_root = cache_path();
        let task = cx.background_spawn(async move {
            let update_progress = |rows| progress.store(rows, Ordering::Relaxed);
            let document = CsvDocument::open_cancellable_cached(
                &path,
                Some(overrides),
                Some(&update_progress),
                &cancellation,
                &cache_root,
            )?;
            Ok::<_, String>((path, document))
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.open_request_id != request_id {
                    return;
                }
                this.finish_cancellable_operation();
                this.loading = false;
                match result {
                    Ok((path, document)) => {
                        let show_header_prompt =
                            prompt_if_no_headers && !document.metadata().detected.has_headers;
                        let headers = document.metadata().headers.clone();
                        let row_count = document.row_count();
                        let detected_parse_info = document.metadata().detected.clone();
                        let parse_info = document.metadata().effective.clone();
                        let warnings = document.metadata().warnings.clone();
                        let file_fingerprint = file_fingerprint(&path);
                        this.loaded = Some(LoadedDocument {
                            document: Arc::new(Mutex::new(document)),
                            path,
                            headers,
                            row_count,
                            detected_parse_info,
                            parse_info,
                            warnings,
                            file_fingerprint,
                            dirty: false,
                        });
                        this.show_header_prompt = show_header_prompt;
                        this.external_change_detected = false;
                        this.clear_cell_editor();
                        this.invalidate_row_cache();
                        this.clear_selection();
                        this.search_results.clear();
                        this.search_stale = false;
                        this.search_has_completed = false;
                        this.duplicate_results.clear();
                        this.duplicate_stale = false;
                        this.duplicate_check_has_completed = false;
                        this.current_match = 0;
                        this.last_search_query = None;
                        this.current_duplicate_match = 0;
                        this.active_highlight = None;
                        this.show_find = false;
                        this.show_duplicates = false;
                        this.notice = None;
                    }
                    Err(error) if error.contains("Operation cancelled") => {
                        this.notice =
                            Some("Reload cancelled; the previous document is still open.".into());
                    }
                    Err(error) => {
                        this.error = Some(format!("Unable to reload CSV: {error}").into());
                        this.notice = Some("The previous document is still open.".into());
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn clear_file(&mut self, _: &ClearFile, window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        if self.loading {
            self.notice = Some("Cancel the current operation before clearing the document.".into());
            cx.notify();
            return;
        }
        if self.is_dirty() {
            self.pending_destructive = Some(PendingDestructiveAction::Clear);
            cx.notify();
            return;
        }
        self.clear_file_unchecked(window, cx);
    }

    fn reload_file(&mut self, _: &ReloadFile, _window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() || self.loading {
            return;
        }
        let Some(path) = self.loaded.as_ref().map(|loaded| loaded.path.clone()) else {
            return;
        };
        if self.is_dirty() {
            self.pending_destructive = Some(PendingDestructiveAction::Reload);
            cx.notify();
        } else {
            self.reload_path(path, cx);
        }
    }

    fn close_window_action(
        &mut self,
        _: &CloseWindow,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.is_dirty() {
            self.pending_destructive = Some(PendingDestructiveAction::Close);
            cx.notify();
        } else {
            window.remove_window();
        }
    }

    fn quit_app(&mut self, _: &QuitApp, _window: &mut Window, cx: &mut Context<Self>) {
        if self.is_dirty() {
            self.pending_destructive = Some(PendingDestructiveAction::Quit);
            cx.notify();
        } else {
            cx.quit();
        }
    }

    fn clear_file_unchecked(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(cancellation) = self.operation_cancellation.take() {
            cancellation.cancel();
        }
        self.operation_kind = None;
        self.loading = false;
        self.search_request_id = self.search_request_id.wrapping_add(1);
        self.duplicate_request_id = self.duplicate_request_id.wrapping_add(1);
        self.loaded = None;
        self.clear_cell_editor();
        self.invalidate_row_cache();
        self.clear_selection();
        self.search_results.clear();
        self.search_stale = false;
        self.search_has_completed = false;
        self.duplicate_results.clear();
        self.duplicate_stale = false;
        self.duplicate_check_has_completed = false;
        self.current_match = 0;
        self.last_search_query = None;
        self.current_duplicate_match = 0;
        self.active_highlight = None;
        self.show_find = false;
        self.show_duplicates = false;
        self.error = None;
        self.notice = None;
        window.set_window_title(BASE_TITLE);
        cx.notify();
    }

    fn is_dirty(&self) -> bool {
        self.editing_draft_dirty
            || self.pending_cell_commits > 0
            || self.loaded.as_ref().is_some_and(|loaded| loaded.dirty)
    }

    fn modal_active(&self) -> bool {
        self.show_settings
            || self.show_header_prompt
            || self.show_shortcuts
            || self.show_about
            || self.pending_destructive.is_some()
            || self.pending_bulk_action.is_some()
            || self.pending_external_save.is_some()
    }

    fn begin_cancellable_operation(&mut self, kind: OperationKind) -> CancellationToken {
        debug_assert!(self.operation_cancellation.is_none());
        let cancellation = CancellationToken::new();
        self.operation_kind = Some(kind);
        self.operation_cancellation = Some(cancellation.clone());
        cancellation
    }

    fn finish_cancellable_operation(&mut self) {
        self.operation_kind = None;
        self.operation_cancellation = None;
    }

    fn cancel_current_operation(&mut self, cx: &mut Context<Self>) {
        if let Some(cancellation) = &self.operation_cancellation {
            cancellation.cancel();
            self.notice = Some("Cancelling operation…".into());
            cx.notify();
        }
    }

    fn cancel_pending_destructive(&mut self, cx: &mut Context<Self>) {
        self.pending_destructive = None;
        self.pending_edit_action = None;
        cx.notify();
    }

    fn cancel_pending_bulk_action(&mut self, cx: &mut Context<Self>) {
        self.pending_bulk_action = None;
        cx.notify();
    }

    fn confirm_pending_bulk_action(&mut self, cx: &mut Context<Self>) {
        match self.pending_bulk_action.take() {
            Some(PendingBulkAction::Copy { rows }) => self.copy_rows(rows, cx),
            Some(PendingBulkAction::CopyCells {
                row_start,
                row_end,
                column_start,
                column_end,
            }) => self.copy_cell_range(row_start, row_end, column_start, column_end, cx),
            Some(PendingBulkAction::Delete { rows }) => {
                self.mutate_rows_background(rows, RowMutation::Delete, cx)
            }
            None => {}
        }
    }

    fn save_pending_destructive(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.editing_cell.is_some() {
            self.pending_edit_action = Some(PendingEditAction::Save);
            self.commit_cell_edit(cx);
        } else {
            self.save_file(&SaveFile, window, cx);
        }
    }

    fn discard_pending_destructive(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.pending_edit_action = None;
        self.editing_cell = None;
        self.editing_draft_dirty = false;
        match self.pending_destructive.take() {
            Some(PendingDestructiveAction::Open(path)) => {
                self.loaded = None;
                self.open_path(path, cx);
            }
            Some(PendingDestructiveAction::Reload) => {
                if let Some(path) = self.loaded.as_ref().map(|loaded| loaded.path.clone()) {
                    self.reload_path(path, cx);
                }
            }
            Some(PendingDestructiveAction::Clear) => self.clear_file_unchecked(window, cx),
            Some(PendingDestructiveAction::Close) => window.remove_window(),
            Some(PendingDestructiveAction::Quit) => cx.quit(),
            None => {}
        }
    }

    fn cancel_external_save(&mut self, cx: &mut Context<Self>) {
        self.pending_external_save = None;
        cx.notify();
    }

    fn confirm_external_overwrite(&mut self, cx: &mut Context<Self>) {
        if let Some(path) = self.pending_external_save.take() {
            self.save_to_unchecked(path, cx);
        }
    }

    fn save_external_as(&mut self, cx: &mut Context<Self>) {
        self.pending_external_save = None;
        self.prompt_save_as(cx);
    }

    fn reload_external_change(&mut self, cx: &mut Context<Self>) {
        self.pending_external_save = None;
        let Some(path) = self.loaded.as_ref().map(|loaded| loaded.path.clone()) else {
            return;
        };
        if self.is_dirty() {
            self.pending_destructive = Some(PendingDestructiveAction::Reload);
            cx.notify();
        } else {
            self.reload_path(path, cx);
        }
    }

    fn save_file(&mut self, _: &SaveFile, _window: &mut Window, cx: &mut Context<Self>) {
        if self.editing_cell.is_some() {
            self.pending_edit_action = Some(PendingEditAction::Save);
            self.commit_cell_edit(cx);
            return;
        }
        if self.pending_cell_commits > 0 {
            self.pending_edit_action = Some(PendingEditAction::Save);
            return;
        }
        if self.loading || !self.is_dirty() {
            return;
        }
        let Some(loaded) = &self.loaded else { return };
        let path = loaded.path.clone();
        self.save_to(path, cx);
    }

    fn save_file_as(&mut self, _: &SaveFileAs, _window: &mut Window, cx: &mut Context<Self>) {
        if self.editing_cell.is_some() {
            self.pending_edit_action = Some(PendingEditAction::SaveAs);
            self.commit_cell_edit(cx);
            return;
        }
        if self.pending_cell_commits > 0 {
            self.pending_edit_action = Some(PendingEditAction::SaveAs);
            return;
        }
        self.prompt_save_as(cx);
    }

    fn prompt_save_as(&mut self, cx: &mut Context<Self>) {
        if self.loading {
            return;
        }
        let Some(loaded) = &self.loaded else { return };
        let directory = loaded
            .path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let suggested_name = display_name(&loaded.path);
        let receiver = cx.prompt_for_new_path(&directory, Some(&suggested_name));
        cx.spawn(async move |this, cx| {
            match receiver.await {
                Ok(Ok(Some(path))) => this.update(cx, |this, cx| this.save_to(path, cx))?,
                Ok(Ok(None)) => {}
                Ok(Err(error)) => this.update(cx, |this, cx| {
                    this.error = Some(format!("Unable to show Save As dialog: {error}").into());
                    cx.notify();
                })?,
                Err(error) => this.update(cx, |this, cx| {
                    this.error =
                        Some(format!("Save As dialog closed unexpectedly: {error}").into());
                    cx.notify();
                })?,
            }
            anyhow::Ok(())
        })
        .detach();
    }

    fn save_to(&mut self, path: PathBuf, cx: &mut Context<Self>) {
        if self.external_change_detected
            && self
                .loaded
                .as_ref()
                .is_some_and(|loaded| loaded.path == path)
        {
            self.pending_external_save = Some(path);
            cx.notify();
            return;
        }
        self.save_to_unchecked(path, cx);
    }

    fn save_to_unchecked(&mut self, path: PathBuf, cx: &mut Context<Self>) {
        let Some(loaded) = &self.loaded else { return };
        let document = loaded.document.clone();
        let row_count = loaded.row_count;
        self.loading = true;
        let cancellation = self.begin_cancellable_operation(OperationKind::Save);
        self.error = None;
        self.notice = Some(format!("Saving {}…", display_name(&path)).into());
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_row_progress(
            progress.clone(),
            row_count,
            OperationKind::Save,
            "Saving",
            cx,
        );
        cx.notify();
        let save_path = path.clone();
        let task = cx.background_spawn(async move {
            let update_progress = |processed, _| progress.store(processed, Ordering::Relaxed);
            let mut document = document
                .lock()
                .map_err(|_| "CSV document lock was poisoned".to_string())?;
            document.save_cancellable_with_progress(&save_path, &cancellation, &update_progress)?;
            Ok::<_, String>(())
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                this.finish_cancellable_operation();
                this.loading = false;
                match result {
                    Ok(()) => {
                        if let Some(loaded) = &mut this.loaded {
                            loaded.path = path.clone();
                            if let Ok(document) = loaded.document.lock() {
                                loaded.headers = document.metadata().headers.clone();
                                loaded.row_count = document.row_count();
                                loaded.detected_parse_info = document.metadata().detected.clone();
                                loaded.parse_info = document.metadata().effective.clone();
                                loaded.warnings = document.metadata().warnings.clone();
                                loaded.dirty = false;
                            }
                            loaded.file_fingerprint = file_fingerprint(&path);
                        }
                        this.external_change_detected = false;
                        this.invalidate_row_cache();
                        this.clear_selection();
                        this.search_results.clear();
                        this.search_stale = false;
                        this.search_has_completed = false;
                        this.duplicate_results.clear();
                        this.duplicate_stale = false;
                        this.duplicate_check_has_completed = false;
                        this.current_match = 0;
                        this.settings.remember_file(path.clone());
                        this.persist_settings();
                        this.notice = Some(format!("Saved {}", display_name(&path)).into());
                        let pending = this.pending_destructive.take();
                        match pending {
                            Some(PendingDestructiveAction::Open(next_path)) => {
                                this.open_path(next_path, cx);
                            }
                            Some(PendingDestructiveAction::Reload) => {
                                if let Some(reload_path) =
                                    this.loaded.as_ref().map(|loaded| loaded.path.clone())
                                {
                                    this.reload_path(reload_path, cx);
                                }
                            }
                            Some(PendingDestructiveAction::Clear) => {
                                this.loaded = None;
                                this.invalidate_row_cache();
                                this.clear_selection();
                                this.show_find = false;
                                this.notice = None;
                            }
                            Some(PendingDestructiveAction::Close)
                            | Some(PendingDestructiveAction::Quit) => cx.quit(),
                            None => {}
                        }
                    }
                    Err(error) if error.contains("Operation cancelled") => {
                        this.notice = Some("Save cancelled.".into());
                    }
                    Err(error) => {
                        this.error = Some(format!("Unable to save CSV: {error}").into());
                        this.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn show_find(&mut self, _: &Find, _window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        self.show_find = true;
        self.active_highlight = Some(ActiveHighlight::Search);
        cx.notify();
    }

    fn clear_search(&mut self, _: &ClearSearch, _window: &mut Window, cx: &mut Context<Self>) {
        self.search_refresh_token = self.search_refresh_token.wrapping_add(1);
        self.search_request_id = self.search_request_id.wrapping_add(1);
        if self.operation_kind == Some(OperationKind::Search) {
            if let Some(cancellation) = &self.operation_cancellation {
                cancellation.cancel();
            }
        }
        self.search_results.clear();
        self.current_match = 0;
        self.last_search_query = None;
        self.search_stale = false;
        self.search_has_completed = false;
        if self.active_highlight == Some(ActiveHighlight::Search) {
            self.active_highlight = None;
        }
        cx.notify();
    }

    fn hide_find(&mut self, cx: &mut Context<Self>) {
        self.show_find = false;
        cx.notify();
    }

    fn schedule_search(&mut self, cx: &mut Context<Self>) {
        self.search_refresh_token = self.search_refresh_token.wrapping_add(1);
        let token = self.search_refresh_token;
        self.search_stale = !self.search_results.is_empty();
        self.search_has_completed = false;
        if self.operation_kind == Some(OperationKind::Search) {
            if let Some(cancellation) = &self.operation_cancellation {
                cancellation.cancel();
            }
        }
        cx.notify();
        cx.spawn(async move |this, cx| {
            cx.background_executor()
                .timer(Duration::from_millis(450))
                .await;
            loop {
                let wait_for_operation = this.update(cx, |this, cx| {
                    if this.search_refresh_token != token || !this.show_find {
                        return false;
                    }
                    if this.loading {
                        return true;
                    }
                    this.run_search(cx);
                    false
                })?;
                if !wait_for_operation {
                    break;
                }
                cx.background_executor()
                    .timer(Duration::from_millis(50))
                    .await;
            }
            anyhow::Ok(())
        })
        .detach();
    }

    fn mark_results_stale(&mut self) {
        self.search_stale = !self.search_results.is_empty();
        self.search_has_completed = false;
        self.duplicate_stale = !self.duplicate_results.is_empty();
        self.duplicate_check_has_completed = false;
    }

    fn cycle_search_column(&mut self, cx: &mut Context<Self>) {
        let column_count = self
            .loaded
            .as_ref()
            .map(|loaded| loaded.headers.len())
            .unwrap_or(0);
        self.search_column = next_column_scope(self.search_column, column_count);
        self.schedule_search(cx);
    }

    fn toggle_search_match_case(&mut self, cx: &mut Context<Self>) {
        self.search_match_case = !self.search_match_case;
        self.schedule_search(cx);
    }

    fn toggle_search_whole_word(&mut self, cx: &mut Context<Self>) {
        self.search_whole_word = !self.search_whole_word;
        self.schedule_search(cx);
    }

    fn run_search(&mut self, cx: &mut Context<Self>) {
        if self.modal_active() || self.loading {
            return;
        }
        let Some(loaded) = &self.loaded else { return };
        let query = self.search_input.read(cx).value().to_string();
        if query.trim().is_empty() {
            self.search_results.clear();
            self.current_match = 0;
            self.last_search_query = None;
            self.search_stale = false;
            self.search_has_completed = false;
            cx.notify();
            return;
        }
        let document = loaded.document.clone();
        let requested_query = query.clone();
        self.search_refresh_token = self.search_refresh_token.wrapping_add(1);
        self.last_search_query = Some(query.clone());
        let column = self.search_column;
        let match_case = self.search_match_case;
        let whole_word = self.search_whole_word;
        let enable_indexing = self.settings.enable_indexing;
        self.search_request_id = self.search_request_id.wrapping_add(1);
        let request_id = self.search_request_id;
        self.loading = true;
        let cancellation = self.begin_cancellable_operation(OperationKind::Search);
        self.active_highlight = Some(ActiveHighlight::Search);
        self.search_results.clear();
        self.search_stale = false;
        self.search_has_completed = false;
        self.current_match = 0;
        self.error = None;
        self.notice = Some("Searching…".into());
        let progress = Arc::new(Mutex::new(QueryProgress::default()));
        self.track_query_progress(progress.clone(), OperationKind::Search, request_id, cx);
        cx.notify();
        let task_progress = progress.clone();
        let task = cx.background_spawn(async move {
            let update_progress = |new_results: &[usize], processed: usize, total: usize| {
                if let Ok(mut progress) = task_progress.lock() {
                    progress.processed = processed;
                    progress.total = total;
                    progress.found += new_results.len();
                    progress.pending.extend_from_slice(new_results);
                }
            };
            let mut document = document
                .lock()
                .map_err(|_| "CSV document lock was poisoned".to_string())?;
            if enable_indexing && !match_case {
                if let Some(column) = column {
                    let update_index_progress = |processed, total| {
                        update_progress(&[], processed, total);
                    };
                    document.ensure_search_index_for_column_cancellable(
                        column,
                        &cancellation,
                        Some(&update_index_progress),
                    )?;
                }
            }
            document.search_cancellable_streaming(
                &query,
                column,
                match_case,
                whole_word,
                &cancellation,
                &update_progress,
            )
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                let request_is_current = this.search_request_id == request_id;
                this.finish_cancellable_operation();
                this.loading = false;
                if !request_is_current {
                    cx.notify();
                    return;
                }
                match result {
                    Ok(matches) => {
                        this.search_results = matches;
                        this.current_match = 0;
                        let query_changed =
                            this.search_input.read(cx).value().as_ref() != requested_query;
                        this.search_stale = query_changed;
                        this.search_has_completed = !query_changed;
                        if !query_changed {
                            this.select_current_match();
                        }
                        this.notice = Some(
                            format!("{} search matches", format_count(this.search_results.len()))
                                .into(),
                        );
                        if query_changed {
                            this.schedule_search(cx);
                        }
                    }
                    Err(error) if error.contains("Operation cancelled") => {
                        this.notice = Some("Search cancelled.".into());
                    }
                    Err(error) => {
                        this.error = Some(format!("Search failed: {error}").into());
                        this.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn select_current_match(&mut self) {
        let (Some(loaded), Some(source_row)) = (
            &self.loaded,
            self.search_results.get(self.current_match).copied(),
        ) else {
            return;
        };
        if let Some(row) = loaded
            .document
            .try_lock()
            .ok()
            .and_then(|doc| doc.display_row_for_source(source_row))
        {
            self.row_scroll.scroll_to_item(row, ScrollStrategy::Center);
        }
    }

    fn next_search_result(&mut self, cx: &mut Context<Self>) {
        if !self.search_stale && !self.search_results.is_empty() {
            self.current_match = (self.current_match + 1) % self.search_results.len();
            self.select_current_match();
            cx.notify();
        }
    }

    fn previous_search_result(&mut self, cx: &mut Context<Self>) {
        if !self.search_stale && !self.search_results.is_empty() {
            self.current_match = if self.current_match == 0 {
                self.search_results.len() - 1
            } else {
                self.current_match - 1
            };
            self.select_current_match();
            cx.notify();
        }
    }

    fn next_match(&mut self, _: &NextMatch, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.modal_active() {
            self.next_search_result(cx);
        }
    }

    fn previous_match(&mut self, _: &PreviousMatch, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.modal_active() {
            self.previous_search_result(cx);
        }
    }

    fn check_duplicates(
        &mut self,
        _: &CheckDuplicates,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.modal_active() {
            return;
        }
        self.show_duplicates = true;
        self.active_highlight = Some(ActiveHighlight::Duplicates);
        cx.notify();
    }

    fn cycle_duplicate_column(&mut self, cx: &mut Context<Self>) {
        let column_count = self
            .loaded
            .as_ref()
            .map(|loaded| loaded.headers.len())
            .unwrap_or(0);
        self.duplicate_column = next_column_scope(self.duplicate_column, column_count);
        self.duplicate_stale = !self.duplicate_results.is_empty();
        self.duplicate_check_has_completed = false;
        cx.notify();
    }

    fn run_duplicate_check(&mut self, cx: &mut Context<Self>) {
        if self.modal_active() || self.loading {
            return;
        }
        let Some(loaded) = &self.loaded else { return };
        let document = loaded.document.clone();
        let column = self.duplicate_column;
        self.duplicate_request_id = self.duplicate_request_id.wrapping_add(1);
        let request_id = self.duplicate_request_id;
        self.loading = true;
        let cancellation = self.begin_cancellable_operation(OperationKind::Duplicates);
        self.active_highlight = Some(ActiveHighlight::Duplicates);
        self.duplicate_results.clear();
        self.duplicate_stale = false;
        self.duplicate_check_has_completed = false;
        self.current_duplicate_match = 0;
        self.error = None;
        self.notice = Some("Checking duplicates…".into());
        let progress = Arc::new(Mutex::new(QueryProgress::default()));
        self.track_query_progress(progress.clone(), OperationKind::Duplicates, request_id, cx);
        cx.notify();
        let task_progress = progress.clone();
        let task = cx.background_spawn(async move {
            let update_progress = |new_results: &[usize], processed: usize, total: usize| {
                if let Ok(mut progress) = task_progress.lock() {
                    progress.processed = processed;
                    progress.total = total;
                    progress.found += new_results.len();
                    progress.pending.extend_from_slice(new_results);
                }
            };
            document
                .lock()
                .map_err(|_| "CSV document lock was poisoned".to_string())?
                .find_duplicates_cancellable_streaming(column, &cancellation, &update_progress)
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                let request_is_current = this.duplicate_request_id == request_id;
                this.finish_cancellable_operation();
                this.loading = false;
                if !request_is_current {
                    cx.notify();
                    return;
                }
                match result {
                    Ok(matches) => {
                        this.duplicate_results = matches;
                        this.duplicate_stale = false;
                        this.duplicate_check_has_completed = true;
                        this.current_duplicate_match = 0;
                        this.select_current_duplicate();
                        this.notice = Some(
                            format!(
                                "{} rows are part of duplicate groups",
                                format_count(this.duplicate_results.len())
                            )
                            .into(),
                        );
                    }
                    Err(error) if error.contains("Operation cancelled") => {
                        this.notice = Some("Duplicate check cancelled.".into());
                    }
                    Err(error) => {
                        this.error = Some(format!("Duplicate check failed: {error}").into());
                        this.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn select_current_duplicate(&mut self) {
        let (Some(loaded), Some(source_row)) = (
            &self.loaded,
            self.duplicate_results
                .get(self.current_duplicate_match)
                .copied(),
        ) else {
            return;
        };
        if let Some(row) = loaded
            .document
            .try_lock()
            .ok()
            .and_then(|doc| doc.display_row_for_source(source_row))
        {
            self.row_scroll.scroll_to_item(row, ScrollStrategy::Center);
        }
    }

    fn previous_duplicate(&mut self, cx: &mut Context<Self>) {
        if self.duplicate_stale || self.duplicate_results.is_empty() {
            return;
        }
        self.current_duplicate_match = if self.current_duplicate_match == 0 {
            self.duplicate_results.len() - 1
        } else {
            self.current_duplicate_match - 1
        };
        self.select_current_duplicate();
        cx.notify();
    }

    fn next_duplicate(&mut self, cx: &mut Context<Self>) {
        if self.duplicate_stale || self.duplicate_results.is_empty() {
            return;
        }
        self.current_duplicate_match =
            (self.current_duplicate_match + 1) % self.duplicate_results.len();
        self.select_current_duplicate();
        cx.notify();
    }

    fn clear_duplicates(&mut self, cx: &mut Context<Self>) {
        self.duplicate_request_id = self.duplicate_request_id.wrapping_add(1);
        if self.operation_kind == Some(OperationKind::Duplicates) {
            if let Some(cancellation) = &self.operation_cancellation {
                cancellation.cancel();
            }
        }
        self.duplicate_results.clear();
        self.current_duplicate_match = 0;
        self.duplicate_stale = false;
        self.duplicate_check_has_completed = false;
        if self.active_highlight == Some(ActiveHighlight::Duplicates) {
            self.active_highlight = None;
        }
        cx.notify();
    }

    fn hide_duplicates(&mut self, cx: &mut Context<Self>) {
        self.show_duplicates = false;
        cx.notify();
    }

    fn sort_column(&mut self, column: usize, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        if self.pending_cell_commits > 0 {
            self.pending_edit_action = Some(PendingEditAction::SortColumn(column));
            return;
        }
        let Some(loaded) = &self.loaded else { return };
        if self.loading {
            return;
        }
        let document = loaded.document.clone();
        let row_count = loaded.row_count;
        let next = document
            .try_lock()
            .ok()
            .and_then(|doc| match doc.sort_spec() {
                Some(spec)
                    if spec.column == column && spec.direction == SortDirection::Ascending =>
                {
                    Some(Some(SortSpec {
                        column,
                        direction: SortDirection::Descending,
                    }))
                }
                Some(spec)
                    if spec.column == column && spec.direction == SortDirection::Descending =>
                {
                    Some(None)
                }
                _ => Some(Some(SortSpec {
                    column,
                    direction: SortDirection::Ascending,
                })),
            });
        let Some(next) = next else { return };
        self.loading = true;
        let cancellation = self.begin_cancellable_operation(OperationKind::Sort);
        self.error = None;
        self.notice = Some("Sorting…".into());
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_row_progress(
            progress.clone(),
            row_count,
            OperationKind::Sort,
            "Sorting",
            cx,
        );
        cx.notify();
        let task = cx.background_spawn(async move {
            let update_progress = |processed, _| progress.store(processed, Ordering::Relaxed);
            document
                .lock()
                .map_err(|_| "CSV document lock was poisoned".to_string())?
                .sort_cancellable_with_progress(next, &cancellation, &update_progress)
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                this.finish_cancellable_operation();
                this.loading = false;
                if let Err(error) = result {
                    if error.contains("Operation cancelled") {
                        this.notice = Some("Sort cancelled.".into());
                    } else {
                        this.error = Some(format!("Sort failed: {error}").into());
                        this.notice = None;
                    }
                } else {
                    this.invalidate_row_cache();
                    this.notice = Some("Sort complete.".into());
                }
                this.clear_selection();
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn selected_display_rows(&self) -> Vec<usize> {
        let mut rows = self.selected_rows.iter().collect::<Vec<_>>();
        if rows.is_empty() {
            rows.extend(self.selected_row);
        }
        rows
    }

    fn toggle_delete_selected(&mut self, cx: &mut Context<Self>) {
        let Some(primary_row) = self.selected_row else {
            return;
        };
        let Some(loaded) = &self.loaded else { return };
        let mutation = match loaded.document.try_lock() {
            Ok(document) if document.is_display_row_deleted(primary_row) => RowMutation::Restore,
            Ok(_) => RowMutation::Delete,
            Err(_) => {
                self.notice = Some("Rows are still loading; try the row action again.".into());
                cx.notify();
                return;
            }
        };
        self.mutate_selected_rows(mutation, cx);
    }

    fn delete_selected_rows_action(
        &mut self,
        _: &DeleteSelectedRows,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selected_rows(RowMutation::Delete, cx);
    }

    fn restore_selected_rows_action(
        &mut self,
        _: &RestoreSelectedRows,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selected_rows(RowMutation::Restore, cx);
    }

    fn mutate_selected_rows(&mut self, mutation: RowMutation, cx: &mut Context<Self>) {
        if self.modal_active() || self.loading || self.selected_row.is_none() {
            return;
        }
        let rows = self.selected_display_rows();
        if self.pending_cell_commits > 0 {
            self.pending_edit_action = Some(PendingEditAction::MutateRows { rows, mutation });
            return;
        }
        self.mutate_rows(rows, mutation, cx);
    }

    fn mutate_rows(&mut self, rows: Vec<usize>, mutation: RowMutation, cx: &mut Context<Self>) {
        if mutation == RowMutation::Delete && rows.len() >= DELETE_CONFIRM_THRESHOLD {
            self.pending_bulk_action = Some(PendingBulkAction::Delete { rows });
            cx.notify();
            return;
        }
        if rows.len() >= DELETE_CONFIRM_THRESHOLD {
            self.mutate_rows_background(rows, mutation, cx);
            return;
        }
        let Some(loaded) = &self.loaded else { return };
        let Ok(mut document) = loaded.document.try_lock() else {
            self.notice = Some("Rows are still loading; try the row action again.".into());
            cx.notify();
            return;
        };
        match document.set_display_rows_deleted(&rows, mutation.deleted()) {
            Err(error) => self.error = Some(error.into()),
            Ok(changed) => {
                let dirty = document.is_dirty();
                drop(document);
                if let Some(loaded) = &mut self.loaded {
                    loaded.dirty = dirty;
                }
                self.mark_results_stale();
                self.invalidate_row_cache();
                self.notice = Some(
                    format!(
                        "{} {} {}.{}",
                        mutation.past_tense(),
                        format_count(changed),
                        counted_noun(changed, "row", "rows"),
                        if dirty {
                            " Save to write changes to disk."
                        } else {
                            " All row changes are reverted."
                        }
                    )
                    .into(),
                );
            }
        }
        cx.notify();
    }

    fn mutate_rows_background(
        &mut self,
        rows: Vec<usize>,
        mutation: RowMutation,
        cx: &mut Context<Self>,
    ) {
        let Some(document) = self.loaded.as_ref().map(|loaded| loaded.document.clone()) else {
            return;
        };
        let row_count = rows.len();
        self.loading = true;
        let cancellation = self.begin_cancellable_operation(OperationKind::Rows);
        self.notice = Some(
            format!(
                "{} {} rows…",
                mutation.past_tense().trim_end_matches('d'),
                format_count(row_count)
            )
            .into(),
        );
        cx.notify();
        let task = cx.background_spawn(async move {
            let mut document = document
                .lock()
                .map_err(|_| "CSV document lock was poisoned".to_string())?;
            let changed = document.set_display_rows_deleted_cancellable(
                &rows,
                mutation.deleted(),
                &cancellation,
            )?;
            Ok::<_, String>((document.is_dirty(), changed))
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                this.finish_cancellable_operation();
                this.loading = false;
                match result {
                    Ok((dirty, changed)) => {
                        if let Some(loaded) = &mut this.loaded {
                            loaded.dirty = dirty;
                        }
                        this.mark_results_stale();
                        this.invalidate_row_cache();
                        this.notice = Some(
                            format!(
                                "{} {} {}. Save to write changes to disk.",
                                mutation.past_tense(),
                                format_count(changed),
                                counted_noun(changed, "row", "rows")
                            )
                            .into(),
                        );
                    }
                    Err(error) if error.contains("Operation cancelled") => {
                        this.notice = Some(format!("{} cancelled.", mutation.past_tense()).into());
                    }
                    Err(error) => {
                        this.error = Some(error.into());
                        this.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn begin_cell_edit(
        &mut self,
        display_row: usize,
        source_row: usize,
        column: usize,
        value: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.loading
            || self
                .row_cache
                .get(&display_row)
                .is_none_or(|row| row.deleted || row.source_row != source_row)
        {
            return;
        }
        self.selected_row = Some(display_row);
        self.selected_rows.select_only(display_row);
        self.selection_anchor = Some(display_row);
        self.editing_cell = Some(EditingCell {
            display_row,
            source_row,
            column,
            initial_value: value.clone(),
        });
        self.editing_draft_dirty = false;
        self.edit_input
            .update(cx, |input, cx| input.set_value(value, window, cx));
        cx.on_next_frame(window, |this, window, cx| {
            if this.editing_cell.is_none() {
                return;
            }
            this.edit_input
                .update(cx, |input, cx| input.focus(window, cx));
            window.dispatch_action(Box::new(InputSelectAll), cx);
        });
        cx.notify();
    }

    fn cancel_cell_edit(&mut self, cx: &mut Context<Self>) {
        if !self.loading {
            self.editing_cell = None;
            self.editing_draft_dirty = false;
            self.pending_edit_action = None;
            cx.notify();
        }
    }

    fn continue_pending_edit_action(&mut self, cx: &mut Context<Self>) {
        match self.pending_edit_action.take() {
            Some(PendingEditAction::OpenDialog) => self.prompt_open_dialog(cx),
            Some(PendingEditAction::Save) => {
                if let Some(path) = self
                    .loaded
                    .as_ref()
                    .filter(|loaded| loaded.dirty)
                    .map(|loaded| loaded.path.clone())
                {
                    self.save_to(path, cx);
                }
            }
            Some(PendingEditAction::SaveAs) => self.prompt_save_as(cx),
            Some(PendingEditAction::SortColumn(column)) => self.sort_column(column, cx),
            Some(PendingEditAction::MutateRows { rows, mutation }) => {
                self.mutate_rows(rows, mutation, cx)
            }
            None => {}
        }
    }

    fn commit_cell_edit(&mut self, cx: &mut Context<Self>) {
        if self.loading {
            return;
        }
        let (Some(loaded), Some(editing)) = (&self.loaded, self.editing_cell.clone()) else {
            return;
        };
        let value = self.edit_input.read(cx).value().to_string();
        self.editing_cell = None;
        self.editing_draft_dirty = false;
        if value == editing.initial_value {
            cx.notify();
            if self.pending_cell_commits == 0 {
                self.continue_pending_edit_action(cx);
            }
            return;
        }

        if let Some(row) = self
            .row_cache
            .get_mut(&editing.display_row)
            .filter(|row| row.source_row == editing.source_row)
            && let Some(cell) = row.cells.get_mut(editing.column)
        {
            *cell = value.clone();
        }
        self.cell_commit_queue.push_back(CellCommit {
            editing,
            value,
            document: loaded.document.clone(),
        });
        self.pending_cell_commits = self.pending_cell_commits.saturating_add(1);
        self.error = None;
        self.notice = Some("Applying cell edit…".into());
        self.start_next_cell_commit(cx);
        cx.notify();
    }

    fn start_next_cell_commit(&mut self, cx: &mut Context<Self>) {
        if self.cell_commit_in_flight {
            return;
        }
        let Some(commit) = self.cell_commit_queue.pop_front() else {
            if self.pending_cell_commits == 0 {
                self.continue_pending_edit_action(cx);
            }
            return;
        };
        self.cell_commit_in_flight = true;
        let CellCommit {
            editing,
            value,
            document,
        } = commit;
        let source_row = editing.source_row;
        let display_row = editing.display_row;
        let column = editing.column;
        let initial_value = editing.initial_value.clone();
        let committed_value = value.clone();
        let task = cx.background_spawn({
            let document = document.clone();
            async move {
                let mut document = document
                    .lock()
                    .map_err(|_| "CSV document lock was poisoned".to_string())?;
                document.edit_source_cell(source_row, column, value)?;
                Ok::<_, String>(document.is_dirty())
            }
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                this.cell_commit_in_flight = false;
                this.pending_cell_commits = this.pending_cell_commits.saturating_sub(1);
                let is_current_document = this
                    .loaded
                    .as_ref()
                    .is_some_and(|loaded| Arc::ptr_eq(&loaded.document, &document));
                if is_current_document {
                    match result {
                        Ok(dirty) => {
                            if let Some(loaded) = &mut this.loaded {
                                loaded.dirty = dirty;
                            }
                            this.invalidate_row_cache();
                            this.mark_results_stale();
                            this.notice = Some(
                                if dirty {
                                    "Cell updated. Save to write changes to disk."
                                } else {
                                    "Cell value restored; there are no unsaved row changes."
                                }
                                .into(),
                            );
                        }
                        Err(error) => {
                            if let Some(row) = this
                                .row_cache
                                .get_mut(&display_row)
                                .filter(|row| row.source_row == source_row)
                                && let Some(cell) = row.cells.get_mut(column)
                                && *cell == committed_value
                            {
                                *cell = initial_value;
                            }
                            if this.cell_commit_queue.is_empty() && this.editing_cell.is_none() {
                                this.editing_cell = Some(editing);
                            }
                            this.pending_edit_action = None;
                            this.error = Some(format!("Unable to edit cell: {error}").into());
                            this.notice = None;
                        }
                    }
                }
                this.start_next_cell_commit(cx);
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn copy_selected_action(
        &mut self,
        _: &CopySelected,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.copy_selected(window, cx);
    }

    fn copy_context_cell(
        &mut self,
        _: &CopyContextCell,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some((_, _, _, value)) = self.context_cell.take() {
            cx.write_to_clipboard(ClipboardItem::new_string(value));
            self.notice = Some("Cell copied.".into());
            cx.notify();
        }
    }

    fn search_context_cell(
        &mut self,
        _: &SearchContextCell,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some((_, _, column, value)) = self.context_cell.take() {
            self.show_find = true;
            self.search_column = Some(column);
            self.search_input
                .update(cx, |input, cx| input.set_value(value, window, cx));
            self.run_search(cx);
        }
    }

    fn edit_context_cell(
        &mut self,
        _: &EditContextCell,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some((display_row, source_row, column, value)) = self.context_cell.take() {
            self.begin_cell_edit(display_row, source_row, column, value, window, cx);
        }
    }

    fn cancel_cell_edit_action(
        &mut self,
        _: &InputEscape,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.cancel_cell_edit(cx);
    }

    fn toggle_delete_selected_action(
        &mut self,
        _: &ToggleDeleteSelected,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_delete_selected(cx);
    }

    fn select_row_from_click(&mut self, row: usize, event: &ClickEvent, cx: &mut Context<Self>) {
        let modifiers = event.modifiers();
        if modifiers.shift {
            let anchor = *self.selection_anchor.get_or_insert(row);
            self.selected_rows.select_only_range(anchor, row);
            self.selected_row = Some(row);
        } else if modifiers.control || modifiers.platform {
            let selected = self.selected_rows.toggle(row);
            self.selected_row = selected
                .then_some(row)
                .or_else(|| self.selected_rows.first());
            self.selection_anchor = Some(row);
        } else {
            self.selected_rows.select_only(row);
            self.selected_row = Some(row);
            self.selection_anchor = Some(row);
        }
        self.cell_selection = None;
        self.cell_dragging = false;
        cx.notify();
    }

    fn begin_cell_selection(
        &mut self,
        row: usize,
        column: usize,
        event: &MouseDownEvent,
        cx: &mut Context<Self>,
    ) {
        if self.modal_active() || self.loading {
            return;
        }
        if event.modifiers.shift {
            if let Some(selection) = &mut self.cell_selection {
                selection.set_active(row, column);
            } else {
                self.cell_selection = Some(CellSelection::single(row, column));
            }
        } else {
            self.cell_selection = Some(CellSelection::single(row, column));
        }
        self.cell_dragging = true;
        self.sync_rows_to_cell_selection();
        cx.notify();
    }

    fn drag_cell_selection(&mut self, row: usize, column: usize, cx: &mut Context<Self>) {
        if !self.cell_dragging {
            return;
        }
        if let Some(selection) = &mut self.cell_selection {
            selection.set_active(row, column);
            self.sync_rows_to_cell_selection();
            cx.notify();
        }
    }

    fn sync_rows_to_cell_selection(&mut self) {
        let Some(selection) = self.cell_selection else {
            return;
        };
        let rows = selection.rows();
        let start = *rows.start();
        let end = *rows.end();
        self.selected_rows.select_only_range(start, end);
        self.selected_row = Some(selection.active().row);
        self.selection_anchor = Some(start);
    }

    fn navigate_cell(
        &mut self,
        row_delta: isize,
        column_delta: isize,
        extend: bool,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(mut selection) = self.cell_selection else {
            return false;
        };
        let Some(loaded) = &self.loaded else {
            return false;
        };
        if loaded.row_count == 0 || loaded.headers.is_empty() {
            return false;
        }
        let active = selection.active();
        let row = active
            .row
            .saturating_add_signed(row_delta)
            .min(loaded.row_count - 1);
        let column = active
            .column
            .saturating_add_signed(column_delta)
            .min(loaded.headers.len() - 1);
        selection.move_to(row, column, extend);
        self.cell_selection = Some(selection);
        self.sync_rows_to_cell_selection();
        self.row_scroll.scroll_to_item(row, ScrollStrategy::Center);
        cx.notify();
        true
    }

    fn select_row_and_scroll(&mut self, row: usize, cx: &mut Context<Self>) {
        self.navigate_row_and_scroll(row, false, cx);
    }

    fn navigate_row_and_scroll(&mut self, row: usize, extend: bool, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        let Some(row_count) = self.loaded.as_ref().map(|loaded| loaded.row_count) else {
            return;
        };
        if row_count == 0 {
            return;
        }
        let row = row.min(row_count - 1);
        self.cell_selection = None;
        self.cell_dragging = false;
        if extend {
            let current = self.selected_row.unwrap_or(row);
            let anchor = *self.selection_anchor.get_or_insert(current);
            self.selected_rows.select_only_range(anchor, row);
        } else {
            self.selected_rows.select_only(row);
            self.selection_anchor = Some(row);
        }
        self.selected_row = Some(row);
        self.row_scroll.scroll_to_item(row, ScrollStrategy::Center);
        cx.notify();
    }

    fn page_step(&self) -> usize {
        self.desired_row_range
            .as_ref()
            .map(|range| range.end.saturating_sub(range.start).saturating_sub(1))
            .unwrap_or(20)
            .clamp(1, 100)
    }

    fn move_context_menu_focus(&mut self, delta: isize, cx: &mut Context<Self>) -> bool {
        let Some(menu) = &mut self.table_context_menu else {
            return false;
        };
        let count = context_menu_item_count(menu.kind);
        menu.focused_item = menu
            .focused_item
            .saturating_add_signed(delta)
            .min(count - 1);
        cx.notify();
        true
    }

    fn activate_context_menu(
        &mut self,
        _: &ActivateContextMenu,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(menu) = self.table_context_menu.take() else {
            return;
        };
        match context_menu_command(menu.kind, menu.focused_item) {
            ContextMenuCommand::CopyCell => self.copy_context_cell(&CopyContextCell, window, cx),
            ContextMenuCommand::SearchCell => {
                self.search_context_cell(&SearchContextCell, window, cx)
            }
            ContextMenuCommand::EditCell => self.edit_context_cell(&EditContextCell, window, cx),
            ContextMenuCommand::DeleteRows => {
                self.context_cell = None;
                self.mutate_selected_rows(RowMutation::Delete, cx);
            }
            ContextMenuCommand::RestoreRows => {
                self.context_cell = None;
                self.mutate_selected_rows(RowMutation::Restore, cx);
            }
            ContextMenuCommand::CopySelection => {
                self.context_cell = None;
                self.copy_selected(window, cx);
            }
        }
        cx.notify();
    }

    fn select_previous_row(
        &mut self,
        _: &SelectPreviousRow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.move_context_menu_focus(-1, cx) {
            return;
        }
        if !self.navigate_cell(-1, 0, false, cx) {
            self.select_row_and_scroll(self.selected_row.unwrap_or(0).saturating_sub(1), cx);
        }
    }

    fn select_next_row(&mut self, _: &SelectNextRow, _window: &mut Window, cx: &mut Context<Self>) {
        if self.move_context_menu_focus(1, cx) {
            return;
        }
        if !self.navigate_cell(1, 0, false, cx) {
            self.select_row_and_scroll(
                self.selected_row.map_or(0, |row| row.saturating_add(1)),
                cx,
            );
        }
    }

    fn select_previous_column(
        &mut self,
        _: &SelectPreviousColumn,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_cell(0, -1, false, cx);
    }

    fn select_next_column(
        &mut self,
        _: &SelectNextColumn,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_cell(0, 1, false, cx);
    }

    fn extend_previous_column(
        &mut self,
        _: &ExtendPreviousColumn,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_cell(0, -1, true, cx);
    }

    fn extend_next_column(
        &mut self,
        _: &ExtendNextColumn,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_cell(0, 1, true, cx);
    }

    fn select_first_row(
        &mut self,
        _: &SelectFirstRow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.select_row_and_scroll(0, cx);
    }

    fn select_last_row(&mut self, _: &SelectLastRow, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(last) = self
            .loaded
            .as_ref()
            .and_then(|loaded| loaded.row_count.checked_sub(1))
        {
            self.select_row_and_scroll(last, cx);
        }
    }

    fn extend_previous_row(
        &mut self,
        _: &ExtendPreviousRow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.navigate_cell(-1, 0, true, cx) {
            let row = self.selected_row.unwrap_or(0).saturating_sub(1);
            self.navigate_row_and_scroll(row, true, cx);
        }
    }

    fn extend_next_row(&mut self, _: &ExtendNextRow, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.navigate_cell(1, 0, true, cx) {
            let row = self.selected_row.map_or(0, |row| row.saturating_add(1));
            self.navigate_row_and_scroll(row, true, cx);
        }
    }

    fn extend_first_row(
        &mut self,
        _: &ExtendFirstRow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_row_and_scroll(0, true, cx);
    }

    fn extend_last_row(&mut self, _: &ExtendLastRow, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(last) = self
            .loaded
            .as_ref()
            .and_then(|loaded| loaded.row_count.checked_sub(1))
        {
            self.navigate_row_and_scroll(last, true, cx);
        }
    }

    fn select_page_up(&mut self, _: &SelectPageUp, _window: &mut Window, cx: &mut Context<Self>) {
        let row = self
            .selected_row
            .unwrap_or(0)
            .saturating_sub(self.page_step());
        self.navigate_row_and_scroll(row, false, cx);
    }

    fn select_page_down(
        &mut self,
        _: &SelectPageDown,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let row = self
            .selected_row
            .unwrap_or(0)
            .saturating_add(self.page_step());
        self.navigate_row_and_scroll(row, false, cx);
    }

    fn extend_page_up(&mut self, _: &ExtendPageUp, _window: &mut Window, cx: &mut Context<Self>) {
        let row = self
            .selected_row
            .unwrap_or(0)
            .saturating_sub(self.page_step());
        self.navigate_row_and_scroll(row, true, cx);
    }

    fn extend_page_down(
        &mut self,
        _: &ExtendPageDown,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let row = self
            .selected_row
            .unwrap_or(0)
            .saturating_add(self.page_step());
        self.navigate_row_and_scroll(row, true, cx);
    }

    fn select_all_rows(&mut self, _: &SelectAllRows, _window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        let Some(row_count) = self.loaded.as_ref().map(|loaded| loaded.row_count) else {
            return;
        };
        self.cell_selection = None;
        self.cell_dragging = false;
        self.selected_rows.select_all(row_count);
        self.selected_row = (row_count > 0).then_some(0);
        self.selection_anchor = self.selected_row;
        cx.notify();
    }

    fn clear_row_selection(
        &mut self,
        _: &ClearRowSelection,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.show_settings {
            self.close_settings(cx);
            return;
        }
        if self.show_shortcuts || self.show_about {
            self.close_info_modal(cx);
            return;
        }
        if self.show_header_prompt {
            self.resolve_header_prompt(None, cx);
            return;
        }
        if self.pending_external_save.is_some() {
            self.cancel_external_save(cx);
            return;
        }
        if self.pending_bulk_action.is_some() {
            self.cancel_pending_bulk_action(cx);
            return;
        }
        if self.pending_destructive.is_some() {
            self.cancel_pending_destructive(cx);
            return;
        }
        if self.table_context_menu.take().is_some() {
            cx.notify();
            return;
        }
        self.clear_selection();
        cx.notify();
    }

    fn toggle_index(&mut self, _: &ToggleIndex, _window: &mut Window, cx: &mut Context<Self>) {
        self.settings.show_index = !self.settings.show_index;
        self.persist_settings();
        cx.notify();
    }

    fn compact_rows(&mut self, _: &CompactRows, _window: &mut Window, cx: &mut Context<Self>) {
        self.set_density(RowDensity::Compact, cx);
    }

    fn default_rows(&mut self, _: &DefaultRows, _window: &mut Window, cx: &mut Context<Self>) {
        self.set_density(RowDensity::Default, cx);
    }

    fn spacious_rows(&mut self, _: &SpaciousRows, _window: &mut Window, cx: &mut Context<Self>) {
        self.set_density(RowDensity::Spacious, cx);
    }

    fn set_density(&mut self, density: RowDensity, cx: &mut Context<Self>) {
        self.settings.row_density = density;
        self.persist_settings();
        cx.notify();
    }

    fn toggle_theme(&mut self, _: &ToggleTheme, window: &mut Window, cx: &mut Context<Self>) {
        let (preference, mode) = if cx.theme().mode.is_dark() {
            (ThemePreference::Light, ThemeMode::Light)
        } else {
            (ThemePreference::Dark, ThemeMode::Dark)
        };
        self.settings.theme = preference;
        Theme::change(mode, Some(window), cx);
        self.persist_settings();
        cx.notify();
    }

    fn open_settings(&mut self, _: &OpenSettings, _window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() {
            return;
        }
        self.show_settings = true;
        cx.notify();
    }

    fn open_parse_settings(
        &mut self,
        _: &OpenParseSettings,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_settings(&OpenSettings, _window, cx);
    }

    fn show_shortcuts(&mut self, _: &ShowShortcuts, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.modal_active() {
            self.show_shortcuts = true;
            cx.notify();
        }
    }

    fn show_about(&mut self, _: &ShowAbout, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.modal_active() {
            self.show_about = true;
            cx.notify();
        }
    }

    fn close_settings(&mut self, cx: &mut Context<Self>) {
        self.show_settings = false;
        cx.notify();
    }

    fn close_info_modal(&mut self, cx: &mut Context<Self>) {
        self.show_shortcuts = false;
        self.show_about = false;
        cx.notify();
    }

    fn apply_settings_choice(
        &mut self,
        choice: SettingsChoice,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let updates_parse_settings = !matches!(
            &choice,
            SettingsChoice::Theme(_) | SettingsChoice::Density(_)
        );
        let previous_parse_settings = self.settings.parse_overrides.clone();
        match choice {
            SettingsChoice::Theme(theme) => {
                self.set_theme_preference(theme, window, cx);
                return;
            }
            SettingsChoice::Density(density) => self.settings.row_density = density,
            SettingsChoice::Delimiter(value) => {
                self.settings.parse_overrides.delimiter = value.map(str::to_string)
            }
            SettingsChoice::Quote(value) => {
                self.settings.parse_overrides.quote = value.map(str::to_string)
            }
            SettingsChoice::Escape(value) => {
                self.settings.parse_overrides.escape = value.map(str::to_string)
            }
            SettingsChoice::Comment(value) => {
                self.settings.parse_overrides.comment = value.map(str::to_string)
            }
            SettingsChoice::ExcelSep(value) => self.settings.parse_overrides.excel_sep = value,
            SettingsChoice::LineEnding(value) => {
                self.settings.parse_overrides.line_ending = value.map(str::to_string)
            }
            SettingsChoice::Encoding(value) => {
                self.settings.parse_overrides.encoding = value.map(str::to_string)
            }
            SettingsChoice::Headers(value) => self.settings.parse_overrides.has_headers = value,
            SettingsChoice::Malformed(value) => {
                self.settings.parse_overrides.malformed = value.map(str::to_string)
            }
            SettingsChoice::MaxFieldSize(value) => {
                self.settings.parse_overrides.max_field_size = value
            }
            SettingsChoice::MaxRecordSize(value) => {
                self.settings.parse_overrides.max_record_size = value
            }
        }
        if updates_parse_settings {
            if let Err(error) = validate_syntax_overrides(
                &self.settings.parse_overrides,
                self.loaded.as_ref().map(|loaded| &loaded.parse_info),
            ) {
                self.settings.parse_overrides = previous_parse_settings;
                self.error = Some(error.into());
                cx.notify();
                return;
            }
        }
        self.error = None;
        self.persist_settings();
        cx.notify();
    }

    fn apply_custom_delimiter(&mut self, cx: &mut Context<Self>) {
        let value = self.custom_delimiter_input.read(cx).value().to_string();
        self.apply_custom_syntax_character(
            "delimiter",
            value,
            |overrides, value| overrides.delimiter = Some(value),
            cx,
        );
    }

    fn apply_custom_quote(&mut self, cx: &mut Context<Self>) {
        let value = self.custom_quote_input.read(cx).value().to_string();
        self.apply_custom_syntax_character(
            "quote",
            value,
            |overrides, value| overrides.quote = Some(value),
            cx,
        );
    }

    fn apply_custom_escape(&mut self, cx: &mut Context<Self>) {
        let value = self.custom_escape_input.read(cx).value().to_string();
        self.apply_custom_syntax_character(
            "escape",
            value,
            |overrides, value| overrides.escape = Some(value),
            cx,
        );
    }

    fn apply_custom_comment(&mut self, cx: &mut Context<Self>) {
        let value = self.custom_comment_input.read(cx).value().to_string();
        self.apply_custom_syntax_character(
            "comment",
            value,
            |overrides, value| overrides.comment = Some(value),
            cx,
        );
    }

    fn apply_custom_syntax_character(
        &mut self,
        name: &str,
        value: String,
        apply: impl FnOnce(&mut ParseOverrides, String),
        cx: &mut Context<Self>,
    ) {
        if !is_valid_syntax_character(&value) {
            self.error = Some(
                format!(
                    "Custom {name} must be exactly one character and cannot be NUL, CR, or LF."
                )
                .into(),
            );
            cx.notify();
            return;
        }
        let mut candidate = self.settings.parse_overrides.clone();
        apply(&mut candidate, value);
        if let Err(error) = validate_syntax_overrides(
            &candidate,
            self.loaded.as_ref().map(|loaded| &loaded.parse_info),
        ) {
            self.error = Some(error.into());
            cx.notify();
            return;
        }
        self.settings.parse_overrides = candidate;
        self.error = None;
        self.persist_settings();
        cx.notify();
    }

    fn resolve_header_prompt(&mut self, use_first_row: Option<bool>, cx: &mut Context<Self>) {
        self.show_header_prompt = false;
        if let Some(use_first_row) = use_first_row {
            self.settings.parse_overrides.has_headers = Some(use_first_row);
            self.persist_settings();
            if let Some(path) = self.loaded.as_ref().map(|loaded| loaded.path.clone()) {
                self.reload_path(path, cx);
                return;
            }
        }
        cx.notify();
    }

    fn reload_with_parse_settings(&mut self, cx: &mut Context<Self>) {
        self.show_settings = false;
        let Some(path) = self.loaded.as_ref().map(|loaded| loaded.path.clone()) else {
            cx.notify();
            return;
        };
        if self.is_dirty() {
            self.pending_destructive = Some(PendingDestructiveAction::Reload);
            cx.notify();
            return;
        }
        self.reload_path(path, cx);
    }

    fn reset_parse_settings(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.settings.parse_overrides = Default::default();
        for input in [
            self.custom_delimiter_input.clone(),
            self.custom_quote_input.clone(),
            self.custom_escape_input.clone(),
            self.custom_comment_input.clone(),
        ] {
            input.update(cx, |input, cx| input.set_value("", window, cx));
        }
        self.error = None;
        self.persist_settings();
        cx.notify();
    }

    fn toggle_search_indexing(&mut self, cx: &mut Context<Self>) {
        self.settings.enable_indexing = !self.settings.enable_indexing;
        let enabled = self.settings.enable_indexing;
        self.persist_settings();
        let Some(document) = self.loaded.as_ref().map(|loaded| loaded.document.clone()) else {
            cx.notify();
            return;
        };
        if enabled {
            self.notice = Some(
                "Search indexing enabled; the selected column will be indexed on demand.".into(),
            );
            cx.notify();
        } else {
            match document.try_lock() {
                Ok(mut document) => {
                    document.clear_search_index();
                    self.notice = Some("Search index removed.".into());
                }
                Err(_) => {
                    self.notice = Some(
                        "Search indexing is disabled; the current operation is finishing.".into(),
                    );
                }
            }
            cx.notify();
        }
    }

    fn set_theme_preference(
        &mut self,
        preference: ThemePreference,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.settings.theme = preference;
        let mode = match preference {
            ThemePreference::Light => ThemeMode::Light,
            ThemePreference::Dark => ThemeMode::Dark,
            ThemePreference::System => ThemeMode::from(window.appearance()),
        };
        Theme::change(mode, Some(window), cx);
        self.persist_settings();
        cx.notify();
    }

    fn copy_selected(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if self.modal_active() || self.loading {
            return;
        }
        if let Some(selection) = self.cell_selection {
            let rows = selection.rows();
            let columns = selection.columns();
            let row_start = *rows.start();
            let row_end = *rows.end();
            let column_start = *columns.start();
            let column_end = *columns.end();
            if row_end - row_start + 1 >= COPY_CONFIRM_THRESHOLD {
                self.pending_bulk_action = Some(PendingBulkAction::CopyCells {
                    row_start,
                    row_end,
                    column_start,
                    column_end,
                });
                cx.notify();
            } else {
                self.copy_cell_range(row_start, row_end, column_start, column_end, cx);
            }
            return;
        }
        let mut rows = self.selected_rows.iter().collect::<Vec<_>>();
        if rows.is_empty() {
            rows.extend(self.selected_row);
        }
        if rows.is_empty() {
            return;
        }
        rows.sort_unstable();
        if rows.len() >= COPY_CONFIRM_THRESHOLD {
            self.pending_bulk_action = Some(PendingBulkAction::Copy { rows });
            cx.notify();
            return;
        }
        self.copy_rows(rows, cx);
    }

    fn copy_rows(&mut self, rows: Vec<usize>, cx: &mut Context<Self>) {
        let Some(document) = self.loaded.as_ref().map(|loaded| loaded.document.clone()) else {
            return;
        };
        self.loading = true;
        let cancellation = self.begin_cancellable_operation(OperationKind::Copy);
        self.notice = Some(
            format!(
                "Copying {} {}…",
                format_count(rows.len()),
                counted_noun(rows.len(), "row", "rows")
            )
            .into(),
        );
        let row_count = rows.len();
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_row_progress(
            progress.clone(),
            row_count,
            OperationKind::Copy,
            "Copying",
            cx,
        );
        cx.notify();
        let task = cx.background_spawn(async move {
            let update_progress = |processed, _| progress.store(processed, Ordering::Relaxed);
            document
                .lock()
                .map_err(|_| "CSV document lock was poisoned".to_string())?
                .serialize_display_rows_cancellable_with_progress(
                    &rows,
                    &cancellation,
                    &update_progress,
                )
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                this.finish_cancellable_operation();
                this.loading = false;
                match result {
                    Ok(text) => {
                        cx.write_to_clipboard(ClipboardItem::new_string(text));
                        this.notice = Some(
                            format!(
                                "Copied {} {} to clipboard.",
                                format_count(row_count),
                                counted_noun(row_count, "row", "rows")
                            )
                            .into(),
                        );
                    }
                    Err(error) if error.contains("Operation cancelled") => {
                        this.notice = Some("Copy cancelled.".into());
                    }
                    Err(error) => {
                        this.error = Some(format!("Unable to copy rows: {error}").into());
                        this.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn copy_cell_range(
        &mut self,
        row_start: usize,
        row_end: usize,
        column_start: usize,
        column_end: usize,
        cx: &mut Context<Self>,
    ) {
        let Some(document) = self.loaded.as_ref().map(|loaded| loaded.document.clone()) else {
            return;
        };
        let row_count = row_end - row_start + 1;
        self.loading = true;
        let cancellation = self.begin_cancellable_operation(OperationKind::Copy);
        let progress = Arc::new(AtomicUsize::new(0));
        self.track_row_progress(
            progress.clone(),
            row_count,
            OperationKind::Copy,
            "Copying cell range",
            cx,
        );
        self.notice = Some(
            format!(
                "Copying {} rows × {} columns…",
                format_count(row_count),
                format_count(column_end - column_start + 1)
            )
            .into(),
        );
        cx.notify();
        let task = cx.background_spawn(async move {
            let update_progress = |processed, _| progress.store(processed, Ordering::Relaxed);
            document
                .lock()
                .map_err(|_| "CSV document lock was poisoned".to_string())?
                .serialize_display_cell_range_cancellable_with_progress(
                    row_start,
                    row_end,
                    column_start,
                    column_end,
                    &cancellation,
                    &update_progress,
                )
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                this.finish_cancellable_operation();
                this.loading = false;
                match result {
                    Ok(text) => {
                        cx.write_to_clipboard(ClipboardItem::new_string(text));
                        this.notice = Some(
                            format!(
                                "Copied {} rows × {} columns.",
                                format_count(row_count),
                                format_count(column_end - column_start + 1)
                            )
                            .into(),
                        );
                    }
                    Err(error) if error.contains("Operation cancelled") => {
                        this.notice = Some("Cell-range copy cancelled.".into());
                    }
                    Err(error) => {
                        this.error = Some(format!("Unable to copy cell range: {error}").into());
                        this.notice = None;
                    }
                }
                cx.notify();
            })?;
            anyhow::Ok(())
        })
        .detach();
    }

    fn render_empty(&self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let recent = self.settings.recent_files.clone();
        div()
            .size_full()
            .p_6()
            .bg(cx.theme().secondary.opacity(0.35))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w_full()
                    .max_w(px(560.0))
                    .gap_5()
                    .child(
                        Button::new("open-csv")
                            .icon(IconName::FolderOpen)
                            .label(if self.loading {
                                "Opening…"
                            } else {
                                "Choose a CSV file"
                            })
                            .disabled(self.loading)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.open_dialog(&OpenFile, window, cx)
                            })),
                    )
                    .when(!recent.is_empty(), |this| {
                        this.child(
                            v_flex()
                                .gap_2()
                                .child(
                                    div()
                                        .text_xs()
                                        .font_weight(gpui::FontWeight::SEMIBOLD)
                                        .text_color(cx.theme().muted_foreground)
                                        .child("RECENT FILES"),
                                )
                                .children(recent.into_iter().take(6).map(|path| {
                                    let title = display_name(&path);
                                    let open_path = path.clone();
                                    let remove_path = path.clone();
                                    h_flex()
                                        .gap_2()
                                        .child(
                                            Button::new(SharedString::from(format!(
                                                "recent:{}",
                                                path.display()
                                            )))
                                            .flex_1()
                                            .icon(IconName::File)
                                            .label(title)
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.open_path(open_path.clone().into(), cx)
                                            })),
                                        )
                                        .child(
                                            Button::new(SharedString::from(format!(
                                                "remove-recent:{}",
                                                path.display()
                                            )))
                                            .ghost()
                                            .icon(IconName::Delete)
                                            .tooltip("Remove from recent files")
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.remove_recent_file(&remove_path, cx)
                                            })),
                                        )
                                })),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_unsaved_confirmation(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let action = match self.pending_destructive.as_ref() {
            Some(PendingDestructiveAction::Open(_)) => "opening another file",
            Some(PendingDestructiveAction::Reload) => "reloading with new parse settings",
            Some(PendingDestructiveAction::Clear) => "clearing this file",
            Some(PendingDestructiveAction::Close) => "closing this window",
            Some(PendingDestructiveAction::Quit) => "quitting QuickRows",
            None => "continuing",
        };
        div()
            .id("unsaved-backdrop")
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .bg(gpui::black().opacity(0.35))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w(px(460.0))
                    .p_5()
                    .gap_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .child(
                        div()
                            .text_xl()
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .child("Save changes?"),
                    )
                    .child(format!(
                        "Your cell edits or deleted rows will be lost before {action}."
                    ))
                    .child(
                        h_flex()
                            .justify_end()
                            .gap_2()
                            .child(
                                Button::new("unsaved-cancel")
                                    .ghost()
                                    .label("Cancel")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.cancel_pending_destructive(cx)
                                    })),
                            )
                            .child(
                                Button::new("unsaved-discard")
                                    .danger()
                                    .label("Discard")
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.discard_pending_destructive(window, cx)
                                    })),
                            )
                            .child(
                                Button::new("unsaved-save")
                                    .primary()
                                    .label("Save")
                                    .disabled(self.loading)
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.save_pending_destructive(window, cx)
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_external_save_confirmation(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        div()
            .id("external-save-backdrop")
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .bg(gpui::black().opacity(0.45))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w(px(520.0))
                    .p_5()
                    .gap_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().warning)
                    .bg(cx.theme().background)
                    .child(
                        div()
                            .text_xl()
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .child("The CSV changed on disk"),
                    )
                    .child("Saving here would overwrite changes made by another program. Reload, save a copy, or explicitly overwrite the file.")
                    .child(
                        h_flex()
                            .flex_wrap()
                            .justify_end()
                            .gap_2()
                            .child(
                                Button::new("external-save-cancel")
                                    .ghost()
                                    .label("Cancel")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.cancel_external_save(cx)
                                    })),
                            )
                            .child(
                                Button::new("external-save-reload")
                                    .label("Reload")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.reload_external_change(cx)
                                    })),
                            )
                            .child(
                                Button::new("external-save-as")
                                    .label("Save As…")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.save_external_as(cx)
                                    })),
                            )
                            .child(
                                Button::new("external-save-overwrite")
                                    .danger()
                                    .label("Overwrite")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.confirm_external_overwrite(cx)
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_header_prompt(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        div()
            .id("header-prompt-backdrop")
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .bg(gpui::black().opacity(0.35))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w(px(500.0))
                    .p_5()
                    .gap_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .child(
                        div()
                            .text_xl()
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .child("Does the first row contain headers?"),
                    )
                    .child(
                        div()
                            .text_color(cx.theme().muted_foreground)
                            .child("Header detection was uncertain. Choose how this file should be interpreted."),
                    )
                    .child(
                        h_flex()
                            .flex_wrap()
                            .justify_end()
                            .gap_2()
                            .child(
                                Button::new("header-dismiss")
                                    .ghost()
                                    .label("Dismiss")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.resolve_header_prompt(None, cx)
                                    })),
                            )
                            .child(
                                Button::new("header-as-data")
                                    .label("Keep as data")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.resolve_header_prompt(Some(false), cx)
                                    })),
                            )
                            .child(
                                Button::new("header-use-first")
                                    .primary()
                                    .label("Use first row as headers")
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.resolve_header_prompt(Some(true), cx)
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_shortcuts(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let primary = if cfg!(target_os = "macos") {
            "⌘"
        } else {
            "Ctrl+"
        };
        let shortcuts = vec![
            (format!("{primary}O"), "Open CSV"),
            (format!("{primary}S"), "Save"),
            (format!("{primary}Shift+S"), "Save As"),
            (format!("{primary}F"), "Find"),
            ("F3 / Shift+F3".to_string(), "Next / previous match"),
            (format!("{primary}A"), "Select all rows"),
            ("Shift+Arrow".to_string(), "Extend row or cell selection"),
            ("Page Up / Page Down".to_string(), "Move by one page"),
            (format!("{primary}C"), "Copy selected rows or cells"),
            ("Delete / Backspace".to_string(), "Delete selected rows"),
            ("Escape".to_string(), "Dismiss menu or clear selection"),
        ];
        self.render_info_modal(
            "Keyboard Shortcuts",
            v_flex()
                .gap_2()
                .children(shortcuts.into_iter().map(|(keys, description)| {
                    h_flex()
                        .gap_4()
                        .child(
                            div()
                                .w(px(170.0))
                                .font_weight(gpui::FontWeight::SEMIBOLD)
                                .child(keys),
                        )
                        .child(description)
                }))
                .into_any_element(),
            cx,
        )
    }

    fn render_about(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        self.render_info_modal(
            "About QuickRows",
            v_flex()
                .gap_3()
                .child(
                    div()
                        .text_lg()
                        .font_weight(gpui::FontWeight::SEMIBOLD)
                        .child(format!("QuickRows {}", env!("CARGO_PKG_VERSION"))),
                )
                .child("A fast, local-first native CSV viewer and editor.")
                .child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().muted_foreground)
                        .child("CSV data stays on this computer. Licensed under MIT."),
                )
                .into_any_element(),
            cx,
        )
    }

    fn render_info_modal(
        &mut self,
        title: &'static str,
        content: gpui::AnyElement,
        cx: &mut Context<Self>,
    ) -> gpui::AnyElement {
        div()
            .id(SharedString::from(format!(
                "{}-backdrop",
                title.replace(' ', "-")
            )))
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .bg(gpui::black().opacity(0.45))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w(px(520.0))
                    .max_h(px(680.0))
                    .p_5()
                    .gap_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .child(
                        div()
                            .text_xl()
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .child(title),
                    )
                    .child(content)
                    .child(
                        h_flex().justify_end().child(
                            Button::new("close-info-modal")
                                .primary()
                                .label("Close")
                                .on_click(cx.listener(|this, _, _, cx| this.close_info_modal(cx))),
                        ),
                    ),
            )
            .into_any_element()
    }

    fn render_bulk_confirmation(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let (title, message, confirm_label) = match self.pending_bulk_action.as_ref() {
            Some(PendingBulkAction::Copy { rows }) => (
                "Copy many rows?",
                format!(
                    "Copy {} selected rows to the clipboard?",
                    format_count(rows.len())
                ),
                "Copy",
            ),
            Some(PendingBulkAction::CopyCells {
                row_start,
                row_end,
                column_start,
                column_end,
            }) => (
                "Copy a large cell range?",
                format!(
                    "Copy {} rows × {} columns to the clipboard?",
                    format_count(row_end - row_start + 1),
                    format_count(column_end - column_start + 1)
                ),
                "Copy",
            ),
            Some(PendingBulkAction::Delete { rows }) => (
                "Delete many rows?",
                format!(
                    "Mark {} selected rows as deleted?",
                    format_count(rows.len())
                ),
                "Delete",
            ),
            None => ("Continue?", String::new(), "Continue"),
        };
        div()
            .id("bulk-confirmation-backdrop")
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .bg(gpui::black().opacity(0.35))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w(px(440.0))
                    .p_5()
                    .gap_4()
                    .rounded_lg()
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .child(
                        div()
                            .text_xl()
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .child(title),
                    )
                    .child(message)
                    .child(
                        h_flex()
                            .justify_end()
                            .gap_2()
                            .child(Button::new("bulk-cancel").ghost().label("Cancel").on_click(
                                cx.listener(|this, _, _, cx| this.cancel_pending_bulk_action(cx)),
                            ))
                            .child(
                                Button::new("bulk-confirm")
                                    .danger()
                                    .label(confirm_label)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.confirm_pending_bulk_action(cx)
                                    })),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_settings(&mut self, cx: &mut Context<Self>) -> gpui::AnyElement {
        let theme = self.settings.theme;
        let density = self.settings.row_density;
        let parse = &self.settings.parse_overrides;
        let theme_label = match theme {
            ThemePreference::System => "System",
            ThemePreference::Light => "Light",
            ThemePreference::Dark => "Dark",
        };
        let density_label = match density {
            RowDensity::Compact => "Compact",
            RowDensity::Default => "Default",
            RowDensity::Spacious => "Spacious",
        };
        let delimiter = override_label(parse.delimiter.as_deref());
        let quote = override_label(parse.quote.as_deref());
        let escape = override_label(parse.escape.as_deref());
        let comment = override_label(parse.comment.as_deref());
        let excel_sep = match parse.excel_sep {
            None => "Automatic",
            Some(true) => "Enabled",
            Some(false) => "Disabled",
        };
        let line_ending = override_label(parse.line_ending.as_deref());
        let encoding = override_label(parse.encoding.as_deref());
        let headers = match parse.has_headers {
            None => "Automatic",
            Some(true) => "First row",
            Some(false) => "No header",
        };
        let malformed = override_label(parse.malformed.as_deref());
        let max_field = size_override_label(parse.max_field_size);
        let max_record = size_override_label(parse.max_record_size);
        let show_index = self.settings.show_index;
        let enable_indexing = self.settings.enable_indexing;
        let parse_diagnostics = self.loaded.as_ref().map(|loaded| {
            (
                parse_summary(&loaded.detected_parse_info),
                parse_effective_changes(&loaded.detected_parse_info, &loaded.parse_info),
                loaded.warnings.clone(),
            )
        });
        let show_warning_details = self.show_warning_details;

        let select_width = px(142.0);
        let view = self
            .self_weak
            .clone()
            .expect("QuickRows view identity is initialized before rendering settings");
        let body = v_flex()
            .gap_5()
            .child(settings_section_title("APPEARANCE", cx))
            .child(settings_row(
                "Theme",
                settings_dropdown(
                    "settings-theme-select",
                    format!("{theme_label}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("System", theme == ThemePreference::System, SettingsChoice::Theme(ThemePreference::System)),
                        ("Light", theme == ThemePreference::Light, SettingsChoice::Theme(ThemePreference::Light)),
                        ("Dark", theme == ThemePreference::Dark, SettingsChoice::Theme(ThemePreference::Dark)),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Row Height",
                settings_dropdown(
                    "settings-density-select",
                    format!("{density_label}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Compact", density == RowDensity::Compact, SettingsChoice::Density(RowDensity::Compact)),
                        ("Default", density == RowDensity::Default, SettingsChoice::Density(RowDensity::Default)),
                        ("Spacious", density == RowDensity::Spacious, SettingsChoice::Density(RowDensity::Spacious)),
                    ],
                ),
                cx,
            ))
            .child(settings_section_title("VIEW", cx))
            .child(settings_row(
                "Show Row Numbers",
                Switch::new("settings-index-switch")
                    .checked(show_index)
                    .tooltip("Show row numbers")
                    .on_click({
                        let view = view.clone();
                        move |_, window, cx| {
                            let _ = view.update(cx, |this, cx| {
                                this.toggle_index(&ToggleIndex, window, cx)
                            });
                        }
                    }),
                cx,
            ))
            .child(settings_section_title("SEARCH & PARSING", cx))
            .child(settings_row(
                "Enable Search Indexing",
                Switch::new("settings-indexing-switch")
                    .checked(enable_indexing)
                    .tooltip("Enable search indexing")
                    .on_click({
                        let view = view.clone();
                        move |_, _, cx| {
                            let _ = view.update(cx, |this, cx| this.toggle_search_indexing(cx));
                        }
                    }),
                cx,
            ))
            .child(settings_description(
                "Indexes only the selected column when it is searched and retains one column at a time. Disabled by default to minimize RAM use.",
                cx,
            ))
            .child(settings_row(
                "Delimiter",
                settings_dropdown(
                    "settings-delimiter-select",
                    format!("{delimiter}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.delimiter.is_none(), SettingsChoice::Delimiter(None)),
                        ("Comma", parse.delimiter.as_deref() == Some("comma"), SettingsChoice::Delimiter(Some("comma"))),
                        ("Tab", parse.delimiter.as_deref() == Some("tab"), SettingsChoice::Delimiter(Some("tab"))),
                        ("Semicolon", parse.delimiter.as_deref() == Some("semicolon"), SettingsChoice::Delimiter(Some("semicolon"))),
                        ("Pipe", parse.delimiter.as_deref() == Some("pipe"), SettingsChoice::Delimiter(Some("pipe"))),
                        ("Space", parse.delimiter.as_deref() == Some("space"), SettingsChoice::Delimiter(Some("space"))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Quote Character",
                settings_dropdown(
                    "settings-quote-select",
                    format!("{quote}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.quote.is_none(), SettingsChoice::Quote(None)),
                        ("Double quote", parse.quote.as_deref() == Some("double"), SettingsChoice::Quote(Some("double"))),
                        ("Single quote", parse.quote.as_deref() == Some("single"), SettingsChoice::Quote(Some("single"))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Escape Character",
                settings_dropdown(
                    "settings-escape-select",
                    format!("{escape}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.escape.is_none(), SettingsChoice::Escape(None)),
                        ("None", parse.escape.as_deref() == Some("none"), SettingsChoice::Escape(Some("none"))),
                        ("Backslash", parse.escape.as_deref() == Some("backslash"), SettingsChoice::Escape(Some("backslash"))),
                    ],
                ),
                cx,
            ))
            .child(
                h_flex()
                    .gap_2()
                    .child(Input::new(&self.custom_delimiter_input).flex_1())
                    .child(
                        Button::new("apply-custom-delimiter")
                            .label("Use delimiter")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.apply_custom_delimiter(cx)
                            })),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(Input::new(&self.custom_quote_input).flex_1())
                    .child(
                        Button::new("apply-custom-quote")
                            .label("Use quote")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.apply_custom_quote(cx)
                            })),
                    ),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(Input::new(&self.custom_escape_input).flex_1())
                    .child(
                        Button::new("apply-custom-escape")
                            .label("Use escape")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.apply_custom_escape(cx)
                            })),
                    ),
            )
            .child(settings_row(
                "Comment Character",
                settings_dropdown(
                    "settings-comment-select",
                    format!("{comment}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.comment.is_none(), SettingsChoice::Comment(None)),
                        ("None", parse.comment.as_deref() == Some("none"), SettingsChoice::Comment(Some("none"))),
                        ("Hash (#)", parse.comment.as_deref() == Some("#"), SettingsChoice::Comment(Some("#"))),
                    ],
                ),
                cx,
            ))
            .child(
                h_flex()
                    .gap_2()
                    .child(Input::new(&self.custom_comment_input).flex_1())
                    .child(
                        Button::new("apply-custom-comment")
                            .label("Use comment")
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.apply_custom_comment(cx)
                            })),
                    ),
            )
            .child(settings_row(
                "Excel sep= Directive",
                settings_dropdown(
                    "settings-excel-sep-select",
                    format!("{excel_sep}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.excel_sep.is_none(), SettingsChoice::ExcelSep(None)),
                        ("Enabled", parse.excel_sep == Some(true), SettingsChoice::ExcelSep(Some(true))),
                        ("Disabled", parse.excel_sep == Some(false), SettingsChoice::ExcelSep(Some(false))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Line Ending",
                settings_dropdown(
                    "settings-lines-select",
                    format!("{line_ending}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.line_ending.is_none(), SettingsChoice::LineEnding(None)),
                        ("LF", parse.line_ending.as_deref() == Some("lf"), SettingsChoice::LineEnding(Some("lf"))),
                        ("CRLF", parse.line_ending.as_deref() == Some("crlf"), SettingsChoice::LineEnding(Some("crlf"))),
                        ("CR", parse.line_ending.as_deref() == Some("cr"), SettingsChoice::LineEnding(Some("cr"))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Encoding",
                settings_dropdown(
                    "settings-encoding-select",
                    format!("{encoding}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.encoding.is_none(), SettingsChoice::Encoding(None)),
                        ("UTF-8", parse.encoding.as_deref() == Some("utf-8"), SettingsChoice::Encoding(Some("utf-8"))),
                        ("UTF-16 LE", parse.encoding.as_deref() == Some("utf-16le"), SettingsChoice::Encoding(Some("utf-16le"))),
                        ("UTF-16 BE", parse.encoding.as_deref() == Some("utf-16be"), SettingsChoice::Encoding(Some("utf-16be"))),
                        ("Windows-1252", parse.encoding.as_deref() == Some("windows-1252"), SettingsChoice::Encoding(Some("windows-1252"))),
                        ("ISO-8859-1", parse.encoding.as_deref() == Some("iso-8859-1"), SettingsChoice::Encoding(Some("iso-8859-1"))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Header Row",
                settings_dropdown(
                    "settings-headers-select",
                    format!("{headers}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.has_headers.is_none(), SettingsChoice::Headers(None)),
                        ("First row", parse.has_headers == Some(true), SettingsChoice::Headers(Some(true))),
                        ("No header", parse.has_headers == Some(false), SettingsChoice::Headers(Some(false))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Malformed Rows",
                settings_dropdown(
                    "settings-malformed-select",
                    format!("{malformed}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Automatic", parse.malformed.is_none(), SettingsChoice::Malformed(None)),
                        ("Strict", parse.malformed.as_deref() == Some("strict"), SettingsChoice::Malformed(Some("strict"))),
                        ("Skip", parse.malformed.as_deref() == Some("skip"), SettingsChoice::Malformed(Some("skip"))),
                        ("Repair", parse.malformed.as_deref() == Some("repair"), SettingsChoice::Malformed(Some("repair"))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Field Size Limit",
                settings_dropdown(
                    "settings-field-limit-select",
                    format!("{max_field}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Default", parse.max_field_size.is_none(), SettingsChoice::MaxFieldSize(None)),
                        ("1 MiB", parse.max_field_size == Some(1 << 20), SettingsChoice::MaxFieldSize(Some(1 << 20))),
                        ("8 MiB", parse.max_field_size == Some(8 << 20), SettingsChoice::MaxFieldSize(Some(8 << 20))),
                        ("64 MiB", parse.max_field_size == Some(64 << 20), SettingsChoice::MaxFieldSize(Some(64 << 20))),
                    ],
                ),
                cx,
            ))
            .child(settings_row(
                "Record Size Limit",
                settings_dropdown(
                    "settings-record-limit-select",
                    format!("{max_record}  ▾"),
                    select_width,
                    view.clone(),
                    vec![
                        ("Default", parse.max_record_size.is_none(), SettingsChoice::MaxRecordSize(None)),
                        ("8 MiB", parse.max_record_size == Some(8 << 20), SettingsChoice::MaxRecordSize(Some(8 << 20))),
                        ("64 MiB", parse.max_record_size == Some(64 << 20), SettingsChoice::MaxRecordSize(Some(64 << 20))),
                        ("256 MiB", parse.max_record_size == Some(256 << 20), SettingsChoice::MaxRecordSize(Some(256 << 20))),
                    ],
                ),
                cx,
            ))
            .when_some(
                parse_diagnostics,
                |body, (detected, effective_changes, warnings)| {
                    let warning_count = warnings.len();
                    body.child(settings_parse_diagnostics(
                        detected,
                        effective_changes,
                        cx,
                    ))
                        .when(warning_count > 0, |body| {
                            body.child(
                                Button::new("parse-warning-details")
                                    .ghost()
                                    .label(if show_warning_details {
                                        format!(
                                            "Hide {} {}",
                                            format_count(warning_count),
                                            counted_noun(
                                                warning_count,
                                                "parse warning",
                                                "parse warnings",
                                            )
                                        )
                                    } else {
                                        format!(
                                            "View {} {}",
                                            format_count(warning_count),
                                            counted_noun(
                                                warning_count,
                                                "parse warning",
                                                "parse warnings",
                                            )
                                        )
                                    })
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.show_warning_details = !this.show_warning_details;
                                        cx.notify();
                                    })),
                            )
                        })
                        .when(show_warning_details && warning_count > 0, |body| {
                            body.child(
                                v_flex()
                                    .id("parse-warning-list")
                                    .max_h(px(260.0))
                                    .overflow_y_scroll()
                                    .gap_2()
                                    .p_2()
                                    .rounded(px(8.0))
                                    .border_1()
                                    .border_color(cx.theme().warning)
                                    .children(warnings.into_iter().enumerate().map(
                                        |(index, warning)| {
                                            v_flex()
                                                .p_2()
                                                .gap_1()
                                                .rounded(px(6.0))
                                                .bg(cx.theme().warning.opacity(0.1))
                                                .child(
                                                    div()
                                                        .text_sm()
                                                        .font_weight(gpui::FontWeight::SEMIBOLD)
                                                        .child(format!(
                                                            "{}. {}",
                                                            index + 1,
                                                            warning.message
                                                        )),
                                                )
                                                .child(
                                                    div()
                                                        .text_xs()
                                                        .text_color(cx.theme().muted_foreground)
                                                        .child(parse_warning_location(&warning)),
                                                )
                                        },
                                    )),
                            )
                        })
                },
            )
            .child(
                h_flex()
                    .flex_wrap()
                    .justify_end()
                    .gap_2()
                    .child(
                        Button::new("parse-reset")
                            .ghost()
                            .label("Reset overrides")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.reset_parse_settings(window, cx)
                            })),
                    )
                    .child(
                        Button::new("parse-reload")
                            .primary()
                            .label("Apply and Reload")
                            .disabled(self.loaded.is_none() || self.loading)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.reload_with_parse_settings(cx)
                            })),
                    ),
            );

        div()
            .id("settings-backdrop")
            .occlude()
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .on_mouse_down(MouseButton::Right, |_, _, cx| cx.stop_propagation())
            .absolute()
            .top_0()
            .right_0()
            .bottom_0()
            .left_0()
            .p_4()
            .bg(gpui::black().opacity(0.6))
            .flex()
            .items_center()
            .justify_center()
            .child(
                v_flex()
                    .w_full()
                    .max_w(px(520.0))
                    .h_full()
                    .max_h(px(720.0))
                    .overflow_hidden()
                    .rounded(px(12.0))
                    .border_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .shadow_2xl()
                    .child(
                        h_flex()
                            .flex_none()
                            .h(px(58.0))
                            .px_5()
                            .border_b_1()
                            .border_color(cx.theme().border)
                            .bg(cx.theme().secondary.opacity(0.55))
                            .gap_3()
                            .child(
                                div()
                                    .w(px(32.0))
                                    .h(px(32.0))
                                    .rounded(px(9.0))
                                    .bg(cx.theme().accent)
                                    .text_color(cx.theme().accent_foreground)
                                    .font_weight(gpui::FontWeight::BOLD)
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .child("Q"),
                            )
                            .child(
                                v_flex()
                                    .child(
                                        div()
                                            .text_lg()
                                            .font_weight(gpui::FontWeight::SEMIBOLD)
                                            .child("QuickRows Settings"),
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(cx.theme().muted_foreground)
                                            .child("Tune your data workspace"),
                                    ),
                            )
                            .child(div().flex_1())
                            .child(
                                Button::new("settings-close").ghost().label("×").on_click(
                                    cx.listener(|this, _, _, cx| this.close_settings(cx)),
                                ),
                            ),
                    )
                    .child(
                        div()
                            .id("settings-body")
                            .flex_1()
                            .min_h_0()
                            .overflow_y_scrollbar()
                            .p_5()
                            .bg(cx.theme().secondary.opacity(0.24))
                            .child(body),
                    )
                    .child(
                        h_flex()
                            .flex_none()
                            .h(px(64.0))
                            .px_5()
                            .justify_end()
                            .border_t_1()
                            .border_color(cx.theme().border)
                            .child(
                                Button::new("settings-done")
                                    .primary()
                                    .label("Done")
                                    .on_click(
                                        cx.listener(|this, _, _, cx| this.close_settings(cx)),
                                    ),
                            ),
                    ),
            )
            .into_any_element()
    }

    fn render_table(&mut self, window: &mut Window, cx: &mut Context<Self>) -> gpui::AnyElement {
        let Some(loaded) = &self.loaded else {
            window.set_window_title(BASE_TITLE);
            return self.render_empty(cx);
        };
        let headers = loaded.headers.clone();
        let row_count = loaded.row_count;
        let show_toolbar_labels = toolbar_shows_labels(f32::from(window.viewport_size().width));
        let show_index = self.settings.show_index;
        let row_height = self.settings.row_density.height();
        let column_widths = self.settings.column_widths.clone();
        let default_width = self.settings.column_width.max(MIN_COLUMN_WIDTH);
        let table_width = (headers
            .iter()
            .enumerate()
            .map(|(index, _)| {
                column_widths
                    .get(index)
                    .copied()
                    .unwrap_or(default_width)
                    .max(MIN_COLUMN_WIDTH)
            })
            .sum::<f32>()
            + if show_index { 72.0 } else { 0.0 })
        .max(640.0);
        let selected_rows = self.selected_rows.clone();
        let selected_count = selected_rows.len();
        let cell_selection = self.cell_selection;
        let cell_dimensions = cell_selection.map(CellSelection::dimensions);
        let search_rows: HashSet<usize> =
            if self.active_highlight == Some(ActiveHighlight::Search) && !self.search_stale {
                self.row_cache
                    .values()
                    .filter_map(|row| {
                        self.search_results
                            .binary_search(&row.source_row)
                            .is_ok()
                            .then_some(row.source_row)
                    })
                    .collect()
            } else {
                HashSet::new()
            };
        let duplicate_rows: HashSet<usize> = if self.active_highlight
            == Some(ActiveHighlight::Duplicates)
            && !self.duplicate_stale
        {
            self.row_cache
                .values()
                .filter_map(|row| {
                    self.duplicate_results
                        .binary_search(&row.source_row)
                        .is_ok()
                        .then_some(row.source_row)
                })
                .collect()
        } else {
            HashSet::new()
        };
        let current_source_match = (self.active_highlight == Some(ActiveHighlight::Search)
            && !self.search_stale)
            .then(|| self.search_results.get(self.current_match).copied())
            .flatten();
        let current_duplicate_source = (self.active_highlight == Some(ActiveHighlight::Duplicates)
            && !self.duplicate_stale)
            .then(|| {
                self.duplicate_results
                    .get(self.current_duplicate_match)
                    .copied()
            })
            .flatten();
        let filename = display_name(&loaded.path);
        let dirty = loaded.dirty || self.pending_cell_commits > 0 || self.editing_draft_dirty;
        let search_scope = column_scope_label(self.search_column, &headers);
        let duplicate_scope = column_scope_label(self.duplicate_column, &headers);
        let cell_search = (self.active_highlight == Some(ActiveHighlight::Search)
            && !self.search_stale)
            .then(|| {
                self.last_search_query.clone().map(|query| {
                    (
                        query,
                        self.search_column,
                        self.search_match_case,
                        self.search_whole_word,
                    )
                })
            })
            .flatten();
        window.set_window_title(&format!(
            "{}{} - {}",
            filename,
            if dirty { " *" } else { "" },
            BASE_TITLE
        ));

        let header_content = h_flex()
            .w(px(table_width))
            .h(px(row_height))
            .flex_none()
            .border_b_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().secondary.opacity(0.65))
            .when(show_index, |this| this.child(header_cell("#", 72.0, cx)))
            .children(headers.iter().enumerate().map(|(index, label)| {
                let width = column_widths
                    .get(index)
                    .copied()
                    .unwrap_or(default_width)
                    .max(120.0);
                let label = display_header_label(label, index);
                header_cell(&label, width, cx)
                    .id(("header", index))
                    .relative()
                    .cursor_pointer()
                    .on_click(cx.listener(move |this, _, _, cx| this.sort_column(index, cx)))
                    .child(
                        div()
                            .id(("column-resizer", index))
                            .absolute()
                            .right_0()
                            .top_0()
                            .bottom_0()
                            .w(px(COLUMN_RESIZE_HANDLE_WIDTH))
                            .cursor_col_resize()
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, event: &MouseDownEvent, _, cx| {
                                    this.begin_column_resize(
                                        index,
                                        f32::from(event.position.x),
                                        cx,
                                    );
                                    cx.stop_propagation();
                                }),
                            )
                            .on_click(|_, _, cx| cx.stop_propagation()),
                    )
            }));

        // GPUI Component's data table pattern shares one horizontal handle
        // between the header, body, drag scrollbar, and ScrollableMask. Both
        // surfaces are translated by GPUI at paint time, so virtual rows cannot
        // lag behind the header while a scrollbar or trackpad gesture is active.
        let header = div()
            .id("table-header-scroll")
            .w_full()
            .h(px(row_height))
            .flex_none()
            .overflow_scroll()
            .track_scroll(&self.column_scroll)
            .child(header_content);

        let rows = uniform_list(
            "csv-rows",
            row_count,
            cx.processor(move |this, range: std::ops::Range<usize>, _window, cx| {
                this.request_visible_rows(range.clone(), cx);
                range
                    .map(|display_row| {
                        let cached = this.row_cache.get(&display_row).cloned();
                        let source_row = cached.as_ref().map(|row| row.source_row);
                        let is_deleted = cached.as_ref().is_some_and(|row| row.deleted);
                        let can_edit = cached.is_some() && !is_deleted;
                        let editing_cell = this.editing_cell.clone();
                        let edit_input = this.edit_input.clone();
                        let cells =
                            cached
                                .as_ref()
                                .map(|row| row.cells.clone())
                                .unwrap_or_else(|| {
                                    let mut cells = vec![String::new(); headers.len()];
                                    if let Some(first) = cells.first_mut() {
                                        *first = "Loading…".to_string();
                                    }
                                    cells
                                });
                        let is_selected = selected_rows.contains(display_row);
                        let is_search_match =
                            source_row.is_some_and(|row| search_rows.contains(&row));
                        let is_current_match = source_row == current_source_match
                            || source_row == current_duplicate_source;
                        let is_duplicate =
                            source_row.is_some_and(|row| duplicate_rows.contains(&row));
                        h_flex()
                            .id(("row", display_row))
                            .h(px(row_height))
                            .w(px(table_width))
                            .flex_none()
                            .border_b_1()
                            .border_color(cx.theme().border)
                            .when(!is_selected && !is_current_match, |row| {
                                row.hover(|row| row.bg(cx.theme().table_hover))
                            })
                            .when(!is_selected && display_row % 2 == 1, |row| {
                                row.bg(cx.theme().table_even)
                            })
                            .when(is_duplicate, |row| row.bg(cx.theme().warning.opacity(0.18)))
                            .when(is_search_match, |row| row.bg(cx.theme().info.opacity(0.18)))
                            .when(is_deleted && !is_selected, |row| {
                                row.bg(cx.theme().danger.opacity(0.16)).opacity(0.65)
                            })
                            .when(is_current_match && !is_selected, |row| {
                                row.bg(cx.theme().warning.opacity(0.24))
                                    .border_1()
                                    .border_color(cx.theme().warning)
                            })
                            .when(is_selected, |row| {
                                row.bg(cx.theme().selection.opacity(0.72))
                                    .border_1()
                                    .border_color(cx.theme().foreground.opacity(0.55))
                            })
                            .on_click(cx.listener(move |this, event: &ClickEvent, _, cx| {
                                this.select_row_from_click(display_row, event, cx);
                            }))
                            .on_mouse_down(
                                MouseButton::Right,
                                cx.listener(move |this, _, _, cx| {
                                    if !this.selected_rows.contains(display_row) {
                                        this.selected_rows.select_only(display_row);
                                        this.selection_anchor = Some(display_row);
                                    }
                                    this.selected_row = Some(display_row);
                                    cx.notify();
                                }),
                            )
                            .when(show_index, |row| {
                                let index = source_row
                                    .map(|source_row| source_row + 1)
                                    .unwrap_or(display_row + 1);
                                row.child(
                                    body_cell(&index.to_string(), 72.0, cx)
                                        .id(("row-index", display_row))
                                        .on_mouse_down(
                                            MouseButton::Right,
                                            cx.listener(
                                                move |this, event: &MouseDownEvent, _, cx| {
                                                    this.context_cell = None;
                                                    this.cell_selection = None;
                                                    this.table_context_menu =
                                                        Some(TableContextMenu {
                                                            position: event.position,
                                                            kind: TableContextMenuKind::Row,
                                                            focused_item: 0,
                                                        });
                                                    cx.notify();
                                                },
                                            ),
                                        ),
                                )
                            })
                            .children(cells.into_iter().enumerate().map(|(index, value)| {
                                let width = column_widths
                                    .get(index)
                                    .copied()
                                    .unwrap_or(default_width)
                                    .max(120.0);
                                let is_cell_match = cell_search.as_ref().is_some_and(
                                    |(query, column, match_case, whole_word)| {
                                        column.is_none_or(|column| column == index)
                                            && cell_matches_search(
                                                &value,
                                                query,
                                                *match_case,
                                                *whole_word,
                                            )
                                    },
                                );
                                let is_cell_selected = cell_selection.is_some_and(|selection| {
                                    selection.contains(display_row, index)
                                });
                                let is_editing = editing_cell.as_ref().is_some_and(|editing| {
                                    editing.display_row == display_row
                                        && editing.source_row == source_row.unwrap_or(display_row)
                                        && editing.column == index
                                });
                                if is_editing {
                                    body_cell_frame(width, cx)
                                        .id(SharedString::from(format!(
                                            "cell-{display_row}-{index}-editor"
                                        )))
                                        .border_1()
                                        .border_color(cx.theme().accent)
                                        .child(
                                            Input::new(&edit_input)
                                                .appearance(false)
                                                .bordered(false)
                                                .focus_bordered(false)
                                                .disabled(this.loading),
                                        )
                                        .into_any_element()
                                } else {
                                    let editor_value = value.clone();
                                    let keyboard_value = value.clone();
                                    let context_value = value.clone();
                                    let source_row = source_row.unwrap_or(display_row);
                                    body_cell(&value, width, cx)
                                        .when(is_cell_match, |cell| {
                                            cell.bg(cx.theme().info.opacity(0.32))
                                                .border_1()
                                                .border_color(cx.theme().info)
                                        })
                                        .when(is_cell_selected, |cell| {
                                            cell.bg(cx.theme().selection)
                                                .border_1()
                                                .border_color(cx.theme().accent)
                                        })
                                        .id(SharedString::from(format!(
                                            "cell-{display_row}-{index}"
                                        )))
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(
                                                move |this, event: &MouseDownEvent, _, cx| {
                                                    this.begin_cell_selection(
                                                        display_row,
                                                        index,
                                                        event,
                                                        cx,
                                                    );
                                                    cx.stop_propagation();
                                                },
                                            ),
                                        )
                                        .on_mouse_move(cx.listener(move |this, _, _, cx| {
                                            this.drag_cell_selection(display_row, index, cx);
                                        }))
                                        .on_click(|_, _, cx| cx.stop_propagation())
                                        .when(can_edit, |cell| {
                                            cell.cursor_text()
                                                .tab_index(0)
                                                .on_click(cx.listener(
                                                    move |this,
                                                          event: &ClickEvent,
                                                          window,
                                                          cx| {
                                                        if event.click_count() == 2 {
                                                            this.begin_cell_edit(
                                                                display_row,
                                                                source_row,
                                                                index,
                                                                editor_value.clone(),
                                                                window,
                                                                cx,
                                                            );
                                                        }
                                                    },
                                                ))
                                                .on_key_down(cx.listener(
                                                    move |this,
                                                          event: &KeyDownEvent,
                                                          window,
                                                          cx| {
                                                        if event.keystroke.key.as_str() == "enter" {
                                                            cx.stop_propagation();
                                                            this.begin_cell_edit(
                                                                display_row,
                                                                source_row,
                                                                index,
                                                                keyboard_value.clone(),
                                                                window,
                                                                cx,
                                                            );
                                                        }
                                                    },
                                                ))
                                        })
                                        .on_mouse_down(
                                            MouseButton::Right,
                                            cx.listener(
                                                move |this, event: &MouseDownEvent, _, cx| {
                                                    this.context_cell = Some((
                                                        display_row,
                                                        source_row,
                                                        index,
                                                        context_value.clone(),
                                                    ));
                                                    this.table_context_menu =
                                                        Some(TableContextMenu {
                                                            position: event.position,
                                                            kind: TableContextMenuKind::Cell {
                                                                can_edit,
                                                            },
                                                            focused_item: 0,
                                                        });
                                                    cx.notify();
                                                },
                                            ),
                                        )
                                        .into_any_element()
                                }
                            }))
                    })
                    .collect::<Vec<_>>()
            }),
        )
        .track_scroll(self.row_scroll.clone())
        .h_full()
        .w(px(table_width));

        let table_content = v_flex()
            .size_full()
            .pr(px(16.0))
            .pb(px(16.0))
            .overflow_hidden()
            .child(header)
            .child(
                div()
                    .id("table-body-horizontal-scroll")
                    .relative()
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_scroll()
                    .track_scroll(&self.column_scroll)
                    .child(rows),
            );

        v_flex()
            .size_full()
            .child(
                h_flex()
                    .flex_none()
                    .min_h(px(46.0))
                    .px_3()
                    .py_1()
                    .gap_2()
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().background)
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("toolbar-open")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::FolderOpen)
                                    .tooltip("Open")
                                    .when(show_toolbar_labels, |button| button.label("Open"))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.open_dialog(&OpenFile, window, cx)
                                    })),
                            )
                            .child(
                                Button::new("toolbar-save")
                                    .compact()
                                    .icon(IconName::Check)
                                    .label("Save")
                                    .tooltip("Save")
                                    .disabled(!dirty)
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.save_file(&SaveFile, window, cx)
                                    })),
                            ),
                    )
                    .child(toolbar_divider(cx))
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("toolbar-copy")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::Copy)
                                    .tooltip("Copy")
                                    .when(show_toolbar_labels, |button| button.label("Copy"))
                                    .disabled(self.selected_row.is_none())
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.copy_selected(window, cx)
                                    })),
                            )
                            .child(
                                Button::new("toolbar-delete")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::Delete)
                                    .tooltip("Delete")
                                    .when(show_toolbar_labels, |button| button.label("Delete"))
                                    .disabled(self.selected_row.is_none())
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.mutate_selected_rows(RowMutation::Delete, cx)
                                    })),
                            )
                            .child(
                                Button::new("toolbar-restore")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::Undo)
                                    .tooltip("Restore")
                                    .when(show_toolbar_labels, |button| button.label("Restore"))
                                    .disabled(self.selected_row.is_none())
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.mutate_selected_rows(RowMutation::Restore, cx)
                                    })),
                            ),
                    )
                    .child(toolbar_divider(cx))
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Button::new("toolbar-find")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::Search)
                                    .tooltip("Find")
                                    .when(show_toolbar_labels, |button| button.label("Find"))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.show_find(&Find, window, cx)
                                    })),
                            )
                            .child(
                                Button::new("toolbar-duplicates")
                                    .compact()
                                    .ghost()
                                    .icon(IconName::GalleryVerticalEnd)
                                    .tooltip("Find duplicates")
                                    .when(show_toolbar_labels, |button| button.label("Dupes"))
                                    .disabled(self.loading)
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.check_duplicates(&CheckDuplicates, window, cx)
                                    })),
                            ),
                    )
                    .child(div().flex_1())
                    .child(
                        div()
                            .px_2()
                            .text_xs()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .text_color(cx.theme().muted_foreground)
                            .whitespace_nowrap()
                            .child(format!("{} rows", format_count(row_count))),
                    )
                    .child(
                        Button::new("toolbar-settings")
                            .compact()
                            .ghost()
                            .icon(IconName::Settings2)
                            .tooltip("Settings")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.open_settings(&OpenSettings, window, cx)
                            })),
                    ),
            )
            .when(self.show_find, |this| {
                this.child(
                    h_flex()
                        .flex_none()
                        .min_h(px(46.0))
                        .flex_wrap()
                        .px_3()
                        .py_1()
                        .gap_2()
                        .border_l_2()
                        .border_b_1()
                        .border_color(cx.theme().border)
                        .bg(cx.theme().secondary.opacity(0.55))
                        .child(
                            div()
                                .px_2()
                                .text_xs()
                                .font_weight(gpui::FontWeight::BOLD)
                                .text_color(cx.theme().accent)
                                .child("FIND"),
                        )
                        .child(
                            Button::new("search-scope")
                                .compact()
                                .ghost()
                                .label(search_scope)
                                .disabled(self.loading)
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.cycle_search_column(cx)),
                                ),
                        )
                        .child(Input::new(&self.search_input).flex_1().min_w(px(180.0)))
                        .child(
                            Button::new("search-match-case")
                                .compact()
                                .label(if self.search_match_case {
                                    "Aa ✓"
                                } else {
                                    "Aa"
                                })
                                .disabled(self.loading)
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.toggle_search_match_case(cx)),
                                ),
                        )
                        .child(
                            Button::new("search-whole-word")
                                .compact()
                                .label(if self.search_whole_word {
                                    "ab ✓"
                                } else {
                                    "ab"
                                })
                                .disabled(self.loading)
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.toggle_search_whole_word(cx)),
                                ),
                        )
                        .child(
                            Button::new("run-search")
                                .compact()
                                .primary()
                                .label("Find")
                                .disabled(self.loading)
                                .on_click(cx.listener(|this, _, _, cx| this.run_search(cx))),
                        )
                        .child(
                            Button::new("previous-match")
                                .compact()
                                .ghost()
                                .label("↑")
                                .disabled(self.search_stale || self.search_results.is_empty())
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.previous_match(&PreviousMatch, window, cx)
                                })),
                        )
                        .child(
                            Button::new("next-match")
                                .compact()
                                .ghost()
                                .label("↓")
                                .disabled(self.search_stale || self.search_results.is_empty())
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.next_match(&NextMatch, window, cx)
                                })),
                        )
                        .when_some(
                            query_result_label(
                                self.current_match,
                                self.search_results.len(),
                                self.search_has_completed,
                                "No matches",
                            ),
                            |panel, label| panel.child(label),
                        )
                        .when(self.search_stale, |panel| {
                            panel.child("Outdated").child(
                                Button::new("rerun-search")
                                    .compact()
                                    .primary()
                                    .label("Re-run")
                                    .disabled(self.loading)
                                    .on_click(cx.listener(|this, _, _, cx| this.run_search(cx))),
                            )
                        })
                        .child(
                            Button::new("clear-find")
                                .compact()
                                .ghost()
                                .label("Clear")
                                .disabled(self.search_results.is_empty())
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.clear_search(&ClearSearch, window, cx)
                                })),
                        )
                        .child(
                            Button::new("close-find")
                                .compact()
                                .ghost()
                                .label("Close")
                                .on_click(cx.listener(|this, _, _, cx| this.hide_find(cx))),
                        ),
                )
            })
            .when(self.show_duplicates, |this| {
                this.child(
                    h_flex()
                        .flex_none()
                        .min_h(px(46.0))
                        .flex_wrap()
                        .px_3()
                        .py_1()
                        .gap_2()
                        .border_l_2()
                        .border_b_1()
                        .border_color(cx.theme().border)
                        .bg(cx.theme().secondary.opacity(0.55))
                        .child(
                            div()
                                .px_2()
                                .text_xs()
                                .font_weight(gpui::FontWeight::BOLD)
                                .text_color(cx.theme().accent)
                                .child("DUPLICATES"),
                        )
                        .child(
                            Button::new("duplicate-scope")
                                .compact()
                                .ghost()
                                .label(duplicate_scope)
                                .disabled(self.loading)
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.cycle_duplicate_column(cx)),
                                ),
                        )
                        .child(
                            Button::new("run-duplicates")
                                .compact()
                                .primary()
                                .label(if self.loading { "Checking…" } else { "Check" })
                                .disabled(self.loading)
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.run_duplicate_check(cx)),
                                ),
                        )
                        .child(
                            Button::new("clear-duplicates")
                                .compact()
                                .ghost()
                                .label("Clear")
                                .disabled(self.duplicate_results.is_empty())
                                .on_click(cx.listener(|this, _, _, cx| this.clear_duplicates(cx))),
                        )
                        .when_some(
                            query_result_label(
                                self.current_duplicate_match,
                                self.duplicate_results.len(),
                                self.duplicate_check_has_completed,
                                "No duplicates",
                            ),
                            |panel, label| panel.child(label),
                        )
                        .when(self.duplicate_stale, |panel| {
                            panel.child("Outdated").child(
                                Button::new("rerun-duplicates")
                                    .compact()
                                    .primary()
                                    .label("Re-run")
                                    .disabled(self.loading)
                                    .on_click(
                                        cx.listener(|this, _, _, cx| this.run_duplicate_check(cx)),
                                    ),
                            )
                        })
                        .child(
                            Button::new("previous-duplicate")
                                .compact()
                                .ghost()
                                .label("↑")
                                .disabled(self.duplicate_stale || self.duplicate_results.is_empty())
                                .on_click(
                                    cx.listener(|this, _, _, cx| this.previous_duplicate(cx)),
                                ),
                        )
                        .child(
                            Button::new("next-duplicate")
                                .compact()
                                .ghost()
                                .label("↓")
                                .disabled(self.duplicate_stale || self.duplicate_results.is_empty())
                                .on_click(cx.listener(|this, _, _, cx| this.next_duplicate(cx))),
                        )
                        .child(
                            Button::new("close-duplicates")
                                .compact()
                                .ghost()
                                .label("Close")
                                .on_click(cx.listener(|this, _, _, cx| this.hide_duplicates(cx))),
                        ),
                )
            })
            .child(
                div()
                    .id("table-viewport")
                    .relative()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(table_content)
                    // ScrollableMask is the GPUI Component table pattern for
                    // separating dominant horizontal trackpad movement from the
                    // virtual list's vertical wheel handling.
                    .child(ScrollableMask::new(Axis::Horizontal, &self.column_scroll))
                    .child(
                        div()
                            .id("table-horizontal-scrollbar-track")
                            .occlude()
                            .absolute()
                            .left_0()
                            .right(px(16.0))
                            .bottom_0()
                            .h(px(16.0))
                            .border_t_1()
                            .border_color(cx.theme().border)
                            .bg(cx.theme().secondary)
                            .child(
                                Scrollbar::horizontal(&self.column_scroll)
                                    .id("table-horizontal-scrollbar")
                                    .scrollbar_show(ScrollbarShow::Always),
                            ),
                    )
                    .child(
                        div()
                            .id("table-vertical-scrollbar-track")
                            .occlude()
                            .absolute()
                            .top(px(row_height))
                            .right_0()
                            .bottom(px(16.0))
                            .w(px(16.0))
                            .border_l_1()
                            .border_color(cx.theme().border)
                            .bg(cx.theme().secondary)
                            .child(
                                Scrollbar::vertical(&self.row_scroll)
                                    .id("table-vertical-scrollbar")
                                    .scrollbar_show(ScrollbarShow::Always),
                            ),
                    )
                    .child(
                        div()
                            .absolute()
                            .right_0()
                            .bottom_0()
                            .w(px(16.0))
                            .h(px(16.0))
                            .border_t_1()
                            .border_l_1()
                            .border_color(cx.theme().border)
                            .bg(cx.theme().secondary),
                    ),
            )
            .child(
                h_flex()
                    .flex_none()
                    .h(px(30.0))
                    .px_3()
                    .gap_3()
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .bg(cx.theme().secondary.opacity(0.65))
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(div().w(px(7.0)).h(px(7.0)).rounded_full().bg(if dirty {
                        cx.theme().yellow
                    } else {
                        cx.theme().green
                    }))
                    .child(if dirty { "Unsaved changes" } else { "Saved" })
                    .child("•")
                    .child(format!(
                        "{} delimiter",
                        visible_control(&loaded.parse_info.delimiter)
                    ))
                    .child("•")
                    .child(loaded.parse_info.encoding.clone())
                    .when_some(cell_dimensions, |status, (rows, columns)| {
                        status.child("•").child(format!(
                            "{} × {} cells selected",
                            format_count(rows),
                            format_count(columns)
                        ))
                    })
                    .when(cell_dimensions.is_none() && selected_count > 0, |status| {
                        status
                            .child("•")
                            .child(format!("{} rows selected", format_count(selected_count)))
                    })
                    .child(div().flex_1())
                    .child(if cfg!(target_os = "macos") {
                        "⌘F Find"
                    } else {
                        "Ctrl+F Find"
                    })
                    .child(if cfg!(target_os = "macos") {
                        "⌘, Settings"
                    } else {
                        "Ctrl+, Settings"
                    }),
            )
            .into_any_element()
    }

    fn render_table_context_menu(
        &self,
        window: &Window,
        cx: &mut Context<Self>,
    ) -> Option<gpui::AnyElement> {
        let context_menu = self.table_context_menu?;
        let can_edit = matches!(
            context_menu.kind,
            TableContextMenuKind::Cell { can_edit: true }
        );
        let is_cell_menu = matches!(context_menu.kind, TableContextMenuKind::Cell { .. });
        let selected_count = self
            .selected_rows
            .len()
            .max(usize::from(self.selected_row.is_some()))
            .max(1);
        let delete_label = if selected_count == 1 {
            "Delete row".to_string()
        } else {
            format!("Delete {} rows", format_count(selected_count))
        };
        let restore_label = if selected_count == 1 {
            "Restore row".to_string()
        } else {
            format!("Restore {} rows", format_count(selected_count))
        };
        let copy_shortcut = if cfg!(target_os = "macos") {
            "⌘C"
        } else {
            "Ctrl+C"
        };
        let copy_rows_label = if self.cell_selection.is_some() {
            "Copy selected cells".to_string()
        } else if selected_count == 1 {
            "Copy row".to_string()
        } else {
            format!("Copy {} rows", format_count(selected_count))
        };
        let menu_width = px(220.0);
        let item_count = if is_cell_menu {
            if can_edit { 6.0 } else { 5.0 }
        } else {
            3.0
        };
        let menu_height = px(item_count * 36.0 + 8.0);
        let viewport = window.viewport_size();
        let max_x = (viewport.width - menu_width - px(8.0)).max(px(8.0));
        let max_y = (viewport.height - menu_height - px(8.0)).max(px(8.0));
        let left = context_menu.position.x.max(px(8.0)).min(max_x);
        let top = context_menu.position.y.max(px(8.0)).min(max_y);

        let mut items = Vec::new();
        let row_action_start = if is_cell_menu {
            2 + usize::from(can_edit)
        } else {
            0
        };
        if is_cell_menu {
            items.push(table_context_menu_item(
                "context-copy-cell",
                "Copy cell",
                Some(copy_shortcut),
                context_menu.focused_item == 0,
                false,
                |this, _, window, cx| {
                    this.table_context_menu = None;
                    this.copy_context_cell(&CopyContextCell, window, cx);
                },
                cx,
            ));
            items.push(table_context_menu_item(
                "context-search-cell",
                "Search for this",
                None,
                context_menu.focused_item == 1,
                false,
                |this, _, window, cx| {
                    this.table_context_menu = None;
                    this.search_context_cell(&SearchContextCell, window, cx);
                },
                cx,
            ));
            if can_edit {
                items.push(table_context_menu_item(
                    "context-edit-cell",
                    "Edit cell",
                    None,
                    context_menu.focused_item == 2,
                    false,
                    |this, _, window, cx| {
                        this.table_context_menu = None;
                        this.edit_context_cell(&EditContextCell, window, cx);
                    },
                    cx,
                ));
            }
        }
        items.push(table_context_menu_item(
            "context-delete-row",
            delete_label,
            None,
            context_menu.focused_item == row_action_start,
            true,
            |this, _, _, cx| {
                this.table_context_menu = None;
                this.context_cell = None;
                this.mutate_selected_rows(RowMutation::Delete, cx);
            },
            cx,
        ));
        items.push(table_context_menu_item(
            "context-restore-row",
            restore_label,
            None,
            context_menu.focused_item == row_action_start + 1,
            false,
            |this, _, _, cx| {
                this.table_context_menu = None;
                this.context_cell = None;
                this.mutate_selected_rows(RowMutation::Restore, cx);
            },
            cx,
        ));
        items.push(table_context_menu_item(
            "context-copy-row",
            copy_rows_label,
            Some(copy_shortcut),
            context_menu.focused_item == row_action_start + 2,
            false,
            |this, _, window, cx| {
                this.table_context_menu = None;
                this.context_cell = None;
                this.copy_selected(window, cx);
            },
            cx,
        ));

        Some(
            v_flex()
                .id("table-context-menu")
                .absolute()
                .left(left)
                .top(top)
                .w(menu_width)
                .p_1()
                .rounded(px(6.0))
                .border_1()
                .border_color(cx.theme().border)
                .bg(cx.theme().popover)
                .text_color(cx.theme().popover_foreground)
                .occlude()
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.table_context_menu = None;
                    this.context_cell = None;
                    cx.notify();
                }))
                .children(items)
                .into_any_element(),
        )
    }
}

fn table_context_menu_item(
    id: &'static str,
    label: impl Into<SharedString>,
    shortcut: Option<&'static str>,
    focused: bool,
    destructive: bool,
    on_click: impl Fn(&mut QuickRowsView, &ClickEvent, &mut Window, &mut Context<QuickRowsView>)
    + 'static,
    cx: &mut Context<QuickRowsView>,
) -> gpui::AnyElement {
    let label = label.into();
    h_flex()
        .id(id)
        .h(px(36.0))
        .px_3()
        .gap_3()
        .rounded(px(4.0))
        .cursor_pointer()
        .text_sm()
        .when(focused, |item| item.bg(cx.theme().accent))
        .when(destructive, |item| item.text_color(cx.theme().danger))
        .hover(|item| item.bg(cx.theme().accent))
        .child(div().flex_1().child(label))
        .when_some(shortcut, |item, shortcut| {
            item.child(
                div()
                    .text_color(cx.theme().muted_foreground)
                    .child(shortcut),
            )
        })
        .on_click(cx.listener(on_click))
        .into_any_element()
}

impl gpui::Focusable for QuickRowsView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for QuickRowsView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let content = self.render_table(window, cx);
        let settings = self.show_settings.then(|| self.render_settings(cx));
        let header_prompt = self
            .show_header_prompt
            .then(|| self.render_header_prompt(cx));
        let shortcuts = self.show_shortcuts.then(|| self.render_shortcuts(cx));
        let about = self.show_about.then(|| self.render_about(cx));
        let external_save = self
            .pending_external_save
            .is_some()
            .then(|| self.render_external_save_confirmation(cx));
        let unsaved = self
            .pending_destructive
            .is_some()
            .then(|| self.render_unsaved_confirmation(cx));
        let bulk_confirmation = self
            .pending_bulk_action
            .is_some()
            .then(|| self.render_bulk_confirmation(cx));
        let modal_active = self.modal_active();
        let table_context_menu = (!modal_active)
            .then(|| self.render_table_context_menu(window, cx))
            .flatten();
        let cell_editing = self.editing_cell.is_some();
        let can_cancel = self.operation_cancellation.is_some();
        let can_retry_rows = self.failed_row_range.is_some();
        let can_reload_external = self.external_change_detected && !self.loading;
        div()
            .key_context("QuickRows")
            .track_focus(&self.focus_handle)
            .on_mouse_move(cx.listener(Self::update_column_resize))
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(Self::finish_pointer_interaction),
            )
            .on_action(cx.listener(Self::cancel_cell_edit_action))
            .on_action(cx.listener(Self::clear_row_selection))
            .when(!modal_active && !cell_editing, |this| {
                this.on_action(cx.listener(Self::open_dialog))
                    .on_action(cx.listener(Self::save_file))
                    .on_action(cx.listener(Self::save_file_as))
                    .on_action(cx.listener(Self::reload_file))
                    .on_action(cx.listener(Self::clear_file))
                    .on_action(cx.listener(Self::close_window_action))
                    .on_action(cx.listener(Self::quit_app))
                    .on_action(cx.listener(Self::show_find))
                    .on_action(cx.listener(Self::clear_search))
                    .on_action(cx.listener(Self::next_match))
                    .on_action(cx.listener(Self::previous_match))
                    .on_action(cx.listener(Self::check_duplicates))
                    .on_action(cx.listener(Self::toggle_theme))
                    .on_action(cx.listener(Self::open_settings))
                    .on_action(cx.listener(Self::open_parse_settings))
                    .on_action(cx.listener(Self::show_shortcuts))
                    .on_action(cx.listener(Self::show_about))
                    .on_action(cx.listener(Self::toggle_index))
                    .on_action(cx.listener(Self::compact_rows))
                    .on_action(cx.listener(Self::default_rows))
                    .on_action(cx.listener(Self::spacious_rows))
                    .on_action(cx.listener(Self::copy_selected_action))
                    .on_action(cx.listener(Self::copy_context_cell))
                    .on_action(cx.listener(Self::search_context_cell))
                    .on_action(cx.listener(Self::edit_context_cell))
                    .on_action(cx.listener(Self::activate_context_menu))
                    .on_action(cx.listener(Self::toggle_delete_selected_action))
                    .on_action(cx.listener(Self::delete_selected_rows_action))
                    .on_action(cx.listener(Self::restore_selected_rows_action))
                    .on_action(cx.listener(Self::select_all_rows))
                    .on_action(cx.listener(Self::select_previous_row))
                    .on_action(cx.listener(Self::select_next_row))
                    .on_action(cx.listener(Self::select_previous_column))
                    .on_action(cx.listener(Self::select_next_column))
                    .on_action(cx.listener(Self::extend_previous_column))
                    .on_action(cx.listener(Self::extend_next_column))
                    .on_action(cx.listener(Self::select_first_row))
                    .on_action(cx.listener(Self::select_last_row))
                    .on_action(cx.listener(Self::extend_previous_row))
                    .on_action(cx.listener(Self::extend_next_row))
                    .on_action(cx.listener(Self::extend_first_row))
                    .on_action(cx.listener(Self::extend_last_row))
                    .on_action(cx.listener(Self::select_page_up))
                    .on_action(cx.listener(Self::select_page_down))
                    .on_action(cx.listener(Self::extend_page_up))
                    .on_action(cx.listener(Self::extend_page_down))
            })
            .size_full()
            .bg(cx.theme().background)
            .text_color(cx.theme().foreground)
            .child(content)
            .when_some(table_context_menu, |this, menu| this.child(menu))
            .when_some(settings, |this, settings| this.child(settings))
            .when_some(header_prompt, |this, prompt| this.child(prompt))
            .when_some(shortcuts, |this, shortcuts| this.child(shortcuts))
            .when_some(about, |this, about| this.child(about))
            .when_some(external_save, |this, confirmation| this.child(confirmation))
            .when_some(unsaved, |this, unsaved| this.child(unsaved))
            .when_some(bulk_confirmation, |this, confirmation| {
                this.child(confirmation)
            })
            .when_some(self.error.clone(), |this, error| {
                this.child(
                    div()
                        .absolute()
                        .top_3()
                        .left_3()
                        .right_3()
                        .p_3()
                        .rounded_md()
                        .bg(cx.theme().danger)
                        .text_color(cx.theme().danger_foreground)
                        .child(
                            h_flex()
                                .gap_3()
                                .child(div().flex_1().child(error))
                                .when(can_retry_rows, |status| {
                                    status.child(
                                        Button::new("retry-row-load").label("Retry").on_click(
                                            cx.listener(|this, _, _, cx| {
                                                this.retry_failed_rows(cx)
                                            }),
                                        ),
                                    )
                                })
                                .child(
                                    Button::new("dismiss-error")
                                        .ghost()
                                        .label("Dismiss")
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.error = None;
                                            cx.notify();
                                        })),
                                ),
                        ),
                )
            })
            .when_some(self.notice.clone(), |this, notice| {
                this.child(
                    div()
                        .absolute()
                        .bottom_3()
                        .right_3()
                        .p_3()
                        .rounded_md()
                        .bg(cx.theme().popover)
                        .border_1()
                        .border_color(cx.theme().border)
                        .child(
                            h_flex()
                                .gap_3()
                                .child(div().flex_1().child(notice))
                                .when(can_cancel, |status| {
                                    status.child(
                                        Button::new("cancel-operation")
                                            .danger()
                                            .label("Cancel")
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.cancel_current_operation(cx)
                                            })),
                                    )
                                })
                                .when(can_reload_external, |status| {
                                    status.child(
                                        Button::new("reload-external-change")
                                            .label("Reload")
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.reload_file(&ReloadFile, window, cx)
                                            })),
                                    )
                                })
                                .child(
                                    Button::new("dismiss-notice")
                                        .ghost()
                                        .label("Dismiss")
                                        .on_click(cx.listener(|this, _, _, cx| {
                                            this.notice = None;
                                            cx.notify();
                                        })),
                                ),
                        ),
                )
            })
    }
}

fn settings_section_title(label: &'static str, cx: &App) -> gpui::AnyElement {
    h_flex()
        .pt_1()
        .gap_2()
        .child(
            div()
                .w(px(4.0))
                .h(px(14.0))
                .rounded_full()
                .bg(cx.theme().accent),
        )
        .child(
            div()
                .text_xs()
                .font_weight(gpui::FontWeight::BOLD)
                .text_color(cx.theme().muted_foreground)
                .child(label),
        )
        .child(div().flex_1().h(px(1.0)).bg(cx.theme().border))
        .into_any_element()
}

fn settings_dropdown(
    id: &'static str,
    label: impl Into<SharedString>,
    width: Pixels,
    view: WeakEntity<QuickRowsView>,
    choices: Vec<(&'static str, bool, SettingsChoice)>,
) -> impl IntoElement {
    Button::new(id)
        .w(width)
        .label(label)
        .dropdown_menu(move |menu, _, _| {
            choices.iter().fold(menu, |menu, (label, checked, choice)| {
                let view = view.clone();
                let choice = *choice;
                menu.item(PopupMenuItem::new(*label).checked(*checked).on_click(
                    move |_, window, cx| {
                        let _ = view.update(cx, |this, cx| {
                            this.apply_settings_choice(choice, window, cx)
                        });
                    },
                ))
            })
        })
}

fn settings_row(label: &'static str, control: impl IntoElement, cx: &App) -> gpui::AnyElement {
    h_flex()
        .min_h(px(48.0))
        .p_3()
        .gap_4()
        .rounded(px(9.0))
        .border_1()
        .border_color(cx.theme().border)
        .bg(cx.theme().background)
        .shadow_sm()
        .child(
            div()
                .flex_1()
                .min_w_0()
                .text_sm()
                .font_weight(gpui::FontWeight::MEDIUM)
                .text_color(cx.theme().foreground)
                .child(label),
        )
        .child(control)
        .into_any_element()
}

fn settings_description(text: impl Into<SharedString>, cx: &App) -> gpui::AnyElement {
    div()
        .mt(px(-12.0))
        .px_3()
        .max_w(px(430.0))
        .text_sm()
        .line_height(relative(1.45))
        .text_color(cx.theme().muted_foreground)
        .child(text.into())
        .into_any_element()
}

fn settings_parse_diagnostic(label: &'static str, summary: String, cx: &App) -> gpui::AnyElement {
    v_flex()
        .gap_1()
        .child(
            div()
                .text_xs()
                .font_weight(gpui::FontWeight::SEMIBOLD)
                .text_color(cx.theme().muted_foreground)
                .child(label),
        )
        .child(
            div()
                .text_sm()
                .line_height(relative(1.45))
                .text_color(cx.theme().foreground)
                .child(summary),
        )
        .into_any_element()
}

fn parse_diagnostic_rows(
    detected: String,
    effective_changes: Vec<String>,
) -> Vec<(&'static str, String)> {
    if effective_changes.is_empty() {
        vec![("Detected and effective settings", detected)]
    } else {
        vec![
            ("Detected from file", detected),
            ("Overrides in effect", effective_changes.join(" · ")),
        ]
    }
}

fn settings_parse_diagnostics(
    detected: String,
    effective_changes: Vec<String>,
    cx: &App,
) -> gpui::AnyElement {
    let rows = parse_diagnostic_rows(detected, effective_changes);
    let mut panel = v_flex()
        .gap_3()
        .p_3()
        .rounded(px(9.0))
        .border_1()
        .border_color(cx.theme().border)
        .bg(cx.theme().background)
        .shadow_sm();

    for (index, (label, summary)) in rows.into_iter().enumerate() {
        if index > 0 {
            panel = panel.child(div().h(px(1.0)).w_full().bg(cx.theme().border));
        }
        panel = panel.child(settings_parse_diagnostic(label, summary, cx));
    }
    panel.into_any_element()
}

fn is_named_delimiter(value: &str) -> bool {
    matches!(
        value.to_lowercase().as_str(),
        "comma" | "tab" | "semicolon" | "pipe" | "space"
    )
}

fn is_named_quote(value: &str) -> bool {
    matches!(value.to_lowercase().as_str(), "double" | "single")
}

fn is_named_escape(value: &str) -> bool {
    matches!(value.to_lowercase().as_str(), "none" | "off" | "backslash")
}

fn is_named_comment(value: &str) -> bool {
    matches!(value.to_lowercase().as_str(), "none" | "off" | "hash" | "#")
}

fn validate_syntax_overrides(
    candidate: &ParseOverrides,
    base: Option<&ParseInfo>,
) -> Result<(), String> {
    quickrows_core::validate_parse_overrides(candidate)?;
    let delimiter = candidate
        .delimiter
        .as_deref()
        .and_then(quickrows_core::csv::normalize_delimiter)
        .or_else(|| base.and_then(|info| info.delimiter.chars().next()))
        .unwrap_or(',');
    let quote = candidate
        .quote
        .as_deref()
        .and_then(quickrows_core::csv::normalize_quote)
        .or_else(|| base.and_then(|info| info.quote.chars().next()))
        .unwrap_or('"');
    let escape = match candidate.escape.as_deref() {
        Some(value) => quickrows_core::csv::normalize_escape(value).flatten(),
        None => base.and_then(|info| info.escape.as_deref()?.chars().next()),
    };
    let comment = match candidate.comment.as_deref() {
        Some(value) => quickrows_core::csv::normalize_comment(value).flatten(),
        None => base.and_then(|info| info.comment.as_deref()?.chars().next()),
    };
    let syntax = [
        ("delimiter", Some(delimiter)),
        ("quote", Some(quote)),
        ("escape", escape),
        ("comment", comment),
    ];
    for left in 0..syntax.len() {
        for right in left + 1..syntax.len() {
            if syntax[left].1.is_some() && syntax[left].1 == syntax[right].1 {
                return Err(format!(
                    "CSV {} and {} characters must be different",
                    syntax[left].0, syntax[right].0
                ));
            }
        }
    }
    Ok(())
}

fn is_valid_syntax_character(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(ch) if !matches!(ch, '\0' | '\r' | '\n')) && chars.next().is_none()
}

fn cell_matches_search(value: &str, query: &str, match_case: bool, whole_word: bool) -> bool {
    if query.is_empty() {
        return false;
    }
    if match_case {
        if whole_word {
            value == query
        } else {
            value.contains(query)
        }
    } else {
        let value = value.to_lowercase();
        let query = query.to_lowercase();
        if whole_word {
            value == query
        } else {
            value.contains(&query)
        }
    }
}

const TOOLBAR_LABEL_BREAKPOINT: f32 = 900.0;

fn toolbar_shows_labels(viewport_width: f32) -> bool {
    viewport_width >= TOOLBAR_LABEL_BREAKPOINT
}

fn toolbar_divider(cx: &App) -> gpui::Div {
    div()
        .flex_none()
        .w(px(1.0))
        .h(px(24.0))
        .ml_1()
        .mr_1()
        .bg(cx.theme().border)
}

fn format_count(count: usize) -> String {
    let digits = count.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len().saturating_sub(1) / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(digit);
    }
    formatted
}

fn counted_noun(count: usize, singular: &'static str, plural: &'static str) -> &'static str {
    if count == 1 { singular } else { plural }
}

fn query_result_label(
    current: usize,
    result_count: usize,
    has_completed: bool,
    empty_label: &str,
) -> Option<String> {
    if result_count > 0 {
        Some(format!(
            "{} of {}",
            format_count(current + 1),
            format_count(result_count)
        ))
    } else if has_completed {
        Some(empty_label.to_string())
    } else {
        None
    }
}

fn next_column_scope(current: Option<usize>, column_count: usize) -> Option<usize> {
    match current {
        None if column_count > 0 => Some(0),
        Some(column) if column + 1 < column_count => Some(column + 1),
        _ => None,
    }
}

fn display_header_label(header: &str, column: usize) -> String {
    let label = header.split_whitespace().collect::<Vec<_>>().join(" ");
    if label.is_empty() {
        format!("Column {}", column + 1)
    } else {
        label
    }
}

fn column_scope_label(column: Option<usize>, headers: &[String]) -> String {
    match column {
        Some(column) => headers
            .get(column)
            .map(|header| display_header_label(header, column))
            .unwrap_or_else(|| format!("Column {}", column + 1)),
        None => "Entire row".to_string(),
    }
}

fn override_label(value: Option<&str>) -> String {
    value
        .map(|value| value.replace('-', " "))
        .unwrap_or_else(|| "Automatic".to_string())
}

fn visible_control(value: &str) -> &str {
    match value {
        "\t" => "Tab",
        " " => "Space",
        value => value,
    }
}

fn size_override_label(value: Option<usize>) -> String {
    value
        .map(|value| format!("{} MiB", value / (1 << 20)))
        .unwrap_or_else(|| "Default".to_string())
}

fn delimiter_label(value: &str) -> String {
    match value {
        "," => "Comma".to_string(),
        "\t" => "Tab".to_string(),
        ";" => "Semicolon".to_string(),
        "|" => "Pipe".to_string(),
        " " => "Space".to_string(),
        value => visible_control(value).to_string(),
    }
}

fn quote_label(value: &str) -> String {
    match value {
        "\"" => "Double quote".to_string(),
        "'" => "Single quote".to_string(),
        value => format!("{} quote", visible_control(value)),
    }
}

fn optional_character_label(value: Option<&str>) -> String {
    match value {
        None => "None".to_string(),
        Some("\\") => "Backslash".to_string(),
        Some(value) => visible_control(value).to_string(),
    }
}

fn malformed_rows_label(value: &str) -> String {
    match value {
        "strict" => "Reject".to_string(),
        "skip" => "Skip".to_string(),
        "repair" => "Repair".to_string(),
        value => value.to_string(),
    }
}

fn parse_limit_label(value: usize) -> String {
    if value == usize::MAX {
        "Default".to_string()
    } else {
        format!("{} MiB", value / (1 << 20))
    }
}

fn parse_summary(info: &ParseInfo) -> String {
    let comments = info
        .comment
        .as_deref()
        .map(|value| format!("{} comments", visible_control(value)))
        .unwrap_or_else(|| "No comments".to_string());
    format!(
        "{} delimiter · {} · {} · {}\n{} · {comments} · Excel sep= {} · {} malformed rows",
        delimiter_label(&info.delimiter),
        quote_label(&info.quote),
        info.encoding,
        info.line_ending.to_uppercase(),
        if info.has_headers {
            "First row is header"
        } else {
            "No headers"
        },
        if info.excel_sep { "on" } else { "off" },
        malformed_rows_label(&info.malformed),
    )
}

fn parse_effective_changes(detected: &ParseInfo, effective: &ParseInfo) -> Vec<String> {
    let mut changes = Vec::new();
    if detected.delimiter != effective.delimiter {
        changes.push(format!(
            "Delimiter: {}",
            delimiter_label(&effective.delimiter)
        ));
    }
    if detected.quote != effective.quote {
        changes.push(format!("Quote: {}", quote_label(&effective.quote)));
    }
    if detected.escape != effective.escape {
        changes.push(format!(
            "Escape: {}",
            optional_character_label(effective.escape.as_deref())
        ));
    }
    if detected.comment != effective.comment {
        changes.push(format!(
            "Comments: {}",
            optional_character_label(effective.comment.as_deref())
        ));
    }
    if detected.encoding != effective.encoding {
        changes.push(format!("Encoding: {}", effective.encoding));
    }
    if detected.line_ending != effective.line_ending {
        changes.push(format!(
            "Line ending: {}",
            effective.line_ending.to_uppercase()
        ));
    }
    if detected.has_headers != effective.has_headers {
        changes.push(format!(
            "Headers: {}",
            if effective.has_headers {
                "First row"
            } else {
                "None"
            }
        ));
    }
    if detected.excel_sep != effective.excel_sep {
        changes.push(format!(
            "Excel sep=: {}",
            if effective.excel_sep { "On" } else { "Off" }
        ));
    }
    if detected.malformed != effective.malformed {
        changes.push(format!(
            "Malformed rows: {}",
            malformed_rows_label(&effective.malformed)
        ));
    }
    if detected.max_field_size != effective.max_field_size {
        changes.push(format!(
            "Field limit: {}",
            parse_limit_label(effective.max_field_size)
        ));
    }
    if detected.max_record_size != effective.max_record_size {
        changes.push(format!(
            "Record limit: {}",
            parse_limit_label(effective.max_record_size)
        ));
    }
    changes
}

fn parse_warning_location(warning: &ParseWarning) -> String {
    let mut parts = Vec::new();
    if let Some(record) = warning.record {
        parts.push(format!("record {record}"));
    }
    if let Some(line) = warning.line {
        parts.push(format!("line {line}"));
    }
    if let Some(field) = warning.field {
        parts.push(format!("field {field}"));
    }
    if let Some(byte) = warning.byte {
        parts.push(format!("byte {byte}"));
    }
    if let (Some(expected), Some(actual)) = (warning.expected_len, warning.len) {
        parts.push(format!("expected {expected} fields, found {actual}"));
    }
    parts.push(warning.kind.clone());
    parts.join(" · ")
}

fn header_cell(label: &str, width: f32, cx: &App) -> gpui::Div {
    div()
        .w(px(width))
        .min_w(px(width))
        .max_w(px(width))
        .flex_none()
        .h_full()
        .px_2()
        .flex()
        .items_center()
        .font_weight(gpui::FontWeight::SEMIBOLD)
        .text_color(cx.theme().secondary_foreground)
        .overflow_hidden()
        .whitespace_nowrap()
        .text_ellipsis()
        .child(label.to_string())
}

fn body_cell_frame(width: f32, cx: &App) -> gpui::Div {
    div()
        .w(px(width))
        .min_w(px(width))
        .max_w(px(width))
        .flex_none()
        .h_full()
        .flex()
        .items_center()
        .border_r_1()
        .border_color(cx.theme().border)
        .overflow_hidden()
}

fn body_cell(value: &str, width: f32, cx: &App) -> gpui::Div {
    body_cell_frame(width, cx)
        .px_2()
        .text_ellipsis()
        .child(value.to_string())
}

fn settings_path() -> PathBuf {
    ProjectDirs::from("com", "el", "csv-viewer")
        .map(|dirs| dirs.config_dir().join("settings.json"))
        .unwrap_or_else(|| PathBuf::from("quickrows-settings.json"))
}

fn migrate_legacy_settings() {
    let target = settings_path();
    if target.exists() {
        return;
    }
    let Some(legacy) = ProjectDirs::from("com", "el", "QuickRows")
        .map(|dirs| dirs.config_dir().join("settings.json"))
        .filter(|path| path.is_file())
    else {
        return;
    };
    if let Some(parent) = target.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::copy(legacy, target);
}

fn diagnostics_path() -> PathBuf {
    ProjectDirs::from("com", "el", "csv-viewer")
        .map(|dirs| dirs.data_dir().join("logs"))
        .unwrap_or_else(|| PathBuf::from("quickrows-logs"))
}

fn cache_path() -> PathBuf {
    ProjectDirs::from("com", "el", "csv-viewer")
        .map(|dirs| dirs.cache_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("quickrows-cache"))
}

fn file_fingerprint(path: &Path) -> Option<(u64, SystemTime)> {
    let metadata = std::fs::metadata(path).ok()?;
    Some((metadata.len(), metadata.modified().ok()?))
}

fn display_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_else(|| path.to_str().unwrap_or("CSV"))
        .to_string()
}

fn parse_fragment(value: &str) -> Option<CsvFragment> {
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .ok()?
        .parse()
        .ok()
}

fn open_target_from_value(value: &str) -> Option<OpenTarget> {
    if let Ok(url) = url::Url::parse(value) {
        if url.scheme() == "file" {
            let fragment = url.fragment().and_then(parse_fragment);
            let path = url.to_file_path().ok().filter(|path| path.is_file())?;
            return Some(OpenTarget { path, fragment });
        }
    }
    let path = PathBuf::from(value);
    if path.is_file() {
        return Some(path.into());
    }
    let (path, fragment) = value.rsplit_once('#')?;
    let path = PathBuf::from(path);
    path.is_file().then(|| OpenTarget {
        path,
        fragment: parse_fragment(fragment),
    })
}

fn initial_paths() -> Vec<OpenTarget> {
    std::env::args_os()
        .skip(1)
        .filter_map(|value| open_target_from_value(&value.to_string_lossy()))
        .collect()
}

#[cfg(test)]
fn path_from_open_value(value: &str) -> Option<PathBuf> {
    open_target_from_value(value).map(|target| target.path)
}

fn coordinate_instance(
    paths: &[OpenTarget],
) -> Result<Option<Arc<Mutex<VecDeque<RuntimeRequest>>>>, String> {
    let address = SocketAddr::from(([127, 0, 0, 1], INSTANCE_PORT));
    match TcpListener::bind(address) {
        Ok(listener) => {
            let requests = Arc::new(Mutex::new(VecDeque::new()));
            let listener_requests = requests.clone();
            std::thread::Builder::new()
                .name("quickrows-instance-listener".to_string())
                .spawn(move || {
                    for stream in listener.incoming() {
                        let Ok(mut stream) = stream else { continue };
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                        let mut bytes = Vec::new();
                        if Read::take(&mut stream, 4 * 1024 * 1024)
                            .read_to_end(&mut bytes)
                            .is_err()
                        {
                            continue;
                        }
                        let parts = bytes
                            .split(|byte| *byte == 0)
                            .filter_map(|part| std::str::from_utf8(part).ok())
                            .collect::<Vec<_>>();
                        if parts.first().copied() != Some(INSTANCE_MAGIC) {
                            continue;
                        }
                        if let Ok(mut requests) = listener_requests.lock() {
                            for part in parts.into_iter().skip(1) {
                                if part == "A" {
                                    requests.push_back(RuntimeRequest::Activate);
                                } else if let Some(target) =
                                    part.strip_prefix('P').and_then(open_target_from_value)
                                {
                                    requests.push_back(RuntimeRequest::Open(target));
                                }
                            }
                        }
                        let _ = stream.write_all(b"OK");
                    }
                })
                .map_err(|error| error.to_string())?;
            Ok(Some(requests))
        }
        Err(bind_error) if bind_error.kind() == std::io::ErrorKind::AddrInUse => {
            let mut stream =
                TcpStream::connect_timeout(&address, Duration::from_secs(2)).map_err(|error| {
                    format!("QuickRows is already running, but forwarding failed: {error}")
                })?;
            stream
                .write_all(INSTANCE_MAGIC.as_bytes())
                .and_then(|_| stream.write_all(&[0]))
                .map_err(|error| error.to_string())?;
            if paths.is_empty() {
                stream
                    .write_all(b"A\0")
                    .map_err(|error| error.to_string())?;
            } else {
                for target in paths {
                    stream.write_all(b"P").map_err(|error| error.to_string())?;
                    let mut value = target.path.to_string_lossy().into_owned();
                    if let Some(fragment) = target.fragment.as_ref() {
                        value.push('#');
                        value.push_str(&fragment.to_string());
                    }
                    stream
                        .write_all(value.as_bytes())
                        .and_then(|_| stream.write_all(&[0]))
                        .map_err(|error| error.to_string())?;
                }
            }
            stream
                .shutdown(Shutdown::Write)
                .map_err(|error| error.to_string())?;
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let mut response = [0u8; 2];
            stream
                .read_exact(&mut response)
                .map_err(|error| format!("QuickRows forwarding acknowledgement failed: {error}"))?;
            if &response != b"OK" {
                return Err("Another process is using the QuickRows instance channel".to_string());
            }
            Ok(None)
        }
        Err(error) => Err(format!(
            "Unable to initialize single-instance channel: {error}"
        )),
    }
}

fn main() {
    let paths = initial_paths();
    let requests = match coordinate_instance(&paths) {
        Ok(Some(requests)) => requests,
        Ok(None) => return,
        Err(error) => {
            eprintln!("{error}");
            return;
        }
    };
    let initial_path = paths.first().cloned();
    if let Ok(mut pending) = requests.lock() {
        pending.extend(paths.into_iter().skip(1).map(RuntimeRequest::Open));
    }
    migrate_legacy_settings();
    let initial_settings = SettingsStore::new(settings_path())
        .load()
        .unwrap_or_default();
    let diagnostics_directory = diagnostics_path();
    let (diagnostics, diagnostics_error) =
        match Diagnostics::new(diagnostics_directory.clone(), false) {
            Ok(diagnostics) => (diagnostics, None),
            Err(error) => {
                eprintln!("QuickRows diagnostics are unavailable: {error}");
                (
                    Diagnostics::disabled(diagnostics_directory),
                    Some(format!("Diagnostics are unavailable: {error}")),
                )
            }
        };
    diagnostics.install_panic_hook();
    let application = Application::new().with_assets(Assets);
    let url_requests = requests.clone();
    application.on_open_urls(move |values| {
        if let Ok(mut requests) = url_requests.lock() {
            requests.push_back(RuntimeRequest::Activate);
            requests.extend(
                values
                    .into_iter()
                    .filter_map(|value| open_target_from_value(&value))
                    .map(RuntimeRequest::Open),
            );
        }
    });
    let reopen_requests = requests.clone();
    application.on_reopen(move |cx| {
        cx.activate(true);
        if let Ok(mut requests) = reopen_requests.lock() {
            requests.push_back(RuntimeRequest::Activate);
        }
    });
    application.run(move |cx: &mut App| {
        gpui_component::init(cx);
        let initial_mode = match initial_settings.theme {
            ThemePreference::Light => ThemeMode::Light,
            ThemePreference::Dark => ThemeMode::Dark,
            ThemePreference::System => ThemeMode::from(cx.window_appearance()),
        };
        Theme::change(initial_mode, None, cx);
        cx.activate(true);
        #[cfg(target_os = "macos")]
        let primary = "cmd";
        #[cfg(not(target_os = "macos"))]
        let primary = "ctrl";
        cx.bind_keys([
            KeyBinding::new(&format!("{primary}-o"), OpenFile, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-s"), SaveFile, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-shift-s"), SaveFileAs, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-r"), ReloadFile, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-shift-k"), ClearFile, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-w"), CloseWindow, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-q"), QuitApp, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-f"), Find, Some("QuickRows")),
            KeyBinding::new(
                &format!("{primary}-shift-f"),
                ClearSearch,
                Some("QuickRows"),
            ),
            KeyBinding::new("f3", NextMatch, Some("QuickRows")),
            KeyBinding::new("shift-f3", PreviousMatch, Some("QuickRows")),
            KeyBinding::new(
                &format!("{primary}-shift-d"),
                CheckDuplicates,
                Some("QuickRows"),
            ),
            KeyBinding::new(
                &format!("{primary}-shift-t"),
                ToggleTheme,
                Some("QuickRows"),
            ),
            KeyBinding::new(&format!("{primary}-,"), OpenSettings, Some("QuickRows")),
            KeyBinding::new(
                &format!("{primary}-shift-p"),
                OpenParseSettings,
                Some("QuickRows"),
            ),
            KeyBinding::new(&format!("{primary}-i"), ToggleIndex, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-a"), SelectAllRows, Some("QuickRows")),
            KeyBinding::new("escape", ClearRowSelection, Some("QuickRows")),
            KeyBinding::new("enter", ActivateContextMenu, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-c"), CopySelected, Some("QuickRows")),
            KeyBinding::new("delete", DeleteSelectedRows, Some("QuickRows")),
            KeyBinding::new("backspace", DeleteSelectedRows, Some("QuickRows")),
            KeyBinding::new("up", SelectPreviousRow, Some("QuickRows")),
            KeyBinding::new("down", SelectNextRow, Some("QuickRows")),
            KeyBinding::new("left", SelectPreviousColumn, Some("QuickRows")),
            KeyBinding::new("right", SelectNextColumn, Some("QuickRows")),
            KeyBinding::new("shift-left", ExtendPreviousColumn, Some("QuickRows")),
            KeyBinding::new("shift-right", ExtendNextColumn, Some("QuickRows")),
            KeyBinding::new("home", SelectFirstRow, Some("QuickRows")),
            KeyBinding::new("end", SelectLastRow, Some("QuickRows")),
            KeyBinding::new("pageup", SelectPageUp, Some("QuickRows")),
            KeyBinding::new("pagedown", SelectPageDown, Some("QuickRows")),
            KeyBinding::new("shift-up", ExtendPreviousRow, Some("QuickRows")),
            KeyBinding::new("shift-down", ExtendNextRow, Some("QuickRows")),
            KeyBinding::new("shift-home", ExtendFirstRow, Some("QuickRows")),
            KeyBinding::new("shift-end", ExtendLastRow, Some("QuickRows")),
            KeyBinding::new("shift-pageup", ExtendPageUp, Some("QuickRows")),
            KeyBinding::new("shift-pagedown", ExtendPageDown, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-alt-1"), CompactRows, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-alt-2"), DefaultRows, Some("QuickRows")),
            KeyBinding::new(&format!("{primary}-alt-3"), SpaciousRows, Some("QuickRows")),
        ]);

        let diagnostics_error_for_window = diagnostics_error.clone();
        let requests_for_window = requests.clone();
        let bounds = Bounds::centered(None, size(px(800.0), px(600.0)), cx);
        cx.open_window(
            WindowOptions {
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                app_id: Some("com.el.csv-viewer".to_string()),
                window_min_size: Some(size(px(640.0), px(420.0))),
                ..Default::default()
            },
            |window, cx| {
                window.set_window_title(BASE_TITLE);
                let view = cx.new(|cx| QuickRowsView::new(initial_path, window, cx));
                let weak_view = view.downgrade();
                let window_handle = window.window_handle();
                view.update(cx, |view, cx| {
                    view.self_weak = Some(weak_view.clone());
                    view.error = diagnostics_error_for_window.clone().map(Into::into);
                    view.track_runtime_requests(requests_for_window.clone(), window_handle, cx);
                    view.track_external_changes(cx);
                });
                window.on_window_should_close(cx, move |_, cx| {
                    weak_view
                        .update(cx, |view, cx| {
                            if view.is_dirty() {
                                view.pending_destructive = Some(PendingDestructiveAction::Close);
                                cx.notify();
                                false
                            } else {
                                true
                            }
                        })
                        .unwrap_or(true)
                });
                cx.new(|cx| Root::new(view, window, cx))
            },
        )
        .expect("Failed to open QuickRows window");
    });
}
