// Workspace-owned state and pure state transformations.
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

fn requeue_deferred_runtime_requests(
    requests: &Mutex<VecDeque<RuntimeRequest>>,
    current: RuntimeRequest,
    remaining: VecDeque<RuntimeRequest>,
) {
    if let Ok(mut requests) = requests.lock() {
        for request in remaining.into_iter().rev() {
            requests.push_front(request);
        }
        requests.push_front(current);
    }
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
    cells: Arc<[SharedString]>,
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

#[derive(Clone)]
struct ColumnLayout {
    widths: Arc<[f32]>,
    offsets: Arc<[f32]>,
}

impl ColumnLayout {
    fn from_settings(column_count: usize, settings: &AppSettings) -> Self {
        let widths = (0..column_count)
            .map(|column| {
                settings
                    .column_widths
                    .get(column)
                    .copied()
                    .unwrap_or(settings.column_width)
                    .max(MIN_COLUMN_WIDTH)
            })
            .collect::<Vec<_>>();
        let mut offsets = Vec::with_capacity(column_count.saturating_add(1));
        offsets.push(0.0);
        for width in &widths {
            let next = (offsets.last().copied().unwrap_or_default() + width).min(f32::MAX);
            offsets.push(next);
        }
        Self {
            widths: Arc::from(widths),
            offsets: Arc::from(offsets),
        }
    }

    fn width(&self, column: usize) -> f32 {
        self.widths.get(column).copied().unwrap_or_default()
    }

    fn total_width(&self) -> f32 {
        self.offsets.last().copied().unwrap_or_default()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ColumnRenderPlan {
    runs: Vec<std::ops::Range<usize>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ActiveHighlight {
    Search,
    Duplicates,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
enum QueryScopeKind {
    Search,
    Duplicates,
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
    headers: Arc<[String]>,
    header_labels: Arc<[SharedString]>,
    row_count: usize,
    detected_parse_info: ParseInfo,
    parse_info: ParseInfo,
    warnings: Vec<ParseWarning>,
    file_fingerprint: Option<FileFingerprint>,
    dirty: bool,
}

#[derive(Default)]
struct DocumentState {
    loaded: Option<LoadedDocument>,
    external_change_detected: bool,
    pending_initial_path: Option<OpenTarget>,
    open_request_id: u64,
}

#[derive(Default)]
enum ForegroundOperation {
    #[default]
    Idle,
    Running {
        kind: OperationKind,
        cancellation: CancellationToken,
    },
}

impl ForegroundOperation {
    fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }

    fn kind(&self) -> Option<OperationKind> {
        match self {
            Self::Idle => None,
            Self::Running { kind, .. } => Some(*kind),
        }
    }

    fn cancellation(&self) -> Option<&CancellationToken> {
        match self {
            Self::Idle => None,
            Self::Running { cancellation, .. } => Some(cancellation),
        }
    }

    fn start(&mut self, kind: OperationKind) -> CancellationToken {
        debug_assert!(!self.is_running());
        let cancellation = CancellationToken::new();
        *self = Self::Running {
            kind,
            cancellation: cancellation.clone(),
        };
        cancellation
    }

    fn finish(&mut self) {
        *self = Self::Idle;
    }

    fn cancel(&mut self) -> bool {
        let Some(cancellation) = self.cancellation() else {
            return false;
        };
        cancellation.cancel();
        true
    }
}

#[derive(Default)]
struct FeedbackState {
    error: Option<SharedString>,
    notice: Option<SharedString>,
}

#[derive(Default)]
struct WorkspaceSelection {
    selected_row: Option<usize>,
    selected_rows: RowSelection,
    selection_anchor: Option<usize>,
    cell_selection: Option<CellSelection>,
    cell_dragging: bool,
}

#[derive(Default)]
struct SearchState {
    column: Option<usize>,
    match_case: bool,
    whole_word: bool,
    results: Vec<usize>,
    current_match: usize,
    last_query: Option<String>,
    request_id: u64,
    refresh_token: u64,
    stale: bool,
    completed: bool,
}

#[derive(Default)]
struct DuplicateState {
    column: Option<usize>,
    current_match: usize,
    results: Vec<usize>,
    request_id: u64,
    stale: bool,
    completed: bool,
}

#[derive(Default)]
struct QueryState {
    show_find: bool,
    show_duplicates: bool,
    active_highlight: Option<ActiveHighlight>,
    search: SearchState,
    duplicates: DuplicateState,
}

struct InputEntities {
    search_input: gpui::Entity<InputState>,
    custom_delimiter_input: gpui::Entity<InputState>,
    custom_quote_input: gpui::Entity<InputState>,
    custom_escape_input: gpui::Entity<InputState>,
    custom_comment_input: gpui::Entity<InputState>,
    edit_input: gpui::Entity<InputState>,
}

#[derive(Default)]
struct EditorState {
    editing_cell: Option<EditingCell>,
    editing_draft_dirty: bool,
    pending_cell_commits: usize,
    cell_commit_in_flight: bool,
    cell_commit_queue: VecDeque<CellCommit>,
    pending_edit_action: Option<PendingEditAction>,
}

struct TableState {
    context_cell: Option<(usize, usize, usize, String)>,
    table_context_menu: Option<TableContextMenu>,
    resizing_column: Option<ColumnResize>,
    row_cache: HashMap<usize, CachedRow>,
    desired_row_range: Option<std::ops::Range<usize>>,
    row_request_in_flight: Option<(u64, u64)>,
    failed_row_range: Option<(u64, std::ops::Range<usize>)>,
    next_row_request_id: u64,
    document_generation: u64,
    row_scroll: UniformListScrollHandle,
    column_scroll: ScrollHandle,
    column_layout: Option<ColumnLayout>,
}

impl Default for TableState {
    fn default() -> Self {
        Self {
            context_cell: None,
            table_context_menu: None,
            resizing_column: None,
            row_cache: HashMap::new(),
            desired_row_range: None,
            row_request_in_flight: None,
            failed_row_range: None,
            next_row_request_id: 0,
            document_generation: 0,
            row_scroll: UniformListScrollHandle::new(),
            column_scroll: ScrollHandle::new(),
            column_layout: None,
        }
    }
}

#[derive(Default)]
enum Modal {
    #[default]
    None,
    Settings,
    HeaderPrompt,
    Shortcuts,
    About,
    Destructive(PendingDestructiveAction),
    Bulk(PendingBulkAction),
    ExternalChange,
}

impl Modal {
    fn is_active(&self) -> bool {
        !matches!(self, Self::None)
    }
}

#[derive(Default)]
struct OverlayState {
    modal: Modal,
}

struct PreferencesState {
    settings: AppSettings,
    settings_store: SettingsStore,
    show_warning_details: bool,
}

#[derive(Default)]
struct RuntimeState {
    self_weak: Option<WeakEntity<QuickRowsView>>,
    window_handle: Option<gpui::AnyWindowHandle>,
    operation_generation: u64,
}

struct QuickRowsView {
    focus_handle: FocusHandle,
    document: DocumentState,
    operation: ForegroundOperation,
    feedback: FeedbackState,
    selection: WorkspaceSelection,
    queries: QueryState,
    inputs: InputEntities,
    editor: EditorState,
    table: TableState,
    overlay: OverlayState,
    preferences: PreferencesState,
    runtime: RuntimeState,
}

type SelectedRowRanges = Vec<std::ops::RangeInclusive<usize>>;
type CellSelectionBounds = (usize, usize, usize, usize);

fn fragment_regions_to_selection(
    regions: &[ResolvedFragmentRegion],
    row_count: usize,
    column_count: usize,
    has_headers: bool,
) -> (SelectedRowRanges, Option<CellSelectionBounds>) {
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

fn load_settings_for_window(settings_store: &SettingsStore) -> (AppSettings, Option<SharedString>) {
    match settings_store.load() {
        Ok(settings) => (settings, None),
        Err(error) => (
            AppSettings::default(),
            Some(format!("Unable to load settings: {error}").into()),
        ),
    }
}
