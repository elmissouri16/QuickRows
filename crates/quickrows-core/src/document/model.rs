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

    pub fn check(&self) -> QuickRowsResult<()> {
        if self.is_cancelled() {
            Err(QuickRowsError::cancelled())
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

    fn edited_cell(&self, source_row: usize, column: usize) -> Option<&str> {
        self.cells
            .get(&source_row)
            .and_then(|edits| edits.get(&column))
            .map(String::as_str)
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
    generation: u64,
    revision: u64,
}
