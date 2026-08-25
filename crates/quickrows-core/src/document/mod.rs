use crate::cache::CsvCache;
use crate::csv::{
    MAX_WARNING_COUNT, ParseInfo, ParseOverrides, ParseSettings, ParseWarning, PreparedCsvSource,
    PreservedComment, apply_parse_overrides, build_row_offsets, build_row_offsets_cancellable,
    build_row_offsets_mmap, build_row_offsets_mmap_cancellable, detect_headers_for_settings,
    detect_parse_settings_for_encoding, get_headers, parse_info_from_settings,
    prepare_csv_source_cancellable, prepare_saved_csv_source_cancellable, read_chunk_with_offsets,
    read_chunk_with_offsets_mmap, read_column_range_with_offsets,
    read_column_range_with_offsets_mmap, read_rows_by_index, read_rows_by_index_mmap,
    search_range_with_offsets, search_range_with_offsets_mmap, settings_cache_hash,
    validate_parse_overrides, validate_parse_settings,
};
#[cfg(test)]
use crate::disk_cache::file_fingerprint;
use crate::disk_cache::{
    CacheKey, FileFingerprint, cache_key_from_fingerprint, ensure_cache_dir, offsets_cache_path,
    order_cache_path, prune_cache_dir, read_offsets_cache, read_order_cache, read_warnings_cache,
    warnings_cache_path, write_offsets_cache, write_order_cache, write_warnings_cache,
};
use crate::error::{ErrorKind, QuickRowsError, QuickRowsResult};
use crate::fragment::{CsvFragment, ResolvedFragmentRegion};
use crate::mmap::open_immutable_mmap_if_large;
use crate::source_snapshot::{
    OpenFileState, SourceSnapshot, capture_open_file_state, file_fingerprint_cancellable,
    snapshot_csv_source, verify_open_file_state, verify_path_references_open_file,
};
use memmap2::Mmap;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

// The UI keeps the visible range in its own cache. Retain only a small number
// of recently decoded chunks here so scrolling stays responsive without
// accumulating dozens of duplicate String grids.
const DEFAULT_CACHE_CHUNKS: usize = 8;
const SORT_CHUNK_SIZE: usize = 10_000;
const INDEX_CHUNK_SIZE: usize = 10_000;
const SAVE_IO_BUFFER_BYTES: usize = 1024 * 1024;
const INDEX_MAX_CARDINALITY: usize = 500_000;
const SORT_MERGE_CHUNK_SIZE: usize = 65_536;
const SORT_CANCELLATION_INTERVAL: usize = 4_096;

type OpenProgressCallback<'a> = dyn Fn(usize) + 'a;
type RowProgressCallback<'a> = dyn Fn(usize, usize) + 'a;
type QueryProgressCallback<'a> = dyn Fn(&[usize], usize, usize) + 'a;

/// Cancellation and progress always describe one foreground engine operation.
/// Keeping them together prevents internal helpers from accepting mismatched
/// tokens/callbacks as their signatures grow.
struct OperationContext<'a, P: ?Sized> {
    cancellation: Option<&'a CancellationToken>,
    progress: Option<&'a P>,
}

impl<'a, P: ?Sized> OperationContext<'a, P> {
    fn new(cancellation: Option<&'a CancellationToken>, progress: Option<&'a P>) -> Self {
        Self {
            cancellation,
            progress,
        }
    }

    fn check(&self) -> QuickRowsResult<()> {
        if let Some(cancellation) = self.cancellation {
            cancellation.check()?;
        }
        Ok(())
    }
}

type CancellationContext<'a> = OperationContext<'a, ()>;
type OpenOperationContext<'a> = OperationContext<'a, OpenProgressCallback<'a>>;
type RowOperationContext<'a> = OperationContext<'a, RowProgressCallback<'a>>;
type QueryOperationContext<'a> = OperationContext<'a, QueryProgressCallback<'a>>;

static NEXT_DOCUMENT_GENERATION: AtomicU64 = AtomicU64::new(1);

// `CsvDocument` remains one public façade; focused implementation files share
// its private state without widening internal visibility.
include!("index.rs");
include!("model.rs");
include!("open.rs");
include!("rows.rs");
include!("query.rs");
include!("output.rs");
include!("atomic_save.rs");

#[cfg(test)]
mod tests;
