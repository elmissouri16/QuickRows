//! Shared CSV parsing, caching, search, and document services for QuickRows.
//!
//! This crate deliberately has no dependency on a desktop UI framework. The
//! native GPUI shell uses the engine without depending on UI concerns.

mod cache;
pub mod csv;
mod diagnostics;
pub mod disk_cache;
mod document;
mod error;
mod fragment;
mod mmap;
mod settings;
mod source_file;
mod storage;

pub use csv::{
    DetectedSettings, MAX_WARNING_COUNT, MalformedMode, ParseInfo, ParseOverrides, ParseSettings,
    ParseWarning, PreparedCsvSource, PreservedComment, apply_parse_overrides, build_reader,
    build_row_offsets, build_row_offsets_mmap, decode_record, default_parse_settings,
    detect_parse_settings, detect_parse_settings_for_encoding, find_duplicates_hashed,
    find_duplicates_hashed_mmap, get_headers, parse_info_from_settings, prepare_csv_source,
    prepare_csv_source_cancellable, read_chunk, read_chunk_mmap, read_chunk_with_offsets,
    read_chunk_with_offsets_mmap, read_rows_by_index, read_rows_by_index_mmap,
    search_range_with_offsets, search_range_with_offsets_mmap, settings_cache_hash,
    validate_parse_overrides, validate_parse_overrides_for_info, validate_parse_settings,
};
pub use diagnostics::Diagnostics;
pub use disk_cache::{FileFingerprint, file_fingerprint};
pub use document::{
    BuiltSearchIndex, CancellationToken, CsvDocument, CsvMetadata, DocumentEdits, SearchIndexBuild,
    SortDirection, SortSpec,
};
pub use error::{ErrorKind, QuickRowsError, QuickRowsResult};
pub use fragment::{
    CsvFragment, FragmentCellSpan, FragmentPosition, FragmentSpan, ResolvedFragmentRegion,
};
pub use settings::{AppSettings, RowDensity, SettingsStore, ThemePreference};
