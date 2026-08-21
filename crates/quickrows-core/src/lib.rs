//! Shared CSV parsing, caching, search, and document services for QuickRows.
//!
//! This crate deliberately has no dependency on a desktop UI framework. The
//! native GPUI shell uses the engine without depending on UI concerns.

pub mod cache;
pub mod csv;
pub mod diagnostics;
pub mod disk_cache;
pub mod document;
pub mod fragment;
pub mod mmap;
pub mod ops;
pub mod settings;

pub use cache::CsvCache;
pub use csv::{
    apply_parse_overrides, build_reader, build_row_offsets, build_row_offsets_mmap, decode_record,
    default_parse_settings, detect_parse_settings, detect_parse_settings_for_encoding,
    find_duplicates_hashed, find_duplicates_hashed_mmap, get_headers, parse_info_from_settings,
    prepare_csv_source, prepare_csv_source_cancellable, read_chunk, read_chunk_mmap,
    read_chunk_with_offsets, read_chunk_with_offsets_mmap, read_rows_by_index,
    read_rows_by_index_mmap, search_range_with_offsets, search_range_with_offsets_mmap,
    settings_cache_hash, validate_parse_overrides, validate_parse_settings, DetectedSettings,
    MalformedMode, ParseInfo, ParseOverrides, ParseSettings, ParseWarning, PreparedCsvSource,
    PreservedComment, MAX_WARNING_COUNT,
};
pub use diagnostics::Diagnostics;
pub use disk_cache::FileFingerprint;
pub use document::{
    BuiltSearchIndex, CancellationToken, CsvDocument, CsvMetadata, DocumentEdits, SearchIndexBuild,
    SortDirection, SortSpec,
};
pub use fragment::{
    CsvFragment, FragmentCellSpan, FragmentPosition, FragmentSpan, ResolvedFragmentRegion,
};
pub use settings::{AppSettings, RowDensity, SettingsStore, ThemePreference};
