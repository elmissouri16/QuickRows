# Architecture

QuickRows is a two-crate Rust workspace. The crate boundary separates the CSV engine from the native GPUI application; new crates should be introduced only when they provide an independently useful ownership boundary.

## `quickrows-core`

`quickrows-core` owns UI-independent behavior:

- CSV dialect detection, validation, canonicalization, and indexed reads.
- Immutable source snapshots, fingerprints, and persistent caches.
- Document edits, deletion, search, duplicate detection, sorting, export, and save.
- Versioned settings persistence and diagnostics services shared by the application.

`CsvDocument` is the primary application-facing façade. `csv/` groups dialect, canonicalization, record, offset, read, and query implementation; `document/` groups model, open, row, query, output, and atomic-save behavior. Low-level storage modules support the façade and remain crate-private unless the UI has an intentional top-level re-export.

Fallible public operations return `QuickRowsResult<T>`. UI and integration code should branch on `ErrorKind` rather than parsing `QuickRowsError` display text. Display messages remain suitable for user-facing error details.

## `quickrows-gpui`

`quickrows-gpui` owns:

- Application startup, keybindings, native paths, and single-instance coordination.
- Window/workspace state and asynchronous operation coordination.
- Virtualized table presentation, selection, editing, overlays, and settings surfaces.
- Mapping typed core outcomes to user-facing messages.

Blocking core operations run on GPUI's background executor. Results are applied on the foreground executor and guarded by document generations and request identifiers so stale work cannot replace current state.

## Dependency direction

```text
quickrows-gpui  --->  quickrows-core
```

The core crate must not depend on GPUI or presentation types. CSV validation and matching semantics belong in core; labels, colors, layout, and input entities belong in GPUI.

## Source organization

Large façade types may span focused implementation files, but state ownership remains explicit. Files are grouped by behavior rather than by an arbitrary line limit. Tests that need private access live in dedicated test files beside the owning module; public compatibility scenarios live under `tests/`.
