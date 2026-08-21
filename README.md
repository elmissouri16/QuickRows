# QuickRows

QuickRows is a fast native desktop CSV viewer designed to remain responsive
with very large files. The application is built with
[GPUI](https://gpui.rs/) and
[GPUI Component](https://github.com/longbridge/gpui-component).

## Features

- Open large CSVs without loading the complete file into memory.
- Memory-mapped reads for large files and virtualized native row rendering.
- RFC 4180-style parsing plus real-world dialect support: automatic and custom
  Unicode delimiters, quote/escape/comment characters, Excel `sep=` directives,
  UTF-8 and UTF-16 BOM handling, legacy encodings, LF/CRLF/CR records, optional
  headers, strict/skip/repair policies, opt-in field/record limits, and RFC 7111
  row/column/cell fragments. See [`docs/csv-compatibility.md`](docs/csv-compatibility.md)
  for the tested compatibility matrix.
- Search, duplicate detection, and sorting in Rust.
- Cell-edit and row-delete document model with streaming Rust save support.
- Native file dialogs, clipboard integration, recent files, menus, and
  keyboard shortcuts.
- System, light, and dark theme preferences with native JSON persistence.
- macOS, Windows, X11 Linux, and Wayland Linux targets.

## Workspace

- `crates/quickrows-core/`: UI-independent CSV parsing, mmap, offset/cache,
  search, duplicate, sort, editing, saving, and settings services.
- `crates/quickrows-gpui/`: native GPUI application.
- `assets/`: desktop application icons and source artwork.
- `packager.toml`: cross-platform `cargo-packager` configuration and `.csv`
  file association.

GPUI and GPUI Component are pinned to the compatible published pair
`gpui = 0.2.2` and `gpui-component = 0.5.1`. GPUI is pre-1.0, so update these
versions together and validate all platforms when upgrading.

## Native GPUI Development

The project requires a recent stable Rust toolchain (edition 2024 support).

```sh
# Type-check the native app
cargo check -p quickrows-gpui

# Run core tests
cargo test -p quickrows-core

# Run the native app
cargo run -p quickrows-gpui

# Open a CSV directly
cargo run -p quickrows-gpui -- /path/to/file.csv
```

The direct GPUI dependency enables `runtime_shaders`. This permits macOS
development on machines where the optional Xcode Metal toolchain component is
not installed. Release environments may instead install it with:

```sh
xcodebuild -downloadComponent MetalToolchain
```

Linux needs the development packages required by GPUI's X11 and Wayland
backends. Windows uses Win32 and DirectWrite through GPUI.

### Million-row stress fixture

Generate the ignored deterministic fixture, then open it directly:

```sh
python3 scripts/generate_million_csv.py
cargo run -p quickrows-gpui -- test-data/million-rows.csv
```

The generator writes 1,000,000 CRLF records with duplicate groups, quoted
fields, and embedded newlines. See `test-data/README.md` for the stress
checklist. The matching ignored core stress test can be run with:

```sh
cargo test -p quickrows-core --test million_rows -- --ignored --nocapture
```

## Packaging

Install the packager and build the platform-default native package:

```sh
cargo install cargo-packager --locked
cargo packager --release --config packager.toml
```

On macOS, the release helper builds and verifies a DMG, installs
`QuickRows.app` in `/Applications`, and launches it:

```sh
scripts/macos-dmg.sh
```

Useful alternatives:

```sh
scripts/macos-dmg.sh --build-only          # only create the DMG
scripts/macos-dmg.sh --user --no-launch    # install in ~/Applications
scripts/macos-dmg.sh --install-dmg PATH    # install an existing DMG
scripts/macos-dmg.sh --build-only --signing-identity "Developer ID Application: …"
```

The signing option also enables cargo-packager notarization when its standard
Apple credential environment variables are present. A locally built
application may otherwise be unsigned. Public DMGs still require a
Developer ID signature and Apple notarization as described in
`docs/release-checklist.md`.

`packager.toml` builds `quickrows`, uses the existing application icons, and
registers QuickRows as a viewer for `.csv`/`text-csv` files. The manual/tagged
`Package smoke` workflow builds native artifacts on macOS, Windows, and Linux.
Installers are not cross-built; complete `docs/release-checklist.md`, including
signing/notarization and X11/Wayland checks, before publishing.

## Settings and Data

The GPUI application stores a versioned `settings.json` in the platform-native
QuickRows configuration directory. It persists recent files, row density,
column widths, theme preference, row-number visibility, indexing preference,
diagnostics preference, and parse overrides.

Settings are persisted natively after first use.

## Architecture

`quickrows-core::CsvDocument` owns parse settings, row offsets, optional mmap,
a bounded row cache, sort order, edits, and deleted rows. GPUI starts blocking
CSV operations on its background executor and applies results back to the UI
entity on the foreground executor. A separate bounded 1,024-row presentation
cache keeps file reads and document locking out of the virtual-list render
path; stale viewport requests are rejected by document generation and request
identity. Sorting changes only the display-to-source mapping.

The current GPUI shell includes native open/save/save-as, removable recent
files, virtualized rows, persistent offset/sort caches, configurable density and
row numbers, compact range-based row selection, complete keyboard extension,
spreadsheet-style rectangular cell selection, inline editing, explicit
multi-row delete/restore actions, accessible context menus, sort cycling,
streamed search and duplicate results, operation progress/cancellation, parse
override dropdowns, detected-versus-effective format details, parse-warning
inspection, diagnostics controls, runtime operating-system file-open handling,
and single-instance forwarding. Final installer/signing validation and broader
automated end-to-end scroll/platform interaction coverage remain before public
stable distribution.

## Diagnostics

The native app installs a panic hook before GPUI starts, writes crash details to
the platform application-data directory, and exposes debug logging, exact log
paths, copy/reveal actions, and log clearing from Settings. Initialization and
clear failures are shown in the UI. Logs are UTF-8-safe and capped before
appending.

## License

MIT
