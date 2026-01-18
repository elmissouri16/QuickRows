# QuickRows

QuickRows is a fast, desktop CSV viewer built with Tauri (Rust) and React.
It is designed to stay responsive with very large files by pushing heavy work
into the Rust backend and rendering rows virtually in the UI.
This is a vibe-coded project tailored to my personal workflow and needs.

## Features
- Open large CSVs without loading everything into memory.
- Virtualized table rendering for smooth scrolling.
- Optional in-memory search indexing for faster queries.
- Sorting, duplicate checks, and search driven by Rust operations.
- Parse overrides for delimiter, quote, escape, encoding, line endings, headers.
- Settings persistence via `localStorage`.

## Usage
- Open the app and choose **File > Open** or drop a CSV file.
- Launch with a file path (file association or CLI).
  - Linux packages install `quickrows`:
    - `quickrows`
    - `quickrows /path/to/file.csv`

## How It Works
- **Rust backend** handles CSV parsing, row offset indexing, sorting, and search.
  - Large files can be memory-mapped to reduce memory pressure.
  - Row offsets are cached on disk for faster re-open.
  - Optional inverted search index is built in a background thread and stored in memory.
- **React frontend** uses `@tanstack/react-virtual` to render only visible rows.
- **Tauri bridge** provides commands for chunked reads, search, and metadata.

## Project Layout
- `src/`: React + TypeScript frontend (`App.tsx`, `App.css`, `hooks/`, `assets/`).
- `public/`: static assets served by Vite.
- `src-tauri/`: Rust backend and Tauri configuration.
  - `src-tauri/src/lib.rs`: Tauri commands and menu wiring.
  - `src-tauri/src/csv_*.rs`: CSV parsing, caching, and search utilities.

## Development
- `npm run dev`: start the Vite dev server (frontend only).
- `npm run build`: type-check and build the frontend bundle.
- `npm run preview`: serve the production build locally.
- `npm run tauri dev`: run the full desktop app with Tauri.
- `npm run tauri build`: package the desktop app.

## Diagnostics (Logs & Crashes)
- Enable **Settings → Diagnostics → Enable Debug Logging** to write a log file you can attach to issues.
- Use the buttons in the same section to open/copy the log path.
- Crash/panic info is written to a separate crash log file (also linked in **Settings → Diagnostics**).

## License
MIT
