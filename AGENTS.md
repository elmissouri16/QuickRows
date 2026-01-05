# Repository Guidelines

## Project Structure & Module Organization
- `src/`: React + TypeScript frontend (`App.tsx`, `App.css`, `hooks/`, `assets/`).
- `public/`: static assets served by Vite.
- `src-tauri/`: Rust backend and Tauri configuration.
  - `src-tauri/src/lib.rs`: Tauri commands and menu wiring.
  - `src-tauri/src/csv_*.rs`: CSV parsing, caching, and search utilities.
- Reference architecture/performance notes live in `instructions.md`.

## Build, Test, and Development Commands
- `npm run dev`: start the Vite dev server (frontend only).
- `npm run build`: type-check (`tsc`) and build the frontend bundle.
- `npm run preview`: serve the production build locally.
- `npm run tauri dev`: run the full desktop app with Tauri (frontend + Rust).
- `npm run tauri build`: package the Tauri desktop app.

## Coding Style & Naming Conventions
- Indentation: 2 spaces for `.ts/.tsx`, 4 spaces for `.css` and `.rs` (match existing files).
- React components use `PascalCase`; hooks use `useX` (e.g., `useDebounce` in `src/hooks/`).
- CSS class names use kebab-case (e.g., `.table-row`, `.find-panel`).
- No formatter/linter scripts are configured; keep edits consistent with adjacent code.

## Testing Guidelines
- No automated test runner is configured.
- Validate changes manually:
  - `npm run tauri dev` for full app behavior.
  - `npm run dev` for frontend-only layout checks.

## Commit & Pull Request Guidelines
- Git history only shows an `init` commit; no enforced convention observed.
- Use short, imperative commit subjects (e.g., “Add row-number toggle”).
- PRs should include:
  - A concise summary of changes.
  - Screenshots/GIFs for UI updates.
  - Linked issues if applicable.

## Configuration & Tips
- Settings persistence uses `localStorage` in the frontend.
- CSV-heavy operations are expected to run in Rust (see `src-tauri/src/`).
