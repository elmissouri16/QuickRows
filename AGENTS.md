# Repository Guidelines

## Project Structure & Module Organization
- `crates/quickrows-core/`: UI-independent CSV engine, settings, and tests.
- `crates/quickrows-gpui/`: native GPUI + GPUI Component application.
- `assets/`: desktop application icons and source artwork.
- `docs/`: CSV compatibility and release-validation documentation.
- `scripts/`: packaging and large-fixture helpers.
- `packager.toml`: native package and CSV file-association configuration.

## Build, Test, and Development Commands
- `cargo run -p quickrows-gpui`: run the native GPUI app.
- `cargo check --workspace`: type-check the complete workspace.
- `cargo test --workspace`: run the shared engine and native state tests.
- `cargo fmt --all -- --check`: verify Rust formatting.
- `cargo packager --release --config packager.toml`: package the native app.

## Coding Style & Naming Conventions
- Use four-space indentation for Rust and match adjacent code for configuration files.
- Use `snake_case` for Rust functions/modules and `PascalCase` for types and GPUI views.
- Use GPUI Component primitives and semantic theme roles instead of custom control colors.
- Keep CSV parsing, search, sort, editing, and save behavior in `quickrows-core`.

## Testing Guidelines
- Add focused unit tests for CSV and state-model changes.
- Run `cargo test --workspace` and `cargo check --workspace` before submitting changes.
- For UI changes, run `cargo run -p quickrows-gpui` and inspect the affected surface manually.
- For large-file changes, generate the million-row fixture and follow `test-data/README.md`.

## Commit & Pull Request Guidelines
- Use short, imperative commit subjects.
- PRs should include:
  - A concise summary of changes.
  - Validation commands and results.
  - Screenshots/GIFs for UI updates.
  - Linked issues if applicable.

## Configuration & Tips
- Settings use native JSON persistence through `quickrows-core`.
- Packaging and file associations are owned by `packager.toml`.
- Public releases must complete `docs/release-checklist.md`.
