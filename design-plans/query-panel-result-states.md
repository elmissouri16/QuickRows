# Show query outcomes only after they are established

Written against: 4a5aaee047734715cced1259f1fc01aafe8718cd (with pre-existing working-tree changes)

## Evidence chain

- Surface: Native GPUI CSV workspace, with a document open and either the Find or Duplicates panel visible.
- Problem: Both panels render a conclusive empty-result message before their operation has run. They also render that message after an operation clears the old results and while the replacement operation is still running.
- Design evidence: `crates/quickrows-gpui/src/main.rs:1013-1018` initializes both result collections as empty for a newly opened file. `show_find` and `check_duplicates` only reveal their panels at `crates/quickrows-gpui/src/main.rs:1526-1532` and `crates/quickrows-gpui/src/main.rs:1748-1759`. The renderer currently maps an empty collection directly to `No matches` and `No duplicates` at `crates/quickrows-gpui/src/main.rs:5071-5079` and `crates/quickrows-gpui/src/main.rs:5153-5161`. Search and duplicate execution clear those collections before completion at `crates/quickrows-gpui/src/main.rs:1628-1631` and `crates/quickrows-gpui/src/main.rs:1785-1788`.
- Owner: `QuickRowsView` query state, query lifecycle methods, and `QuickRowsView::render_table` in `crates/quickrows-gpui/src/main.rs`.
- Scope and affected surfaces: Find result status, Duplicates result status, and every lifecycle path that resets or invalidates those results: initialization, open, reload, clear file, save/reload of document metadata, query input/scope changes, clear actions, document mutation, operation start, success, cancellation, and failure.
- Uncertainty: None. The correction can omit an unearned empty-result label without inventing replacement copy; existing buttons and notices already communicate running operations.

## Design decision

Track whether each query has completed successfully for the current query parameters and document state. Continue showing a position such as `1 of 8` whenever actual results exist, including streamed partial results. Show `No matches` or `No duplicates` only when the corresponding operation completed successfully with zero results. Render no result-outcome text while the panel is idle, cleared, invalidated, cancelled, failed, or running with zero results.

This separates “no outcome yet” from the legitimate zero-result outcome while preserving the existing streamed-result behavior and copy.

## Reuse

- Reuse `OperationKind::{Search, Duplicates}`, `search_results`, `duplicate_results`, `search_stale`, and `duplicate_stale`; do not introduce a parallel operation-progress system.
- Reuse the existing `No matches`, `No duplicates`, and `{current} of {total}` copy in `QuickRowsView::render_table`.
- Exemplar: Existing query lifecycle ownership in `run_search`, `run_duplicate_check`, `clear_search`, `clear_duplicates`, `schedule_search`, and `mark_results_stale` in `crates/quickrows-gpui/src/main.rs`.
- A small pure result-label helper may be added near the existing text helpers at the bottom of `main.rs` because both panels need the same three-way rule: nonempty results produce a position, successfully completed empty results produce the supplied empty label, and uncompleted empty results produce no label. It must not own operation state.

## Changes

1. `crates/quickrows-gpui/src/main.rs` — `QuickRowsView` query state and initialization
   - Change: Add one completion-validity flag for search and one for duplicate checking (for example, `search_has_completed` and `duplicate_check_has_completed`). Initialize both to `false`.
   - Preserve: Existing result vectors, current-result indices, request IDs, cancellation tokens, stale flags, active highlighting, and streamed progress.
   - Verify: A fresh view and a newly opened document cannot represent an empty collection as a completed result.

2. `crates/quickrows-gpui/src/main.rs` — query lifecycle methods
   - Change: Set the corresponding completion flag to `false` whenever its outcome is not current or established: new document/open/reload/clear/save resets, search input or option scheduling, duplicate scope changes, explicit clear actions, document-result invalidation, blank search submission, operation start, cancellation, and failure. Set it to `true` only after a successful request that is still current; for search, do not mark the superseded result complete when the input changed during execution.
   - Preserve: Closing and reopening a panel without changing the document or query must preserve a valid completed result. Nonempty stale results must continue to display with the existing `Outdated` treatment until re-run or cleared.
   - Verify: Idle, running, cancelled, failed, cleared, and invalidated zero-result states remain neutral; only a successful zero-result completion enables the negative outcome label.

3. `crates/quickrows-gpui/src/main.rs` — Find and Duplicates result rendering
   - Change: Replace the unconditional empty-vector branches with the shared rule: show `{current} of {total}` when results are nonempty; show the existing negative label only when the relevant completion flag is true; otherwise omit the result label.
   - Preserve: Button order, panel color treatment, `Outdated` controls, Previous/Next disabled rules, and all existing result copy.
   - Verify: Opening Find shows no `No matches`; opening Duplicates shows no `No duplicates`; running an empty duplicate check never shows `Checking…` beside `No duplicates`; a completed empty operation shows the correct existing negative label.

4. `crates/quickrows-gpui/src/state_tests.rs`
   - Change: If a pure result-label helper is introduced, add focused tests for idle-empty, running/uncompleted-empty, completed-empty, and nonempty states. If no helper is introduced, keep state-transition coverage manual rather than creating a second test-only state model.
   - Preserve: Existing URL, context-menu, and fingerprint tests.
   - Verify: Tests prove the deterministic label-selection rule without requiring a rendered GPUI window.

## Scope

- Inherit: Both Find and Duplicates panels because they share the same outcome-state contradiction.
- Verify: Fresh document, panel reopen with a valid prior result, zero and nonzero results, streamed nonzero progress, query text/options changes, duplicate scope changes, clear, cancel, failure, row edit/delete/restore invalidation, reload, and save pathways.
- Exclude: Search algorithms, duplicate algorithms, progress-notice wording, panel layout, highlighting colors, indexing behavior, and broader query-state refactors.

## Validation

- Product: Open a CSV, open Find without searching, and open Duplicates without checking; neither panel should claim an empty result. Run each operation against a known zero-result case; the correct negative label should appear only after successful completion. Repeat with known matches and confirm position navigation remains unchanged.
- Interface: Validate idle, running, completed-empty, completed-nonempty, cancelled, failed, stale, cleared, and close/reopen states at the 640×420 minimum window and the default 800×600 window, in both light and dark themes.
- System: Confirm result completion is represented once per query type and reuses existing operation, stale, result, and notice owners rather than adding a second progress mechanism.
- Repository: `cargo test -p quickrows-gpui && cargo check -p quickrows-gpui` → all native UI tests pass and the GPUI application type-checks.

## Stop conditions

- Stop if GPUI rendering cannot omit an optional child without changing panel composition, or if streamed query updates require a different definition of “completed”; resolve that ownership before adding new user-facing copy or a broader state machine.

## Design documentation

- After acceptance and validation: None. This corrects state-specific copy while preserving the query behavior already documented in `README.md`.
