---
status: implemented
written_against: historical
implemented_by: historical
updated: 2026-08-24
---

# Replace cyclic query scopes with column pickers

Written against: 9135d8d1004b31ead09c6c97757361b5e53c7fe6 (with pre-existing working-tree changes)

## Evidence chain

- Surface: Native GPUI CSV workspace with either the Find or Duplicates strip open; the user-provided dark-theme Duplicates screenshot shows the current `category` scope control.
- Problem: The scope control presents the current value as an ordinary button and advances through every column one click at a time. The rendered control does not communicate that more values exist, and selecting a distant column requires repeatedly cycling through unrelated values. The user explicitly requires a picker and asks that every equivalent cyclic control be corrected.
- Design evidence: `crates/quickrows-gpui/src/main.rs:1919-1926` and `2114-2124` implement `cycle_search_column` and `cycle_duplicate_column` with `next_column_scope`. Their rendered `search-scope` and `duplicate-scope` buttons call those methods at `crates/quickrows-gpui/src/main.rs:5272-5280` and `5404-5412`. A surface-local audit found these are the only `cycle_*` controls. Multi-value settings already use GPUI Component's `Button::dropdown_menu` and checked `PopupMenuItem` choices through `settings_dropdown` at `crates/quickrows-gpui/src/main.rs:6009-6032`. Root `DESIGN.md` requires compact GPUI Component controls and classifies query scope as a ghost utility; a dropdown trigger can preserve that hierarchy.
- Owner: `QuickRowsView` query scope state and lifecycle methods plus the Find/Duplicates strip composition in `crates/quickrows-gpui/src/main.rs`.
- Scope and affected surfaces: The Find scope selector and Duplicates scope selector, including entire-row and individual-column choices, loading-disabled state, long or blank header labels, and stale/current query results.
- Uncertainty: Dynamic popup behavior with unusually high column counts and very long labels requires runtime validation, but the selection model and existing dropdown primitive are unambiguous.

## Design decision

Replace both cyclic scope buttons with compact ghost dropdown-menu triggers. Each trigger displays the current scope followed by a down chevron and opens a direct picker containing `Entire row` followed by every document column in source order. The active choice is checked. Selecting a choice updates the corresponding existing `Option<usize>` state directly and invokes the same search refresh or duplicate-result invalidation behavior currently owned by the cycle methods.

Keep match-case and whole-word as default-variant toggle buttons because they are binary states. Keep Find, Check, navigation, clear, close, and re-run controls as actions. No other workspace control should be converted: the audit found no other multi-value button that cycles values, and the Settings surface already uses dropdowns for multi-value choices and switches for booleans.

## Reuse

- GPUI Component `Button`, `.compact()`, `.ghost()`, `.disabled()`, and `.dropdown_menu()`.
- GPUI Component `PopupMenuItem::new(...).checked(...).on_click(...)`.
- Existing `settings_dropdown` at `crates/quickrows-gpui/src/main.rs:6009-6032` as the menu-construction and checked-choice exemplar; do not route query state through `SettingsChoice`.
- Existing `header_labels`, `display_header_label`, and `column_scope_label` owners so blank headers continue to display as `Column N` and current labels remain consistent with table headers.
- Existing query lifecycle behavior in `schedule_search`, `mark_results_stale`, and the duplicate scope mutation path.
- Existing `workspace-control-hierarchy.md` decisions remain in force: the dropdown triggers retain compact ghost geometry, while execution and binary-toggle variants remain unchanged.

No new general-purpose picker component is required. A small query-scope menu composition helper is acceptable only if both strips can share it without introducing a second state owner or coupling query choices to settings persistence.

## Changes

1. `crates/quickrows-gpui/src/main.rs` — direct query-scope state transitions
   - Change: Replace `cycle_search_column` with a direct setter that accepts `Option<usize>`, returns without work when the selection is unchanged, assigns `search_column`, and calls `schedule_search(cx)` when it changes.
   - Change: Replace `cycle_duplicate_column` with a direct setter that accepts `Option<usize>`, returns without work when unchanged, assigns `duplicate_column`, marks existing duplicate results stale, clears `duplicate_check_has_completed`, and notifies the view exactly as the current cycle path does.
   - Change: Remove `next_column_scope` after its only two consumers are gone.
   - Preserve: `None` means `Entire row`; `Some(index)` means that source column. Preserve request IDs, cancellation, streamed results, loading guards, active highlights, and the distinction between stale results and cleared results.
   - Verify: Choosing any scope reaches it in one selection; choosing the already-active item does not schedule or invalidate work again.

2. `crates/quickrows-gpui/src/main.rs` — Find scope picker
   - Change: Replace the `search-scope` click-to-cycle handler with a compact ghost dropdown trigger labeled `<current scope>  ▾`. Populate it with checked `PopupMenuItem` entries for `Entire row` and every `header_labels` entry, each calling the direct search-scope setter with its exact `Option<usize>` value.
   - Preserve: Control ID, placement before the search input, compact geometry, ghost hierarchy, loading-disabled rule, flexible strip layout, header fallback labels, and automatic search scheduling after a genuine change.
   - Verify: The menu opens from the Find strip, visibly identifies the selected scope, supports direct first/middle/last-column selection, and updates the trigger label after selection.

3. `crates/quickrows-gpui/src/main.rs` — Duplicates scope picker
   - Change: Replace the `duplicate-scope` click-to-cycle handler with the same compact ghost dropdown pattern and the same ordered scope choices, wired to the direct duplicate-scope setter.
   - Preserve: Control ID, placement before Check, compact geometry, ghost hierarchy, loading-disabled rule, stale-result behavior, completion-state behavior, and explicit Check execution.
   - Verify: Selecting a new column updates the label immediately, marks prior duplicate results `Outdated` when results exist, and does not automatically run a duplicate check.

4. `crates/quickrows-gpui/src/main.rs` — shared query-scope menu composition, if needed
   - Change: If avoiding duplicate menu construction requires a helper, keep it local to the query-strip UI and parameterize the target query kind, current `Option<usize>`, current label, `header_labels`, disabled state, and weak view reference. Build dynamic menu entries from owned/cloned labels and captured indices.
   - Preserve: Find and Duplicates retain separate state setters and lifecycle semantics; the helper must only compose controls and dispatch selections.
   - Verify: There is one option-generation rule, but no new shared query state, custom popup implementation, or settings dependency.

5. `crates/quickrows-gpui/src/state_tests.rs`
   - Change: Add focused coverage for direct selection of `None`, the first column, and a non-adjacent later column for both query kinds. Assert that search selection schedules refresh only on change and duplicate selection preserves the existing stale/completion transitions.
   - Change: If option generation is extracted as a pure helper, test ordering, checked state, and blank-header fallback without creating a test-only menu model.
   - Preserve: Existing query completion, loading navigation, and rendering-helper tests.
   - Verify: Tests would fail if selection reverts to sequential cycling, if `Entire row` disappears, or if the two scope owners acquire inconsistent option ordering.

6. `DESIGN.md`
   - Change: After runtime acceptance, clarify the workspace-controls section: multi-value query scope uses a compact ghost dropdown picker, while match-case and whole-word remain compact default binary toggles.
   - Preserve: Existing semantic palette, compact geometry, and primary/ghost/default hierarchy.
   - Verify: Documentation distinguishes multi-value selection from binary state and action execution without prescribing a parallel component system.

## Scope

- Inherit: Light, dark, and system themes; narrow and wide workspace layouts; any CSV header count because both controls consume the loaded document's shared header labels.
- Verify: Find and Duplicates with no results, completed results, stale results, loading, both strips open simultaneously, blank headers, duplicate display labels, long labels, and first/middle/last-column choices.
- Exclude: Search and duplicate algorithms, result copy, query execution timing, match-case and whole-word toggles, parsing/settings dropdowns, toolbar actions, table-header sorting, modal decisions, and a repository-wide replacement of ordinary buttons.

## Validation

- Product: Open `test-data/million-rows.csv`. In Find, open the scope picker and directly choose `Entire row`, `id`, a middle column such as `category`, and the last column; confirm each search uses the selected scope. In Duplicates, repeat those direct selections, run Check, change scope, and confirm existing results become `Outdated` until explicitly re-run.
- Interface: Inspect both menus in light and dark themes at the 640×420 minimum, 800×600 default, and a wide viewport. Verify current-choice checkmarks, chevrons, disabled behavior during operations, menu dismissal, long labels, and strip wrapping with both panels open.
- System: Confirm `grep` finds no remaining `cycle_search_column`, `cycle_duplicate_column`, or `next_column_scope`; confirm all multi-value query scopes use GPUI Component dropdown menus, all binary query options remain toggles, and no custom menu/palette primitive was introduced.
- Repository: `cargo fmt --all -- --check && cargo test -p quickrows-gpui --bin quickrows && cargo check --workspace --all-targets --locked` → formatting, native state tests, and all targets pass.

## Stop conditions

- Stop if GPUI Component's existing popup menu cannot present or navigate the loaded document's dynamic column list, or if it cannot keep the active item checked. Investigate the installed component's supported menu/scroll APIs rather than restoring click-to-cycle behavior or inventing an unbounded custom popup.
- Stop if selecting a scope cannot reuse the current search scheduling and duplicate invalidation paths without changing query semantics; resolve lifecycle ownership before altering result behavior.

## Design documentation

- After acceptance and validation: Update root `DESIGN.md` with the distinction specified in Change 6. No other design documentation changes are required.
