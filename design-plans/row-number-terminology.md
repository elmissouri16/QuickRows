# Call CSV record indices row numbers

Written against: 4a5aaee047734715cced1259f1fc01aafe8718cd (with pre-existing working-tree changes)

## Evidence chain

- Surface: Native GPUI View menu and Settings > View section for the optional table index column.
- Problem: Both controls call the index column “line numbers,” but the table displays CSV record/row ordinals rather than physical file-line positions.
- Design evidence: `README.md:138-159` explicitly documents persisted `row-number visibility` and a native shell with configurable `row numbers`. The renderer uses `source_row + 1` or `display_row + 1` at `crates/quickrows-gpui/src/main.rs:4647-4654`. Settings and the View menu use `Show Line Numbers` at `crates/quickrows-gpui/src/main.rs:3957` and `crates/quickrows-gpui/src/main.rs:6088`. The application supports CSV fields with embedded newlines, so a record ordinal is not reliably a physical line number.
- Owner: User-facing labels in `QuickRowsView::render_settings` and native menu construction in `crates/quickrows-gpui/src/main.rs`.
- Scope and affected surfaces: Settings > View toggle and View menu action only.
- Uncertainty: None. The governing README and table implementation both establish `row numbers` as the correct term.

## Design decision

Rename the two user-facing controls from `Show Line Numbers` to `Show Row Numbers`. Keep the underlying `show_index` setting and `ToggleIndex` action unchanged because this is a terminology correction, not a persistence or behavior migration.

## Reuse

- Reuse the documented term `row numbers` from `README.md`.
- Reuse `settings.show_index`, `ToggleIndex`, the existing switch composition, key binding, and `#` table header.
- Exemplar: Row terminology already used by selection, copy, delete, restore, density, and status copy throughout `crates/quickrows-gpui/src/main.rs`, including the adjacent View submenu name `Row Height`.

## Changes

1. `crates/quickrows-gpui/src/main.rs` — `QuickRowsView::render_settings`
   - Change: Replace the Settings > View row label `Show Line Numbers` with `Show Row Numbers`.
   - Preserve: Switch ID, visual styling, `show_index` state, `ToggleIndex` click handler, settings persistence, and immediate table update.
   - Verify: Settings presents the persisted table-record index using the documented product term.

2. `crates/quickrows-gpui/src/main.rs` — native View menu construction
   - Change: Replace the menu action label `Show Line Numbers` with `Show Row Numbers`.
   - Preserve: `ToggleIndex`, its shortcut, platform menu behavior, and the Row Height submenu.
   - Verify: Menu and Settings use identical terminology for the same action.

3. `README.md`
   - Change: None expected; it already contains the governing `row-number visibility` and `row numbers` terminology.
   - Preserve: Existing settings and architecture documentation.
   - Verify: No new “line number” language is introduced for this table feature.

## Scope

- Inherit: All platforms using the shared native menu/settings labels.
- Verify: macOS application menu, Windows/Linux in-window `AppMenuBar`, Settings overlay, persisted toggle state, and the `Ctrl/⌘+I` action.
- Exclude: Internal identifier renames (`show_index`, `ToggleIndex`, `settings-index-switch`, `row-index`), CSV parser line-location diagnostics, warning copy that correctly refers to physical lines, and table-header redesign.

## Validation

- Product: Open a CSV, toggle Show Row Numbers from Settings and from the View menu, and confirm the same `#` column appears/disappears and the preference survives restart.
- Interface: Confirm the exact label `Show Row Numbers` appears in both Settings and the View menu without clipping at the 640×420 minimum window, on macOS and one in-window-menu platform when available.
- System: Search the native GPUI source for `Show Line Numbers`; there should be no remaining table-index control using that term. Confirm physical-line diagnostics such as parse warning locations remain unchanged.
- Repository: `cargo check -p quickrows-gpui` → the native application type-checks with unchanged toggle behavior.

## Stop conditions

- Stop if current product documentation or a platform-specific menu contract is changed to define the displayed ordinal as a physical line number; reconcile that decision before changing labels.

## Design documentation

- After acceptance and validation: None. `README.md` already records the accepted `row numbers` terminology.
