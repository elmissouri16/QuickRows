# Reframe the document header as a quiet, grouped command bar

Written against: 4a5aaee047734715cced1259f1fc01aafe8718cd (with pre-existing working-tree changes)

## Evidence chain

- Surface: Native GPUI open-document workspace header in `QuickRowsView::render_table`, shown in the user-provided wide light-theme screenshot with `million-rows.csv` open.
- Problem: The header presents the Q brand mark, a two-line document identity, seven equally boxed actions, a bordered row-count pill, and Settings as one uninterrupted sequence. The user explicitly rejected this rendered header. At runtime, all content is owned by one wrapping `h_flex` at `crates/quickrows-gpui/src/main.rs:4882-5040`; action buttons are adjacent with only one-pixel gaps, so file, selection, destructive, and analysis commands have no visible grouping. The Q mark and `CSV workspace` subtitle consume height while the native title bar already names both the document and QuickRows.
- Design evidence: Root `DESIGN.md` requires compact GPUI Component buttons, semantic theme tokens, and restrained structural color. Microsoft Fluent 2 Toolbar guidance (`https://fluent2.microsoft.design/components/web/react/core/toolbar/usage`) says toolbar actions should be logically grouped with dividers or whitespace, destructive or work-status actions should be separated, toolbars should stay on one line, and icon-only responsive controls need familiar icons plus tooltips. Nielsen Norman Group’s “Aesthetic and Minimalist Design” guidance (`https://www.nngroup.com/articles/aesthetic-minimalist-design/`) says interfaces should retain necessary, high-information elements and prefer clarity over visual flourish. The installed `gpui-component 0.5.1` Button already supports `.compact()`, `.ghost()`, `.icon()`, `.label()`, and `.tooltip()`, and the current icon set contains every icon already used by the header.
- Owner: The document-toolbar composition in `crates/quickrows-gpui/src/main.rs`, GPUI Component `Button`, and the connected semantic `Theme` documented in root `DESIGN.md`.
- Scope and affected surfaces: Open-document header at wide and minimum supported window widths; enabled, disabled, dirty, loading, no-selection, and selected-row states; light, dark, and system themes.
- Uncertainty: None. The current 640 px minimum window width and the existing icon set determine the compact responsive branch; no new icon asset or component dependency is required.

## Design decision

Replace the undifferentiated wrapping header with one quiet, non-wrapping command bar that has four explicit zones:

1. **Document identity:** one-line filename only.
2. **File commands:** Open and Save.
3. **Selection commands:** Copy, Delete, and Restore.
4. **Analysis commands:** Find and Dupes.

Place a subtle semantic divider between the identity and each command group. Keep row count and Settings right-aligned as document metadata and utility controls. Remove the in-workspace Q tile and `CSV workspace`/`Modified` subtitle because the native title bar and bottom status bar already own that information. Remove the bordered row-count pill and render the formatted count as muted metadata so it does not compete with commands.

At widths below 900 px, keep the bar on one line by showing icon-only compact buttons for Open, Copy, Delete, Restore, Find, and Dupes, each with its full text in a tooltip. Keep the Save text label in both branches because the installed icon bundle has no conventional save icon and the existing check icon alone would be ambiguous. This preserves clarity without introducing an overflow implementation or allowing command groups to wrap unpredictably.

Use compact ghost buttons for routine command-bar actions so the data grid remains the visual focus. Save remains the compact default variant because it represents document state and is enabled only when work is dirty. Settings remains compact ghost and receives a `Settings` tooltip. Do not introduce custom action colors; Delete remains separated within the selection group and continues to use the existing confirmation behavior.

## Reuse

- `gpui_component::button::Button` with existing `.compact()`, `.ghost()`, `.icon()`, `.label()`, `.tooltip()`, `.disabled()`, and `.on_click()` APIs.
- Existing `IconName::FolderOpen`, `Check`, `Copy`, `Delete`, `Undo`, `Search`, `GalleryVerticalEnd`, and `Settings2` assets; do not add icons.
- Existing `cx.theme().background`, `secondary`, `border`, `foreground`, and `muted_foreground` roles from root `DESIGN.md`.
- Existing `format_count(row_count)` output for row metadata.
- Existing responsive input: `window.viewport_size().width`, already used by `render_table_context_menu` in `crates/quickrows-gpui/src/main.rs`.
- Geometry exemplar: the connected Find/Duplicates strips already use `min_h(px(46.0))`, `px_3()`, `py_1()`, and compact buttons. Reuse those bar metrics rather than inventing another toolbar height.
- Existing settings dropdown demonstrates GPUI Component menu ownership, but no new menu is needed for this responsive design.

No new toolbar component is required: this is one composition with a wide/narrow label decision. If the same grouping is later needed elsewhere, extract only after a second runtime consumer exists.

## Changes

1. `crates/quickrows-gpui/src/main.rs` — derive the responsive header state in `QuickRowsView::render_table`
   - Change: Read `window.viewport_size().width` and set an icon-only toolbar branch for widths below `px(900.0)`.
   - Preserve: The 640×420 minimum window, all command IDs, handlers, keyboard actions, disabled rules, operation behavior, and formatted row count.
   - Verify: Resizing through 900 px changes labels without changing command order, enabled state, or bar height.

2. `crates/quickrows-gpui/src/main.rs` — simplify document identity
   - Change: Remove the 30×30 Q tile and the two-line `v_flex` identity. Render `filename` as one semibold, ellipsized, single-line label with the existing 140 px maximum width. Remove the `CSV workspace`/`Modified` subtitle; dirty state remains visible in the native title and bottom status bar.
   - Preserve: Filename truncation, native window title, dirty-state behavior, and bottom saved/unsaved status.
   - Verify: The header contains one document name, no duplicate brand tile, and no redundant workspace subtitle.

3. `crates/quickrows-gpui/src/main.rs` — establish one-line command-bar geometry
   - Change: Replace the toolbar container’s `min_h(px(56.0))`, `flex_wrap()`, `px_4()`, `py_2()`, `gap_1()`, and top border with the existing query-strip bar metrics: `min_h(px(46.0))`, no wrapping, `px_3()`, `py_1()`, and semantic bottom border on `cx.theme().background`. Keep the table-header border as the next structural separator.
   - Preserve: Full-width placement above the table, semantic theme ownership, and fixed status/metadata at the right edge.
   - Verify: The command bar remains exactly one row at 640 px and wide screenshot-like widths; no command falls onto a second line.

4. `crates/quickrows-gpui/src/main.rs` — group commands by task
   - Change: Compose three nested `h_flex().gap_1()` command groups in this exact order: Open/Save; Copy/Delete/Restore; Find/Dupes. Insert a one-pixel vertical divider using `cx.theme().border` between the filename and File group, between File and Selection, and between Selection and Analysis. Give each divider the compact control height minus the bar’s vertical padding so it reads as a separator rather than a full-height rule.
   - Preserve: Existing action order within each semantic group, IDs, icons, labels, disabled logic, and event handlers.
   - Verify: File, selection, and analysis commands are distinguishable by whitespace/dividers; Delete is no longer visually adjacent to Save without a group boundary.

5. `crates/quickrows-gpui/src/main.rs` — reduce routine button chrome and add responsive labels
   - Change: Apply `.ghost()` to Open, Copy, Delete, Restore, Find, and Dupes; retain `.compact()` on all controls and retain Save as the compact default variant. In the wide branch, retain each current icon plus text label. Below 900 px, omit labels from those six ghost buttons and add tooltips with the exact labels `Open`, `Copy`, `Delete`, `Restore`, `Find`, and `Find duplicates`. Keep Save labeled `Save` in both branches. Add a `Settings` tooltip to the icon-only Settings button in both branches.
   - Preserve: Save’s check icon while labeled, selected-row and loading disabled states, query-panel opening behavior, and all actions.
   - Verify: Wide layouts remain self-explanatory; narrow layouts fit on one line and every icon-only action reveals an unambiguous tooltip.

6. `crates/quickrows-gpui/src/main.rs` — demote row count from badge to metadata
   - Change: Remove the rounded border and background from the row-count container. Keep `format!("{} rows", format_count(row_count))`, render it as compact muted text, and retain the flexible spacer before it and Settings after it.
   - Preserve: Exact count, comma grouping, right alignment, and settings access.
   - Verify: `1,000,000 rows` remains easy to scan but no longer looks like a primary control.

7. `DESIGN.md` — reconcile the accepted command-bar hierarchy after implementation validation
   - Change: Amend `Workspace controls` to state that routine persistent command-bar actions use compact ghost treatment, Save retains default treatment as the document-state action, commands are grouped as File/Selection/Analysis, and the bar uses icon-only controls with tooltips below 900 px rather than wrapping.
   - Preserve: Existing semantic palette ownership and query-strip hierarchy.
   - Verify: The documentation describes the implemented wide and narrow header without conflicting with Find/Duplicates strip rules.

8. `crates/quickrows-gpui/src/state_tests.rs` — cover deterministic responsive presentation decisions
   - Change: Extract a pure helper that returns whether toolbar action labels are shown for a supplied viewport width, and test the values immediately below, at, and above 900 px. If label/tooltip selection is extracted into a pure helper, test `Find duplicates` as the narrow tooltip and the existing wide `Dupes` label.
   - Preserve: Existing count, query-state, selection, and context-menu tests.
   - Verify: The responsive breakpoint and potentially ambiguous duplicate label cannot drift silently.

## Scope

- Inherit: Light, dark, and system themes through semantic tokens; all open CSV documents; dirty/clean, selected/unselected, and loading states.
- Verify: 640×420 minimum, 899 px narrow, 900 px boundary, 1200 px desktop, and the user-provided wide viewport; very long filename; 1 row and 1,000,000 rows; every disabled-state combination; Find and Duplicates panels open below the command bar.
- Exclude: Native OS title-bar styling, table column headers, Find/Duplicates strip redesign, command behavior, keyboard shortcuts, status-bar redesign, settings contents, new icons, overflow menus, and palette changes.

## Validation

- Product: Open a million-row CSV, select/deselect rows, make and revert an edit, then exercise Open, Save, Copy, Delete, Restore, Find, Dupes, and Settings. Every command must behave exactly as before while the header remains one row.
- Interface: Capture light and dark screenshots at 640, 899, 900, 1200, and the original wide viewport. Confirm clear File/Selection/Analysis grouping, subdued routine actions, one-line ellipsized filename, readable row metadata, tooltips on every icon-only action, no clipping, and no wrapping.
- System: Confirm all colors resolve through root `DESIGN.md` semantic roles, every button remains GPUI Component-owned, routine actions use ghost, Save alone remains default, and no new toolbar/palette/icon abstraction was added.
- Repository: `cargo fmt -p quickrows-gpui -- --check && cargo test -p quickrows-gpui && cargo check -p quickrows-gpui` → formatting, helper tests, and native type checking pass.

## Stop conditions

- Stop if `.tooltip()` is unavailable on the installed GPUI Component Button API, if removing `flex_wrap()` clips any command at the 640 px minimum after the narrow branch is active, or if runtime measurement shows the labeled layout does not fit at 900 px. In that case, keep the one-line grouping decision but re-measure the breakpoint from actual rendered widths; do not restore wrapping or invent an overflow menu without a separate design decision.

## Design documentation

- After acceptance and validation: Update root `DESIGN.md` as specified in Change 7; do not document the plan before the runtime result is accepted.
