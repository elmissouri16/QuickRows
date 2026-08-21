# Give QuickRows one semantic workspace palette

Written against: 4a5aaee047734715cced1259f1fc01aafe8718cd (with pre-existing working-tree changes)

## Evidence chain

- Surface: Native GPUI CSV task family: empty/open-file entry, open-document toolbar, Find and Duplicates strips, table header, row-count badge, status bar, and Settings overlay.
- Problem: The rendered open-document workspace presents cyan structural chrome, a yellow Duplicates region, component-owned primary-button color, and green/yellow status color at the same time. The result has no single accent hierarchy and makes tool identity compete with actual data/status states.
- Design evidence: The user-provided dark-theme screenshot explicitly identifies the colors as visually uncoordinated. The runtime paints the table header with `cyan` at `crates/quickrows-gpui/src/main.rs:4570-4571`; the toolbar border and Q mark at `crates/quickrows-gpui/src/main.rs:4906` and `crates/quickrows-gpui/src/main.rs:4917`; the row-count badge at `crates/quickrows-gpui/src/main.rs:5027-5028`; the Find strip at `crates/quickrows-gpui/src/main.rs:5054-5061`; and the Duplicates strip with `yellow` at `crates/quickrows-gpui/src/main.rs:5169-5176`. Component primary buttons use the connected theme’s semantic accent instead of either strip color. The same Q identity also uses `cyan` in the empty state and Settings overlay at `crates/quickrows-gpui/src/main.rs:3400`, `crates/quickrows-gpui/src/main.rs:4393-4410`, and section markers use it in `settings_section_title` near `crates/quickrows-gpui/src/main.rs:5749`.
- Owner: GPUI Component `Theme` semantic tokens consumed by the native compositions in `crates/quickrows-gpui/src/main.rs`.
- Scope and affected surfaces: Structural/background color, border color, brand mark color, and localized state color throughout the native CSV task family.
- Uncertainty: Exact light-theme contrast must be validated at runtime because the current theme resolves token values by mode. No custom hexadecimal colors are required.

## Design decision

Use the connected GPUI Component theme as the only palette owner and assign each semantic role once:

- `background`, `secondary`, and `border` own canvases, bars, table headers, panels, badges, and separators.
- `accent` and `accent_foreground` own QuickRows identity and interactive emphasis, including every Q mark and the small localized marker/label for an open tool strip.
- `muted_foreground` owns supporting text.
- `info`, `warning`, `danger`, `selection`, `green`, and `yellow` remain localized to real search, duplicate, destructive, selection, saved, and dirty states; they must not tint entire structural regions.

Apply this mapping in both theme modes. Do not add a parallel palette struct, custom RGB values, or hard-coded light/dark branches: the existing semantic theme tokens already express the decision and keep component primary buttons in the same system.

## Reuse

- Reuse `cx.theme().background`, `secondary`, `border`, `foreground`, `muted_foreground`, `accent`, and `accent_foreground` from GPUI Component.
- Reuse the existing state colors already attached to rows/cells and status indicators: `info`, `warning`, `danger`, `selection`, `green`, and `yellow`.
- Reuse the existing Q compositions in `QuickRowsView::render_empty`, `QuickRowsView::render_settings`, and `QuickRowsView::render_table`; change their token roles rather than creating a logo component solely for this work.
- Exemplar: GPUI Component primary buttons already derive their emphasis from the theme accent and therefore remain aligned when structural identity also uses `accent`.
- Do not introduce a new palette primitive. The current `Theme` is connected to every affected consumer and already provides all required semantic roles.

## Changes

1. `crates/quickrows-gpui/src/main.rs` — Q identity marks in `render_empty`, `render_settings`, and `render_table`
   - Change: Replace `cx.theme().cyan` backgrounds with `cx.theme().accent`, and replace hard-coded white foregrounds with `cx.theme().accent_foreground`.
   - Preserve: Mark dimensions, corner radii, Q copy, typography, shadows, and placement.
   - Verify: All three Q marks use the same theme-owned brand treatment in light and dark modes.

2. `crates/quickrows-gpui/src/main.rs` — open-document toolbar and row-count badge
   - Change: Use `border` for toolbar separators; retain `secondary` as the toolbar surface; render the row-count badge with neutral `border` plus `background` or `secondary` rather than a cyan tint.
   - Preserve: Toolbar layout, wrapping, actions, disabled states, document identity, row count, and Settings access.
   - Verify: The toolbar reads as one neutral workspace surface, while the Q mark and component action states supply the only interactive accent.

3. `crates/quickrows-gpui/src/main.rs` — Find and Duplicates strips
   - Change: Give both strips the same neutral `secondary`/`border` structural treatment. Use `accent` only on their localized section label or narrow marker; remove the cyan and yellow full-strip border/background tinting.
   - Preserve: FIND/DUPLICATES labels, panel order, controls, result text, stale state, loading state, and show/hide behavior.
   - Verify: Opening either or both tools does not add a new region-wide hue. Duplicates remain identifiable through their label and warning-colored row results, not a yellow workspace band.

4. `crates/quickrows-gpui/src/main.rs` — table header
   - Change: Replace the cyan-tinted table-header surface and separator with `secondary` and `border`.
   - Preserve: Header height, typography, column sizing, sorting click targets, resizers, horizontal scrolling, and body cell styling.
   - Verify: The table header belongs to the neutral data surface and does not compete with active query or selection states.

5. `crates/quickrows-gpui/src/main.rs` — empty-state card, Settings shell, and `settings_section_title`
   - Change: Replace cyan structural borders/tints with `border`/`secondary`; use `accent` only for the Q mark and section marker.
   - Preserve: Empty-state messaging, recent files, modal layout, settings rows, close/done controls, shadows, and scrolling.
   - Verify: Entry, workspace, and Settings surfaces share the same palette roles instead of using cyan as both identity and general-purpose chrome.

6. `crates/quickrows-gpui/src/main.rs` — state colors
   - Change: Keep existing localized row/cell/status colors and confirm no structural element is assigned `info`, `warning`, `danger`, `selection`, `green`, or `yellow`.
   - Preserve: Search-match, duplicate-match, current-result, deleted-row, selected-row/cell, dirty, saved, warning, and error meanings.
   - Verify: State colors become more legible because they appear only when that state is present.

## Scope

- Inherit: Light, dark, and system theme preferences because every replacement uses resolved theme tokens.
- Verify: Empty workspace, open document, Find only, Duplicates only, both strips open, search and duplicate result highlights, selection, deleted rows, dirty/saved status, Settings, disabled controls, and loading notices.
- Exclude: New custom themes, user-selectable accent colors, icon redesign, typography changes, spacing changes, button geometry, table density, CSV behavior, and legacy React/Tauri surfaces.

## Validation

- Product: Open a CSV, toggle Find and Duplicates, run queries with results, select and delete rows, save the document, and open Settings. Confirm identity, actions, and states retain distinct meanings without region-wide competing hues.
- Interface: Compare 640×420 minimum and 800×600 default windows in light and dark themes. Inspect empty/open states, long filenames, large row counts, one and two query strips, disabled actions, warning/error notices, and Settings.
- System: Search the native surface for structural uses of `theme().cyan` and `theme().yellow`. Q/section identity may use `accent`; yellow/green/info/warning/danger/selection must remain limited to state feedback. Confirm no new color constants or palette owner were introduced.
- Repository: `cargo fmt -p quickrows-gpui -- --check && cargo test -p quickrows-gpui && cargo check -p quickrows-gpui` → formatting, tests, and native type checking pass.

## Stop conditions

- Stop if the installed GPUI Component theme does not provide sufficient contrast for `accent_foreground` on `accent` in either mode, or if a repository-owned theme override is introduced before implementation. Resolve the theme owner rather than adding per-surface hard-coded colors.

## Design documentation

- After acceptance and validation: Add a short native visual-system section to a root `DESIGN.md` documenting the semantic role mapping above and that raw status hues are reserved for localized state feedback.
