---
status: implemented
written_against: historical
implemented_by: historical
updated: 2026-08-24
---

# Unify workspace controls under one compact hierarchy

Written against: 4a5aaee047734715cced1259f1fc01aafe8718cd (with pre-existing working-tree changes)

## Evidence chain

- Surface: Native GPUI open-document workspace toolbar plus Find and Duplicates strips.
- Problem: Controls that occupy the same stacked workspace-bar family use different heights and variant rules. The rendered Duplicates strip shows a large bright Check button beside text-like Clear, Previous, Next, and Close controls, while every document-toolbar action uses a smaller contained button. The mismatch makes each strip appear to come from a different component system.
- Design evidence: The user-provided dark-theme screenshot explicitly requests unified elements. All persistent toolbar buttons apply GPUI Component’s `.compact()` modifier at `crates/quickrows-gpui/src/main.rs:4951-5036`. None of the connected Find and Duplicates buttons apply `.compact()` at `crates/quickrows-gpui/src/main.rs:5065-5152` and `crates/quickrows-gpui/src/main.rs:5180-5242`. Query execution uses `.primary()`, secondary panel actions use `.ghost()`, and search toggles use the default variant, but their geometry does not share the toolbar contract.
- Owner: GPUI Component `Button`, `ButtonVariants`, and the button compositions inside `QuickRowsView::render_table` in `crates/quickrows-gpui/src/main.rs`.
- Scope and affected surfaces: Persistent document-toolbar actions and every button in the Find and Duplicates strips, including enabled, disabled, loading, stale, and toggle states.
- Uncertainty: The compact button height must be checked beside the standard GPUI Component Input at the minimum window width; no custom dimensions should be invented unless the connected component cannot align them.

## Design decision

Use GPUI Component’s existing compact Button modifier as the shared geometry for every control in the stacked workspace bars, and retain one deterministic variant hierarchy:

- **Execution:** compact primary buttons for `Find`, `Check`, and their existing re-run execution action when shown.
- **Navigation and utilities:** compact ghost buttons for scope, Previous, Next, Clear, Close, and Settings.
- **Toggles:** compact default buttons for match-case and whole-word, retaining their existing check-mark selected presentation.
- **Persistent document actions:** keep their existing compact geometry and current default/ghost roles.

This aligns element height and emphasis without adding custom colors, pixel heights, or a wrapper component. Primary remains rare and means “run the current tool”; ghost remains secondary; default remains a persistent action or stateful toggle.

## Reuse

- Reuse GPUI Component `Button`, `.compact()`, `.primary()`, `.ghost()`, `.disabled()`, `.icon()`, and `.label()`.
- Reuse the existing persistent toolbar at `crates/quickrows-gpui/src/main.rs:4951-5036` as the geometry exemplar.
- Reuse the current variant intent in Find and Duplicates: execution is already primary, navigation/utilities are already ghost, and the two search toggles are already default.
- Do not add a `WorkspaceButton`, custom CSS-like dimensions, or another button abstraction. Existing Button modifiers express the complete decision, and the affected calls remain localized to one render composition.

## Changes

1. `crates/quickrows-gpui/src/main.rs` — Find strip buttons in `QuickRowsView::render_table`
   - Change: Add `.compact()` to search scope, match-case, whole-word, Find, Previous, Next, re-run, Clear, and Close buttons. Keep Find and re-run primary; keep scope/navigation/clear/close ghost; keep match-case and whole-word default.
   - Preserve: Button IDs, labels, check marks, input placement, disabled rules, search lifecycle, keyboard behavior, result labels, stale treatment, and event handlers.
   - Verify: All Find controls share compact geometry, with one clear primary execution action and lower-emphasis supporting actions.

2. `crates/quickrows-gpui/src/main.rs` — Duplicates strip buttons in `QuickRowsView::render_table`
   - Change: Add `.compact()` to duplicate scope, Check, Clear, re-run, Previous, Next, and Close. Keep Check and re-run primary; keep scope/navigation/clear/close ghost.
   - Preserve: Button IDs, labels, loading copy, disabled rules, duplicate lifecycle, result labels, stale treatment, and event handlers.
   - Verify: Check no longer appears as a different-sized control, and the strip uses the same hierarchy as Find.

3. `crates/quickrows-gpui/src/main.rs` — persistent document toolbar
   - Change: Retain the existing compact modifier on Open, Save, Copy, Delete, Restore, Find, Dupes, and Settings; audit the final implementation to ensure none is enlarged or restyled to compensate for query-strip changes.
   - Preserve: Icons, labels, disabled states, selection requirements, action behavior, wrapping, row-count badge, and document identity.
   - Verify: Persistent actions and temporary tool-strip controls align as members of one control family while preserving their different semantic roles.

4. `crates/quickrows-gpui/src/main.rs` — responsive workspace bars
   - Change: Keep existing flex wrapping and spacing owners; adjust no fixed width or height unless runtime validation proves compact controls are clipped beside the search Input.
   - Preserve: `min_h`, padding, gaps, `flex_wrap`, search Input `flex_1`/`min_w`, and support for the 640×420 minimum window.
   - Verify: Compact controls wrap cleanly, labels remain readable, and no click target or input is clipped at supported sizes.

## Scope

- Inherit: Light, dark, and system themes because the change uses existing Button variants rather than custom styling.
- Verify: Toolbar with no selection, selected rows, dirty document, loading operation, Find idle/running/results/stale, Duplicates idle/running/results/stale, both strips open, and minimum-width wrapping.
- Exclude: Modal buttons, empty-state actions, settings controls, context-menu items, notification actions, button color/palette changes, toolbar information architecture, icon replacement, copy changes, and query behavior.

## Validation

- Product: Open a CSV; exercise toolbar actions; open Find and Duplicates; toggle match case/whole word; run, clear, navigate, re-run, and close both tools. Confirm hierarchy remains understandable and every action behaves unchanged.
- Interface: Inspect 640×420 minimum, 800×600 default, and the wide screenshot-like viewport in light and dark themes. Check enabled, disabled, loading, stale, selected-toggle, one-strip, and two-strip states, with long column names in scope controls.
- System: Confirm every Button in the persistent toolbar and query strips uses `.compact()`, execution actions alone use `.primary()`, secondary query actions use `.ghost()`, and no custom button size or parallel wrapper was introduced.
- Repository: `cargo fmt -p quickrows-gpui -- --check && cargo test -p quickrows-gpui && cargo check -p quickrows-gpui` → formatting, tests, and native type checking pass.

## Stop conditions

- Stop if GPUI Component’s compact Button cannot align with the standard Input or produces clipped labels at the minimum window. Resolve alignment through an existing Input/Button size API; do not introduce arbitrary per-button pixel heights.

## Design documentation

- After acceptance and validation: In the native visual-system section of root `DESIGN.md`, record that workspace-bar controls use compact Button geometry and that primary, ghost, and default variants map to execution, utility/navigation, and stateful-toggle roles respectively.
