# Restore dark-mode hierarchy on the empty workspace

Written against: 4a5aaee047734715cced1259f1fc01aafe8718cd (with pre-existing working-tree changes)

## Evidence chain

- Surface: Native GPUI empty workspace in dark mode, rendered by `QuickRowsView::render_empty` in `crates/quickrows-gpui/src/main.rs:3304-3374`.
- Problem: The user-provided dark-theme screenshot shows the full-width `Choose a CSV file` action as an almost-white slab against an almost-black canvas, while the recent-file actions use quiet dark surfaces. This makes the entry action dominate the entire workspace rather than participating in the compact control hierarchy.
- Design evidence: Root `DESIGN.md:5` makes GPUI Component's connected `Theme` the palette owner and forbids per-surface color constants when semantic roles exist. `DESIGN.md:21` reserves the Primary button variant for executing or re-running a query. The empty-state Open action is not query execution, but `render_empty` applies `.primary()` at `crates/quickrows-gpui/src/main.rs:3319-3321`. In installed `gpui-component 0.5.1`, `ButtonVariant` defaults to `Secondary`, Primary resolves through `theme.primary`, and Secondary resolves through `theme.secondary`. Its connected default dark theme resolves `background` to `#0a0a0a`, `primary.background` to `#fafafa`, `primary.foreground` to `#171717`, and `secondary.background` to `#171717`, exactly accounting for the screenshot's white Open action and quiet recent-file rows. The same surface's recent-file buttons omit a variant modifier at `crates/quickrows-gpui/src/main.rs:3350-3360`, providing the local default-variant exemplar.
- Owner: GPUI Component `Button`/`ButtonVariant` and the connected `Theme`; consumer is `QuickRowsView::render_empty`.
- Scope and affected surfaces: Empty workspace Open action in Light, Dark, and System modes, including idle and loading/disabled states. Recent-file history remains on the same surface but does not change.
- Uncertainty: None. The existing default Button variant and the rendered sibling controls determine the correction without inventing a color or new component.

## Design decision

Remove `.primary()` from the empty-state `Choose a CSV file` button so it uses GPUI Component's default Secondary variant. Primary remains reserved for Find/Check query execution as documented. This replaces the inverted white dark-mode slab with the connected theme's quiet secondary surface while preserving the action's full width, position, icon, label, and behavior.

Do not change the application-wide dark palette, add a dark-mode branch, or hard-code colors. The reported mismatch is caused by a variant-role violation, and the existing semantic owner already provides the deterministic correction.

## Reuse

- GPUI Component `Button` with its default `ButtonVariant::Secondary`.
- Connected `cx.theme().secondary`, `secondary_foreground`, `secondary_hover`, and `secondary_active` behavior supplied by GPUI Component.
- Exemplar: Recent-file buttons in `QuickRowsView::render_empty` at `crates/quickrows-gpui/src/main.rs:3350-3360`, which already use the default Button variant and render as quiet dark rows in the supplied screenshot.
- No new primitive, palette override, or per-mode color is required.

## Changes

1. `crates/quickrows-gpui/src/main.rs` — `QuickRowsView::render_empty`
   - Change: Remove `.primary()` from `Button::new("open-csv")`; leave the button on GPUI Component's default Secondary variant.
   - Preserve: `open-csv` ID, full available width from the parent stack, FolderOpen icon, `Choose a CSV file`/`Opening…` labels, loading-disabled rule, open-dialog handler, spacing, recent-file list, and recent-file removal controls.
   - Verify: In dark mode, the Open action uses the connected secondary dark surface instead of an almost-white primary fill; in light mode it remains a clearly bounded standard button.

2. `DESIGN.md` — workspace control hierarchy
   - Change: After runtime acceptance, clarify that Primary is reserved for query execution and that the empty-workspace file-entry action uses the default Button variant.
   - Preserve: Connected Theme ownership, compact workspace-bar rules, Save hierarchy, and the prohibition on custom button colors.
   - Verify: Documentation names the exact variant used by the empty-state entry action and does not introduce a new palette role.

## Scope

- Inherit: Light, Dark, and System themes because the Button continues to resolve colors from the connected Theme.
- Verify: Empty workspace with no recent files and with one to six recent files; idle and `Opening…` disabled states; 640×420 minimum and 800×600 default windows.
- Exclude: Recent-file button structure, empty-state spacing, global theme values, table/workspace colors, settings colors, query buttons, toolbar buttons, and any custom QuickRows palette.

## Validation

- Product: Launch without an open document, use `Choose a CSV file`, cancel once, then open a CSV; confirm behavior and loading state are unchanged.
- Interface: Compare Light, Dark, and System modes with empty and populated recent history. Dark mode should show a quiet secondary Open button with readable foreground and hover/pressed states, not a white slab; light mode must remain legible and visibly actionable.
- System: Confirm `open-csv` has no `.primary()` or custom `.bg()`/`.text_color()` override, recent-file buttons remain default, and query execution remains the only Primary role on this task family.
- Repository: `cargo fmt -p quickrows-gpui -- --check && cargo test -p quickrows-gpui && cargo check -p quickrows-gpui` → formatting, native tests, and type checking pass.

## Stop conditions

- Stop if the installed GPUI Component default Button variant no longer resolves to Secondary, or if runtime inspection shows insufficient foreground contrast in either theme. Resolve the connected semantic theme/variant owner rather than adding per-surface hard-coded colors.

## Design documentation

- After acceptance and validation: Update `DESIGN.md` under `Workspace controls` to state that the empty-workspace file-entry action uses the default Button variant and Primary remains reserved for executing or re-running a query.
