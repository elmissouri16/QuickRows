---
status: implemented
written_against: historical
implemented_by: historical
updated: 2026-08-24
---

# Use grammatical singular and plural UI copy

Written against: 4a5aaee047734715cced1259f1fc01aafe8718cd (with pre-existing working-tree changes)

## Evidence chain

- Surface: Native GPUI operation notices for row mutation/copy and Settings parse-warning disclosure.
- Problem: Counted user-facing messages expose implementation shorthand such as `1 row(s)` and `1 parse warning(s)` even though the exact count is available.
- Design evidence: Row mutation notices format `row(s)` at `crates/quickrows-gpui/src/main.rs:2074` and `crates/quickrows-gpui/src/main.rs:2134`; copy progress/completion notices do so at `crates/quickrows-gpui/src/main.rs:3217` and `crates/quickrows-gpui/src/main.rs:3248`; parse-warning controls format `parse warning(s)` at `crates/quickrows-gpui/src/main.rs:4183-4185`. Nearby established messages already use normal grammatical forms such as `Copied {row_count} rows × ... columns` and `{selected_count} rows selected` when their contexts guarantee or intentionally present plural nouns.
- Owner: User-facing format strings and text-formatting helpers in `crates/quickrows-gpui/src/main.rs`.
- Scope and affected surfaces: Six known `row(s)`/`warning(s)` format strings in row mutation, row copy, and parse-warning disclosure.
- Uncertainty: None. Singular is required only for a count of one; plural is required for zero and all other counts.

## Design decision

Replace parenthetical plural placeholders with count-aware nouns. Use `row` for exactly one and `rows` otherwise; use `parse warning` for exactly one and `parse warnings` otherwise. Keep every surrounding verb, count, punctuation mark, progress state, and action unchanged.

Because six call sites share the exact rule across two nouns, add one small pure noun-selection helper in `main.rs` and test it directly. This prevents the corrected call sites from drifting back to mixed inline plural rules without introducing a broader localization system.

## Reuse

- Reuse each call site’s existing count (`changed`, `rows.len()`, `row_count`, and `warning_count`) and surrounding message.
- Reuse the existing pure-helper pattern near `column_scope_label`, `visible_control`, `size_override_label`, and other text helpers at the bottom of `crates/quickrows-gpui/src/main.rs`.
- Exemplar: `column_scope_label` in `crates/quickrows-gpui/src/main.rs` centralizes deterministic UI-label selection without owning application state.
- New helper: Add a narrowly scoped function such as `counted_noun(count, singular, plural) -> &'static str` near the existing text helpers. Existing code has no shared count-aware noun selector, and the six affected consumers justify one local owner. Do not add a localization framework or a generic message builder.

## Changes

1. `crates/quickrows-gpui/src/main.rs` — text helper section
   - Change: Add a pure helper that returns the singular noun only when `count == 1`, otherwise the plural noun.
   - Preserve: Existing formatting helpers and all application state ownership.
   - Verify: Counts `0`, `1`, and `2` resolve to plural, singular, and plural respectively.

2. `crates/quickrows-gpui/src/main.rs` — row mutation notices
   - Change: Replace both `row(s)` placeholders with count-aware `row`/`rows` using `changed`.
   - Preserve: `Deleted`/`Restored` wording, dirty-state suffixes, cancellation/error copy, and foreground/background mutation behavior.
   - Verify: A one-row operation says `Deleted 1 row.` or `Restored 1 row.`; multi-row operations retain the correct plural.

3. `crates/quickrows-gpui/src/main.rs` — row copy notices
   - Change: Replace `Copying {} row(s)…` and `Copied {row_count} row(s) to clipboard.` with count-aware nouns using `rows.len()` and `row_count`.
   - Preserve: Progress tracking, cancellation, clipboard behavior, and confirmation thresholds.
   - Verify: Copying one row produces singular progress and completion messages; copying multiple rows produces plural messages.

4. `crates/quickrows-gpui/src/main.rs` — Settings parse-warning disclosure
   - Change: Replace both `parse warning(s)` branches with count-aware `parse warning`/`parse warnings` using `warning_count`.
   - Preserve: View/Hide state, warning count, disclosure interaction, warning-list rendering, and warning details.
   - Verify: One warning renders `View 1 parse warning` / `Hide 1 parse warning`; multiple warnings render the plural forms.

5. `crates/quickrows-gpui/src/state_tests.rs`
   - Change: Import the count-aware noun helper and add unit coverage for counts zero, one, and more than one.
   - Preserve: Existing state-independent test organization and current tests.
   - Verify: The grammatical rule is locked without requiring GPUI rendering.

## Scope

- Inherit: All platforms and themes because the affected strings are shared by the native view.
- Verify: Single- and multi-row delete, restore, and copy notices; one and multiple parse warnings; foreground and background mutation branches.
- Exclude: Guaranteed-plural messages whose operation thresholds cannot be singular, broader copy rewrites, number localization, internationalization, operation behavior, and unrelated singular/plural wording outside the six audited placeholders.

## Validation

- Product: Perform single-row and multi-row copy/delete/restore operations and inspect progress/completion notices. Open Settings for CSV fixtures with one and multiple parse warnings and toggle the warning disclosure.
- Interface: Confirm the corrected phrases fit the existing notice and Settings button compositions at minimum and default window sizes in light and dark themes.
- System: Search `crates/quickrows-gpui/src/main.rs` for `row(s)` and `warning(s)`; none of the six audited user-facing placeholders should remain. Confirm all changed strings use the shared noun selector.
- Repository: `cargo test -p quickrows-gpui && cargo check -p quickrows-gpui` → helper tests pass and the native application type-checks.

## Stop conditions

- Stop if the project adopts a localization/message-formatting owner before implementation; move plural selection into that owner instead of adding the local helper.

## Design documentation

- After acceptance and validation: None. This is a grammatical correction, not a new product-language decision.
