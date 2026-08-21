# QuickRows Design System

## Native workspace visual system

The native GPUI app uses GPUI Component's connected `Theme` as its palette owner. Do not add per-surface color constants when a semantic theme role expresses the same intent.

### Palette roles

- `background`, `secondary`, and `border`: canvases, bars, panels, table headers, badges, and separators.
- `accent` and `accent_foreground`: QuickRows identity and interactive emphasis, including the Q mark and localized active-tool labels.
- `muted_foreground`: supporting copy and secondary metadata.
- `info`, `warning`, `danger`, and `selection`: localized query, destructive, and selection feedback.
- `green` and `yellow`: localized saved and dirty status feedback.

Status colors must not tint whole structural regions. Find and Duplicates share neutral panel surfaces; their result states provide semantic color only where needed.

### Workspace controls

Buttons in the document command bar and Find/Duplicates strips use compact geometry.

- Primary: execute or re-run the current query.
- Ghost: routine document commands, scope, navigation, clear, close, and settings utilities.
- Default: the empty-workspace file-entry action, Save as the document-state action, and stateful query toggles.

The native window title owns document identity; do not repeat the filename in the command bar. The command bar stays on one line and groups commands as File, Selection, and Analysis. Below 900 px, familiar actions become icon-only controls with text tooltips instead of wrapping onto another line.

Use GPUI Component's existing `Button` modifiers rather than custom button colors or a parallel wrapper component.
