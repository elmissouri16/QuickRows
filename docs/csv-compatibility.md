# CSV compatibility

QuickRows treats RFC 4180 and RFC 7111 as the standards baseline, then adds opt-in support for common spreadsheet and data-export dialects. CSV has no single universal dialect beyond those specifications, so compatibility is described explicitly rather than as an unqualified “all CSV” claim.

## Standards baseline

- RFC 4180 records: comma-separated fields, double-quoted fields, doubled quotes, embedded commas, embedded CR/LF, CRLF records, empty fields, optional header records, and a missing final record terminator.
- RFC 7111 fragments: `row=`, `col=`, and `cell=` selectors; single positions, ranges, multiple `;`-separated selections, `*`, one-based coordinates, clamping, and header-inclusive row coordinates.
- Native open requests retain RFC 7111 fragments from file URLs and command-line values, percent-decode them, resolve them after parsing, and apply the resulting row/cell selection.

## Supported real-world dialects

- Automatic or explicit comma, tab, semicolon, pipe, colon, and Unicode delimiters.
- Double, single, or one-Unicode-scalar quote characters.
- Optional backslash or one-Unicode-scalar escape characters.
- Optional ASCII or Unicode comment characters. Comment lines and their record positions are preserved when saving.
- Excel `sep=<character>` directives, including tab and Unicode separators; directives are omitted from table rows and preserved when saving.
- LF, CRLF, and CR record endings, including embedded line endings inside quoted fields.
- Header, no-header, and automatic/ambiguous-header workflows. Ambiguous all-text input is not silently consumed as a header.

## Encodings and byte handling

- UTF-8, UTF-16LE, and UTF-16BE, with or without a BOM when endianness is explicit.
- BOM length is tracked independently from an encoding override, so a forced encoding cannot leave part of a BOM in the first field.
- BOM presence is preserved only when compatible with the output encoding; BOMless UTF-16 stays BOMless.
- Legacy encodings exposed by `encoding_rs`, with atomic save failure when edited text is not representable.
- Incremental transcoding and canonicalization with bounded buffers, cancellation checks, and progress callbacks.
- NUL bytes and leading/trailing spaces are preserved as field content.

## Malformed input and limits

- `strict`: rejects malformed encoding, quote grammar, unequal row widths, and configured size-limit violations.
- `skip`: skips records rejected by encoding, row-width, parser, or size validation and records bounded warnings.
- `repair`: replaces malformed encoding, repairs row widths, preserves rectangular rows when truncating oversized records, and records bounded warnings.
- Field and record sizes are unlimited by default. Optional byte limits are enforced in all malformed modes.
- Warning storage is capped at 200 entries.

## Save behavior

- Saves stream records instead of accumulating the document in memory.
- The target is atomically replaced only after all rows encode successfully.
- Delimiter, quote, escape, comment behavior, Excel separator directive, line endings, encoding, and compatible BOM state are retained.
- A first field beginning with the configured comment character is quoted so it cannot disappear on reopen.
- Source comment lines are retained at their logical record positions.

## Verification

Run:

```sh
cargo test --workspace
cargo check --workspace
```

The core suite includes RFC 4180 round trips; the dialect matrix; RFC 7111 parsing/resolution; UTF-16 LE/BE, malformed-surrogate, odd-byte, and chunk-boundary cases; BOM overrides; Unicode syntax; comments; Excel separators; strict/skip/repair behavior; explicit and default-unlimited size handling; warning bounds; cancellation; detection false-positive fixtures; atomic encoding failure; and prepared-source cache reuse.
