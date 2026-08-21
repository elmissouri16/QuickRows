# Large-file test data

Generate the ignored million-record fixture from the repository root:

```bash
python3 scripts/generate_million_csv.py
cargo run -p quickrows-gpui -- test-data/million-rows.csv
```

The generated file contains exactly 1,000,000 data records plus a header. It uses CRLF endings and includes intentional duplicate groups, quoted commas, quotes, and embedded newlines.

Suggested stress pass:

1. Measure startup time and memory until the first rows are visible.
2. Select all rows and confirm selection remains compact/responsive.
3. Shift-click and Shift+Page Down across large ranges.
4. Search for `quoted value`, then cancel and re-run.
5. Check duplicates in the `email` column.
6. Sort `name`, `category`, and `amount` in both directions.
7. Copy a large row selection and a rectangular cell range; test cancellation.
8. Delete and restore a large selection.
9. Save As, reopen, and verify record count and CRLF/quoted fields.
10. Repeat open and sort to verify persistent offset/sort cache reuse.
