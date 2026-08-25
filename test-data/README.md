# Test data

- `generated/` contains ignored large or transient files created locally.
- `fixtures/` is reserved for small, deterministic CSV fixtures that should be reviewed and tracked.

Generate the ignored million-record fixture from the repository root:

```bash
python3 scripts/generate_million_csv.py
cargo run -p quickrows-gpui -- test-data/generated/million-rows.csv
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

Run the permanent core performance probe in the optimized benchmark profile:

```bash
cargo bench -p quickrows-core --bench million_rows
```

It reports JSON lines containing median timings for uncached, cold-cache, and
warm-cache opens; clean sorting; clean and dirty searches; and save-after-edit.
The default is seven samples per operation and three save samples. Override them
for a quick local pass with `QUICKROWS_BENCH_SAMPLES` and
`QUICKROWS_BENCH_SAVE_SAMPLES`. Set `QUICKROWS_MILLION_FIXTURE` to benchmark a
fixture outside `test-data/generated/`.

For stable comparisons, use the same release toolchain and fixture, close other
I/O-heavy applications, and record both elapsed time and peak resident memory.
Treat median regressions above 10% or peak-memory growth above the greater of
10% and 16 MiB as requiring investigation rather than as automatic proof of a
code regression on shared CI hardware. The latest pinned-machine reference
results are recorded in [`docs/performance.md`](../docs/performance.md).
