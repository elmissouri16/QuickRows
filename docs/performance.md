# Performance validation

QuickRows keeps a permanent, release-profile million-row probe at
`crates/quickrows-core/benches/million_rows.rs`. Generate the fixture and run it
from the repository root:

```bash
python3 scripts/generate_million_csv.py
cargo bench -p quickrows-core --bench million_rows
```

The benchmark prints one JSON object per operation. Defaults are seven samples
for open, sort, and search operations and three samples for save. See
[`test-data/README.md`](../test-data/README.md) for environment overrides and the
manual large-file validation pass.

## Optimization reference run

Reference environment, captured 2026-08-24:

- Apple M3 Pro, arm64 macOS/Darwin 25.5.0
- Rust 1.97.1 (`8bab26f4f`, LLVM 22.1.6)
- Generated `test-data/million-rows.csv` fixture with 1,000,000 data rows
- Release benchmark profile; seven samples except three save samples

| Benchmark | Median |
| --- | ---: |
| Uncached open | 206.247 ms |
| Cold persistent-cache open | 218.280 ms |
| Warm persistent-cache open | 98.225 ms |
| Clean selected-column sort | 137.291 ms |
| Clean selected-column search | 86.111 ms |
| Clean all-column search | 261.331 ms |
| Dirty selected-column search | 88.286 ms |
| Save after one edit | 1,362.869 ms |

An initial one-sample smoke run before projected sort/search and fused save
validation measured warm cached open at 91.579 ms, clean sort at 394.855 ms,
dirty selected-column search at 293.946 ms, and save after one edit at
1,347.271 ms. Compared directionally with that smoke baseline, the reference
run reduced sort time by about 65% and dirty selected-column search by about
70%. Warm open and save remained within the 10% investigation threshold of the
one-sample smoke baseline. Because the baseline used one sample, use the medians
above—not those percentages—as the reference for future regression checks.

## Regression policy

Use the same machine, release toolchain, fixture, and sample counts for a strict
comparison. Investigate median regressions above 10%. Also inspect peak resident
memory for changes above the greater of 10% or 16 MiB. Shared CI timings are
informational unless runners are pinned; correctness checks remain mandatory on
macOS, Windows, and Linux.
