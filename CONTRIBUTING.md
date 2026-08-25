# Contributing to QuickRows

## Development setup

QuickRows uses the pinned Rust toolchain in `rust-toolchain.toml`. Linux also requires the GPUI native development packages listed in `.github/workflows/ci.yml`.

## Before submitting a change

Run from the repository root:

```sh
cargo fmt --all -- --check
cargo test --workspace --locked
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

For UI changes, run `cargo run -p quickrows-gpui` and inspect the affected surface. For large-file behavior, generate the fixture and follow `test-data/README.md`.

## Ownership rules

- Keep CSV parsing, validation, query, sorting, editing, and saving in `quickrows-core`.
- Keep GPUI entities, layout, rendering, and platform interaction in `quickrows-gpui`.
- Use semantic theme roles and GPUI Component primitives.
- Add focused tests beside the module that owns changed behavior.

Use short imperative commit subjects. Pull requests should summarize the change, list validation performed, include UI evidence when applicable, and link relevant issues.
