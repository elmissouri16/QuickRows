# Repository scripts

## Development and validation

- `generate_million_csv.py` creates the ignored stress fixture under `test-data/generated/`.
- `check-release-version.py` verifies Cargo, packager, and optional release-tag versions.
- `install-linux-deps.sh` installs GPUI development dependencies; pass `--packaging` for package-build dependencies.

## macOS packaging

`scripts/macos-dmg.sh` is the stable user-facing command. It delegates build and verification to `package/macos-dmg-build.sh`, installation and rollback to `install/macos-app.sh`, and shared cleanup to `lib/macos-dmg-common.sh`.

The helper files are sourced modules, not standalone commands. Keep option parsing in the entry script and preserve transactional cleanup and rollback when changing packaging behavior.
