# Repository scripts

## Development and validation

- `generate_million_csv.py` creates the ignored stress fixture under `test-data/generated/`.
- `check-release-version.py` verifies Cargo, packager, and optional release-tag versions.
- `install-linux-deps.sh` installs GPUI development dependencies; pass `--packaging` for package-build dependencies.

## macOS packaging

`scripts/macos-dmg.sh` is the stable user-facing command. It delegates build and verification to `package/macos-dmg-build.sh`, installation and rollback to `install/macos-app.sh`, and shared cleanup to `lib/macos-dmg-common.sh`.

The helper files are sourced modules, not standalone commands. Keep option parsing in the entry script and preserve transactional cleanup and rollback when changing packaging behavior.

## macOS native picker smoke test

After installing a packaged build, run:

```sh
scripts/smoke-test-macos-picker.sh
```

Pass an application path to test a non-default installation:

```sh
scripts/smoke-test-macos-picker.sh /path/to/QuickRows.app
```

The test opens and cancels the native CSV picker repeatedly, opens a real CSV, verifies the document window title, opens the picker again after loading, checks that the process remains alive, and rejects any newly generated macOS crash report. The terminal running the script must have permission to control **System Events** under System Settings → Privacy & Security → Accessibility.
