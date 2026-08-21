# Native release validation

Run the Package smoke workflow and complete this checklist on physical or representative virtual machines before publishing a release.

## Every platform

- Install the generated package over a clean system account.
- Launch from the application launcher and from a terminal.
- Open CSV paths containing spaces, emoji, accented characters, and non-Latin text.
- Open `file:///…/sample.csv#row=2-4`, `#col=2-3`, and `#cell=2,2-4,3`; verify the resolved data/cell selection is visible.
- Double-click a `.csv`; verify QuickRows opens it.
- Double-click another `.csv` while QuickRows is running; verify the existing instance activates and prompts before replacing dirty work.
- Launch a second `quickrows PATH` process; verify it forwards the file and exits.
- Save, Save As, reload, recent files, parse overrides, warning details, logging, and log clearing.
- Upgrade over the previous released version; confirm settings and recent files survive.
- Uninstall and verify application files/associations are removed without deleting user CSVs.
- Run `python3 scripts/generate_million_csv.py` and complete `test-data/README.md`.

## macOS

- Test the `.dmg`/`.app` on the oldest supported macOS and current macOS.
- Confirm bundle identifier `com.el.csv-viewer`, icons, CSV document role, Dock reopen, and Finder open-document events.
- Validate Developer ID signing, hardened runtime, notarization, and Gatekeeper on a downloaded artifact.

## Windows

- Test installer install, repair/upgrade, and uninstall on supported Windows versions.
- Confirm Start menu metadata, icons, `.csv` association, Explorer open events, Unicode paths, and single-instance forwarding.
- Validate Authenticode signing and SmartScreen behavior for the downloaded installer.

## Linux

- Test both X11 and Wayland sessions.
- Test the generated `.deb` and AppImage (when published).
- Confirm desktop entry, icon sizes, MIME registration, `text/csv` association, second-instance forwarding, clipboard, file dialogs, and log-folder reveal.
- Refresh desktop/MIME databases where the target distribution requires it and verify behavior after reboot.

Record OS versions, package hashes, pass/fail results, memory/startup measurements for the million-row fixture, and links to any release-blocking issues in the release notes.
