#!/bin/bash
# Exercise the packaged QuickRows/AppKit open-panel boundary.
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "error: this smoke test requires macOS" >&2
    exit 2
fi

app_path="${1:-/Applications/QuickRows.app}"
executable="$app_path/Contents/MacOS/quickrows"
if [[ ! -x "$executable" ]]; then
    echo "error: QuickRows executable not found at $executable" >&2
    echo "build/install first with: scripts/macos-dmg.sh" >&2
    exit 2
fi

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/quickrows-picker-smoke.XXXXXX")"
crash_marker="$work_dir/crash-marker"
fixture="$work_dir/picker-smoke.csv"
log_file="$work_dir/quickrows.log"
printf 'name,value\nalpha,1\nbeta,2\n' > "$fixture"
touch "$crash_marker"

pid=""
if pgrep -f "^${executable//./\\.}" >/dev/null 2>&1; then
    echo "error: QuickRows is already running; quit it before this isolated smoke test" >&2
    exit 2
fi

cleanup() {
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
    fi
    rm -rf "$work_dir"
}
trap cleanup EXIT INT TERM

open -na "$app_path"
for _ in {1..30}; do
    pid="$(pgrep -n -f "^${executable//./\\.}" 2>/dev/null || true)"
    [[ -n "$pid" ]] && break
    sleep 0.2
done
if [[ -z "$pid" ]]; then
    echo "error: QuickRows did not launch" >&2
    exit 1
fi

assert_alive() {
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "error: QuickRows crashed or exited during the picker smoke test" >&2
        [[ -f "$log_file" ]] && cat "$log_file" >&2
        exit 1
    fi
}

open_picker() {
    osascript <<'APPLESCRIPT'
tell application id "com.el.csv-viewer" to activate
delay 0.2
tell application "System Events"
    keystroke "o" using command down
    delay 0.6
    tell process "quickrows"
        if not (exists window "Open") then error "QuickRows Open panel did not appear"
    end tell
end tell
APPLESCRIPT
    assert_alive
}

cancel_picker() {
    osascript <<'APPLESCRIPT'
tell application "System Events"
    key code 53
    delay 0.3
end tell
APPLESCRIPT
    assert_alive
}

# Repeated open/cancel catches event-loop re-entrancy and callback-lifetime bugs.
for _ in 1 2 3; do
    open_picker
    cancel_picker
done

# Exercise the packaged app's open-document route with a real CSV. Selection
# and cancellation inside the picker are covered deterministically by the GPUI
# tests; using `open` here avoids localization-dependent Finder UI scripting.
open -a "$app_path" "$fixture"
for _ in {1..30}; do
    assert_alive
    window_names="$(osascript <<'APPLESCRIPT'
tell application "System Events" to tell process "quickrows" to get name of every window
APPLESCRIPT
)"
    [[ "$window_names" == *"picker-smoke.csv - QuickRows"* ]] && break
    sleep 0.2
done
if [[ "$window_names" != *"picker-smoke.csv - QuickRows"* ]]; then
    echo "error: packaged open-document handling did not load the CSV" >&2
    echo "observed windows: $window_names" >&2
    exit 1
fi

# Make sure the native picker can still be opened safely after a document has
# loaded through the packaged application's document route.
open_picker
cancel_picker

sleep 0.5
assert_alive
new_crashes="$(find "$HOME/Library/Logs/DiagnosticReports" -maxdepth 1 \
    -type f \( -name 'quickrows*.ips' -o -name 'QuickRows*.ips' \) \
    -newer "$crash_marker" -print 2>/dev/null || true)"
if [[ -n "$new_crashes" ]]; then
    echo "error: macOS produced a QuickRows crash report:" >&2
    echo "$new_crashes" >&2
    exit 1
fi

echo "macOS picker smoke test passed (PID $pid)"
