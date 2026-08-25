#!/usr/bin/env bash
# Non-destructive CLI contract checks; safe to run on every platform.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
COMMAND=(bash "$REPO_ROOT/scripts/macos-dmg.sh")

help_output=$("${COMMAND[@]}" --help)
grep -q '^Usage: scripts/macos-dmg.sh' <<<"$help_output"
grep -q -- '--install-dmg PATH' <<<"$help_output"

assert_failure() {
  local expected_status=$1
  local expected_message=$2
  shift 2
  local output status
  set +e
  output=$("${COMMAND[@]}" "$@" 2>&1)
  status=$?
  set -e
  if ((status != expected_status)); then
    echo "expected status $expected_status, got $status for: $*" >&2
    exit 1
  fi
  grep -q -- "$expected_message" <<<"$output"
}

assert_failure 2 'unknown option' --not-an-option
assert_failure 2 'requires a value' --install-dmg
assert_failure 2 'requires a value' --install-dmg --user
assert_failure 2 'requires a value' --destination
assert_failure 2 'requires a value' --signing-identity

if [[ $(uname -s) != Darwin ]]; then
  assert_failure 1 'must run on macOS' --build-only --skip-tool-install
fi

mount_dir=$(mktemp -d)
cleanup_output=$(
  exec 2>&1
  MOUNTED=1
  MOUNT_DIR=$mount_dir
  INSTALL_TRANSACTION_ACTIVE=1
  BACKUP_DIR=/tmp/quickrows-retained-backup
  hdiutil() { return 1; }
  restore_previous_application() { return 1; }
  # shellcheck source=../lib/macos-dmg-common.sh
  source "$REPO_ROOT/scripts/lib/macos-dmg-common.sh"
  cleanup_macos_dmg
  [[ $MOUNTED == 1 ]]
  [[ $MOUNT_DIR == "$mount_dir" ]]
  [[ -d $mount_dir ]]
)
grep -q 'unable to detach mounted DMG' <<<"$cleanup_output"
grep -q 'installation backup retained at /tmp/quickrows-retained-backup' <<<"$cleanup_output"
rm -rf "$mount_dir"

echo 'macos-dmg CLI checks passed'
