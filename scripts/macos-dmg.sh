#!/usr/bin/env bash
# Build a release DMG for QuickRows and install QuickRows.app on macOS.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

BUILD_DMG=1
INSTALL_APP=1
LAUNCH_APP=1
INSTALL_SCOPE=system
CUSTOM_DESTINATION=""
INSTALL_ONLY_DMG=""
INSTALL_PACKAGER=1
SIGNING_IDENTITY=${QUICKROWS_SIGNING_IDENTITY:-}

usage() {
  cat <<'EOF'
Usage: scripts/macos-dmg.sh [options]

By default this script:
  1. installs cargo-packager if it is missing;
  2. builds QuickRows with the release profile;
  3. creates and verifies a DMG;
  4. installs QuickRows.app in /Applications; and
  5. launches QuickRows.

Options:
  --build-only          Build and verify the DMG without installing it.
  --install-dmg PATH    Skip the build and install QuickRows from PATH.
  --user                Install in ~/Applications without sudo.
  --system              Install in /Applications (default; may request sudo).
  --destination DIR     Install in a custom application directory.
  --no-launch           Do not launch QuickRows after installation.
  --skip-tool-install   Fail instead of installing cargo-packager when missing.
  --signing-identity ID Sign with an installed Developer ID Application identity.
  -h, --help            Show this help.

Examples:
  scripts/macos-dmg.sh
  scripts/macos-dmg.sh --build-only
  scripts/macos-dmg.sh --user --no-launch
  scripts/macos-dmg.sh --install-dmg target/release/QuickRows_0.1.1_aarch64.dmg

Set QUICKROWS_SIGNING_IDENTITY or pass --signing-identity for a signed release.
When signing, cargo-packager notarizes automatically if APPLE_KEYCHAIN_PROFILE,
APPLE_ID/APPLE_PASSWORD/APPLE_TEAM_ID, or App Store Connect API credentials are
available in the environment.
EOF
}

while (($#)); do
  case "$1" in
    --build-only)
      INSTALL_APP=0
      LAUNCH_APP=0
      ;;
    --install-dmg)
      [[ $# -ge 2 ]] || { echo "error: --install-dmg requires a path" >&2; exit 2; }
      BUILD_DMG=0
      INSTALL_ONLY_DMG=$2
      shift
      ;;
    --user)
      INSTALL_SCOPE=user
      ;;
    --system)
      INSTALL_SCOPE=system
      ;;
    --destination)
      [[ $# -ge 2 ]] || { echo "error: --destination requires a directory" >&2; exit 2; }
      INSTALL_SCOPE=custom
      CUSTOM_DESTINATION=$2
      shift
      ;;
    --no-launch)
      LAUNCH_APP=0
      ;;
    --skip-tool-install)
      INSTALL_PACKAGER=0
      ;;
    --signing-identity)
      [[ $# -ge 2 ]] || { echo "error: --signing-identity requires an identity" >&2; exit 2; }
      SIGNING_IDENTITY=$2
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ $(uname -s) != Darwin ]]; then
  echo "error: DMG creation and installation must run on macOS" >&2
  exit 1
fi

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: required command not found: $1" >&2
    exit 1
  }
}

require_command cargo
require_command hdiutil
require_command ditto
require_command open

cd "$REPO_ROOT"

find_newest_dmg() {
  local newest=""
  local candidate
  while IFS= read -r candidate; do
    if [[ -z "$newest" || "$candidate" -nt "$newest" ]]; then
      newest=$candidate
    fi
  done < <(find "$REPO_ROOT/target/release" -maxdepth 1 -type f -name '*.dmg' -print 2>/dev/null)
  [[ -n "$newest" ]] || return 1
  printf '%s\n' "$newest"
}

if ((BUILD_DMG)); then
  if ! cargo packager --version >/dev/null 2>&1; then
    if ((INSTALL_PACKAGER)); then
      echo "==> Installing cargo-packager"
      cargo install cargo-packager --locked
    else
      echo "error: cargo-packager is missing; run 'cargo install cargo-packager --locked'" >&2
      exit 1
    fi
  fi

  PACKAGE_CONFIG=packager.toml
  TEMP_PACKAGE_CONFIG=""
  if [[ -n "$SIGNING_IDENTITY" ]]; then
    if grep -q '^\[macos\]' packager.toml; then
      echo "error: packager.toml already has a [macos] table; set signingIdentity there" >&2
      exit 1
    fi
    TEMP_PACKAGE_BASE=$(mktemp "$REPO_ROOT/.packager-macos.XXXXXX")
    TEMP_PACKAGE_CONFIG="${TEMP_PACKAGE_BASE}.toml"
    mv "$TEMP_PACKAGE_BASE" "$TEMP_PACKAGE_CONFIG"
    cp packager.toml "$TEMP_PACKAGE_CONFIG"
    ESCAPED_IDENTITY=${SIGNING_IDENTITY//\\/\\\\}
    ESCAPED_IDENTITY=${ESCAPED_IDENTITY//\"/\\\"}
    printf '\n[macos]\nsigningIdentity = "%s"\n' "$ESCAPED_IDENTITY" >>"$TEMP_PACKAGE_CONFIG"
    PACKAGE_CONFIG=$TEMP_PACKAGE_CONFIG
    echo "==> Signing with $SIGNING_IDENTITY"
  fi

  echo "==> Building QuickRows release DMG"
  PACKAGE_STATUS=0
  cargo packager --release --formats dmg --config "$PACKAGE_CONFIG" || PACKAGE_STATUS=$?
  [[ -z "$TEMP_PACKAGE_CONFIG" ]] || rm -f "$TEMP_PACKAGE_CONFIG"
  if ((PACKAGE_STATUS != 0)); then
    exit "$PACKAGE_STATUS"
  fi
  DMG_PATH=$(find_newest_dmg) || {
    echo "error: cargo-packager completed but no DMG was found in target/release" >&2
    exit 1
  }
else
  DMG_PATH=$INSTALL_ONLY_DMG
  [[ "$DMG_PATH" = /* ]] || DMG_PATH="$REPO_ROOT/$DMG_PATH"
  [[ -f "$DMG_PATH" ]] || { echo "error: DMG not found: $DMG_PATH" >&2; exit 1; }
fi

DMG_PATH=$(cd "$(dirname "$DMG_PATH")" && pwd)/$(basename "$DMG_PATH")

echo "==> Verifying $(basename "$DMG_PATH")"
hdiutil verify "$DMG_PATH" >/dev/null
printf 'DMG: %s\n' "$DMG_PATH"

if ((!INSTALL_APP)); then
  echo "==> DMG release is ready"
  exit 0
fi

MOUNT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/quickrows-dmg.XXXXXX")
MOUNTED=0
cleanup() {
  if ((MOUNTED)); then
    hdiutil detach "$MOUNT_DIR" -quiet >/dev/null 2>&1 || true
  fi
  rm -rf "$MOUNT_DIR"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

echo "==> Mounting DMG"
hdiutil attach "$DMG_PATH" -nobrowse -readonly -mountpoint "$MOUNT_DIR" >/dev/null
MOUNTED=1

SOURCE_APP="$MOUNT_DIR/QuickRows.app"
if [[ ! -d "$SOURCE_APP" ]]; then
  SOURCE_APP=$(find "$MOUNT_DIR" -maxdepth 2 -type d -name 'QuickRows.app' -print | head -n 1)
fi
[[ -n "$SOURCE_APP" && -d "$SOURCE_APP" ]] || {
  echo "error: QuickRows.app was not found in the DMG" >&2
  exit 1
}
[[ -x "$SOURCE_APP/Contents/MacOS/quickrows" ]] || {
  echo "error: the QuickRows application bundle has no executable" >&2
  exit 1
}

if command -v codesign >/dev/null 2>&1; then
  if codesign --verify --deep --strict "$SOURCE_APP" >/dev/null 2>&1; then
    echo "==> Application signature structure is valid"
  else
    echo "warning: application is unsigned or its signature is invalid" >&2
    echo "warning: use an Apple Developer ID and notarization before public distribution" >&2
  fi
fi

if [[ "$INSTALL_SCOPE" == user ]]; then
  DESTINATION_DIR="$HOME/Applications"
  mkdir -p "$DESTINATION_DIR"
  USE_SUDO=0
elif [[ "$INSTALL_SCOPE" == custom ]]; then
  DESTINATION_DIR=$CUSTOM_DESTINATION
  [[ "$DESTINATION_DIR" = /* ]] || DESTINATION_DIR="$REPO_ROOT/$DESTINATION_DIR"
  mkdir -p "$DESTINATION_DIR"
  DESTINATION_DIR=$(cd "$DESTINATION_DIR" && pwd)
  USE_SUDO=0
else
  DESTINATION_DIR=/Applications
  if [[ -w "$DESTINATION_DIR" ]]; then
    USE_SUDO=0
  else
    require_command sudo
    USE_SUDO=1
  fi
fi

run_privileged() {
  if ((USE_SUDO)); then
    sudo "$@"
  else
    "$@"
  fi
}

DESTINATION_APP="$DESTINATION_DIR/QuickRows.app"

# Ask a running copy to exit before replacing its bundle. Ignore failures when
# QuickRows is not running or automation permission is unavailable.
osascript -e 'tell application "QuickRows" to quit' >/dev/null 2>&1 || true
sleep 1

echo "==> Installing QuickRows.app in $DESTINATION_DIR"
BACKUP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/quickrows-backup.XXXXXX")
BACKUP_APP="$BACKUP_DIR/QuickRows.app"
if [[ -e "$DESTINATION_APP" ]]; then
  echo "==> Backing up the existing application"
  ditto --rsrc --extattr "$DESTINATION_APP" "$BACKUP_APP"
fi

run_privileged rm -rf "$DESTINATION_APP"
if ! run_privileged ditto --rsrc --extattr "$SOURCE_APP" "$DESTINATION_APP"; then
  echo "error: installation failed" >&2
  if [[ -d "$BACKUP_APP" ]]; then
    echo "==> Restoring the previous application"
    run_privileged rm -rf "$DESTINATION_APP"
    run_privileged ditto --rsrc --extattr "$BACKUP_APP" "$DESTINATION_APP"
  fi
  exit 1
fi
if [[ ! -x "$DESTINATION_APP/Contents/MacOS/quickrows" ]]; then
  echo "error: installed application is missing its executable" >&2
  if [[ -d "$BACKUP_APP" ]]; then
    echo "==> Restoring the previous application"
    run_privileged rm -rf "$DESTINATION_APP"
    run_privileged ditto --rsrc --extattr "$BACKUP_APP" "$DESTINATION_APP"
  fi
  exit 1
fi
rm -rf "$BACKUP_DIR"

echo "==> Installed $DESTINATION_APP"

hdiutil detach "$MOUNT_DIR" -quiet
MOUNTED=0
rm -rf "$MOUNT_DIR"

if ((LAUNCH_APP)); then
  echo "==> Launching QuickRows"
  open "$DESTINATION_APP"
fi

echo "==> Done"
