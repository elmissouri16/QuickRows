#!/usr/bin/env bash
# Application mount and transactional installation helpers for macOS.
# This file is sourced after scripts/lib/macos-dmg-common.sh.

mount_quickrows_dmg() {
  MOUNT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/quickrows-dmg.XXXXXX")
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
}

verify_application_signature() {
  command -v codesign >/dev/null 2>&1 || return
  if codesign --verify --deep --strict "$SOURCE_APP" >/dev/null 2>&1; then
    echo "==> Application signature structure is valid"
  else
    echo "warning: application is unsigned or its signature is invalid" >&2
    echo "warning: use an Apple Developer ID and notarization before public distribution" >&2
  fi
}

resolve_install_destination() {
  case "$INSTALL_SCOPE" in
    user)
      DESTINATION_DIR="$HOME/Applications"
      mkdir -p "$DESTINATION_DIR"
      USE_SUDO=0
      ;;
    custom)
      DESTINATION_DIR=$CUSTOM_DESTINATION
      [[ "$DESTINATION_DIR" = /* ]] || DESTINATION_DIR="$REPO_ROOT/$DESTINATION_DIR"
      mkdir -p "$DESTINATION_DIR"
      DESTINATION_DIR=$(cd "$DESTINATION_DIR" && pwd)
      USE_SUDO=0
      ;;
    system)
      DESTINATION_DIR=/Applications
      if [[ -w "$DESTINATION_DIR" ]]; then
        USE_SUDO=0
      else
        require_command sudo
        USE_SUDO=1
      fi
      ;;
    *)
      echo "error: unsupported installation scope: $INSTALL_SCOPE" >&2
      exit 1
      ;;
  esac
  DESTINATION_APP="$DESTINATION_DIR/QuickRows.app"
}

run_privileged() {
  if ((USE_SUDO)); then
    sudo "$@"
  else
    "$@"
  fi
}

restore_previous_application() {
  if ! run_privileged rm -rf "$DESTINATION_APP"; then
    echo "error: unable to remove the failed installation" >&2
    return 1
  fi
  if [[ -d "$BACKUP_APP" ]]; then
    echo "==> Restoring the previous application"
    if ! run_privileged ditto --rsrc --extattr "$BACKUP_APP" "$DESTINATION_APP"; then
      echo "error: unable to restore the previous application" >&2
      return 1
    fi
  fi
  INSTALL_TRANSACTION_ACTIVE=0
}

copy_application_transactionally() {
  # Ask a running copy to exit before replacing its bundle. Ignore failures when
  # QuickRows is not running or automation permission is unavailable.
  osascript -e 'tell application "QuickRows" to quit' >/dev/null 2>&1 || true
  sleep 1

  echo "==> Installing QuickRows.app in $DESTINATION_DIR"
  BACKUP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/quickrows-backup.XXXXXX")
  BACKUP_APP="$BACKUP_DIR/QuickRows.app"
  if [[ -e "$DESTINATION_APP" ]]; then
    echo "==> Backing up the existing application"
    run_privileged ditto --rsrc --extattr "$DESTINATION_APP" "$BACKUP_APP"
  fi

  INSTALL_TRANSACTION_ACTIVE=1
  run_privileged rm -rf "$DESTINATION_APP"
  if ! run_privileged ditto --rsrc --extattr "$SOURCE_APP" "$DESTINATION_APP"; then
    echo "error: installation failed" >&2
    restore_previous_application
    exit 1
  fi
  if [[ ! -x "$DESTINATION_APP/Contents/MacOS/quickrows" ]]; then
    echo "error: installed application is missing its executable" >&2
    restore_previous_application
    exit 1
  fi
  echo "==> Installed $DESTINATION_APP"
}

commit_application_install() {
  INSTALL_TRANSACTION_ACTIVE=0
  cleanup_install_backup
}

install_macos_app() {
  mount_quickrows_dmg
  verify_application_signature
  resolve_install_destination
  copy_application_transactionally
  cleanup_mounted_dmg

  if ((LAUNCH_APP)); then
    echo "==> Launching QuickRows"
    if ! open "$DESTINATION_APP"; then
      echo "error: unable to launch the installed application; restoring the previous version" >&2
      restore_previous_application
      return 1
    fi
  fi
  commit_application_install
}
