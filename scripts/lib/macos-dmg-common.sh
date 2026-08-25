#!/usr/bin/env bash
# Shared lifecycle helpers for the macOS DMG command.
# This file is sourced by scripts/macos-dmg.sh.

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: required command not found: $1" >&2
    exit 1
  }
}

cleanup_package_config() {
  if [[ -n ${TEMP_PACKAGE_CONFIG:-} ]]; then
    rm -f "$TEMP_PACKAGE_CONFIG"
    TEMP_PACKAGE_CONFIG=""
  fi
  if [[ -n ${PACKAGE_BUILD_MARKER:-} ]]; then
    rm -f "$PACKAGE_BUILD_MARKER"
    PACKAGE_BUILD_MARKER=""
  fi
}

cleanup_mounted_dmg() {
  if ((${MOUNTED:-0})) && [[ -n ${MOUNT_DIR:-} ]]; then
    if hdiutil detach "$MOUNT_DIR" -quiet >/dev/null 2>&1; then
      MOUNTED=0
    else
      echo "warning: unable to detach mounted DMG at $MOUNT_DIR" >&2
      return 0
    fi
  fi
  if [[ -n ${MOUNT_DIR:-} ]]; then
    if rm -rf "$MOUNT_DIR"; then
      MOUNT_DIR=""
    else
      echo "warning: unable to remove DMG mount directory at $MOUNT_DIR" >&2
    fi
  fi
}

cleanup_install_backup() {
  if [[ -n ${BACKUP_DIR:-} ]]; then
    if declare -F run_privileged >/dev/null; then
      run_privileged rm -rf "$BACKUP_DIR"
    else
      rm -rf "$BACKUP_DIR"
    fi
    BACKUP_DIR=""
  fi
}

cleanup_macos_dmg() {
  if ((${INSTALL_TRANSACTION_ACTIVE:-0})) && declare -F restore_previous_application >/dev/null; then
    echo "warning: interrupted installation; restoring the previous application" >&2
    if ! restore_previous_application; then
      echo "warning: automatic restoration failed" >&2
    fi
  fi
  cleanup_package_config
  cleanup_mounted_dmg
  if ((${INSTALL_TRANSACTION_ACTIVE:-0})); then
    echo "warning: installation backup retained at ${BACKUP_DIR:-unknown}" >&2
  else
    cleanup_install_backup
  fi
}
