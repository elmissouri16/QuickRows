#!/usr/bin/env bash
# Build, verify, install, and optionally launch the QuickRows macOS application.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

# shellcheck source=scripts/lib/macos-dmg-common.sh
source "$SCRIPT_DIR/lib/macos-dmg-common.sh"
# shellcheck source=scripts/package/macos-dmg-build.sh
source "$SCRIPT_DIR/package/macos-dmg-build.sh"
# shellcheck source=scripts/install/macos-app.sh
source "$SCRIPT_DIR/install/macos-app.sh"

BUILD_DMG=1
INSTALL_APP=1
LAUNCH_APP=1
INSTALL_SCOPE=system
CUSTOM_DESTINATION=""
INSTALL_ONLY_DMG=""
INSTALL_PACKAGER=1
PACKAGER_VERSION=0.11.8
SIGNING_IDENTITY=${QUICKROWS_SIGNING_IDENTITY:-}
TEMP_PACKAGE_CONFIG=""
PACKAGE_BUILD_MARKER=""
MOUNT_DIR=""
MOUNTED=0
BACKUP_DIR=""
INSTALL_TRANSACTION_ACTIVE=0

usage() {
  cat <<'USAGE'
Usage: scripts/macos-dmg.sh [options]

By default this script:
  1. installs the pinned cargo-packager if necessary;
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
  scripts/macos-dmg.sh --install-dmg target/release/QuickRows_VERSION_ARCH.dmg

Set QUICKROWS_SIGNING_IDENTITY or pass --signing-identity for a signed release.
When signing, cargo-packager notarizes automatically if APPLE_KEYCHAIN_PROFILE,
APPLE_ID/APPLE_PASSWORD/APPLE_TEAM_ID, or App Store Connect API credentials are
available in the environment.
USAGE
}

require_option_value() {
  local option=$1
  local count=$2
  local value=${3:-}
  if ((count < 2)) || [[ -z "$value" || "$value" == --* ]]; then
    echo "error: $option requires a value" >&2
    exit 2
  fi
}

while (($#)); do
  case "$1" in
    --build-only)
      INSTALL_APP=0
      LAUNCH_APP=0
      ;;
    --install-dmg)
      require_option_value "$1" "$#" "${2:-}"
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
      require_option_value "$1" "$#" "${2:-}"
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
      require_option_value "$1" "$#" "${2:-}"
      SIGNING_IDENTITY=$2
      shift
      ;;
    -h | --help)
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

trap cleanup_macos_dmg EXIT
trap 'exit 130' HUP INT TERM

require_command hdiutil
if ((BUILD_DMG)); then
  require_command cargo
fi
if ((INSTALL_APP)); then
  require_command ditto
fi
if ((LAUNCH_APP)); then
  require_command open
fi
cd "$REPO_ROOT"

prepare_macos_dmg
if ((!INSTALL_APP)); then
  echo "==> DMG release is ready"
  exit 0
fi

install_macos_app
echo "==> Done"
