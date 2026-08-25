#!/usr/bin/env bash
# Build and verification helpers for the macOS DMG command.
# This file is sourced after scripts/lib/macos-dmg-common.sh.

find_newest_dmg_since_build_started() {
  local newest=""
  local candidate
  while IFS= read -r candidate; do
    if [[ -z "$newest" || "$candidate" -nt "$newest" ]]; then
      newest=$candidate
    fi
  done < <(
    find "$REPO_ROOT/target/release" -maxdepth 1 -type f -name '*.dmg' \
      -newer "$PACKAGE_BUILD_MARKER" -print 2>/dev/null
  )
  [[ -n "$newest" ]] || return 1
  printf '%s\n' "$newest"
}

ensure_cargo_packager() {
  local installed_version
  installed_version=$(cargo packager --version 2>/dev/null || true)
  if [[ "$installed_version" == "cargo-packager $PACKAGER_VERSION" ]]; then
    return
  fi
  if ((INSTALL_PACKAGER)); then
    echo "==> Installing cargo-packager $PACKAGER_VERSION"
    cargo install cargo-packager --version "$PACKAGER_VERSION" --locked
  else
    echo "error: cargo-packager $PACKAGER_VERSION is required; run 'cargo install cargo-packager --version $PACKAGER_VERSION --locked'" >&2
    exit 1
  fi
}

create_package_config() {
  PACKAGE_CONFIG=packager.toml
  [[ -n "$SIGNING_IDENTITY" ]] || return

  if grep -q '^\[macos\]' packager.toml; then
    echo "error: packager.toml already has a [macos] table; set signingIdentity there" >&2
    exit 1
  fi

  local temporary_base escaped_identity
  temporary_base=$(mktemp "$REPO_ROOT/.packager-macos.XXXXXX")
  TEMP_PACKAGE_CONFIG="${temporary_base}.toml"
  mv "$temporary_base" "$TEMP_PACKAGE_CONFIG"
  cp packager.toml "$TEMP_PACKAGE_CONFIG"
  escaped_identity=${SIGNING_IDENTITY//\\/\\\\}
  escaped_identity=${escaped_identity//\"/\\\"}
  printf '\n[macos]\nsigningIdentity = "%s"\n' "$escaped_identity" >>"$TEMP_PACKAGE_CONFIG"
  PACKAGE_CONFIG=$TEMP_PACKAGE_CONFIG
  echo "==> Signing with $SIGNING_IDENTITY"
}

build_macos_dmg() {
  ensure_cargo_packager
  create_package_config

  echo "==> Building QuickRows release DMG"
  mkdir -p "$REPO_ROOT/target/release"
  PACKAGE_BUILD_MARKER=$(mktemp "$REPO_ROOT/target/release/.quickrows-dmg-build.XXXXXX")
  local package_status=0
  cargo packager --release --formats dmg --config "$PACKAGE_CONFIG" || package_status=$?
  if ((package_status != 0)); then
    cleanup_package_config
    return "$package_status"
  fi

  DMG_PATH=$(find_newest_dmg_since_build_started) || {
    echo "error: cargo-packager completed but produced no new DMG in target/release" >&2
    cleanup_package_config
    exit 1
  }
  cleanup_package_config
}

verify_macos_dmg_contents() {
  local mount_dir source_app status=0
  mount_dir=$(mktemp -d "${TMPDIR:-/tmp}/quickrows-dmg-verify.XXXXXX")
  if ! hdiutil attach "$DMG_PATH" -nobrowse -readonly -mountpoint "$mount_dir" >/dev/null; then
    rm -rf "$mount_dir"
    echo "error: unable to mount $(basename "$DMG_PATH") for bundle verification" >&2
    return 1
  fi
  source_app="$mount_dir/QuickRows.app"
  if [[ ! -d "$source_app" ]]; then
    source_app=$(find "$mount_dir" -maxdepth 2 -type d -name 'QuickRows.app' -print | head -n 1)
  fi
  if [[ -z "$source_app" || ! -x "$source_app/Contents/MacOS/quickrows" ]]; then
    echo "error: packaged QuickRows.app has no quickrows executable" >&2
    status=1
  fi
  if ! hdiutil detach "$mount_dir" -quiet >/dev/null; then
    echo "error: unable to detach DMG verification mount" >&2
    status=1
  fi
  rm -rf "$mount_dir"
  return "$status"
}

resolve_existing_dmg() {
  DMG_PATH=$INSTALL_ONLY_DMG
  [[ "$DMG_PATH" = /* ]] || DMG_PATH="$REPO_ROOT/$DMG_PATH"
  [[ -f "$DMG_PATH" ]] || {
    echo "error: DMG not found: $DMG_PATH" >&2
    exit 1
  }
}

prepare_macos_dmg() {
  if ((BUILD_DMG)); then
    build_macos_dmg
  else
    resolve_existing_dmg
  fi

  DMG_PATH=$(cd "$(dirname "$DMG_PATH")" && pwd)/$(basename "$DMG_PATH")
  echo "==> Verifying $(basename "$DMG_PATH")"
  hdiutil verify "$DMG_PATH" >/dev/null
  verify_macos_dmg_contents
  printf 'DMG: %s\n' "$DMG_PATH"
}
