#!/usr/bin/env bash
set -euo pipefail

PACKAGING=0
if [[ ${1:-} == "--packaging" ]]; then
  PACKAGING=1
elif (($#)); then
  echo "usage: $0 [--packaging]" >&2
  exit 2
fi

packages=(
  libxkbcommon-dev libxkbcommon-x11-dev libwayland-dev libx11-xcb-dev
  libxcb1-dev libxcb-render0-dev libxcb-shape0-dev libxcb-xfixes0-dev
  libvulkan-dev libfontconfig-dev libasound2-dev libssl-dev
)
if ((PACKAGING)); then
  packages+=(libgtk-3-dev)
fi

sudo apt-get update
sudo apt-get install -y "${packages[@]}"
