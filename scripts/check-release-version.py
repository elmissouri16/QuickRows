#!/usr/bin/env python3
"""Verify that Cargo, packaging, and an optional release tag use one version."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from release_version import required_toml_string


def workspace_version(path: Path) -> str:
    return required_toml_string(path, ("workspace", "package"), "version")


def package_version(path: Path) -> str:
    return required_toml_string(path, (), "version")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", help="release tag to validate")
    args = parser.parse_args()

    cargo = workspace_version(Path("Cargo.toml"))
    package = package_version(Path("packager.toml"))
    if cargo != package:
        raise SystemExit(f"version mismatch: Cargo.toml={cargo}, packager.toml={package}")

    tag = args.tag
    if tag is None and os.environ.get("GITHUB_REF_TYPE") == "tag":
        tag = os.environ.get("GITHUB_REF_NAME")
    if tag is not None and tag != f"v{cargo}":
        raise SystemExit(f"tag {tag} does not match workspace version {cargo}")
    print(f"QuickRows version {cargo} is consistent")


if __name__ == "__main__":
    main()
