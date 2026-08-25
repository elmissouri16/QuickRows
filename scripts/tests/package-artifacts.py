#!/usr/bin/env python3
"""Focused tests for package artifact naming and archive inspection."""

from __future__ import annotations

import importlib.util
import io
import stat
import tarfile
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))
SCRIPT = SCRIPTS_DIR / "check-package-artifacts.py"
SPEC = importlib.util.spec_from_file_location("check_package_artifacts", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class PackageArtifactTests(unittest.TestCase):
    def test_version_match_is_delimiter_bounded(self) -> None:
        self.assertTrue(
            MODULE.has_bounded_version(Path("QuickRows_0.1.1_amd64.deb"), "0.1.1")
        )
        self.assertFalse(
            MODULE.has_bounded_version(Path("QuickRows_0.1.10_amd64.deb"), "0.1.1")
        )
        self.assertFalse(
            MODULE.has_bounded_version(Path("QuickRows_10.1.1_amd64.deb"), "0.1.1")
        )

    def test_windows_raw_binary_is_not_treated_as_an_installer(self) -> None:
        self.assertFalse(MODULE.matches_platform(Path("quickrows.exe"), "windows"))
        self.assertTrue(
            MODULE.matches_platform(Path("QuickRows_0.1.1_x64-setup.exe"), "windows")
        )

    def test_windows_metadata_versions_match_exact_release(self) -> None:
        self.assertTrue(MODULE.metadata_version_matches("0.1.1", "0.1.1"))
        self.assertTrue(MODULE.metadata_version_matches("0.1.1.0", "0.1.1"))
        self.assertFalse(MODULE.metadata_version_matches("0.1.10", "0.1.1"))
        self.assertFalse(MODULE.metadata_version_matches("1.0.1.1", "0.1.1"))

    def test_linux_archive_requires_an_executable_quickrows(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            valid = Path(temporary) / "QuickRows_0.1.1_x86_64.tar.gz"
            with tarfile.open(valid, "w:gz") as archive:
                info = tarfile.TarInfo("usr/bin/quickrows")
                info.mode = stat.S_IFREG | 0o755
                info.size = 4
                archive.addfile(info, io.BytesIO(b"test"))
            MODULE.validate_package(valid, "linux", "0.1.1")

            invalid = Path(temporary) / "QuickRows_0.1.1_docs.tar.gz"
            with tarfile.open(invalid, "w:gz") as archive:
                info = tarfile.TarInfo("README.md")
                info.mode = stat.S_IFREG | 0o644
                info.size = 4
                archive.addfile(info, io.BytesIO(b"test"))
            with self.assertRaises(MODULE.ArtifactValidationError):
                MODULE.validate_package(invalid, "linux", "0.1.1")

            wrong_version = Path(temporary) / "QuickRows_0.1.10_x86_64.tar.gz"
            wrong_version.write_bytes(valid.read_bytes())
            with self.assertRaisesRegex(
                MODULE.ArtifactValidationError,
                "invalid package artifacts would be uploaded",
            ):
                MODULE.validated_candidates(
                    [valid, wrong_version], "linux", "0.1.1"
                )


if __name__ == "__main__":
    unittest.main()
