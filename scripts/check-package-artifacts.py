#!/usr/bin/env python3
"""Validate that package smoke produced a versioned, runnable QuickRows package."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Optional

from release_version import required_toml_string


class ArtifactValidationError(RuntimeError):
    """Raised when a candidate package does not contain the expected application."""


def workspace_version() -> str:
    return required_toml_string(Path("Cargo.toml"), ("workspace", "package"), "version")


def matches_platform(path: Path, platform: str) -> bool:
    name = path.name.lower()
    if platform == "macos":
        return name.endswith((".dmg", ".app.tar.gz"))
    if platform == "windows":
        return name != "quickrows.exe" and name.endswith((".msi", ".exe"))
    if platform == "linux":
        return name.endswith((".deb", ".appimage", ".tar.gz"))
    raise SystemExit(f"unsupported platform: {platform}")


def has_bounded_version(path: Path, version: str) -> bool:
    pattern = rf"(?<![0-9A-Za-z]){re.escape(version)}(?![0-9A-Za-z])"
    return re.search(pattern, path.name) is not None


def find_extracted_executable(root: Path, windows: bool = False) -> Optional[Path]:
    expected_name = "quickrows.exe" if windows else "quickrows"
    for path in root.rglob("*"):
        if not path.is_file() or path.name.lower() != expected_name:
            continue
        if windows or path.stat().st_mode & 0o111:
            return path
    return None


def metadata_version_matches(actual: str, expected: str) -> bool:
    if not re.fullmatch(r"[0-9]+(?:\.[0-9]+){1,3}", actual):
        return False
    actual_parts = [int(part) for part in actual.split(".")]
    expected_parts = [int(part) for part in expected.split(".")]
    return (
        actual_parts[: len(expected_parts)] == expected_parts
        and all(part == 0 for part in actual_parts[len(expected_parts) :])
    )


def read_windows_installer_metadata(path: Path) -> tuple[str, str]:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        raise ArtifactValidationError("PowerShell is required to inspect installer metadata")
    environment = os.environ.copy()
    environment["QUICKROWS_PACKAGE_PATH"] = str(path.resolve())
    if path.suffix.lower() == ".msi":
        script = r'''
$installer = New-Object -ComObject WindowsInstaller.Installer
$database = $installer.GetType().InvokeMember(
    "OpenDatabase", "InvokeMethod", $null, $installer,
    @($env:QUICKROWS_PACKAGE_PATH, 0)
)
function Read-MsiProperty([string]$name) {
    $query = "SELECT ``Value`` FROM ``Property`` WHERE ``Property``='$name'"
    $view = $database.GetType().InvokeMember(
        "OpenView", "InvokeMethod", $null, $database, @($query)
    )
    $view.GetType().InvokeMember("Execute", "InvokeMethod", $null, $view, $null)
    $record = $view.GetType().InvokeMember("Fetch", "InvokeMethod", $null, $view, $null)
    if ($null -eq $record) { return "" }
    return $record.GetType().InvokeMember(
        "StringData", "GetProperty", $null, $record, @(1)
    )
}
[pscustomobject]@{
    ProductName = Read-MsiProperty "ProductName"
    ProductVersion = Read-MsiProperty "ProductVersion"
} | ConvertTo-Json -Compress
'''
    else:
        script = r'''
$version = (Get-Item -LiteralPath $env:QUICKROWS_PACKAGE_PATH).VersionInfo
[pscustomobject]@{
    ProductName = $version.ProductName
    ProductVersion = $version.ProductVersion
} | ConvertTo-Json -Compress
'''
    result = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-Command", script],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    if result.returncode != 0:
        raise ArtifactValidationError(
            f"installer metadata inspection failed with exit code {result.returncode}"
        )
    try:
        metadata = json.loads(result.stdout.strip())
    except (json.JSONDecodeError, TypeError) as error:
        raise ArtifactValidationError("installer metadata output was invalid") from error
    return str(metadata.get("ProductName") or ""), str(
        metadata.get("ProductVersion") or ""
    )


def validate_windows_package(path: Path, version: str) -> None:
    product_name, product_version = read_windows_installer_metadata(path)
    if product_name.lower() != "quickrows":
        raise ArtifactValidationError(
            f"installer ProductName is {product_name or 'missing'}, expected QuickRows"
        )
    if not metadata_version_matches(product_version, version):
        raise ArtifactValidationError(
            f"installer ProductVersion is {product_version or 'missing'}, expected {version}"
        )
    with tempfile.TemporaryDirectory(prefix="quickrows-package-") as temporary:
        destination = Path(temporary)
        if path.suffix.lower() == ".msi":
            result = subprocess.run(
                [
                    "msiexec",
                    "/a",
                    str(path.resolve()),
                    "/qn",
                    f"TARGETDIR={destination.resolve()}",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode not in (0, 3010):
                raise ArtifactValidationError(
                    f"msiexec extraction failed with exit code {result.returncode}"
                )
        else:
            seven_zip = shutil.which("7z") or shutil.which("7z.exe")
            if seven_zip is None:
                raise ArtifactValidationError("7-Zip is required to inspect the NSIS package")
            result = subprocess.run(
                [seven_zip, "x", "-y", f"-o{destination}", str(path.resolve())],
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise ArtifactValidationError(
                    f"7-Zip extraction failed with exit code {result.returncode}"
                )
        if find_extracted_executable(destination, windows=True) is None:
            raise ArtifactValidationError("package does not contain quickrows.exe")


def validate_debian_package(path: Path, version: str) -> None:
    metadata = subprocess.run(
        ["dpkg-deb", "--field", str(path), "Package", "Version"],
        check=False,
        capture_output=True,
        text=True,
    )
    if metadata.returncode != 0:
        raise ArtifactValidationError(
            f"dpkg-deb metadata inspection failed with exit code {metadata.returncode}"
        )
    fields: dict[str, str] = {}
    for line in metadata.stdout.splitlines():
        key, separator, value = line.partition(":")
        if separator:
            fields[key.strip().lower()] = value.strip()
    if fields.get("package", "").lower() != "quickrows":
        raise ArtifactValidationError("Debian Package metadata is not quickrows")
    if fields.get("version") != version:
        raise ArtifactValidationError(
            f"Debian Version metadata is {fields.get('version', 'missing')}, expected {version}"
        )
    with tempfile.TemporaryDirectory(prefix="quickrows-package-") as temporary:
        result = subprocess.run(
            ["dpkg-deb", "--extract", str(path), temporary],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise ArtifactValidationError(
                f"dpkg-deb extraction failed with exit code {result.returncode}"
            )
        if find_extracted_executable(Path(temporary)) is None:
            raise ArtifactValidationError("Debian package does not contain executable quickrows")


def validate_appimage(path: Path) -> None:
    if not path.stat().st_mode & 0o111:
        raise ArtifactValidationError("AppImage is not executable")
    with tempfile.TemporaryDirectory(prefix="quickrows-package-") as temporary:
        result = subprocess.run(
            [str(path.resolve()), "--appimage-extract"],
            cwd=temporary,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise ArtifactValidationError(
                f"AppImage extraction failed with exit code {result.returncode}"
            )
        if find_extracted_executable(Path(temporary) / "squashfs-root") is None:
            raise ArtifactValidationError("AppImage does not contain executable quickrows")


def validate_linux_archive(path: Path) -> None:
    try:
        with tarfile.open(path, "r:gz") as archive:
            executable = next(
                (
                    member
                    for member in archive.getmembers()
                    if member.isfile()
                    and Path(member.name).name.lower() == "quickrows"
                    and member.mode & 0o111
                ),
                None,
            )
    except (OSError, tarfile.TarError) as error:
        raise ArtifactValidationError(f"unable to inspect tar archive: {error}") from error
    if executable is None:
        raise ArtifactValidationError("archive does not contain executable quickrows")


def validate_package(path: Path, platform: str, version: str) -> None:
    if path.stat().st_size == 0:
        raise ArtifactValidationError("artifact is empty")
    if "quickrows" not in path.name.lower():
        raise ArtifactValidationError("artifact filename does not identify QuickRows")
    if not has_bounded_version(path, version):
        raise ArtifactValidationError(f"artifact filename does not contain exact version {version}")
    if platform == "windows":
        validate_windows_package(path, version)
    elif platform == "linux":
        name = path.name.lower()
        if name.endswith(".deb"):
            validate_debian_package(path, version)
        elif name.endswith(".appimage"):
            validate_appimage(path)
        else:
            validate_linux_archive(path)


def validated_candidates(
    candidates: list[Path], platform: str, version: str
) -> list[Path]:
    valid = []
    rejected = []
    for path in candidates:
        try:
            validate_package(path, platform, version)
        except ArtifactValidationError as error:
            rejected.append(f"{path.name}: {error}")
        else:
            valid.append(path)
    if rejected:
        found = "; ".join(sorted(rejected))
        raise ArtifactValidationError(
            f"invalid package artifacts would be uploaded: {found}"
        )
    if not valid:
        raise ArtifactValidationError(f"no QuickRows {version} package candidates")
    return valid


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--os", required=True, choices=("macOS", "Windows", "Linux"))
    parser.add_argument("--target-dir", type=Path, default=Path("target/release"))
    args = parser.parse_args()

    version = workspace_version()
    platform = args.os.lower()
    if not args.target_dir.is_dir():
        raise SystemExit(f"package directory does not exist: {args.target_dir}")
    candidates = [
        path
        for path in args.target_dir.iterdir()
        if path.is_file() and matches_platform(path, platform)
    ]
    try:
        valid = validated_candidates(candidates, platform, version)
    except ArtifactValidationError as error:
        raise SystemExit(f"{args.os} package validation failed: {error}") from error
    print("Validated package artifacts: " + ", ".join(sorted(path.name for path in valid)))


if __name__ == "__main__":
    main()
