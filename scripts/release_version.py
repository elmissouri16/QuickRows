"""Small compatibility layer for reading exact TOML version keys."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import tomllib  # type: ignore[import-not-found]
except ModuleNotFoundError:  # Python < 3.11 (for local developer tooling)
    tomllib = None  # type: ignore[assignment]


def required_toml_string(path: Path, table_keys: tuple[str, ...], key: str) -> str:
    """Read one exact TOML string key without searching across table boundaries."""
    if tomllib is not None:
        try:
            with path.open("rb") as source:
                value: Any = tomllib.load(source)
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise SystemExit(f"unable to read {path}: {error}") from error
        for table_key in table_keys:
            if not isinstance(value, dict) or table_key not in value:
                dotted = ".".join((*table_keys, key))
                raise SystemExit(f"missing {dotted} in {path}")
            value = value[table_key]
        if not isinstance(value, dict) or key not in value:
            dotted = ".".join((*table_keys, key))
            raise SystemExit(f"missing {dotted} in {path}")
        value = value[key]
    else:
        value = _read_simple_string_for_legacy_python(path, table_keys, key)

    if not isinstance(value, str) or not value:
        dotted = ".".join((*table_keys, key))
        raise SystemExit(f"{dotted} in {path} must be a non-empty string")
    return value


def _read_simple_string_for_legacy_python(
    path: Path, table_keys: tuple[str, ...], key: str
) -> str:
    """Parse the simple string assignment used by these manifests on Python 3.9/3.10."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise SystemExit(f"unable to read {path}: {error}") from error

    current_table: tuple[str, ...] = ()
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            current_table = tuple(part.strip() for part in line[1:-1].split("."))
            continue
        if current_table != table_keys or "=" not in line:
            continue
        candidate, encoded = line.split("=", 1)
        if candidate.strip() != key:
            continue
        try:
            value, _ = json.JSONDecoder().raw_decode(encoded.lstrip())
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            dotted = ".".join((*table_keys, key))
            raise SystemExit(f"unable to parse {dotted} in {path}: {error}") from error
        if not isinstance(value, str):
            dotted = ".".join((*table_keys, key))
            raise SystemExit(f"{dotted} in {path} must be a string")
        return value

    dotted = ".".join((*table_keys, key))
    raise SystemExit(f"missing {dotted} in {path}")
