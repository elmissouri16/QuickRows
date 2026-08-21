#!/usr/bin/env python3
"""Generate a deterministic one-million-record CSV for QuickRows stress tests."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "output",
        nargs="?",
        type=Path,
        default=Path("test-data/million-rows.csv"),
    )
    parser.add_argument("--rows", type=int, default=1_000_000)
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream, lineterminator="\r\n")
        writer.writerow(["id", "name", "category", "email", "amount", "notes"])
        for row in range(1, args.rows + 1):
            # Duplicate groups are intentional, making duplicate detection useful.
            duplicate_bucket = row // 10_000 if row % 10_000 < 4 else row
            notes = (
                f'quoted value "{row}", with comma'
                if row % 25_000 == 0
                else (f"two-line note {row}\ncontinued" if row % 40_000 == 0 else f"note {row}")
            )
            writer.writerow(
                [
                    row,
                    f"Person {row:07d}",
                    f"category-{row % 24:02d}",
                    f"user-{duplicate_bucket}@example.test",
                    f"{(row * 7919) % 1_000_000 / 100:.2f}",
                    notes,
                ]
            )

    size_mib = args.output.stat().st_size / (1024 * 1024)
    print(f"Wrote {args.rows:,} records to {args.output} ({size_mib:.1f} MiB)")


if __name__ == "__main__":
    main()
