#!/usr/bin/env python3
"""Download and decompress the SNAP web graph onto shared storage."""

from __future__ import annotations

import gzip
import os
import shutil
import urllib.request
from pathlib import Path


DATASET_URL = "https://snap.stanford.edu/data/web-BerkStan.txt.gz"
TARGET = Path("/data/web-BerkStan.txt")
EXPECTED_EDGE_COUNT = 7_600_595


def validate_dataset(path: Path) -> int:
    """Validate every non-comment edge and return its count."""

    edge_count = 0
    with path.open("rt", encoding="ascii") as source:
        for line_number, line in enumerate(source, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            columns = stripped.split()
            if len(columns) != 2:
                raise RuntimeError(
                    f"{path} line {line_number} has {len(columns)} columns; expected 2"
                )
            try:
                int(columns[0])
                int(columns[1])
            except ValueError as error:
                raise RuntimeError(
                    f"{path} line {line_number} contains a non-integer node ID"
                ) from error
            edge_count += 1

    if edge_count != EXPECTED_EDGE_COUNT:
        raise RuntimeError(
            f"{path} contains {edge_count} edges; expected {EXPECTED_EDGE_COUNT}"
        )
    return edge_count


def download() -> None:
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    temporary = TARGET.with_suffix(".txt.part")
    request = urllib.request.Request(
        DATASET_URL,
        headers={"User-Agent": "Coventry-Big-Data-Coursework/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            with gzip.GzipFile(fileobj=response) as compressed:
                with temporary.open("wb") as destination:
                    shutil.copyfileobj(compressed, destination, length=1024 * 1024)
        os.replace(temporary, TARGET)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    if TARGET.is_file() and TARGET.stat().st_size > 0:
        try:
            edge_count = validate_dataset(TARGET)
        except (OSError, RuntimeError, UnicodeError) as error:
            print(f"Existing dataset is invalid and will be replaced: {error}")
        else:
            print(
                f"Using validated dataset at {TARGET}: "
                f"{edge_count} directed edges"
            )
            return

    TARGET.unlink(missing_ok=True)
    download()
    try:
        edge_count = validate_dataset(TARGET)
    except (OSError, RuntimeError, UnicodeError):
        TARGET.unlink(missing_ok=True)
        raise
    print(f"Downloaded and validated {TARGET}: {edge_count} directed edges")


if __name__ == "__main__":
    main()
