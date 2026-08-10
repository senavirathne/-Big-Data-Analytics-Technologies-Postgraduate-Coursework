#!/usr/bin/env python3
"""Download and validate the uncompressed SNAP web graph on shared storage."""

from __future__ import annotations

import gzip
import os
import shutil
import urllib.request
from pathlib import Path


DATASET_URL = "https://snap.stanford.edu/data/web-BerkStan.txt.gz"
TARGET = Path("/data/web-BerkStan.txt")
EXPECTED_EDGE_ROWS = 7_600_595


def count_valid_edges(path: Path) -> int:
    count = 0
    with path.open("rt", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            fields = stripped.split()
            if len(fields) != 2:
                raise ValueError(f"Malformed edge at line {line_number}: {stripped!r}")
            int(fields[0])
            int(fields[1])
            count += 1
    return count


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
    edge_rows = -1
    if TARGET.exists():
        try:
            edge_rows = count_valid_edges(TARGET)
        except (OSError, UnicodeError, ValueError):
            edge_rows = -1
        if edge_rows == EXPECTED_EDGE_ROWS:
            print(f"Validated {TARGET}: {edge_rows:,} edge rows")
            return
        TARGET.unlink(missing_ok=True)

    download()
    edge_rows = count_valid_edges(TARGET)
    if edge_rows != EXPECTED_EDGE_ROWS:
        TARGET.unlink(missing_ok=True)
        raise RuntimeError(
            f"Dataset validation failed: expected {EXPECTED_EDGE_ROWS:,} edge rows, "
            f"found {edge_rows:,}"
        )
    print(f"Validated {TARGET}: {edge_rows:,} edge rows")


if __name__ == "__main__":
    main()
