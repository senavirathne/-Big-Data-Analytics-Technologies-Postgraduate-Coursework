#!/usr/bin/env python3
"""Stream the first exactly 5,000 SNAP patent citation paths into a CSV."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import os
import urllib.request
from pathlib import Path


SOURCE_URL = "https://snap.stanford.edu/data/cit-Patents.txt.gz"
OUTPUT = Path("/import/patent_edges_5000.csv")
EDGE_LIMIT = 5_000
EXPECTED_OUTPUT_SHA256 = (
    "3c913ccdf8bfbfd2529343dc32dc861478ba12c22a264df87ace78cc772f72ba"
)


def validate(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        with path.open("r", newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames != ["source", "target"]:
                return False
            count = 0
            for row in reader:
                int(row["source"])
                int(row["target"])
                count += 1
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        return count == EDGE_LIMIT and digest == EXPECTED_OUTPUT_SHA256
    except (OSError, TypeError, ValueError):
        return False


def extract() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT.with_suffix(".csv.part")
    request = urllib.request.Request(
        SOURCE_URL,
        headers={"User-Agent": "Coventry-Big-Data-Coursework/1.0"},
    )
    count = 0
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            with gzip.GzipFile(fileobj=response) as compressed:
                with io.TextIOWrapper(compressed, encoding="utf-8") as source:
                    with temporary.open("w", newline="", encoding="utf-8") as destination:
                        writer = csv.writer(destination)
                        writer.writerow(["source", "target"])
                        for line_number, line in enumerate(source, start=1):
                            stripped = line.strip()
                            if not stripped or stripped.startswith("#"):
                                continue
                            fields = stripped.split()
                            if len(fields) != 2:
                                raise ValueError(
                                    f"Malformed citation at source line {line_number}: {stripped!r}"
                                )
                            source_id, target_id = (int(value) for value in fields)
                            writer.writerow([source_id, target_id])
                            count += 1
                            if count == EDGE_LIMIT:
                                break
        if count != EDGE_LIMIT:
            raise RuntimeError(
                f"Source ended after {count} paths; expected exactly {EDGE_LIMIT}"
            )
        os.replace(temporary, OUTPUT)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    if not validate(OUTPUT):
        OUTPUT.unlink(missing_ok=True)
        extract()
    if not validate(OUTPUT):
        raise RuntimeError(
            "Prepared CSV did not match the deterministic first 5,000 SNAP citation paths"
        )
    print(
        f"Validated {OUTPUT}: exactly {EDGE_LIMIT} directed citation paths "
        f"(SHA-256 {EXPECTED_OUTPUT_SHA256})"
    )


if __name__ == "__main__":
    main()
