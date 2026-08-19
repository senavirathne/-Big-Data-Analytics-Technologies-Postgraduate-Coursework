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
        print(f"Using existing uncompressed dataset at {TARGET}")
        return

    TARGET.unlink(missing_ok=True)
    download()
    if not TARGET.is_file() or TARGET.stat().st_size == 0:
        TARGET.unlink(missing_ok=True)
        raise RuntimeError("The downloaded dataset is empty")
    print(f"Downloaded and decompressed {TARGET}")


if __name__ == "__main__":
    main()
