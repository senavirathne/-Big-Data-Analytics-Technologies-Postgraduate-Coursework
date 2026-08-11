#!/usr/bin/env python3
"""Create a commit-bound, content-addressed manifest for coursework evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
from pathlib import Path
from typing import NamedTuple


COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class LabeledPath(NamedTuple):
    label: str
    path: Path


def labeled_path(value: str) -> LabeledPath:
    if "=" not in value:
        raise argparse.ArgumentTypeError("expected LABEL=PATH")
    label, raw_path = value.split("=", 1)
    label = label.strip()
    if (
        not label
        or label.startswith("/")
        or any(part in {"", ".", ".."} for part in Path(label).parts)
    ):
        raise argparse.ArgumentTypeError(
            "source label must be a non-empty relative path without '.' or '..'"
        )
    if not raw_path:
        raise argparse.ArgumentTypeError("source path must not be empty")
    return LabeledPath(label, Path(raw_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--source-file",
        action="append",
        default=[],
        type=labeled_path,
        metavar="LABEL=PATH",
        help="Record a source input and its SHA-256 hash; may be repeated.",
    )
    parser.add_argument(
        "--status",
        choices=("PASS", "FAIL"),
        help="Validation status represented by this evidence bundle.",
    )
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    args = parse_args()
    commit_sha = args.commit_sha.casefold()
    if not COMMIT_PATTERN.fullmatch(commit_sha):
        raise ValueError("--commit-sha must be a full 40-character lowercase Git SHA")

    evidence_directory = args.evidence_dir.resolve(strict=True)
    output = args.output.resolve()
    if evidence_directory not in output.parents:
        raise ValueError("--output must be located inside --evidence-dir")

    files = []
    for path in sorted(evidence_directory.rglob("*")):
        if not path.is_file() or path.resolve() == output:
            continue
        relative = path.relative_to(evidence_directory).as_posix()
        files.append(
            {
                "path": relative,
                "size_bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
        )
    source_files = []
    seen_labels: set[str] = set()
    for source in args.source_file:
        if source.label in seen_labels:
            raise ValueError(f"duplicate --source-file label: {source.label}")
        seen_labels.add(source.label)
        path = source.path.resolve(strict=True)
        if not path.is_file():
            raise ValueError(f"source path is not a regular file: {source.path}")
        source_files.append(
            {
                "path": source.label,
                "size_bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
        )
    source_files.sort(key=lambda item: item["path"])

    if not files and not source_files:
        raise RuntimeError("no evidence or source files were found to manifest")

    manifest = {
        "schema_version": 2,
        "task": args.task,
        "source_commit": commit_sha,
        "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        "status": args.status,
        "file_count": len(files),
        "files": files,
        "source_file_count": len(source_files),
        "source_files": source_files,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(
        f"Evidence manifest written to {output}: "
        f"{len(files)} files bound to {commit_sha}"
    )


if __name__ == "__main__":
    main()
