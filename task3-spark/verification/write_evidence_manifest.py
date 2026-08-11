#!/usr/bin/env python3
"""Bind Task 3 runtime artifacts to one full source commit SHA."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
from pathlib import Path


COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
LABEL_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument(
        "--artifact-root",
        action="append",
        required=True,
        metavar="LABEL=PATH",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(block)
    return checksum.hexdigest()


def parse_roots(values: list[str]) -> list[tuple[str, Path]]:
    roots: list[tuple[str, Path]] = []
    labels: set[str] = set()
    for value in values:
        label, separator, raw_path = value.partition("=")
        if not separator or not LABEL_PATTERN.fullmatch(label):
            raise ValueError(
                f"invalid --artifact-root {value!r}; expected LABEL=PATH"
            )
        if label in labels:
            raise ValueError(f"duplicate artifact-root label: {label}")
        root = Path(raw_path).resolve(strict=True)
        if not root.is_dir():
            raise ValueError(f"artifact root is not a directory: {root}")
        labels.add(label)
        roots.append((label, root))
    return roots


def main() -> None:
    args = arguments()
    commit_sha = args.commit_sha.casefold()
    if not COMMIT_PATTERN.fullmatch(commit_sha):
        raise ValueError("--commit-sha must be a full 40-character Git SHA")

    roots = parse_roots(args.artifact_root)
    output = args.output.resolve()
    files: list[dict[str, object]] = []
    logical_paths: set[str] = set()
    for label, root in roots:
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.resolve() == output:
                continue
            relative = path.relative_to(root)
            if any(part.startswith(".") for part in relative.parts):
                continue
            logical_path = f"{label}/{relative.as_posix()}"
            if logical_path in logical_paths:
                raise RuntimeError(f"duplicate logical artifact path: {logical_path}")
            logical_paths.add(logical_path)
            files.append(
                {
                    "path": logical_path,
                    "size_bytes": path.stat().st_size,
                    "sha256": digest(path),
                }
            )

    if not files:
        raise RuntimeError("no Task 3 runtime artifacts were found")

    manifest = {
        "schema_version": 1,
        "task": args.task,
        "status": "PASS",
        "source_commit": commit_sha,
        "generated_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        "artifact_root_labels": [label for label, _ in roots],
        "file_count": len(files),
        "files": files,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(
        f"Task 3 evidence manifest contains {len(files)} artifacts "
        f"bound to commit {commit_sha}"
    )


if __name__ == "__main__":
    main()
