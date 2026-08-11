#!/usr/bin/env python3
"""Snapshot Task 4 sources and bind the evidence run to the checked-out commit."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path


SOURCE_FILES = {
    "task4-neo4j/cypher/analysis_queries.cypher": "analysis_queries.cypher",
    "task4-neo4j/cypher/import_patents.cypher": "import_patents.cypher",
    "task4-neo4j/scripts/prepare_patents.py": "prepare_patents.py",
    "task4-neo4j/scripts/run_import.sh": "run_import.sh",
    "task4-neo4j/scripts/run_analysis.sh": "run_analysis.sh",
    "task4-neo4j/scripts/capture_browser_evidence.sh": "capture_browser_evidence.sh",
    "task4-neo4j/scripts/stage_evidence.py": "stage_evidence.py",
    "task4-neo4j/browser-evidence.Dockerfile": "browser-evidence.Dockerfile",
    "task4-neo4j/docker-compose.yml": "docker-compose.yml",
    ".github/scripts/run_task4_neo4j_ci.sh": "run_task4_neo4j_ci.sh",
    ".github/scripts/validate_task4_evidence.py": "validate_task4_evidence.py",
    ".github/workflows/tasks-4-5.yml": "tasks-4-5.yml",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--evidence-directory", type=Path, required=True)
    return parser.parse_args()


def resolve_head(repository: Path) -> str | None:
    git_entry = repository / ".git"
    if not git_entry.is_dir():
        return None

    head = (git_entry / "HEAD").read_text(encoding="utf-8").strip()
    if re.fullmatch(r"[0-9a-fA-F]{40}", head):
        return head.lower()
    if not head.startswith("ref: "):
        return None

    reference = head.removeprefix("ref: ")
    loose_reference = git_entry / reference
    if loose_reference.is_file():
        value = loose_reference.read_text(encoding="utf-8").strip()
        return value.lower() if re.fullmatch(r"[0-9a-fA-F]{40}", value) else None

    packed_references = git_entry / "packed-refs"
    if packed_references.is_file():
        for line in packed_references.read_text(encoding="utf-8").splitlines():
            if not line or line.startswith(("#", "^")):
                continue
            value, name = line.split(" ", maxsplit=1)
            if name == reference and re.fullmatch(r"[0-9a-fA-F]{40}", value):
                return value.lower()
    return None


def main() -> None:
    args = parse_args()
    repository = args.repository.resolve()
    evidence = args.evidence_directory.resolve()
    evidence.mkdir(parents=True, exist_ok=True)

    resolved_commit = resolve_head(repository)
    supplied_commit = os.environ.get("COURSEWORK_COMMIT_SHA", "").strip().lower()
    if supplied_commit and not re.fullmatch(r"[0-9a-f]{40}", supplied_commit):
        raise RuntimeError("COURSEWORK_COMMIT_SHA must be a complete 40-character commit SHA")
    if supplied_commit and resolved_commit and supplied_commit != resolved_commit:
        raise RuntimeError(
            f"Supplied commit {supplied_commit} does not match checked-out HEAD {resolved_commit}"
        )
    commit = supplied_commit or resolved_commit
    if not commit:
        raise RuntimeError("Could not resolve a complete commit SHA for this evidence run")

    for source_relative, evidence_name in SOURCE_FILES.items():
        source = repository / source_relative
        if not source.is_file():
            raise FileNotFoundError(f"Required Task 4 source is missing: {source_relative}")
        shutil.copyfile(source, evidence / evidence_name)

    revision = {
        "schema_version": 1,
        "source_commit": commit,
        "checked_out_head": resolved_commit,
        "ci_supplied_commit": supplied_commit or None,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    (evidence / "source-revision.json").write_text(
        json.dumps(revision, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Task 4 source snapshot staged for commit {commit}")


if __name__ == "__main__":
    main()
