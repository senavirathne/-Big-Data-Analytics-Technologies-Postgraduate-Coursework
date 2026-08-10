#!/usr/bin/env python3
"""Validate the Task 4 evidence generated on a GitHub-hosted runner."""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import struct
from pathlib import Path


EDGE_LIMIT = 5_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-directory", type=Path, required=True)
    parser.add_argument("--evidence-directory", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    checks: list[tuple[str, bool, str]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append((name, passed, detail))

    query_source = (args.task_directory / "cypher/analysis_queries.cypher").read_text(
        encoding="utf-8"
    )
    profile_count = len(re.findall(r"(?m)^\s*PROFILE\s*$", query_source))
    check(
        "Exactly three PROFILE analyses",
        profile_count == 3,
        f"Found {profile_count} PROFILE statements.",
    )

    required_query_markers = {
        "direct-neighbor analysis": "direct_neighbor",
        "incoming-degree centrality": "in_degree",
        "shortest-path analysis": "hop_count",
    }
    for name, marker in required_query_markers.items():
        check(name, marker in query_source, f"Required marker: `{marker}`.")

    csv_path = args.evidence_directory / "patent_edges_5000.csv"
    csv_rows = 0
    csv_valid = True
    csv_detail = "CSV was not produced."
    if csv_path.is_file():
        try:
            with csv_path.open(newline="", encoding="utf-8") as source:
                reader = csv.DictReader(source)
                csv_valid = reader.fieldnames == ["source", "target"]
                for row in reader:
                    int(row["source"])
                    int(row["target"])
                    csv_rows += 1
            csv_valid = csv_valid and csv_rows == EDGE_LIMIT
            digest = hashlib.sha256(csv_path.read_bytes()).hexdigest()
            csv_detail = (
                f"Header is `source,target`; rows: {csv_rows}; SHA-256: `{digest}`."
            )
        except (KeyError, OSError, TypeError, ValueError) as error:
            csv_valid = False
            csv_detail = f"CSV validation error: {error}."
    check("First 5,000 SNAP citation edges", csv_valid, csv_detail)

    relationship_path = args.evidence_directory / "relationship-count.txt"
    relationship_text = (
        relationship_path.read_text(encoding="utf-8", errors="replace")
        if relationship_path.is_file()
        else ""
    )
    relationship_values = re.findall(r"(?m)^\s*\"?(\d+)\"?\s*$", relationship_text)
    relationship_ok = bool(relationship_values) and int(relationship_values[-1]) == EDGE_LIMIT
    check(
        "Exactly 5,000 directed CITES relationships",
        relationship_ok,
        f"Captured count: {relationship_values[-1] if relationship_values else 'missing'}.",
    )

    relationship_types_path = args.evidence_directory / "relationship-types.txt"
    relationship_types = (
        relationship_types_path.read_text(encoding="utf-8", errors="replace")
        if relationship_types_path.is_file()
        else ""
    )
    type_ok = "CITES" in relationship_types and "5000" in relationship_types
    check(
        "Relationship type is explicitly CITES",
        type_ok,
        "Runtime relationship-type query contains `CITES` and `5000`.",
    )

    execution_path = args.evidence_directory / "query-execution.txt"
    execution_text = (
        execution_path.read_text(encoding="utf-8", errors="replace")
        if execution_path.is_file()
        else ""
    )
    plan_markers = ("Operator", "Rows", "DB Hits")
    plans_ok = len(execution_text.encode("utf-8")) >= 1_000 and all(
        marker in execution_text for marker in plan_markers
    )
    check(
        "PROFILE execution plans are nonempty",
        plans_ok,
        f"Output bytes: {len(execution_text.encode('utf-8'))}; markers: {', '.join(plan_markers)}.",
    )
    for name, marker in required_query_markers.items():
        check(
            f"Nonempty {name} result",
            marker in execution_text,
            f"Runtime output contains result column `{marker}`.",
        )

    browser_dom_path = args.evidence_directory / "neo4j-browser-dom.html"
    browser_dom = (
        browser_dom_path.read_text(encoding="utf-8", errors="replace")
        if browser_dom_path.is_file()
        else ""
    )
    check(
        "Neo4j Browser rendered at localhost:7474",
        bool(re.search(r"neo4j", browser_dom, flags=re.IGNORECASE)),
        f"Browser-rendered DOM bytes: {len(browser_dom.encode('utf-8'))}.",
    )

    screenshot_path = args.evidence_directory / "neo4j-browser.png"
    screenshot_ok = False
    screenshot_detail = "Screenshot was not produced."
    if screenshot_path.is_file():
        screenshot = screenshot_path.read_bytes()
        if len(screenshot) >= 10_000 and screenshot.startswith(b"\x89PNG\r\n\x1a\n"):
            width, height = struct.unpack(">II", screenshot[16:24])
            screenshot_ok = width >= 1_000 and height >= 700
            screenshot_detail = (
                f"PNG bytes: {len(screenshot)}; dimensions: {width} x {height}."
            )
        else:
            screenshot_detail = f"Invalid or undersized PNG ({len(screenshot)} bytes)."
    check("Neo4j Browser screenshot", screenshot_ok, screenshot_detail)

    passed = sum(1 for _, result, _ in checks if result)
    failed = len(checks) - passed
    lines = [
        "# Task 4 CI validation report",
        "",
        f"Overall: **{'PASS' if failed == 0 else 'FAIL'}** ({passed} passed, {failed} failed)",
        "",
        "| Check | Status | Evidence |",
        "|---|---:|---|",
    ]
    for name, result, detail in checks:
        safe_detail = detail.replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {name} | {'PASS' if result else 'FAIL'} | {safe_detail} |")
    lines.extend(
        [
            "",
            "The CSV, Cypher source, verbose PROFILE output, container logs, runtime counts,",
            "browser-rendered DOM, and screenshot are stored beside this report.",
            "",
        ]
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines), encoding="utf-8")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
