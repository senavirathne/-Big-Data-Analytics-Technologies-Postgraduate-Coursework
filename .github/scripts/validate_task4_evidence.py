#!/usr/bin/env python3
"""Validate the Task 4 evidence generated on a GitHub-hosted runner."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import struct
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path


EDGE_LIMIT = 5_000
EXPECTED_CSV_SHA256 = (
    "3c913ccdf8bfbfd2529343dc32dc861478ba12c22a264df87ace78cc772f72ba"
)
EXPECTED_DISTANT_PATH = (
    3_484_134,
    3_858_825,
    3_635_420,
    3_858_826,
    3_741_496,
    3_858_824,
    253_889,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-directory", type=Path, required=True)
    parser.add_argument("--import-directory", type=Path, required=True)
    parser.add_argument("--evidence-directory", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.evidence_directory.mkdir(parents=True, exist_ok=True)
    prepared_csv = args.import_directory / "patent_edges_5000.csv"
    if prepared_csv.is_file():
        shutil.copyfile(
            prepared_csv,
            args.evidence_directory / "patent_edges_5000.csv",
        )

    checks: list[tuple[str, bool, str]] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append((name, passed, detail))

    query_source = (args.task_directory / "cypher/analysis_queries.cypher").read_text(
        encoding="utf-8"
    )
    import_source = (args.task_directory / "cypher/import_patents.cypher").read_text(
        encoding="utf-8"
    )

    source_bindings = (
        ("analysis query source", "cypher/analysis_queries.cypher", "analysis_queries.cypher"),
        ("import query source", "cypher/import_patents.cypher", "import_patents.cypher"),
        ("dataset preparation source", "scripts/prepare_patents.py", "prepare_patents.py"),
        ("import runner source", "scripts/run_import.sh", "run_import.sh"),
        ("analysis runner source", "scripts/run_analysis.sh", "run_analysis.sh"),
        (
            "containerized browser capture source",
            "scripts/capture_browser_evidence.sh",
            "capture_browser_evidence.sh",
        ),
        ("source staging source", "scripts/stage_evidence.py", "stage_evidence.py"),
        ("browser image source", "browser-evidence.Dockerfile", "browser-evidence.Dockerfile"),
        ("Compose source", "docker-compose.yml", "docker-compose.yml"),
    )
    for name, task_relative, evidence_name in source_bindings:
        current_path = args.task_directory / task_relative
        captured_path = args.evidence_directory / evidence_name
        current_bytes = current_path.read_bytes() if current_path.is_file() else b""
        captured_bytes = captured_path.read_bytes() if captured_path.is_file() else b""
        current_digest = hashlib.sha256(current_bytes).hexdigest() if current_bytes else "missing"
        captured_digest = (
            hashlib.sha256(captured_bytes).hexdigest() if captured_bytes else "missing"
        )
        check(
            f"Runtime evidence is bound to current {name}",
            bool(current_bytes) and current_bytes == captured_bytes,
            f"Current SHA-256: `{current_digest}`; captured SHA-256: `{captured_digest}`.",
        )

    repository_root = args.task_directory.parent
    repository_bindings = (
        (
            "container-only CI runner source",
            ".github/scripts/run_task4_neo4j_ci.sh",
            "run_task4_neo4j_ci.sh",
        ),
        (
            "containerized evidence validator source",
            ".github/scripts/validate_task4_evidence.py",
            "validate_task4_evidence.py",
        ),
        (
            "Task 4 workflow source",
            ".github/workflows/tasks-4-5.yml",
            "tasks-4-5.yml",
        ),
    )
    for name, repository_relative, evidence_name in repository_bindings:
        current_path = repository_root / repository_relative
        captured_path = args.evidence_directory / evidence_name
        current_bytes = current_path.read_bytes() if current_path.is_file() else b""
        captured_bytes = captured_path.read_bytes() if captured_path.is_file() else b""
        current_digest = hashlib.sha256(current_bytes).hexdigest() if current_bytes else "missing"
        captured_digest = (
            hashlib.sha256(captured_bytes).hexdigest() if captured_bytes else "missing"
        )
        check(
            f"Runtime evidence is bound to current {name}",
            bool(current_bytes) and current_bytes == captured_bytes,
            f"Current SHA-256: `{current_digest}`; captured SHA-256: `{captured_digest}`.",
        )

    revision_path = args.evidence_directory / "source-revision.json"
    revision: dict[str, object] = {}
    try:
        parsed_revision = json.loads(revision_path.read_text(encoding="utf-8"))
        if isinstance(parsed_revision, dict):
            revision = parsed_revision
    except (OSError, json.JSONDecodeError):
        revision = {}
    source_commit = revision.get("source_commit")
    checked_out_head = revision.get("checked_out_head")
    supplied_commit = revision.get("ci_supplied_commit")
    commit_ok = bool(
        isinstance(source_commit, str)
        and re.fullmatch(r"[0-9a-f]{40}", source_commit)
        and (checked_out_head is None or checked_out_head == source_commit)
        and (supplied_commit is None or supplied_commit == source_commit)
    )
    check(
        "Evidence is bound to one complete source commit",
        commit_ok,
        f"Source commit: `{source_commit or 'missing'}`; checked-out HEAD: "
        f"`{checked_out_head or 'not resolved'}`; CI-supplied commit: "
        f"`{supplied_commit or 'not supplied'}`.",
    )

    profile_count = len(re.findall(r"(?m)^\s*PROFILE\s*$", query_source))
    check(
        "Exactly three PROFILE analyses",
        profile_count == 3,
        f"Found {profile_count} PROFILE statements.",
    )

    dataset_marker = "snap-cit-Patents-first-5000-v1"
    check(
        "Import is idempotent and scoped to the coursework dataset",
        "DETACH DELETE" not in import_source
        and "MERGE (source)-[:CITES" in import_source
        and "coursework_edge" in import_source
        and dataset_marker in import_source,
        "The import must use a stable dataset/edge identity and must not globally delete Patent nodes.",
    )
    check(
        "All three analyses are scoped to the imported subset",
        len(re.split(r"(?m)^\s*PROFILE\s*$", query_source)[1:]) == 3
        and all(
            dataset_marker in section
            for section in re.split(r"(?m)^\s*PROFILE\s*$", query_source)[1:]
        ),
        "Each PROFILE statement must reference the coursework dataset marker; "
        f"total marker occurrences: {query_source.count(dataset_marker)}.",
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
    csv_valid = False
    csv_detail = "CSV was not produced."
    citation_edges: list[tuple[int, int]] = []
    if csv_path.is_file():
        try:
            with csv_path.open(newline="", encoding="utf-8") as source:
                reader = csv.DictReader(source)
                csv_valid = reader.fieldnames == ["source", "target"]
                for row in reader:
                    citation_edges.append(
                        (int(row["source"]), int(row["target"]))
                    )
                    csv_rows += 1
            digest = hashlib.sha256(csv_path.read_bytes()).hexdigest()
            csv_valid = (
                csv_valid
                and csv_rows == EDGE_LIMIT
                and digest == EXPECTED_CSV_SHA256
            )
            csv_detail = (
                f"Header is `source,target`; rows: {csv_rows}; SHA-256: `{digest}`."
            )
        except (KeyError, OSError, TypeError, ValueError) as error:
            csv_valid = False
            csv_detail = f"CSV validation error: {error}."
    check("First 5,000 SNAP citation edges", csv_valid, csv_detail)

    relationship_path = args.evidence_directory / "relationship-count.txt"
    relationship_rows: list[list[str]] = []
    if relationship_path.is_file():
        try:
            with relationship_path.open(newline="", encoding="utf-8") as source:
                relationship_rows = list(csv.reader(source, skipinitialspace=True))
        except (OSError, csv.Error):
            relationship_rows = []
    relationship_ok = relationship_rows == [["directed_cites"], [str(EDGE_LIMIT)]]
    check(
        "Exactly 5,000 directed CITES relationships",
        relationship_ok,
        f"Parsed runtime rows: {relationship_rows or 'missing'}.",
    )

    relationship_types_path = args.evidence_directory / "relationship-types.txt"
    relationship_type_rows: list[list[str]] = []
    parsed_types: list[str] | None = None
    if relationship_types_path.is_file():
        try:
            with relationship_types_path.open(newline="", encoding="utf-8") as source:
                relationship_type_rows = list(csv.reader(source, skipinitialspace=True))
            if len(relationship_type_rows) == 2 and len(relationship_type_rows[1]) == 2:
                decoded_types = json.loads(relationship_type_rows[1][1])
                if isinstance(decoded_types, list) and all(
                    isinstance(value, str) for value in decoded_types
                ):
                    parsed_types = decoded_types
        except (OSError, csv.Error, json.JSONDecodeError):
            relationship_type_rows = []
    type_ok = (
        relationship_type_rows[:1]
        == [["total_relationships", "relationship_types"]]
        and len(relationship_type_rows) == 2
        and relationship_type_rows[1][0] == str(EDGE_LIMIT)
        and parsed_types == ["CITES"]
    )
    check(
        "Exact runtime relationship type set is CITES",
        type_ok,
        f"Count: {relationship_type_rows[1][0] if len(relationship_type_rows) == 2 else 'missing'}; "
        f"types: {parsed_types if parsed_types is not None else 'missing'}.",
    )

    import_scope_path = args.evidence_directory / "import-scope.txt"
    import_scope_rows: list[list[str]] = []
    if import_scope_path.is_file():
        try:
            with import_scope_path.open(newline="", encoding="utf-8") as source:
                import_scope_rows = list(csv.reader(source, skipinitialspace=True))
        except (OSError, csv.Error):
            import_scope_rows = []
    scope_ok = False
    if (
        len(import_scope_rows) == 2
        and import_scope_rows[0] == ["all_cites", "coursework_cites", "unrelated_cites"]
        and len(import_scope_rows[1]) == 3
    ):
        try:
            all_cites, coursework_cites, unrelated_cites = map(int, import_scope_rows[1])
            scope_ok = (
                coursework_cites == EDGE_LIMIT
                and all_cites >= coursework_cites
                and unrelated_cites == all_cites - coursework_cites
            )
        except ValueError:
            scope_ok = False
    check(
        "Runtime count distinguishes coursework and unrelated CITES data",
        scope_ok,
        f"Parsed runtime rows: {import_scope_rows or 'missing'}.",
    )

    execution_path = args.evidence_directory / "query-execution.txt"
    execution_text = (
        execution_path.read_text(encoding="utf-8", errors="replace")
        if execution_path.is_file()
        else ""
    )
    plan_markers = ("Operator", "Rows", "DB Hits")
    profile_plan_count = len(
        re.findall(
            r'(?m)^\|\s*"PROFILE"\s*\|\s*"READ_ONLY"\s*\|', execution_text
        )
    )
    database_access_summary_count = execution_text.count("Total database accesses:")
    timing_summary_count = execution_text.count("ready to start consuming query")
    plans_ok = (
        len(execution_text.encode("utf-8")) >= 1_000
        and all(marker in execution_text for marker in plan_markers)
        and profile_plan_count == 3
        and database_access_summary_count == 3
        and timing_summary_count == 3
    )
    check(
        "Exactly three nonempty PROFILE execution plans",
        plans_ok,
        f"Output bytes: {len(execution_text.encode('utf-8'))}; PROFILE summaries: "
        f"{profile_plan_count}; database-access summaries: {database_access_summary_count}; "
        f"timing summaries: {timing_summary_count}.",
    )

    direct_neighbor_rows = [
        (int(target), int(neighbor), direction)
        for target, neighbor, direction in re.findall(
            r'(?m)^\|\s*(3858514)\s*\|\s*(\d+)\s*\|\s*'
            r'"(OUTGOING_CITES|INCOMING_CITED_BY)"\s*\|\s*$',
            execution_text,
        )
    ]
    expected_direct_neighbors = sorted(
        [
            (3_858_514, target, "OUTGOING_CITES")
            for source, target in citation_edges
            if source == 3_858_514
        ]
        + [
            (3_858_514, source, "INCOMING_CITED_BY")
            for source, target in citation_edges
            if target == 3_858_514
        ],
        key=lambda row: (row[1], row[2]),
    )
    check(
        "Nonempty exact direct-neighbor result",
        csv_valid
        and bool(expected_direct_neighbors)
        and direct_neighbor_rows == expected_direct_neighbors,
        f"Runtime rows: {len(direct_neighbor_rows)}; independently expected rows: "
        f"{len(expected_direct_neighbors)}.",
    )

    centrality_rows = [
        (int(patent), int(in_degree))
        for patent, in_degree in re.findall(
            r"(?m)^\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*$", execution_text
        )
    ]
    nodes = {node for edge in citation_edges for node in edge}
    incoming_counts = Counter(target for _, target in citation_edges)
    expected_centrality = sorted(
        ((node, incoming_counts[node]) for node in nodes),
        key=lambda row: (-row[1], row[0]),
    )[:10]
    check(
        "Exact top-10 incoming-degree centrality result",
        csv_valid and centrality_rows == expected_centrality,
        f"Runtime rows: {centrality_rows or 'none'}; independently expected: "
        f"{expected_centrality or 'none'}.",
    )

    path_rows = re.findall(
        r"(?m)^\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*"
        r"\[([0-9,\s]+)\]\s*\|\s*$",
        execution_text,
    )
    distant_paths: list[tuple[int, int, int, list[int]]] = []
    adjacency: dict[int, set[int]] = defaultdict(set)
    for source, target in citation_edges:
        adjacency[source].add(target)
        adjacency[target].add(source)
    shortest_distance: int | None = None
    if csv_valid:
        start_node, finish_node = EXPECTED_DISTANT_PATH[0], EXPECTED_DISTANT_PATH[-1]
        frontier = deque([(start_node, 0)])
        visited = {start_node}
        while frontier:
            node, distance = frontier.popleft()
            if node == finish_node:
                shortest_distance = distance
                break
            for neighbor in adjacency[node]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    frontier.append((neighbor, distance + 1))
    for raw_start, raw_finish, raw_hops, raw_nodes in path_rows:
        nodes = [int(value.strip()) for value in raw_nodes.split(",")]
        start, finish, hops = int(raw_start), int(raw_finish), int(raw_hops)
        if (
            start == EXPECTED_DISTANT_PATH[0]
            and finish == EXPECTED_DISTANT_PATH[-1]
            and hops == len(EXPECTED_DISTANT_PATH) - 1
            and nodes == list(EXPECTED_DISTANT_PATH)
            and shortest_distance == hops
            and all(
                right in adjacency[left]
                for left, right in zip(nodes, nodes[1:])
            )
        ):
            distant_paths.append((start, finish, hops, nodes))
    check(
        "Nonempty shortest path between distant patents",
        bool(distant_paths),
        (
            "Required the independently verified six-hop sequence "
            f"{list(EXPECTED_DISTANT_PATH)}; qualifying rows: "
            f"{distant_paths or 'none'}; independently recomputed shortest distance: "
            f"{shortest_distance if shortest_distance is not None else 'unavailable'}."
        ),
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

    browser_headers_path = args.evidence_directory / "neo4j-browser-headers.txt"
    browser_headers = (
        browser_headers_path.read_text(encoding="utf-8", errors="replace")
        if browser_headers_path.is_file()
        else ""
    )
    browser_http_path = args.evidence_directory / "neo4j-browser-http.html"
    browser_http = (
        browser_http_path.read_text(encoding="utf-8", errors="replace")
        if browser_http_path.is_file()
        else ""
    )
    check(
        "Neo4j Browser HTTP response captured inside the Compose network",
        bool(re.search(r"(?mi)^HTTP/\S+\s+200\b", browser_headers))
        and bool(re.search(r"neo4j", browser_http, flags=re.IGNORECASE)),
        f"Header bytes: {len(browser_headers.encode('utf-8'))}; response body bytes: "
        f"{len(browser_http.encode('utf-8'))}.",
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
            "The CSV, Cypher source, verbose PROFILE output, runtime counts,",
            "browser-rendered DOM, screenshot, source revision, and evidence manifest are stored",
            "beside this report.",
            "",
        ]
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines), encoding="utf-8")

    manifest_files = sorted(
        {
            evidence_name for _, _, evidence_name in source_bindings
        }
        | {
            evidence_name for _, _, evidence_name in repository_bindings
        }
        | {
            "source-revision.json",
            "patent_edges_5000.csv",
            "query-execution.txt",
            "relationship-count.txt",
            "relationship-types.txt",
            "import-scope.txt",
            "neo4j-browser-headers.txt",
            "neo4j-browser-http.html",
            "neo4j-browser-dom.html",
            "neo4j-browser-console.log",
            "neo4j-browser.png",
            args.report.name,
        }
    )
    artifacts: dict[str, dict[str, object]] = {}
    for name in manifest_files:
        path = args.evidence_directory / name
        if path.is_file():
            payload = path.read_bytes()
            artifacts[name] = {
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        else:
            artifacts[name] = {"bytes": None, "sha256": None}

    manifest = {
        "schema_version": 1,
        "task": "Coventry Big Data Analytics coursework Task 4",
        "source_commit": source_commit,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "validation_status": "PASS" if failed == 0 else "FAIL",
        "checks_passed": passed,
        "checks_failed": failed,
        "artifacts": artifacts,
    }
    (args.evidence_directory / "evidence-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
