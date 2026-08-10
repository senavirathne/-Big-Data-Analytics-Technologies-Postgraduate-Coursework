#!/usr/bin/env python3
"""Fail CI unless Task 3 produced genuine ranking and metrics evidence."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ranking", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--events", type=Path, required=True)
    return parser.parse_args()


def required(mapping: dict[str, Any], key: str) -> Any:
    if key not in mapping:
        raise AssertionError(f"Missing required metrics category: {key}")
    return mapping[key]


def validate_ranking(path: Path) -> list[dict[str, int]]:
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))
    if len(rows) != 50:
        raise AssertionError(f"Top-50 CSV has {len(rows)} data rows, expected 50")
    if list(rows[0]) != ["rank", "destination", "in_degree"]:
        raise AssertionError(f"Unexpected ranking columns: {list(rows[0])}")

    parsed = [
        {
            "rank": int(row["rank"]),
            "destination": int(row["destination"]),
            "in_degree": int(row["in_degree"]),
        }
        for row in rows
    ]
    if [row["rank"] for row in parsed] != list(range(1, 51)):
        raise AssertionError("Ranking rows are not numbered consecutively 1 through 50")
    expected_order = sorted(
        parsed, key=lambda row: (-row["in_degree"], row["destination"])
    )
    if parsed != expected_order:
        raise AssertionError("Ranking is not deterministically ordered by in-degree")
    if any(row["in_degree"] <= 0 for row in parsed):
        raise AssertionError("All Top-50 in-degree values must be positive")
    return parsed


def validate_metrics(path: Path) -> dict[str, Any]:
    report = json.loads(path.read_text(encoding="utf-8"))
    application = required(report, "application")
    if application.get("name") != "WebBerkStanInDegreeAnalysis":
        raise AssertionError(f"Unexpected Spark application: {application}")
    for key in ("application_id", "started_at_epoch_ms", "ended_at_epoch_ms"):
        if not application.get(key):
            raise AssertionError(f"Application evidence is missing {key}")

    dag = required(report, "full_dag_evidence")
    if not dag.get("jobs_with_stage_and_rdd_parent_chains"):
        raise AssertionError("Full DAG evidence has no Spark jobs")
    if not dag.get("sql_physical_plans"):
        raise AssertionError("Full DAG evidence has no SQL physical plans")

    stages = required(report, "stage_duration_shuffle_and_worker_metrics")
    if not stages:
        raise AssertionError("No completed stage metrics were extracted")
    stage_fields = {
        "stage_duration_ms",
        "shuffle_read_bytes",
        "shuffle_write_bytes",
        "shuffle_and_task_allocation_by_worker",
        "worker_imbalance",
    }
    for index, stage in enumerate(stages):
        missing = stage_fields.difference(stage)
        if missing:
            raise AssertionError(f"Stage {index} is missing metrics: {sorted(missing)}")

    allocation = required(report, "application_worker_allocation")
    worker_hosts = {key.split("@", 1)[-1] for key in allocation}
    if len(allocation) != 2 or len(worker_hosts) != 2:
        raise AssertionError(
            "Expected task-allocation evidence for exactly two distinct Spark workers, "
            f"found {allocation}"
        )
    if any(worker.get("tasks", 0) <= 0 for worker in allocation.values()):
        raise AssertionError("Both Spark workers must have a non-zero task allocation")

    bottlenecks = required(report, "performance_bottleneck_candidates")
    for key in (
        "longest_stage",
        "largest_shuffle_read_stage",
        "largest_shuffle_write_stage",
        "largest_disk_spill_stage",
    ):
        if key not in bottlenecks:
            raise AssertionError(f"Bottleneck evidence is missing {key}")
    skew = required(report, "skew_assessment")
    for key in ("task_criterion", "worker_criterion", "task_findings", "skew_detected"):
        if key not in skew:
            raise AssertionError(f"Skew evidence is missing {key}")
    required(report, "application_worker_imbalance")
    return report


def validate_events(path: Path) -> list[Path]:
    logs = [
        candidate
        for candidate in path.rglob("*")
        if candidate.is_file() and not candidate.name.startswith(".")
    ]
    if not logs:
        raise AssertionError(f"No Spark event logs found under {path}")
    if any(log.stat().st_size == 0 for log in logs):
        raise AssertionError("A Spark event log is empty")
    for log in logs:
        try:
            with log.open("rb") as source:
                source.read(1)
        except OSError as error:
            raise AssertionError(
                f"Spark event log is not readable by the CI runner: {log}: {error}"
            ) from error
    return logs


def main() -> None:
    args = arguments()
    ranking = validate_ranking(args.ranking)
    report = validate_metrics(args.metrics)
    event_logs = validate_events(args.events)
    summary = {
        "ranking_rows": len(ranking),
        "highest_ranked_vertex": ranking[0],
        "completed_stages": len(
            report["stage_duration_shuffle_and_worker_metrics"]
        ),
        "worker_count": len(report["application_worker_allocation"]),
        "event_logs": [
            {"path": str(path), "bytes": path.stat().st_size} for path in event_logs
        ],
        "validated_categories": [
            "top_50",
            "application",
            "full_dag",
            "stage_durations",
            "shuffle",
            "two_worker_allocation",
            "skew",
            "bottlenecks",
            "event_logs",
        ],
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
