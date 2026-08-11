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
    jobs = dag.get("jobs_with_stage_and_rdd_parent_chains")
    if not jobs:
        raise AssertionError("Full DAG evidence has no Spark jobs")
    stage_edges = dag.get("stage_edges_parent_to_child")
    rdd_edges = dag.get("rdd_edges_parent_to_child")
    if not stage_edges:
        raise AssertionError("Full DAG evidence has no parent-to-child stage edges")
    if not rdd_edges:
        raise AssertionError("Full DAG evidence has no parent-to-child RDD lineage")
    sql_plans = dag.get("sql_physical_plans")
    if not sql_plans:
        raise AssertionError("Full DAG evidence has no SQL physical plans")
    if not any(
        isinstance(plan.get("physical_plan"), str)
        and plan["physical_plan"].strip()
        for plan in sql_plans
    ):
        raise AssertionError("SQL evidence contains no substantive physical plan text")

    stages = required(report, "stage_duration_shuffle_and_worker_metrics")
    if not stages:
        raise AssertionError("No completed stage metrics were extracted")
    stage_fields = {
        "stage_duration_ms",
        "shuffle_read_bytes",
        "shuffle_write_bytes",
        "shuffle_and_task_allocation_by_worker",
        "worker_data_balance",
        "worker_duration_balance",
    }
    for index, stage in enumerate(stages):
        missing = stage_fields.difference(stage)
        if missing:
            raise AssertionError(f"Stage {index} is missing metrics: {sorted(missing)}")

    positive_durations = [
        stage["stage_duration_ms"]
        for stage in stages
        if isinstance(stage["stage_duration_ms"], int)
        and stage["stage_duration_ms"] > 0
    ]
    if not positive_durations:
        raise AssertionError("No positive completed-stage duration was recorded")
    total_shuffle_read = sum(stage["shuffle_read_bytes"] for stage in stages)
    total_shuffle_write = sum(stage["shuffle_write_bytes"] for stage in stages)
    if total_shuffle_read <= 0 or total_shuffle_write <= 0:
        raise AssertionError(
            "Expected substantive non-zero shuffle read and write allocation, found "
            f"read={total_shuffle_read}, write={total_shuffle_write}"
        )
    if not any(
        any(
            worker["shuffle_read_bytes"] > 0
            or worker["shuffle_write_bytes"] > 0
            for worker in stage["shuffle_and_task_allocation_by_worker"].values()
        )
        for stage in stages
    ):
        raise AssertionError("Shuffle totals are not allocated to workers in stage evidence")

    allocation = required(report, "application_worker_allocation")
    worker_hosts = {key.split("@", 1)[-1] for key in allocation}
    if len(allocation) != 2 or len(worker_hosts) != 2:
        raise AssertionError(
            "Expected task-allocation evidence for exactly two distinct Spark workers, "
            f"found {allocation}"
        )
    if any(worker.get("tasks", 0) <= 0 for worker in allocation.values()):
        raise AssertionError("Both Spark workers must have a non-zero task allocation")
    if any(
        sum(
            worker.get(metric, 0)
            for metric in (
                "input_records",
                "shuffle_read_records",
                "shuffle_write_records",
            )
        )
        <= 0
        for worker in allocation.values()
    ):
        raise AssertionError("Both Spark workers must have non-zero data allocation")

    bottlenecks = required(report, "performance_bottleneck_candidates")
    required_bottlenecks = {
        "longest_stage": "stage_duration_ms",
        "largest_shuffle_read_stage": "shuffle_read_bytes",
        "largest_shuffle_write_stage": "shuffle_write_bytes",
    }
    for key, metric in required_bottlenecks.items():
        if key not in bottlenecks:
            raise AssertionError(f"Bottleneck evidence is missing {key}")
        candidate = bottlenecks[key]
        if not isinstance(candidate, dict) or candidate.get(metric, 0) <= 0:
            raise AssertionError(f"Bottleneck evidence is not substantive: {key}")
    if "largest_disk_spill_stage" not in bottlenecks:
        raise AssertionError("Bottleneck evidence is missing largest_disk_spill_stage")
    data_skew = required(report, "data_skew_assessment")
    for key in (
        "definition",
        "task_criterion",
        "worker_criterion",
        "evaluated_stage_count",
        "task_partition_findings",
        "stage_worker_findings",
        "application_worker_data_balance",
        "data_skew_detected",
    ):
        if key not in data_skew:
            raise AssertionError(f"Data-skew evidence is missing {key}")
    if data_skew["evaluated_stage_count"] <= 0:
        raise AssertionError("No multi-task stage was evaluated for data skew")
    if not isinstance(data_skew["data_skew_detected"], bool):
        raise AssertionError("data_skew_detected must be an evidence-based boolean")
    data_metric_names = {
        "input_records",
        "shuffle_read_records",
        "shuffle_read_bytes",
        "shuffle_write_records",
        "shuffle_write_bytes",
    }
    for finding in data_skew["task_partition_findings"]:
        unexpected = set(finding.get("signals_at_or_above_3x_median", [])).difference(
            data_metric_names
        )
        if unexpected:
            raise AssertionError(
                "Data-skew findings include non-data signals: "
                f"{sorted(unexpected)}"
            )
    for finding in data_skew["stage_worker_findings"]:
        unexpected = {
            signal.split(":", 1)[0] for signal in finding.get("signals", [])
        }.difference(data_metric_names)
        if unexpected:
            raise AssertionError(
                "Worker data-skew findings include non-data signals: "
                f"{sorted(unexpected)}"
            )
    application_data_balance = data_skew["application_worker_data_balance"]
    if application_data_balance.get("eligible") is not True:
        raise AssertionError("Application-level data skew was not evaluated across workers")
    missing_data_ratios = data_metric_names.difference(
        application_data_balance.get("maximum_to_minimum_ratios", {})
    )
    if missing_data_ratios:
        raise AssertionError(
            "Application data-skew evidence is missing metrics: "
            f"{sorted(missing_data_ratios)}"
        )

    execution_balance = required(report, "execution_imbalance_assessment")
    for key in (
        "definition",
        "task_duration_criterion",
        "task_duration_findings",
        "stage_worker_duration_findings",
        "structural_stage_allocations",
        "application_worker_duration_balance",
        "application_task_allocation",
    ):
        if key not in execution_balance:
            raise AssertionError(f"Execution-imbalance evidence is missing {key}")
    if execution_balance["application_worker_duration_balance"].get("eligible") is not True:
        raise AssertionError("Application duration balance was not evaluated across workers")
    for structural in execution_balance["structural_stage_allocations"]:
        if structural.get("data_skew_inferred") is not False:
            raise AssertionError(
                "A structurally single-worker/single-task stage was mislabeled as data skew"
            )
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
            "data_skew",
            "execution_imbalance",
            "bottlenecks",
            "event_logs",
        ],
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
