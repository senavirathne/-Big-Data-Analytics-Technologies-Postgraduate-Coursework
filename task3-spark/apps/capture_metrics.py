#!/usr/bin/env python3
"""Extract the Task 3 DAG, stage, shuffle, worker, and skew records."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any


EVENT_DIRECTORY = Path("/opt/spark-events")
OUTPUT = Path("/metrics/execution-metrics.json")
ALLOCATION_METRICS = (
    "tasks",
    "input_records",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
)


def newest_event_log() -> Path:
    logs = [
        path
        for path in EVENT_DIRECTORY.rglob("*")
        if path.is_file() and not path.name.startswith(".")
    ]
    if not logs:
        raise FileNotFoundError(f"No Spark event log found below {EVENT_DIRECTORY}")
    return max(logs, key=lambda path: path.stat().st_mtime_ns)


def nested_metric(metrics: dict[str, Any], *keys: str) -> int:
    value: Any = metrics
    for key in keys:
        if not isinstance(value, dict):
            return 0
        value = value.get(key, 0)
    return int(value or 0)


def empty_allocation() -> dict[str, int]:
    return {metric: 0 for metric in ALLOCATION_METRICS}


def compare_workers(
    allocation: dict[str, dict[str, int]],
) -> dict[str, Any]:
    """Report allocation ratios without treating imbalance as proof of its cause."""

    comparisons: dict[str, Any] = {}
    for metric in ALLOCATION_METRICS:
        values = {
            worker: worker_metrics[metric]
            for worker, worker_metrics in allocation.items()
        }
        if not values:
            comparisons[metric] = {
                "minimum": None,
                "maximum": None,
                "max_to_min_ratio": None,
                "workers_with_zero": [],
            }
            continue
        minimum = min(values.values())
        maximum = max(values.values())
        comparisons[metric] = {
            "minimum": minimum,
            "maximum": maximum,
            "max_to_min_ratio": (
                round(maximum / minimum, 3) if minimum > 0 else None
            ),
            "workers_at_maximum": sorted(
                worker for worker, value in values.items() if value == maximum
            ),
            "workers_with_zero": sorted(
                worker for worker, value in values.items() if value == 0
            ),
        }
    return {
        "meaning": (
            "These are direct maximum-to-minimum allocation comparisons. A null ratio "
            "with a non-zero maximum means at least one worker received zero allocation. "
            "Imbalance is a sign to investigate, not by itself proof of data skew."
        ),
        "worker_count": len(allocation),
        "metrics": comparisons,
    }


def stage_identity(stage: dict[str, Any]) -> tuple[int, int]:
    return (
        int(stage.get("Stage ID", -1)),
        int(stage.get("Stage Attempt ID", 0)),
    )


def main() -> None:
    event_log = newest_event_log()
    application: dict[str, Any] = {}
    jobs: list[dict[str, Any]] = []
    dag_stages: dict[int, dict[str, Any]] = {}
    executors: dict[str, str] = {}
    completed_stages: dict[tuple[int, int], dict[str, Any]] = {}
    stage_tasks: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)

    with event_log.open("rt", encoding="utf-8") as source:
        for line in source:
            event = json.loads(line)
            event_name = event.get("Event")

            if event_name == "SparkListenerApplicationStart":
                application = {
                    "name": event.get("App Name"),
                    "application_id": event.get("App ID"),
                    "started_at_epoch_ms": event.get("Timestamp"),
                }
            elif event_name == "SparkListenerApplicationEnd":
                application["ended_at_epoch_ms"] = event.get("Timestamp")
            elif event_name == "SparkListenerJobStart":
                stages = event.get("Stage Infos", [])
                jobs.append(
                    {
                        "job_id": event.get("Job ID"),
                        "stage_ids": event.get("Stage IDs", []),
                    }
                )
                for stage in stages:
                    stage_id = int(stage.get("Stage ID", -1))
                    dag_stages[stage_id] = {
                        "stage_id": stage_id,
                        "name": stage.get("Stage Name"),
                        "parent_stage_ids": stage.get("Parent IDs", []),
                    }
            elif event_name == "SparkListenerExecutorAdded":
                executor_id = str(event.get("Executor ID"))
                if executor_id != "driver":
                    info = event.get("Executor Info", {})
                    executors[executor_id] = str(info.get("Host") or "unknown")
            elif event_name == "SparkListenerStageCompleted":
                stage = event.get("Stage Info", {})
                completed_stages[stage_identity(stage)] = stage
                stage_id = int(stage.get("Stage ID", -1))
                dag_stages[stage_id] = {
                    "stage_id": stage_id,
                    "name": stage.get("Stage Name"),
                    "parent_stage_ids": stage.get("Parent IDs", []),
                }
            elif event_name == "SparkListenerTaskEnd":
                reason = event.get("Task End Reason", {})
                if isinstance(reason, dict) and reason.get("Reason") not in (
                    None,
                    "Success",
                ):
                    continue
                info = event.get("Task Info", {})
                metrics = event.get("Task Metrics", {})
                executor_id = str(info.get("Executor ID") or "unknown")
                host = str(info.get("Host") or executors.get(executor_id) or "unknown")
                shuffle_read = nested_metric(
                    metrics, "Shuffle Read Metrics", "Remote Bytes Read"
                ) + nested_metric(
                    metrics, "Shuffle Read Metrics", "Local Bytes Read"
                )
                key = (
                    int(event.get("Stage ID", -1)),
                    int(event.get("Stage Attempt ID", 0)),
                )
                stage_tasks[key].append(
                    {
                        "worker": f"{executor_id}@{host}",
                        "input_records": nested_metric(
                            metrics, "Input Metrics", "Records Read"
                        ),
                        "shuffle_read_bytes": shuffle_read,
                        "shuffle_write_bytes": nested_metric(
                            metrics, "Shuffle Write Metrics", "Shuffle Bytes Written"
                        ),
                    }
                )

    if not application.get("application_id"):
        raise RuntimeError("Spark event log has no application ID")
    if not jobs or not completed_stages:
        raise RuntimeError("Spark event log has no completed job and stage records")

    registered_workers = {
        f"{executor_id}@{host}": empty_allocation()
        for executor_id, host in executors.items()
    }
    application_allocation = {
        worker: empty_allocation() for worker in registered_workers
    }
    stage_records: list[dict[str, Any]] = []

    for key, stage in sorted(completed_stages.items()):
        allocation = {
            worker: empty_allocation() for worker in registered_workers
        }
        for task in stage_tasks.get(key, []):
            worker = task["worker"]
            allocation.setdefault(worker, empty_allocation())
            application_allocation.setdefault(worker, empty_allocation())
            allocation[worker]["tasks"] += 1
            application_allocation[worker]["tasks"] += 1
            for metric in ALLOCATION_METRICS[1:]:
                allocation[worker][metric] += task[metric]
                application_allocation[worker][metric] += task[metric]

        submission = stage.get("Submission Time")
        completion = stage.get("Completion Time")
        duration_ms = (
            int(completion) - int(submission)
            if submission is not None and completion is not None
            else None
        )
        stage_records.append(
            {
                "stage_id": key[0],
                "attempt_id": key[1],
                "name": stage.get("Stage Name"),
                "parent_stage_ids": stage.get("Parent IDs", []),
                "duration_ms": duration_ms,
                "shuffle_read_bytes": sum(
                    worker["shuffle_read_bytes"] for worker in allocation.values()
                ),
                "shuffle_write_bytes": sum(
                    worker["shuffle_write_bytes"] for worker in allocation.values()
                ),
                "worker_allocation": dict(sorted(allocation.items())),
                "worker_skew_comparison": compare_workers(allocation),
            }
        )

    dag_edges = {
        (parent_id, stage["stage_id"])
        for stage in dag_stages.values()
        for parent_id in stage["parent_stage_ids"]
    }
    report = {
        "source_event_log": str(event_log),
        "application": application,
        "jobs": jobs,
        "stage_parent_dag": {
            "stages": [dag_stages[key] for key in sorted(dag_stages)],
            "edges_parent_to_child": [
                {"parent_stage_id": parent, "child_stage_id": child}
                for parent, child in sorted(dag_edges)
            ],
        },
        "stage_metrics": stage_records,
        "application_worker_allocation": dict(
            sorted(application_allocation.items())
        ),
        "application_worker_skew_comparison": compare_workers(
            application_allocation
        ),
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Execution metrics written to {OUTPUT}")


if __name__ == "__main__":
    main()
