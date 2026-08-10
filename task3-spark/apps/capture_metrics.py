#!/usr/bin/env python3
"""Extract DAG, stage, shuffle, and task-skew evidence from a Spark event log."""

from __future__ import annotations

import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


EVENT_DIRECTORY = Path("/opt/spark-events")
OUTPUT = Path("/metrics/execution-metrics.json")


def newest_event_log() -> Path:
    candidates = [
        path
        for path in EVENT_DIRECTORY.rglob("*")
        if path.is_file() and not path.name.startswith(".")
    ]
    if not candidates:
        raise FileNotFoundError(f"No event log found below {EVENT_DIRECTORY}")
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def metric(task_metrics: dict[str, Any], *keys: str) -> int:
    current: Any = task_metrics
    for key in keys:
        if not isinstance(current, dict):
            return 0
        current = current.get(key, 0)
    return int(current or 0)


def ratio_max_to_median(values: list[int]) -> float | None:
    positive = [value for value in values if value > 0]
    if not positive:
        return None
    median = statistics.median(positive)
    return round(max(positive) / median, 3) if median else None


def task_distribution(values: list[int]) -> dict[str, Any]:
    if not values:
        return {
            "minimum": None,
            "median": None,
            "maximum": None,
            "mean": None,
            "max_to_median_ratio": None,
        }
    return {
        "minimum": min(values),
        "median": statistics.median(values),
        "maximum": max(values),
        "mean": round(statistics.mean(values), 3),
        "max_to_median_ratio": ratio_max_to_median(values),
    }


def empty_worker_metrics() -> dict[str, int]:
    return {
        "tasks": 0,
        "total_task_duration_ms": 0,
        "input_records": 0,
        "shuffle_read_bytes": 0,
        "shuffle_write_bytes": 0,
    }


def worker_imbalance(worker_metrics: dict[str, dict[str, int]]) -> dict[str, Any]:
    ratios: dict[str, float | None] = {}
    zero_allocation: list[str] = []
    signals: list[str] = []
    for metric_name in empty_worker_metrics():
        values = [worker[metric_name] for worker in worker_metrics.values()]
        if not values or max(values) == 0:
            ratios[metric_name] = None
            continue
        if min(values) == 0:
            ratios[metric_name] = None
            zero_allocation.append(metric_name)
            signals.append(f"{metric_name}:nonzero-on-one-worker-and-zero-on-another")
            continue
        ratio = round(max(values) / min(values), 3)
        ratios[metric_name] = ratio
        if ratio >= 3.0:
            signals.append(f"{metric_name}:maximum-at-least-3x-minimum")
    return {
        "maximum_to_minimum_ratios": ratios,
        "metrics_with_zero_allocation_on_a_worker": zero_allocation,
        "signals": signals,
    }


def main() -> None:
    event_log = newest_event_log()
    application: dict[str, Any] = {}
    jobs: list[dict[str, Any]] = []
    sql_executions: list[dict[str, Any]] = []
    executors: dict[str, dict[str, Any]] = {}
    completed_stages: dict[tuple[int, int], dict[str, Any]] = {}
    tasks: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)

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
                stage_chain = []
                for stage in event.get("Stage Infos", []):
                    stage_chain.append(
                        {
                            "stage_id": stage.get("Stage ID"),
                            "name": stage.get("Stage Name"),
                            "parent_stage_ids": stage.get("Parent IDs", []),
                            "details": stage.get("Details"),
                            "rdd_lineage": [
                                {
                                    "rdd_id": rdd.get("RDD ID"),
                                    "name": rdd.get("Name"),
                                    "parent_rdd_ids": rdd.get("Parent IDs", []),
                                    "storage_level": rdd.get("Storage Level"),
                                }
                                for rdd in stage.get("RDD Info", [])
                            ],
                        }
                    )
                jobs.append(
                    {
                        "job_id": event.get("Job ID"),
                        "submitted_at_epoch_ms": event.get("Submission Time"),
                        "stage_ids": event.get("Stage IDs", []),
                        "stage_dag_and_rdd_lineage": stage_chain,
                    }
                )
            elif event_name == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                sql_executions.append(
                    {
                        "execution_id": event.get("executionId"),
                        "description": event.get("description"),
                        "details": event.get("details"),
                        "physical_plan": event.get("physicalPlanDescription"),
                    }
                )
            elif event_name == "SparkListenerExecutorAdded":
                executor_info = event.get("Executor Info", {})
                executor_id = str(event.get("Executor ID"))
                executors[executor_id] = {
                    "executor_id": executor_id,
                    "host": executor_info.get("Host"),
                    "total_cores": executor_info.get("Total Cores"),
                }
            elif event_name == "SparkListenerStageCompleted":
                stage = event.get("Stage Info", {})
                key = (int(stage.get("Stage ID", -1)), int(stage.get("Stage Attempt ID", 0)))
                completed_stages[key] = stage
            elif event_name == "SparkListenerTaskEnd":
                key = (int(event.get("Stage ID", -1)), int(event.get("Stage Attempt ID", 0)))
                info = event.get("Task Info", {})
                values = event.get("Task Metrics", {})
                shuffle_read_remote_bytes = metric(
                    values, "Shuffle Read Metrics", "Remote Bytes Read"
                )
                shuffle_read_local_bytes = metric(
                    values, "Shuffle Read Metrics", "Local Bytes Read"
                )
                tasks[key].append(
                    {
                        "executor_id": info.get("Executor ID"),
                        "host": info.get("Host"),
                        "duration_ms": max(
                            0,
                            int(info.get("Finish Time", 0)) - int(info.get("Launch Time", 0)),
                        ),
                        "input_records": metric(values, "Input Metrics", "Records Read"),
                        "shuffle_read_bytes": (
                            shuffle_read_remote_bytes + shuffle_read_local_bytes
                        ),
                        "shuffle_read_remote_bytes": shuffle_read_remote_bytes,
                        "shuffle_read_local_bytes": shuffle_read_local_bytes,
                        "shuffle_read_records": metric(
                            values, "Shuffle Read Metrics", "Total Records Read"
                        ),
                        "shuffle_write_bytes": metric(
                            values, "Shuffle Write Metrics", "Shuffle Bytes Written"
                        ),
                        "shuffle_write_records": metric(
                            values, "Shuffle Write Metrics", "Shuffle Records Written"
                        ),
                        "executor_run_time_ms": metric(values, "Executor Run Time"),
                        "jvm_gc_time_ms": metric(values, "JVM GC Time"),
                        "memory_spilled_bytes": metric(values, "Memory Bytes Spilled"),
                        "disk_spilled_bytes": metric(values, "Disk Bytes Spilled"),
                    }
                )

    stage_metrics = []
    task_skew_findings = []
    worker_skew_findings = []
    application_worker_metrics: dict[str, dict[str, int]] = {
        f"{executor_id}@{executor['host']}": empty_worker_metrics()
        for executor_id, executor in executors.items()
    }
    for key, stage in sorted(completed_stages.items()):
        stage_tasks = tasks.get(key, [])
        durations = [task["duration_ms"] for task in stage_tasks]
        input_records = [task["input_records"] for task in stage_tasks]
        shuffle_reads = [task["shuffle_read_bytes"] for task in stage_tasks]
        worker_summary: dict[str, dict[str, int]] = {
            worker: empty_worker_metrics() for worker in application_worker_metrics
        }
        for task in stage_tasks:
            worker = f"{task['executor_id']}@{task['host']}"
            worker_summary.setdefault(worker, empty_worker_metrics())
            application_worker_metrics.setdefault(worker, empty_worker_metrics())
            worker_summary[worker]["tasks"] += 1
            worker_summary[worker]["total_task_duration_ms"] += task["duration_ms"]
            worker_summary[worker]["input_records"] += task["input_records"]
            worker_summary[worker]["shuffle_read_bytes"] += task["shuffle_read_bytes"]
            worker_summary[worker]["shuffle_write_bytes"] += task["shuffle_write_bytes"]
            application_worker_metrics[worker]["tasks"] += 1
            application_worker_metrics[worker]["total_task_duration_ms"] += task[
                "duration_ms"
            ]
            application_worker_metrics[worker]["input_records"] += task["input_records"]
            application_worker_metrics[worker]["shuffle_read_bytes"] += task[
                "shuffle_read_bytes"
            ]
            application_worker_metrics[worker]["shuffle_write_bytes"] += task[
                "shuffle_write_bytes"
            ]

        submission = stage.get("Submission Time")
        completion = stage.get("Completion Time")
        duration_ms = (
            int(completion) - int(submission)
            if submission is not None and completion is not None
            else None
        )
        duration_distribution = task_distribution(durations)
        input_distribution = task_distribution(input_records)
        shuffle_distribution = task_distribution(shuffle_reads)
        ratios = {
            "task_duration": duration_distribution["max_to_median_ratio"],
            "input_records": input_distribution["max_to_median_ratio"],
            "shuffle_read": shuffle_distribution["max_to_median_ratio"],
        }
        skew_signals = [
            name for name, ratio in ratios.items() if ratio is not None and ratio >= 3.0
        ]
        if skew_signals:
            task_skew_findings.append(
                {
                    "stage_id": key[0],
                    "signals_at_or_above_3x_median": skew_signals,
                    "ratios": ratios,
                }
            )
        stage_worker_imbalance = worker_imbalance(worker_summary)
        if stage_worker_imbalance["signals"]:
            worker_skew_findings.append(
                {
                    "stage_id": key[0],
                    "attempt_id": key[1],
                    **stage_worker_imbalance,
                }
            )

        stage_metrics.append(
            {
                "stage_id": key[0],
                "attempt_id": key[1],
                "name": stage.get("Stage Name"),
                "parent_stage_ids": stage.get("Parent IDs", []),
                "task_count": len(stage_tasks),
                "stage_duration_ms": duration_ms,
                "shuffle_read_bytes": sum(shuffle_reads),
                "shuffle_read_remote_bytes": sum(
                    task["shuffle_read_remote_bytes"] for task in stage_tasks
                ),
                "shuffle_read_local_bytes": sum(
                    task["shuffle_read_local_bytes"] for task in stage_tasks
                ),
                "shuffle_read_records": sum(
                    task["shuffle_read_records"] for task in stage_tasks
                ),
                "shuffle_write_bytes": sum(
                    task["shuffle_write_bytes"] for task in stage_tasks
                ),
                "shuffle_write_records": sum(
                    task["shuffle_write_records"] for task in stage_tasks
                ),
                "executor_run_time_ms": sum(
                    task["executor_run_time_ms"] for task in stage_tasks
                ),
                "jvm_gc_time_ms": sum(task["jvm_gc_time_ms"] for task in stage_tasks),
                "memory_spilled_bytes": sum(
                    task["memory_spilled_bytes"] for task in stage_tasks
                ),
                "disk_spilled_bytes": sum(
                    task["disk_spilled_bytes"] for task in stage_tasks
                ),
                "task_duration_distribution_ms": duration_distribution,
                "input_record_distribution": input_distribution,
                "shuffle_read_distribution_bytes": shuffle_distribution,
                "shuffle_and_task_allocation_by_worker": dict(
                    sorted(worker_summary.items())
                ),
                "worker_imbalance": stage_worker_imbalance,
            }
        )

    stage_dag_edges = set()
    rdd_dag_edges = set()
    for job in jobs:
        for stage in job["stage_dag_and_rdd_lineage"]:
            stage_id = stage["stage_id"]
            stage_dag_edges.update(
                (parent_id, stage_id) for parent_id in stage["parent_stage_ids"]
            )
            for rdd in stage["rdd_lineage"]:
                rdd_dag_edges.update(
                    (parent_id, rdd["rdd_id"]) for parent_id in rdd["parent_rdd_ids"]
                )

    def largest_stage(metric_name: str) -> dict[str, Any] | None:
        candidates = [
            stage
            for stage in stage_metrics
            if stage.get(metric_name) is not None and stage.get(metric_name, 0) > 0
        ]
        if not candidates:
            return None
        stage = max(candidates, key=lambda candidate: candidate[metric_name])
        return {
            "stage_id": stage["stage_id"],
            "attempt_id": stage["attempt_id"],
            "name": stage["name"],
            metric_name: stage[metric_name],
        }

    application_worker_imbalance = worker_imbalance(application_worker_metrics)
    report = {
        "source_event_log": str(event_log),
        "application": application,
        "full_dag_evidence": {
            "jobs_with_stage_and_rdd_parent_chains": jobs,
            "stage_edges_parent_to_child": [
                {"parent_stage_id": parent, "child_stage_id": child}
                for parent, child in sorted(stage_dag_edges)
            ],
            "rdd_edges_parent_to_child": [
                {"parent_rdd_id": parent, "child_rdd_id": child}
                for parent, child in sorted(rdd_dag_edges)
            ],
            "sql_physical_plans": sql_executions,
        },
        "stage_duration_shuffle_and_worker_metrics": stage_metrics,
        "application_worker_allocation": dict(sorted(application_worker_metrics.items())),
        "application_worker_imbalance": application_worker_imbalance,
        "performance_bottleneck_candidates": {
            "longest_stage": largest_stage("stage_duration_ms"),
            "largest_shuffle_read_stage": largest_stage("shuffle_read_bytes"),
            "largest_shuffle_write_stage": largest_stage("shuffle_write_bytes"),
            "largest_disk_spill_stage": largest_stage("disk_spilled_bytes"),
        },
        "skew_assessment": {
            "task_criterion": "A task-duration, input-record, or shuffle-read maximum at least 3x its positive median is flagged.",
            "worker_criterion": "For each stage and the whole application, zero allocation on one registered worker or a maximum at least 3x the other worker is flagged.",
            "task_findings": task_skew_findings,
            "worker_findings": worker_skew_findings,
            "skew_detected": bool(
                task_skew_findings
                or worker_skew_findings
                or application_worker_imbalance["signals"]
            ),
        },
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Execution metrics written to {OUTPUT}")


if __name__ == "__main__":
    main()
