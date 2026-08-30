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


def parse_scope(value: Any) -> Any:
    """Decode Spark's JSON-encoded RDD scope when one is available."""

    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def flatten_spark_plan(plan: Any) -> dict[str, Any]:
    """Convert a nested SparkPlanInfo tree into explicit DAG nodes and edges."""

    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, int]] = []

    def visit(node: Any, parent_node_id: int | None = None) -> None:
        if not isinstance(node, dict):
            return
        node_id = len(nodes)
        nodes.append(
            {
                "node_id": node_id,
                "node_name": node.get("nodeName"),
                "simple_string": node.get("simpleString"),
                "metadata": node.get("metadata") or {},
                "metrics": node.get("metrics") or [],
            }
        )
        if parent_node_id is not None:
            edges.append(
                {
                    "parent_node_id": parent_node_id,
                    "child_node_id": node_id,
                }
            )
        children = node.get("children") or []
        if isinstance(children, list):
            for child in children:
                visit(child, node_id)

    visit(plan)
    return {
        "root_node_id": 0 if nodes else None,
        "nodes": nodes,
        "edges_parent_to_child": edges,
    }


def record_dag_stage(
    stage: dict[str, Any],
    dag_stages: dict[int, dict[str, Any]],
    dag_rdds: dict[int, dict[str, Any]],
) -> None:
    """Retain stage dependencies and the RDD lineage attached to each stage."""

    stage_id = int(stage.get("Stage ID", -1))
    rdd_ids: list[int] = []
    for rdd in stage.get("RDD Info", []):
        rdd_id = int(rdd.get("RDD ID", -1))
        rdd_ids.append(rdd_id)
        dag_rdds[rdd_id] = {
            "rdd_id": rdd_id,
            "name": rdd.get("Name"),
            "parent_rdd_ids": [int(value) for value in rdd.get("Parent IDs", [])],
            "number_of_partitions": int(rdd.get("Number of Partitions", 0)),
            "number_of_cached_partitions": int(
                rdd.get("Number of Cached Partitions", 0)
            ),
            "storage_level": rdd.get("Storage Level") or {},
            "memory_size_bytes": int(rdd.get("Memory Size", 0)),
            "disk_size_bytes": int(rdd.get("Disk Size", 0)),
            "scope": parse_scope(rdd.get("Scope")),
            "callsite": rdd.get("Callsite"),
            "barrier": bool(rdd.get("Barrier", False)),
            "deterministic_level": rdd.get("DeterministicLevel"),
        }

    previous = dag_stages.get(stage_id, {})
    known_rdd_ids = set(previous.get("rdd_ids", []))
    known_rdd_ids.update(rdd_ids)
    dag_stages[stage_id] = {
        "stage_id": stage_id,
        "attempt_id": int(stage.get("Stage Attempt ID", 0)),
        "name": stage.get("Stage Name"),
        "details": stage.get("Details"),
        "number_of_tasks": int(stage.get("Number of Tasks", 0)),
        "parent_stage_ids": [int(value) for value in stage.get("Parent IDs", [])],
        "rdd_ids": sorted(known_rdd_ids),
    }


def sql_execution_slot(
    executions: dict[int, dict[str, Any]], execution_id: int
) -> dict[str, Any]:
    return executions.setdefault(
        execution_id,
        {
            "execution_id": execution_id,
            "root_execution_id": None,
            "description": None,
            "details": None,
            "started_at_epoch_ms": None,
            "ended_at_epoch_ms": None,
            "duration_ms": None,
            "job_ids": [],
            "initial_physical_plan": None,
            "initial_spark_plan_info": None,
            "initial_operator_dag": flatten_spark_plan(None),
            "adaptive_plan_updates": [],
        },
    )


def main() -> None:
    event_log = newest_event_log()
    application: dict[str, Any] = {}
    jobs: list[dict[str, Any]] = []
    dag_stages: dict[int, dict[str, Any]] = {}
    dag_rdds: dict[int, dict[str, Any]] = {}
    sql_execution_plans: dict[int, dict[str, Any]] = {}
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
                properties = event.get("Properties") or {}
                sql_execution_id = properties.get("spark.sql.execution.id")
                jobs.append(
                    {
                        "job_id": int(event.get("Job ID", -1)),
                        "stage_ids": [
                            int(value) for value in event.get("Stage IDs", [])
                        ],
                        "sql_execution_id": (
                            int(sql_execution_id)
                            if sql_execution_id is not None
                            else None
                        ),
                    }
                )
                for stage in stages:
                    record_dag_stage(stage, dag_stages, dag_rdds)
            elif event_name == "SparkListenerExecutorAdded":
                executor_id = str(event.get("Executor ID"))
                if executor_id != "driver":
                    info = event.get("Executor Info", {})
                    executors[executor_id] = str(info.get("Host") or "unknown")
            elif event_name == "SparkListenerStageCompleted":
                stage = event.get("Stage Info", {})
                completed_stages[stage_identity(stage)] = stage
                record_dag_stage(stage, dag_stages, dag_rdds)
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
            elif event_name == (
                "org.apache.spark.sql.execution.ui."
                "SparkListenerSQLExecutionStart"
            ):
                execution_id = int(event.get("executionId", -1))
                execution = sql_execution_slot(sql_execution_plans, execution_id)
                plan_info = event.get("sparkPlanInfo")
                execution.update(
                    {
                        "root_execution_id": event.get("rootExecutionId"),
                        "description": event.get("description"),
                        "details": event.get("details"),
                        "started_at_epoch_ms": event.get("time"),
                        "initial_physical_plan": event.get(
                            "physicalPlanDescription"
                        ),
                        "initial_spark_plan_info": plan_info,
                        "initial_operator_dag": flatten_spark_plan(plan_info),
                        "modified_configs": event.get("modifiedConfigs") or {},
                        "job_tags": event.get("jobTags") or [],
                    }
                )
            elif event_name == (
                "org.apache.spark.sql.execution.ui."
                "SparkListenerSQLAdaptiveExecutionUpdate"
            ):
                execution_id = int(event.get("executionId", -1))
                execution = sql_execution_slot(sql_execution_plans, execution_id)
                plan_info = event.get("sparkPlanInfo")
                execution["adaptive_plan_updates"].append(
                    {
                        "sequence": len(execution["adaptive_plan_updates"]) + 1,
                        "physical_plan": event.get("physicalPlanDescription"),
                        "spark_plan_info": plan_info,
                        "operator_dag": flatten_spark_plan(plan_info),
                    }
                )
            elif event_name == (
                "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"
            ):
                execution_id = int(event.get("executionId", -1))
                execution = sql_execution_slot(sql_execution_plans, execution_id)
                ended_at = event.get("time")
                execution["ended_at_epoch_ms"] = ended_at
                started_at = execution.get("started_at_epoch_ms")
                execution["duration_ms"] = (
                    int(ended_at) - int(started_at)
                    if ended_at is not None and started_at is not None
                    else None
                )

    if not application.get("application_id"):
        raise RuntimeError("Spark event log has no application ID")
    if not jobs or not completed_stages:
        raise RuntimeError("Spark event log has no completed job and stage records")
    if not dag_rdds or not sql_execution_plans:
        raise RuntimeError("Spark event log has no RDD lineage or SQL execution plans")

    for execution_id, execution in sql_execution_plans.items():
        execution["job_ids"] = sorted(
            job["job_id"]
            for job in jobs
            if job["sql_execution_id"] == execution_id
        )
        if not execution.get("initial_physical_plan"):
            raise RuntimeError(
                f"SQL execution {execution_id} has no physical plan description"
            )
        if not execution["initial_operator_dag"]["nodes"]:
            raise RuntimeError(
                f"SQL execution {execution_id} has no structured operator DAG"
            )

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
    rdd_edges = {
        (parent_id, rdd["rdd_id"])
        for rdd in dag_rdds.values()
        for parent_id in rdd["parent_rdd_ids"]
    }
    sql_plan_records = [
        sql_execution_plans[key] for key in sorted(sql_execution_plans)
    ]
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
        "rdd_lineage": {
            "rdds": [dag_rdds[key] for key in sorted(dag_rdds)],
            "edges_parent_to_child": [
                {"parent_rdd_id": parent, "child_rdd_id": child}
                for parent, child in sorted(rdd_edges)
            ],
        },
        "sql_execution_plans": sql_plan_records,
        "dag_logic_chain_summary": {
            "meaning": (
                "SQL executions link to jobs; jobs link to stages; stages list their "
                "RDDs; RDD and SQL operator edges preserve the lower-level lineage."
            ),
            "stage_count": len(dag_stages),
            "stage_edge_count": len(dag_edges),
            "rdd_count": len(dag_rdds),
            "rdd_edge_count": len(rdd_edges),
            "sql_execution_count": len(sql_plan_records),
            "initial_sql_operator_count": sum(
                len(execution["initial_operator_dag"]["nodes"])
                for execution in sql_plan_records
            ),
            "adaptive_sql_plan_update_count": sum(
                len(execution["adaptive_plan_updates"])
                for execution in sql_plan_records
            ),
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
