#!/usr/bin/env python3
"""Validate Task 2 entirely within the Compose network and write evidence."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient


JOB_NAME = "Austin traffic telemetry: 10-minute sensor totals"
PROGRAM_ARGUMENTS = "--bootstrap-servers kafka:9092 --topic traffic-telemetry"
REQUIRED_MESSAGE_FIELDS = {
    "record_id",
    "sensor_id",
    "event_timestamp",
    "event_timestamp_ms",
    "vehicle_count",
}
EVIDENCE_DIR = Path("/workspace/evidence")
JAR_PATH = Path("/workspace/artifacts/traffic-window-job.jar")
SOURCE_ROOT = Path("/workspace/coursework")
FLINK_BASE_URL = os.getenv("FLINK_BASE_URL", "http://flink-jobmanager:8081").rstrip(
    "/"
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic-telemetry")
SOURCE_COMMIT_SHA = os.getenv("SOURCE_COMMIT_SHA", "")
TIMEOUT_SECONDS = int(os.getenv("TASK2_EVIDENCE_TIMEOUT_SECONDS", "900"))


def write_json(name: str, value: Any) -> None:
    (EVIDENCE_DIR / name).write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def read_json(url: str, timeout: float = 15) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def read_text(url: str, timeout: float = 30) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def wait_for_json(url: str, timeout: int = 180) -> Any:
    deadline = time.monotonic() + timeout
    last_error = "endpoint not queried"
    while time.monotonic() < deadline:
        try:
            return read_json(url)
        except Exception as error:  # Service may still be starting.
            last_error = str(error)
            time.sleep(2)
    raise TimeoutError(f"Endpoint did not become ready: {url}: {last_error}")


def validate_source_commit() -> dict[str, str]:
    if re.fullmatch(r"[0-9a-f]{40}", SOURCE_COMMIT_SHA) is None:
        raise ValueError(
            "SOURCE_COMMIT_SHA must be the full lowercase 40-hex Git commit"
        )
    return {"git_sha": SOURCE_COMMIT_SHA}


def validate_coursework_source() -> dict[str, Any]:
    required_fragments = {
        "flink-job/src/main/java/com/coursework/TrafficWindowJob.java": [
            "Duration.ofSeconds(10)",
            "TumblingEventTimeWindows.of(Time.minutes(10))",
            ".keyBy(TrafficEvent::getSensorId)",
            ".print()",
        ],
        "docker-compose.yml": [
            "KAFKA_NUM_PARTITIONS: 3",
            "--partitions 3",
            "--replication-factor 1",
            "taskmanager.numberOfTaskSlots: 3",
            "PUBLISH_INTERVAL_SECONDS: 2",
        ],
        "producer/producer.py": [
            'interval = float(os.environ["PUBLISH_INTERVAL_SECONDS"])',
            '"sensor_id": str(row["atd_device_id"])',
            '"event_timestamp_ms": int(event_time.timestamp() * 1000)',
            '"vehicle_count": int(float(row["volume"]))',
            "time.sleep(remaining_interval)",
        ],
    }
    hashes: dict[str, str] = {}
    for relative_path, fragments in required_fragments.items():
        path = SOURCE_ROOT / relative_path
        source = path.read_text(encoding="utf-8")
        missing = [fragment for fragment in fragments if fragment not in source]
        if missing:
            raise AssertionError(
                f"Required Task 2 source semantics are absent from {relative_path}: {missing!r}"
            )
        hashes[relative_path] = sha256(path)
    return {"required_semantics_present": True, "source_sha256": hashes}


def validate_jar() -> dict[str, Any]:
    if not JAR_PATH.is_file() or JAR_PATH.stat().st_size == 0:
        raise AssertionError(f"Missing shaded Flink JAR: {JAR_PATH}")
    jar_hash = hashlib.sha256(JAR_PATH.read_bytes()).hexdigest()
    with zipfile.ZipFile(JAR_PATH) as archive:
        manifest = archive.read("META-INF/MANIFEST.MF").decode(
            "utf-8", errors="replace"
        )
    if "Main-Class: com.coursework.TrafficWindowJob" not in manifest:
        raise AssertionError("The JAR manifest does not contain the required main class")
    (EVIDENCE_DIR / "flink-job-sha256.txt").write_text(
        f"{jar_hash}  {JAR_PATH.name}\n", encoding="utf-8"
    )
    (EVIDENCE_DIR / "flink-job-manifest.txt").write_text(
        manifest, encoding="utf-8"
    )
    return {"file": JAR_PATH.name, "sha256": jar_hash, "bytes": JAR_PATH.stat().st_size}


def validate_kafka_topic() -> dict[str, Any]:
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="task2-evidence-admin",
        request_timeout_ms=30_000,
    )
    try:
        topics = admin.describe_topics([KAFKA_TOPIC])
    finally:
        admin.close()
    if len(topics) != 1:
        raise AssertionError(f"Expected one Kafka topic description, got {topics!r}")
    topic = topics[0]
    partitions = topic.get("partitions", [])
    if len(partitions) != 3:
        raise AssertionError(f"Expected exactly 3 partitions, got {len(partitions)}")
    replica_counts = [len(partition.get("replicas", [])) for partition in partitions]
    if replica_counts != [1, 1, 1]:
        raise AssertionError(
            f"Expected replication factor 1 for every partition, got {replica_counts}"
        )
    lines = [
        f"Topic: {KAFKA_TOPIC}\tPartitionCount: 3\tReplicationFactor: 1"
    ]
    for partition in partitions:
        replicas = ",".join(str(replica) for replica in partition["replicas"])
        lines.append(
            f"Topic: {KAFKA_TOPIC}\tPartition: {partition['partition']}"
            f"\tLeader: {partition['leader']}\tReplicas: {replicas}"
        )
    (EVIDENCE_DIR / "kafka-topic-description.txt").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    return {
        "name": KAFKA_TOPIC,
        "partition_count": len(partitions),
        "replication_factor_by_partition": replica_counts,
    }


def validate_flink_cluster() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    overview = wait_for_json(f"{FLINK_BASE_URL}/overview")
    taskmanagers_document = read_json(f"{FLINK_BASE_URL}/taskmanagers")
    taskmanagers = taskmanagers_document.get("taskmanagers", [])
    write_json("flink-cluster-overview.json", overview)
    write_json("flink-taskmanagers.json", taskmanagers_document)
    if overview.get("taskmanagers") != 1 or overview.get("slots-total") != 3:
        raise AssertionError(f"Unexpected Flink cluster overview: {overview!r}")
    if len(taskmanagers) != 1 or taskmanagers[0].get("slotsNumber") != 3:
        raise AssertionError(f"Unexpected Flink TaskManager topology: {taskmanagers!r}")
    return overview, taskmanagers


def validate_dashboard_audit() -> tuple[dict[str, Any], str]:
    audit_path = EVIDENCE_DIR / "flink-dashboard-audit.json"
    if not audit_path.is_file():
        raise AssertionError("Dashboard audit is missing; deploy through the dashboard first")
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    expected = {
        "source_commit": SOURCE_COMMIT_SHA,
        "jar": JAR_PATH.name,
        "parallelism": 3,
        "program_arguments": PROGRAM_ARGUMENTS,
        "entry_class": "com.coursework.TrafficWindowJob",
    }
    mismatches = {
        key: {"expected": value, "actual": audit.get(key)}
        for key, value in expected.items()
        if audit.get(key) != value
    }
    if mismatches:
        raise AssertionError(f"Dashboard audit mismatch: {mismatches!r}")
    if audit.get("run_request", {}).get("status") != 200:
        raise AssertionError(f"Dashboard submission was not successful: {audit!r}")
    job_id = audit.get("job_id", "")
    if not isinstance(job_id, str) or len(job_id) != 32:
        raise AssertionError(f"Dashboard audit has an invalid Flink job ID: {job_id!r}")
    return audit, job_id


def collect_messages(required: int = 5) -> tuple[list[dict[str, Any]], list[int]]:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="task2-evidence-consumer",
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=30_000,
    )
    captured: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    selected_deltas: list[int] = []
    deadline = time.monotonic() + 60
    try:
        # Begin at the current end of every partition. The evidence therefore consists
        # only of records produced live after this verifier joins, never stale records
        # retained from an earlier run.
        consumer.poll(timeout_ms=5_000)
        partitions = consumer.assignment()
        if len(partitions) != 3:
            raise AssertionError(
                f"Expected assignments for 3 partitions, got {sorted(partitions)!r}"
            )
        consumer.seek_to_end(*partitions)
        while time.monotonic() < deadline:
            records = consumer.poll(timeout_ms=2_000, max_records=1_000)
            for partition_records in records.values():
                for record in partition_records:
                    message = json.loads(record.value.decode("utf-8"))
                    missing = REQUIRED_MESSAGE_FIELDS.difference(message)
                    if missing:
                        raise AssertionError(
                            f"Kafka message is missing structured fields: {sorted(missing)}"
                        )
                    if not isinstance(message["vehicle_count"], int) or not isinstance(
                        message["event_timestamp_ms"], int
                    ):
                        raise AssertionError(
                            "vehicle_count and event_timestamp_ms must be JSON integers"
                        )
                    captured.append(
                        {
                            "partition": record.partition,
                            "offset": record.offset,
                            "broker_timestamp_ms": record.timestamp,
                            "key": (
                                record.key.decode("utf-8") if record.key is not None else None
                            ),
                            "value": message,
                        }
                    )
            chronological = sorted(
                captured, key=lambda record: record["broker_timestamp_ms"]
            )
            for start in range(max(0, len(chronological) - required * 2), len(chronological)):
                candidate = chronological[start : start + required]
                if len(candidate) != required:
                    continue
                record_ids = [record["value"]["record_id"] for record in candidate]
                event_times = [
                    record["value"]["event_timestamp_ms"] for record in candidate
                ]
                deltas = [
                    later["broker_timestamp_ms"] - earlier["broker_timestamp_ms"]
                    for earlier, later in zip(candidate, candidate[1:])
                ]
                if (
                    len(set(record_ids)) == required
                    and event_times == sorted(event_times)
                    and all(1_500 <= delta <= 10_000 for delta in deltas)
                ):
                    selected = candidate
                    selected_deltas = deltas
                    break
            if selected:
                break
    finally:
        consumer.close()
    if not selected:
        raise AssertionError(
            f"Did not observe {required} live, unique messages at the required "
            f"two-second interval within 60 seconds; observed {len(captured)} records"
        )
    with (EVIDENCE_DIR / "kafka-five-genuine-messages.jsonl").open(
        "w", encoding="utf-8"
    ) as output:
        for record in selected:
            output.write(json.dumps(record, sort_keys=True) + "\n")
    return selected, selected_deltas


def validate_running_job(job_id: str) -> dict[str, Any]:
    jobs_overview = read_json(f"{FLINK_BASE_URL}/jobs/overview")
    details = read_json(f"{FLINK_BASE_URL}/jobs/{job_id}")
    config = read_json(f"{FLINK_BASE_URL}/jobs/{job_id}/config")
    write_json("flink-jobs-overview.json", jobs_overview)
    write_json("flink-job-details.json", details)
    write_json("flink-job-config.json", config)
    if details.get("state") != "RUNNING" or details.get("name") != JOB_NAME:
        raise AssertionError(f"Required Flink job is not RUNNING: {details!r}")
    execution_config = config.get("execution-config", {})
    configured_parallelism = execution_config.get("job-parallelism")
    if configured_parallelism not in (3, "3"):
        raise AssertionError(
            f"Dashboard did not submit with parallelism 3: {config!r}"
        )
    return details


def window_documents(lines: list[str]) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    decoder = json.JSONDecoder()
    for line in lines:
        start = line.find('{"sensor_id"')
        if start == -1:
            continue
        document, _ = decoder.raw_decode(line[start:])
        if document.get("window_end_ms", 0) - document.get("window_start_ms", 0) != 600_000:
            raise AssertionError(f"Window is not exactly 10 minutes: {document!r}")
        if not isinstance(document.get("vehicle_count_total"), int):
            raise AssertionError(f"Window total is not an integer: {document!r}")
        documents.append(document)
    return documents


def wait_for_window_result(job_id: str, taskmanager_id: str) -> list[dict[str, Any]]:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    stdout_url = f"{FLINK_BASE_URL}/taskmanagers/{taskmanager_id}/stdout"
    log_url = f"{FLINK_BASE_URL}/taskmanagers/{taskmanager_id}/log"
    while time.monotonic() < deadline:
        # DataStream.print() writes to TaskManager stdout. Include the configured
        # TaskManager log as well; Compose redirects System.out to that log so it is
        # available even when the foreground container has no separate stdout file.
        try:
            stdout = read_text(stdout_url)
        except Exception as error:
            stdout = f"TaskManager stdout endpoint unavailable: {error}"
        taskmanager_log = read_text(log_url)
        combined_log = (
            "===== TaskManager stdout =====\n"
            f"{stdout}\n"
            "===== TaskManager log =====\n"
            f"{taskmanager_log}"
        )
        (EVIDENCE_DIR / "flink-taskmanager-live.log").write_text(
            combined_log, encoding="utf-8"
        )
        result_lines = [
            line
            for line in combined_log.splitlines()
            if '"vehicle_count_total"' in line
        ]
        results = window_documents(result_lines)
        if results:
            (EVIDENCE_DIR / "flink-window-results.log").write_text(
                "\n".join(result_lines) + "\n", encoding="utf-8"
            )
            with (EVIDENCE_DIR / "flink-window-results.jsonl").open(
                "w", encoding="utf-8"
            ) as output:
                for document in results:
                    output.write(json.dumps(document, sort_keys=True) + "\n")
            return results
        state = read_json(f"{FLINK_BASE_URL}/jobs/{job_id}").get("state")
        if state != "RUNNING":
            raise AssertionError(
                f"Flink job left RUNNING while waiting for a window result: {state}"
            )
        time.sleep(5)
    raise TimeoutError(
        f"No genuine 10-minute event-time result appeared within {TIMEOUT_SECONDS} seconds"
    )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as content:
        for block in iter(lambda: content.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_evidence_manifest(status: str, checks: dict[str, Any]) -> None:
    files: list[dict[str, Any]] = []
    for path in sorted(EVIDENCE_DIR.iterdir()):
        if path.is_file() and path.name != "task2-evidence-manifest.json":
            files.append(
                {
                    "path": f"evidence/{path.name}",
                    "bytes": path.stat().st_size,
                    "sha256": sha256(path),
                }
            )
    if JAR_PATH.is_file():
        files.append(
            {
                "path": f"artifacts/{JAR_PATH.name}",
                "bytes": JAR_PATH.stat().st_size,
                "sha256": sha256(JAR_PATH),
            }
        )
    write_json(
        "task2-evidence-manifest.json",
        {
            "task": 2,
            "status": status,
            "source_commit": SOURCE_COMMIT_SHA,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "execution_boundary": "Docker Compose services only",
            "checks": checks,
            "files": files,
        },
    )


def main() -> None:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    checks: dict[str, Any] = {}
    try:
        checks["source_commit"] = validate_source_commit()
        checks["coursework_source"] = validate_coursework_source()
        checks["jar"] = validate_jar()
        checks["kafka_topic"] = validate_kafka_topic()
        overview, taskmanagers = validate_flink_cluster()
        checks["flink_cluster"] = {
            "taskmanagers": overview["taskmanagers"],
            "slots_total": overview["slots-total"],
        }
        audit, job_id = validate_dashboard_audit()
        checks["dashboard_deployment"] = {
            "job_id": job_id,
            "method": audit["deployment_method"],
            "parallelism": audit["parallelism"],
            "program_arguments": audit["program_arguments"],
        }
        messages, timestamp_deltas = collect_messages()
        checks["genuine_messages"] = {
            "count": len(messages),
            "structured_json": True,
            "producer_interval_seconds": 2,
            "broker_timestamp_deltas_ms": timestamp_deltas,
        }
        details = validate_running_job(job_id)
        checks["running_job"] = {"name": details["name"], "state": details["state"]}
        results = wait_for_window_result(job_id, taskmanagers[0]["id"])
        checks["ten_minute_window"] = {
            "result_count": len(results),
            "vehicle_count_total_present": True,
            "window_duration_ms": 600_000,
        }
        write_json("task2-validation.json", {"status": "PASS", "checks": checks})
        write_evidence_manifest("PASS", checks)
    except Exception as error:
        checks["failure"] = {"type": type(error).__name__, "message": str(error)}
        write_json("task2-validation.json", {"status": "FAIL", "checks": checks})
        write_evidence_manifest("FAIL", checks)
        raise
    print(json.dumps({"status": "PASS", "checks": checks}, indent=2))


if __name__ == "__main__":
    main()
