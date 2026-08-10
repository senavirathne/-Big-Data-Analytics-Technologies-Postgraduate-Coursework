import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator

import requests
from kafka import KafkaProducer


HEARTBEAT_PATH = Path("/tmp/producer-heartbeat")


def touch_heartbeat() -> None:
    HEARTBEAT_PATH.touch()


def austin_rows(session: requests.Session) -> Iterator[dict]:
    url = os.environ["AUSTIN_DATA_URL"]
    page_size = int(os.environ["SOCRATA_PAGE_SIZE"])
    offset = 0

    while True:
        touch_heartbeat()
        response = session.get(
            url,
            params={
                "$limit": page_size,
                "$offset": offset,
                "$order": "read_date ASC, record_id ASC",
            },
            timeout=60,
        )
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list):
            raise RuntimeError("the Austin dataset endpoint did not return a JSON row array")
        if not rows:
            return

        yield from rows
        offset += len(rows)
        if len(rows) < page_size:
            return


def utc_event_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def optional_float(row: dict, name: str) -> float | None:
    value = row.get(name)
    return float(value) if value not in (None, "") else None


def telemetry_message(row: dict) -> dict:
    event_time = utc_event_time(row["read_date"])
    return {
        "record_id": row["record_id"],
        "sensor_id": str(row["atd_device_id"]),
        "event_timestamp": event_time.isoformat().replace("+00:00", "Z"),
        "event_timestamp_ms": int(event_time.timestamp() * 1000),
        "vehicle_count": int(float(row["volume"])),
        "intersection_name": row.get("intersection_name"),
        "direction": row.get("direction"),
        "movement": row.get("movement"),
        "heavy_vehicle": row.get("heavy_vehicle", False),
        "speed_average": optional_float(row, "speed_average"),
    }


def main() -> None:
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    topic = os.environ["KAFKA_TOPIC"]
    interval = float(os.environ["PUBLISH_INTERVAL_SECONDS"])

    session = requests.Session()
    app_token = os.getenv("SOCRATA_APP_TOKEN")
    if app_token:
        session.headers["X-App-Token"] = app_token

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=10,
    )

    last_publication_at: float | None = None
    published = 0
    try:
        for row in austin_rows(session):
            try:
                message = telemetry_message(row)
            except (KeyError, TypeError, ValueError) as error:
                print(
                    f"skipping malformed Austin record {row.get('record_id', '<unknown>')}: {error}",
                    flush=True,
                )
                continue
            if last_publication_at is not None:
                remaining_interval = interval - (time.monotonic() - last_publication_at)
                if remaining_interval > 0:
                    time.sleep(remaining_interval)

            producer.send(topic, key=message["sensor_id"], value=message).get(timeout=30)
            last_publication_at = time.monotonic()
            published += 1
            touch_heartbeat()
            print(
                f"published record={message['record_id']} sensor={message['sensor_id']} "
                f"event_time={message['event_timestamp']} count={message['vehicle_count']}",
                flush=True,
            )
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)
        session.close()

    print(f"published {published} Austin traffic records", flush=True)


if __name__ == "__main__":
    main()
