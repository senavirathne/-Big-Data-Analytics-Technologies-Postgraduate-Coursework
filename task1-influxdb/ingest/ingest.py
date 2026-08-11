import csv
import io
import math
import os
from datetime import UTC, datetime
from typing import Iterator, TextIO
from urllib.request import Request, urlopen

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


ASSIGNED_DATASET_URL = (
    "https://data.snap.uaf.edu/data/Base/Other/historical_winds_Alaska_airports/"
    "alaska_airports_hourly_winds_PAFA.csv"
)
EXPECTED_COLUMNS = ["ts", "ws", "wd"]
EXPECTED_RECORD_COUNT = 345_587
MEASUREMENT = "airport_wind"
STATION = "PAFA"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
BATCH_SIZE = 5_000


def source_url() -> str:
    configured = os.environ.get("WIND_CSV_URL", ASSIGNED_DATASET_URL)
    if configured != ASSIGNED_DATASET_URL:
        raise RuntimeError(
            "WIND_CSV_URL must be the assigned PAFA historical-winds CSV; "
            "alternate datasets are not accepted"
        )
    return configured


def csv_rows(url: str) -> Iterator[dict[str, str]]:
    request = Request(
        url,
        headers={
            "Accept": "text/csv",
            "User-Agent": "Coventry-Big-Data-Coursework-Task-1/1.0",
        },
    )
    with urlopen(request, timeout=120) as response:
        content_type = response.headers.get_content_type().lower()
        if content_type not in {"text/csv", "application/csv", "text/plain"}:
            raise RuntimeError(
                f"assigned dataset returned {content_type!r}, not a CSV content type"
            )

        stream: TextIO = io.TextIOWrapper(response, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(stream)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise RuntimeError(
                f"unexpected CSV columns {reader.fieldnames!r}; expected {EXPECTED_COLUMNS!r}"
            )
        yield from reader


def finite_float(value: str, column: str, *, required: bool) -> float | None:
    candidate = value.strip()
    if not candidate:
        if required:
            raise ValueError(f"{column} is empty")
        return None

    try:
        number = float(candidate)
    except ValueError as error:
        raise ValueError(f"{column} is not numeric: {value!r}") from error
    if not math.isfinite(number):
        raise ValueError(f"{column} is not finite: {value!r}")
    return number


def point_for_row(row: dict[str, str]) -> Point:
    if set(row) != set(EXPECTED_COLUMNS):
        raise ValueError(f"row does not match the expected columns: {row!r}")

    try:
        timestamp = datetime.strptime(row["ts"].strip(), TIMESTAMP_FORMAT).replace(tzinfo=UTC)
    except ValueError as error:
        raise ValueError(f"invalid original timestamp: {row['ts']!r}") from error

    wind_speed = finite_float(row["ws"], "ws", required=True)
    wind_direction = finite_float(row["wd"], "wd", required=False)

    point = (
        Point(MEASUREMENT)
        .tag("station", STATION)
        .field("ws", wind_speed)
        .time(timestamp, WritePrecision.NS)
    )
    if wind_direction is not None:
        point.field("wd", wind_direction)
    return point


def flush(write_api, line_protocol: list[str], bucket: str, org: str) -> None:
    if not line_protocol:
        return
    write_api.write(
        bucket=bucket,
        org=org,
        record="\n".join(line_protocol),
        write_precision=WritePrecision.NS,
    )
    line_protocol.clear()


def main() -> None:
    influx_url = os.environ["INFLUX_URL"]
    influx_org = os.environ["INFLUX_ORG"]
    influx_bucket = os.environ["INFLUX_BUCKET"]
    influx_token = os.environ["INFLUX_TOKEN"]

    written = 0
    batch: list[str] = []

    with InfluxDBClient(url=influx_url, token=influx_token, org=influx_org) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        for line_number, row in enumerate(csv_rows(source_url()), start=2):
            try:
                point = point_for_row(row)
            except ValueError as error:
                raise RuntimeError(f"CSV line {line_number}: {error}") from error

            batch.append(point.to_line_protocol(precision=WritePrecision.NS))
            written += 1
            if len(batch) == BATCH_SIZE:
                flush(write_api, batch, influx_bucket, influx_org)

        flush(write_api, batch, influx_bucket, influx_org)

    if written != EXPECTED_RECORD_COUNT:
        raise RuntimeError(
            f"assigned CSV supplied {written} rows; expected {EXPECTED_RECORD_COUNT}"
        )
    print(f"ingested {written} PAFA historical wind records")


if __name__ == "__main__":
    main()
