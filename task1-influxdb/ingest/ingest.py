import csv
import io
import itertools
import math
import os
import re
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator, TextIO
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


FULL_TIMESTAMP_COLUMNS = (
    "timestamp",
    "datetime",
    "date_time",
    "observation_timestamp",
    "observation_datetime",
    "observation_date",
    "read_date",
)
TIMESTAMP_COLUMNS = FULL_TIMESTAMP_COLUMNS + ("date", "time")
TAG_COLUMNS = {
    "station",
    "station_id",
    "location",
    "community",
    "city",
    "model",
    "scenario",
    "variable",
    "metric",
    "unit",
    "units",
}
TIME_COMPONENT_COLUMNS = {"year", "month", "day", "hour", "minute", "second"}
BATCH_SIZE = 500


class NonCsvResponse(RuntimeError):
    pass


def normalized_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def parse_datetime(value: str) -> datetime:
    candidate = value.strip()
    if not candidate:
        raise ValueError("empty timestamp")

    if re.fullmatch(r"\d{4}(?:-\d{2})?", candidate):
        raise ValueError(
            f"timestamp {value!r} has only year/month precision; refusing to invent a day"
        )

    if candidate.isdigit() and len(candidate) in (10, 13):
        epoch = int(candidate)
        if len(candidate) == 13:
            epoch /= 1000
        return datetime.fromtimestamp(epoch, tz=UTC)

    iso_candidate = candidate[:-1] + "+00:00" if candidate.endswith("Z") else candidate
    try:
        parsed = datetime.fromisoformat(iso_candidate)
    except ValueError:
        parsed = None

    if parsed is None:
        for pattern in (
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y",
        ):
            try:
                parsed = datetime.strptime(candidate, pattern)
                break
            except ValueError:
                continue

    if parsed is None:
        raise ValueError(f"unsupported historical timestamp {value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def historical_timestamp(row: dict[str, str]) -> tuple[datetime, set[str]]:
    normalized = {normalized_name(key): value for key, value in row.items() if key}

    date_value = normalized.get("date", "").strip()
    time_value = normalized.get("time", "").strip()
    if date_value and time_value:
        return parse_datetime(f"{date_value} {time_value}"), {"date", "time"}

    for column in FULL_TIMESTAMP_COLUMNS + ("date",):
        value = normalized.get(column, "")
        if value and value.strip():
            return parse_datetime(value), {column}

    year_value = normalized.get("year", "").strip()
    month_value = normalized.get("month", "").strip()
    day_value = normalized.get("day", "").strip()
    if year_value and month_value and day_value:
        hour_value = normalized.get("hour", "0").strip() or "0"
        minute_value = normalized.get("minute", "0").strip() or "0"
        second_value = normalized.get("second", "0").strip() or "0"
        timestamp = datetime(
            int(float(year_value)),
            int(float(month_value)),
            int(float(day_value)),
            int(float(hour_value)),
            int(float(minute_value)),
            int(float(second_value)),
            tzinfo=UTC,
        )
        used = {"year", "month", "day", "hour", "minute", "second"}
        return timestamp, used

    raise ValueError(
        "row has no exact historical timestamp, date, or complete year/month/day components"
    )


def numeric_value(value: str) -> float | None:
    candidate = value.strip().replace(",", "")
    if not candidate or candidate.lower() in {"na", "n/a", "nan", "null", "none", "true", "false"}:
        return None
    try:
        number = float(candidate)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def prepared_csv_lines(stream: TextIO, source_name: str) -> Iterator[str]:
    first_line = None
    for line in stream:
        stripped = line.lstrip("\ufeff").strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.lower().startswith(("<!doctype html", "<html", "<head", "<script")):
            raise NonCsvResponse(f"{source_name} returned HTML instead of CSV")
        first_line = line
        break

    if first_line is None:
        raise NonCsvResponse(f"{source_name} returned no CSV header or records")

    def remaining_data_lines() -> Iterator[str]:
        for line in stream:
            stripped = line.lstrip("\ufeff").strip()
            if stripped and not stripped.startswith("#"):
                yield line

    return itertools.chain((first_line,), remaining_data_lines())


@contextmanager
def remote_csv_source(url: str) -> Iterator[Iterator[str]]:
    request = Request(url, headers={"Accept": "text/csv,application/csv;q=0.9,*/*;q=0.1"})
    with urlopen(request, timeout=60) as response:
        content_type = response.headers.get_content_type().lower()
        if content_type == "text/html":
            raise NonCsvResponse(f"{url} returned Content-Type text/html instead of CSV")
        stream: TextIO = io.TextIOWrapper(response, encoding="utf-8-sig", newline="")
        yield prepared_csv_lines(stream, url)


@contextmanager
def csv_source() -> Iterator[Iterator[str]]:
    local_path = Path(os.environ["CLIMATE_CSV_PATH"])
    if local_path.is_file():
        with local_path.open("r", encoding="utf-8-sig", newline="") as stream:
            yield prepared_csv_lines(stream, str(local_path))
        return

    assigned_url = os.environ["CLIMATE_CSV_URL"]
    try:
        with remote_csv_source(assigned_url) as lines:
            yield lines
        return
    except (HTTPError, NonCsvResponse, URLError) as error:
        print(f"assigned Fairbanks source is unavailable as CSV: {error}")

    current_url = os.environ["CLIMATE_CURRENT_CSV_URL"]
    with remote_csv_source(current_url) as lines:
        yield lines


def validate_timestamp_schema(fieldnames: list[str]) -> None:
    names = {normalized_name(name) for name in fieldnames if name}
    is_current_aggregate_export = "daterange" in names and any(
        name.startswith("jan_") or name.startswith("janmin") or name.startswith("janmean")
        for name in names
    )
    if is_current_aggregate_export:
        raise RuntimeError(
            "the current official Fairbanks export contains monthly climatological/decadal "
            "aggregates (for example, daterange=Historical or 2030-2039), not records with "
            "original observation timestamps; strict ingestion is impossible without the "
            "assigned timestamped fairbanks_climate.csv, so no timestamps will be fabricated"
        )

    has_timestamp = bool(names.intersection(FULL_TIMESTAMP_COLUMNS + ("date",)))
    has_complete_date_parts = {"year", "month", "day"}.issubset(names)
    if not has_timestamp and not has_complete_date_parts:
        raise RuntimeError(
            "the CSV schema has no exact timestamp/date or complete year/month/day columns; "
            "refusing to substitute the deployment clock or invent timestamp components"
        )


def point_for_row(row: dict[str, str]) -> Point:
    timestamp, timestamp_parts = historical_timestamp(row)
    point = Point("fairbanks_climate")
    field_count = 0
    normalized_row = {
        normalized_name(key): value.strip()
        for key, value in row.items()
        if key is not None and value is not None
    }
    metric_name = normalized_name(
        normalized_row.get("variable", "") or normalized_row.get("metric", "")
    )

    for original_name, raw_value in row.items():
        if original_name is None or raw_value is None:
            continue
        name = normalized_name(original_name)
        value = raw_value.strip()
        if (
            not name
            or not value
            or name in timestamp_parts
            or name in TIMESTAMP_COLUMNS
            or name in TIME_COMPONENT_COLUMNS
        ):
            continue

        if name in TAG_COLUMNS:
            point.tag(name, value)
            continue

        number = numeric_value(value)
        if number is not None:
            field_name = metric_name if name == "value" and metric_name else name
            point.field(field_name, number)
            field_count += 1

    if field_count == 0:
        raise ValueError("row contains no numeric climate observation fields")
    return point.time(timestamp, WritePrecision.NS)


def flush(write_api, lines: list[str], bucket: str, org: str) -> None:
    if not lines:
        return
    write_api.write(
        bucket=bucket,
        org=org,
        record="\n".join(lines),
        write_precision=WritePrecision.NS,
    )
    lines.clear()


def main() -> None:
    influx_url = os.environ["INFLUX_URL"]
    influx_org = os.environ["INFLUX_ORG"]
    influx_bucket = os.environ["INFLUX_BUCKET"]
    influx_token = os.environ["INFLUX_TOKEN"]

    written = 0
    skipped = 0
    batch: list[str] = []

    with InfluxDBClient(url=influx_url, token=influx_token, org=influx_org) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        with csv_source() as lines:
            reader = csv.DictReader(lines)
            if not reader.fieldnames:
                raise RuntimeError("the climate CSV has no header row")
            validate_timestamp_schema(reader.fieldnames)

            for line_number, row in enumerate(reader, start=2):
                try:
                    point = point_for_row(row)
                except ValueError as error:
                    skipped += 1
                    print(f"skipping CSV line {line_number}: {error}")
                    continue

                batch.append(point.to_line_protocol(precision=WritePrecision.NS))
                written += 1
                if len(batch) >= BATCH_SIZE:
                    flush(write_api, batch, influx_bucket, influx_org)

            flush(write_api, batch, influx_bucket, influx_org)

    if written == 0:
        raise RuntimeError("no valid historical climate records were ingested")
    print(f"ingested {written} historical climate records; skipped {skipped}")


if __name__ == "__main__":
    main()
