import csv
import io
import math
import os
import shutil
import tempfile
import zipfile
from datetime import UTC, datetime
from time import perf_counter
from typing import Iterator
from urllib.request import Request, urlopen

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


SZEGED_DATASET_URL = (
    "https://www.kaggle.com/api/v1/datasets/download/"
    "budincsevity/szeged-weather"
)
CSV_MEMBER = "weatherHistory.csv"
EXPECTED_COLUMNS = [
    "Formatted Date",
    "Summary",
    "Precip Type",
    "Temperature (C)",
    "Apparent Temperature (C)",
    "Humidity",
    "Wind Speed (km/h)",
    "Wind Bearing (degrees)",
    "Visibility (km)",
    "Loud Cover",
    "Pressure (millibars)",
    "Daily Summary",
]
NUMERIC_FIELDS = {
    "Temperature (C)": "temperature_c",
    "Apparent Temperature (C)": "apparent_temperature_c",
    "Humidity": "humidity",
    "Wind Speed (km/h)": "wind_speed_kmh",
    "Wind Bearing (degrees)": "wind_bearing_degrees",
    "Visibility (km)": "visibility_km",
    "Loud Cover": "cloud_cover",
    "Pressure (millibars)": "pressure_millibars",
}
REQUIRED_TEXT_FIELDS = {
    "Summary": "summary",
    "Daily Summary": "daily_summary",
}
OPTIONAL_TEXT_FIELDS = {
    "Precip Type": "precip_type",
}
EXPECTED_RECORD_COUNT = 96_453
MEASUREMENT = "weather"
LOCATION = "Szeged"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f %z"
BATCH_SIZE = 5_000


def source_url() -> str:
    return os.environ.get("SZEGED_DATASET_URL", SZEGED_DATASET_URL)


def csv_rows(url: str) -> Iterator[dict[str, str]]:
    request = Request(
        url,
        headers={
            "Accept": "application/zip",
            "User-Agent": "Coventry-Big-Data-Coursework-Task-1/1.0",
        },
    )

    with urlopen(request, timeout=120) as response:
        with tempfile.SpooledTemporaryFile(max_size=4 * 1024 * 1024) as archive:
            shutil.copyfileobj(response, archive)
            archive.seek(0)

            try:
                with zipfile.ZipFile(archive) as dataset:
                    if dataset.namelist() != [CSV_MEMBER]:
                        raise RuntimeError(
                            f"unexpected Kaggle archive members {dataset.namelist()!r}; "
                            f"expected only {CSV_MEMBER!r}"
                        )

                    with dataset.open(CSV_MEMBER) as raw_csv:
                        with io.TextIOWrapper(
                            raw_csv, encoding="utf-8-sig", newline=""
                        ) as stream:
                            reader = csv.DictReader(stream)
                            if reader.fieldnames != EXPECTED_COLUMNS:
                                raise RuntimeError(
                                    f"unexpected CSV columns {reader.fieldnames!r}; "
                                    f"expected {EXPECTED_COLUMNS!r}"
                                )
                            yield from reader
            except zipfile.BadZipFile as error:
                raise RuntimeError("Kaggle did not return a valid ZIP archive") from error


def finite_float(value: str, column: str) -> float:
    candidate = value.strip()
    if not candidate:
        raise ValueError(f"{column} is empty")

    try:
        number = float(candidate)
    except ValueError as error:
        raise ValueError(f"{column} is not numeric: {value!r}") from error
    if not math.isfinite(number):
        raise ValueError(f"{column} is not finite: {value!r}")
    return number


def required_text(value: str, column: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise ValueError(f"{column} is empty")
    return candidate


def point_for_row(row: dict[str, str]) -> Point:
    if list(row) != EXPECTED_COLUMNS:
        raise ValueError(f"row does not match the expected columns: {row!r}")

    try:
        timestamp = datetime.strptime(
            row["Formatted Date"].strip(), TIMESTAMP_FORMAT
        ).astimezone(UTC)
    except ValueError as error:
        raise ValueError(
            f"invalid original timestamp: {row['Formatted Date']!r}"
        ) from error

    point = Point(MEASUREMENT).tag("location", LOCATION)
    for source_column, field_name in NUMERIC_FIELDS.items():
        point.field(field_name, finite_float(row[source_column], source_column))
    for source_column, field_name in REQUIRED_TEXT_FIELDS.items():
        point.field(field_name, required_text(row[source_column], source_column))
    for source_column, field_name in OPTIONAL_TEXT_FIELDS.items():
        value = row[source_column].strip()
        if value:
            point.field(field_name, value)

    return point.time(timestamp, WritePrecision.NS)


def flush(write_api, line_protocol: list[str], bucket: str, org: str) -> float:
    if not line_protocol:
        return 0.0

    write_started = perf_counter()
    write_api.write(
        bucket=bucket,
        org=org,
        record="\n".join(line_protocol),
        write_precision=WritePrecision.NS,
    )
    write_elapsed = perf_counter() - write_started
    line_protocol.clear()
    return write_elapsed


def main() -> None:
    influx_url = os.environ["INFLUX_URL"]
    influx_org = os.environ["INFLUX_ORG"]
    influx_bucket = os.environ["INFLUX_BUCKET"]
    influx_token = os.environ["INFLUX_TOKEN"]

    written = 0
    write_elapsed = 0.0
    batch: list[str] = []
    pipeline_started = perf_counter()

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
                write_elapsed += flush(write_api, batch, influx_bucket, influx_org)

        write_elapsed += flush(write_api, batch, influx_bucket, influx_org)

    pipeline_elapsed = perf_counter() - pipeline_started

    if written != EXPECTED_RECORD_COUNT:
        raise RuntimeError(
            f"Szeged CSV supplied {written} rows; expected {EXPECTED_RECORD_COUNT}"
        )
    print(f"ingested {written} Szeged weather records")
    if write_elapsed > 0.0:
        print(
            f"InfluxDB write throughput: {written / write_elapsed:,.0f} records/s "
            f"across {write_elapsed:.3f} s of synchronous writes"
        )
    if pipeline_elapsed > 0.0:
        print(
            f"end-to-end pipeline throughput: {written / pipeline_elapsed:,.0f} "
            f"records/s across {pipeline_elapsed:.3f} s"
        )


if __name__ == "__main__":
    main()
