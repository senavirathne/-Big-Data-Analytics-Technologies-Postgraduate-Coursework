# Task 1: Distributed time-series data management with InfluxDB

This directory implements the three Task 1 requirements using InfluxDB 2.x and the
Szeged Weather dataset. Docker Engine and Docker Compose are the only host requirements.

## Step 1.1: Containerized environment

[`docker-compose.yml`](docker-compose.yml) defines:

- an InfluxDB 2.7 container exposed at <http://localhost:8086>;
- persistent host storage at `./data/influxdb2`, mounted at `/var/lib/influxdb2`;
- automatic setup of the `big-data-coursework` organization and the unlimited-retention
  `climate_raw` bucket;
- an `influx-setup` service that creates the `climate_30d` auxiliary bucket with an
  explicit `720h` (30-day) retention rule and registers the required recurring
  downsampling task; and
- a `climate-ingest` service that downloads and ingests the assigned dataset.

Start the environment from this directory:

```sh
docker compose up --build
```

## Step 1.2: Szeged schema and ingestion

The implementation uses the coursework's [Weather in Szeged 2006–2016](https://www.kaggle.com/datasets/budincsevity/szeged-weather/data)
dataset. The ingestion service downloads the official Kaggle ZIP, opens
`weatherHistory.csv`, and parses its 96,453 records line by line.

Each source timestamp includes either a `+0100` or `+0200` offset. The importer parses
that offset and writes the corresponding UTC instant at nanosecond precision; it never
substitutes the container's current clock.

The InfluxDB schema is:

| CSV value | InfluxDB mapping |
|---|---|
| Szeged, Hungary | tag `location=Szeged` |
| `Formatted Date` | point timestamp |
| `Temperature (C)` | field `temperature_c` |
| `Apparent Temperature (C)` | field `apparent_temperature_c` |
| `Humidity` | field `humidity` |
| `Wind Speed (km/h)` | field `wind_speed_kmh` |
| `Wind Bearing (degrees)` | field `wind_bearing_degrees` |
| `Visibility (km)` | field `visibility_km` |
| `Loud Cover` | field `cloud_cover` |
| `Pressure (millibars)` | field `pressure_millibars` |
| `Summary`, `Precip Type`, `Daily Summary` | string fields with normalized names |

All values use the `weather` measurement and are written through the official Python
`influxdb-client` in batches of 5,000 line-protocol records. Successful ingestion prints:

```text
ingested 96453 Szeged weather records
```

The source contains 24 byte-identical duplicate rows. InfluxDB's measurement, tag, and
timestamp identity makes those writes idempotent, leaving 96,429 distinct weather points
without changing any source timestamps.

## Step 1.3: Flux analytical queries

The `flux/` directory contains exactly the three required operations:

1. `01_sliding_hourly_average.flux` calculates sliding one-hour temperature averages
   with a 15-minute window stride across the full 2006–2016 observation span.
2. `02_two_sigma_anomalies.flux` calculates the population mean and standard deviation
   for temperature, then returns observations strictly outside `mean ± 2σ`.
3. `03_continuous_downsample.flux` runs every hour and writes hourly temperature means
   into the `climate_30d` bucket.

Execute the first two analytical queries with the InfluxDB CLI:

```sh
docker compose exec influxdb influx query \
  --org big-data-coursework \
  --token coursework-influx-token \
  --file /queries/01_sliding_hourly_average.flux

docker compose exec influxdb influx query \
  --org big-data-coursework \
  --token coursework-influx-token \
  --file /queries/02_two_sigma_anomalies.flux
```

Confirm the recurring downsampling task and the 30-day bucket rule:

```sh
docker compose exec influxdb influx task list \
  --org big-data-coursework \
  --token coursework-influx-token

docker compose exec influxdb influx bucket list \
  --org big-data-coursework \
  --token coursework-influx-token
```

The dataset ends in 2016. Therefore, its original historical timestamps cannot remain in
a bucket that retains only the most recent 30 days. The recurring task is correctly
configured for new in-window data; the unlimited `climate_raw` bucket preserves the
complete historical dataset as required.
