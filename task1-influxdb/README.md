# Distributed Time-Series Data Management

This component implements a high-throughput time-series database architecture using InfluxDB to manage the Weather in Szeged dataset.

## Architecture & Tech Stack
- **Database:** InfluxDB 2.7
- **Ingestion:** Python (using `influxdb-client`)
- **Query Language:** Flux
- **Deployment:** Docker & Docker Compose

## Features
- **Automated Ingestion:** Parses historical climate CSV data, maps them to InfluxDB Line Protocol, and preserves original timestamps.
- **Sliding Hourly Averages:** Computes temperature means over rolling windows.
- **Anomaly Detection:** Identifies observations falling outside two standard deviations from the dataset mean.
- **Continuous Downsampling:** Summarizes historical data into an auxiliary bucket with an explicit 30-day retention policy.

## Getting Started

### 1. Start the Database
From this directory, spin up the InfluxDB container:
```bash
docker compose up --detach --wait --wait-timeout 900 influxdb
```

### 2. Configure & Ingest Data
Run the setup script to create buckets and the downsampling task, then build and run the Python ingestion container:
```bash
docker compose run --rm --no-deps --no-TTY influx-setup
docker compose run --build --rm --no-deps --no-TTY climate-ingest
```

## Usage

Access the InfluxDB User Interface at `http://localhost:8086`.

**Credentials:**
- **Username:** `coursework`
- **Password:** `Coursework-InfluxDB-2026`
- **Organization:** `big-data-coursework`
- **Source Bucket:** `climate_raw`

### Schema Mapping
The ingested CSV data is structured into the `weather` measurement as follows:
- **Tags:** `location`
- **Fields:** `temperature_c`, `apparent_temperature_c`, `humidity`, `wind_speed_kmh`, `wind_bearing_degrees`, `visibility_km`, `cloud_cover`, `pressure_millibars`, `Summary`, `Daily Summary`, `precip_type`

## Cleanup
To stop the services while preserving the ingested data:
```bash
docker compose stop
```
