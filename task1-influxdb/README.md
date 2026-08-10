# Task 1: InfluxDB time-series pipeline

Run the entirely containerized stack from this directory:

```sh
docker compose up --build
```

InfluxDB is available at `http://localhost:8086`. The Compose bootstrap creates the
`big-data-coursework` organization and `climate_raw` bucket. The setup container creates
the auxiliary `climate_30d` bucket with a 720-hour (30-day) retention rule and registers
the continuous hourly downsampling task.

The ingestion container first looks for the assigned timestamped file at
`data/fairbanks_climate.csv`. It ignores blank/comment lines, parses records line by line,
converts them to InfluxDB Line Protocol, and uses only timestamps explicitly present in the
source (`timestamp`/`date` or complete year-month-day fields).

As verified on 2026-08-10, the PDF's exact assigned URL currently serves HTML. The importer
then checks SNAP's current Fairbanks export endpoint. That export contains monthly
climatological and decadal aggregates (`Historical`, `2030-2039`, and similar ranges), not
original timestamped observations, so the importer deliberately fails instead of assigning
made-up dates or the deployment clock. Strict ingestion therefore requires the original
assigned timestamped CSV at the path above.

Execute the two direct Flux queries inside the InfluxDB container:

```sh
docker compose exec influxdb influx query --org big-data-coursework --token coursework-influx-token --file /queries/01_sliding_hourly_average.flux
docker compose exec influxdb influx query --org big-data-coursework --token coursework-influx-token --file /queries/02_two_sigma_anomalies.flux
```

Confirm the third operation, the continuously scheduled downsampling task:

```sh
docker compose exec influxdb influx task list --org big-data-coursework --token coursework-influx-token
```
