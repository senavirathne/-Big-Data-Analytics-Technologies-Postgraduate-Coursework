# Task 1: Distributed time-series data management with InfluxDB

This directory is the complete Task 1 implementation. Every application runtime and
third-party dependency runs in Docker. The only host requirements are Docker Engine and
Docker Compose.

## Dataset and schema

The ingestion container streams exactly this assigned dataset:

<https://data.snap.uaf.edu/data/Base/Other/historical_winds_Alaska_airports/alaska_airports_hourly_winds_PAFA.csv>

The source has the columns `ts`, `ws`, and `wd`, representing the original timestamp,
wind speed, and wind direction for airport station PAFA. It contains 345,587 observations
from `1980-01-01 08:00:00` through `2019-12-31 00:00:00`. Some source rows have no `wd`;
those points retain `ws` and omit only the absent field.

InfluxDB requires an absolute instant. Because the source supplies no offset, the importer
encodes the displayed `ts` clock value as UTC. It never calls the deployment clock. Each
row becomes an `airport_wind` point tagged with `station=PAFA`, with numeric `ws` and,
where present, `wd` fields. Writes use the official Python `influxdb-client`, explicit
nanosecond timestamps, and batches of 5,000 line-protocol records.

## Provision and ingest

From this directory, run:

```sh
docker compose up --build
```

The Compose topology:

- runs InfluxDB 2.7 on `http://localhost:8086`;
- binds `./data/influxdb2` on the host to `/var/lib/influxdb2` in the container;
- automatically bootstraps the `big-data-coursework` organization and primary
  `climate_raw` bucket;
- creates the auxiliary `climate_30d` bucket with an explicit `720h` (30-day) retention
  rule; and
- streams, validates, parses, converts, batches, and writes every CSV row from the exact
  URL above; then executes and verifies all three required Flux operations in a dedicated
  container.

Successful ingestion ends with:

```text
ingested 345587 PAFA historical wind records
```

Successful containerized verification ends with:

```text
PASS: all three Flux operations executed; task and 30-day retention verified
```

Keep InfluxDB running and use a second terminal for the commands below.

## The three required Flux operations

There are exactly three coursework Flux scripts in `flux/`.

1. `01_sliding_hourly_average.flux` calculates one-hour moving means every 15 minutes
   across the full historical observation span.
2. `02_two_sigma_anomalies.flux` calculates the full-dataset mean and population standard
   deviation independently for each source field, then returns values strictly outside
   `mean ± (2 * standard deviation)`.
3. `03_continuous_downsample.flux` is registered automatically as the hourly recurring
   task `climate-hourly-downsample-30d`; it continuously writes one-hour means for each
   new task window to `climate_30d`.

InfluxDB evaluates retention against its current clock. The assigned observations end in
2019, so in 2026 their downsampled timestamps are already outside a 30-day retention
policy and cannot remain in `climate_30d`. The recurring task therefore uses the standard
relative `-task.every` input window: it is active and correct for in-window records, while
the complete 1980–2019 history remains in the unlimited-retention primary bucket. The
pipeline deliberately does not relabel old source records with current timestamps merely
to bypass retention.

The verifier executes these operations automatically. They can also be repeated manually
with the InfluxDB CLI inside its container:

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

Confirm the third operation and its auxiliary bucket retention policy:

```sh
docker compose exec influxdb influx task list \
  --org big-data-coursework \
  --token coursework-influx-token

docker compose exec influxdb influx bucket list \
  --org big-data-coursework \
  --token coursework-influx-token
```

For an immediate execution of the same downsampling operation, without waiting for
its next hourly schedule, run:

```sh
docker compose exec influxdb influx query \
  --org big-data-coursework \
  --token coursework-influx-token \
  --file /queries/03_continuous_downsample.flux
```
