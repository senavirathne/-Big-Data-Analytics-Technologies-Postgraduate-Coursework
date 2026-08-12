# Task 2: Kafka and Flink traffic telemetry

All coursework services and verification tools run in Docker. The host needs only Docker
Engine, Docker Compose 2.30.0 or newer, and enough free space for the images. No host Python,
Playwright, browser, `curl`, `jq`, Java, Maven, Kafka, or Flink installation is used.

## Run and capture evidence

Run these commands from this directory. Replace the example value with the exact lowercase
40-character Git commit being assessed, and use the same value in all three commands that
set it. The dashboard and verifier reject a missing, abbreviated, or uppercase value:

```sh
docker compose run --build --rm flink-job-build
docker compose up --build --detach --wait kafka flink-jobmanager flink-taskmanager
SOURCE_COMMIT_SHA=0123456789abcdef0123456789abcdef01234567 docker compose run --build --rm flink-dashboard-runner
SOURCE_COMMIT_SHA=0123456789abcdef0123456789abcdef01234567 docker compose up --detach --wait traffic-producer
SOURCE_COMMIT_SHA=0123456789abcdef0123456789abcdef01234567 docker compose run --build --rm task2-evidence-verifier
```

The first command creates a fresh `artifacts/traffic-window-job.jar` from the checked-out
source. The second starts one KRaft Kafka broker, one Flink JobManager, and one three-slot
TaskManager. The third uses a browser **inside the `flink-dashboard-runner` container** to
upload and submit the JAR through the Flink Web Dashboard. It records the configured
parallelism (`3`), program arguments, browser trace, HTML, and screenshots in `evidence/`.

The producer retrieves the assigned City of Austin dataset and publishes structured JSON
every two seconds. It uses `atd_device_id`, historical `read_date`, and `volume` as sensor ID,
event time, and vehicle count. The Kafka service creates `traffic-telemetry` inside the broker
with exactly three partitions and replication factor one before the producer can start.

The final command validates the JAR manifest and checksum, Kafka topology, Flink topology,
Dashboard submission, five genuine structured messages, the running job, and at least one
genuine 15-minute event-time window result aligned to the 10-minute slide. It reaches Kafka
and Flink only through the internal `telemetry-network`; even Flink TaskManager logs are read
through Flink's internal REST endpoint. A successful run writes
`evidence/task2-validation.json` and a checksum list bound to `SOURCE_COMMIT_SHA` in
`evidence/task2-evidence-manifest.json`.

The validation may take several minutes because publication is deliberately limited to one
record every two seconds. To allow more than the default 900 seconds for a genuine window,
set `TASK2_EVIDENCE_TIMEOUT_SECONDS` on the verifier invocation.

## Coursework behavior

The Java DataStream job allows 10 seconds of bounded out-of-orderness, keys events by sensor,
and prints vehicle-count totals from 15-minute sliding event-time windows that start every
10 minutes. Each result therefore covers a 15-minute interval, while overlapping results
provide the required 10-minute moving update cadence. The cadence follows event time and
watermark progress, not ten minutes of wall-clock execution. Submission is performed through
the Web Dashboard rather than Flink's command-line job submission.

Interpretation note: the supplied brief also uses the precise phrase “10-minute tumbling
window.” This implementation intentionally prioritizes its “moving total” wording and the
Austin source's 15-minute resolution. Because each source row is already a 15-minute aggregate
timestamped at its interval start, Flink treats it as one event and may include it in two
overlapping results; the job does not prorate a row across its physical measurement interval.

For an optional live view while the containers are running, open `http://localhost:8081`.
This is not needed by the containerized verifier.

Stop and remove the Task 2 containers after preserving `artifacts/` and `evidence/`:

```sh
docker compose down --volumes --remove-orphans
```
