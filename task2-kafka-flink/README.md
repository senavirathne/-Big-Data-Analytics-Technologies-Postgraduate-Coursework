# Task 2: Kafka and Flink traffic telemetry

This implementation runs entirely in Docker. The host requires Docker Engine and Docker
Compose 2.30.0 or newer; Java, Maven, Python, Kafka, and Flink are provided by containers.

## Build and start the stream platform

Run the following commands from this directory:

```sh
docker compose run --build --rm flink-job-build
docker compose up --build --detach --wait kafka flink-jobmanager flink-taskmanager
```

The first command writes `artifacts/traffic-window-job.jar`. The second starts one KRaft
Kafka broker, one Flink JobManager, and one three-slot Flink TaskManager on the shared
`telemetry-network` bridge. Kafka creates `traffic-telemetry` with three partitions and a
replication factor of one.

## Submit the job through the Flink Web Dashboard

1. Open `http://localhost:8081/#/submit`.
2. Upload `artifacts/traffic-window-job.jar`.
3. Set the entry class to `com.coursework.TrafficWindowJob`.
4. Set parallelism to `3`.
5. Set program arguments to:

   ```text
   --bootstrap-servers kafka:9092 --topic traffic-telemetry
   ```

6. Select **Submit** and confirm that the job is running.

## Start the producer

After the Flink job is running, start the Austin traffic producer:

```sh
docker compose up --build --detach traffic-producer
docker compose logs --follow traffic-producer flink-taskmanager
```

The producer publishes JSON containing `sensor_id`, `event_timestamp_ms`, and
`vehicle_count` every two seconds. The Java job allows ten seconds of out-of-order event
time, keys records by sensor, and prints totals from UTC-aligned 15-minute tumbling windows.

## Stop the platform

```sh
docker compose down --volumes --remove-orphans
```
