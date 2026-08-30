# Real-Time Stream Ingestion & Processing

This component implements a localized stream processing infrastructure using Apache Kafka and Apache Flink to process live telemetry data.

## Architecture & Tech Stack
- **Message Broker:** Apache Kafka (KRaft mode, 3 partitions, replication factor 1)
- **Stream Processing Engine:** Apache Flink (1 JobManager, 1 TaskManager)
- **Producer:** Python script replaying historical Austin traffic records
- **Consumer/Job:** Java Flink Job (`TrafficWindowJob`)
- **Deployment:** Docker & Docker Compose

## Features
- **Live Stream Simulation:** A Python producer application pushes structured JSON traffic-count records into Kafka every 2 seconds.
- **Stateful Processing:** A Flink pipeline consumes the stream, applying a `Bounded-OutOf-Orderness` watermarking strategy with a 10-second tolerance.
- **Tumbling Windows:** Aggregates non-overlapping 15-minute vehicle-count totals per sensor.

## Getting Started

### 1. Build and Start Infrastructure
From this directory, build the Flink Job JAR and start the Kafka/Flink cluster:
```bash
docker compose run --build --rm --no-deps --no-TTY flink-job-build
docker compose up --detach --wait --wait-timeout 240 kafka flink-jobmanager flink-taskmanager
```

### 2. Submit the Flink Job
1. Open the Flink Dashboard at `http://localhost:8081/#/submit`.
2. Upload the compiled `artifacts/traffic-window-job.jar`.
3. Set the Entry Class to: `com.coursework.TrafficWindowJob`
4. Set Parallelism to: `3`
5. Set Program Arguments to: `--bootstrap-servers kafka:9092 --topic traffic-telemetry`
6. Click **Submit**.

### 3. Start the Live Producer
Once the Flink job is running, start the Python traffic producer:
```bash
docker compose build traffic-producer
docker compose up --detach --no-build --no-deps traffic-producer
```

## Usage
You can monitor the live tumbling-window totals by tailing the TaskManager logs:
```bash
docker compose logs --follow flink-taskmanager
```

## Cleanup
To stop and completely remove the containers, networks, and ephemeral data:
```bash
docker compose down --remove-orphans
```
