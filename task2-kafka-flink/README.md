# Task 2: Kafka and Flink traffic telemetry

Build the uploadable Java job JAR and start the fully containerized KRaft Kafka broker,
Austin producer, Flink JobManager, and TaskManager:

```sh
docker compose up --build
```

The Kafka service's Compose `post_start` hook waits for the broker and then runs
`kafka-topics.sh --create` **inside the running broker container**. The broker is not marked
healthy until `traffic-telemetry` has exactly three partitions and replication factor one,
so the producer cannot start before the topic is ready. This hook requires Docker Compose
2.30.0 or newer. Verify the topic from the same Kafka container:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic traffic-telemetry
```

The producer retrieves the assigned City of Austin dataset, emits structured JSON every two
seconds, and uses `atd_device_id`, historical `read_date`, and `volume` as sensor ID, event
time, and vehicle count.

The `flink-job-build` container writes the shaded job bundle to
`artifacts/traffic-window-job.jar`. Deploy that bundle through the Flink console dashboard:

1. Open `http://localhost:8081` and select **Submit New Job**.
2. Select **Add New**, then upload `artifacts/traffic-window-job.jar`.
3. Select the uploaded JAR. Set **Program Arguments** to
   `--bootstrap-servers kafka:9092 --topic traffic-telemetry`, set parallelism to `3`, and
   submit the job.

The Java DataStream job explicitly allows 10 seconds of bounded out-of-orderness, keys events
by sensor, and continuously prints moving vehicle-count totals from 10-minute tumbling
event-time windows. No command-line submission is performed; the steps above are the job
deployment through the Flink console dashboard required by the brief.

Window output is written by the print sink in the TaskManager logs:

```sh
docker compose logs --follow flink-taskmanager
```
