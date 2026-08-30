# Big Data Analytics Technologies

This repository contains an end-to-end containerized big data architecture demonstrating time-series data management, real-time stream processing, distributed batch analytics, and graph database engineering.

## Architecture & Tech Stack

The project is divided into four distinct data processing components:
- **Task 1 — Time-Series Database:** [InfluxDB](task1-influxdb/) for high-throughput sensor data ingestion and Flux-based analytical downsampling.
- **Task 2 — Real-Time Streaming:** [Apache Kafka & Apache Flink](task2-kafka-flink/) for stateful stream processing and out-of-order event handling with tumbling windows.
- **Task 3 — Distributed Batch Processing:** [Apache Spark](task3-spark/) cluster for large-scale graph data aggregation (ETL) and in-degree calculations.
- **Task 4 — Graph Database:** [Neo4j](task4-neo4j/) for dense relational graph network storage and Cypher-based shortest-path querying.

## Prerequisites

- **Docker Desktop:** Configured to use the **WSL 2** backend.
- **Windows Subsystem for Linux (WSL 2):** Version 2.1.5 or newer.
- **Docker Compose:** Version 2.30.0 or newer.

*Note: All tasks are fully containerized. No native host OS dependencies (like local Python or Java installations) are required.*

## Getting Started

Each component is isolated in its own directory with a dedicated `docker-compose.yml` file. Because these big data frameworks are memory-intensive (especially Flink and Spark), it is highly recommended to run them sequentially rather than concurrently.

Navigate to the respective directory to launch a stack:

```bash
cd task1-influxdb
docker compose up --detach
```

Refer to the individual `README.md` within each task directory for specific deployment, usage, and teardown instructions.
