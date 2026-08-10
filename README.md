# Big Data Analytics Technologies — Postgraduate Coursework

This repository contains the containerized implementations and academic review required by the supplied coursework brief.

| Task | Implementation | Cloud verification |
|---|---|---|
| 1 — InfluxDB | [`task1-influxdb/`](task1-influxdb/) | Deferred until the original timestamped Fairbanks CSV is available |
| 2 — Kafka and Flink | [`task2-kafka-flink/`](task2-kafka-flink/) | [Tasks 2–3 workflow](.github/workflows/tasks-2-3.yml) |
| 3 — Spark | [`task3-spark/`](task3-spark/) | [Tasks 2–3 workflow](.github/workflows/tasks-2-3.yml) |
| 4 — Neo4j | [`task4-neo4j/`](task4-neo4j/) | [Tasks 4–5 workflow](.github/workflows/tasks-4-5.yml) |
| 5 — Literature review | [`task5-literature-review.md`](task5-literature-review.md) | [Tasks 4–5 workflow](.github/workflows/tasks-4-5.yml) |

The GitHub Actions jobs run Tasks 2–4 on separate Docker-capable Ubuntu runners and upload reproducible runtime evidence. Task 5 receives deterministic structure, coverage, citation, DOI, and authoritative-link checks. Generated datasets, database state, and evidence are intentionally excluded from Git history and should be downloaded from the workflow artifacts into the local workspace after successful runs.
