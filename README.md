# Big Data Analytics Technologies — Postgraduate Coursework

This repository contains the containerized implementations and academic review required by the supplied coursework brief.

| Task | Implementation | Cloud verification |
|---|---|---|
| 1 — InfluxDB | [`task1-influxdb/`](task1-influxdb/) | [Task 1 workflow](.github/workflows/task1-influxdb.yml) |
| 2 — Kafka and Flink | [`task2-kafka-flink/`](task2-kafka-flink/) | [Tasks 2–3 workflow](.github/workflows/tasks-2-3.yml) |
| 3 — Spark | [`task3-spark/`](task3-spark/) | [Tasks 2–3 workflow](.github/workflows/tasks-2-3.yml) |
| 4 — Neo4j | [`task4-neo4j/`](task4-neo4j/) | [Tasks 4–5 workflow](.github/workflows/tasks-4-5.yml) |
| 5 — Literature review | [`task5-literature-review.md`](task5-literature-review.md) | [Tasks 4–5 workflow](.github/workflows/tasks-4-5.yml) |

The GitHub Actions jobs run and verify Tasks 1–5 through Docker Compose. The
Tasks 2–5 workflows also upload evidence tied to the exact source commit.
Generated datasets, database state, and runtime evidence are intentionally
excluded from Git history. Follow
[`SUBMISSION-RUNBOOK.md`](SUBMISSION-RUNBOOK.md) to create the final evidence and
check the remaining work that must be done in a Docker-capable environment.
