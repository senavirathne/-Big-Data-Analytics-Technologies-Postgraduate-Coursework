# Task 3 — Spark

From this directory, start the two-worker cluster and one-shot analysis:

```sh
docker compose up --detach
docker compose logs --follow run-analysis
```

While the cluster is running, open the master UI at <http://localhost:8080>. It
must show exactly two `ALIVE` workers, each with 2 cores and 2048 MiB memory.
After `run-analysis` exits successfully, open the Spark History Server at
<http://localhost:18080>, select `WebBerkStanInDegreeAnalysis`, and use its Jobs
and Stages pages to review the completed application's DAG visualization, stage
durations, shuffle metrics, and executor allocation.

The ranking is in `output/top_50_indegree.csv`. The History Server reconstructs
its Web Console from the event logs in `events/`; the same logs are parsed into
`metrics/execution-metrics.json` to record the stage, DAG, shuffle-allocation,
two-worker data-skew, execution-imbalance, and bottleneck evidence in a
machine-readable form. Data skew is assessed only from input and shuffle
record/byte allocation. Task-duration imbalance and stages that inherently use
only one task or worker are recorded separately and are not mislabeled as data
skew.

Stop the cluster with `docker compose down`.
