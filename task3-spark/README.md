# Task 3 — Scalable analytics with Apache Spark

Task 3 runs entirely in Docker Compose. The topology contains one Spark master,
two independent workers limited to 2 CPU cores and 2 GB RAM each, a dataset
initializer, a one-shot `spark-submit` container, and a History Server for
reviewing the completed application.

## Run the analysis

From this directory:

```sh
docker compose up --detach
docker compose wait run-analysis
```

On the first run, `dataset-init` downloads, decompresses, and validates the
assigned SNAP `web-BerkStan` edge list in `data/web-BerkStan.txt`. The same host
directory is mounted read-only into both workers and the analysis container.

The PySpark job removes blank and `#` metadata lines lazily, parses the two edge
columns into a DataFrame, caches the parsed edges, groups by destination to
calculate in-degree, and writes the ordered Top 50 to:

```text
output/top_50_indegree.csv
```

## Review execution metrics

Open the Spark Master UI at <http://localhost:8080>. It should show exactly two
`ALIVE` workers, each advertising 2 cores and 2048 MiB memory.

After `run-analysis` completes, open <http://localhost:18080>, select
`WebBerkStanInDegreeAnalysis`, and review its Jobs and Stages pages. Record the
job DAG, stage durations, shuffle read/write values, and the allocation of work
across the two workers.

The completed Spark event log is retained in `events/`. A compact extraction is
written to `metrics/execution-metrics.json`; it contains the application and job
IDs, stage-parent DAG, per-stage duration and shuffle totals, per-worker task,
input, and shuffle allocation, and direct maximum-to-minimum comparisons for
investigating possible worker skew.

Stop the containers when the review is complete:

```sh
docker compose down
```
