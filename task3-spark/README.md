# Task 3 — Spark

All Task 3 runtime and verification dependencies run in Docker. From this
directory, build the verification image before the dataset is downloaded, then
start the two-worker cluster and one-shot analysis:

```sh
docker compose build evidence-verifier
docker compose up --detach
docker compose wait run-analysis
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

After `run-analysis` exits with code zero, run the validator and browser capture
inside the opt-in verification container. Supply the full commit SHA for the
source revision that produced the evidence:

```sh
COURSEWORK_COMMIT_SHA="$(git rev-parse HEAD)" \
  docker compose run --rm --no-deps evidence-verifier
```

The container validates the Top 50, Spark event log, DAG, stage-duration,
shuffle, worker-allocation, data-skew, execution-imbalance, and bottleneck
evidence. Its bundled headless Chromium connects to `spark-master` and
`spark-history-server` over `spark-network`; no host Python, Playwright, browser,
`curl`, or `jq` installation is required. Screenshots and audit files are written
to `evidence/`. `evidence/task3-evidence-manifest.json` records SHA-256 hashes for
the evidence, ranking, metrics, and event logs and binds them to the supplied
commit SHA.

Stop the cluster with `docker compose down`.
