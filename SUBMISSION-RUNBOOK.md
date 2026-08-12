# Coursework verification and submission runbook

The implementations and automated checks for Tasks 1–5 are in the repository;
the remaining work is to run them on Docker and retain evidence for the exact
commit you submit.

## 1. Prepare the exact source revision

Start Docker Desktop and confirm that Docker Compose is version 2.30.0 or newer:

```sh
docker version
docker compose version
```

Review the remediation before committing it:

```sh
git status --short
git diff --check
git diff
```

Commit only the intended coursework changes. Do not generate evidence from
an uncommitted or dirty tree: every evidence manifest records a full Git commit
SHA, so the recorded revision must contain the code that actually ran.

After committing, record the revision and confirm the tree is clean:

```sh
git rev-parse HEAD
git status --short
```

The second command should print nothing.

## 2. Preferred route: run GitHub Actions

Push the committed branch, open the repository's **Actions** page, and manually
run all three workflows against that same branch:

1. **Task 1 containerized InfluxDB verification**
2. **Tasks 2 and 3 containerized verification**
3. **Coursework Tasks 4 and 5**

All five jobs must be green. The Task 1 workflow log must end with the ingestion
count and `PASS` verification message documented in `task1-influxdb/README.md`.
Download these four artifacts from the Tasks 2–5 workflow runs:

- `task-2-kafka-flink-evidence-*`
- `task-3-spark-evidence-*`
- `task-4-neo4j-evidence-*`
- `task-5-literature-validation-*`

Check each JSON evidence manifest. Its `source_commit` must exactly equal the
40-character SHA from `git rev-parse HEAD`, and its validation status/report
must say `PASS`. A green result from another commit is not evidence for the
revision being submitted.

## 3. Local Docker route, if needed

The GitHub workflows are the easiest reproducible route. To run locally instead,
use the following commands from a clean committed checkout.

### Task 1

```sh
cd task1-influxdb
docker compose up --build --detach
verifier_id="$(docker compose ps --all --quiet task1-verifier)"
test -n "$verifier_id"
verifier_status="$(docker container wait "$verifier_id")"
docker compose logs influx-setup climate-ingest task1-verifier
docker compose down --remove-orphans
test "$verifier_status" -eq 0
cd ..
```

The logs must report `ingested 345587 PAFA historical wind records` followed by
`PASS: all three Flux operations executed; task and 30-day retention verified`.
The persistent InfluxDB files remain under `task1-influxdb/data/influxdb2/` and
are intentionally excluded from Git.

### Task 2

```sh
cd task2-kafka-flink
export SOURCE_COMMIT_SHA="$(git rev-parse HEAD)"
docker compose run --build --rm flink-job-build
docker compose up --build --detach --wait --wait-timeout 180 \
  kafka flink-jobmanager flink-taskmanager
docker compose run --build --rm --no-deps flink-dashboard-runner
docker compose up --build --detach --wait --wait-timeout 180 traffic-producer
docker compose run --rm --no-deps task2-evidence-verifier
docker compose down --volumes --remove-orphans
cd ..
```

Keep `task2-kafka-flink/artifacts/traffic-window-job.jar` and the contents of
`task2-kafka-flink/evidence/`. The live verifier deliberately waits for genuine
two-second Kafka messages and a completed 15-minute event-time window aligned
to the 10-minute slide.

### Task 3

```sh
cd task3-spark
export COURSEWORK_COMMIT_SHA="$(git rev-parse HEAD)"
docker compose build --pull evidence-verifier
docker compose up --detach
docker compose wait run-analysis
docker compose run --rm --no-deps evidence-verifier
docker compose down --volumes --remove-orphans
cd ..
```

Keep `task3-spark/output/`, `task3-spark/metrics/`, `task3-spark/events/`, and
`task3-spark/evidence/` together; their combined manifest proves the Top 50,
two-worker allocation, DAG, stage, shuffle, skew, bottleneck, and Spark UI
evidence came from one run.

### Task 4

Choose a local password and do not commit it:

```sh
export NEO4J_PASSWORD='choose-a-local-password'
bash .github/scripts/run_task4_neo4j_ci.sh
```

Keep `task4-neo4j/results/`. The import is dataset-scoped and idempotent; it
does not delete unrelated Neo4j data. If reusing an existing
`task4-neo4j/data/` directory, use the password that initialized it.

### Task 5

```sh
COURSEWORK_COMMIT_SHA="$(git rev-parse HEAD)" \
  docker compose --file task5-validation/docker-compose.yml \
  run --rm --build literature-validation
```

Keep `ci-evidence/task5/validation-report.md` and its manifest.

## 4. Final evidence check

Use only evidence produced by the final commit. Do not add the downloaded graph,
database state, event logs, or generated evidence to Git—the repository ignores
them intentionally. Retain the workflow artifacts separately and use the
relevant screenshots, query plans, tables, and validation reports in the final
coursework submission format required by the brief.
