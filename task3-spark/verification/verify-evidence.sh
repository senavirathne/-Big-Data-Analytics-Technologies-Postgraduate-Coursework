#!/usr/bin/env bash
set -euo pipefail

: "${COURSEWORK_COMMIT_SHA:?Set COURSEWORK_COMMIT_SHA to the full 40-character commit SHA being verified}"

master_url="${SPARK_MASTER_URL:-http://spark-master:8080}"
history_url="${SPARK_HISTORY_URL:-http://spark-history-server:18080}"

mkdir -p /evidence

python3 /opt/coursework/verification/validate_task3_outputs.py \
  --ranking /output/top_50_indegree.csv \
  --metrics /metrics/execution-metrics.json \
  --events /events \
  2>&1 | tee /evidence/task3-validation.json

python3 /opt/coursework/verification/task3_capture_spark_ui.py \
  --master-url "${master_url}" \
  --history-url "${history_url}" \
  --evidence-dir /evidence \
  2>&1 | tee /evidence/spark-ui-capture.log

python3 /opt/coursework/verification/write_evidence_manifest.py \
  --task task-3-spark \
  --commit-sha "${COURSEWORK_COMMIT_SHA}" \
  --artifact-root evidence=/evidence \
  --artifact-root output=/output \
  --artifact-root metrics=/metrics \
  --artifact-root events=/events \
  --output /evidence/task3-evidence-manifest.json
