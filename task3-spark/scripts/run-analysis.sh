#!/usr/bin/env bash
set -euo pipefail

python3 /opt/coursework/apps/wait_for_cluster.py

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host=run-analysis \
  --conf spark.cores.max=4 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=1g \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///opt/spark-events \
  --conf spark.eventLog.compress=false \
  --conf spark.eventLog.logBlockUpdates.enabled=true \
  /opt/coursework/apps/analyze_graph.py \
  --input file:///data/web-BerkStan.txt \
  --output /output/top_50_indegree.csv

python3 /opt/coursework/apps/capture_metrics.py
