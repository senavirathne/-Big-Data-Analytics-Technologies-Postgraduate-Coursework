#!/usr/bin/env bash
set -euo pipefail

cypher-shell \
  --address "${NEO4J_URI}" \
  --username "${NEO4J_USERNAME}" \
  --password "${NEO4J_PASSWORD}" \
  --format verbose \
  --file /queries/analysis_queries.cypher \
  > /results/query-execution.txt

test -s /results/query-execution.txt
echo "Three PROFILE query plans and results written to /results/query-execution.txt"
