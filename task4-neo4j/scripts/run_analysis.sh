#!/usr/bin/env bash
set -euo pipefail

cypher-shell \
  --address "${NEO4J_URI}" \
  --username "${NEO4J_USERNAME}" \
  --password "${NEO4J_PASSWORD}" \
  --format verbose \
  --file /queries/analysis_queries.cypher \
  > /evidence/query-execution.txt

cypher-shell \
  --address "${NEO4J_URI}" \
  --username "${NEO4J_USERNAME}" \
  --password "${NEO4J_PASSWORD}" \
  --format plain \
  "MATCH ()-[citation:CITES {coursework_dataset: 'snap-cit-Patents-first-5000-v1'}]->() RETURN count(citation) AS directed_cites;" \
  > /evidence/relationship-count.txt

cypher-shell \
  --address "${NEO4J_URI}" \
  --username "${NEO4J_USERNAME}" \
  --password "${NEO4J_PASSWORD}" \
  --format plain \
  "MATCH ()-[relationship:CITES {coursework_dataset: 'snap-cit-Patents-first-5000-v1'}]->() RETURN count(relationship) AS total_relationships, collect(DISTINCT type(relationship)) AS relationship_types;" \
  > /evidence/relationship-types.txt

cypher-shell \
  --address "${NEO4J_URI}" \
  --username "${NEO4J_USERNAME}" \
  --password "${NEO4J_PASSWORD}" \
  --format plain \
  "MATCH ()-[citation:CITES]->() RETURN count(citation) AS all_cites, sum(CASE WHEN citation.coursework_dataset = 'snap-cit-Patents-first-5000-v1' THEN 1 ELSE 0 END) AS coursework_cites, sum(CASE WHEN citation.coursework_dataset = 'snap-cit-Patents-first-5000-v1' THEN 0 ELSE 1 END) AS unrelated_cites;" \
  > /evidence/import-scope.txt

test -s /evidence/query-execution.txt
test -s /evidence/relationship-count.txt
test -s /evidence/relationship-types.txt
test -s /evidence/import-scope.txt
echo "Three PROFILE plans, scoped counts, and results written to /evidence"
