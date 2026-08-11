#!/usr/bin/env bash
set -euo pipefail

cypher-shell \
  --address "${NEO4J_URI}" \
  --username "${NEO4J_USERNAME}" \
  --password "${NEO4J_PASSWORD}" \
  --file /queries/import_patents.cypher

relationship_count="$(
  cypher-shell \
    --address "${NEO4J_URI}" \
    --username "${NEO4J_USERNAME}" \
    --password "${NEO4J_PASSWORD}" \
    --format plain \
    "MATCH ()-[citation:CITES {coursework_dataset: 'snap-cit-Patents-first-5000-v1'}]->() RETURN count(citation) AS relationships;" \
    | tail -n 1 | tr -d '\r'
)"

if [[ "${relationship_count}" != "5000" ]]; then
  echo "Import validation failed: expected 5000 coursework CITES relationships, found ${relationship_count}" >&2
  exit 1
fi

edge_key_count="$(
  cypher-shell \
    --address "${NEO4J_URI}" \
    --username "${NEO4J_USERNAME}" \
    --password "${NEO4J_PASSWORD}" \
    --format plain \
    "MATCH ()-[citation:CITES {coursework_dataset: 'snap-cit-Patents-first-5000-v1'}]->() RETURN count(DISTINCT citation.coursework_edge) AS edge_keys;" \
    | tail -n 1 | tr -d '\r'
)"

if [[ "${edge_key_count}" != "5000" ]]; then
  echo "Import validation failed: expected 5000 unique coursework edge keys, found ${edge_key_count}" >&2
  exit 1
fi

echo "Import validation passed: exactly 5000 idempotent, dataset-scoped CITES relationships"
