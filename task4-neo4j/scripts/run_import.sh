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
    "MATCH ()-[:CITES]->() RETURN count(*) AS relationships;" \
    | tail -n 1 | tr -d '\r'
)"

if [[ "${relationship_count}" != "5000" ]]; then
  echo "Import failed: expected exactly 5000 CITES relationships, found ${relationship_count}" >&2
  exit 1
fi

echo "Imported exactly 5000 directed CITES relationships"
