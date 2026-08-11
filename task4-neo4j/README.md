# Task 4 — Neo4j

The Task 4 runtime, data preparation, import, three analyses, browser capture,
and evidence validator all run in containers on `neo4j-network`. The host needs
Docker with Docker Compose; Python, `curl`, and Chromium are not host
dependencies.

## Run and validate

From the repository root, choose a custom Neo4j password for this run and start
the container-only evidence pipeline:

```sh
export NEO4J_PASSWORD='choose-a-local-password'
bash .github/scripts/run_task4_neo4j_ci.sh
```

Do not commit the password. CI uses the `NEO4J_PASSWORD` repository secret when
it is configured and otherwise creates a run-specific credential. The local
password is used to initialise a new database; keep it unchanged when reusing
the existing `task4-neo4j/data` directory.

The pipeline streams and verifies the deterministic first 5,000 paths from the
official SNAP patent citation file. Every imported relationship has the stable
dataset identity `snap-cit-Patents-first-5000-v1`. Re-running the import uses
`MERGE`, so it remains at exactly 5,000 coursework relationships. It does not
delete unrelated `Patent` nodes or relationships.

## Evidence

Local evidence is written to `task4-neo4j/results`. CI redirects the same
container output to `ci-evidence/task4` and uploads it. The bundle includes:

- the exact 5,000-edge CSV and its deterministic checksum;
- verbose output for exactly three `PROFILE` analyses;
- scoped relationship counts and relationship types;
- Neo4j Browser HTTP, rendered-DOM, console, and PNG evidence; and
- `source-revision.json`, `validation-report.md`, and
  `evidence-manifest.json`, which bind source and artifact hashes to the full
  commit SHA.

After the evidence pipeline finishes, restart only the persisted Neo4j service
for manual inspection:

```sh
cd task4-neo4j
docker compose up --detach --wait neo4j
```

Open <http://localhost:7474>, connect to
`bolt://localhost:7687`, and sign in as `neo4j` with the password supplied in
`NEO4J_PASSWORD`. The direct-neighbor query uses patent `3858514`; the
shortest-path query uses patents `3484134` and `253889`, which are six hops
apart in this subset.

The evidence pipeline stops its containers on completion. To stop a manually
started stack, run this from `task4-neo4j` with the same password environment:

```sh
docker compose down --remove-orphans
```
