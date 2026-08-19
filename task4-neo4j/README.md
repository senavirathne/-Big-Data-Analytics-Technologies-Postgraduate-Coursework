# Task 4 — Neo4j patent citation graph

This task runs entirely in Docker Compose. It downloads the SNAP patent citation
dataset, prepares its first 5,000 directed citation paths, imports them into
Neo4j Community Edition, and executes the three required `PROFILE` analyses.

## Run the task

From the repository root:

```sh
cd task4-neo4j
export NEO4J_PASSWORD='choose-a-local-password'

docker compose up --detach --wait neo4j
docker compose run --rm --no-deps import-patents
docker compose run --rm --no-deps analyze-patents
```

Starting `neo4j` first runs the `prepare-patents` container. That container
streams the official `cit-Patents.txt.gz` file and writes exactly the first
5,000 non-comment citation rows to `import/patent_edges_5000.csv`.

The import creates a uniqueness constraint on `Patent.id`, merges patent nodes,
and merges directed `CITES` relationships. Re-running it is idempotent. The
analysis output is written to `results/query-execution.txt` and contains exactly
three profiled queries:

1. direct neighbours of patent `3858514`;
2. the ten patents with the highest incoming citation degree; and
3. a shortest path between patents `3484134` and `253889`.

## Verify in Neo4j Browser

With the `neo4j` service running, open <http://localhost:7474>. Connect to
`bolt://localhost:7687` with username `neo4j` and the password exported above.

Run this query to confirm that the graph contains the prepared relationships:

```cypher
MATCH ()-[citation:CITES]->()
RETURN count(citation) AS directed_citations;
```

The result should be `5000`. Then copy the three statements from
`cypher/analysis_queries.cypher` into Neo4j Browser and run them individually.
Each statement begins with `PROFILE`, so Browser displays both its result and
execution plan.

Stop the containers when the analysis is complete:

```sh
docker compose down --remove-orphans
```
