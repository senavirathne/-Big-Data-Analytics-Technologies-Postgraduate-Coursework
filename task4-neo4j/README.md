# Task 4 — Neo4j

From this directory, start Neo4j and the one-shot preparation, import, and
analysis services:

```sh
docker compose up --detach
docker compose logs --follow analyze-patents
```

Open <http://localhost:7474>, connect to `bolt://localhost:7687`, and sign in as
`neo4j` with password `coursework2026`. The imported graph contains exactly
5,000 directed `CITES` relationships. The verbose plans and results for the
three `PROFILE` queries are written to `results/query-execution.txt`.
The direct-neighbor query uses target patent `3858514`; the shortest-path query
uses patents `3484134` and `253889`, which are six hops apart in this subset.

Stop the services with `docker compose down`.
