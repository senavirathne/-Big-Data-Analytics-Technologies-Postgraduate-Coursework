# Graph Database Engineering using Neo4j

This component develops graph-native storage structures to run complex network lookups using Neo4j without incurring standard relational database multi-join computational penalties.

## Architecture & Tech Stack
- **Database:** Neo4j Community Edition
- **Query Language:** Cypher
- **Dataset:** SNAP patent citation network
- **Deployment:** Docker & Docker Compose

## Features
- **Graph Ingestion:** Cypher's `LOAD CSV` mechanism creates a dense graph network from 5,000 document citation paths.
- **Structural Analysis:** Queries direct neighbor layers, computes degree centrality for incoming citations, and maps shortest path linkages across distant nodes.
- **Query Optimization:** Utilizes `PROFILE` to analyze operator plans, database hits, and traversal behavior.

## Getting Started

### 1. Configure and Start Neo4j
Set a secure local password for the database and spin up the Neo4j container:
```bash
$env:NEO4J_PASSWORD = 'choose-a-local-password'  # On Windows PowerShell
# export NEO4J_PASSWORD='choose-a-local-password' # On Linux/macOS

docker compose up --detach --wait --wait-timeout 240 neo4j
```

### 2. Import Data and Run Analytics
Execute the automated scripts to prepare the dataset, ingest it into Neo4j, and run the analytical Cypher queries:
```bash
docker compose run --rm --no-deps --no-TTY import-patents
docker compose run --rm --no-deps --no-TTY analyze-patents
```

## Usage

You can visually explore the graph and execute custom queries via the Neo4j Browser:
- **URL:** `http://localhost:7474`
- **Connection:** `bolt://localhost:7687`
- **Username:** `neo4j`
- **Password:** *(the password you set above)*

### Sample Queries
You can run standard Cypher statements directly in the browser. For example, to verify the total relationship count:
```cypher
MATCH ()-[citation:CITES]->()
RETURN count(citation) AS directed_citations;
```

## Cleanup
To stop the services while maintaining the current Neo4j container state:
```bash
docker compose stop --timeout 60
```
*Note: Using `docker compose down` will delete the container-local graph data, requiring a re-import on the next startup.*
