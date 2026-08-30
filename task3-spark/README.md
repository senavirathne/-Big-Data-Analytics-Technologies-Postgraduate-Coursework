# Scalable Data Analytics with Apache Spark

This component utilizes an Apache Spark cluster to handle large-scale dataset ingestion and execute graph network topology transformations.

## Architecture & Tech Stack
- **Engine:** Apache Spark (1 Master node, 2 Worker nodes)
- **Implementation:** PySpark
- **Dataset:** SNAP web-BerkStan network text file
- **Deployment:** Docker & Docker Compose

## Features
- **Cluster Resource Management:** Worker nodes are explicitly constrained (e.g., 2 compute cores, 2GB RAM).
- **Distributed ETL:** Lazy evaluation parses 7.6 million directed edges into a Spark DataFrame.
- **Graph Analytics:** Aggregates target vertex indexes to calculate in-degree distributions, extracting the top 50 dominant destination nodes.
- **Performance Profiling:** Spark Web Console and History Server capture DAG logic chains, stage execution durations, and shuffle metrics.

## Getting Started

### 1. Start the Spark Cluster & Run Analysis
From this directory, spin up the entire Spark topology. This command will automatically start the master, workers, initialize the dataset, and submit the PySpark analysis job:
```bash
docker compose up --pull never --detach
```

### 2. Monitor Job Completion
Wait for the analysis container to finish executing the script:
```bash
docker compose wait run-analysis
```

## Usage

Once the job is completed, you can review the results and the cluster UI:
- **Spark Master UI:** `http://localhost:8080` (Verify worker allocation).
- **Spark History Server:** `http://localhost:18080` (Review DAGs, Shuffle Read/Write, and Stage timings for the `WebBerkStanInDegreeAnalysis` application).
- **Output Results:** The top 50 in-degree node ranking is saved locally in `output/top_50_indegree.csv`.
- **Metrics JSON:** Detailed performance tracking is exported to `metrics/execution-metrics.json`.

## Cleanup
To stop the Spark services while preserving the downloaded dataset and generated outputs:
```bash
docker compose stop
```
To perform a complete teardown:
```bash
docker compose down --remove-orphans
```
