#!/usr/bin/env python3
"""Calculate deterministic in-degree rankings for the SNAP web graph."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.storagelevel import StorageLevel


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    spark = SparkSession.builder.appName("WebBerkStanInDegreeAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Every operation through persist() is a lazy DataFrame transformation. The
    # count below is the first action and deliberately materializes the cache.
    raw_lines = spark.read.text(args.input)
    data_lines = raw_lines.filter(
        (F.length(F.trim(F.col("value"))) > 0)
        & (~F.trim(F.col("value")).startswith("#"))
    )
    fields = F.split(F.trim(F.col("value")), r"\s+")
    edges = (
        data_lines.select(
            fields.getItem(0).cast("long").alias("source"),
            fields.getItem(1).cast("long").alias("destination"),
            F.size(fields).alias("field_count"),
        )
        .filter(
            (F.col("field_count") == 2)
            & F.col("source").isNotNull()
            & F.col("destination").isNotNull()
        )
        .drop("field_count")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    edge_count = edges.count()

    in_degrees = (
        edges.groupBy("destination")
        .agg(F.count(F.lit(1)).alias("in_degree"))
    )
    top_50 = in_degrees.orderBy(
        F.desc("in_degree"), F.asc("destination")
    ).limit(50)
    rows = top_50.collect()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.writer(destination)
        writer.writerow(["rank", "destination", "in_degree"])
        for rank, row in enumerate(rows, start=1):
            writer.writerow([rank, row["destination"], row["in_degree"]])

    print(f"Parsed edge rows: {edge_count}")
    print(f"Top-50 ranking written to {output}")

    edges.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
