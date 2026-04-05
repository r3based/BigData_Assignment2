#!/usr/bin/env python3
"""Build local TXT files from Parquet on HDFS, then build /input/data for MapReduce (via build_input)."""
from __future__ import annotations

import os
import sys
from typing import Optional

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession

N_DOCS = int(os.environ.get("NUM_DOCS", "100"))
PARQUET_PATH = os.environ.get("PARQUET_HDFS_PATH", "hdfs:///a.parquet")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")


def spark_session(name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(name)
        .master(SPARK_MASTER)
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )


def build_docs() -> None:
    spark = spark_session("prepare_docs")
    try:
        df = spark.read.parquet(PARQUET_PATH).select("id", "title", "text")
        total = max(1, df.count())
        fraction = min(1.0, (N_DOCS * 2.0) / float(total))
        sampled = (
            df.where(df.text.isNotNull())
            .where(df.text != "")
            .sample(withReplacement=False, fraction=fraction, seed=0)
            .limit(N_DOCS)
        )
        rows = sampled.collect()
        os.makedirs("data", exist_ok=True)
        for row in rows:
            title = row.title or ""
            body = row.text or ""
            if not str(body).strip():
                continue
            raw_name = f"{row.id}_{title}".replace(" ", "_")
            safe = sanitize_filename(raw_name).replace(" ", "_")
            path = os.path.join("data", f"{safe}.txt")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(body)
    finally:
        spark.stop()


def build_input() -> None:
    spark = spark_session("prepare_input")
    try:
        sc = spark.sparkContext

        def project(record):
            path, content = record
            base = os.path.basename(path)
            if not base.lower().endswith(".txt"):
                return None
            stem = base[:-4]
            idx = stem.find("_")
            if idx <= 0:
                return None
            doc_id = stem[:idx]
            title = stem[idx + 1 :].replace("_", " ")
            text = " ".join(content.split())
            if not text:
                return None
            return f"{doc_id}\t{title}\t{text}"

        rdd = sc.wholeTextFiles("hdfs:///data")
        rdd.map(project).filter(lambda x: x is not None).coalesce(1).saveAsTextFile(
            "hdfs:///input/data"
        )
    finally:
        spark.stop()


def main(argv) -> None:
    mode: Optional[str] = argv[1] if len(argv) > 1 else None
    if mode == "build_input":
        build_input()
    else:
        build_docs()


if __name__ == "__main__":
    main(sys.argv)
