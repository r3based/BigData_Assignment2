#!/usr/bin/env python3
"""PySpark BM25 search: read query from stdin; print top-10 doc_id and title."""
from __future__ import annotations

import math
import os
import re
import sys
from collections import defaultdict
from typing import Dict, List, Tuple

from cassandra.cluster import Cluster

KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "searchengine")
CASSANDRA_HOSTS = os.environ.get("CASSANDRA_HOSTS", "cassandra-server").split(",")


def cassandra_cluster():
    return Cluster(CASSANDRA_HOSTS, connect_timeout=15, control_connection_timeout=15)

K1 = 1.0
B = 0.75


def tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def idf(n_docs: int, df: int) -> float:
    # Assignment-style idf: log(N / df); guard edge cases
    if df <= 0 or n_docs <= 0:
        return 0.0
    return math.log(n_docs / float(df))


def main() -> None:
    query = sys.stdin.read().strip()
    terms = [t for t in tokenize(query) if t]
    if not terms:
        print("No query terms after tokenization.", file=sys.stderr)
        return

    cluster = cassandra_cluster()
    session = cluster.connect(KEYSPACE)

    stat = session.execute(
        "SELECT n_docs, avg_dl FROM corpus_stat WHERE singleton = %s", ("global",)
    ).one()
    if stat is None:
        print("Corpus not indexed.", file=sys.stderr)
        cluster.shutdown()
        return
    n_docs = int(stat.n_docs)
    avgdl = float(stat.avg_dl)
    if avgdl <= 0:
        avgdl = 1.0

    scores: Dict[str, float] = defaultdict(float)
    doc_dl_cache: Dict[str, int] = {}

    sel_df = session.prepare("SELECT df FROM vocabulary WHERE term = ?")
    sel_posts = session.prepare("SELECT doc_id, tf FROM postings WHERE term = ?")
    sel_dl = session.prepare("SELECT dl FROM documents WHERE doc_id = ?")

    for term in terms:
        vrow = session.execute(sel_df, (term,)).one()
        if vrow is None:
            continue
        df = int(vrow.df)
        idf_t = idf(n_docs, df)
        for prow in session.execute(sel_posts, (term,)):
            doc_id = prow.doc_id
            tf = int(prow.tf)
            if doc_id not in doc_dl_cache:
                drow = session.execute(sel_dl, (doc_id,)).one()
                if drow is None:
                    continue
                doc_dl_cache[doc_id] = int(drow.dl)
            dl = doc_dl_cache[doc_id]
            denom = K1 * ((1.0 - B) + B * dl / avgdl) + tf
            if denom == 0:
                continue
            num = idf_t * (K1 + 1.0) * tf
            scores[doc_id] += num / denom

    cluster.shutdown()

    if not scores:
        print("No matching documents.")
        return

    from pyspark.sql import SparkSession

    spark = None
    try:
        spark = (
            SparkSession.builder.appName("bm25_query")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .getOrCreate()
        )
        sc = spark.sparkContext
        ranked: List[Tuple[str, float]] = sc.parallelize(list(scores.items())).takeOrdered(
            10, key=lambda x: -x[1]
        )

        cluster2 = cassandra_cluster()
        session2 = cluster2.connect(KEYSPACE)
        sel_title = session2.prepare("SELECT title FROM documents WHERE doc_id = ?")
        for doc_id, _score in ranked:
            trow = session2.execute(sel_title, (doc_id,)).one()
            title = trow.title if trow else ""
            print(f"{doc_id}\t{title}")
        cluster2.shutdown()
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
