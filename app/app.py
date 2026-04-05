#!/usr/bin/env python3
"""Cassandra schema and index load / incremental update for the search engine."""
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

from cassandra.cluster import Cluster

KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "searchengine")
CASSANDRA_HOSTS = os.environ.get("CASSANDRA_HOSTS", "cassandra-server").split(",")


def connect():
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect()
    return cluster, session


def ensure_schema(session) -> None:
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
        """
    )
    session.set_keyspace(KEYSPACE)
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_stat (
            singleton text PRIMARY KEY,
            n_docs bigint,
            avg_dl double,
            sum_dl double
        );
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int
        );
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS postings (
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        );
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id text PRIMARY KEY,
            title text,
            dl int
        );
        """
    )


def hdfs_concat(glob_path: str) -> str:
    try:
        return subprocess.check_output(
            ["bash", "-lc", f"hdfs dfs -cat {glob_path}"],
            text=True,
        )
    except subprocess.CalledProcessError:
        return ""


def truncate_tables(session) -> None:
    for table in ("postings", "vocabulary", "documents", "corpus_stat"):
        session.execute(f"TRUNCATE {table}")


def load_from_hdfs(session) -> None:
    """Read partitioned index files from HDFS and bulk-load tables."""
    parts: List[str] = []
    for pattern in ("/indexer/vocab/part-*", "/indexer/index/part-*", "/indexer/stats/part-*"):
        parts.append(hdfs_concat(pattern))
    text = "".join(parts)
    if not text.strip():
        raise RuntimeError("No index data read from HDFS (/indexer/vocab, /indexer/index, /indexer/stats).")


    vocab: Dict[str, int] = {}
    postings: List[tuple] = []
    n_docs = 0
    avg_dl = 0.0
    sum_dl = 0.0
    doc_len: Dict[str, int] = {}
    doc_title: Dict[str, str] = {}

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("VOCAB\t"):
            _, term, df_s = line.split("\t", 2)
            vocab[term] = int(df_s)
        elif line.startswith("POST\t"):
            _, term, doc_id, tf_s = line.split("\t", 3)
            postings.append((term, doc_id, int(tf_s)))
        elif line.startswith("STAT\tn_docs\t"):
            n_docs = int(line.split("\t", 2)[2])
        elif line.startswith("STAT\tavg_dl\t"):
            avg_dl = float(line.split("\t", 2)[2])
        elif line.startswith("STAT\tsum_dl\t"):
            sum_dl = float(line.split("\t", 2)[2])
        elif line.startswith("DOC_LEN\t"):
            _, doc_id, dl_s = line.split("\t", 2)
            doc_len[doc_id] = int(dl_s)
        elif line.startswith("DOC_TITLE\t"):
            _, doc_id, title = line.split("\t", 2)
            doc_title[doc_id] = title

    if not n_docs and doc_len:
        n_docs = len(doc_len)
        sum_dl = float(sum(doc_len.values()))
        avg_dl = sum_dl / n_docs if n_docs else 0.0

    truncate_tables(session)

    ins_vocab = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
    ins_post = session.prepare("INSERT INTO postings (term, doc_id, tf) VALUES (?, ?, ?)")
    ins_doc = session.prepare("INSERT INTO documents (doc_id, title, dl) VALUES (?, ?, ?)")

    for term, df in vocab.items():
        session.execute(ins_vocab, (term, df))
    for term, doc_id, tf in postings:
        session.execute(ins_post, (term, doc_id, tf))
    for doc_id, title in doc_title.items():
        dl = doc_len.get(doc_id, 0)
        session.execute(ins_doc, (doc_id, title, dl))

    ins_stat = session.prepare(
        "INSERT INTO corpus_stat (singleton, n_docs, avg_dl, sum_dl) VALUES (?, ?, ?, ?)"
    )
    session.execute(ins_stat, ("global", int(n_docs), float(avg_dl), float(sum_dl)))

def tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def add_local_document(session, file_path: str) -> None:
    path = Path(file_path)
    text_body = path.read_text(encoding="utf-8", errors="replace")
    base = path.name
    if base.lower().endswith(".txt"):
        base = base[:-4]
    if "_" not in base:
        raise ValueError("File name must be <doc_id>_<title>.txt")
    doc_id, title = base.split("_", 1)
    title = title.replace("_", " ")
    tokens = tokenize(text_body)
    dl = len(tokens)
    tf: Dict[str, int] = {}
    for t in tokens:
        tf[t] = tf.get(t, 0) + 1

    row = session.execute(
        "SELECT n_docs, sum_dl FROM corpus_stat WHERE singleton = %s", ("global",)
    ).one()
    if row is None:
        raise RuntimeError("Corpus statistics missing; run full index load first.")

    n_docs = int(row.n_docs)
    sum_dl = float(row.sum_dl)
    n_docs_new = n_docs + 1
    sum_dl_new = sum_dl + dl
    avg_dl_new = sum_dl_new / n_docs_new

    session.execute(
        "INSERT INTO documents (doc_id, title, dl) VALUES (%s, %s, %s)",
        (doc_id, title, dl),
    )

    sel_df = session.prepare("SELECT df FROM vocabulary WHERE term = ?")
    ins_vocab = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
    upd_vocab = session.prepare("UPDATE vocabulary SET df = ? WHERE term = ?")
    ins_post = session.prepare("INSERT INTO postings (term, doc_id, tf) VALUES (?, ?, ?)")

    for term, term_tf in tf.items():
        existing = session.execute(sel_df, (term,)).one()
        if existing is None:
            session.execute(ins_vocab, (term, 1))
        else:
            session.execute(upd_vocab, (existing.df + 1, term))
        session.execute(ins_post, (term, doc_id, term_tf))

    session.execute(
        "INSERT INTO corpus_stat (singleton, n_docs, avg_dl, sum_dl) VALUES (%s, %s, %s, %s)",
        ("global", n_docs_new, avg_dl_new, sum_dl_new),
    )


def main(argv: List[str]) -> None:
    if len(argv) < 2:
        print("Usage: app.py init-schema | load-hdfs | add-doc PATH", file=sys.stderr)
        sys.exit(1)
    cmd = argv[1]
    cluster, session = connect()
    try:
        if cmd == "init-schema":
            ensure_schema(session)
        elif cmd == "load-hdfs":
            ensure_schema(session)
            load_from_hdfs(session)
        elif cmd == "add-doc":
            if len(argv) < 3:
                print("add-doc requires a file path", file=sys.stderr)
                sys.exit(1)
            ensure_schema(session)
            add_local_document(session, argv[2])
        else:
            print(f"Unknown command {cmd}", file=sys.stderr)
            sys.exit(1)
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main(sys.argv)
