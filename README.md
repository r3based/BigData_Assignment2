# Big Data Assignment 2 — Simple Search Engine

English-only deliverables: code, scripts, and `report.pdf` (Methodology, Demonstration).

## Quick start

1. Download `a.parquet` from the [Wikipedia Kaggle dataset](https://www.kaggle.com/datasets/jjinho/wikipedia-20230701?select=a.parquet) and place it as `app/a.parquet` (or ensure it is already at `hdfs:///a.parquet`).
2. From the repository root: `docker compose up`

The `cluster-master` container runs `app/app.sh`: starts Hadoop, installs the Python env, prepares **100** documents (`NUM_DOCS` optional), builds the MapReduce index, loads Cassandra, and runs sample `search.sh` queries.

Top-level symlinks point to scripts under `app/`, matching the assignment filenames.

## Main scripts (under `app/`)

| Script | Role |
|--------|------|
| `prepare_data.sh` | Parquet → `/data` on HDFS → `/input/data` (one partition, tab-separated) |
| `create_index.sh` | Two Hadoop Streaming jobs → `/indexer/*` on HDFS |
| `store_index.sh` / `app.py` | Cassandra schema + bulk load from HDFS |
| `index.sh` | `create_index.sh` then `store_index.sh` |
| `add_to_index.sh` | Incremental index of one local `.txt` file (+10 bonus) |
| `query.py` | BM25 (stdin query), reads Cassandra, ranks with PySpark RDD |
| `search.sh` | `spark-submit` on YARN, query as CLI argument |

## Report

Add `report.pdf` before submission (not generated here). Corpus size: **100** documents per instructor direction.
