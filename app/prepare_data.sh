#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck source=/dev/null
source .venv/bin/activate

export NUM_DOCS="${NUM_DOCS:-100}"
export PARQUET_HDFS_PATH="${PARQUET_HDFS_PATH:-hdfs:///a.parquet}"

# Upload local file first; only then require it on HDFS (first run has no /a.parquet yet).
if [[ -f a.parquet ]]; then
  echo "Uploading local a.parquet to HDFS /a.parquet"
  hdfs dfs -put -f a.parquet /a.parquet
fi

if ! hdfs dfs -test -f /a.parquet; then
  echo "Missing HDFS /a.parquet. Copy Kaggle parquet to app/a.parquet (same folder as this script) or upload it to HDFS." >&2
  exit 1
fi

echo "Stage 1: sample ${NUM_DOCS} documents and write local data/*.txt"
export SPARK_MASTER="${SPARK_MASTER_DOCS:-local[*]}"
unset PYSPARK_PYTHON || true
export PYSPARK_DRIVER_PYTHON
PYSPARK_DRIVER_PYTHON="$(which python)"
spark-submit prepare_data.py

echo "Stage 2: upload text files to HDFS /data"
hdfs dfs -rm -r -f /data 2>/dev/null || true
hdfs dfs -put -f data /

echo "Stage 3: build /input/data (tab-separated, single partition)"
hdfs dfs -rm -r -f /input/data 2>/dev/null || true
export SPARK_MASTER="${SPARK_MASTER_INPUT:-yarn}"
export PYSPARK_DRIVER_PYTHON
PYSPARK_DRIVER_PYTHON="$(which python)"
# Executors run on slaves; use path inside the shipped archive, not /app/.venv (only exists on master).
export PYSPARK_PYTHON="./.venv/bin/python"
spark-submit --master yarn --deploy-mode client --archives "${PWD}/.venv.tar.gz#.venv" prepare_data.py build_input

hdfs dfs -ls /input/data
echo "Data preparation finished."
