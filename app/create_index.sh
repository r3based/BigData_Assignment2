#!/bin/bash
set -euo pipefail

INPUT_PATH="${1:-/input/data}"

STREAMING_JAR="$(find /usr/local/hadoop /opt/hadoop "${HADOOP_HOME:-}" -name 'hadoop-streaming*.jar' 2>/dev/null | head -n 1 || true)"
if [[ -z "${STREAMING_JAR}" ]]; then
  STREAMING_JAR="$(find / -name 'hadoop-streaming*.jar' 2>/dev/null | head -n 1 || true)"
fi
if [[ -z "${STREAMING_JAR}" ]]; then
  echo "hadoop-streaming JAR not found" >&2
  exit 1
fi

hdfs dfs -rm -r -f /tmp/indexer/job1 /indexer/final /indexer/vocab /indexer/index /indexer/stats 2>/dev/null || true
hdfs dfs -mkdir -p /indexer

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MR_DIR="${SCRIPT_DIR}/mapreduce"

echo "MapReduce job 1: postings and doc meta from ${INPUT_PATH}"

hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.name=indexer_job1 \
  -files "${MR_DIR}/mapper1.py,${MR_DIR}/reducer1.py" \
  -mapper mapper1.py \
  -reducer reducer1.py \
  -input "${INPUT_PATH}" \
  -output /tmp/indexer/job1

echo "MapReduce job 2: vocabulary, postings, corpus stats"

hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.name=indexer_job2 \
  -files "${MR_DIR}/mapper2.py,${MR_DIR}/reducer2.py" \
  -mapper mapper2.py \
  -reducer reducer2.py \
  -input /tmp/indexer/job1 \
  -output /indexer/final

hdfs dfs -mkdir -p /indexer/vocab /indexer/index /indexer/stats

python3 - <<'PY'
import os
import subprocess
import sys
import tempfile

def run(cmd: str) -> str:
    return subprocess.check_output(cmd, shell=True, text=True)


def hdfs_put_text(content: str, dest: str) -> None:
    if not content:
        return
    fd, path = tempfile.mkstemp(prefix="idx_", text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        subprocess.check_call(["hdfs", "dfs", "-put", "-f", path, dest])
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


try:
    text = run("hdfs dfs -cat /indexer/final/part-*")
except subprocess.CalledProcessError as e:
    print(e, file=sys.stderr)
    sys.exit(1)

vocab, posts, stats = [], [], []
for line in text.splitlines():
    if line.startswith("VOCAB\t"):
        vocab.append(line + "\n")
    elif line.startswith("POST\t"):
        posts.append(line + "\n")
    elif line.startswith(("STAT\t", "DOC_LEN\t", "DOC_TITLE\t")):
        stats.append(line + "\n")

subprocess.check_call("hdfs dfs -mkdir -p /indexer/vocab /indexer/index /indexer/stats".split())
hdfs_put_text("".join(vocab), "/indexer/vocab/part-00000")
hdfs_put_text("".join(posts), "/indexer/index/part-00000")
hdfs_put_text("".join(stats), "/indexer/stats/part-00000")
PY

echo "Index build complete: /indexer/final and /indexer/{vocab,index,stats}"
