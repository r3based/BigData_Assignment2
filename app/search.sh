#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -d .venv ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi

export PYSPARK_DRIVER_PYTHON
PYSPARK_DRIVER_PYTHON="$(which python3)"
# Worker containers unpack .venv.tar.gz; absolute /app/.venv does not exist on NMs.
export PYSPARK_PYTHON
PYSPARK_PYTHON="./.venv/bin/python"

QUERY="${1:-}"
if [[ -z "${QUERY}" ]]; then
  echo "Usage: search.sh \"your query text\"" >&2
  exit 1
fi

printf '%s\n' "${QUERY}" | spark-submit \
  --master yarn \
  --deploy-mode client \
  --archives "${PWD}/.venv.tar.gz#.venv" \
  "${PWD}/query.py"
