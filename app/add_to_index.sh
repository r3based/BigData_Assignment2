#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ $# -lt 1 ]]; then
  echo "Usage: add_to_index.sh /path/to/local/doc.txt" >&2
  exit 1
fi

# shellcheck source=/dev/null
source .venv/bin/activate

python3 app.py add-doc "$1"
echo "Document added to Cassandra index."
