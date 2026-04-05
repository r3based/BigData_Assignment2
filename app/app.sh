#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

service ssh restart

bash start-services.sh

python3 -m venv .venv
# shellcheck source=/dev/null
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
venv-pack -o .venv.tar.gz

echo "Waiting for Cassandra to accept connections..."
python - <<'PY'
import time

from cassandra.cluster import Cluster

hosts = ["cassandra-server"]
for _ in range(60):
    try:
        Cluster(hosts).connect()
        print("Cassandra is up.")
        break
    except Exception:
        time.sleep(2)
else:
    raise SystemExit("Cassandra did not become ready in time.")
PY

bash prepare_data.sh
bash index.sh /input/data

bash search.sh "christmas carol history"
bash search.sh "architecture design"
