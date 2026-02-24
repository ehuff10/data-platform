#!/usr/bin/env bash
set -euo pipefail

# Create venv only if missing
if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate

pip install -r requirements-etl.txt

python -m src.etl.ingest_api
