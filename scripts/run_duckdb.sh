#!/usr/bin/env bash
# Этап 3б: подсчёт навыков, пар и троек в DuckDB по данным из S3.
set -euo pipefail
cd "$(dirname "$0")/.."
python scripts/run_duckdb.py "$@"
