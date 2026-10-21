#!/usr/bin/env bash
# Этап 3а: MapReduce поверх объектного хранилища.
# Демонстрация отказа маппера: --fail-chunk 6 --fail-mode exit --tag _failure --no-upload
set -euo pipefail
cd "$(dirname "$0")/.."
python src/mapreduce/main.py "$@"
