#!/usr/bin/env bash
# ОТГ-Audit launcher
set -euo pipefail

cd "$(dirname "$0")"

PORT="${PORT:-8000}"
HOST="${HOST:-0.0.0.0}"

echo "▶ Installing dependencies (if needed)…"
python3 -m pip install --quiet -r requirements.txt

echo "▶ Starting ОТГ-Audit on http://${HOST}:${PORT}"
cd backend
exec python3 -m uvicorn main:app --host "${HOST}" --port "${PORT}"
