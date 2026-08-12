#!/usr/bin/env bash
set -euo pipefail

cd /root/task

echo "[1/6] Installing Python dependencies..."
pip install -q -r requirements.txt

echo "[2/6] Starting MinIO object storage..."
docker compose up -d

echo "[3/6] Waiting for MinIO to become healthy..."
ATTEMPTS=0
MAX_ATTEMPTS=40
until curl -sf http://127.0.0.1:9000/minio/health/ready >/dev/null 2>&1; do
  ATTEMPTS=$((ATTEMPTS+1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "ERROR: MinIO did not become ready in time." >&2
    exit 1
  fi
  sleep 2
done
echo "MinIO is ready."

echo "[4/6] Initializing lakehouse bucket and loading bronze fixtures..."
python -m src.selfcheck --init

echo "[5/6] Verifying starter modules import and configuration loads..."
python -c "import src.lakehouse_config, src.models, src.pipeline, src.queries; print('Starter modules import OK')"

echo "[6/6] Collecting candidate tests (collect-only, not executed)..."
python -m pytest tests/ --collect-only -q || true

echo "Ready: lakehouse scaffold is running. Run the tests separately after making changes."
