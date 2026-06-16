#!/usr/bin/env bash
# Boots the API, submits a 1,000-prompt batch, polls progress, and prints a
# sample of the aggregated results. Used for local verification.
set -euo pipefail
cd "$(dirname "$0")/.."

uvicorn app.main:app --host 127.0.0.1 --port 8000 > data/server.log 2>&1 &
SVR=$!
trap 'kill "$SVR" 2>/dev/null || true' EXIT

for _ in $(seq 1 40); do
  curl -sf http://127.0.0.1:8000/healthz >/dev/null 2>&1 && break
  sleep 0.25
done

echo "=== health ==="
curl -s http://127.0.0.1:8000/healthz; echo

echo "=== submit 1,000 prompts via file upload ==="
JOB=$(curl -s -X POST http://127.0.0.1:8000/batches/upload \
  -F "file=@data/prompts_1000.json" | jq -r .job_id)
echo "job_id=$JOB"

echo "=== poll progress ==="
for _ in $(seq 1 15); do
  curl -s "http://127.0.0.1:8000/jobs/$JOB" \
    | jq -c '{state,progress,percent,succeeded,failed,retries,duration_seconds}'
  STATE=$(curl -s "http://127.0.0.1:8000/jobs/$JOB" | jq -r .state)
  [ "$STATE" = "completed" ] && break
  sleep 0.4
done

echo "=== aggregated results (sample) ==="
curl -s "http://127.0.0.1:8000/jobs/$JOB/results" \
  | jq -c '{total,succeeded,failed,first_result:.results[0]}'
