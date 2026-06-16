#!/usr/bin/env bash
# Boots the API, submits a 1,000-prompt batch, polls progress, shows the
# aggregated results (paginated), Prometheus metrics, and a cancellation.
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

echo "=== aggregated results (paginated: limit=2) ==="
curl -s "http://127.0.0.1:8000/jobs/$JOB/results?limit=2&offset=0" \
  | jq -c '{total,succeeded,failed,returned,limit,offset,first:.results[0].id}'

echo "=== prometheus /metrics (sample) ==="
curl -s http://127.0.0.1:8000/metrics \
  | grep -E '^(batch_jobs_submitted_total|batch_prompts_completed_total|inference_retries_total|inference_rate_limited_total) '

echo "=== cancellation demo (new job, cancelled mid-flight) ==="
JOB2=$(curl -s -X POST http://127.0.0.1:8000/batches/upload \
  -F "file=@data/prompts_1000.json" | jq -r .job_id)
sleep 0.3
curl -s -X POST "http://127.0.0.1:8000/jobs/$JOB2/cancel" | jq -c '{state,progress}'

echo "=== sample structured logs ==="
grep -m 2 '"msg": "prompt processed"' data/server.log || tail -n 3 data/server.log
