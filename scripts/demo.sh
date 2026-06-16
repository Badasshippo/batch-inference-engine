#!/usr/bin/env bash
# Boots the platform and exercises the headline features:
# fair scheduling, 1k-batch throughput, priority, idempotency, cost accounting,
# health model, Prometheus metrics, dead-letter, and cancellation.
set -euo pipefail
cd "$(dirname "$0")/.."

# Enable a modest provider RPS cap so the token bucket is visibly active.
PROVIDER_MAX_RPS=2000 uvicorn app.main:app --host 127.0.0.1 --port 8000 \
  > data/server.log 2>&1 &
SVR=$!
trap 'kill "$SVR" 2>/dev/null || true' EXIT

for _ in $(seq 1 40); do
  curl -sf http://127.0.0.1:8000/healthz >/dev/null 2>&1 && break
  sleep 0.25
done

echo "=== health model ==="
echo -n "livez: ";  curl -s http://127.0.0.1:8000/livez
echo -n "  readyz: "; curl -s http://127.0.0.1:8000/readyz | jq -c '{status,active_jobs}'

echo "=== submit 1,000 prompts (v1, high priority) ==="
JOB=$(curl -s -X POST "http://127.0.0.1:8000/v1/batches/upload?priority=high" \
  -F "file=@data/prompts_1000.json" | jq -r .job_id)
echo "job_id=$JOB"

echo "=== poll progress ==="
for _ in $(seq 1 40); do
  SNAP=$(curl -s "http://127.0.0.1:8000/v1/jobs/$JOB")
  echo "$SNAP" | jq -c '{state,progress,percent,pending,retries,cost:.cost.estimated_cost_usd}'
  [ "$(echo "$SNAP" | jq -r .state)" = "completed" ] && break
  sleep 0.4
done

echo "=== idempotency: same key returns same job ==="
H='Idempotency-Key: demo-key-42'
A=$(curl -s -X POST http://127.0.0.1:8000/v1/batches -H "$H" \
  -H 'Content-Type: application/json' -d '{"prompts":[{"prompt":"hi"}]}' | jq -r .job_id)
B=$(curl -s -X POST http://127.0.0.1:8000/v1/batches -H "$H" \
  -H 'Content-Type: application/json' -d '{"prompts":[{"prompt":"hi"}]}' \
  | jq -c '{job_id,idempotent_reuse}')
echo "first=$A  second=$B"

echo "=== prometheus /metrics (sample) ==="
curl -s http://127.0.0.1:8000/metrics | grep -E \
  '^(batch_prompts_completed_total|inference_rate_limited_total|inference_estimated_cost_usd_total|scheduler_queue_depth|inference_concurrency_limit) '

echo "=== cancellation mid-flight ==="
JOB2=$(curl -s -X POST http://127.0.0.1:8000/v1/batches/upload \
  -F "file=@data/prompts_1000.json" | jq -r .job_id)
sleep 0.3
curl -s -X POST "http://127.0.0.1:8000/v1/jobs/$JOB2/cancel" | jq -c '{state,progress}'

echo "=== sample structured logs (with request_id) ==="
grep -m 1 '"msg": "prompt processed"' data/server.log || tail -n 2 data/server.log
