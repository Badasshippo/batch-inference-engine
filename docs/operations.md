# Operations Runbook

How to operate the Batch Inference Engine in production.

## Signals

| Type | Endpoint / source | Notes |
|---|---|---|
| Liveness | `GET /livez` | Process up. Wire to Kubernetes livenessProbe. |
| Readiness | `GET /readyz` | `503` when draining or saturated. Wire to readinessProbe / App Platform health check. |
| Metrics | `GET /metrics` | Prometheus text format. |
| Logs | stdout (JSON) | One object per line; includes `request_id`, `job_id`, `prompt_id`, `attempt`, `status`, `latency_ms`, `cost_usd`. |

## Key metrics

Counters:
`batch_jobs_submitted_total`, `batch_jobs_completed_total`, `batch_jobs_rejected_total`,
`batch_jobs_cancelled_total`, `batch_prompts_completed_total`,
`batch_prompts_succeeded_total`, `batch_prompts_failed_total`,
`inference_retries_total`, `inference_rate_limited_total`,
`inference_retries_exhausted_total`, `batch_idempotent_reuse_total`,
`inference_estimated_cost_usd_total`, `inference_tokens_total`.

Gauges:
`scheduler_queue_depth`, `scheduler_active_jobs`, `inference_inflight`,
`inference_concurrency_limit`.

Histograms:
`inference_latency_seconds`, `job_duration_seconds`.

## Suggested alert rules (Prometheus)

```yaml
groups:
  - name: batch-inference
    rules:
      - alert: HighRateLimitRate
        expr: rate(inference_rate_limited_total[5m]) / clamp_min(rate(batch_prompts_completed_total[5m]), 1) > 0.3
        for: 10m
        annotations:
          summary: ">30% of prompts are hitting 429s; provider is throttling hard."

      - alert: RetryExhaustionTooHigh
        expr: rate(inference_retries_exhausted_total[10m]) / clamp_min(rate(batch_prompts_completed_total[10m]), 1) > 0.01
        for: 10m
        annotations:
          summary: ">1% of prompts exhausted retries (landing in dead-letter)."

      - alert: QueueSaturation
        expr: scheduler_queue_depth > 8000
        for: 5m
        annotations:
          summary: "Scheduler backlog high; scale out or raise concurrency."

      - alert: HighInferenceLatencyP95
        expr: histogram_quantile(0.95, sum(rate(inference_latency_seconds_bucket[5m])) by (le)) > 2
        for: 10m
        annotations:
          summary: "p95 inference latency > 2s."

      - alert: ConcurrencyCollapsed
        expr: inference_concurrency_limit <= 4
        for: 15m
        annotations:
          summary: "AIMD limiter pinned at floor; provider sustained throttling."

      - alert: NotReady
        expr: up == 1 and probe_success{job="readyz"} == 0
        for: 5m
        annotations:
          summary: "Instance failing readiness for 5m."

      - alert: CostSpike
        expr: rate(inference_estimated_cost_usd_total[15m]) * 3600 > 5
        for: 15m
        annotations:
          summary: "Estimated inference spend > $5/hour; check for runaway batches."

      - alert: DeadLetterGrowth
        expr: rate(batch_prompts_failed_total[15m]) > 0.5
        for: 15m
        annotations:
          summary: "Dead-letter queue growing > 0.5 prompts/s; inspect failures."
```

## Common runbook procedures

**Symptom: jobs slow, `inference_concurrency_limit` low, 429 rate high.**
The provider is throttling. The AIMD limiter is doing its job. Mitigate by
adding provider capacity/quota, or lower `PROVIDER_MAX_RPS` to stop overshooting.

**Symptom: `/readyz` returning 503 (saturated).**
`active_jobs >= MAX_ACTIVE_JOBS`. Scale out replicas or raise `MAX_ACTIVE_JOBS`
if the box has headroom. Clients are already being shed with `503 + Retry-After`.

**Symptom: prompts in dead-letter.**
Inspect `GET /v1/jobs/{id}/dead-letter`. If transient/provider-side, recover with
`POST /v1/jobs/{id}/replay-failed`.

**Symptom: cost spike.**
Watch `inference_estimated_cost_usd_total`. Per-job cost is in the status
response. Set budget alerts on the cost counter.

## Deploy / rollout

Rolling deploys rely on the health split (ADR-004) + graceful shutdown:
on `SIGTERM` the engine stops accepting (`/readyz` → 503), drains in-flight
prompts for `GRACEFUL_SHUTDOWN_SECONDS`, then marks any unfinished jobs cancelled.
Set the orchestrator's grace period ≥ `GRACEFUL_SHUTDOWN_SECONDS`.
See [deploy-digitalocean.md](deploy-digitalocean.md).
