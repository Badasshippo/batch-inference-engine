# Service Level Objectives

SLOs define the reliability target; the error budget is what we are allowed to
burn before we stop shipping features and focus on reliability.

## SLIs → SLOs

| # | SLI (what we measure) | SLO (target) | Source |
|---|---|---|---|
| 1 | **Availability** — fraction of `POST /v1/batches` returning non-5xx (excluding intentional `503` backpressure) | **99.9%** over 30 days | API metrics / load balancer |
| 2 | **Submit latency** — p95 time to `202 Accepted` | **< 150 ms** | `http_request_duration` (LB) |
| 3 | **Batch completion** — fraction of 1,000-prompt jobs completing | **99%** complete | `batch_jobs_completed_total` / `submitted` |
| 4 | **Batch timeliness** — p95 `job_duration_seconds` for a 1,000-prompt job | **< 30 s** at default concurrency | `job_duration_seconds` histogram |
| 5 | **Prompt success** — successful prompts / total | **≥ 99%** (≤ 1% dead-letter) | `prompts_succeeded` / `prompts_completed` |

## Error budget

- Availability SLO 99.9%/30d ⇒ **~43 min/month** of allowed downtime.
- Prompt success 99% ⇒ up to **1%** of prompts may land in the dead-letter queue
  before the budget is exhausted.

Policy: if the 30-day error budget for SLI #1 or #5 is >50% burned, freeze
feature work and prioritize reliability (provider quota, scale-out, retry tuning).

## How the design defends each SLO

- **#1 Availability:** backpressure sheds load with `503 + Retry-After` instead of
  crashing; idempotency keys make client retries safe.
- **#2 Submit latency:** submit only enqueues and returns; all work is background.
- **#3/#4 Completion & timeliness:** bounded global worker pool + AIMD keeps
  throughput near provider capacity without self-inflicted 429 storms.
- **#5 Prompt success:** retries with backoff absorb transient 429s; only truly
  unrecoverable prompts reach the dead-letter queue (and can be replayed).

## Alerting

Alert when burn rate implies the monthly budget will be exhausted early (fast-burn
1h window + slow-burn 6h window). Concrete Prometheus rules:
[operations.md](operations.md#suggested-alert-rules-prometheus).

## Runbook

Incident procedures and dashboards: [operations.md](operations.md).
