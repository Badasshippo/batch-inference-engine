# Architecture Decision Records

Short ADRs capturing the *why* behind the design. Each is intentionally terse:
context → decision → consequences.

## ADR-001: Single global fair scheduler instead of per-job worker pools

**Context.** The naive design gives each submitted batch its own worker pool. If
100 clients each submit a 1,000-prompt batch, that is 100 pools and concurrency
multiplies — the box and the upstream provider get hammered, and a single huge
batch can monopolize resources.

**Decision.** All jobs feed one `FairScheduler`; a single global worker pool
drains it using **weighted round-robin with deficit credits** (priority weights
high=4 / normal=2 / low=1; every job has weight ≥ 1).

**Consequences.** Total concurrency is bounded regardless of how many jobs exist.
Small jobs interleave with and finish ahead of huge ones; higher priority gets
proportionally more throughput; nothing starves. The scheduler is the single
place that defines multi-tenant fairness.

## ADR-002: Proactive rate limiting (token bucket + AIMD) on top of retries

**Context.** Retries with backoff handle a 429 *after* it happens. Under load
that still produces a thundering herd that repeatedly trips the provider.

**Decision.** Add two proactive controls: a **token bucket** for a hard global
RPS ceiling, and an **AIMD controller** (additive-increase / multiplicative-
decrease — TCP's congestion-control law) that shrinks allowed concurrency on each
observed 429 and slowly grows it back during success streaks.

**Consequences.** The system self-tunes to the provider's real capacity and
settles at an equilibrium rather than oscillating between "blast" and "blocked."
Defaults are tuned (gentle 0.8 decrease, short success streak) so a steady
background 429 rate converges to a healthy concurrency level instead of the floor.

## ADR-003: Idempotency keys on submit

**Context.** Clients retry POSTs (network blips, timeouts). Without protection a
retried submit creates duplicate jobs and double work.

**Decision.** Honor an `Idempotency-Key` header; the first submit creates the job
and the key→job mapping, and any later submit with the same key returns the
original job with `idempotent_reuse: true`.

**Consequences.** Safe client retries; exactly-once submit semantics per key.
The mapping currently lives in the in-memory store (see ADR-005).

## ADR-004: Split health into liveness vs readiness

**Context.** Orchestrators need to distinguish "process is alive" from "can take
new work." Conflating them causes traffic to be routed to a saturated/draining
instance, or healthy instances to be killed.

**Decision.** `/livez` (process up), `/healthz` (basic ok), and `/readyz` which
returns `503` when the engine is not accepting (graceful shutdown) or is
saturated (`active_jobs >= MAX_ACTIVE_JOBS`).

**Consequences.** Clean rolling deploys: a draining pod fails readiness (stops
receiving traffic) while still passing liveness (not force-killed) until
in-flight work drains.

## ADR-005: In-memory job store behind a `JobStore` interface

**Context.** Durability and multi-instance scale-out require a shared, persistent
store. Building that now is out of scope for the interview timebox.

**Decision.** Depend only on a `JobStore` protocol; ship `InMemoryJobStore`.
Document the production swap: Postgres for job/result rows, Redis for hot
progress counters.

**Consequences.** The scheduling/retry logic is decoupled from persistence.
Known limitation today: state is per-process, so a job is only visible on the
instance that accepted it. Horizontal scale-out requires the shared-store swap
(and ideally a distributed task queue).

## ADR-006: Dead-letter queue + replay

**Context.** Some prompts fail permanently (or exhaust retries). Operators need
to see and recover them.

**Decision.** Failed results form a per-job dead-letter queue
(`GET /v1/jobs/{id}/dead-letter`), and `POST /v1/jobs/{id}/replay-failed` spawns a
new job containing only the failed prompts.

**Consequences.** Failures are inspectable and recoverable without resubmitting
the whole batch — a real operability win.

## ADR-007: Hand-rolled Prometheus metrics (no client dependency)

**Context.** We want first-class metrics but `prometheus_client` carries global/
multiprocess state that complicates deterministic testing.

**Decision.** A tiny in-process registry that emits the Prometheus text format.
Call sites (`metrics.prompts_completed.inc()`) match the real library, so it can
be swapped later with no engine changes.

**Consequences.** Zero extra deps, trivially testable, scrapable as-is. For
multi-process servers you'd switch to `prometheus_client` with a multiprocess
collector.
