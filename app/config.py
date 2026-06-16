"""Runtime configuration for the batch inference engine.

All values can be overridden via environment variables so the service can be
tuned in different deployments (and so tests can force deterministic behavior).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _get_int(name: str, default: int) -> int:
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default


def _get_float(name: str, default: float) -> float:
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return default


@dataclass(frozen=True)
class Settings:
    # Size of the bounded worker pool. This is the single most important knob
    # for concurrency discipline: it caps how many prompts are in flight at once
    # so we never spawn unbounded tasks or exhaust memory.
    worker_pool_size: int = _get_int("WORKER_POOL_SIZE", 16)

    # Upper bound on the internal job queue. Bounding the queue applies
    # backpressure so a huge batch cannot balloon memory usage.
    max_queue_size: int = _get_int("MAX_QUEUE_SIZE", 10_000)

    # WORKER_POOL_SIZE is the size of the single, *global* worker pool that
    # drains the fair scheduler (all jobs share it).
    # GLOBAL_MAX_CONCURRENCY is the upper bound the adaptive limiter may grow to.
    global_max_concurrency: int = _get_int("GLOBAL_MAX_CONCURRENCY", 64)

    # Adaptive (AIMD) concurrency controller bounds + tuning. Defaults are tuned
    # to settle at a healthy equilibrium under a steady background 429 rate
    # rather than collapsing to the floor: gentle multiplicative decrease + a
    # short success streak before additive increase.
    adaptive_min_concurrency: int = _get_int("ADAPTIVE_MIN_CONCURRENCY", 4)
    adaptive_increase_after: int = _get_int("ADAPTIVE_INCREASE_AFTER", 5)
    adaptive_decrease_factor: float = _get_float("ADAPTIVE_DECREASE_FACTOR", 0.8)

    # Token-bucket cap on upstream requests/sec (0 disables proactive limiting).
    provider_max_rps: float = _get_float("PROVIDER_MAX_RPS", 0.0)
    provider_burst: float = _get_float("PROVIDER_BURST", 0.0)

    # Cost accounting (USD per 1K tokens) and a rough token estimator.
    cost_per_1k_input_tokens: float = _get_float("COST_PER_1K_INPUT_TOKENS", 0.00015)
    cost_per_1k_output_tokens: float = _get_float("COST_PER_1K_OUTPUT_TOKENS", 0.0006)
    chars_per_token: float = _get_float("CHARS_PER_TOKEN", 4.0)

    # API-level backpressure: refuse new batches (HTTP 503 + Retry-After) once
    # this many jobs are actively running, so the service degrades gracefully
    # instead of falling over under load.
    max_active_jobs: int = _get_int("MAX_ACTIVE_JOBS", 50)
    overload_retry_after_seconds: int = _get_int("OVERLOAD_RETRY_AFTER_SECONDS", 5)

    # How long to let in-flight prompts finish during a graceful shutdown.
    graceful_shutdown_seconds: float = _get_float("GRACEFUL_SHUTDOWN_SECONDS", 10.0)

    # Optional API-key auth. Empty => auth disabled (open). When set, requests to
    # the versioned API must send a matching `X-API-Key` header.
    api_key: str = os.environ.get("API_KEY", "")

    # Retry / backoff policy for HTTP 429 (and transient errors).
    max_retries: int = _get_int("MAX_RETRIES", 5)
    backoff_base_seconds: float = _get_float("BACKOFF_BASE_SECONDS", 0.2)
    backoff_max_seconds: float = _get_float("BACKOFF_MAX_SECONDS", 10.0)
    backoff_jitter: float = _get_float("BACKOFF_JITTER", 0.1)

    # Mock inference endpoint behavior.
    mock_rate_limit_every: int = _get_int("MOCK_RATE_LIMIT_EVERY", 7)
    mock_min_latency_ms: int = _get_int("MOCK_MIN_LATENCY_MS", 5)
    mock_max_latency_ms: int = _get_int("MOCK_MAX_LATENCY_MS", 25)


def get_settings() -> Settings:
    return Settings()
