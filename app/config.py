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

    # Global cap on concurrent inference calls *across all jobs*. Each job runs
    # its own worker pool, so without this a flood of concurrent batches could
    # multiply concurrency. This semaphore is the true system-wide limit.
    global_max_concurrency: int = _get_int("GLOBAL_MAX_CONCURRENCY", 64)

    # API-level backpressure: refuse new batches (HTTP 503 + Retry-After) once
    # this many jobs are actively running, so the service degrades gracefully
    # instead of falling over under load.
    max_active_jobs: int = _get_int("MAX_ACTIVE_JOBS", 50)
    overload_retry_after_seconds: int = _get_int("OVERLOAD_RETRY_AFTER_SECONDS", 5)

    # How long to let in-flight prompts finish during a graceful shutdown.
    graceful_shutdown_seconds: float = _get_float("GRACEFUL_SHUTDOWN_SECONDS", 10.0)

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
