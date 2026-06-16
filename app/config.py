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
