"""Job persistence behind a small interface.

The engine depends only on the `JobStore` protocol, so the in-memory store used
here can be swapped for a durable backend (DigitalOcean Managed Postgres for the
job/result rows, Managed Redis for hot progress counters) without touching the
scheduling or retry logic. That swap is what makes the service horizontally
scalable: today the store is per-process, so a job is only visible on the
instance that accepted it.
"""
from __future__ import annotations

from typing import Protocol

from .models import Job


class JobStore(Protocol):
    def create(self, job: Job) -> None: ...
    def get(self, job_id: str) -> Job | None: ...
    def all(self) -> list[Job]: ...
    def find_by_idempotency_key(self, key: str) -> Job | None: ...


class InMemoryJobStore:
    """Dict-backed store. Safe on the single asyncio event loop (no preemption)."""

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._by_key: dict[str, str] = {}

    def create(self, job: Job) -> None:
        self._jobs[job.id] = job
        if job.idempotency_key:
            self._by_key[job.idempotency_key] = job.id

    def get(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def all(self) -> list[Job]:
        return list(self._jobs.values())

    def find_by_idempotency_key(self, key: str) -> Job | None:
        job_id = self._by_key.get(key)
        return self._jobs.get(job_id) if job_id else None
