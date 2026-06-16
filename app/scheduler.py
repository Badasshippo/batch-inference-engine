"""A global, multi-tenant fair scheduler.

Instead of giving every job its own worker pool (which lets a single huge batch
monopolize the box and multiplies concurrency), all jobs feed one global
scheduler that a fixed worker pool drains. Scheduling is **weighted round-robin
with deficit credits**:

* Each active job gets a per-round credit equal to its priority weight
  (high=4, normal=2, low=1). Within a round, jobs are served up to their credit;
  when all credits are spent the round resets.
* Because every job (even `low`) has weight >= 1, no job can be starved: a small
  job interleaves with and finishes well ahead of a 10k-prompt batch, while
  higher-priority jobs still get proportionally more throughput.

`pop()` is synchronous and non-blocking (returns `None` when empty); the engine's
workers call it in a loop. This keeps the hot path lock-free on the single
asyncio event loop.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from .models import Priority, WorkItem

PRIORITY_WEIGHTS: dict[Priority, int] = {
    Priority.HIGH: 4,
    Priority.NORMAL: 2,
    Priority.LOW: 1,
}


@dataclass
class _JobQueue:
    job_id: str
    weight: int
    items: deque[WorkItem]
    credit: int = 0


@dataclass
class FairScheduler:
    _queues: dict[str, _JobQueue] = field(default_factory=dict)
    _pending: int = 0

    def add_job(self, job_id: str, items: list[WorkItem], priority: Priority) -> None:
        weight = PRIORITY_WEIGHTS.get(priority, 2)
        self._queues[job_id] = _JobQueue(job_id, weight, deque(items), credit=weight)
        self._pending += len(items)

    def remove_job(self, job_id: str) -> int:
        """Drop a job's *pending* items (e.g. on cancel). Returns count removed."""
        q = self._queues.pop(job_id, None)
        if q is None:
            return 0
        removed = len(q.items)
        self._pending -= removed
        return removed

    def pop(self) -> tuple[str, WorkItem] | None:
        """Return the next (job_id, item) by weighted round-robin, or None."""
        # Drop drained queues.
        for jid in [j for j, q in self._queues.items() if not q.items]:
            del self._queues[jid]
        if not self._queues:
            return None

        # If nobody has credit left for this round, start a new round.
        if not any(q.credit > 0 for q in self._queues.values()):
            for q in self._queues.values():
                q.credit = q.weight

        for q in self._queues.values():
            if q.credit > 0 and q.items:
                q.credit -= 1
                self._pending -= 1
                return q.job_id, q.items.popleft()

        # Some jobs have items but zero credit and none could be reset above
        # (shouldn't happen); reset and retry once.
        for q in self._queues.values():
            q.credit = q.weight
        for q in self._queues.values():
            if q.items:
                q.credit -= 1
                self._pending -= 1
                return q.job_id, q.items.popleft()
        return None

    @property
    def pending(self) -> int:
        return self._pending

    @property
    def active_jobs(self) -> int:
        return len(self._queues)

    def job_pending(self, job_id: str) -> int:
        q = self._queues.get(job_id)
        return len(q.items) if q else 0
