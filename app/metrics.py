"""A tiny, dependency-free Prometheus metrics registry.

Why hand-rolled instead of `prometheus_client`?
  * Zero extra dependencies and no global multiprocess state to reason about.
  * Fully deterministic and trivially unit-testable (we can read counter values
    directly and reset between tests).
The exposition output conforms to the Prometheus text format v0.0.4, so it is
scrapable by a real Prometheus / DigitalOcean monitoring stack as-is. In a
larger system you would swap this for `prometheus_client`; the call sites
(`metrics.prompts_completed.inc()`) would not change.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field


class Counter:
    """A monotonically increasing counter."""

    def __init__(self, name: str, help: str) -> None:
        self.name = name
        self.help = help
        self._value = 0.0
        self._lock = threading.Lock()

    def inc(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value += amount

    @property
    def value(self) -> float:
        return self._value

    def render(self) -> list[str]:
        return [
            f"# HELP {self.name} {self.help}",
            f"# TYPE {self.name} counter",
            f"{self.name} {self._value}",
        ]


# Default histogram buckets (seconds), tuned for sub-second inference latencies.
DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


@dataclass
class Histogram:
    """A cumulative histogram with the standard Prometheus bucket semantics."""

    name: str
    help: str
    buckets: tuple[float, ...] = DEFAULT_BUCKETS
    _counts: list[int] = field(default_factory=list)
    _sum: float = 0.0
    _count: int = 0

    def __post_init__(self) -> None:
        self._counts = [0 for _ in self.buckets]
        self._lock = threading.Lock()

    def observe(self, value: float) -> None:
        with self._lock:
            self._sum += value
            self._count += 1
            for i, edge in enumerate(self.buckets):
                if value <= edge:
                    self._counts[i] += 1

    @property
    def count(self) -> int:
        return self._count

    def render(self) -> list[str]:
        lines = [
            f"# HELP {self.name} {self.help}",
            f"# TYPE {self.name} histogram",
        ]
        cumulative = 0
        for edge, c in zip(self.buckets, self._counts):
            cumulative += c
            lines.append(f'{self.name}_bucket{{le="{edge}"}} {cumulative}')
        lines.append(f'{self.name}_bucket{{le="+Inf"}} {self._count}')
        lines.append(f"{self.name}_sum {self._sum}")
        lines.append(f"{self.name}_count {self._count}")
        return lines


class Metrics:
    """The application's metric registry."""

    def __init__(self) -> None:
        self.jobs_submitted = Counter(
            "batch_jobs_submitted_total", "Total batch jobs accepted."
        )
        self.jobs_rejected = Counter(
            "batch_jobs_rejected_total", "Batch jobs rejected due to overload."
        )
        self.jobs_completed = Counter(
            "batch_jobs_completed_total", "Batch jobs that reached completed state."
        )
        self.jobs_cancelled = Counter(
            "batch_jobs_cancelled_total", "Batch jobs cancelled."
        )
        self.prompts_completed = Counter(
            "batch_prompts_completed_total", "Prompts processed (success or failure)."
        )
        self.prompts_succeeded = Counter(
            "batch_prompts_succeeded_total", "Prompts that produced a completion."
        )
        self.prompts_failed = Counter(
            "batch_prompts_failed_total", "Prompts that ultimately failed."
        )
        self.inference_retries = Counter(
            "inference_retries_total", "Total retry attempts across all prompts."
        )
        self.inference_rate_limited = Counter(
            "inference_rate_limited_total", "Total HTTP 429 responses observed."
        )
        self.inference_latency = Histogram(
            "inference_latency_seconds", "End-to-end latency per prompt (incl. retries)."
        )
        self.job_duration = Histogram(
            "job_duration_seconds",
            "Wall-clock duration of a batch job.",
            buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
        )

    def reset(self) -> None:
        """Re-initialize all metrics in place (keeps the singleton identity)."""
        self.__init__()

    def render(self) -> str:
        blocks: list[str] = []
        for attr in vars(self).values():
            if isinstance(attr, (Counter, Histogram)):
                blocks.append("\n".join(attr.render()))
        return "\n".join(blocks) + "\n"


# Process-wide singleton. Reset in place via `reset_metrics()` in tests.
metrics = Metrics()


def reset_metrics() -> None:
    """Reset the global metrics in place (test helper)."""
    metrics.reset()
