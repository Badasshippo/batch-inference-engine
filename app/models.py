"""Pydantic request/response models and internal data structures."""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field


class Priority(str, enum.Enum):
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class PromptItem(BaseModel):
    """A single prompt to run inference on."""

    id: str | None = Field(
        default=None,
        description="Optional caller-supplied id. Auto-generated if omitted.",
    )
    prompt: str = Field(..., min_length=1, description="The prompt text.")


class BatchRequest(BaseModel):
    """Payload for submitting a batch of prompts via JSON."""

    prompts: list[PromptItem] = Field(..., min_length=1)
    priority: Priority = Field(
        default=Priority.NORMAL,
        description="Scheduling priority: high jobs get more throughput; low never starves.",
    )


class JobState(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SubmitResponse(BaseModel):
    """Immediate acknowledgment returned when a batch is accepted."""

    job_id: str
    state: JobState
    total: int
    priority: Priority
    idempotent_reuse: bool = False
    message: str
    status_url: str
    results_url: str


class CostSummary(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    estimated_cost_usd: float = 0.0


class JobStatusResponse(BaseModel):
    """Real-time progress for a batch job."""

    job_id: str
    state: JobState
    priority: Priority
    total: int
    completed: int
    succeeded: int
    failed: int
    pending: int
    retries: int
    progress: str
    percent: float
    cost: CostSummary
    created_at: float
    started_at: float | None
    finished_at: float | None
    duration_seconds: float | None


class InferenceResult(BaseModel):
    """Result of running inference on one prompt."""

    id: str
    prompt: str
    success: bool
    output: str | None = None
    error: str | None = None
    attempts: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0


class JobResultsResponse(BaseModel):
    job_id: str
    state: JobState
    total: int
    succeeded: int
    failed: int
    returned: int = 0
    limit: int = 0
    offset: int = 0
    results: list[InferenceResult]


class DeadLetterResponse(BaseModel):
    """Prompts that exhausted retries or failed unrecoverably."""

    job_id: str
    failed: int
    returned: int
    items: list[InferenceResult]


# --------------------------------------------------------------------------- #
# Internal (non-serialized) structures
# --------------------------------------------------------------------------- #
@dataclass
class WorkItem:
    """A unit of work with a stable, collision-free identity.

    `seq` is assigned once at submission time and is globally unique within the
    job. `id` is the user-facing identifier (their own id, or a generated
    `prompt-N`). Keying results by `seq` means concurrent workers can never
    overwrite each other even if prompts share (or omit) an id.
    """

    seq: int
    id: str
    prompt: str


@dataclass
class Job:
    """In-memory record of a batch job and its live progress counters."""

    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    state: JobState = JobState.PENDING
    priority: Priority = Priority.NORMAL
    total: int = 0
    completed: int = 0
    succeeded: int = 0
    failed: int = 0
    retries: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    idempotency_key: str | None = None
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    # Keyed by WorkItem.seq (unique per item) to prevent result collisions.
    results: dict[int, InferenceResult] = field(default_factory=dict)

    def ordered_results(self) -> list[InferenceResult]:
        """Results in submission order."""
        return [self.results[k] for k in sorted(self.results)]

    def dead_letter(self) -> list[InferenceResult]:
        """Failed results, in submission order (the dead-letter queue)."""
        return [r for r in self.ordered_results() if not r.success]

    def cost_summary(self) -> dict[str, Any]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.input_tokens + self.output_tokens,
            "estimated_cost_usd": round(self.cost_usd, 6),
        }

    def snapshot(self, pending: int = 0) -> dict[str, Any]:
        """Return a consistent point-in-time view of progress counters."""
        duration: float | None = None
        if self.started_at is not None:
            end = self.finished_at if self.finished_at is not None else time.time()
            duration = round(end - self.started_at, 4)
        percent = round((self.completed / self.total) * 100, 2) if self.total else 0.0
        return {
            "job_id": self.id,
            "state": self.state,
            "priority": self.priority,
            "total": self.total,
            "completed": self.completed,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "pending": pending,
            "retries": self.retries,
            "progress": f"{self.completed}/{self.total}",
            "percent": percent,
            "cost": self.cost_summary(),
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": duration,
        }
