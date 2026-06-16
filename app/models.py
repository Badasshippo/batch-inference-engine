"""Pydantic request/response models and internal data structures."""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field


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


class JobState(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class SubmitResponse(BaseModel):
    """Immediate acknowledgment returned when a batch is accepted."""

    job_id: str
    state: JobState
    total: int
    message: str
    status_url: str
    results_url: str


class JobStatusResponse(BaseModel):
    """Real-time progress for a batch job."""

    job_id: str
    state: JobState
    total: int
    completed: int
    succeeded: int
    failed: int
    retries: int
    progress: str
    percent: float
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


class JobResultsResponse(BaseModel):
    job_id: str
    state: JobState
    total: int
    succeeded: int
    failed: int
    results: list[InferenceResult]


# --------------------------------------------------------------------------- #
# Internal (non-serialized) structures
# --------------------------------------------------------------------------- #
@dataclass
class Job:
    """In-memory record of a batch job and its live progress counters."""

    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    state: JobState = JobState.PENDING
    total: int = 0
    completed: int = 0
    succeeded: int = 0
    failed: int = 0
    retries: int = 0
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    results: dict[str, InferenceResult] = field(default_factory=dict)

    def snapshot(self) -> dict[str, Any]:
        """Return a consistent point-in-time view of progress counters."""
        duration: float | None = None
        if self.started_at is not None:
            end = self.finished_at if self.finished_at is not None else time.time()
            duration = round(end - self.started_at, 4)
        percent = round((self.completed / self.total) * 100, 2) if self.total else 0.0
        return {
            "job_id": self.id,
            "state": self.state,
            "total": self.total,
            "completed": self.completed,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "retries": self.retries,
            "progress": f"{self.completed}/{self.total}",
            "percent": percent,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": duration,
        }
