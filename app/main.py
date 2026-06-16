"""FastAPI surface for the batch inference engine.

Endpoints
---------
POST /batches            Submit a JSON batch of prompts (immediate ack).
POST /batches/upload     Submit a batch by uploading a .json file (immediate ack).
GET  /jobs/{id}          Real-time progress for a job (e.g. 400/1000 completed).
GET  /jobs/{id}/results  Aggregated results once (or while) processing.
GET  /healthz            Liveness probe.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import JSONResponse, PlainTextResponse

from . import __version__
from .engine import BatchEngine, OverloadedError
from .logging_config import get_logger, setup_logging
from .metrics import metrics
from .models import (
    BatchRequest,
    JobResultsResponse,
    JobStatusResponse,
    PromptItem,
    SubmitResponse,
)

log = get_logger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    app.state.engine = BatchEngine()
    log.info("service started", extra={"version": __version__})
    try:
        yield
    finally:
        await app.state.engine.shutdown()
        log.info("service stopped")


app = FastAPI(
    title="Batch Inference Engine",
    version=__version__,
    description=(
        "Reads a batch of AI prompts, processes them concurrently against a "
        "mock rate-limited inference endpoint, and aggregates the results."
    ),
    lifespan=lifespan,
)


def _engine(request: Request) -> BatchEngine:
    return request.app.state.engine


def _ack(job, request: Request) -> SubmitResponse:
    base = str(request.base_url).rstrip("/")
    return SubmitResponse(
        job_id=job.id,
        state=job.state,
        total=job.total,
        message="Batch accepted. Processing in the background.",
        status_url=f"{base}/jobs/{job.id}",
        results_url=f"{base}/jobs/{job.id}/results",
    )


def _overloaded(exc: OverloadedError) -> HTTPException:
    """Translate an OverloadedError into a 503 with a Retry-After header."""
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=str(exc),
        headers={"Retry-After": str(exc.retry_after)},
    )


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics() -> str:
    """Prometheus exposition endpoint (text format v0.0.4)."""
    return metrics.render()


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "service": "batch-inference-engine",
        "version": __version__,
        "docs": "/docs",
    }


@app.post(
    "/batches",
    response_model=SubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_batch(payload: BatchRequest, request: Request) -> SubmitResponse:
    """Accept a JSON array of prompts and start background processing."""
    try:
        job = await _engine(request).submit(payload.prompts)
    except OverloadedError as exc:
        raise _overloaded(exc) from exc
    return _ack(job, request)


@app.post(
    "/batches/upload",
    response_model=SubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def upload_batch(request: Request, file: UploadFile = File(...)) -> SubmitResponse:
    """Accept a batch via uploaded JSON file.

    The file may contain either a bare array of prompts or an object with a
    top-level `prompts` key. Each prompt may be a string or {id?, prompt}.
    """
    raw = await file.read()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}") from exc

    items = data.get("prompts") if isinstance(data, dict) else data
    if not isinstance(items, list) or not items:
        raise HTTPException(
            status_code=400,
            detail="Expected a non-empty JSON array of prompts (or {prompts: [...]}).",
        )

    prompts: list[PromptItem] = []
    for i, entry in enumerate(items):
        if isinstance(entry, str):
            prompts.append(PromptItem(id=f"prompt-{i + 1}", prompt=entry))
        elif isinstance(entry, dict) and "prompt" in entry:
            prompts.append(PromptItem(id=entry.get("id"), prompt=entry["prompt"]))
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Item {i} must be a string or an object with a 'prompt' field.",
            )

    try:
        job = await _engine(request).submit(prompts)
    except OverloadedError as exc:
        raise _overloaded(exc) from exc
    return _ack(job, request)


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def job_status(job_id: str, request: Request) -> JobStatusResponse:
    """Return live progress for a batch job."""
    job = _engine(request).get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    return JobStatusResponse(**job.snapshot())


@app.get("/jobs/{job_id}/results", response_model=JobResultsResponse)
async def job_results(
    job_id: str,
    request: Request,
    limit: int = Query(100, ge=1, le=1000, description="Max results to return."),
    offset: int = Query(0, ge=0, description="Number of results to skip."),
) -> JobResultsResponse:
    """Return the aggregated results for a job (partial if still running).

    Results are paginated; a 1,000-prompt job should not return everything in a
    single response by default.
    """
    job = _engine(request).get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    ordered = job.ordered_results()
    page = ordered[offset : offset + limit]
    return JobResultsResponse(
        job_id=job.id,
        state=job.state,
        total=job.total,
        succeeded=job.succeeded,
        failed=job.failed,
        returned=len(page),
        limit=limit,
        offset=offset,
        results=page,
    )


@app.post("/jobs/{job_id}/cancel", response_model=JobStatusResponse)
async def cancel_job(job_id: str, request: Request) -> JobStatusResponse:
    """Request cancellation of a running job."""
    engine = _engine(request)
    outcome = await engine.cancel(job_id)
    if outcome is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    if outcome is False:
        raise HTTPException(status_code=409, detail="Job has already finished.")
    job = engine.get_job(job_id)
    return JobStatusResponse(**job.snapshot())


@app.exception_handler(Exception)
async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})
