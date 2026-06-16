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

from fastapi import FastAPI, File, HTTPException, Request, UploadFile, status
from fastapi.responses import JSONResponse

from . import __version__
from .engine import BatchEngine
from .models import (
    BatchRequest,
    JobResultsResponse,
    JobStatusResponse,
    PromptItem,
    SubmitResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.engine = BatchEngine()
    try:
        yield
    finally:
        await app.state.engine.shutdown()


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


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


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
    job = await _engine(request).submit(payload.prompts)
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

    job = await _engine(request).submit(prompts)
    return _ack(job, request)


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def job_status(job_id: str, request: Request) -> JobStatusResponse:
    """Return live progress for a batch job."""
    job = _engine(request).get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    return JobStatusResponse(**job.snapshot())


@app.get("/jobs/{job_id}/results", response_model=JobResultsResponse)
async def job_results(job_id: str, request: Request) -> JobResultsResponse:
    """Return the aggregated results for a job (partial if still running)."""
    job = _engine(request).get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    return JobResultsResponse(
        job_id=job.id,
        state=job.state,
        total=job.total,
        succeeded=job.succeeded,
        failed=job.failed,
        results=list(job.results.values()),
    )


@app.exception_handler(Exception)
async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})
