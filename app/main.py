"""FastAPI surface for the batch inference platform.

Endpoints are mounted under both ``/v1`` (canonical, versioned) and the root
path (back-compat). Health is split into liveness vs readiness so orchestrators
(Kubernetes / App Platform) can route traffic correctly.
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    File,
    Header,
    HTTPException,
    Query,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import __version__
from .engine import BatchEngine, IdempotencyConflictError, OverloadedError
from .logging_config import get_logger, request_id_var, setup_logging
from .metrics import metrics
from .models import (
    BatchRequest,
    DeadLetterResponse,
    JobsListResponse,
    JobStatusResponse,
    JobResultsResponse,
    JobSummary,
    Priority,
    PromptItem,
    SubmitResponse,
)


async def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    """Optional API-key gate. No-op unless API_KEY is configured.

    Reads the environment live so the key can be rotated without a code change.
    """
    expected = os.environ.get("API_KEY", "")
    if expected and x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")

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
        "Concurrent, rate-limit-aware batch inference platform: fair global "
        "scheduling, adaptive concurrency, retries/backoff, idempotent submits, "
        "cost accounting, and full observability."
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """Attach an X-Request-ID to every request for log correlation."""
    rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    token = request_id_var.set(rid)
    try:
        response = await call_next(request)
    finally:
        request_id_var.reset(token)
    response.headers["X-Request-ID"] = rid
    return response


def _engine(request: Request) -> BatchEngine:
    return request.app.state.engine


def _ack(job, request: Request, reused: bool) -> SubmitResponse:
    # Respect the scheme set by the upstream proxy (App Platform, nginx, etc.)
    # so returned URLs use https:// rather than the internal http://.
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    base = str(request.base_url).rstrip("/").replace(request.url.scheme, proto, 1)
    return SubmitResponse(
        job_id=job.id,
        state=job.state,
        total=job.total,
        priority=job.priority,
        idempotent_reuse=reused,
        message=(
            "Existing job returned (idempotent)."
            if reused
            else "Batch accepted. Processing in the background."
        ),
        status_url=f"{base}/v1/jobs/{job.id}",
        results_url=f"{base}/v1/jobs/{job.id}/results",
    )


def _overloaded(exc: OverloadedError) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=str(exc),
        headers={"Retry-After": str(exc.retry_after)},
    )


def _status(job, engine) -> JobStatusResponse:
    return JobStatusResponse(**job.snapshot(engine.job_pending(job.id)))


# --------------------------------------------------------------------------- #
# Health & metrics (root only)
# --------------------------------------------------------------------------- #
@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@app.get("/livez")
async def livez() -> dict[str, str]:
    """Liveness: the process is up. Always 200 unless the process is dead."""
    return {"status": "alive"}


@app.get("/readyz")
async def readyz(request: Request):
    """Readiness: can we accept new work right now?"""
    engine = _engine(request)
    saturated = engine.active_jobs >= engine.settings.max_active_jobs
    ready = engine.accepting and not saturated
    body = {
        "status": "ready" if ready else "not_ready",
        "accepting": engine.accepting,
        "active_jobs": engine.active_jobs,
        "max_active_jobs": engine.settings.max_active_jobs,
        "saturated": saturated,
    }
    code = status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(status_code=code, content=body)


@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics() -> str:
    return metrics.render()


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "batch-inference-engine", "version": __version__, "docs": "/docs"}


# --------------------------------------------------------------------------- #
# Versioned API router
# --------------------------------------------------------------------------- #
api = APIRouter(tags=["batches"], dependencies=[Depends(require_api_key)])


@api.get("/jobs", response_model=JobsListResponse)
async def list_jobs(
    request: Request,
    state: str | None = Query(default=None, description="Filter by job state."),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> JobsListResponse:
    """List jobs (newest first), optionally filtered by state."""
    jobs = sorted(_engine(request).store.all(), key=lambda j: j.created_at, reverse=True)
    if state:
        jobs = [j for j in jobs if j.state.value == state]
    page = jobs[offset : offset + limit]
    return JobsListResponse(
        total=len(jobs),
        returned=len(page),
        limit=limit,
        offset=offset,
        jobs=[
            JobSummary(
                job_id=j.id,
                state=j.state,
                priority=j.priority,
                total=j.total,
                completed=j.completed,
                succeeded=j.succeeded,
                failed=j.failed,
                created_at=j.created_at,
            )
            for j in page
        ],
    )


@api.post("/system/self-test")
async def system_self_test(
    request: Request,
    n: int = Query(default=50, ge=5, le=200, description="Number of synthetic prompts"),
) -> JSONResponse:
    """Run a built-in invariant smoke test against the live platform.

    Submits *n* synthetic prompts (priority=low), verifies concurrency bounds,
    retry-recovery, idempotency, fair scheduling, and result aggregation, then
    returns a structured pass/fail report. HTTP 200 = all invariants green.
    HTTP 500 = regression detected.
    """
    from .selftest import run_self_test

    result = await run_self_test(_engine(request), n=n)
    code = 200 if result.get("status") == "passed" else 500
    return JSONResponse(content=result, status_code=code)


@api.get("/system/capacity")
async def system_capacity(request: Request) -> dict:
    """Live capacity snapshot for dashboards / autoscaling decisions."""
    engine = _engine(request)
    s = engine.settings
    return {
        "accepting": engine.accepting,
        "active_jobs": engine.active_jobs,
        "max_active_jobs": s.max_active_jobs,
        "queue_depth": engine.scheduler.pending,
        "max_queue_size": s.max_queue_size,
        "inflight": int(metrics.inflight.value),
        "concurrency_limit": engine._limiter.limit,
        "global_max_concurrency": s.global_max_concurrency,
        "saturated": engine.active_jobs >= s.max_active_jobs,
    }


@api.post("/batches", response_model=SubmitResponse, status_code=status.HTTP_202_ACCEPTED)
async def submit_batch(
    payload: BatchRequest,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> SubmitResponse:
    """Submit a JSON array of prompts and start background processing."""
    try:
        job, reused = await _engine(request).submit_with_idempotency(
            payload.prompts, priority=payload.priority, idempotency_key=idempotency_key
        )
    except OverloadedError as exc:
        raise _overloaded(exc) from exc
    except IdempotencyConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _ack(job, request, reused)


@api.post("/batches/upload", response_model=SubmitResponse, status_code=status.HTTP_202_ACCEPTED)
async def upload_batch(
    request: Request,
    file: UploadFile = File(...),
    priority: Priority = Query(default=Priority.NORMAL),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> SubmitResponse:
    """Submit a batch via uploaded JSON file (bare array or {prompts:[...]})."""
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
        job, reused = await _engine(request).submit_with_idempotency(
            prompts, priority=priority, idempotency_key=idempotency_key
        )
    except OverloadedError as exc:
        raise _overloaded(exc) from exc
    except IdempotencyConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _ack(job, request, reused)


@api.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def job_status(job_id: str, request: Request) -> JobStatusResponse:
    engine = _engine(request)
    job = engine.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    return _status(job, engine)


@api.get("/jobs/{job_id}/results", response_model=JobResultsResponse)
async def job_results(
    job_id: str,
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> JobResultsResponse:
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


@api.get("/jobs/{job_id}/dead-letter", response_model=DeadLetterResponse)
async def job_dead_letter(
    job_id: str,
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> DeadLetterResponse:
    """Inspect prompts that failed after exhausting retries."""
    job = _engine(request).get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    failed = job.dead_letter()
    page = failed[offset : offset + limit]
    return DeadLetterResponse(
        job_id=job.id, failed=len(failed), returned=len(page), items=page
    )


@api.post("/jobs/{job_id}/replay-failed", response_model=SubmitResponse, status_code=202)
async def replay_failed(job_id: str, request: Request) -> SubmitResponse:
    """Create a new job from only the failed prompts of an existing job."""
    engine = _engine(request)
    if engine.get_job(job_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    try:
        new_job = await engine.replay_failed(job_id)
    except OverloadedError as exc:
        raise _overloaded(exc) from exc
    if new_job is None:
        raise HTTPException(status_code=409, detail="No failed prompts to replay.")
    return _ack(new_job, request, False)


@api.post("/jobs/{job_id}/cancel", response_model=JobStatusResponse)
async def cancel_job(job_id: str, request: Request) -> JobStatusResponse:
    engine = _engine(request)
    outcome = await engine.cancel(job_id)
    if outcome is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")
    if outcome is False:
        raise HTTPException(status_code=409, detail="Job has already finished.")
    return _status(engine.get_job(job_id), engine)


@api.get("/jobs/{job_id}/events")
async def job_events(job_id: str, request: Request) -> StreamingResponse:
    """Server-Sent Events stream of a job's lifecycle (progress + terminal)."""
    engine = _engine(request)
    if engine.get_job(job_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id: {job_id}")

    async def stream():
        def sse(event: str, data: dict) -> str:
            return f"event: {event}\ndata: {json.dumps(data)}\n\n"

        yield sse("job_started", engine.get_job(job_id).snapshot(engine.job_pending(job_id)))
        last = -1
        terminal = {"completed", "failed", "cancelled"}
        while True:
            if await request.is_disconnected():
                return
            job = engine.get_job(job_id)
            snap = job.snapshot(engine.job_pending(job_id))
            if job.completed != last:
                yield sse("progress", snap)
                last = job.completed
            if job.state.value in terminal:
                yield sse(f"job_{job.state.value}", snap)
                return
            await asyncio.sleep(0.2)

    return StreamingResponse(stream(), media_type="text/event-stream")


# Mount under /v1 (canonical) and at root (back-compat).
app.include_router(api, prefix="/v1")
app.include_router(api)

# Operator dashboard – served from app/static/ui.html.
_STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/ui", include_in_schema=False)
async def ui_dashboard() -> FileResponse:
    return FileResponse(_STATIC_DIR / "ui.html")


@app.exception_handler(OverloadedError)
async def _overloaded_handler(request: Request, exc: OverloadedError) -> JSONResponse:
    return JSONResponse(
        status_code=503, content={"detail": str(exc)}, headers={"Retry-After": str(exc.retry_after)}
    )
