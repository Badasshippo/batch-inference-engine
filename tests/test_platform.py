"""Tests for platform features: idempotency, dead-letter, replay, cost,
provider abstraction, adaptive throttling, health model, priority, and SSE."""
from __future__ import annotations

import asyncio

import httpx
import pytest

from app.config import Settings
from app.engine import BatchEngine
from app.main import app
from app.mock_inference import InferenceError
from app.models import JobState, Priority, PromptItem
from app.providers import FlakyProvider


async def _wait_for(job, timeout: float = 10.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while job.state in (JobState.PENDING, JobState.RUNNING):
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(f"job did not finish; state={job.state}")
        await asyncio.sleep(0.01)


def _settings(**kw) -> Settings:
    base = dict(worker_pool_size=8, mock_rate_limit_every=0, max_retries=3,
                backoff_base_seconds=0.001, backoff_max_seconds=0.01, backoff_jitter=0.0)
    base.update(kw)
    return Settings(**base)


# ------------------------------- engine level ------------------------------ #
async def test_idempotency_returns_same_job():
    async def infer(p: str) -> str:
        await asyncio.sleep(0.005)
        return f"ok::{p}"

    engine = BatchEngine(infer=infer, settings=_settings())
    prompts = [PromptItem(prompt=f"x{i}") for i in range(5)]

    job1, reused1 = await engine.submit_with_idempotency(prompts, idempotency_key="abc")
    job2, reused2 = await engine.submit_with_idempotency(prompts, idempotency_key="abc")

    assert reused1 is False
    assert reused2 is True
    assert job1.id == job2.id


async def test_dead_letter_and_replay():
    async def infer(p: str) -> str:
        if p.endswith("-bad"):
            raise InferenceError("permanent failure")
        return f"ok::{p}"

    engine = BatchEngine(infer=infer, settings=_settings())
    prompts = [
        PromptItem(id="g1", prompt="good"),
        PromptItem(id="b1", prompt="x-bad"),
        PromptItem(id="b2", prompt="y-bad"),
    ]
    job = await engine.submit(prompts)
    await _wait_for(job)

    dl = job.dead_letter()
    assert job.failed == 2
    assert {r.id for r in dl} == {"b1", "b2"}
    assert all(r.error for r in dl)

    # Replay should create a new job with just the 2 failed prompts.
    replay = await engine.replay_failed(job.id)
    assert replay is not None
    assert replay.total == 2
    await _wait_for(replay)
    # They still fail (deterministic), so the new job also has 2 dead letters.
    assert replay.failed == 2


async def test_replay_with_no_failures_returns_none():
    async def infer(p: str) -> str:
        return f"ok::{p}"

    engine = BatchEngine(infer=infer, settings=_settings())
    job = await engine.submit([PromptItem(prompt="x")])
    await _wait_for(job)
    assert await engine.replay_failed(job.id) is None


async def test_cost_and_token_accounting():
    async def infer(p: str) -> str:
        return "a much longer completion than the prompt itself here"

    engine = BatchEngine(infer=infer, settings=_settings())
    job = await engine.submit([PromptItem(prompt="short prompt") for _ in range(4)])
    await _wait_for(job)

    assert job.input_tokens > 0
    assert job.output_tokens > 0
    assert job.cost_usd > 0
    summary = job.cost_summary()
    assert summary["total_tokens"] == job.input_tokens + job.output_tokens


async def test_provider_abstraction_with_flaky_provider():
    engine = BatchEngine(provider=FlakyProvider(fail_times=2), settings=_settings(max_retries=5))
    job = await engine.submit([PromptItem(prompt="hello")])
    await _wait_for(job)
    assert job.succeeded == 1
    assert job.retries >= 2  # the two 429s were retried


async def test_adaptive_limiter_shrinks_under_throttle():
    # Heavy 429 rate should drive the AIMD limiter below its initial value.
    settings = _settings(
        mock_rate_limit_every=2,
        global_max_concurrency=16,
        adaptive_min_concurrency=2,
        adaptive_decrease_factor=0.5,
        adaptive_increase_after=1000,  # effectively no recovery during the test
        max_retries=10,
    )
    engine = BatchEngine(settings=settings)
    job = await engine.submit([PromptItem(prompt=f"x{i}") for i in range(40)])
    await _wait_for(job, timeout=15.0)
    assert job.state == JobState.COMPLETED
    assert engine._limiter.limit < settings.global_max_concurrency


# -------------------------------- API level -------------------------------- #
@pytest.fixture
async def client():
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


async def _poll(client, path: str, timeout: float = 15.0) -> dict:
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        body = (await client.get(path)).json()
        if body["state"] in ("completed", "failed", "cancelled"):
            return body
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(f"stuck in {body['state']}")
        await asyncio.sleep(0.02)


async def test_health_model(client):
    assert (await client.get("/healthz")).status_code == 200
    assert (await client.get("/livez")).json()["status"] == "alive"
    r = await client.get("/readyz")
    assert r.status_code == 200
    assert r.json()["status"] == "ready"


async def test_v1_prefix_and_priority_and_request_id(client):
    resp = await client.post(
        "/v1/batches",
        json={"prompts": [{"prompt": "hi"}], "priority": "high"},
        headers={"X-Request-ID": "req-123"},
    )
    assert resp.status_code == 202
    assert resp.headers["X-Request-ID"] == "req-123"
    ack = resp.json()
    assert ack["priority"] == "high"
    assert "/v1/jobs/" in ack["status_url"]

    final = await _poll(client, "/v1/jobs/" + ack["job_id"])
    assert final["priority"] == "high"
    assert "cost" in final and "pending" in final


async def test_idempotency_key_header(client):
    payload = {"prompts": [{"prompt": "a"}, {"prompt": "b"}]}
    h = {"Idempotency-Key": "dup-key-1"}
    a = (await client.post("/v1/batches", json=payload, headers=h)).json()
    b = (await client.post("/v1/batches", json=payload, headers=h)).json()
    assert a["job_id"] == b["job_id"]
    assert b["idempotent_reuse"] is True


async def test_sse_events_stream(client):
    ack = (await client.post("/v1/batches", json={"prompts": [{"prompt": "x"}]})).json()
    async with client.stream("GET", "/v1/jobs/" + ack["job_id"] + "/events") as resp:
        assert resp.status_code == 200
        events = []
        async for line in resp.aiter_lines():
            if line.startswith("event:"):
                events.append(line.split(":", 1)[1].strip())
            if "job_completed" in events:
                break
    assert "job_started" in events
    assert "job_completed" in events


async def test_dead_letter_endpoint(client):
    # Force failures via the upload path is hard; use the JSON path with a
    # prompt the default mock always succeeds on, so expect empty dead-letter.
    ack = (await client.post("/v1/batches", json={"prompts": [{"prompt": "ok"}]})).json()
    await _poll(client, "/v1/jobs/" + ack["job_id"])
    dl = (await client.get("/v1/jobs/" + ack["job_id"] + "/dead-letter")).json()
    assert dl["failed"] == 0
    assert dl["items"] == []
