"""End-to-end API tests using httpx ASGI transport."""
from __future__ import annotations

import asyncio

import httpx
import pytest

from app.main import app


@pytest.fixture
async def client():
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


async def _poll_until_complete(client, status_url: str, timeout: float = 15.0) -> dict:
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        resp = await client.get(status_url)
        body = resp.json()
        if body["state"] in ("completed", "failed"):
            return body
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(f"job stuck in {body['state']}")
        await asyncio.sleep(0.02)


async def test_healthz(client):
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


async def test_metrics_endpoint_exposes_prometheus(client):
    resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    body = resp.text
    assert "# TYPE batch_jobs_submitted_total counter" in body
    assert "inference_latency_seconds_bucket" in body


async def test_results_pagination(client):
    prompts = [{"prompt": f"p{i}"} for i in range(25)]
    ack = (await client.post("/batches", json={"prompts": prompts})).json()
    status_path = "/jobs/" + ack["job_id"]
    await _poll_until_complete(client, status_path)

    page = (await client.get(status_path + "/results?limit=10&offset=0")).json()
    assert page["limit"] == 10
    assert page["returned"] == 10
    assert len(page["results"]) == 10

    last = (await client.get(status_path + "/results?limit=10&offset=20")).json()
    assert last["returned"] == 5


async def test_submit_and_track_batch(client):
    prompts = [{"id": f"p{i}", "prompt": f"hello {i}"} for i in range(40)]
    resp = await client.post("/batches", json={"prompts": prompts})
    assert resp.status_code == 202
    ack = resp.json()
    assert ack["total"] == 40
    assert ack["state"] in ("pending", "running")

    status_path = "/jobs/" + ack["job_id"]
    final = await _poll_until_complete(client, status_path)
    assert final["state"] == "completed"
    assert final["completed"] == 40
    assert final["progress"] == "40/40"
    assert final["percent"] == 100.0

    results = await client.get(status_path + "/results")
    body = results.json()
    assert body["succeeded"] == 40
    assert len(body["results"]) == 40


async def test_upload_bare_array(client):
    import io
    import json

    data = json.dumps(["alpha", "beta", "gamma"]).encode()
    files = {"file": ("prompts.json", io.BytesIO(data), "application/json")}
    resp = await client.post("/batches/upload", files=files)
    assert resp.status_code == 202
    assert resp.json()["total"] == 3


async def test_unknown_job_returns_404(client):
    resp = await client.get("/jobs/does-not-exist")
    assert resp.status_code == 404


async def test_empty_batch_is_rejected(client):
    resp = await client.post("/batches", json={"prompts": []})
    assert resp.status_code == 422  # pydantic validation error
