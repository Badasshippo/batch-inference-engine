"""Structured JSON logging.

Emitting one JSON object per line is what log aggregators (DigitalOcean log
forwarding, Loki, Datadog, CloudWatch, etc.) expect. Any extra fields passed via
`logger.info(msg, extra={...})` are merged into the JSON object, so we can attach
`job_id`, `prompt_id`, `attempt`, `status`, and `latency_ms` to each event for
easy filtering and dashboards.
"""
from __future__ import annotations

import contextvars
import json
import logging
import os
import sys
import time

# Carries the current request id across await points so every log line emitted
# while handling a request can be correlated (set by the X-Request-ID middleware).
request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "request_id", default=None
)

# Standard LogRecord attributes we don't want to duplicate in the JSON payload.
_RESERVED = set(
    vars(logging.makeLogRecord({})).keys()
) | {"message", "asctime", "taskName"}


class _RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        rid = request_id_var.get()
        if rid is not None:
            record.request_id = rid
        return True


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str | None = None) -> None:
    """Install the JSON formatter on the root logger (idempotent)."""
    level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    handler.addFilter(_RequestIdFilter())

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


def get_logger(name: str = "batch_engine") -> logging.Logger:
    return logging.getLogger(name)
