# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base

# Don't write .pyc files; flush stdout/stderr for live container logs.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8080

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy application code.
COPY app ./app

# Run as a non-root user (security best practice / required by many platforms).
RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8080

# Container-native health check hitting the liveness endpoint.
# urlopen raises on a non-200 response, so a clean exit means healthy.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('PORT','8080')+'/healthz', timeout=2)" || exit 1

# Honor the platform-provided $PORT (App Platform, Cloud Run, etc.).
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
