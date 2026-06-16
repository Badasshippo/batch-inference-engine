# Deploying to DigitalOcean

Two deployment paths, from simplest to most operationally complete. Both build
the included [`Dockerfile`](../Dockerfile), which runs as a non-root user, honors
`$PORT`, and ships a container `HEALTHCHECK`.

---

## Path A — App Platform (fastest)

[App Platform](https://docs.digitalocean.com/products/app-platform/) can deploy
straight from this GitHub repo (it auto-detects the Dockerfile).

```bash
# Using doctl + the app spec below.
doctl apps create --spec .do/app.yaml
```

Example app spec (`.do/app.yaml`):

A ready-to-use spec lives at [`.do/app.yaml`](../.do/app.yaml):

```yaml
name: batch-inference-engine
services:
  - name: api
    github:
      repo: Badasshippo/batch-inference-engine
      branch: master
      deploy_on_push: true
    dockerfile_path: Dockerfile
    http_port: 8080
    instance_count: 1
    instance_size_slug: basic-xxs
    health_check:
      http_path: /readyz   # readiness: 503 while draining or saturated
    envs:
      - key: WORKER_POOL_SIZE
        value: "32"
      - key: GLOBAL_MAX_CONCURRENCY
        value: "64"
      - key: MAX_ACTIVE_JOBS
        value: "50"
      - key: LOG_LEVEL
        value: "INFO"
```

What you get out of the box:

- **Health checks** against `/healthz` (rolling deploys wait for healthy).
- **Logs & metrics / insights** in the App Platform dashboard; the app emits
  structured JSON logs and exposes Prometheus metrics at `/metrics`.
- **Log forwarding** to an external sink (Datadog/Logtail/Papertrail) if desired.
- **Scaling** by bumping `instance_size_slug` / `instance_count`, and tuning
  worker concurrency via the `WORKER_POOL_SIZE` / `GLOBAL_MAX_CONCURRENCY` env vars.

> Note: App Platform runs one container per instance and the job store is
> in-memory, so a job submitted to one instance is only visible there. For
> multi-instance horizontal scaling, move the job store to a shared backend
> (see "Production next steps" in the README).

---

## Path B — DOKS (DigitalOcean Kubernetes)

For full control: build, push to DO Container Registry (DOCR), and deploy to a
[DOKS](https://docs.digitalocean.com/products/kubernetes/) cluster (managed
control plane, HA, autoscaling, DO load balancer integration).

```bash
# 1. Build and push to DOCR
doctl registry login
docker build -t registry.digitalocean.com/<registry>/batch-inference-engine:latest .
docker push registry.digitalocean.com/<registry>/batch-inference-engine:latest

# 2. Apply manifests
kubectl apply -f k8s/
```

Example `k8s/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: batch-inference-engine
spec:
  replicas: 2
  selector:
    matchLabels: { app: batch-inference-engine }
  template:
    metadata:
      labels: { app: batch-inference-engine }
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: api
          image: registry.digitalocean.com/<registry>/batch-inference-engine:latest
          ports: [{ containerPort: 8080 }]
          env:
            - { name: WORKER_POOL_SIZE, value: "32" }
            - { name: GLOBAL_MAX_CONCURRENCY, value: "64" }
            - { name: GRACEFUL_SHUTDOWN_SECONDS, value: "20" }
          readinessProbe:
            httpGet: { path: /readyz, port: 8080 }
            initialDelaySeconds: 3
            periodSeconds: 10
          livenessProbe:
            httpGet: { path: /livez, port: 8080 }
            initialDelaySeconds: 5
            periodSeconds: 15
          resources:
            requests: { cpu: "100m", memory: "128Mi" }
            limits:   { cpu: "500m", memory: "256Mi" }
          # Give in-flight prompts time to finish on rolling deploys.
          lifecycle:
            preStop:
              exec: { command: ["sleep", "5"] }
      terminationGracePeriodSeconds: 30
---
apiVersion: v1
kind: Service
metadata:
  name: batch-inference-engine
spec:
  selector: { app: batch-inference-engine }
  ports:
    - { port: 80, targetPort: 8080 }
  type: ClusterIP
```

Operational notes:

- **Readiness/liveness probes** use `/healthz`. The app's graceful shutdown
  (`GRACEFUL_SHUTDOWN_SECONDS`) lets in-flight prompts drain before the pod dies,
  pairing with `terminationGracePeriodSeconds` and the `preStop` hook for
  zero-drop rolling deploys.
- **Autoscaling**: add an `HorizontalPodAutoscaler` on CPU (or a custom metric
  like `inference_latency_seconds`) once a metrics pipeline is in place.
- **Observability**: the pod annotations let a Prometheus operator scrape
  `/metrics`; JSON logs flow to the cluster's logging stack.
- **State**: as with App Platform, the in-memory job store is per-pod. For
  multi-pod correctness, back the job store with DO Managed Redis/Postgres.

---

## Monitoring summary

| Signal | Where |
|---|---|
| Liveness/readiness | `GET /healthz` |
| Metrics (Prometheus) | `GET /metrics` — counters (`batch_prompts_completed_total`, `inference_rate_limited_total`, …) + histograms (`inference_latency_seconds`, `job_duration_seconds`) |
| Logs | structured JSON on stdout (`job_id`, `prompt_id`, `attempt`, `status`, `latency_ms`) |
| Backpressure | `429`/`503 + Retry-After` when overloaded |
