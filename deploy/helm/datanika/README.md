# Datanika Helm Chart

Minimal Helm chart that installs Datanika (multi-tenant data pipeline platform)
on any Kubernetes cluster. Functionally equivalent to the production
docker-compose deployment on Hetzner, minus the bundled monitoring stack.

> **Status: v0.1.0 stub.** Closes the enterprise "Helm chart" checkbox. Good
> enough for `helm install` onto a cluster that provides `ReadWriteMany`
> storage; not yet hardened for HA or large multi-tenant fleets. See the
> [Known caveats](#known-caveats) section before using it in production.

## What it installs

| Component | Kind | Notes |
|-----------|------|-------|
| `app` | Deployment (1 replica) | Reflex frontend (`:3000`) + backend (`:8000`). Runs `alembic upgrade head` on startup. |
| `celery` | Deployment (1 replica) | Background worker. Shares the `dbt_projects` **and `uploaded_files`** PVCs with `app`. |
| `postgres` | StatefulSet (1 replica) | Postgres 16-alpine with `pg_stat_statements`. Disable via `postgres.enabled=false` to bring your own. |
| `redis` | Deployment (1 replica) | Redis 7-alpine. Broker + cache. Disable via `redis.enabled=false` to bring your own. |
| `ingress` | Ingress (optional) | `/` → app frontend, `/api` → app backend. Mirrors the production Nginx reverse proxy. |
| `dbt-projects` PVC | PVC (RWX) | Shared between `app` and `celery` pods. **Requires a ReadWriteMany storage class.** |
| `uploaded-files` PVC | PVC (RWX) | Uploaded-file archives. `app` writes them, `celery` reads them back when the run executes — so this is **not optional**: without it a CSV upload succeeds and its run fails immediately with 0 rows (core#529). Also RWX. |
| Secret | `Opaque` | `DATABASE_URL`, `REDIS_URL`, `SECRET_KEY`, and all `POSTGRES_*`/`REDIS_*` values, rendered from `.Values.env` + `.Values.secrets`. |

Monitoring (Prometheus, Grafana, node-exporter, cAdvisor) is **not** in scope
for this chart — run your cluster's existing observability stack and point it
at the app's metrics endpoints.

## Quickstart

```bash
# From a clone of the datanika-core repo:
helm install my-datanika ./deploy/helm/datanika \
  --namespace datanika --create-namespace \
  --set secrets.POSTGRES_PASSWORD=$(openssl rand -base64 24) \
  --set secrets.REDIS_PASSWORD=$(openssl rand -base64 24) \
  --set secrets.SECRET_KEY=$(openssl rand -base64 32)

# Watch the rollout
kubectl -n datanika get pods -w

# Port-forward the UI (or enable ingress)
kubectl -n datanika port-forward svc/my-datanika-app 3000:3000
# open http://localhost:3000
```

To use managed databases instead of the bundled Postgres/Redis:

```bash
helm install my-datanika ./deploy/helm/datanika \
  --namespace datanika --create-namespace \
  --set postgres.enabled=false \
  --set externalDatabase.host=<managed-postgres-host> \
  --set env.POSTGRES_USER=<user> \
  --set env.POSTGRES_DB=<db> \
  --set secrets.POSTGRES_PASSWORD=<password> \
  --set redis.enabled=false \
  --set externalRedis.host=<managed-redis-host> \
  --set secrets.REDIS_PASSWORD=<password> \
  --set secrets.SECRET_KEY=$(openssl rand -base64 32)
```

## Configuration highlights

See `values.yaml` for the full list. Common overrides:

| Key | Default | Purpose |
|-----|---------|---------|
| `image.repository` | `ghcr.io/datanika-io/datanika` | Override for a private registry |
| `image.tag` | `latest` | Pin to a release tag |
| `env.DATANIKA_EDITION` | `oss` | Set to `cloud` to activate the Paddle billing plugin |
| `env.extra` | `{}` | Map of extra env keys injected into the Secret (anything else the app reads from `.env.docker`) |
| `postgres.enabled` | `true` | Set `false` + fill `externalDatabase.host` for managed Postgres |
| `redis.enabled` | `true` | Set `false` + fill `externalRedis.host` for managed Redis |
| `ingress.enabled` | `false` | Turn on once you have a TLS-terminating ingress class |
| `app.replicaCount` | `1` | Keep at `1` until the migration job is extracted (see caveats) |
| `app.dbtProjectsSize` | `5Gi` | Size of the shared dbt projects PVC |
| `app.uploadedFilesSize` | `5Gi` | Size of the shared uploaded-files PVC |
| `env.FILE_UPLOADS_DIR` | `/app/uploaded_files` | Must match the mount path and be identical for both tiers — the app default is relative to the working directory, so a shared volume alone is not enough |

## Known caveats

- **RWX storage required.** The `app` and `celery` Deployments both mount
  the `dbt_projects` and `uploaded_files` PVCs, so the chart requests
  `ReadWriteMany` for both. On managed
  Kubernetes that usually means NFS, EFS, Azure Files, or Filestore. On
  bare-metal clusters, Longhorn and Rook/CephFS both work. If your cluster
  only has `ReadWriteOnce`, you can pin both Deployments to the same node
  via `nodeSelector` + `affinity` as a workaround.
- **Migrations run on every app pod startup.** The `app` container runs
  `alembic upgrade head` before starting Reflex. With `app.replicaCount=1`
  this is fine. For HA, move migrations into a
  `helm.sh/hook: pre-install,pre-upgrade` Job (tracked separately — see the
  infrastructure plan).
- **Bundled Postgres + Redis are single-replica, single-PVC.** They're fine
  for evaluation and small installs. For anything real, disable them and
  point to managed databases via `externalDatabase` / `externalRedis`.
- **No monitoring stack.** Unlike the Hetzner docker-compose, this chart
  does not ship Grafana, Prometheus, cAdvisor or node-exporter. Use your
  cluster's existing observability tooling.
- **`secrets.*` default to `"CHANGE-ME"`.** The chart will install with
  these placeholders, which is useful for `helm lint` / CI but unsafe for
  any real environment. Always override them at install time.

## Compared to the production Hetzner deploy

| Feature | Hetzner docker-compose | This chart |
|---------|------------------------|------------|
| App + Celery + Postgres + Redis | yes | yes |
| Persistent volumes | Docker volumes | PVCs (Postgres, `dbt_projects`, `uploaded_files`) |
| Nginx reverse proxy + TLS | host-level Nginx | ingress resource (optional) |
| Grafana + Prometheus + cAdvisor | yes | no (run separately) |
| Auto-deploy on push to master | yes | no (managed by user) |
| HA / rolling updates | `replicas=1` | `replicas=1` (see caveats) |
| Paddle billing plugin | yes (`DATANIKA_EDITION=cloud`) | yes (`--set env.DATANIKA_EDITION=cloud`) |

## Development

Lint the chart locally before committing:

```bash
helm lint deploy/helm/datanika
helm template test deploy/helm/datanika | kubectl apply --dry-run=client -f -
```

`helm lint` is also run on every PR and push to `dev` / `master` via
`.github/workflows/ci.yml`.
