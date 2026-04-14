# Smoke Suite — Staging + Landing

Owner: QA. Tracks [core#107](https://github.com/datanika-io/datanika-core/issues/107) → Phase 2 and [core#133](https://github.com/datanika-io/datanika-core/issues/133).

Post-deploy probes that a handful of public-facing endpoints return HTTP 200 with the expected shape. Fails the CI workflow (and pages via Telegram) if any probe is red.

## Scope

- **Core** — ran against `https://staging-app.datanika.io/` in CI, against any `DATANIKA_SMOKE_CORE_URL` target locally. Exercises the SPA root, `/login`, `/llms.txt`, `/api/v1/openapi.json`, `/api/v1/agent-guide.md`, `/api/v1/meta/agent-tiers`.
- **Landing** — ran against `https://datanika.io/` (prod — there is no landing staging). Exercises `/`, `/pricing/`, `/docs/`, `/sitemap-index.xml`.

Intentionally out of scope for v1:

- `/healthz` and `/readyz`. Reflex catches all routes and serves the SPA fallback with HTTP 200, so these are false-positive green. Would need real Starlette routes added by Engineering before they're smoke-useful. Tracked in core#107 comments.
- Authed API endpoints (`/api/v1/meta/connection-types`, `/api/v1/meta/dlt-config-schema`, etc.). They 401 without an API key. Need a fixture token baked into staging bootstrap or a smoke-scoped key. Deferred.
- Latency SLO hard-fails. v1 records elapsed time per probe and prints it in pytest output; no hard threshold. Tighten once we have two weeks of baseline data.
- Smoke against prod. Phase 1 stays planned but un-shipped until Phase 2 has been green for ~two weeks.

## Running locally

```bash
# Against staging (default)
uv run pytest scripts/smoke/ -v

# Against a different core target
DATANIKA_SMOKE_CORE_URL=https://app.datanika.io/ \
  uv run pytest scripts/smoke/test_core_smoke.py -v

# Skip the landing suite entirely
uv run pytest scripts/smoke/test_core_smoke.py -v
```

The suite is **not** picked up by `uv run pytest` with no args because `pyproject.toml` scopes `testpaths = ["tests"]` — you have to pass `scripts/smoke/` explicitly. That's intentional: smoke shouldn't run on every PR, only after deploy.

## CI

The `smoke-staging` job in `.github/workflows/ci.yml` runs after `deploy-staging` succeeds (`needs: [deploy-staging]`), only on `push` to `dev`. If any probe fails, the job fails and the pre-existing Grafana Telegram alert channel (`@DatanikaBot`, chat `1201995`) pages via the same pattern landing#145 set up for landing deploys.

The smoke job runs entirely from the GHA runner — it doesn't SSH into Hetzner. Just HTTP from GitHub's network to Cloudflare → Aweb/Hetzner. This is deliberate: we want smoke to exercise the same network path real users hit.

## Adding new probes

1. Add the target endpoint to `test_core_smoke.py` or `test_landing_smoke.py` as a new `test_<descriptive_name>` function.
2. Hit the endpoint via the `core_client` / `landing_client` fixture.
3. Assert `response.status_code == 200` and a **specific shape claim** (not just "is not empty"). Shape claims are what catch deploys that return 200-with-wrong-payload.
4. Latency: fixture records `response.elapsed`; print it via pytest's `-v` capture. No hard threshold in v1.

## Known issues surfaced by building this

- **landing sitemap path is `/sitemap-index.xml`, not `/sitemap.xml`.** PLAN_QA.md §P0 #2 listed the wrong path. Astro's sitemap integration publishes an index at `/sitemap-index.xml` with referenced children (`/sitemap-0.xml`, etc.). `/sitemap.xml` returns 404.
- **core `/healthz` and `/readyz` silently 200.** Reflex handles any unknown path with the SPA fallback. Any dashboard or external monitor pointing at `/healthz` is effectively no-op'd. Worth a separate ticket to Engineering for real health routes.
- **All `/api/v1/meta/*` endpoints except `/agent-tiers` require auth.** `/llms.txt`, `/agent-guide.md`, `/openapi.json`, `/agent-tiers` are public; the rest 401 without a token. Documentation in `agent-tiers` claims Tier 5 (Machine-Readable Discovery) is auth-less — that's true for the 4 listed endpoints, but `/meta/connection-types`, `/meta/dlt-config-schema`, `/meta/dbt-tests`, `/meta/materializations` are NOT Tier 5 per the JSON and do require auth. Minor doc ambiguity, not a bug.
