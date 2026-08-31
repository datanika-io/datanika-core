# Datanika E2E Test Harness (Playwright)

Owner: QA. See `plans/qa/PLAN_QA.md` → P0 — E2E Test Framework.

This directory holds browser-level golden-path tests. Unit and integration tests live in `tests/` and are Engineering's responsibility. E2E tests here drive the real app in a real browser against a real Docker Compose stack.

## Scope

E2E tests cover flows that span the full stack:
- UI → Starlette routes → services → Celery → Postgres → back to UI
- Auth flows that cross WebSocket + HTTP boundaries (Reflex quirk)
- Tenant isolation at the HTTP edge, not just the service layer
- Billing quota enforcement via real hook dispatch (with Paddle mocked at the `PaddleClient` boundary)

E2E tests do NOT cover:
- Pure service-level behavior (use pytest in `tests/test_services/`)
- dbt compilation correctness (use pytest in `tests/test_services/test_transformation_compile.py`)
- Anything reachable by a unit test — if a unit test would catch it, write the unit test instead

## Prerequisites

- Node 22 (`D:/Tools/node-v22.14.0-win-x64`) on PATH
- A target Datanika stack (one of):
  - Local Docker Compose (`cd datanika && docker compose up -d`) — default
  - pointer.gr staging (`https://staging-app.datanika.io/`) — see "Running against staging"
- `uv` on PATH (for the default seed command)

## Running (local stack — default)

```bash
# From repo root
cd e2e
npm install
npx playwright install chromium
npm test                 # headless
npm run test:headed      # watch a run in a real browser window
npm run test:ui          # Playwright UI mode (best for writing new tests)
```

`global-setup.ts` shells `uv run python -m datanika.scripts.e2e_seed` against the local stack, captures the 9-field JSON fixture, and exposes it to `fixtures/auth.ts` via `DATANIKA_E2E_*` env vars.

## Running against staging

Staging is a usable Playwright target when the local Compose stack is down or you want to rehearse the harness against a stable, prod-shaped deploy. Staging resets its Postgres on every container boot (see `CLAUDE.md` pointer.gr section), so `e2e_seed.py` has to run **inside** the staging container — direct DB access from your laptop isn't possible (staging Postgres is on the pointer.gr docker network, not exposed publicly).

The harness supports this via `DATANIKA_E2E_SEED_CMD`. Whatever value you set is shelled verbatim and its stdout is parsed as the standard seed JSON. Point it at an SSH wrapper that runs the seed inside the container:

```bash
cd e2e
DATANIKA_E2E_BASE_URL=https://staging-app.datanika.io/ \
DATANIKA_E2E_SEED_CMD="ssh root@185.25.22.188 'docker exec datanika-staging-app uv run python -m datanika.scripts.e2e_seed'" \
npm test
```

Notes:

- **OAuth is disabled on staging** (callbacks aren't registered for the single-label hostname). Email/password signup works; any spec that relies on Google or GitHub OAuth stays `.skip`'d — that's intentional.
- **Staging's Postgres resets on boot**, so the seed fixture is ephemeral. That's fine — the seed is idempotent and reruns on every `npm test`.
- **Paddle sandbox is shared with prod** on staging (both are non-production from Paddle's POV). Billing edge-case tests that mutate subscription state should use `DATANIKA_E2E_SEED_CMD` with the local stack, not staging, until billing resets are sandboxed.
- **The pointer.gr SSH key must be in your agent** (Windows OpenSSH Pageant), same key Infra uses for deploys.

If you want to iterate against an already-seeded staging without re-running the seed each time, add `DATANIKA_E2E_SKIP_SEED=1` and export the nine `DATANIKA_E2E_*` fixture fields manually from a prior run's `.e2e-fixture.json`.

## Structure

```
e2e/
├── README.md            # this file
├── package.json         # playwright + test-runner deps only
├── playwright.config.ts # base URL, retries, trace on failure
├── fixtures/
│   ├── auth.ts          # test_user, test_org, api_client fixtures
│   └── data.ts          # deterministic seed + teardown
└── tests/
    ├── golden-path.spec.ts        # signup → connection → pipeline → run → assert
    ├── template-prefill.spec.ts   # pipeline template happy path
    ├── rbac.spec.ts               # viewer cannot delete pipeline
    ├── tenant-isolation.spec.ts   # user A cannot read org B
    └── oauth-template-param.spec.ts # ?template= preserved through OAuth
```

## Status

Scaffolded 2026-04-14 by QA. Current state of the 5 specs:

| Spec | State | Unblock |
|---|---|---|
| `golden-path.spec.ts` | enabled (`@slow`) | core#109 — merged |
| `template-prefill.spec.ts` | enabled | core#109 — merged |
| `oauth-template-param.spec.ts` email-signup | enabled | core#109 — merged |
| `oauth-template-param.spec.ts` Google + GitHub | `.skip` | needs mock IdP wiring (deferred) |
| `rbac.spec.ts` | `.skip` | core#113 — extended seed (viewer flag) |
| `tenant-isolation.spec.ts` | `.skip` | core#113 — extended seed (second-tenant flag) |

Do not delete the remaining skips without coordinating with QA.

## Running in CI

Not yet wired. Will be added as a separate workflow job once:
1. `make e2e-seed` lands in core (Engineering)
2. Docker Compose stack can be brought up inside GHA (RAM constraint TBD)
3. QA marks the first 5 tests `.skip` off

Tag expensive tests with `@slow`; `@slow` tests only run on PRs targeting `master`, not on every PR to `dev`.
