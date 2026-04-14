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

- Docker Desktop running
- Node 22 (`D:/Tools/node-v22.14.0-win-x64`) on PATH
- A seed command that produces a deterministic clean org — TBD, Engineering dependency (`make e2e-seed`)

## Running

```bash
# From repo root
cd e2e
npm install
npx playwright install chromium
npm test                 # headless
npm run test:headed      # watch a run in a real browser window
npm run test:ui          # Playwright UI mode (best for writing new tests)
```

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

Scaffolded 2026-04-14 by QA. Tests are drafted (`.skip` until seed script + CI wiring land). Do not delete the skips without coordinating with QA.

## Running in CI

Not yet wired. Will be added as a separate workflow job once:
1. `make e2e-seed` lands in core (Engineering)
2. Docker Compose stack can be brought up inside GHA (RAM constraint TBD)
3. QA marks the first 5 tests `.skip` off

Tag expensive tests with `@slow`; `@slow` tests only run on PRs targeting `master`, not on every PR to `dev`.
