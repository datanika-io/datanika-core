"""Machine-readable agent context: /llms.txt and /api/v1/agent-guide.md.

Tier 5 of the AI Agent Compatibility roadmap (#37). These are
discovery documents — no auth required, tenant-agnostic.
"""

from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

LLMS_TXT = """\
# Datanika — AI Agent Discovery

> Datanika is an open-source data pipeline platform that combines
> extraction (dlt), transformation (dbt), scheduling, and monitoring
> in a single API. This file helps LLMs discover the API surface.

## OpenAPI Spec
https://app.datanika.io/api/v1/openapi.json

## Agent Guide
https://app.datanika.io/api/v1/agent-guide.md

## Base URL
https://app.datanika.io/api/v1/

## Authentication
All API requests require a Bearer API key:
  Authorization: Bearer etf_<your_key>
Create keys in the Datanika UI at Settings → API Keys.

## Rate Limits
Default: 60 requests/minute per API key.
Plan-based overrides: Free 30 RPM, Pro 120 RPM, Enterprise 300 RPM.
Rate limit headers: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset.

## Capabilities (5 tiers)

1. **Discover** — GET /meta/connection-types, /meta/dlt-config-schema, \
/meta/dbt-tests, /meta/materializations
2. **Introspect** — POST /connections/{id}/introspect, /columns, /preview, \
/query
3. **Build** — CRUD for /connections, /uploads, /pipelines, \
/transformations, /schedules, /notifications/channels
4. **Validate** — POST /transformations/{id}/compile, /preview
5. **Execute** — POST /uploads/{id}/run, /pipelines/{id}/run, \
/transformations/{id}/run (?wait=true supported)
6. **Control** — POST /runs/{id}/cancel, GET /runs/{id}/logs, \
GET /catalog

## Idempotency
POST requests accept an Idempotency-Key header. Same key within 24h \
returns the cached response.

## Tenant Isolation
API keys are scoped to an organization. All resources are filtered \
by org_id — you cannot read or modify another org's data.
"""

AGENT_GUIDE_MD = """\
# Datanika Agent Guide

> How to build a complete data integration with the Datanika API.
> This is a strategy guide, not an API reference — see the
> [OpenAPI spec](https://app.datanika.io/api/v1/openapi.json) for
> full request/response schemas.

## Quick-Start Loop

The golden-path sequence for building a pipeline from scratch:

1. `GET /api/v1/meta/connection-types` — discover available source \
and destination types with their config schemas
2. `POST /api/v1/connections` — create a source connection (e.g. postgres)
3. `POST /api/v1/connections` — create a destination connection (e.g. bigquery)
4. `POST /api/v1/connections/{id}/test` — verify both connections work
5. `POST /api/v1/connections/{id}/introspect` — list source tables
6. `POST /api/v1/connections/{id}/columns` — inspect column types
7. `GET /api/v1/meta/dlt-config-schema` — learn dlt_config options
8. `POST /api/v1/uploads` — create an extract+load job
9. `POST /api/v1/uploads/{id}/run?wait=true` — trigger and wait for completion
10. `GET /api/v1/catalog` — discover loaded tables in the destination
11. `GET /api/v1/meta/materializations` — learn dbt materialization types
12. `GET /api/v1/meta/dbt-tests` — learn available test types
13. `POST /api/v1/transformations` — create a dbt SQL model
14. `POST /api/v1/transformations/{id}/compile` — validate Jinja + refs
15. `POST /api/v1/transformations/{id}/preview` — see sample output rows
16. `POST /api/v1/schedules` — schedule the pipeline on a cron
17. `GET /api/v1/runs` — monitor run history

## Error Handling

Tier 2+ endpoints return typed string error codes so you can branch
without regex-matching messages:

| Code | Meaning | Action |
|------|---------|--------|
| `compilation_error` | Jinja/ref error in dbt compile | Fix the SQL and re-compile |
| `execution_error` | Warehouse query failed | Check table names and SQL syntax |
| `missing_destination` | Transformation has no destination set | Set destination_connection_id |
| `unsafe_sql` | Compiled SQL failed read-only check | Review the transformation |
| `invalid_request` | Malformed request body | Check field types and required fields |
| `not_cancellable` | Run already completed | No action needed |

## Idempotency

Add an `Idempotency-Key` header to POST requests. If a request with
the same key was already processed within 24 hours, the original
response is returned instead of creating a duplicate. Useful when
retrying after network errors.

```
POST /api/v1/connections
Authorization: Bearer etf_...
Idempotency-Key: my-unique-key-123
Content-Type: application/json

{"name": "Prod PG", "connection_type": "postgres", "config": {...}}
```

## Tenant Isolation

- API keys are scoped to a single organization
- All CRUD operations filter by the key's `org_id`
- You cannot read or modify resources belonging to another org
- Creating an API key requires admin role in the org

## Common Patterns

- **Compile before preview** — `POST /compile` is cheap (no warehouse
  call). Always compile first to catch Jinja errors, then preview.
- **Use `?wait=true`** on run triggers instead of polling `GET /runs/{id}`
  in a loop. Default timeout is 120s, max 300s.
- **Cancel + retry** if a run appears stuck — `POST /runs/{id}/cancel`
  then re-trigger.
- **Introspect before building** — list source tables and columns
  before writing the upload config. Don't guess table names.

## What the API Cannot Do

These operations are only available in the Datanika UI:

- **User management** — create/invite users, assign roles
- **Billing** — change plans, view invoices
- **SSO configuration** — SAML/OIDC setup
- **File uploads** — CSV/JSON/Parquet drag-and-drop (UI only)
- **OAuth connections** — Google/GitHub login setup
- **Organization creation** — orgs are created via the signup flow

Do not attempt these via the API — there are no endpoints for them.
"""


async def llms_txt(request: Request) -> PlainTextResponse:
    return PlainTextResponse(LLMS_TXT)


async def agent_guide(request: Request) -> Response:
    return Response(
        content=AGENT_GUIDE_MD,
        media_type="text/markdown; charset=utf-8",
    )


agent_doc_routes = [
    Route("/llms.txt", llms_txt, methods=["GET"]),
    Route("/api/v1/agent-guide.md", agent_guide, methods=["GET"]),
]
