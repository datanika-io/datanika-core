"""Machine-readable agent context: /llms.txt, agent-guide.md, agent-tiers.json.

Tier 5 of the AI Agent Compatibility roadmap (#37). These are
discovery documents — no auth required, tenant-agnostic.

The actual tier and capability data lives in `agent_tiers.py`. This
module is a thin renderer that interpolates the SoT into the markdown
templates and serves them via Starlette routes. **Do not hardcode any
tier counts, capability lists, error codes, or golden-path steps in
this file** — derive them from `agent_tiers` so a single edit there
updates every surface.
"""

from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from datanika.services.agent_tiers import (
    capability_count,
    render_error_codes_table,
    render_golden_path,
    render_llms_capabilities,
    render_ui_only_operations,
    tier_count,
    to_json_dict,
)


def _render_llms_txt() -> str:
    return f"""\
# Datanika — AI Agent Discovery

> Datanika is an open-source data pipeline platform that combines
> extraction (dlt), transformation (dbt), scheduling, and monitoring
> in a single API. This file helps LLMs discover the API surface.

## OpenAPI Spec
https://app.datanika.io/api/v1/openapi.json

## Agent Guide
https://app.datanika.io/api/v1/agent-guide.md

## Agent Tiers (JSON)
https://app.datanika.io/api/v1/meta/agent-tiers

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

## Capabilities

{render_llms_capabilities()}

## Idempotency
POST requests accept an Idempotency-Key header. Same key within 24h \
returns the cached response.

## Tenant Isolation
API keys are scoped to an organization. All resources are filtered \
by org_id — you cannot read or modify another org's data.
"""


def _render_agent_guide() -> str:
    return f"""\
# Datanika Agent Guide

> How to build a complete data integration with the Datanika API.
> This is a strategy guide, not an API reference — see the
> [OpenAPI spec](https://app.datanika.io/api/v1/openapi.json) for
> full request/response schemas. For machine-readable tier structure,
> fetch [/api/v1/meta/agent-tiers](https://app.datanika.io/api/v1/meta/agent-tiers).

## Quick-Start Loop

The golden-path sequence for building a pipeline from scratch:

{render_golden_path()}

## Error Handling

Tier 2+ endpoints return typed string error codes so you can branch
without regex-matching messages:

{render_error_codes_table()}

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

{{"name": "Prod PG", "connection_type": "postgres", "config": {{...}}}}
```

## Tenant Isolation

- API keys are scoped to a single organization
- All CRUD operations filter by the key's `org_id`
- You cannot read or modify resources belonging to another org
- Creating an API key requires admin role in the org

## Common Patterns

- **Compile before preview** — `POST /compile` is cheap (no warehouse
  call). Always compile first to catch Jinja errors, then preview.
- **Use `?wait=true`** on run triggers instead of polling `GET /runs/{{id}}`
  in a loop. Default timeout is 120s, max 300s.
- **Cancel + retry** if a run appears stuck — `POST /runs/{{id}}/cancel`
  then re-trigger.
- **Introspect before building** — list source tables and columns
  before writing the upload config. Don't guess table names.

## What the API Cannot Do

These operations are only available in the Datanika UI:

{render_ui_only_operations()}

Do not attempt these via the API — there are no endpoints for them.
"""


# Module-level constants for backwards compatibility with existing tests
# and any code that imports the rendered strings directly. Computed once at
# import time from the SoT in agent_tiers.py.
LLMS_TXT: str = _render_llms_txt()
AGENT_GUIDE_MD: str = _render_agent_guide()


async def llms_txt(request: Request) -> PlainTextResponse:
    return PlainTextResponse(LLMS_TXT)


async def agent_guide(request: Request) -> Response:
    return Response(
        content=AGENT_GUIDE_MD,
        media_type="text/markdown; charset=utf-8",
    )


async def agent_tiers_json(request: Request) -> JSONResponse:
    """Machine-readable tier + capability structure.

    Used by the landing site at build time to render `/ai-agents` and
    `/docs/ai-agents` from a single source of truth, eliminating the
    drift bug that hit core PR #80 and landing PR #97.

    No auth required — same posture as `/llms.txt`.
    """
    return JSONResponse(to_json_dict())


agent_doc_routes = [
    Route("/llms.txt", llms_txt, methods=["GET"]),
    Route("/api/v1/agent-guide.md", agent_guide, methods=["GET"]),
    Route("/api/v1/meta/agent-tiers", agent_tiers_json, methods=["GET"]),
]


__all__ = [
    "LLMS_TXT",
    "AGENT_GUIDE_MD",
    "agent_doc_routes",
    "agent_tiers_json",
    "agent_guide",
    "llms_txt",
    "tier_count",
    "capability_count",
]
