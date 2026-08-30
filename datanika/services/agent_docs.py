"""Machine-readable agent context: /llms.txt, agent-guide.md, agent-tiers.json.

Tier 5 of the AI Agent Compatibility roadmap (#37). These are
discovery documents — no auth required, tenant-agnostic.

The actual tier and capability data lives in `agent_tiers.py`. This
module is a thin renderer that interpolates the SoT into the markdown
templates and serves them via Starlette routes. **Do not hardcode any
tier counts, capability lists, error codes, or golden-path steps in
this file** — derive them from `agent_tiers` so a single edit there
updates every surface.

Delivery note (issue #124): `/llms.txt` is at the site root, not under
the `/api/v1/*` backend prefix. Nginx routes `/` to the Reflex frontend
(port 3000) and `/api/*` / `/_event` to Starlette (port 8000), so a
Starlette route registered at `/llms.txt` never receives the request
in production — the frontend 404s first. To match the published URL
(`https://app.datanika.io/llms.txt`, referenced from landing / blog /
comparison pages) we also materialise the rendered text into the
Reflex `assets/` directory at app startup, which Reflex's frontend
serves at root. The Starlette route is kept for tests + API clients
that hit the backend directly.
"""

from pathlib import Path

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

## MCP Server
For MCP clients (Claude Desktop, Cursor), Datanika ships a first-class MCP \
server — `datanika-mcp` — so you can browse connections, preview data, compile \
and validate transformations, and run pipelines as native tools instead of raw \
REST. Read-only by default; pass `--allow-write` to enable mutations.
Install (no clone needed):
uvx --from "git+https://github.com/datanika-io/datanika-core#subdirectory=datanika-mcp" datanika-mcp
Full tool list + Claude Desktop config:
https://github.com/datanika-io/datanika-core/blob/master/datanika-mcp/README.md

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

## MCP Server (alternative to raw REST)

If you're an MCP client (Claude Desktop, Cursor), use Datanika's first-class
`datanika-mcp` server instead of calling REST directly — it exposes the same
capabilities (browse connections, preview data, compile/validate
transformations, run pipelines) as native tools. Read-only by default;
`--allow-write` enables mutations.

```
uvx --from "git+https://github.com/datanika-io/datanika-core#subdirectory=datanika-mcp" datanika-mcp
```

Full tool list + Claude Desktop / Cursor config:
[datanika-mcp/README.md](https://github.com/datanika-io/datanika-core/blob/master/datanika-mcp/README.md)

The REST golden path below still applies to plain HTTP clients.

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
- **With `?wait=true`, the status code carries the run's outcome**, not just
  the transport's — so `curl --fail` and `raise_for_status()` are correct
  idioms for "did my pipeline work?":
  - `200` — the run finished **successfully**
  - `408` — still `pending`/`running` when the wait timed out; the body has
    `"timed_out": true` and the run is still going. Poll `GET /runs/{{id}}`
    or re-wait; this is not a failure.
  - `422` — the run reached a **terminal, non-success** status (`failed`,
    `cancelled`). Not a 5xx: the failure is in your pipeline, not our server.
  The serialised run is the body in all three cases, so `status` and
  `error_message` remain the source of truth. Without `?wait=true` the trigger
  still returns `202` immediately and says nothing about the outcome.
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


# Default path to the Reflex assets directory, relative to the project
# root (the directory Reflex is run from). Reflex serves contents of
# this directory at the site root, so writing `assets/llms.txt` makes
# it reachable at `https://app.datanika.io/llms.txt` through nginx.
DEFAULT_ASSETS_DIR = Path(__file__).resolve().parents[2] / "assets"


def write_llms_txt_asset(assets_dir: Path = DEFAULT_ASSETS_DIR) -> Path:
    """Materialise LLMS_TXT into the Reflex assets directory.

    Called from ``datanika.datanika`` at app bootstrap. Idempotent —
    safe to call on every startup. The file is gitignored so it stays
    in sync with the SoT (``agent_tiers.py``) automatically.

    Returns the path the file was written to, for logging/testing.
    """
    assets_dir.mkdir(parents=True, exist_ok=True)
    target = assets_dir / "llms.txt"
    target.write_text(LLMS_TXT, encoding="utf-8")
    return target


__all__ = [
    "LLMS_TXT",
    "AGENT_GUIDE_MD",
    "agent_doc_routes",
    "agent_tiers_json",
    "agent_guide",
    "llms_txt",
    "tier_count",
    "capability_count",
    "write_llms_txt_asset",
    "DEFAULT_ASSETS_DIR",
]
