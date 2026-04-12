"""Single source of truth for AI agent tier + capability definitions.

This module owns the canonical structure of Datanika's agent-facing API
surface. Every other place that talks about tiers or capabilities —
`/llms.txt`, `/api/v1/agent-guide.md`, `/api/v1/meta/agent-tiers`,
the landing site (via the JSON endpoint), the engineering plan — must
ultimately derive from this list.

Why: we shipped three off-by-one bugs in two days where a header
claimed N tiers but the list below it had M items (#80, landing #97).
The fix is structural — counts should be computed, not hardcoded.

This module has zero runtime dependencies (no Reflex, no Starlette, no
SQLAlchemy) so it can be imported from anywhere: HTTP handlers, CLI
export scripts, tests, or a future build-time JSON dump.

The 5-tier numbering matches the implementation order in
`plans/engineering/PLAN_ENGINEERING.md` → P0 AI Agent Compatibility.
Each tier groups one or more user-facing capabilities (the bullets the
LLMS_TXT discovery doc lists). Tier count and capability count are
deliberately not equal — agents see capabilities, engineering tracks
tiers.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Capability:
    name: str
    description: str
    endpoints: tuple[str, ...]


@dataclass(frozen=True)
class Tier:
    number: int
    name: str
    summary: str
    capabilities: tuple[Capability, ...]


TIERS: tuple[Tier, ...] = (
    Tier(
        number=1,
        name="Discover & Introspect",
        summary=(
            "Read JSON Schemas for every connection type and config, then "
            "introspect real source data — list tables, inspect columns, "
            "preview rows, run read-only SQL. The agent never guesses."
        ),
        capabilities=(
            Capability(
                name="Discover",
                description=(
                    "Meta endpoints return full JSON Schema for every "
                    "connection type, dlt config, dbt test, and materialization."
                ),
                endpoints=(
                    "GET /api/v1/meta/connection-types",
                    "GET /api/v1/meta/dlt-config-schema",
                    "GET /api/v1/meta/dbt-tests",
                    "GET /api/v1/meta/materializations",
                ),
            ),
            Capability(
                name="Introspect",
                description=(
                    "List source tables, inspect column types, preview "
                    "sample rows, and execute read-only SQL."
                ),
                endpoints=(
                    "POST /api/v1/connections/{id}/introspect",
                    "POST /api/v1/connections/{id}/columns",
                    "POST /api/v1/connections/{id}/preview",
                    "POST /api/v1/connections/{id}/query",
                ),
            ),
        ),
    ),
    Tier(
        number=2,
        name="Build",
        summary=(
            "Full CRUD for every resource the agent needs to assemble a "
            "pipeline: connections, uploads, pipelines, transformations, "
            "schedules, notification channels."
        ),
        capabilities=(
            Capability(
                name="Build",
                description=(
                    "Create and update connections, uploads, pipelines, "
                    "transformations, schedules, and notification channels."
                ),
                endpoints=(
                    "POST /api/v1/connections",
                    "POST /api/v1/uploads",
                    "POST /api/v1/pipelines",
                    "POST /api/v1/transformations",
                    "POST /api/v1/schedules",
                    "POST /api/v1/notifications/channels",
                ),
            ),
        ),
    ),
    Tier(
        number=3,
        name="Validate",
        summary=(
            "Compile dbt transformations to catch Jinja and ref errors, "
            "preview rows without touching production. Typed error codes "
            "let the agent branch on failures."
        ),
        capabilities=(
            Capability(
                name="Validate",
                description=(
                    "Compile dbt SQL without executing, then preview output "
                    "rows in a sandbox. Typed error codes."
                ),
                endpoints=(
                    "POST /api/v1/transformations/{id}/compile",
                    "POST /api/v1/transformations/{id}/preview",
                ),
            ),
        ),
    ),
    Tier(
        number=4,
        name="Execute & Control",
        summary=(
            "Trigger runs synchronously with `?wait=true`, cancel stuck "
            "runs, retry safely with Idempotency-Key headers, monitor "
            "history, and browse the auto-generated catalog."
        ),
        capabilities=(
            Capability(
                name="Execute",
                description=(
                    "Trigger runs with optional ?wait=true for synchronous "
                    "completion (default 120s, max 300s)."
                ),
                endpoints=(
                    "POST /api/v1/uploads/{id}/run?wait=true",
                    "POST /api/v1/pipelines/{id}/run?wait=true",
                    "POST /api/v1/transformations/{id}/run?wait=true",
                ),
            ),
            Capability(
                name="Control",
                description=(
                    "Cancel runs, monitor run history, stream logs, browse the data catalog."
                ),
                endpoints=(
                    "POST /api/v1/runs/{id}/cancel",
                    "GET /api/v1/runs",
                    "GET /api/v1/runs/{id}/logs",
                    "GET /api/v1/catalog",
                ),
            ),
        ),
    ),
    Tier(
        number=5,
        name="Machine-Readable Discovery",
        summary=(
            "Plain-text and Markdown documents an LLM agent can fetch "
            "without auth to learn the API surface, golden-path loop, "
            "error codes, and tier structure before it has an API key."
        ),
        capabilities=(
            Capability(
                name="Discover Docs",
                description=(
                    "Plain-text discovery, agent strategy guide, OpenAPI "
                    "spec, and the agent-tiers JSON endpoint."
                ),
                endpoints=(
                    "GET /llms.txt",
                    "GET /api/v1/agent-guide.md",
                    "GET /api/v1/openapi.json",
                    "GET /api/v1/meta/agent-tiers",
                ),
            ),
        ),
    ),
)


GOLDEN_PATH_LOOP: tuple[str, ...] = (
    (
        "`GET /api/v1/meta/connection-types` — discover available source and "
        "destination types with their config schemas"
    ),
    "`POST /api/v1/connections` — create a source connection (e.g. postgres)",
    "`POST /api/v1/connections` — create a destination connection (e.g. bigquery)",
    "`POST /api/v1/connections/{id}/test` — verify both connections work",
    "`POST /api/v1/connections/{id}/introspect` — list source tables",
    "`POST /api/v1/connections/{id}/columns` — inspect column types",
    "`GET /api/v1/meta/dlt-config-schema` — learn dlt_config options",
    "`POST /api/v1/uploads` — create an extract+load job",
    "`POST /api/v1/uploads/{id}/run?wait=true` — trigger and wait for completion",
    "`GET /api/v1/catalog` — discover loaded tables in the destination",
    "`GET /api/v1/meta/materializations` — learn dbt materialization types",
    "`GET /api/v1/meta/dbt-tests` — learn available test types",
    "`POST /api/v1/transformations` — create a dbt SQL model",
    "`POST /api/v1/transformations/{id}/compile` — validate Jinja + refs",
    "`POST /api/v1/transformations/{id}/preview` — see sample output rows",
    "`POST /api/v1/schedules` — schedule the pipeline on a cron",
    "`GET /api/v1/runs` — monitor run history",
)


@dataclass(frozen=True)
class ErrorCode:
    code: str
    meaning: str
    action: str


ERROR_CODES: tuple[ErrorCode, ...] = (
    ErrorCode("compilation_error", "Jinja/ref error in dbt compile", "Fix the SQL and re-compile"),
    ErrorCode("execution_error", "Warehouse query failed", "Check table names and SQL syntax"),
    ErrorCode(
        "missing_destination",
        "Transformation has no destination set",
        "Set destination_connection_id",
    ),
    ErrorCode("unsafe_sql", "Compiled SQL failed read-only check", "Review the transformation"),
    ErrorCode("invalid_request", "Malformed request body", "Check field types and required fields"),
    ErrorCode("not_cancellable", "Run already completed", "No action needed"),
)


UI_ONLY_OPERATIONS: tuple[str, ...] = (
    "**User management** — create/invite users, assign roles",
    "**Billing** — change plans, view invoices",
    "**SSO configuration** — SAML/OIDC setup",
    "**File uploads** — CSV/JSON/Parquet drag-and-drop (UI only)",
    "**OAuth connections** — Google/GitHub login setup",
    "**Organization creation** — orgs are created via the signup flow",
)


# ---------------------------------------------------------------------------
# Derived counts — never hardcode these in headers, always call the function
# ---------------------------------------------------------------------------


def tier_count() -> int:
    return len(TIERS)


def capability_count() -> int:
    return sum(len(t.capabilities) for t in TIERS)


def all_capabilities() -> tuple[Capability, ...]:
    return tuple(cap for tier in TIERS for cap in tier.capabilities)


# ---------------------------------------------------------------------------
# Renderers — single source for any markdown/text view of the tier data
# ---------------------------------------------------------------------------


def render_llms_capabilities() -> str:
    """Numbered capability list for /llms.txt.

    Each capability gets a number; tier grouping is implicit. Endpoints
    are joined with commas and trimmed for compactness so the list reads
    well in a terminal-rendered file.
    """
    lines: list[str] = []
    for n, cap in enumerate(all_capabilities(), start=1):
        endpoints = ", ".join(cap.endpoints)
        lines.append(f"{n}. **{cap.name}** — {endpoints}")
    return "\n".join(lines)


def render_golden_path() -> str:
    """Numbered 17-step golden-path loop for the agent guide."""
    return "\n".join(f"{n}. {step}" for n, step in enumerate(GOLDEN_PATH_LOOP, start=1))


def render_error_codes_table() -> str:
    """Markdown table of typed error codes."""
    rows = ["| Code | Meaning | Action |", "|------|---------|--------|"]
    for ec in ERROR_CODES:
        rows.append(f"| `{ec.code}` | {ec.meaning} | {ec.action} |")
    return "\n".join(rows)


def render_ui_only_operations() -> str:
    """Markdown bullet list of operations the API does not support."""
    return "\n".join(f"- {op}" for op in UI_ONLY_OPERATIONS)


# ---------------------------------------------------------------------------
# JSON serialization — for the /api/v1/meta/agent-tiers endpoint and any
# build-time export script
# ---------------------------------------------------------------------------


def to_json_dict() -> dict:
    """Serialize the full tier structure to a JSON-safe dict.

    Shape:
        {
            "tier_count": int,
            "capability_count": int,
            "tiers": [
                {
                    "number": int,
                    "name": str,
                    "summary": str,
                    "capabilities": [
                        {"name": str, "description": str, "endpoints": [str, ...]},
                        ...
                    ],
                },
                ...
            ],
            "golden_path": [str, ...],
            "error_codes": [{"code": str, "meaning": str, "action": str}, ...],
            "ui_only_operations": [str, ...],
        }
    """
    # Build the dict manually rather than using dataclasses.asdict so that
    # tuple fields (e.g. capability.endpoints) become JSON-friendly lists.
    return {
        "tier_count": tier_count(),
        "capability_count": capability_count(),
        "tiers": [
            {
                "number": t.number,
                "name": t.name,
                "summary": t.summary,
                "capabilities": [
                    {
                        "name": c.name,
                        "description": c.description,
                        "endpoints": list(c.endpoints),
                    }
                    for c in t.capabilities
                ],
            }
            for t in TIERS
        ],
        "golden_path": list(GOLDEN_PATH_LOOP),
        "error_codes": [
            {"code": ec.code, "meaning": ec.meaning, "action": ec.action} for ec in ERROR_CODES
        ],
        "ui_only_operations": list(UI_ONLY_OPERATIONS),
    }
