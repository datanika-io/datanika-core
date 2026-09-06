"""Shared service helpers for compiling and previewing dbt transformations.

Extracted from ``ui/state/transformation_state.py`` so that the REST API
v1 endpoints (``/transformations/{id}/compile`` and ``/preview``) and
the Reflex UI can share a single code path. The UI state wrappers
remain responsible for error-to-string translation; this module returns
structured results that either side can format.

All functions are tenant-isolated: the caller must pass a valid
``org_id`` and the service looks up the transformation scoped to that
org. No cross-tenant reads possible.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from datanika.config import settings
from datanika.errors import UserFacingError
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.dbt_project import DbtProjectError, DbtProjectService
from datanika.services.encryption import EncryptionService
from datanika.services.transformation_service import TransformationService

logger = logging.getLogger(__name__)

_LINE_COLUMN_RE = re.compile(r"line\s+(\d+)(?:.*?column\s+(\d+))?", re.IGNORECASE)

MAX_PREVIEW_LIMIT = 1000
DEFAULT_PREVIEW_LIMIT = 100


# ---------------------------------------------------------------------------
# Result dataclasses — structured so both API and UI can consume them.
# ---------------------------------------------------------------------------


@dataclass
class CompileResult:
    """Outcome of compiling a transformation.

    ``success`` mirrors dbt's own flag. On failure, ``error_message``
    contains the dbt error log and ``line`` / ``column`` may be parsed
    from it when dbt surfaces them (best-effort — dbt's output format
    is not stable).
    """

    success: bool
    compiled_sql: str = ""
    node: str = ""
    error_message: str = ""
    line: int | None = None
    column: int | None = None


@dataclass
class PreviewResult:
    """Outcome of previewing a transformation against the warehouse."""

    success: bool
    columns: list[dict] = field(default_factory=list)
    rows: list[list] = field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    compiled_sql: str = ""
    error_code: str = ""
    error_message: str = ""


class TransformationNotFoundError(UserFacingError):
    """Raised when a transformation doesn't exist for the given org."""


class MissingDestinationError(UserFacingError):
    """Raised when a transformation has no destination connection set."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compile_transformation(
    session: Session,
    org_id: int,
    transformation_id: int,
    *,
    dbt_projects_dir: str | None = None,
) -> CompileResult:
    """Compile a transformation via dbt and return the result.

    Raises ``TransformationNotFoundError`` if the transformation
    doesn't exist for the given org (use this to map to a 404 at
    the API layer). Other exceptions are caught and returned as
    ``CompileResult(success=False, ...)``.
    """
    transform, conn_svc, conn, config = _load_for_compile(session, org_id, transformation_id)

    projects_dir = dbt_projects_dir or settings.dbt_projects_dir
    dbt_svc = DbtProjectService(projects_dir)

    try:
        dbt_svc.ensure_project(org_id)
        default_schema = _default_schema(session, org_id)
        if conn is not None and config:
            dbt_svc.generate_profiles_yml(
                org_id,
                conn.connection_type.value,
                config,
                default_schema=default_schema,
            )
        dbt_svc.write_model(
            org_id,
            transform.name,
            transform.sql_body,
            schema_name=transform.schema_name,
            materialization=transform.materialization.value,
            incremental_config=transform.incremental_config,
        )
        result = dbt_svc.compile_model(org_id, transform.name)
    except DbtProjectError as exc:
        return CompileResult(success=False, error_message=str(exc))
    except Exception as exc:  # pragma: no cover — defensive
        logger.exception("Unexpected error compiling transformation %s", transform.id)
        return CompileResult(success=False, error_message=str(exc))

    if not result.get("success") or not result.get("compiled_sql"):
        logs = result.get("logs") or "Compilation produced no output"
        line, column = _parse_line_column(logs)
        return CompileResult(
            success=False,
            error_message=logs,
            line=line,
            column=column,
            node=f"model.tenant_{org_id}.{transform.name}",
        )

    return CompileResult(
        success=True,
        compiled_sql=result["compiled_sql"],
        node=f"model.tenant_{org_id}.{transform.name}",
    )


def preview_transformation(
    session: Session,
    org_id: int,
    transformation_id: int,
    *,
    limit: int = DEFAULT_PREVIEW_LIMIT,
    dbt_projects_dir: str | None = None,
) -> PreviewResult:
    """Compile a transformation, add a safe LIMIT, run against the
    destination warehouse, and return rows.

    ``limit`` is clamped to ``[1, MAX_PREVIEW_LIMIT]``.

    Raises ``TransformationNotFoundError`` or
    ``MissingDestinationError`` for the caller to map to 404 / 400.
    Warehouse errors come back as ``PreviewResult(success=False)``.
    """
    clamped = max(1, min(int(limit or DEFAULT_PREVIEW_LIMIT), MAX_PREVIEW_LIMIT))

    # Fail-fast before running dbt compile: if there's no destination,
    # preview is impossible. Agents get a clean typed error that's
    # distinguishable from a compilation failure.
    transform, conn_svc, conn, config = _load_for_compile(session, org_id, transformation_id)
    if conn is None or not config:
        raise MissingDestinationError("Transformation has no destination connection set")

    compile_result = compile_transformation(
        session,
        org_id,
        transformation_id,
        dbt_projects_dir=dbt_projects_dir,
    )
    if not compile_result.success:
        return PreviewResult(
            success=False,
            error_code="compilation_error",
            error_message=compile_result.error_message,
        )

    # Wrap the compiled SQL in a bounded SELECT so the existing
    # read-only SQL guard applies. Defense in depth: never trust that
    # dbt compiled something safe.
    inner = compile_result.compiled_sql.strip().rstrip(";")
    # noqa S608: the interpolation is the point, not an oversight — this wraps
    # dbt's own compiled SQL. `clamped` is an int, and the wrapped result is
    # handed to `ConnectionService.is_select_only()` on the very next line,
    # which is the guard. Flagging it here would only move it somewhere ruff
    # cannot see it.
    query = f"SELECT * FROM (\n{inner}\n) AS _preview LIMIT {clamped}"  # noqa: S608
    if not ConnectionService.is_select_only(query):
        return PreviewResult(
            success=False,
            error_code="unsafe_sql",
            error_message="Compiled SQL did not pass the read-only validator",
            compiled_sql=compile_result.compiled_sql,
        )

    try:
        columns, rows = ConnectionService.execute_query(
            config,
            conn.connection_type,
            query,
        )
    except ValueError as exc:
        return PreviewResult(
            success=False,
            error_code="execution_error",
            error_message=str(exc),
            compiled_sql=compile_result.compiled_sql,
        )
    except Exception as exc:
        # Warehouse-side failure. Truncate the message so OpenAPI
        # clients don't choke on megabyte-long stack traces.
        return PreviewResult(
            success=False,
            error_code="execution_error",
            error_message=_truncate(str(exc), 500),
            compiled_sql=compile_result.compiled_sql,
        )

    serialized_rows = [[_to_json_safe(v) for v in row] for row in rows]
    column_objs = [{"name": c, "type": ""} for c in columns]
    return PreviewResult(
        success=True,
        columns=column_objs,
        rows=serialized_rows,
        row_count=len(serialized_rows),
        truncated=len(serialized_rows) >= clamped,
        compiled_sql=compile_result.compiled_sql,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_for_compile(
    session: Session,
    org_id: int,
    transformation_id: int,
):
    """Load the transformation, decrypt the destination connection,
    and return the full tuple needed by compile/preview.

    Raises ``TransformationNotFoundError`` when the transformation
    doesn't exist for ``org_id``. Missing destination is permitted
    for compile (the service still writes the model file), but
    ``preview_transformation`` re-checks and raises its own error.
    """
    svc = TransformationService()
    transform = svc.get_transformation(session, org_id, transformation_id)
    if transform is None:
        raise TransformationNotFoundError(f"Transformation {transformation_id} not found")

    encryption = EncryptionService(settings.credential_encryption_key)
    conn_svc = ConnectionService(encryption)
    conn = None
    config: dict | None = None
    if transform.destination_connection_id:
        conn = conn_svc.get_connection(session, org_id, transform.destination_connection_id)
        if conn is not None:
            config = conn_svc.get_connection_config(
                session, org_id, transform.destination_connection_id
            )
    return transform, conn_svc, conn, config


def _default_schema(session: Session, org_id: int) -> str:
    org = session.get(Organization, org_id)
    if org and getattr(org, "default_dbt_schema", None):
        return org.default_dbt_schema
    return "datanika"


def _parse_line_column(message: str) -> tuple[int | None, int | None]:
    """Best-effort parse of dbt error message for line/column."""
    if not message:
        return None, None
    m = _LINE_COLUMN_RE.search(message)
    if not m:
        return None, None
    try:
        line = int(m.group(1))
    except (TypeError, ValueError):
        line = None
    try:
        column = int(m.group(2)) if m.group(2) else None
    except (TypeError, ValueError):
        column = None
    return line, column


def _to_json_safe(value):
    """Convert a DB row value to something JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _truncate(text: str, limit: int) -> str:
    if not text:
        return ""
    return text if len(text) <= limit else text[: limit - 1] + "…"
