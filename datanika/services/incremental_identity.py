"""The identity an incremental upload's cursor is recorded under (core#1404).

Contract: ``docs/specs/SPEC_INCREMENTAL_UPLOADS.md``.

dlt restores an incremental cursor from the destination **by pipeline name**. Every upload run used
to build a pipeline named after the run, so no run ever found the previous run's cursor, and every
incremental upload started again from its initial value (§0). The fix gives an incremental upload
one pipeline name across runs, and that name is this module's identity.

§2.3: the identity is the source connection, source schema, table, cursor column and initial value,
and the destination connection and destination schema. Changing any of them starts the cursor again
from the initial value, because a cursor carried across such a change loses rows silently:

* **destination connection or schema**: a new destination would receive only rows past the old
  cursor;
* **source connection, schema or table**: one database's cursor would skip another's older rows;
* **cursor column**: a value recorded for one column means nothing for another;
* **initial value**: changing it is the user saying where to start, and it doubles as the reset.

Nothing else is in the key: ``write_disposition``, ``row_order``, batch size, the schema contract,
primary key, description. Values are compared, not their stored spelling: the form stores
``initial_value`` as text and the API can store a number, and re-saving an unchanged form must not
restart a cursor.

⚠️ No import of ``dlt_runner`` here. Importing it pulls dlt into the web app's imports, and the form
and the API need :func:`refuses_incremental`. :data:`RESUMABLE_INCREMENTAL_SOURCE_TYPES` is
therefore a literal, and ``tests/test_services/test_incremental_identity.py`` pins it to the
loader's own set.
"""

from __future__ import annotations

import hashlib
import json

#: Source types whose builder passes ``incremental`` to dlt's ``sql_table``. Equal to
#: ``DltRunnerService.SUPPORTED_SOURCE_TYPES``, asserted by a test.
RESUMABLE_INCREMENTAL_SOURCE_TYPES = frozenset(
    {"postgres", "mysql", "mssql", "sqlite", "clickhouse", "duckdb", "oracle"}
)


def _cursor(dlt_config: dict) -> dict | None:
    incremental = dlt_config.get("incremental")
    if isinstance(incremental, dict) and incremental.get("cursor_path"):
        return incremental
    return None


def _canonical(value) -> str | None:
    """A value as the key compares it: absent and empty are one, and ``30`` equals ``"30"``."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def has_resumable_cursor(source_type: str, dlt_config: dict) -> bool:
    """Whether this upload's runs share a cursor: a single-table SQL upload with a cursor column."""
    return (
        _cursor(dlt_config) is not None
        and source_type in RESUMABLE_INCREMENTAL_SOURCE_TYPES
        and dlt_config.get("mode", "full_database") == "single_table"
    )


def incremental_pipeline_name(
    *,
    upload_id: int,
    source_type: str,
    source_connection_id: int,
    destination_connection_id: int,
    destination_schema: str | None,
    dlt_config: dict,
) -> str | None:
    """The pipeline name an incremental upload's runs share, or ``None`` when no cursor resumes.

    ``None`` keeps today's per-run name, which SPEC §3 requires for every upload without a resumable
    cursor: SaaS, OpenAPI, file and non-incremental SQL uploads were designed around no dlt state
    crossing runs (core#1336).
    """
    if not has_resumable_cursor(source_type, dlt_config):
        return None
    incremental = _cursor(dlt_config)
    identity = {
        "upload": upload_id,
        "source_connection": source_connection_id,
        "source_schema": _canonical(dlt_config.get("source_schema")),
        "table": _canonical(dlt_config.get("table")),
        "cursor_path": _canonical(incremental.get("cursor_path")),
        "initial_value": _canonical(incremental.get("initial_value")),
        "destination_connection": destination_connection_id,
        "destination_schema": _canonical(destination_schema),
    }
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()
    return f"upload_{upload_id}_incremental_{digest[:16]}"


def refuses_incremental(source_type: str, dlt_config: dict) -> str | None:
    """Why a cursor cannot be saved for this source, or ``None`` when it resumes (SPEC §2.5).

    The product never calls something incremental that does not resume. MongoDB's builder applies
    the initial value as a fixed filter that never advances, and every other non-SQL source ignores
    the cursor, so both are refused at save rather than stored and silently not honoured.
    """
    if _cursor(dlt_config) is None or source_type in RESUMABLE_INCREMENTAL_SOURCE_TYPES:
        return None
    return (
        f"Incremental loads are not available for {source_type} sources: the cursor would not "
        "advance between runs, so every run would read the same rows. Remove 'incremental' "
        "from the upload configuration."
    )
