"""Column reflection for DuckDB, read from ``information_schema`` (core#1431).

``duckdb_engine`` derives its dialect from PostgreSQL's and inherits PostgreSQL's column
reflection, which joins ``pg_catalog.pg_collation``. DuckDB provides much of ``pg_catalog`` and
**not** that table, so every column read raises::

    Catalog Error: Table with name pg_collation does not exist!

Measured on the locked versions (SQLAlchemy 2.0.46, duckdb 1.5.1, duckdb-engine 0.17.0), method by
method: ``get_schema_names``, ``get_table_names``, ``get_view_names``, ``get_pk_constraint``,
``get_foreign_keys``, ``get_indexes``, ``get_unique_constraints``, ``get_check_constraints`` and
``get_table_comment`` all answer correctly. **Only the column read fails**, and
``information_schema.columns`` — which DuckDB implements — answers it. So this replaces exactly
that one method and leaves the rest of the dialect alone.

The same error was already known one path over: ``CatalogService._columns_for`` falls back to
``information_schema`` for it (core#494). That fallback is per-call and lives in the catalog; a
source run reflects through SQLAlchemy itself and never reaches it, which is why a DuckDB source
could not load a row while the catalog could list one.

⚠️ **Both the single and the multi form are replaced.** ``MetaData.reflect`` — which is what dlt's
``sql_database`` calls — batches through ``get_multi_columns``, so patching ``get_columns`` alone
fixes the connector's column picker and leaves the run raising exactly as before.

⚠️ **This is a patch on someone else's dialect, so it is written to be removable.** When a
duckdb-engine release reflects columns without ``pg_collation``,
``tests/test_services/test_a_duckdb_source_loads_its_rows.py`` keeps passing with this deleted, and
that is the signal to delete it.
"""

from __future__ import annotations

import logging
import re

from sqlalchemy import text
from sqlalchemy import types as sqltypes

logger = logging.getLogger(__name__)

#: `DECIMAL(10,2)` -> `decimal`. Parameters come from the numeric/character columns instead, so the
#: name is matched bare.
_TYPE_PARAMS = re.compile(r"\s*\(.*\)\s*$", re.S)

#: DuckDB spellings that PostgreSQL's `ischema_names` does not carry under the same key. Anything
#: not here and not in the dialect's own map resolves to `NullType`, which reflects as "a column
#: whose type this dialect cannot name" rather than as a wrong type.
_DUCKDB_TYPE_NAMES = {
    "bigint": sqltypes.BigInteger,
    "blob": sqltypes.LargeBinary,
    "bit": sqltypes.String,
    "boolean": sqltypes.Boolean,
    "date": sqltypes.Date,
    "decimal": sqltypes.Numeric,
    "double": sqltypes.Float,
    "float": sqltypes.Float,
    "integer": sqltypes.Integer,
    "interval": sqltypes.Interval,
    "numeric": sqltypes.Numeric,
    "real": sqltypes.Float,
    "smallint": sqltypes.SmallInteger,
    "time": sqltypes.Time,
    "timestamp": sqltypes.TIMESTAMP,
    "timestamp with time zone": sqltypes.TIMESTAMP,
    "timestamp without time zone": sqltypes.TIMESTAMP,
    "tinyint": sqltypes.SmallInteger,
    "uuid": sqltypes.Uuid,
    "varchar": sqltypes.String,
}

_COLUMNS_SQL = text(
    """
    SELECT table_name, column_name, data_type, is_nullable, column_default,
           character_maximum_length, numeric_precision, numeric_scale
    FROM information_schema.columns
    WHERE table_schema = :schema
    ORDER BY table_name, ordinal_position
    """
)


def _resolve_type(dialect, raw: str, char_len, precision, scale):
    """A SQLAlchemy type for one DuckDB type name, with its parameters where they matter."""
    bare = _TYPE_PARAMS.sub("", (raw or "")).strip().lower()
    cls = _DUCKDB_TYPE_NAMES.get(bare) or getattr(dialect, "ischema_names", {}).get(bare)
    if cls is None:
        # A nested or extension type (LIST, STRUCT, MAP, UNION, an enum). Naming it wrongly is
        # worse than declining to name it: dlt infers from the data, and a wrong declared type
        # makes it coerce.
        logger.debug("duckdb: no SQLAlchemy type for %r, reflecting as NullType", raw)
        return sqltypes.NullType()
    try:
        if cls is sqltypes.Numeric and precision is not None:
            return cls(precision=precision, scale=scale)
        if cls is sqltypes.String and char_len:
            return cls(length=char_len)
        return cls()
    except TypeError:  # pragma: no cover - a type that takes different arguments
        return sqltypes.NullType()


def _rows_by_table(dialect, connection, schema):
    out: dict[str, list[dict]] = {}
    resolved = schema or dialect.default_schema_name or "main"
    for row in connection.execute(_COLUMNS_SQL, {"schema": resolved}):
        out.setdefault(row.table_name, []).append(
            {
                "name": row.column_name,
                "type": _resolve_type(
                    dialect,
                    row.data_type,
                    row.character_maximum_length,
                    row.numeric_precision,
                    row.numeric_scale,
                ),
                "nullable": str(row.is_nullable).upper() != "NO",
                "default": row.column_default,
                "autoincrement": False,
                "comment": None,
            }
        )
    return out


def _get_columns(self, connection, table_name, schema=None, **kw):
    columns = _rows_by_table(self, connection, schema).get(table_name)
    if columns is None:
        from sqlalchemy import exc as sa_exc

        raise sa_exc.NoSuchTableError(table_name)
    return columns


def _get_multi_columns(self, connection, schema=None, filter_names=None, **kw):
    """The batch form ``MetaData.reflect`` uses, keyed the way SQLAlchemy looks results up.

    ⚠️ **The key carries ``schema`` exactly as it was passed, ``None`` included** — it is not the
    resolved schema the query ran against. SQLAlchemy builds the lookup key from the argument it
    gave, so keying on the resolved name produces a mapping whose every entry misses, and the
    reflection then reports ``NoSuchTableError`` for a table it has just read the columns of.
    Measured: that is the difference between this working and the single form working while
    ``MetaData.reflect`` still fails.
    """
    by_table = _rows_by_table(self, connection, schema)
    wanted = list(filter_names) if filter_names is not None else list(by_table)
    return (((schema, name), by_table.get(name, [])) for name in wanted)


def install_duckdb_column_reflection() -> bool:
    """Replace duckdb-engine's inherited column reflection. Idempotent; never raises.

    Returns whether the dialect now carries the replacement, so a caller (or a test) can assert a
    positive outcome rather than the absence of an exception.
    """
    try:
        import duckdb_engine
    except Exception:  # pragma: no cover - duckdb is optional for a core-only install
        logger.debug("duckdb_engine is not installed; column reflection left as it is")
        return False

    dialect = duckdb_engine.Dialect
    if getattr(dialect.get_columns, "_datanika_duckdb_reflection", False):
        return True

    for name, replacement in (
        ("get_columns", _get_columns),
        ("get_multi_columns", _get_multi_columns),
    ):
        replacement._datanika_duckdb_reflection = True
        setattr(dialect, name, replacement)
    return True
