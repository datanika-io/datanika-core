"""Arrow utilities shared across destination landers.

E9 — nested-column stringification. When a source yields nested types
(struct / list / map) — common for SaaS sources, MongoDB, REST APIs,
Kafka, and JSON files — columnar destinations like Postgres can't COPY
them as-is. The portable pattern is to serialize each nested column to
a JSON string, letting the destination store it as TEXT or JSONB.

Fast path — ``orjson.dumps()``. 5-10× faster than ``json.dumps`` and
supports ``datetime`` / ``UUID`` / ``numpy`` natively.

Safe path — ``bson.json_util.dumps()``. Falls back on per-column basis
when orjson raises ``TypeError`` for BSON-specific types (``ObjectId``,
``Decimal128``). The fallback decision is remembered at the column level
so we don't re-probe every batch.

See ``D:/Projects/whisk/data_repos/DB data transfer architecture.md``
§"MongoDB serialization" for the performance justification — Whisk's
prod MongoDB flattening spends ~12s/batch on ``bson.json_util.dumps``;
``orjson`` drops it to ~3s where safe.
"""

from __future__ import annotations

from typing import Any

import orjson
import pyarrow as pa


def _serialize_with_orjson(value: Any) -> str | None:
    """Fast-path serializer. Returns None for null values."""
    if value is None:
        return None
    return orjson.dumps(value, default=_orjson_default).decode("utf-8")


def _orjson_default(obj: Any) -> Any:
    """orjson fallback for types it doesn't handle natively.

    - ``bytes`` → hex string (rare in JSON payloads; dlt normally decodes)
    - Everything else → raise TypeError so the caller switches to bson fallback
    """
    if isinstance(obj, bytes):
        return obj.hex()
    raise TypeError(f"orjson: unsupported type {type(obj).__name__}")


def _serialize_with_bson(value: Any) -> str | None:
    """Safe-path serializer for BSON types (ObjectId, Decimal128, etc.)."""
    if value is None:
        return None
    from bson import json_util

    return json_util.dumps(value)


def _serialize_array(column: pa.ChunkedArray | pa.Array) -> pa.Array:
    """Serialize a nested Arrow column to a string column.

    Tries ``orjson`` first; on first ``TypeError`` switches to
    ``bson.json_util`` for the rest of the column.
    """
    # Convert to Python once — Arrow's nested-to-Python path is fast in C++.
    python_values = column.to_pylist()

    use_bson = False
    serialized: list[str | None] = []
    for value in python_values:
        if use_bson:
            serialized.append(_serialize_with_bson(value))
            continue
        try:
            serialized.append(_serialize_with_orjson(value))
        except TypeError:
            # Switch to bson for the rest of this column — retry current value.
            use_bson = True
            serialized.append(_serialize_with_bson(value))

    return pa.array(serialized, type=pa.string())


def stringify_nested_columns(table: pa.Table) -> pa.Table:
    """Replace struct/list/map columns with JSON-string columns.

    Scalar columns (int, float, string, bool, timestamp, etc.) pass through
    unchanged. This is the single entry point used by ``PostgresLander`` and
    any other columnar-CSV destination lander.
    """
    new_columns: list[pa.Array | pa.ChunkedArray] = []
    new_fields: list[pa.Field] = []

    for field, column in zip(table.schema, table.columns, strict=True):
        if _is_nested(field.type):
            new_columns.append(_serialize_array(column))
            new_fields.append(pa.field(field.name, pa.string(), nullable=field.nullable))
        else:
            new_columns.append(column)
            new_fields.append(field)

    return pa.Table.from_arrays(new_columns, schema=pa.schema(new_fields))


def _is_nested(arrow_type: pa.DataType) -> bool:
    """True for struct, list, large_list, fixed_size_list, map types."""
    return (
        pa.types.is_struct(arrow_type)
        or pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_map(arrow_type)
    )
