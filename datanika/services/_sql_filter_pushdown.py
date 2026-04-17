"""SQL filter pushdown — translate Datanika ``FILTER_OPS`` to SQLAlchemy WHERE.

E10 — makes ``filters:`` config server-side for SQL sources. Before this,
``source.add_filter(fn)`` dropped rows in memory AFTER the source yielded,
so we pulled the whole table over the network and discarded 99% of it.

Surface: one factory ``make_query_adapter(filters)`` returns a callable
matching dlt's ``query_adapter_callback`` contract. Sources in the SQL
path pass it straight to ``sql_table``/``sql_database``.

Non-SQL sources (REST, Mongo, SaaS, Kafka) continue to use the existing
in-memory ``source.add_filter`` path — see ``DltRunnerService.execute``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import sqlalchemy as sa


class FilterPushdownError(ValueError):
    """Raised when a filter spec can't be translated to SQL."""


def _filter_to_where(table: sa.Table, op: str, column: str, value: Any) -> sa.ColumnElement:
    """Translate a single ``{op, column, value}`` spec to a SQLAlchemy WHERE."""
    if column not in table.c:
        raise FilterPushdownError(f"filter column {column!r} not found on table {table.name!r}")
    col = table.c[column]

    if op == "eq":
        return col == value
    if op == "ne":
        return col != value
    if op == "gt":
        return col > value
    if op == "gte":
        return col >= value
    if op == "lt":
        return col < value
    if op == "lte":
        return col <= value
    if op == "in":
        return col.in_(value)
    if op == "not_in":
        return sa.not_(col.in_(value))
    raise FilterPushdownError(f"unsupported filter op {op!r}")


def make_query_adapter(filters: list[dict]) -> Callable:
    """Build a ``query_adapter_callback`` that applies ``filters`` as WHERE.

    Contract matches dlt's ``TableLoader.make_query`` call site:
    the callback receives ``(query, table)`` and returns a modified query.

    dlt preserves any WHERE already added by ``incremental`` — we ``.where()``
    on top of it, which SQLAlchemy translates to ``AND``. Filters compose
    freely with incremental cursors.
    """

    def adapter(query, table):
        wheres = [_filter_to_where(table, f["op"], f["column"], f["value"]) for f in filters]
        if not wheres:
            return query
        if len(wheres) == 1:
            return query.where(wheres[0])
        return query.where(sa.and_(*wheres))

    return adapter
