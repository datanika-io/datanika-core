"""Custom dlt source for MongoDB using pymongo.

BSON type handling (E9): ``_normalize_bson_types`` walks documents
recursively, converting ``ObjectId`` / ``Decimal128`` / ``bytes`` at any
depth — not just top-level. Without this, nested BSON types reach dlt's
JSON encoder as unknown objects and either fail serialization or serialize
to incomplete string representations (e.g. ``"ObjectId('...')"`` instead
of the hex string).

Server-side filter + incremental pushdown (E11): ``query`` is passed to
``collection.find(filter=query)`` verbatim, and ``incremental_cursor``
translates a cursor field + last value into a ``$gt`` match that's merged
into the same filter. Before this, every run did ``collection.find()``
and pulled the whole collection — bare extracts on >100k-doc collections
were painful.

Upstream swap candidate: ``dlt-mongodb`` verified source exists as of
dlt 1.21+. We ship this custom one because it predates the upstream
version being stable. Re-evaluate on next dlt bump.
"""

from __future__ import annotations

from typing import Any

import dlt
from bson import Decimal128, ObjectId
from pymongo import MongoClient

DEFAULT_BATCH_SIZE = 10_000


def _normalize_bson_types(value: Any) -> Any:
    """Recursively convert BSON types to JSON-safe primitives.

    - ``ObjectId`` → str (hex string)
    - ``Decimal128`` → str (preserves precision, downstream can cast)
    - ``bytes`` → hex string
    - ``dict`` / ``list`` → recurse
    - everything else → as-is (datetime, int, float, bool, str, None)
    """
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Decimal128):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {k: _normalize_bson_types(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_bson_types(item) for item in value]
    return value


def _build_mongo_filter(
    query: dict | None,
    incremental_cursor: tuple[str, Any] | None,
) -> dict:
    """Merge user query + incremental cursor into a single Mongo find() filter.

    - ``query``: verbatim dict passed by the caller (e.g. ``{"price": {"$gt": 100}}``).
    - ``incremental_cursor``: ``(cursor_path, last_value)`` translated into
      ``{cursor_path: {"$gt": last_value}}`` and merged with ``query`` via ``$and``.

    If ``cursor_path`` is already a key in ``query``, we wrap with ``$and``
    to preserve both conditions instead of overwriting.
    """
    filter_dict: dict = dict(query) if query else {}

    if incremental_cursor is not None:
        cursor_path, last_value = incremental_cursor
        cursor_cond = {cursor_path: {"$gt": last_value}}

        if cursor_path in filter_dict:
            # Avoid clobbering user-supplied condition on the same field.
            filter_dict = {"$and": [filter_dict, cursor_cond]}
        else:
            filter_dict.update(cursor_cond)

    return filter_dict


def _make_collection_resource(
    db,
    collection_name: str,
    batch_size: int,
    query: dict | None = None,
    incremental_cursor: tuple[str, Any] | None = None,
):
    """Create a named dlt resource for a single MongoDB collection.

    ``query`` and ``incremental_cursor`` are pushed down to
    ``collection.find(filter=...)`` — the server does the filtering,
    we don't pull then drop.
    """
    mongo_filter = _build_mongo_filter(query, incremental_cursor)

    @dlt.resource(name=collection_name, write_disposition="replace")
    def _resource():
        collection = db[collection_name]
        batch = []
        for doc in collection.find(filter=mongo_filter):
            batch.append(_normalize_bson_types(doc))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    return _resource


@dlt.source
def mongodb_source(
    connection_uri: str,
    database: str,
    collection_names: list[str] | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    query: dict | None = None,
    incremental_cursor: tuple[str, Any] | None = None,
):
    """Extract collections from a MongoDB database.

    Args:
        connection_uri: MongoDB connection URI string.
        database: Name of the database to extract from.
        collection_names: Optional list of collection names. If None, all collections.
        batch_size: Number of documents per yielded batch.
        query: Optional Mongo find() filter dict (e.g. ``{"price": {"$gt": 100}}``).
            Applied to every collection extracted. For collection-specific
            queries, call the source once per collection.
        incremental_cursor: Optional ``(cursor_path, last_value)`` tuple that
            translates to ``{cursor_path: {"$gt": last_value}}`` and merges
            with ``query``. Mirrors the sql_table incremental behaviour.
    """
    client = MongoClient(connection_uri)
    db = client[database]
    collections = collection_names or db.list_collection_names()
    for name in collections:
        yield _make_collection_resource(
            db,
            name,
            batch_size,
            query=query,
            incremental_cursor=incremental_cursor,
        )
