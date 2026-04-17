"""Custom dlt source for MongoDB using pymongo.

BSON type handling (E9): ``_normalize_bson_types`` walks documents
recursively, converting ``ObjectId`` / ``Decimal128`` / ``bytes`` at any
depth — not just top-level. Without this, nested BSON types reach dlt's
JSON encoder as unknown objects and either fail serialization or serialize
to incomplete string representations (e.g. ``"ObjectId('...')"`` instead
of the hex string).

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


def _make_collection_resource(db, collection_name, batch_size):
    """Create a named dlt resource for a single MongoDB collection."""

    @dlt.resource(name=collection_name, write_disposition="replace")
    def _resource():
        collection = db[collection_name]
        batch = []
        for doc in collection.find():
            batch.append(_normalize_bson_types(doc))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    return _resource


@dlt.source
def mongodb_source(connection_uri, database, collection_names=None, batch_size=DEFAULT_BATCH_SIZE):
    """Extract collections from a MongoDB database.

    Args:
        connection_uri: MongoDB connection URI string.
        database: Name of the database to extract from.
        collection_names: Optional list of collection names. If None, all collections.
        batch_size: Number of documents per yielded batch.
    """
    client = MongoClient(connection_uri)
    db = client[database]
    collections = collection_names or db.list_collection_names()
    for name in collections:
        yield _make_collection_resource(db, name, batch_size)
