"""Custom dlt source for MongoDB using pymongo."""

import dlt
from bson import ObjectId
from pymongo import MongoClient

DEFAULT_BATCH_SIZE = 10_000


def _convert_object_ids(doc: dict) -> dict:
    """Convert ObjectId fields to strings for JSON serialization."""
    return {k: str(v) if isinstance(v, ObjectId) else v for k, v in doc.items()}


def _make_collection_resource(db, collection_name, batch_size):
    """Create a named dlt resource for a single MongoDB collection."""

    @dlt.resource(name=collection_name, write_disposition="replace")
    def _resource():
        collection = db[collection_name]
        batch = []
        for doc in collection.find():
            batch.append(_convert_object_ids(doc))
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
