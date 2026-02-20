"""Tests for MongoDB dlt source — mocks pymongo to avoid real connections."""

from unittest.mock import MagicMock, patch

from bson import ObjectId


class TestMongodbSource:
    @patch("datanika.services.mongodb_source.MongoClient")
    def test_yields_resource_per_collection(self, mock_client_cls):
        from datanika.services.mongodb_source import mongodb_source

        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ["users", "orders"]
        mock_db.__getitem__ = MagicMock(return_value=MagicMock())
        mock_db["users"].find.return_value = []
        mock_db["orders"].find.return_value = []

        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        source = mongodb_source("mongodb://localhost:27017/testdb", "testdb")
        resources = list(source.resources.keys())
        assert len(resources) == 2
        assert "users" in resources
        assert "orders" in resources

    @patch("datanika.services.mongodb_source.MongoClient")
    def test_object_id_converted_to_string(self, mock_client_cls):
        from datanika.services.mongodb_source import mongodb_source

        oid = ObjectId("507f1f77bcf86cd799439011")
        mock_collection = MagicMock()
        mock_collection.find.return_value = [{"_id": oid, "name": "Alice"}]

        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ["users"]
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        source = mongodb_source("mongodb://localhost:27017/testdb", "testdb")
        resource = source.resources["users"]
        # dlt flattens yielded batches into individual items
        items = list(resource)
        assert len(items) == 1
        doc = items[0]
        assert doc["_id"] == "507f1f77bcf86cd799439011"
        assert isinstance(doc["_id"], str)
        assert doc["name"] == "Alice"

    @patch("datanika.services.mongodb_source.MongoClient")
    def test_batch_yielding(self, mock_client_cls):
        from datanika.services.mongodb_source import mongodb_source

        docs = [{"_id": f"id_{i}", "val": i} for i in range(5)]
        mock_collection = MagicMock()
        mock_collection.find.return_value = docs

        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ["items"]
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        source = mongodb_source(
            "mongodb://localhost:27017/testdb", "testdb", batch_size=2
        )
        resource = source.resources["items"]
        # dlt flattens batches — all 5 docs arrive as individual items
        items = list(resource)
        assert len(items) == 5
        assert items[0]["_id"] == "id_0"
        assert items[4]["_id"] == "id_4"

    @patch("datanika.services.mongodb_source.MongoClient")
    def test_collection_names_filter(self, mock_client_cls):
        from datanika.services.mongodb_source import mongodb_source

        mock_collection = MagicMock()
        mock_collection.find.return_value = []

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        source = mongodb_source(
            "mongodb://localhost:27017/testdb",
            "testdb",
            collection_names=["orders"],
        )
        resources = list(source.resources.keys())
        assert resources == ["orders"]
        # list_collection_names should NOT be called when collection_names provided
        mock_db.list_collection_names.assert_not_called()

    @patch("datanika.services.mongodb_source.MongoClient")
    def test_empty_collection_yields_nothing(self, mock_client_cls):
        from datanika.services.mongodb_source import mongodb_source

        mock_collection = MagicMock()
        mock_collection.find.return_value = []

        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ["empty"]
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        source = mongodb_source("mongodb://localhost:27017/testdb", "testdb")
        resource = source.resources["empty"]
        batches = list(resource)
        assert batches == []


class TestConvertObjectIds:
    def test_converts_object_id_values(self):
        from datanika.services.mongodb_source import _convert_object_ids

        oid = ObjectId("507f1f77bcf86cd799439011")
        result = _convert_object_ids({"_id": oid, "name": "test", "count": 42})
        assert result["_id"] == "507f1f77bcf86cd799439011"
        assert result["name"] == "test"
        assert result["count"] == 42

    def test_preserves_non_objectid_types(self):
        from datanika.services.mongodb_source import _convert_object_ids

        doc = {"a": 1, "b": "hello", "c": [1, 2], "d": None, "e": True}
        result = _convert_object_ids(doc)
        assert result == doc
