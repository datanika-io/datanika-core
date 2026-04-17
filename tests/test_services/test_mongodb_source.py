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

        source = mongodb_source("mongodb://localhost:27017/testdb", "testdb", batch_size=2)
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


class TestNormalizeBsonTypes:
    def test_converts_object_id_values(self):
        from datanika.services.mongodb_source import _normalize_bson_types

        oid = ObjectId("507f1f77bcf86cd799439011")
        result = _normalize_bson_types({"_id": oid, "name": "test", "count": 42})
        assert result["_id"] == "507f1f77bcf86cd799439011"
        assert result["name"] == "test"
        assert result["count"] == 42

    def test_preserves_non_objectid_types(self):
        from datanika.services.mongodb_source import _normalize_bson_types

        doc = {"a": 1, "b": "hello", "c": [1, 2], "d": None, "e": True}
        result = _normalize_bson_types(doc)
        assert result == doc


class TestBuildMongoFilter:
    """E11 — _build_mongo_filter merges query + incremental cursor."""

    def test_empty_returns_empty_dict(self):
        from datanika.services.mongodb_source import _build_mongo_filter

        assert _build_mongo_filter(None, None) == {}

    def test_query_only(self):
        from datanika.services.mongodb_source import _build_mongo_filter

        result = _build_mongo_filter({"price": {"$gt": 100}}, None)
        assert result == {"price": {"$gt": 100}}

    def test_incremental_only(self):
        from datanika.services.mongodb_source import _build_mongo_filter

        result = _build_mongo_filter(None, ("updated_at", "2024-01-01"))
        assert result == {"updated_at": {"$gt": "2024-01-01"}}

    def test_query_and_incremental_merged(self):
        from datanika.services.mongodb_source import _build_mongo_filter

        result = _build_mongo_filter({"status": "active"}, ("updated_at", "2024-01-01"))
        assert result == {"status": "active", "updated_at": {"$gt": "2024-01-01"}}

    def test_cursor_overlaps_query_field_uses_and(self):
        """If the cursor field is already in the query, wrap with $and so neither gets clobbered."""
        from datanika.services.mongodb_source import _build_mongo_filter

        result = _build_mongo_filter(
            {"updated_at": {"$lt": "2025-01-01"}},
            ("updated_at", "2024-01-01"),
        )
        assert result == {
            "$and": [
                {"updated_at": {"$lt": "2025-01-01"}},
                {"updated_at": {"$gt": "2024-01-01"}},
            ]
        }


class TestMongoFilterPushdown:
    """E11 — filter/incremental reach collection.find() as the filter arg."""

    @patch("datanika.services.mongodb_source.MongoClient")
    def test_query_passed_to_find_verbatim(self, mock_client_cls):
        from datanika.services.mongodb_source import mongodb_source

        mock_collection = MagicMock()
        mock_collection.find.return_value = []
        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ["orders"]
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        source = mongodb_source(
            "mongodb://localhost:27017/testdb",
            "testdb",
            query={"price": {"$gt": 100}},
        )
        list(source.resources["orders"])
        # collection.find(filter=...) — verify the filter dict is exact
        mock_collection.find.assert_called_once_with(filter={"price": {"$gt": 100}})

    @patch("datanika.services.mongodb_source.MongoClient")
    def test_incremental_cursor_becomes_gt(self, mock_client_cls):
        from datanika.services.mongodb_source import mongodb_source

        mock_collection = MagicMock()
        mock_collection.find.return_value = []
        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ["orders"]
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        source = mongodb_source(
            "mongodb://localhost:27017/testdb",
            "testdb",
            incremental_cursor=("updated_at", "2024-01-01"),
        )
        list(source.resources["orders"])
        mock_collection.find.assert_called_once_with(filter={"updated_at": {"$gt": "2024-01-01"}})

    @patch("datanika.services.mongodb_source.MongoClient")
    def test_no_query_no_incremental_sends_empty_filter(self, mock_client_cls):
        from datanika.services.mongodb_source import mongodb_source

        mock_collection = MagicMock()
        mock_collection.find.return_value = []
        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = ["orders"]
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_cls.return_value = mock_client

        source = mongodb_source("mongodb://localhost:27017/testdb", "testdb")
        list(source.resources["orders"])
        # Default filter is {} — empty, not None — so pymongo treats it as match-all
        mock_collection.find.assert_called_once_with(filter={})


class TestDltRunnerMongoPushdownWiring:
    """_build_mongodb_source wires dlt_config.query and .incremental through."""

    def test_query_forwarded(self):
        from unittest.mock import MagicMock, patch

        from datanika.services.dlt_runner import DltRunnerService

        runner = DltRunnerService()

        with patch("datanika.services.mongodb_source.mongodb_source") as mock_src:
            mock_src.return_value = MagicMock()
            runner.build_source(
                "mongodb",
                {"host": "h", "port": 27017, "database": "d"},
                {"query": {"status": "active"}},
                batch_size=1000,
            )
            kwargs = mock_src.call_args.kwargs
            assert kwargs["query"] == {"status": "active"}

    def test_incremental_forwarded_as_tuple(self):
        from unittest.mock import MagicMock, patch

        from datanika.services.dlt_runner import DltRunnerService

        runner = DltRunnerService()

        with patch("datanika.services.mongodb_source.mongodb_source") as mock_src:
            mock_src.return_value = MagicMock()
            runner.build_source(
                "mongodb",
                {"host": "h", "port": 27017, "database": "d"},
                {"incremental": {"cursor_path": "updated_at", "initial_value": "2024-01-01"}},
                batch_size=1000,
            )
            kwargs = mock_src.call_args.kwargs
            assert kwargs["incremental_cursor"] == ("updated_at", "2024-01-01")

    def test_no_filter_no_incremental_yields_none(self):
        from unittest.mock import MagicMock, patch

        from datanika.services.dlt_runner import DltRunnerService

        runner = DltRunnerService()

        with patch("datanika.services.mongodb_source.mongodb_source") as mock_src:
            mock_src.return_value = MagicMock()
            runner.build_source(
                "mongodb",
                {"host": "h", "port": 27017, "database": "d"},
                {},
                batch_size=1000,
            )
            kwargs = mock_src.call_args.kwargs
            assert kwargs["query"] is None
            assert kwargs["incremental_cursor"] is None
