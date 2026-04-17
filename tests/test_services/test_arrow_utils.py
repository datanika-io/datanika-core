"""E9 — stringify_nested_columns + orjson/bson fallback."""

from __future__ import annotations

import pyarrow as pa

from datanika.services.destinations._arrow_utils import stringify_nested_columns


class TestStringifyNestedColumns:
    def test_scalar_columns_pass_through(self):
        table = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "name": pa.array(["a", "b", "c"], type=pa.string()),
                "active": pa.array([True, False, True], type=pa.bool_()),
            }
        )
        result = stringify_nested_columns(table)
        assert result.schema == table.schema
        assert result.equals(table)

    def test_struct_column_becomes_string(self):
        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "payload": pa.array(
                    [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}],
                    type=pa.struct([("a", pa.int64()), ("b", pa.string())]),
                ),
            }
        )
        result = stringify_nested_columns(table)
        assert pa.types.is_string(result.schema.field("payload").type)
        # Each row is a JSON-parseable string.
        import json

        values = result.column("payload").to_pylist()
        assert json.loads(values[0]) == {"a": 1, "b": "x"}
        assert json.loads(values[1]) == {"a": 2, "b": "y"}

    def test_list_column_becomes_string(self):
        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "tags": pa.array([["a", "b"], ["c"]], type=pa.list_(pa.string())),
            }
        )
        result = stringify_nested_columns(table)
        assert pa.types.is_string(result.schema.field("tags").type)
        import json

        values = result.column("tags").to_pylist()
        assert json.loads(values[0]) == ["a", "b"]
        assert json.loads(values[1]) == ["c"]

    def test_nested_null_values_preserved(self):
        table = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "payload": pa.array(
                    [{"a": 1}, None, {"a": 3}],
                    type=pa.struct([("a", pa.int64())]),
                ),
            }
        )
        result = stringify_nested_columns(table)
        values = result.column("payload").to_pylist()
        assert values[1] is None

    def test_scalar_and_nested_mixed(self):
        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "name": pa.array(["x", "y"], type=pa.string()),
                "payload": pa.array(
                    [{"a": 1}, {"a": 2}],
                    type=pa.struct([("a", pa.int64())]),
                ),
                "count": pa.array([10, 20], type=pa.int32()),
            }
        )
        result = stringify_nested_columns(table)
        # Scalar columns preserve their exact type.
        assert result.schema.field("id").type == pa.int64()
        assert result.schema.field("name").type == pa.string()
        assert result.schema.field("count").type == pa.int32()
        # Nested becomes string.
        assert pa.types.is_string(result.schema.field("payload").type)

    def test_empty_table(self):
        table = pa.table(
            {
                "payload": pa.array([], type=pa.struct([("a", pa.int64())])),
            }
        )
        result = stringify_nested_columns(table)
        assert pa.types.is_string(result.schema.field("payload").type)
        assert result.num_rows == 0

    def test_bytes_field_orjson_hex_encodes(self):
        """bytes inside a struct trigger orjson's default (hex encoding)."""
        import json

        # bytes fields reach stringify_nested_columns via Arrow's binary type
        # inside a struct — the orjson default handles them as hex.
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "payload": pa.array(
                    [{"blob": b"\x01\x02\x03"}],
                    type=pa.struct([("blob", pa.binary())]),
                ),
            }
        )
        result = stringify_nested_columns(table)
        value = result.column("payload").to_pylist()[0]
        # orjson serializes the struct; bytes → hex string via _orjson_default.
        parsed = json.loads(value)
        assert parsed == {"blob": "010203"}


class TestMongoBsonNormalization:
    """E9 — _normalize_bson_types walks nested BSON types."""

    def test_top_level_object_id(self):
        from bson import ObjectId

        from datanika.services.mongodb_source import _normalize_bson_types

        doc = {"_id": ObjectId("507f1f77bcf86cd799439011"), "name": "x"}
        result = _normalize_bson_types(doc)
        assert result == {"_id": "507f1f77bcf86cd799439011", "name": "x"}

    def test_nested_object_id_in_dict(self):
        from bson import ObjectId

        from datanika.services.mongodb_source import _normalize_bson_types

        nested_oid = ObjectId("507f1f77bcf86cd799439022")
        doc = {"_id": "a", "user": {"id": nested_oid, "name": "y"}}
        result = _normalize_bson_types(doc)
        assert result["user"]["id"] == "507f1f77bcf86cd799439022"
        assert result["user"]["name"] == "y"

    def test_object_id_in_list(self):
        from bson import ObjectId

        from datanika.services.mongodb_source import _normalize_bson_types

        oid1 = ObjectId("507f1f77bcf86cd799439033")
        oid2 = ObjectId("507f1f77bcf86cd799439044")
        doc = {"ids": [oid1, oid2]}
        result = _normalize_bson_types(doc)
        assert result == {"ids": ["507f1f77bcf86cd799439033", "507f1f77bcf86cd799439044"]}

    def test_decimal128_converted(self):
        from bson import Decimal128

        from datanika.services.mongodb_source import _normalize_bson_types

        doc = {"price": Decimal128("19.99")}
        result = _normalize_bson_types(doc)
        assert result == {"price": "19.99"}

    def test_bytes_hex_encoded(self):
        from datanika.services.mongodb_source import _normalize_bson_types

        doc = {"blob": b"\x01\x02\x03"}
        result = _normalize_bson_types(doc)
        assert result == {"blob": "010203"}

    def test_datetime_unchanged(self):
        """datetime passes through — dlt handles it natively."""
        from datetime import UTC, datetime

        from datanika.services.mongodb_source import _normalize_bson_types

        dt = datetime(2026, 4, 17, tzinfo=UTC)
        doc = {"created": dt}
        result = _normalize_bson_types(doc)
        assert result == {"created": dt}

    def test_deeply_nested(self):
        from bson import ObjectId

        from datanika.services.mongodb_source import _normalize_bson_types

        oid = ObjectId("507f1f77bcf86cd799439055")
        doc = {
            "level1": {
                "level2": {
                    "level3": [
                        {"ref": oid},
                    ]
                }
            }
        }
        result = _normalize_bson_types(doc)
        assert result["level1"]["level2"]["level3"][0]["ref"] == "507f1f77bcf86cd799439055"
