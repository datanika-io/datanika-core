"""V2 P3 — IR dataclass, validator, introspection, and builder tests.

TDD: these tests define the IR contract per SPEC_ELT_IR_ARCHITECTURE.md §5.
Tests are written first; implementations follow.
"""

import pytest

from datanika.services.ir import IR_VERSION
from datanika.services.ir.schema import (
    IR,
    IRColumn,
    IRIncremental,
    IRSource,
    IRTarget,
)
from datanika.services.ir.validator import IRValidationError, validate_ir

# ---------------------------------------------------------------------------
# §5.1 — IR document shape
# ---------------------------------------------------------------------------


class TestIRDataclasses:
    """IR document is a frozen dataclass hierarchy matching spec §5.1."""

    def test_ir_column_creation(self):
        col = IRColumn(
            source_ref="id",
            target_name="id",
            type="bigint",
            nullable=False,
        )
        assert col.source_ref == "id"
        assert col.type == "bigint"
        assert col.nullable is False

    def test_ir_source_sql_table(self):
        src = IRSource(
            kind="sql_table",
            connection_id=42,
            schema="public",
            table="orders",
        )
        assert src.kind == "sql_table"
        assert src.connection_id == 42

    def test_ir_source_sql_database(self):
        src = IRSource(
            kind="sql_database",
            connection_id=42,
        )
        assert src.kind == "sql_database"
        assert src.table is None

    def test_ir_target(self):
        tgt = IRTarget(
            connection_id=17,
            raw_schema="raw",
            table="orders",
        )
        assert tgt.raw_schema == "raw"

    def test_ir_incremental(self):
        inc = IRIncremental(mode="append", cursor="created_at")
        assert inc.mode == "append"
        assert inc.cursor == "created_at"

    def test_ir_document_full(self):
        ir = IR(
            ir_version=IR_VERSION,
            source=IRSource(
                kind="sql_table",
                connection_id=42,
                schema="public",
                table="orders",
            ),
            columns=[
                IRColumn(
                    source_ref="id",
                    target_name="id",
                    type="bigint",
                    nullable=False,
                ),
                IRColumn(
                    source_ref="created_at",
                    target_name="created_at",
                    type="timestamptz",
                    nullable=False,
                ),
            ],
            primary_key=["id"],
            target=IRTarget(
                connection_id=17,
                raw_schema="raw",
                table="orders",
            ),
        )
        assert ir.ir_version == IR_VERSION
        assert len(ir.columns) == 2
        assert ir.incremental is None

    def test_ir_document_with_incremental(self):
        ir = IR(
            ir_version=IR_VERSION,
            source=IRSource(kind="sql_table", connection_id=1, table="t"),
            columns=[
                IRColumn(source_ref="id", target_name="id", type="bigint", nullable=False),
            ],
            primary_key=["id"],
            target=IRTarget(connection_id=2, raw_schema="raw", table="t"),
            incremental=IRIncremental(mode="append", cursor="id"),
        )
        assert ir.incremental.cursor == "id"

    def test_ir_to_dict_roundtrip(self):
        ir = IR(
            ir_version=IR_VERSION,
            source=IRSource(kind="sql_table", connection_id=1, table="t"),
            columns=[
                IRColumn(source_ref="id", target_name="id", type="bigint", nullable=False),
            ],
            primary_key=["id"],
            target=IRTarget(connection_id=2, raw_schema="raw", table="t"),
        )
        d = ir.to_dict()
        assert d["ir_version"] == IR_VERSION
        assert d["source"]["kind"] == "sql_table"
        assert len(d["columns"]) == 1
        restored = IR.from_dict(d)
        assert restored == ir


# ---------------------------------------------------------------------------
# §5.1 — IR validator
# ---------------------------------------------------------------------------


class TestIRValidator:
    """validate_ir() checks shape, types, and semantic constraints."""

    def _make_valid_ir(self, **overrides):
        defaults = dict(
            ir_version=IR_VERSION,
            source=IRSource(kind="sql_table", connection_id=1, table="orders"),
            columns=[
                IRColumn(source_ref="id", target_name="id", type="bigint", nullable=False),
            ],
            primary_key=["id"],
            target=IRTarget(connection_id=2, raw_schema="raw", table="orders"),
        )
        defaults.update(overrides)
        return IR(**defaults)

    def test_valid_ir_passes(self):
        ir = self._make_valid_ir()
        validate_ir(ir)  # should not raise

    def test_wrong_version_fails(self):
        ir = self._make_valid_ir(ir_version=999)
        with pytest.raises(IRValidationError, match="version"):
            validate_ir(ir)

    def test_empty_columns_fails(self):
        ir = self._make_valid_ir(columns=[])
        with pytest.raises(IRValidationError, match="column"):
            validate_ir(ir)

    def test_duplicate_target_names_fails(self):
        cols = [
            IRColumn(source_ref="a", target_name="x", type="text", nullable=True),
            IRColumn(source_ref="b", target_name="x", type="text", nullable=True),
        ]
        ir = self._make_valid_ir(columns=cols)
        with pytest.raises(IRValidationError, match="duplicate"):
            validate_ir(ir)

    def test_primary_key_not_in_columns_fails(self):
        ir = self._make_valid_ir(primary_key=["nonexistent"])
        with pytest.raises(IRValidationError, match="primary_key"):
            validate_ir(ir)

    def test_sql_table_without_table_fails(self):
        ir = self._make_valid_ir(
            source=IRSource(kind="sql_table", connection_id=1, table=None),
        )
        with pytest.raises(IRValidationError, match="table"):
            validate_ir(ir)

    def test_incremental_cursor_not_in_columns_fails(self):
        ir = self._make_valid_ir(
            incremental=IRIncremental(mode="append", cursor="nonexistent"),
        )
        with pytest.raises(IRValidationError, match="cursor"):
            validate_ir(ir)

    def test_incremental_cursor_in_columns_passes(self):
        cols = [
            IRColumn(source_ref="id", target_name="id", type="bigint", nullable=False),
            IRColumn(
                source_ref="created_at",
                target_name="created_at",
                type="timestamptz",
                nullable=False,
            ),
        ]
        ir = self._make_valid_ir(
            columns=cols,
            primary_key=["id"],
            incremental=IRIncremental(mode="append", cursor="created_at"),
        )
        validate_ir(ir)  # should not raise


# ---------------------------------------------------------------------------
# §5.2 / §9 P4 — SaaS source IR builder
# ---------------------------------------------------------------------------


class TestSaaSIRBuilder:
    """V2 P4 — build_ir() for SaaS sources via dlt schema discovery."""

    def test_build_ir_saas_with_mock_source(self):
        """SaaS sources build IR from dlt resource.compute_table_schema()."""
        from unittest.mock import MagicMock, patch

        from datanika.services.ir.builder import build_ir
        from datanika.services.ir.schema import IR

        # Mock a dlt source with one resource that has column hints
        mock_resource = MagicMock()
        mock_resource.table_name = "customers"
        mock_resource.compute_table_schema.return_value = {
            "name": "customers",
            "columns": {
                "id": {"name": "id", "data_type": "text", "nullable": False},
                "email": {"name": "email", "data_type": "text", "nullable": True},
                "created": {"name": "created", "data_type": "bigint", "nullable": True},
            },
        }

        mock_source = MagicMock()
        mock_source.resources = {"customers": mock_resource}

        with patch(
            "datanika.services.ir.builder._build_dlt_source",
            return_value=mock_source,
        ):
            ir = build_ir(
                source_type="stripe",
                source_config={"api_key": "sk_test_fake"},
                destination_connection_id=2,
                table="customers",
            )

        assert isinstance(ir, IR)
        assert ir.source.kind == "saas_rest"
        assert len(ir.columns) == 3
        assert ir.columns[0].source_ref == "id"
        assert ir.columns[0].type == "text"
        assert ir.target.table == "customers"

    def test_build_ir_saas_resource_not_found(self):
        """Raises IRBuildError if the requested resource doesn't exist in the source."""
        from unittest.mock import MagicMock, patch

        from datanika.services.ir.builder import IRBuildError, build_ir

        mock_source = MagicMock()
        mock_source.resources = {}

        with (
            patch(
                "datanika.services.ir.builder._build_dlt_source",
                return_value=mock_source,
            ),
            pytest.raises(IRBuildError, match="not found"),
        ):
            build_ir(
                source_type="stripe",
                source_config={"api_key": "sk_test_fake"},
                destination_connection_id=2,
                table="nonexistent_resource",
            )

    def test_build_ir_saas_no_columns_in_schema(self):
        """Raises IRBuildError if the resource schema has no columns."""
        from unittest.mock import MagicMock, patch

        from datanika.services.ir.builder import IRBuildError, build_ir

        mock_resource = MagicMock()
        mock_resource.table_name = "empty"
        mock_resource.compute_table_schema.return_value = {
            "name": "empty",
            "columns": {},
        }

        mock_source = MagicMock()
        mock_source.resources = {"empty": mock_resource}

        with (
            patch(
                "datanika.services.ir.builder._build_dlt_source",
                return_value=mock_source,
            ),
            pytest.raises(IRBuildError, match="No columns"),
        ):
            build_ir(
                source_type="stripe",
                source_config={"api_key": "sk_test_fake"},
                destination_connection_id=2,
                table="empty",
            )

    def test_file_sources_still_raise(self):
        """File sources (csv, json, parquet, s3) are not yet supported."""
        from datanika.services.ir.builder import IRBuildError, build_ir

        for file_type in ("csv", "json", "parquet", "s3"):
            with pytest.raises(IRBuildError, match="not supported"):
                build_ir(
                    source_type=file_type,
                    source_config={},
                    destination_connection_id=1,
                    table="t",
                )


# ---------------------------------------------------------------------------
# §5.5 — StreamStats
# ---------------------------------------------------------------------------


class TestStreamStats:
    def test_stream_stats_creation(self):
        from datanika.services.elt_runner import StreamStats

        stats = StreamStats(
            rows=1000,
            bytes_in=50_000,
            bytes_out=15_000,
            batches=2,
            duration_ms=1234,
        )
        assert stats.rows == 1000
        assert stats.bytes_out == 15_000
