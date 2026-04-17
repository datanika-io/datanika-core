"""E10 — SQL filter pushdown via query_adapter_callback."""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from datanika.services._sql_filter_pushdown import (
    FilterPushdownError,
    make_query_adapter,
)


@pytest.fixture
def sample_table():
    """A SQLAlchemy Table we can build WHERE clauses against."""
    metadata = sa.MetaData()
    return sa.Table(
        "orders",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created_at", sa.DateTime),
        sa.Column("status", sa.String),
        sa.Column("amount", sa.Numeric),
    )


def _compile(query, dialect_name="postgresql"):
    """Compile a SQLAlchemy query to a literal-bound SQL string for assertion."""
    dialect = sa.dialects.registry.load(dialect_name)()
    return str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


class TestMakeQueryAdapter:
    def test_no_filters_returns_query_unchanged(self, sample_table):
        adapter = make_query_adapter([])
        original = sample_table.select()
        assert adapter(original, sample_table) is original

    def test_single_eq_filter(self, sample_table):
        adapter = make_query_adapter([{"op": "eq", "column": "status", "value": "active"}])
        query = adapter(sample_table.select(), sample_table)
        sql = _compile(query).lower()
        assert "where" in sql
        assert "status = 'active'" in sql

    def test_gt_filter_translates_to_greater_than(self, sample_table):
        adapter = make_query_adapter([{"op": "gt", "column": "amount", "value": 100}])
        query = adapter(sample_table.select(), sample_table)
        sql = _compile(query).lower()
        assert "amount > 100" in sql

    def test_gte_lt_lte_ne(self, sample_table):
        for op, sql_op in [("gte", ">="), ("lt", "<"), ("lte", "<="), ("ne", "!=")]:
            adapter = make_query_adapter([{"op": op, "column": "id", "value": 42}])
            query = adapter(sample_table.select(), sample_table)
            sql = _compile(query).lower()
            assert f"id {sql_op} 42" in sql, f"op {op} did not produce {sql_op}"

    def test_in_filter(self, sample_table):
        adapter = make_query_adapter(
            [{"op": "in", "column": "status", "value": ["active", "paid"]}]
        )
        query = adapter(sample_table.select(), sample_table)
        sql = _compile(query).lower()
        assert "status in " in sql
        assert "'active'" in sql
        assert "'paid'" in sql

    def test_not_in_filter(self, sample_table):
        adapter = make_query_adapter([{"op": "not_in", "column": "status", "value": ["cancelled"]}])
        query = adapter(sample_table.select(), sample_table)
        sql = _compile(query).lower()
        # SQLAlchemy renders NOT IN as a single "not in" operator
        assert "not in" in sql
        assert "'cancelled'" in sql

    def test_multiple_filters_combine_with_and(self, sample_table):
        adapter = make_query_adapter(
            [
                {"op": "eq", "column": "status", "value": "active"},
                {"op": "gt", "column": "amount", "value": 100},
            ]
        )
        query = adapter(sample_table.select(), sample_table)
        sql = _compile(query).lower()
        assert "where" in sql
        assert "status = 'active'" in sql
        assert "amount > 100" in sql
        assert " and " in sql

    def test_filter_composes_with_existing_where(self, sample_table):
        """Incremental already added a WHERE — our filter ANDs on top."""
        adapter = make_query_adapter([{"op": "eq", "column": "status", "value": "active"}])
        query_with_incremental = sample_table.select().where(sample_table.c.id > 1000)
        query = adapter(query_with_incremental, sample_table)
        sql = _compile(query).lower()
        # Both conditions present
        assert "id > 1000" in sql
        assert "status = 'active'" in sql
        assert " and " in sql

    def test_unknown_column_raises(self, sample_table):
        adapter = make_query_adapter([{"op": "eq", "column": "nonexistent", "value": "x"}])
        with pytest.raises(FilterPushdownError, match="not found"):
            adapter(sample_table.select(), sample_table)

    def test_unknown_op_raises(self, sample_table):
        adapter = make_query_adapter([{"op": "like", "column": "status", "value": "%active%"}])
        with pytest.raises(FilterPushdownError, match="unsupported filter op"):
            adapter(sample_table.select(), sample_table)


class TestDltRunnerPushdownWiring:
    """DltRunnerService.build_source passes query_adapter_callback for SQL."""

    def test_single_table_with_filters_passes_query_adapter(self):
        from unittest.mock import patch

        from datanika.services.dlt_runner import DltRunnerService

        runner = DltRunnerService()

        with patch("datanika.services.dlt_runner.sql_table") as mock_sql_table:
            mock_sql_table.return_value = object()
            runner.build_source(
                "postgres",
                {"host": "h", "port": 5432, "user": "u", "password": "p", "database": "d"},
                {
                    "mode": "single_table",
                    "table": "orders",
                    "filters": [{"op": "gt", "column": "created_at", "value": "2024-01-01"}],
                },
                batch_size=1000,
            )
            kwargs = mock_sql_table.call_args.kwargs
            assert callable(kwargs.get("query_adapter_callback"))

    def test_single_table_no_filters_no_adapter(self):
        from unittest.mock import patch

        from datanika.services.dlt_runner import DltRunnerService

        runner = DltRunnerService()

        with patch("datanika.services.dlt_runner.sql_table") as mock_sql_table:
            mock_sql_table.return_value = object()
            runner.build_source(
                "postgres",
                {"host": "h", "port": 5432, "user": "u", "password": "p", "database": "d"},
                {"mode": "single_table", "table": "orders"},
                batch_size=1000,
            )
            kwargs = mock_sql_table.call_args.kwargs
            assert "query_adapter_callback" not in kwargs

    def test_full_database_filters_stay_client_side(self):
        """full_database mode doesn't get pushdown — filters apply in memory."""
        from unittest.mock import patch

        from datanika.services.dlt_runner import DltRunnerService

        runner = DltRunnerService()

        with patch("datanika.services.dlt_runner.sql_database") as mock_sql_db:
            mock_sql_db.return_value = object()
            runner.build_source(
                "postgres",
                {"host": "h", "port": 5432, "user": "u", "password": "p", "database": "d"},
                {
                    "mode": "full_database",
                    "filters": [{"op": "gt", "column": "created_at", "value": "2024-01-01"}],
                },
                batch_size=1000,
            )
            kwargs = mock_sql_db.call_args.kwargs
            # full_database mode — no per-table pushdown since a callback
            # binds to a single table.
            assert "query_adapter_callback" not in kwargs
