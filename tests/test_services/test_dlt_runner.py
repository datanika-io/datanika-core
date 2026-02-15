"""TDD tests for DltRunnerService — dlt pipeline/source/destination factory."""

from unittest.mock import MagicMock, patch

import pytest

from etlfabric.services.dlt_runner import (
    DEFAULT_BATCH_SIZE,
    INTERNAL_CONFIG_KEYS,
    DltRunnerError,
    DltRunnerService,
)


@pytest.fixture
def svc():
    return DltRunnerService()


# ---------------------------------------------------------------------------
# DltRunnerError
# ---------------------------------------------------------------------------
class TestDltRunnerError:
    def test_is_value_error_subclass(self):
        assert issubclass(DltRunnerError, ValueError)

    def test_message_preserved(self):
        err = DltRunnerError("bad type")
        assert str(err) == "bad type"


# ---------------------------------------------------------------------------
# build_destination
# ---------------------------------------------------------------------------
class TestBuildDestination:
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_postgres_returns_destination(self, mock_dlt, svc):
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        result = svc.build_destination("postgres", {"host": "localhost"})
        mock_dlt.destinations.postgres.assert_called_once_with(credentials={"host": "localhost"})
        assert result == "pg_dest"

    @patch("etlfabric.services.dlt_runner.dlt")
    def test_mysql_returns_destination(self, mock_dlt, svc):
        mock_dlt.destinations.mysql.return_value = "mysql_dest"
        result = svc.build_destination("mysql", {"host": "localhost"})
        mock_dlt.destinations.mysql.assert_called_once_with(credentials={"host": "localhost"})
        assert result == "mysql_dest"

    @patch("etlfabric.services.dlt_runner.dlt")
    def test_mssql_returns_destination(self, mock_dlt, svc):
        mock_dlt.destinations.mssql.return_value = "mssql_dest"
        result = svc.build_destination("mssql", {"host": "localhost"})
        mock_dlt.destinations.mssql.assert_called_once_with(credentials={"host": "localhost"})
        assert result == "mssql_dest"

    @patch("etlfabric.services.dlt_runner.dlt")
    def test_sqlite_returns_destination(self, mock_dlt, svc):
        mock_dlt.destinations.sqlite.return_value = "sqlite_dest"
        result = svc.build_destination("sqlite", {"path": "db.sqlite"})
        mock_dlt.destinations.sqlite.assert_called_once_with(credentials={"path": "db.sqlite"})
        assert result == "sqlite_dest"

    def test_unsupported_type_raises(self, svc):
        with pytest.raises(DltRunnerError, match="Unsupported destination type"):
            svc.build_destination("oracle", {})


# ---------------------------------------------------------------------------
# build_source
# ---------------------------------------------------------------------------
class TestBuildSource:
    @patch("etlfabric.services.dlt_runner.sql_database")
    def test_postgres_source(self, mock_sql_db, svc):
        mock_sql_db.return_value = "pg_source"
        result = svc.build_source("postgres", {"host": "localhost"}, {})
        mock_sql_db.assert_called_once()
        assert result == "pg_source"

    @patch("etlfabric.services.dlt_runner.sql_database")
    def test_mysql_source(self, mock_sql_db, svc):
        mock_sql_db.return_value = "mysql_source"
        result = svc.build_source("mysql", {"host": "localhost"}, {})
        assert result == "mysql_source"

    @patch("etlfabric.services.dlt_runner.sql_database")
    def test_mssql_source(self, mock_sql_db, svc):
        mock_sql_db.return_value = "mssql_source"
        result = svc.build_source("mssql", {"host": "localhost"}, {})
        assert result == "mssql_source"

    @patch("etlfabric.services.dlt_runner.sql_database")
    def test_sqlite_source(self, mock_sql_db, svc):
        mock_sql_db.return_value = "sqlite_source"
        result = svc.build_source("sqlite", {"path": "db.sqlite"}, {})
        assert result == "sqlite_source"

    def test_unsupported_type_raises(self, svc):
        with pytest.raises(DltRunnerError, match="Unsupported source type"):
            svc.build_source("oracle", {}, {})

    @patch("etlfabric.services.dlt_runner.sql_database")
    def test_passes_chunk_size(self, mock_sql_db, svc):
        svc.build_source("postgres", {"host": "localhost"}, {}, batch_size=5000)
        kwargs = mock_sql_db.call_args
        assert kwargs[1]["chunk_size"] == 5000


class TestBuildSourceModes:
    """build_source branching on mode (Step 20)."""

    @patch("etlfabric.services.dlt_runner.sql_database")
    def test_default_mode_uses_sql_database(self, mock_sql_db, svc):
        """No mode → full_database → sql_database."""
        mock_sql_db.return_value = "src"
        result = svc.build_source("postgres", {"host": "h"}, {"write_disposition": "append"})
        mock_sql_db.assert_called_once()
        assert result == "src"

    @patch("etlfabric.services.dlt_runner.sql_database")
    def test_full_database_passes_table_names(self, mock_sql_db, svc):
        mock_sql_db.return_value = "src"
        svc.build_source(
            "postgres",
            {"host": "h"},
            {"mode": "full_database", "table_names": ["a", "b"]},
        )
        kwargs = mock_sql_db.call_args[1]
        assert kwargs["table_names"] == ["a", "b"]

    @patch("etlfabric.services.dlt_runner.sql_database")
    def test_full_database_passes_source_schema(self, mock_sql_db, svc):
        mock_sql_db.return_value = "src"
        svc.build_source(
            "postgres", {"host": "h"}, {"mode": "full_database", "source_schema": "sales"}
        )
        kwargs = mock_sql_db.call_args[1]
        assert kwargs["schema"] == "sales"

    @patch("etlfabric.services.dlt_runner.sql_table")
    def test_single_table_uses_sql_table(self, mock_sql_table, svc):
        mock_sql_table.return_value = "tbl_src"
        result = svc.build_source(
            "postgres",
            {"host": "h"},
            {"mode": "single_table", "table": "customers"},
        )
        mock_sql_table.assert_called_once()
        assert result == "tbl_src"
        kwargs = mock_sql_table.call_args[1]
        assert kwargs["table"] == "customers"

    @patch("etlfabric.services.dlt_runner.sql_table")
    def test_single_table_passes_schema(self, mock_sql_table, svc):
        mock_sql_table.return_value = "tbl_src"
        svc.build_source(
            "postgres",
            {"host": "h"},
            {"mode": "single_table", "table": "t", "source_schema": "public"},
        )
        kwargs = mock_sql_table.call_args[1]
        assert kwargs["schema"] == "public"

    @patch("etlfabric.services.dlt_runner.sql_table")
    def test_single_table_passes_incremental(self, mock_sql_table, svc):
        mock_sql_table.return_value = "tbl_src"
        svc.build_source(
            "postgres",
            {"host": "h"},
            {
                "mode": "single_table",
                "table": "t",
                "incremental": {"cursor_path": "updated_at", "initial_value": "2024-01-01"},
            },
        )
        kwargs = mock_sql_table.call_args[1]
        inc = kwargs["incremental"]
        assert inc.cursor_path == "updated_at"
        assert inc.initial_value == "2024-01-01"

    @patch("etlfabric.services.dlt_runner.sql_table")
    def test_single_table_incremental_with_row_order(self, mock_sql_table, svc):
        mock_sql_table.return_value = "tbl_src"
        svc.build_source(
            "postgres",
            {"host": "h"},
            {
                "mode": "single_table",
                "table": "t",
                "incremental": {"cursor_path": "id", "row_order": "desc"},
            },
        )
        kwargs = mock_sql_table.call_args[1]
        assert kwargs["incremental"].row_order == "desc"

    @patch("etlfabric.services.dlt_runner.sql_table")
    def test_single_table_no_incremental(self, mock_sql_table, svc):
        """single_table without incremental → no incremental kwarg."""
        mock_sql_table.return_value = "tbl_src"
        svc.build_source(
            "postgres",
            {"host": "h"},
            {"mode": "single_table", "table": "t"},
        )
        kwargs = mock_sql_table.call_args[1]
        assert "incremental" not in kwargs


# ---------------------------------------------------------------------------
# build_pipeline
# ---------------------------------------------------------------------------
class TestBuildPipeline:
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_creates_pipeline_with_name(self, mock_dlt, svc):
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_dlt.pipeline.return_value = "pipe_obj"
        result = svc.build_pipeline(42, "postgres", {"host": "localhost"})
        mock_dlt.pipeline.assert_called_once()
        args = mock_dlt.pipeline.call_args
        assert args[1]["pipeline_name"] == "pipeline_42"
        assert result == "pipe_obj"

    @patch("etlfabric.services.dlt_runner.dlt")
    def test_creates_pipeline_with_dataset_name(self, mock_dlt, svc):
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_dlt.pipeline.return_value = "pipe_obj"
        svc.build_pipeline(10, "postgres", {"host": "localhost"}, dataset_name="raw")
        args = mock_dlt.pipeline.call_args
        assert args[1]["dataset_name"] == "raw"


# ---------------------------------------------------------------------------
# execute
# ---------------------------------------------------------------------------
class TestExecute:
    @patch("etlfabric.services.dlt_runner.sql_database")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_success_returns_rows_and_load_info(self, mock_dlt, mock_sql_db, svc):
        mock_pipeline = MagicMock()
        load_info = MagicMock()
        load_info.loads_count = 100
        mock_pipeline.run.return_value = load_info
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_sql_db.return_value = "source"

        result = svc.execute(
            pipeline_id=1,
            source_type="postgres",
            source_config={"host": "src"},
            destination_type="postgres",
            destination_config={"host": "dst"},
            dlt_config={"write_disposition": "append"},
        )
        assert result["rows_loaded"] == 100
        assert result["load_info"] is load_info

    @patch("etlfabric.services.dlt_runner.sql_database")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_failure_propagates_exception(self, mock_dlt, mock_sql_db, svc):
        mock_pipeline = MagicMock()
        mock_pipeline.run.side_effect = RuntimeError("boom")
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_sql_db.return_value = "source"

        with pytest.raises(RuntimeError, match="boom"):
            svc.execute(
                pipeline_id=1,
                source_type="postgres",
                source_config={},
                destination_type="postgres",
                destination_config={},
                dlt_config={},
            )

    @patch("etlfabric.services.dlt_runner.sql_database")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_passes_dlt_config_to_run(self, mock_dlt, mock_sql_db, svc):
        mock_pipeline = MagicMock()
        load_info = MagicMock()
        load_info.loads_count = 0
        mock_pipeline.run.return_value = load_info
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_sql_db.return_value = "source"

        svc.execute(
            pipeline_id=1,
            source_type="postgres",
            source_config={},
            destination_type="postgres",
            destination_config={},
            dlt_config={"write_disposition": "merge", "primary_key": "id"},
        )
        run_kwargs = mock_pipeline.run.call_args[1]
        assert run_kwargs["write_disposition"] == "merge"
        assert run_kwargs["primary_key"] == "id"

    @patch("etlfabric.services.dlt_runner.sql_database")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_uses_correct_source_destination_types(self, mock_dlt, mock_sql_db, svc):
        mock_pipeline = MagicMock()
        load_info = MagicMock()
        load_info.loads_count = 0
        mock_pipeline.run.return_value = load_info
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.mysql.return_value = "mysql_dest"
        mock_sql_db.return_value = "source"

        svc.execute(
            pipeline_id=1,
            source_type="mysql",
            source_config={"host": "src"},
            destination_type="mysql",
            destination_config={"host": "dst"},
            dlt_config={},
        )
        mock_dlt.destinations.mysql.assert_called_once()
        mock_sql_db.assert_called_once()

    @patch("etlfabric.services.dlt_runner.sql_database")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_default_batch_size(self, mock_dlt, mock_sql_db, svc):
        mock_pipeline = MagicMock()
        load_info = MagicMock()
        load_info.loads_count = 0
        mock_pipeline.run.return_value = load_info
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_sql_db.return_value = "source"

        svc.execute(
            pipeline_id=1,
            source_type="postgres",
            source_config={},
            destination_type="postgres",
            destination_config={},
            dlt_config={},
        )
        kwargs = mock_sql_db.call_args[1]
        assert kwargs["chunk_size"] == DEFAULT_BATCH_SIZE

    @patch("etlfabric.services.dlt_runner.sql_database")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_custom_batch_size_passed_through(self, mock_dlt, mock_sql_db, svc):
        mock_pipeline = MagicMock()
        load_info = MagicMock()
        load_info.loads_count = 0
        mock_pipeline.run.return_value = load_info
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_sql_db.return_value = "source"

        svc.execute(
            pipeline_id=1,
            source_type="postgres",
            source_config={},
            destination_type="postgres",
            destination_config={},
            dlt_config={},
            batch_size=500,
        )
        kwargs = mock_sql_db.call_args[1]
        assert kwargs["chunk_size"] == 500


class TestExecuteModes:
    """execute() internal-key filtering and batch_size extraction (Step 20)."""

    @patch("etlfabric.services.dlt_runner.sql_database")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_filters_internal_keys_from_run_kwargs(self, mock_dlt, mock_sql_db, svc):
        mock_pipeline = MagicMock()
        load_info = MagicMock()
        load_info.loads_count = 5
        mock_pipeline.run.return_value = load_info
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_sql_db.return_value = "source"

        svc.execute(
            pipeline_id=1,
            source_type="postgres",
            source_config={},
            destination_type="postgres",
            destination_config={},
            dlt_config={
                "mode": "full_database",
                "table_names": ["a"],
                "source_schema": "public",
                "batch_size": 1000,
                "write_disposition": "append",
            },
        )
        run_kwargs = mock_pipeline.run.call_args[1]
        # Internal keys must not leak to pipeline.run()
        for key in INTERNAL_CONFIG_KEYS:
            assert key not in run_kwargs
        # Passthrough keys must remain
        assert run_kwargs["write_disposition"] == "append"

    @patch("etlfabric.services.dlt_runner.sql_database")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_extracts_batch_size_from_dlt_config(self, mock_dlt, mock_sql_db, svc):
        mock_pipeline = MagicMock()
        load_info = MagicMock()
        load_info.loads_count = 0
        mock_pipeline.run.return_value = load_info
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_sql_db.return_value = "source"

        svc.execute(
            pipeline_id=1,
            source_type="postgres",
            source_config={},
            destination_type="postgres",
            destination_config={},
            dlt_config={"batch_size": 2000},
        )
        kwargs = mock_sql_db.call_args[1]
        assert kwargs["chunk_size"] == 2000

    @patch("etlfabric.services.dlt_runner.sql_table")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_execute_single_table_mode(self, mock_dlt, mock_sql_table, svc):
        mock_pipeline = MagicMock()
        load_info = MagicMock()
        load_info.loads_count = 42
        mock_pipeline.run.return_value = load_info
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_sql_table.return_value = "tbl_src"

        result = svc.execute(
            pipeline_id=1,
            source_type="postgres",
            source_config={"host": "h"},
            destination_type="postgres",
            destination_config={"host": "d"},
            dlt_config={
                "mode": "single_table",
                "table": "orders",
                "write_disposition": "merge",
                "primary_key": "id",
            },
        )
        assert result["rows_loaded"] == 42
        mock_sql_table.assert_called_once()
        run_kwargs = mock_pipeline.run.call_args[1]
        assert run_kwargs["write_disposition"] == "merge"
        assert run_kwargs["primary_key"] == "id"
        assert "table" not in run_kwargs
        assert "mode" not in run_kwargs

    @patch("etlfabric.services.dlt_runner.sql_database")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_schema_contract_passes_through(self, mock_dlt, mock_sql_db, svc):
        """schema_contract is NOT an internal key — it should pass to pipeline.run()."""
        mock_pipeline = MagicMock()
        load_info = MagicMock()
        load_info.loads_count = 0
        mock_pipeline.run.return_value = load_info
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_sql_db.return_value = "source"

        contract = {"tables": "evolve", "columns": "freeze"}
        svc.execute(
            pipeline_id=1,
            source_type="postgres",
            source_config={},
            destination_type="postgres",
            destination_config={},
            dlt_config={"schema_contract": contract, "write_disposition": "append"},
        )
        run_kwargs = mock_pipeline.run.call_args[1]
        assert run_kwargs["schema_contract"] == contract

    @patch("etlfabric.services.dlt_runner.sql_database")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_filters_not_passed_to_run(self, mock_dlt, mock_sql_db, svc):
        """filters is an internal key — should NOT pass to pipeline.run()."""
        mock_pipeline = MagicMock()
        load_info = MagicMock()
        load_info.loads_count = 0
        mock_pipeline.run.return_value = load_info
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_source = MagicMock()
        mock_sql_db.return_value = mock_source

        svc.execute(
            pipeline_id=1,
            source_type="postgres",
            source_config={},
            destination_type="postgres",
            destination_config={},
            dlt_config={
                "filters": [{"column": "status", "op": "eq", "value": "active"}],
                "write_disposition": "append",
            },
        )
        run_kwargs = mock_pipeline.run.call_args[1]
        assert "filters" not in run_kwargs

    @patch("etlfabric.services.dlt_runner.sql_database")
    @patch("etlfabric.services.dlt_runner.dlt")
    def test_filters_applied_to_source(self, mock_dlt, mock_sql_db, svc):
        """Filters should result in add_filter being called on the source."""
        mock_pipeline = MagicMock()
        load_info = MagicMock()
        load_info.loads_count = 0
        mock_pipeline.run.return_value = load_info
        mock_dlt.pipeline.return_value = mock_pipeline
        mock_dlt.destinations.postgres.return_value = "pg_dest"
        mock_source = MagicMock()
        mock_sql_db.return_value = mock_source

        svc.execute(
            pipeline_id=1,
            source_type="postgres",
            source_config={},
            destination_type="postgres",
            destination_config={},
            dlt_config={
                "filters": [{"column": "status", "op": "eq", "value": "active"}],
            },
        )
        mock_source.add_filter.assert_called_once()
