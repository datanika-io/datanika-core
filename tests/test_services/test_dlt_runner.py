"""TDD tests for DltRunnerService — dlt pipeline/source/destination factory."""

from unittest.mock import MagicMock, patch

import pytest

from etlfabric.services.dlt_runner import (
    DEFAULT_BATCH_SIZE,
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
