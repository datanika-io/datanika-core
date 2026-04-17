"""ELT runner — Arrow mode wiring (E6) + stream_to_raw orchestration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from datanika.services.elt_runner import StreamStats, stream_to_raw
from datanika.services.ir.schema import IR, IRColumn, IRSource, IRTarget


def _make_ir() -> IR:
    return IR(
        ir_version=1,
        source=IRSource(kind="sql_table", connection_id=1, table="orders"),
        columns=[
            IRColumn(source_ref="id", target_name="id", type="bigint", nullable=False),
        ],
        primary_key=["id"],
        target=IRTarget(connection_id=2, raw_schema="raw", table="orders"),
    )


class TestArrowModeWiring:
    """E6 — stream_to_raw sets backend='pyarrow' when building SQL sources."""

    def test_stream_to_raw_passes_arrow_backend_to_build_source(self):
        ir = _make_ir()

        fake_lander = MagicMock()
        fake_lander.land.return_value = {
            "rows": 0,
            "bytes_in": 0,
            "bytes_out": 0,
            "batches": 0,
        }

        with (
            patch(
                "datanika.services.destinations.get_lander",
                return_value=fake_lander,
            ),
            patch(
                "datanika.services.dlt_runner.DltRunnerService.build_source",
                return_value=object(),
            ) as mock_build_source,
        ):
            stats = stream_to_raw(
                ir=ir,
                run_id=1,
                source_config={},
                destination_config={},
                source_type="postgres",
                destination_type="postgres",
            )

        assert isinstance(stats, StreamStats)
        # E6: Arrow backend is wired.
        call_args = mock_build_source.call_args
        # build_source(source_type, source_config, dlt_config, batch_size=...)
        if len(call_args.args) >= 3:
            dlt_config = call_args.args[2]
        else:
            dlt_config = call_args.kwargs["dlt_config"]
        assert dlt_config["backend"] == "pyarrow"
        assert dlt_config["table"] == "orders"
        assert dlt_config["mode"] == "single_table"

    def test_stream_to_raw_captures_stats_from_lander(self):
        ir = _make_ir()

        fake_lander = MagicMock()
        fake_lander.land.return_value = {
            "rows": 1000,
            "bytes_in": 50_000,
            "bytes_out": 20_000,
            "batches": 2,
        }

        with (
            patch(
                "datanika.services.destinations.get_lander",
                return_value=fake_lander,
            ),
            patch(
                "datanika.services.dlt_runner.DltRunnerService.build_source",
                return_value=object(),
            ),
        ):
            stats = stream_to_raw(
                ir=ir,
                run_id=42,
                source_config={},
                destination_config={},
                source_type="postgres",
                destination_type="postgres",
            )

        assert stats.rows == 1000
        assert stats.bytes_out == 20_000
        assert stats.batches == 2
        assert stats.duration_ms >= 0


class TestDltRunnerBackendPassthrough:
    """E6 — DltRunnerService.build_source forwards backend to sql_table/sql_database."""

    def test_single_table_passes_backend(self):
        from datanika.services.dlt_runner import DltRunnerService

        runner = DltRunnerService()

        with patch("datanika.services.dlt_runner.sql_table") as mock_sql_table:
            mock_sql_table.return_value = object()
            runner.build_source(
                "postgres",
                {"host": "h", "port": 5432, "user": "u", "password": "p", "database": "d"},
                {"mode": "single_table", "table": "t", "backend": "pyarrow"},
                batch_size=1000,
            )

            kwargs = mock_sql_table.call_args.kwargs
            assert kwargs["backend"] == "pyarrow"
            assert kwargs["table"] == "t"
            assert kwargs["chunk_size"] == 1000

    def test_full_database_passes_backend(self):
        from datanika.services.dlt_runner import DltRunnerService

        runner = DltRunnerService()

        with patch("datanika.services.dlt_runner.sql_database") as mock_sql_database:
            mock_sql_database.return_value = object()
            runner.build_source(
                "postgres",
                {"host": "h", "port": 5432, "user": "u", "password": "p", "database": "d"},
                {"mode": "full_database", "backend": "pyarrow"},
                batch_size=1000,
            )

            kwargs = mock_sql_database.call_args.kwargs
            assert kwargs["backend"] == "pyarrow"

    def test_backend_is_optional(self):
        """ETL path doesn't set backend — must not be forwarded as None."""
        from datanika.services.dlt_runner import DltRunnerService

        runner = DltRunnerService()

        with patch("datanika.services.dlt_runner.sql_table") as mock_sql_table:
            mock_sql_table.return_value = object()
            runner.build_source(
                "postgres",
                {"host": "h", "port": 5432, "user": "u", "password": "p", "database": "d"},
                {"mode": "single_table", "table": "t"},  # no backend
                batch_size=1000,
            )

            kwargs = mock_sql_table.call_args.kwargs
            assert "backend" not in kwargs
