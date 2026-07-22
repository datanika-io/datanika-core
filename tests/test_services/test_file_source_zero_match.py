"""A file source that matches nothing must not report success (core#493).

Found on production alongside core#492: connection `customerscsv` had
`bucket_url` set to a **full file path**, and the glob is applied *under* that
value, so `*.csv` matched nothing. The run completed **`success`** in 3.7s with
`Rows: 0`, created only dlt's bookkeeping tables, and left no signal anywhere
that distinguished "loaded everything" from "found nothing".

That is the same family as core#456 (run rows that lie) and the alert rules that
could not tell healthy from dead: **a state that should be loud is byte-identical
to the healthy one.** A typo'd path, a moved file, an export that didn't run and
an emptied S3 prefix all landed here.

`Test Connection` could not catch it either — every file type fell into
`_NON_DB_TYPES` and returned `(True, "Test not applicable for this type")`
unconditionally, so a wrong path tested exactly like a right one. Both halves
are covered here, because fixing only the run would leave the user with no way
to find out *before* running.
"""

import pytest

from datanika.models.connection import ConnectionType
from datanika.services.connection_service import ConnectionService
from datanika.services.dlt_runner import DltRunnerError, DltRunnerService


@pytest.fixture
def svc():
    return DltRunnerService(pipelines_dir="/tmp/t493")


def _write_csv(directory, name="customers.csv"):
    path = directory / name
    path.write_text("customer_id,full_name\n1001,Ada Lovelace\n", encoding="utf-8")
    return path


class TestTheRunRefusesToLoadNothing:
    def test_a_glob_matching_nothing_raises(self, svc, tmp_path):
        (tmp_path / "customers.txt").write_text("not a csv", encoding="utf-8")

        with pytest.raises(DltRunnerError) as exc:
            svc.build_source("csv", {}, {"bucket_url": str(tmp_path)})

        assert "*.csv" in str(exc.value)
        assert "zero rows" in str(exc.value)

    def test_bucket_url_pointing_at_a_file_says_so(self, svc, tmp_path):
        """The production repro, and the message that would have solved it.

        `bucket_url` was a full file path. "No files matched" alone sends the
        user to re-check a glob that was never the problem, so the message has
        to name the file-vs-directory mistake and the fix.
        """
        csv_path = _write_csv(tmp_path)

        with pytest.raises(DltRunnerError) as exc:
            svc.build_source("csv", {}, {"bucket_url": str(csv_path)})

        message = str(exc.value)
        assert "is a file" in message
        assert str(tmp_path) in message, "the message should name the directory to use instead"

    def test_a_missing_path_says_it_does_not_exist(self, svc, tmp_path):
        with pytest.raises(DltRunnerError, match="does not exist"):
            svc.build_source("csv", {}, {"bucket_url": str(tmp_path / "nope")})

    def test_an_empty_directory_is_distinguished_from_a_bad_path(self, svc, tmp_path):
        """Three causes, three messages — otherwise the error is a shrug."""
        empty = tmp_path / "empty"
        empty.mkdir()

        with pytest.raises(DltRunnerError) as exc:
            svc.build_source("csv", {}, {"bucket_url": str(empty)})

        assert "exists but holds nothing" in str(exc.value)

    def test_a_matching_source_still_builds(self, svc, tmp_path):
        """The guard must not refuse healthy sources.

        Peeking consumes an item from the lister; if that broke re-iteration,
        every real load would silently lose its first file.
        """
        _write_csv(tmp_path)
        source = svc.build_source("csv", {}, {"bucket_url": str(tmp_path)})
        assert source is not None

    def test_peeking_does_not_consume_the_first_file(self, svc, tmp_path):
        """The specific way this guard could corrupt a working pipeline."""
        duckdb = pytest.importorskip("duckdb")
        import dlt

        _write_csv(tmp_path)
        _write_csv(tmp_path, name="customers_2.csv")

        source = svc.build_source(
            "csv", {}, {"bucket_url": str(tmp_path), "table_name": "customers"}
        )
        pipeline = dlt.pipeline(
            pipeline_name="t493_peek",
            destination=dlt.destinations.duckdb(str(tmp_path / "peek.duckdb")),
            dataset_name="probe",
            pipelines_dir=str(tmp_path / "state"),
        )
        pipeline.run(source)

        con = duckdb.connect(str(tmp_path / "peek.duckdb"), read_only=True)
        try:
            rows = con.execute("SELECT count(*) FROM probe.customers").fetchone()[0]
        finally:
            con.close()
        assert rows == 2, "a row per file — the peeked file must still be loaded"


class TestConnectionTestActuallyTests:
    def test_a_good_path_passes(self, tmp_path):
        _write_csv(tmp_path)
        ok, message = ConnectionService.test_connection(
            {"bucket_url": str(tmp_path)}, ConnectionType.CSV
        )
        assert ok, message

    def test_a_wrong_path_no_longer_looks_like_a_right_one(self, tmp_path):
        """The whole point: this returned `(True, "not applicable")` before."""
        ok, message = ConnectionService.test_connection(
            {"bucket_url": str(tmp_path / "nope")}, ConnectionType.CSV
        )
        assert not ok
        assert "does not exist" in message

    def test_a_file_path_is_diagnosed_at_test_time(self, tmp_path):
        """Catch the production mistake before the user ever runs."""
        csv_path = _write_csv(tmp_path)
        ok, message = ConnectionService.test_connection(
            {"bucket_url": str(csv_path)}, ConnectionType.CSV
        )
        assert not ok
        assert "is a file" in message

    def test_the_legacy_path_key_is_accepted(self, tmp_path):
        """`CONFIG_SCHEMAS` declares `path`; live connections carry `bucket_url`."""
        _write_csv(tmp_path)
        ok, message = ConnectionService.test_connection({"path": str(tmp_path)}, ConnectionType.CSV)
        assert ok, message

    def test_an_empty_config_is_rejected_before_listing(self):
        ok, message = ConnectionService.test_connection({}, ConnectionType.CSV)
        assert not ok

    def test_a_config_without_a_location_is_rejected(self):
        ok, message = ConnectionService.test_connection(
            {"aws_access_key_id": "AKID"}, ConnectionType.S3
        )
        assert not ok
        assert "bucket URL or path" in message

    @pytest.mark.parametrize(
        "connection_type",
        [ConnectionType.CSV, ConnectionType.JSON, ConnectionType.PARQUET, ConnectionType.S3],
    )
    def test_no_file_type_still_answers_not_applicable(self, connection_type, tmp_path):
        """All four, not just the one the bug was reported on."""
        _, message = ConnectionService.test_connection(
            {"bucket_url": str(tmp_path / "nope")}, connection_type
        )
        assert "not applicable" not in message

    def test_non_file_types_are_untouched(self):
        ok, message = ConnectionService.test_connection(
            {"base_url": "https://api.example.com"}, ConnectionType.REST_API
        )
        assert ok and "not applicable" in message
