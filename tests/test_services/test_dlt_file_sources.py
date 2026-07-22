"""File sources must load file **contents** (core#492).

`dlt`'s `filesystem()` is a *lister*: it yields one record per matched file
(name, size, mtime, mime type). Reading contents requires piping it through a
format transformer. Without one, prod loaded a **table of filenames**, reported
`success`, and showed `Rows: 1` — that 1 being the file, not the data. On the
zero-credentials onboarding path we advertise hardest.

**Why 2,500 green tests said nothing about it.** Every existing test in
`TestBuildFileSource` patches `datanika.services.dlt_runner.filesystem` and
asserts the kwargs we passed, with the mock returning the string `"csv_src"`.
What dlt *yields* was unobservable to all of them, so the entire defect lived
in the gap between "we called it correctly" and "it returns rows".

So this file mocks nothing. It writes real files, runs a real dlt pipeline into
a real DuckDB, and asserts the rows and columns that actually land. Every test
here fails on the pre-fix code — that is the point of it existing.
"""

import json

import pytest

from datanika.services.dlt_runner import DltRunnerError, DltRunnerService

duckdb = pytest.importorskip("duckdb", reason="file-source loads assert against a real DuckDB")

CUSTOMERS = [
    {"customer_id": 1001, "full_name": "Ada Lovelace", "lifetime_value_usd": 1840.00},
    {"customer_id": 1002, "full_name": "Grace Hopper", "lifetime_value_usd": 7325.50},
]

#: The columns `filesystem()` yields when nobody pipes it through a reader.
#: Their presence in a destination table *is* the bug.
FILE_LISTING_COLUMNS = {"file_name", "relative_path", "file_url", "mime_type", "size_in_bytes"}


def _write_csv(directory, name="customers.csv", delimiter=","):
    header = delimiter.join(["customer_id", "full_name", "lifetime_value_usd"])
    lines = [
        delimiter.join([str(r["customer_id"]), r["full_name"], str(r["lifetime_value_usd"])])
        for r in CUSTOMERS
    ]
    path = directory / name
    path.write_text("\n".join([header, *lines]) + "\n", encoding="utf-8")
    return path


def _load(source, tmp_path, label="probe"):
    """Run a real dlt pipeline into DuckDB; return {table: (columns, rows)}."""
    import dlt

    db_path = tmp_path / f"{label}.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name=f"t492_{label}",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="probe",
        pipelines_dir=str(tmp_path / f"state_{label}"),
    )
    pipeline.run(source)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'probe' AND table_name NOT LIKE '\\_dlt%' ESCAPE '\\'"
        ).fetchall()
        out = {}
        for (table,) in tables:
            columns = [
                c[0]
                for c in con.execute(f'DESCRIBE probe."{table}"').fetchall()
                if not c[0].startswith("_dlt")
            ]
            rows = con.execute(f'SELECT count(*) FROM probe."{table}"').fetchone()[0]
            out[table] = (columns, rows)
        return out
    finally:
        con.close()


@pytest.fixture
def svc():
    return DltRunnerService(pipelines_dir="/tmp/t492")


class TestFileContentsActuallyLand:
    def test_csv_loads_rows_not_a_file_listing(self, svc, tmp_path):
        _write_csv(tmp_path)
        source = svc.build_source("csv", {}, {"bucket_url": str(tmp_path)})

        tables = _load(source, tmp_path, "csv")

        assert len(tables) == 1, f"expected one table, got {sorted(tables)}"
        (columns, rows) = next(iter(tables.values()))
        assert rows == len(CUSTOMERS), f"expected {len(CUSTOMERS)} data rows, got {rows}"
        assert "customer_id" in columns and "full_name" in columns
        assert not FILE_LISTING_COLUMNS & set(columns), (
            f"the file *listing* landed instead of the data: {columns}"
        )

    def test_csv_values_survive_the_round_trip(self, svc, tmp_path):
        """Row count alone would pass on a table of two filenames."""
        _write_csv(tmp_path)
        source = svc.build_source(
            "csv", {}, {"bucket_url": str(tmp_path), "table_name": "customers"}
        )
        _load(source, tmp_path, "csvvals")

        con = duckdb.connect(str(tmp_path / "csvvals.duckdb"), read_only=True)
        try:
            names = {r[0] for r in con.execute("SELECT full_name FROM probe.customers").fetchall()}
        finally:
            con.close()
        assert names == {"Ada Lovelace", "Grace Hopper"}

    def test_json_array_loads_flat_not_as_a_nested_child_table(self, svc, tmp_path):
        """The shape dlt's own `read_jsonl` gets wrong.

        Fed an array, `read_jsonl` parses the whole file as one value: dlt then
        writes a parent row with no business columns plus a `__value` child
        table. Green run, data in the wrong shape — the same class of failure as
        the file listing.
        """
        (tmp_path / "customers.json").write_text(json.dumps(CUSTOMERS), encoding="utf-8")
        source = svc.build_source("json", {}, {"bucket_url": str(tmp_path)})

        tables = _load(source, tmp_path, "jsonarray")

        assert len(tables) == 1, f"expected one flat table, got {sorted(tables)}"
        table, (columns, rows) = next(iter(tables.items()))
        assert not table.endswith("__value"), f"data landed in a child table: {table}"
        assert rows == len(CUSTOMERS)
        assert "customer_id" in columns

    def test_json_lines_load(self, svc, tmp_path):
        (tmp_path / "customers.jsonl").write_text(
            "\n".join(json.dumps(r) for r in CUSTOMERS), encoding="utf-8"
        )
        source = svc.build_source("json", {}, {"bucket_url": str(tmp_path), "file_glob": "*.jsonl"})

        tables = _load(source, tmp_path, "jsonl")

        (columns, rows) = next(iter(tables.values()))
        assert rows == len(CUSTOMERS)
        assert "customer_id" in columns

    def test_a_single_pretty_printed_object_loads_as_one_row(self, svc, tmp_path):
        """Not line-delimited and not an array — the third real shape."""
        (tmp_path / "customer.json").write_text(json.dumps(CUSTOMERS[0], indent=2), "utf-8")
        source = svc.build_source("json", {}, {"bucket_url": str(tmp_path)})

        tables = _load(source, tmp_path, "jsonobj")

        (columns, rows) = next(iter(tables.values()))
        assert rows == 1
        assert "customer_id" in columns

    def test_parquet_loads_rows(self, svc, tmp_path):
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        pd.DataFrame(CUSTOMERS).to_parquet(tmp_path / "customers.parquet", index=False)

        source = svc.build_source("parquet", {}, {"bucket_url": str(tmp_path)})
        tables = _load(source, tmp_path, "parquet")

        (columns, rows) = next(iter(tables.values()))
        assert rows == len(CUSTOMERS)
        assert not FILE_LISTING_COLUMNS & set(columns)

    def test_custom_delimiter_is_honoured(self, svc, tmp_path):
        """A semicolon export read as comma-delimited yields one fused column."""
        _write_csv(tmp_path, delimiter=";")
        source = svc.build_source("csv", {}, {"bucket_url": str(tmp_path), "delimiter": ";"})

        tables = _load(source, tmp_path, "semicolon")

        (columns, rows) = next(iter(tables.values()))
        assert rows == len(CUSTOMERS)
        assert "customer_id" in columns and "full_name" in columns


class TestTableNaming:
    def test_the_table_is_not_named_after_the_transformer(self, svc, tmp_path):
        """Unrenamed, a piped transformer lands in `_read_csv`.

        That reads as a dlt internal table, and `_dlt`-prefixed names are how
        dlt marks its own bookkeeping — so the naive fix trades a wrong payload
        for a table users would reasonably ignore.
        """
        _write_csv(tmp_path)
        source = svc.build_source("csv", {}, {"bucket_url": str(tmp_path)})

        tables = _load(source, tmp_path, "naming")

        for table in tables:
            assert not table.startswith("_"), f"table {table!r} looks like a dlt internal"

    def test_explicit_table_name_wins(self, svc, tmp_path):
        _write_csv(tmp_path)
        source = svc.build_source(
            "csv", {}, {"bucket_url": str(tmp_path), "table_name": "customers_daily"}
        )
        assert "customers_daily" in _load(source, tmp_path, "explicitname")

    def test_a_single_named_file_names_the_table(self, svc, tmp_path):
        _write_csv(tmp_path)
        source = svc.build_source(
            "csv", {}, {"bucket_url": str(tmp_path), "file_glob": "customers.csv"}
        )
        assert "customers" in _load(source, tmp_path, "stemname")

    def test_a_wildcard_glob_falls_back_to_the_connection_type(self, svc, tmp_path):
        """Documents the fallback rather than leaving it to be discovered.

        A wildcard matches many files, so no filename can name the table. The
        connection type is at least stable and predictable. The *advertised*
        upload path doesn't rely on this — `upload_tasks` sets `table_name`
        from the uploaded file's original name, which is the only layer that
        knows it (the runner sees a hash-named extract dir).
        """
        _write_csv(tmp_path)
        source = svc.build_source("csv", {}, {"bucket_url": str(tmp_path)})
        assert "csv" in _load(source, tmp_path, "wildcardname")


class TestUnreadableFormatsRefuseInsteadOfGuessing:
    def test_bare_s3_glob_raises_rather_than_loading_a_listing(self, svc):
        """`s3` + `*` carries no format. Guessing reproduces core#492."""
        with pytest.raises(DltRunnerError) as exc:
            svc.build_source("s3", {"bucket_url": "s3://bucket"}, {})

        message = str(exc.value)
        assert "file_format" in message, f"error should name the fix, got: {message}"

    def test_s3_infers_the_format_from_the_pattern(self, svc, tmp_path):
        _write_csv(tmp_path)
        source = svc.build_source("s3", {}, {"bucket_url": str(tmp_path), "file_glob": "*.csv"})
        (columns, rows) = next(iter(_load(source, tmp_path, "s3csv").values()))
        assert rows == len(CUSTOMERS)

    def test_explicit_file_format_overrides_an_ambiguous_pattern(self, svc, tmp_path):
        _write_csv(tmp_path, name="customers.data")
        source = svc.build_source(
            "s3",
            {},
            {"bucket_url": str(tmp_path), "file_glob": "*.data", "file_format": "csv"},
        )
        (columns, rows) = next(iter(_load(source, tmp_path, "explicitfmt").values()))
        assert rows == len(CUSTOMERS)
        assert "customer_id" in columns

    def test_an_unknown_file_format_is_rejected(self, svc):
        with pytest.raises(DltRunnerError, match="Unsupported file_format"):
            svc.build_source("s3", {}, {"bucket_url": "s3://b", "file_format": "avro"})
