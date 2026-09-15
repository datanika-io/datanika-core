"""Re-running an unchanged upload must leave each source record in the destination once (core#1336).

What QA measured on the `openapi` path, 2026-09-15
--------------------------------------------------
The same upload, run twice, doubled every table while both runs read green: activities 30 -> 60,
books 200 -> 400, users 10 -> 20, with distinct ids unchanged. The stored row read
`{"mode": "full_database", "write_disposition": "append"}`. It was created through the structured
form, which does not render the Write Disposition control for a non-SQL source.

The mechanism, read on `dev`
----------------------------
1. `UploadState._build_config` stores `form_write_disposition` (default `"append"`) for every
   structured upload, including source types whose form never shows the control.
2. `DltRunnerService.execute` forwards it as `pipeline.run(write_disposition=...)`.
3. dlt applies a run-level disposition to every resource, overwriting any the resource declared
   (`dlt/extract/extract.py` `apply_hint_args`, `dlt/extract/hints.py` `apply_hints`).
4. Each run's pipeline name carries its run id, so no dlt state crosses runs. Every run of a SaaS,
   OpenAPI or file upload therefore re-fetches everything, and `append` lands all of it again.

Why no existing test saw it
---------------------------
The one credential-free real-destination harness, `test_source_builders_move_rows._extract_load`,
forces `{"write_disposition": "replace", **dlt_config}` and loads once. A single run cannot see a
duplicate, and a forced `replace` cannot see the disposition at all.

What these tests measure
------------------------
Two or three runs of the real `execute()` into one DuckDB file, with a distinct run id per run as
production uses. Assertions read the destination back, never `rows_loaded`. Every assertion also
reads `_dlt_loads`, so a run that silently loaded nothing cannot pass as "each record once".
"""

from __future__ import annotations

import copy
import dataclasses
import http.server
import json
import threading

import dlt
import duckdb
import pytest

from datanika.services.dlt_runner import DltRunnerService
from datanika.ui.state.connection_state import (
    FILE_SOURCE_TYPES,
    NON_SQL_SOURCE_TYPES,
    SAAS_SOURCE_TYPES,
)
from datanika.ui.state.upload_state import UploadState

WIDGETS = [
    {"id": 1, "name": "alpha", "price": 100},
    {"id": 2, "name": "beta", "price": 200},
    {"id": 3, "name": "gamma", "price": 300},
]


def _widget_resource(**extra) -> dict:
    """A fresh resource dict per use: dlt writes into the dicts it is handed."""
    return {"name": "widgets", "endpoint": {"path": "widgets"}, **extra}


class _JsonHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path.startswith("/widgets"):
            body = json.dumps(WIDGETS).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def log_message(self, *args):
        pass


@pytest.fixture
def json_api():
    server = http.server.HTTPServer(("127.0.0.1", 0), _JsonHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def allow_loopback(monkeypatch):
    """The three patches `test_source_builders_move_rows.allow_loopback` documents, for the same
    reason: patching only the validators leaves the pinned session refusing the loopback server."""
    monkeypatch.setattr("datanika.services.dlt_runner.validate_egress_host", lambda url: None)
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
    )


def _field_default(field):
    """A declared var's default value.

    List vars declare a `default_factory`, and their `default` is the dataclasses MISSING sentinel.
    Copying `default` blindly stored `endpoints: <MISSING>` and failed the SaaS run with a
    TypeError, a red about this harness rather than about the loader.
    """
    default = getattr(field, "default", None)
    if default is dataclasses.MISSING:
        factory = getattr(field, "default_factory", dataclasses.MISSING)
        return factory() if factory is not dataclasses.MISSING else None
    return copy.deepcopy(default)


class _UploadForm:
    """A stand-in `self` carrying UploadState's declared vars, so the REAL `_build_config` runs."""

    _build_config = UploadState._build_config

    def __init__(self, source_type: str, **values):
        for name, field in UploadState.get_fields().items():
            setattr(self, name, _field_default(field))
        # What `set_form_source_id` sets when a source of this type is picked.
        self.form_is_non_sql_source = source_type in NON_SQL_SOURCE_TYPES
        self.form_is_file_source = source_type in FILE_SOURCE_TYPES
        self.form_is_saas_source = source_type in SAAS_SOURCE_TYPES
        for name, value in values.items():
            setattr(self, name, value)


def form_config(source_type: str, **values) -> dict:
    """The dlt_config the structured upload form stores, produced by the real `_build_config`."""
    return _UploadForm(source_type, **values)._build_config()


def _run(tmp_path, source_type: str, source_config: dict, dlt_config: dict, *, run_id: int):
    DltRunnerService(pipelines_dir=str(tmp_path / "dlt")).execute(
        pipeline_id=7,
        source_type=source_type,
        source_config=copy.deepcopy(source_config),
        destination_type="duckdb",
        destination_config={"path": str(tmp_path / "dest.duckdb")},
        dlt_config=copy.deepcopy(dlt_config),
        dataset_name="probe",
        run_id=run_id,
    )


def _destination(tmp_path, table: str = "widgets") -> tuple[int, int, int]:
    """(rows, distinct ids, completed loads), read back from the destination itself."""
    con = duckdb.connect(str(tmp_path / "dest.duckdb"))
    try:
        rows, distinct = con.execute(
            f'SELECT count(*), count(DISTINCT id) FROM "probe"."{table}"'
        ).fetchone()
        loads = con.execute('SELECT count(*) FROM "probe"."_dlt_loads"').fetchone()[0]
    finally:
        con.close()
    return rows, distinct, loads


def _openapi_connection(base_url: str) -> dict:
    """An openapi connection as the form stores it: the parser's catalog, with its inferred key."""
    return {
        "base_url": base_url,
        "resources": [
            _widget_resource(
                columns=[{"name": "id", "type": "bigint", "nullable": False}],
                _source={"operation_id": "listWidgets", "summary": None},
                primary_key="id",
            )
        ],
    }


def _salesforce_connection(base_url: str) -> dict:
    """Salesforce is the one SaaS type whose host comes from config, so it reaches a local server
    through the shared REST fallback unpatched (see `TestSaasRestFallbackMovesRows`)."""
    return {"instance_url": base_url, "access_token": "probe-token"}


def _write_csv(directory) -> None:
    rows = "".join(f"{w['id']},{w['name']},{w['price']}\n" for w in WIDGETS)
    (directory / "widgets.csv").write_text("id,name,price\n" + rows, encoding="utf-8")


class TestTheFormsConfigLandsEachRecordOnce:
    """Product's AC2: two runs of an unchanged source leave each record in the destination once."""

    def test_openapi(self, tmp_path, json_api, allow_loopback):
        config = form_config("openapi")
        for run_id in (1, 2):
            _run(tmp_path, "openapi", _openapi_connection(json_api), config, run_id=run_id)

        assert _destination(tmp_path) == (3, 3, 2), (
            f"two runs of an openapi upload saved through the form left {_destination(tmp_path)} "
            "(rows, distinct ids, loads). Rows twice the distinct ids is core#1336: the form "
            "stored a disposition it does not show, and dlt appended every record again."
        )

    def test_the_saas_rest_fallback(self, tmp_path, json_api, allow_loopback):
        # `resources` is the harness's override, pointing the fallback at the local server. It is
        # not what the form writes for a SaaS source; everything else in the config is.
        config = {**form_config("salesforce"), "resources": [_widget_resource()]}
        for run_id in (1, 2):
            _run(tmp_path, "salesforce", _salesforce_connection(json_api), config, run_id=run_id)

        assert _destination(tmp_path) == (3, 3, 2), (
            f"two runs through the SaaS REST fallback left {_destination(tmp_path)}. This is the "
            "production path for the REST-backed SaaS connectors."
        )

    def test_a_csv_file(self, tmp_path):
        drop = tmp_path / "drop"
        drop.mkdir()
        _write_csv(drop)
        config = form_config("csv", form_file_glob="widgets.csv")
        for run_id in (1, 2):
            _run(tmp_path, "csv", {"bucket_url": str(drop)}, config, run_id=run_id)

        assert _destination(tmp_path) == (3, 3, 2), (
            f"two runs of an unchanged CSV upload left {_destination(tmp_path)}"
        )


class TestAnUploadSavedBeforeTheFix:
    def test_the_stored_config_qa_measured_lands_each_record_once(
        self, tmp_path, json_api, allow_loopback
    ):
        """QA's stored row, verbatim. Existing uploads must stop doubling without being re-saved."""
        stored = {"mode": "full_database", "write_disposition": "append"}
        for run_id in (1, 2):
            _run(tmp_path, "openapi", _openapi_connection(json_api), stored, run_id=run_id)

        assert _destination(tmp_path) == (3, 3, 2), (
            f"the config QA found stored on the doubled upload still doubles: "
            f"{_destination(tmp_path)}"
        )

    def test_duplicates_from_earlier_runs_are_replaced_at_the_next_run(
        self, tmp_path, json_api, allow_loopback
    ):
        """Measures what the release note may promise about tables that already hold duplicates.

        The precondition is asserted first: without it, a clean result would say nothing.
        """
        connection = _openapi_connection(json_api)
        for run_id in (1, 2):
            _run(tmp_path, "openapi", connection, {"write_disposition": "append"}, run_id=run_id)
        assert _destination(tmp_path) == (6, 3, 2), "precondition: the table holds duplicates"

        _run(tmp_path, "openapi", connection, form_config("openapi"), run_id=3)

        assert _destination(tmp_path) == (3, 3, 3), (
            f"the next run after this fix left {_destination(tmp_path)}; the duplicates written "
            "by earlier runs were not replaced"
        )


class TestADeliberateChoiceStillGoverns:
    def test_control_an_explicit_append_still_appends(self, tmp_path, json_api, allow_loopback):
        """The control that makes the tests above able to fail: this harness sees a doubling.

        It also pins the boundary of the fix. A disposition sent without the form's `mode` key,
        through raw JSON or the API, is a choice, and a choice still governs.
        """
        for run_id in (1, 2):
            _run(
                tmp_path,
                "openapi",
                _openapi_connection(json_api),
                {"write_disposition": "append"},
                run_id=run_id,
            )

        assert _destination(tmp_path) == (6, 3, 2)

    def test_a_resource_that_declares_its_own_disposition_keeps_it(
        self, tmp_path, json_api, allow_loopback
    ):
        """The code-chosen default must not overwrite what a resource declares."""
        config = {"resources": [_widget_resource(write_disposition="append")]}
        for run_id in (1, 2):
            _run(tmp_path, "salesforce", _salesforce_connection(json_api), config, run_id=run_id)

        assert _destination(tmp_path) == (6, 3, 2)

    def test_a_sql_sources_visible_choice_is_still_forwarded(self, tmp_path, monkeypatch):
        """A SQL source's form renders the control, so its stored `append` is a real choice.

        The builder is replaced by a plain resource so no database is needed. The disposition
        question is decided in `execute()`, after the builder has returned.
        """

        @dlt.resource(name="widgets")
        def widgets():
            yield WIDGETS

        monkeypatch.setattr(DltRunnerService, "build_source", lambda self, *a, **k: widgets())
        stored = {"mode": "full_database", "write_disposition": "append"}
        for run_id in (1, 2):
            _run(tmp_path, "postgres", {"host": "unused"}, stored, run_id=run_id)

        assert _destination(tmp_path) == (6, 3, 2)
