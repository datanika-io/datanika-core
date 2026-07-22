"""Format knobs for file sources: file_format, delimiter, encoding (core#499).

core#492 made file sources load contents, and doing that introduced three
settings the runner reads from `dlt_config`. None had a UI, so a semicolon CSV
loaded as one fused column and an `s3` connection with the default `*` glob
failed with an error the user could not act on.

The last test here is the one that matters most: it asserts the keys the form
*writes* are the keys the runner *reads*. core#532 is what happens without it —
the endpoint picker writes `endpoints`, the builders read `resources`, and the
control has been decorative ever since.
"""

import pytest

from datanika.services.dlt_runner import (
    DltRunnerError,
    _build_format_reader,
    _resolve_file_format,
)
from datanika.ui.state.upload_state import UploadState


def _build(**form_values):
    """Build a dlt_config the way the form does, without a live Reflex state."""
    state = UploadState.__new__(UploadState)
    defaults = {
        "form_mode": "full_database",
        "form_write_disposition": "",
        "form_primary_key": "",
        "form_merge_config": "",
        "form_table": "",
        "form_source_schema": "",
        "form_table_names": "",
        "form_batch_size": "",
        "form_enable_incremental": False,
        "form_cursor_path": "",
        "form_initial_value": "",
        "form_row_order": "",
        "form_sc_tables": "",
        "form_sc_columns": "",
        "form_sc_data_type": "",
        "form_is_saas_source": False,
        "form_is_non_sql_source": True,
        "form_selected_endpoints": [],
        "form_sheet_names": "",
        "form_collection_names": "",
        "form_file_glob": "",
        "form_file_format": "",
        "form_delimiter": "",
        "form_encoding": "",
        "form_source_id": "1 — files (csv)",
        "form_use_raw_json": False,
        "form_config": "{}",
    }
    defaults.update(form_values)
    for key, value in defaults.items():
        object.__setattr__(state, key, value)
    return UploadState._build_config(state)


class TestFormatKnobsReachTheConfig:
    def test_delimiter_is_written_when_set(self):
        assert _build(form_delimiter=";")["delimiter"] == ";"

    def test_encoding_is_written_when_set(self):
        assert _build(form_encoding="windows-1252")["encoding"] == "windows-1252"

    def test_file_format_is_written_when_set(self):
        assert _build(form_file_format="csv")["file_format"] == "csv"

    def test_unset_knobs_are_absent_not_empty(self):
        """An empty string would override the runner's inference with nothing."""
        config = _build()
        assert "delimiter" not in config
        assert "encoding" not in config
        assert "file_format" not in config

    def test_auto_file_format_is_absent(self):
        """`auto` is the UI's word for "let the runner infer" — not a format."""
        assert "file_format" not in _build(form_file_format="auto")


class _FakeState:
    """Plain stand-in for UploadState's attribute surface.

    Two Reflex facts make this necessary, and both are worth knowing:
    `set_form_source_id` is an **EventHandler**, so calling it off the class
    runs the event machinery rather than the function (`.fn` unwraps it, as
    test_upload_handlers.py does); and Reflex's `__setattr__` needs an
    initialised state, so a bare `__new__` instance rejects the assignments the
    handler makes. Running the real function body against a fake exercises the
    actual classification instead of restating it.
    """

    _extract_conn_type = staticmethod(UploadState._extract_conn_type)

    def __init__(self):
        self.form_source_id = ""
        self.form_is_non_sql_source = False
        self.form_is_saas_source = False
        self.form_is_file_source = False
        self.form_available_endpoints = []
        self.form_selected_endpoints = []


def _select_source(conn_type: str) -> _FakeState:
    state = _FakeState()
    UploadState.set_form_source_id.fn(state, f"1 — src ({conn_type})")
    return state


class TestFileSourceDetection:
    @pytest.mark.parametrize("conn_type", ["csv", "json", "parquet", "s3"])
    def test_file_sources_are_flagged(self, conn_type):
        assert _select_source(conn_type).form_is_file_source is True

    @pytest.mark.parametrize("conn_type", ["postgres", "stripe", "mongodb"])
    def test_non_file_sources_are_not_flagged(self, conn_type):
        assert _select_source(conn_type).form_is_file_source is False


class TestTheFormWritesTheKeysTheRunnerReads:
    """The guard against the core#532 class: a control the loader never receives.

    These assert against the *real* runner helpers rather than restating the key
    names, so renaming one side breaks the test instead of silently breaking the
    feature.
    """

    @staticmethod
    def _pandas_kwargs(reader):
        """The knobs as dlt actually recorded them.

        They arrive nested under `pandas_kwargs` — `read_csv` forwards
        `**pandas_kwargs` — and `sep` is pandas' name for a delimiter. Asserting
        the nesting explicitly matters: a flat `_explicit_args.get("sep")` reads
        fine and is always `None`, so a test written that way passes only via
        whatever fallback sits next to it.
        """
        return reader._explicit_args.get("pandas_kwargs", {})

    def test_delimiter_reaches_the_csv_reader(self):
        reader = _build_format_reader("csv", _build(form_delimiter=";"))
        assert self._pandas_kwargs(reader)["sep"] == ";"

    def test_encoding_reaches_the_csv_reader(self):
        reader = _build_format_reader("csv", _build(form_encoding="windows-1252"))
        assert self._pandas_kwargs(reader)["encoding"] == "windows-1252"

    def test_an_unset_knob_forwards_nothing(self):
        """Guards the assertions above against passing vacuously.

        If the two tests above could pass without the form writing anything,
        this one would fail — pandas would be handed an override it never asked
        for, which is how an unrequested default becomes a silent wrong answer.
        """
        assert self._pandas_kwargs(_build_format_reader("csv", _build())) == {}

    def test_file_format_resolves_a_bare_s3_glob_that_would_otherwise_raise(self):
        """The case core#499 calls out: today's error names a fix with no UI."""
        with pytest.raises(DltRunnerError):
            _resolve_file_format("s3", "*", {})

        config = _build(form_file_format="parquet")
        assert _resolve_file_format("s3", "*", config) == "parquet"
