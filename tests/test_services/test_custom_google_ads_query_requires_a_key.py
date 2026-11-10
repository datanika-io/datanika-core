"""A custom Google Ads query merges only on merge_config.report.primary_key (core#1400).

A custom query is the text of ``str(dlt_config.get("query") or DEFAULT_GOOGLE_ADS_QUERY).strip()``
when that text is not exactly ``DEFAULT_GOOGLE_ADS_QUERY``. A missing or empty query is the
default. The comparison is that string.

The key is ``merge_config.report.primary_key``, flattened column names. With no usable key the
run raises ``DltRunnerError`` naming that field before the token exchange and before the report
request, and the destination gets no report rows. ``mode`` does not waive the key: these loads
use ``single_table``, which is the mode that skips the generic ``merge_config`` hints.

An ``append`` or ``replace`` sent without ``mode`` is the raw-JSON choice and does not need the
key. The default query's own key is acceptance 1 and 2, on another branch. This file does not
assert that key. refs #1400.
"""

from __future__ import annotations

import copy
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from datanika.services.dlt_runner import (
    DEFAULT_GOOGLE_ADS_QUERY,
    DltRunnerError,
    DltRunnerService,
)

_ADS_CONFIG = {
    "customer_id": "123-456-7890",
    "developer_token": "dev-token-abc",
    "client_id": "client-abc",
    "client_secret": "secret-abc",
    "refresh_token": "refresh-abc",
}

# Same grain as the default report, but a different window, so the text is not the default.
CUSTOM_QUERY = (
    "SELECT campaign.id, segments.date, metrics.clicks FROM campaign "
    "WHERE segments.date DURING LAST_7_DAYS"
)

_KEY = ["campaign_id", "segments_date"]


def _ads_row(campaign_id: str, segments_date: str | None, clicks: str, *, with_date: bool = True):
    row = {
        "campaign": {"id": campaign_id, "name": "Brand"},
        "metrics": {"clicks": clicks},
    }
    if with_date:
        row["segments"] = {"date": segments_date}
    return row


def _chunk(rows: list[dict]) -> list[dict]:
    return [{"results": rows}]


def _window(*rows: dict) -> list[dict]:
    return _chunk(list(rows))


def _session(payloads: list) -> tuple[MagicMock, list]:
    queries: list = []

    def post(url, json=None, **kwargs):
        queries.append(None if json is None else json.get("query"))
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = payloads[len(queries) - 1]
        return response

    session = MagicMock()
    session.post.side_effect = post
    return session, queries


def _run(tmp_path, dlt_config: dict, *, run_id: int) -> None:
    DltRunnerService(pipelines_dir=str(tmp_path / "dlt")).execute(
        pipeline_id=7,
        source_type="google_ads",
        source_config=copy.deepcopy(_ADS_CONFIG),
        destination_type="duckdb",
        destination_config={"path": str(tmp_path / "dest.duckdb")},
        dlt_config=copy.deepcopy(dlt_config),
        dataset_name="probe",
        run_id=run_id,
    )


def _report_count(tmp_path) -> int:
    path = tmp_path / "dest.duckdb"
    if not path.exists():
        return 0
    con = duckdb.connect(str(path), read_only=True)
    try:
        found = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'probe' AND table_name = 'report'"
        ).fetchone()
        if found is None:
            return 0
        return con.execute('SELECT count(*) FROM "probe"."report"').fetchone()[0]
    finally:
        con.close()


def _report_rows(tmp_path):
    con = duckdb.connect(str(tmp_path / "dest.duckdb"), read_only=True)
    try:
        loads = con.execute('SELECT count(*) FROM "probe"."_dlt_loads"').fetchone()[0]
        rows = con.execute(
            "SELECT campaign_id, segments_date, metrics_clicks "
            'FROM "probe"."report" ORDER BY segments_date, campaign_id'
        ).fetchall()
    finally:
        con.close()
    return loads, rows


def _form(query: str = CUSTOM_QUERY, **extra) -> dict:
    """``mode`` plus ``append`` is the form's stored default, not a disposition choice."""
    config = {"mode": "single_table", "write_disposition": "append", "query": query}
    config.update(extra)
    return config


class TestACustomQueryWithoutAKeyFailsBeforeGoogle:
    def test_no_key_fails_before_any_google_call(self, tmp_path):
        session, _queries = _session([_window(_ads_row("111", "2026-01-02", "2"))])
        token = MagicMock(return_value="tok")
        with (
            patch("datanika.services.dlt_runner._google_ads_access_token", token),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
            pytest.raises(DltRunnerError, match="merge_config\\.report\\.primary_key"),
        ):
            _run(tmp_path, _form(), run_id=1)

        token.assert_not_called()
        session.post.assert_not_called()
        assert _report_count(tmp_path) == 0

    @pytest.mark.parametrize(
        "dlt_config",
        [
            pytest.param(_form(mode="full_database"), id="full_database"),
            pytest.param(_form(primary_key="campaign_id"), id="top_level_primary_key"),
            pytest.param(
                _form(merge_config={"report": {"primary_key": []}}),
                id="empty_list",
            ),
            pytest.param(
                _form(merge_config={"report": {"primary_key": "  "}}),
                id="blank_string",
            ),
            pytest.param(
                _form(merge_config={"report": {"primary_key": ["campaign_id", ""]}}),
                id="blank_part",
            ),
            pytest.param(
                {"mode": "single_table", "write_disposition": "replace", "query": CUSTOM_QUERY},
                id="replace_stored_with_mode",
            ),
            pytest.param({"mode": "single_table", "query": "   "}, id="whitespace_query"),
        ],
    )
    def test_these_are_not_a_key(self, tmp_path, dlt_config):
        session, _queries = _session([_window(_ads_row("111", "2026-01-02", "2"))])
        token = MagicMock(return_value="tok")
        with (
            patch("datanika.services.dlt_runner._google_ads_access_token", token),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
            pytest.raises(DltRunnerError, match="merge_config\\.report\\.primary_key"),
        ):
            _run(tmp_path, dlt_config, run_id=1)

        token.assert_not_called()
        session.post.assert_not_called()
        assert _report_count(tmp_path) == 0


class TestAPresentKeyMergesOnThatKey:
    def test_single_table_updates_the_overlap_and_keeps_the_older_day(self, tmp_path):
        payloads = [
            _window(
                _ads_row("111", "2026-01-01", "1"),
                _ads_row("111", "2026-01-02", "2"),
            ),
            _window(
                _ads_row("111", "2026-01-02", "9"),
                _ads_row("111", "2026-01-03", "4"),
            ),
        ]
        session, queries = _session(payloads)
        config = _form(
            merge_config={"report": {"primary_key": list(_KEY)}},
            primary_key="campaign_name",
        )
        with (
            patch("datanika.services.dlt_runner._google_ads_access_token", return_value="tok"),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
        ):
            for run_id in (1, 2):
                _run(tmp_path, config, run_id=run_id)

        assert queries == [CUSTOM_QUERY, CUSTOM_QUERY]
        loads, rows = _report_rows(tmp_path)
        assert loads == 2
        assert rows == [
            ("111", "2026-01-01", "1"),
            ("111", "2026-01-02", "9"),
            ("111", "2026-01-03", "4"),
        ], rows

    def test_a_string_key_merges_on_that_column(self, tmp_path):
        payloads = [
            _window(
                _ads_row("111", "2026-01-01", "1"),
                _ads_row("222", "2026-01-01", "3"),
            ),
            _window(_ads_row("111", "2026-01-02", "9")),
        ]
        session, queries = _session(payloads)
        config = _form(merge_config={"report": {"primary_key": "campaign_id"}})
        with (
            patch("datanika.services.dlt_runner._google_ads_access_token", return_value="tok"),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
        ):
            for run_id in (1, 2):
                _run(tmp_path, config, run_id=run_id)

        assert queries == [CUSTOM_QUERY, CUSTOM_QUERY]
        loads, rows = _report_rows(tmp_path)
        assert loads == 2
        assert rows == [
            ("222", "2026-01-01", "3"),
            ("111", "2026-01-02", "9"),
        ], rows


class TestARowMissingANamedKeyColumnFails:
    @pytest.mark.parametrize(
        "row",
        [
            pytest.param(_ads_row("111", None, "2", with_date=False), id="absent"),
            pytest.param(_ads_row("111", "", "2"), id="empty"),
            pytest.param(_ads_row("111", None, "2"), id="none"),
        ],
    )
    def test_the_column_is_named_and_nothing_is_loaded(self, tmp_path, row):
        payload = _chunk([_ads_row("111", "2026-01-01", "1"), row])
        session, queries = _session([payload])
        config = _form(merge_config={"report": {"primary_key": list(_KEY)}})
        with (
            patch("datanika.services.dlt_runner._google_ads_access_token", return_value="tok"),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
            pytest.raises(Exception, match="segments_date"),
        ):
            _run(tmp_path, config, run_id=1)

        assert queries == [CUSTOM_QUERY]
        assert _report_count(tmp_path) == 0


class TestAnExplicitDispositionWithoutModeIsTheRawChoice:
    def test_append_keeps_every_run_without_a_key(self, tmp_path):
        payloads = [
            _window(
                _ads_row("111", "2026-01-01", "1"),
                _ads_row("111", "2026-01-02", "2"),
            ),
            _window(
                _ads_row("111", "2026-01-02", "9"),
                _ads_row("111", "2026-01-03", "4"),
            ),
        ]
        session, queries = _session(payloads)
        config = {"write_disposition": "append", "query": CUSTOM_QUERY}
        with (
            patch("datanika.services.dlt_runner._google_ads_access_token", return_value="tok"),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
        ):
            for run_id in (1, 2):
                _run(tmp_path, config, run_id=run_id)

        assert queries == [CUSTOM_QUERY, CUSTOM_QUERY]
        loads, rows = _report_rows(tmp_path)
        assert loads == 2
        assert rows == [
            ("111", "2026-01-01", "1"),
            ("111", "2026-01-02", "2"),
            ("111", "2026-01-02", "9"),
            ("111", "2026-01-03", "4"),
        ], rows

    def test_replace_keeps_only_the_latest_window_without_a_key(self, tmp_path):
        payloads = [
            _window(
                _ads_row("111", "2026-01-01", "1"),
                _ads_row("111", "2026-01-02", "2"),
            ),
            _window(
                _ads_row("111", "2026-01-02", "9"),
                _ads_row("111", "2026-01-03", "4"),
            ),
        ]
        session, _queries = _session(payloads)
        config = {"write_disposition": "replace", "query": CUSTOM_QUERY}
        with (
            patch("datanika.services.dlt_runner._google_ads_access_token", return_value="tok"),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
        ):
            for run_id in (1, 2):
                _run(tmp_path, config, run_id=run_id)

        loads, rows = _report_rows(tmp_path)
        assert loads == 2
        assert rows == [
            ("111", "2026-01-02", "9"),
            ("111", "2026-01-03", "4"),
        ], rows


class TestAMissingOrEmptyQueryIsTheDefault:
    @pytest.mark.parametrize(
        "dlt_config",
        [
            pytest.param({"mode": "single_table", "write_disposition": "append"}, id="missing"),
            pytest.param(_form(query=""), id="empty"),
            pytest.param(
                _form(query=f"  {DEFAULT_GOOGLE_ADS_QUERY}\n"),
                id="surrounding_whitespace",
            ),
        ],
    )
    def test_it_loads_without_a_key(self, tmp_path, dlt_config):
        session, queries = _session([_window(_ads_row("111", "2026-01-02", "2"))])
        with (
            patch("datanika.services.dlt_runner._google_ads_access_token", return_value="tok"),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
        ):
            _run(tmp_path, dlt_config, run_id=1)

        assert queries == [DEFAULT_GOOGLE_ADS_QUERY]
        assert _report_count(tmp_path) == 1
