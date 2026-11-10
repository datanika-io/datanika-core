"""A rolling Google report re-run keeps each key once and keeps older days (core#1400).

``google_analytics`` and ``google_ads`` fetch a window, not the whole account.
``replace`` would drop days the new window no longer covers. With no disposition
on the resource, dlt appends, so the overlap is stored again.

The witness is ``execute()`` into one DuckDB file, twice, with the Google HTTP
session stubbed. It reads the destination back. A hint check is not this test:
a run-level ``append`` overrides a resource hint, which is how core#1336
doubled rows while the resource looked correct.

Acceptance 3, a custom GAQL query, is not decided here. The Ads witness uses
the default query. refs #1400.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import duckdb

from datanika.services.dlt_runner import DEFAULT_GOOGLE_ADS_QUERY, DltRunnerService
from tests.test_services.test_rerun_lands_each_record_once import form_config

_GA_CONFIG = {"property_id": "123456", "service_account_json": "{}"}
_ADS_CONFIG = {
    "customer_id": "123-456-7890",
    "developer_token": "dev-token-abc",
    "client_id": "client-abc",
    "client_secret": "secret-abc",
    "refresh_token": "refresh-abc",
}

# Two overlapping windows. The first day is absent from the second window, and
# the shared day comes back with a revised metric.
_GA_WINDOWS = {
    ("2026-01-01", "2026-01-02"): [
        ("20260101", "US", "5"),
        ("20260102", "US", "10"),
        ("20260102", "DE", "7"),
    ],
    ("2026-01-02", "2026-01-03"): [
        ("20260102", "US", "11"),
        ("20260102", "DE", "7"),
        ("20260103", "US", "3"),
    ],
}

_ADS_WINDOWS = [
    [
        {"campaign_id": "111", "segments_date": "2026-01-01", "clicks": "1"},
        {"campaign_id": "111", "segments_date": "2026-01-02", "clicks": "2"},
    ],
    [
        {"campaign_id": "111", "segments_date": "2026-01-02", "clicks": "9"},
        {"campaign_id": "111", "segments_date": "2026-01-03", "clicks": "4"},
    ],
]


def _ga_payload(rows: list[tuple[str, str, str]]) -> dict:
    return {
        "dimensionHeaders": [{"name": "date"}, {"name": "country"}],
        "metricHeaders": [{"name": "sessions", "type": "TYPE_INTEGER"}],
        "rows": [
            {
                "dimensionValues": [{"value": date}, {"value": country}],
                "metricValues": [{"value": sessions}],
            }
            for date, country, sessions in rows
        ],
        "rowCount": len(rows),
    }


def _ga_session() -> MagicMock:
    def post(url, json=None, **kwargs):
        window = (
            json["dateRanges"][0]["startDate"],
            json["dateRanges"][0]["endDate"],
        )
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = _ga_payload(_GA_WINDOWS[window])
        return response

    session = MagicMock()
    session.post.side_effect = post
    return session


def _ads_row(campaign_id: str, segments_date: str, clicks: str) -> dict:
    return {
        "campaign": {"id": campaign_id, "name": "Brand", "status": "ENABLED"},
        "segments": {"date": segments_date},
        "metrics": {
            "impressions": "10",
            "clicks": clicks,
            "cost_micros": "5",
            "conversions": "0",
        },
    }


def _ads_session() -> tuple[MagicMock, list]:
    queries: list = []
    payloads = [[{"results": [_ads_row(**row) for row in window]}] for window in _ADS_WINDOWS]

    def post(url, json=None, **kwargs):
        queries.append(json["query"])
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = payloads[len(queries) - 1]
        return response

    session = MagicMock()
    session.post.side_effect = post
    return session, queries


def _run(tmp_path, source_type: str, source_config: dict, dlt_config: dict, *, run_id: int):
    DltRunnerService(pipelines_dir=str(tmp_path / "dlt")).execute(
        pipeline_id=7,
        source_type=source_type,
        source_config=source_config,
        destination_type="duckdb",
        destination_config={"path": str(tmp_path / "dest.duckdb")},
        dlt_config=dlt_config,
        dataset_name="probe",
        run_id=run_id,
    )


def _read(tmp_path, sql: str):
    con = duckdb.connect(str(tmp_path / "dest.duckdb"), read_only=True)
    try:
        loads = con.execute('SELECT count(*) FROM "probe"."_dlt_loads"').fetchone()[0]
        rows = con.execute(sql).fetchall()
    finally:
        con.close()
    return loads, rows


def _stored(source_type: str, **window) -> dict:
    """The form's hidden ``append``, which ``execute`` pops, plus this run's window."""
    return {**form_config(source_type), "write_disposition": "append", **window}


class TestARollingReportKeepsOlderDays:
    def test_two_overlapping_google_analytics_runs_keep_each_row_once(self, tmp_path):
        session = _ga_session()
        windows = (
            {"start_date": "2026-01-01", "end_date": "2026-01-02"},
            {"start_date": "2026-01-02", "end_date": "2026-01-03"},
        )
        with (
            patch("datanika.services.dlt_runner._ga4_access_token", return_value="tok"),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
        ):
            for run_id, window in enumerate(windows, start=1):
                _run(
                    tmp_path,
                    "google_analytics",
                    dict(_GA_CONFIG),
                    _stored(
                        "google_analytics",
                        dimensions=["date", "country"],
                        metrics=["sessions"],
                        **window,
                    ),
                    run_id=run_id,
                )

        loads, rows = _read(
            tmp_path,
            'SELECT "date", country, sessions FROM "probe"."report" ORDER BY 1, 2',
        )
        assert loads == 2
        assert rows == [
            ("20260101", "US", 5),
            ("20260102", "DE", 7),
            ("20260102", "US", 11),
            ("20260103", "US", 3),
        ], rows

    def test_two_overlapping_google_ads_default_queries_keep_each_row_once(self, tmp_path):
        session, queries = _ads_session()
        with (
            patch("datanika.services.dlt_runner._google_ads_access_token", return_value="tok"),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
        ):
            for run_id in (1, 2):
                _run(
                    tmp_path,
                    "google_ads",
                    dict(_ADS_CONFIG),
                    _stored("google_ads"),
                    run_id=run_id,
                )

        assert queries == [DEFAULT_GOOGLE_ADS_QUERY, DEFAULT_GOOGLE_ADS_QUERY]
        loads, rows = _read(
            tmp_path,
            'SELECT campaign_id, segments_date, metrics_clicks FROM "probe"."report" ORDER BY 2, 1',
        )
        assert loads == 2
        assert rows == [
            ("111", "2026-01-01", "1"),
            ("111", "2026-01-02", "9"),
            ("111", "2026-01-03", "4"),
        ], rows
