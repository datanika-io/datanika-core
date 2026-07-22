"""Google Analytics loads via the Data API (core#543, last of the three).

`google_analytics` had no verified source and no fallback, so its
`except ImportError` re-raised *"run `dlt init google_analytics`"* — an
instruction for a server the user does not operate, and the only outcome it
ever produced.

What blocked it was **not a missing library but a missing decision**. `runReport`
takes a POST body naming dimensions and metrics, and a different body is a
different table, so there was no obvious default and nothing got chosen. This
picks daily traffic — the report almost anyone starts from — and makes the rest
overridable per upload.

Two things the generic REST fallback could not have done, both covered here:

* the credential is a **minted OAuth token**, not a static key, so there is
  nothing to hand a declarative config up front;
* the response is **parallel arrays** (`dimensionHeaders`/`metricHeaders` name
  the columns, rows carry values in the same order), which a `data_selector`
  would load as nested lists nobody can query.

**Verified against the documented API shape, not a live property** — there are
no GA credentials here or in CI. What is asserted is what does not depend on
Google: the request we build, the flattening, the paging loop, and that building
a source touches nothing.
"""

from unittest.mock import MagicMock, patch

import pytest

from datanika.services.dlt_runner import (
    DEFAULT_GA4_DIMENSIONS,
    DEFAULT_GA4_METRICS,
    GA4_PAGE_SIZE,
    DltRunnerError,
    DltRunnerService,
    _ga4_rows,
)

CONFIG = {"property_id": "123456", "service_account_json": '{"type": "service_account"}'}


def _payload(rows: int, row_count: int | None = None) -> dict:
    """A `runReport` response in GA4's real shape."""
    return {
        "dimensionHeaders": [{"name": "date"}],
        "metricHeaders": [
            {"name": "sessions", "type": "TYPE_INTEGER"},
            {"name": "bounceRate", "type": "TYPE_FLOAT"},
        ],
        "rows": [
            {
                "dimensionValues": [{"value": f"2026072{i % 10}"}],
                "metricValues": [{"value": str(100 + i)}, {"value": "0.25"}],
            }
            for i in range(rows)
        ],
        "rowCount": rows if row_count is None else row_count,
    }


@pytest.fixture
def svc(tmp_path):
    return DltRunnerService(pipelines_dir=str(tmp_path))


def _drive(svc, dlt_config=None, payloads=None):
    """Build the source and consume it against a stubbed HTTP session.

    Returns (rows, list_of_request_bodies).
    """
    payloads = payloads or [_payload(2)]
    responses = []
    for body in payloads:
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = body
        responses.append(response)

    session = MagicMock()
    session.post.side_effect = responses

    with (
        patch("datanika.services.dlt_runner._ga4_access_token", return_value="tok"),
        patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
    ):
        source = svc.build_source("google_analytics", CONFIG, dlt_config or {})
        rows = list(source)

    bodies = [call.kwargs["json"] for call in session.post.call_args_list]
    return rows, bodies, session


class TestBuildingTouchesNothing:
    def test_a_source_builds_without_credentials_or_network(self, svc):
        """The whole connector used to be unreachable; now it must also be inert.

        Token minting lives inside the resource on purpose — `build_source` is
        called by contract tests and by anything that wants to inspect a
        connector, so it must not require a valid key or a network.
        """
        with patch("datanika.services.dlt_runner._ga4_access_token") as mint:
            source = svc.build_source(
                "google_analytics", {"property_id": "1", "service_account_json": "{}"}, {}
            )

        assert source is not None
        mint.assert_not_called()

    def test_the_resource_is_named_for_the_picker(self, svc):
        """`SAAS_DEFAULT_ENDPOINTS["google_analytics"] == ["report"]`."""
        source = svc.build_source(
            "google_analytics", {"property_id": "1", "service_account_json": "{}"}, {}
        )
        assert list(source.resources.keys()) == ["report"]

    def test_missing_credentials_are_rejected(self, svc):
        with pytest.raises(DltRunnerError, match="service_account_json"):
            svc.build_source("google_analytics", {"property_id": "1"}, {})

    def test_missing_property_is_rejected(self, svc):
        with pytest.raises(DltRunnerError, match="property_id"):
            svc.build_source("google_analytics", {"service_account_json": "{}"}, {})


class TestTheRequestWeBuild:
    def test_it_defaults_to_daily_traffic(self, svc):
        _rows, bodies, _ = _drive(svc)

        body = bodies[0]
        assert [d["name"] for d in body["dimensions"]] == DEFAULT_GA4_DIMENSIONS
        assert [m["name"] for m in body["metrics"]] == DEFAULT_GA4_METRICS

    def test_the_report_shape_is_overridable(self, svc):
        """The default is a choice, not a limit — that was the whole blocker."""
        _rows, bodies, _ = _drive(
            svc, {"dimensions": ["country", "deviceCategory"], "metrics": ["activeUsers"]}
        )

        body = bodies[0]
        assert [d["name"] for d in body["dimensions"]] == ["country", "deviceCategory"]
        assert [m["name"] for m in body["metrics"]] == ["activeUsers"]

    def test_the_date_range_is_overridable(self, svc):
        _rows, bodies, _ = _drive(svc, {"start_date": "2026-01-01", "end_date": "2026-06-30"})

        assert bodies[0]["dateRanges"] == [{"startDate": "2026-01-01", "endDate": "2026-06-30"}]

    def test_it_addresses_the_property_and_authenticates(self, svc):
        _rows, _bodies, session = _drive(svc)

        url = session.post.call_args_list[0].args[0]
        assert url.endswith("/properties/123456:runReport")
        assert session.post.call_args_list[0].kwargs["headers"]["Authorization"] == "Bearer tok"


class TestFlattening:
    def test_headers_are_zipped_onto_values(self):
        rows = _ga4_rows(_payload(1))

        assert rows == [{"date": "20260720", "sessions": 100, "bounceRate": 0.25}]

    def test_metrics_are_typed_not_left_as_text(self):
        """A `sessions` column of strings cannot be summed.

        GA4 returns every metric as a string; loading it that way would be a
        technically successful load that no one can query — the exact failure
        family this issue belongs to.
        """
        rows = _ga4_rows(_payload(1))

        assert isinstance(rows[0]["sessions"], int)
        assert isinstance(rows[0]["bounceRate"], float)

    def test_an_unparseable_metric_is_kept_as_text(self):
        """Losing a value silently is worse than a column needing a cast."""
        payload = _payload(1)
        payload["rows"][0]["metricValues"][0]["value"] = "n/a"

        assert _ga4_rows(payload)[0]["sessions"] == "n/a"

    def test_an_empty_report_is_not_an_error(self):
        assert _ga4_rows({"dimensionHeaders": [], "metricHeaders": [], "rows": []}) == []


class TestPaging:
    def test_a_full_page_asks_for_the_next(self, svc):
        """GA4 truncates at its page size and says so only in `rowCount`.

        Stopping after one page would be a silent short read — the same shape as
        core#492/#493, where the run looks fine and the data is partial.
        """
        first = _payload(GA4_PAGE_SIZE, row_count=GA4_PAGE_SIZE + 3)
        second = _payload(3, row_count=GA4_PAGE_SIZE + 3)

        rows, bodies, _ = _drive(svc, payloads=[first, second])

        assert len(bodies) == 2, "did not request the second page"
        assert bodies[0]["offset"] == 0
        assert bodies[1]["offset"] == GA4_PAGE_SIZE
        assert len(rows) == GA4_PAGE_SIZE + 3

    def test_a_short_page_stops(self, svc):
        _rows, bodies, _ = _drive(svc, payloads=[_payload(2)])

        assert len(bodies) == 1


class TestErrors:
    def test_googles_own_message_is_surfaced(self, svc):
        """A GA4 rejection is usually a property the service account cannot see.

        "Request failed" would send the user to re-check credentials that are
        fine. Asserted on the *message* rather than the exception type: dlt
        wraps anything raised inside a resource in `ResourceExtractionError`, so
        the type does not survive extraction — the text does, and the text is
        what reaches the run log.
        """
        response = MagicMock()
        response.status_code = 403
        response.text = "User does not have sufficient permissions for this property."
        session = MagicMock()
        session.post.return_value = response

        with (
            patch("datanika.services.dlt_runner._ga4_access_token", return_value="tok"),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
            pytest.raises(Exception) as exc,
        ):
            list(svc.build_source("google_analytics", CONFIG, {}))

        assert "sufficient permissions" in str(exc.value), (
            f"Google's explanation did not reach the caller: {exc.value}"
        )
        assert "403" in str(exc.value)

    def test_the_error_type_is_ours_before_dlt_wraps_it(self, svc):
        """Pin the type at the point we raise it, since extraction hides it."""
        response = MagicMock()
        response.status_code = 500
        response.text = "backend error"
        session = MagicMock()
        session.post.return_value = response

        with (
            patch("datanika.services.dlt_runner._ga4_access_token", return_value="tok"),
            patch("datanika.services.dlt_runner.build_guarded_session", return_value=session),
        ):
            resource = svc.build_source("google_analytics", CONFIG, {}).resources["report"]
            with pytest.raises(DltRunnerError, match="backend error"):
                list(resource._pipe.gen())
