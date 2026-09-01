"""The Salesforce connector must load records, not object metadata (core#850).

`GET /services/data/vXX.X/sobjects/Account` is the **sObject Basic Information**
resource. It answers with one object — `{"objectDescribe": {...field metadata...},
"recentItems": [...]}` — and takes no pagination parameters. Those three paths
were the connector's default resources, so a Salesforce upload that ran
successfully landed **describe output**, and `recentItems` on top of it: capped
at 20 and scoped to the *calling user's* recent activity, so two users got
different answers from the same connection.

Nothing failed. dlt received valid JSON and landed a table, so there was a row
count, a green status and a table in the warehouse — it was simply not the data
the user asked for. Same family as core#823 (a green run with the wrong number
of rows) and core#532 (a value the UI accepts and the loader ignores).

Records come from the Query API instead: `GET /services/data/vXX.X/query?q=...`,
answering `{"totalSize": n, "done": bool, "records": [...], "nextRecordsUrl": ...}`.
That also settles the pagination question core#823 had to defer — which is why
`salesforce` moves out of `SAAS_PAGINATION_EXEMPT` in the same change.

🚨 **Not verified against a live Salesforce org.** We hold no Salesforce
credentials (the connector-credential lockers). What is verified here is the
request shape, the pagination walk and the row extraction, against a local
server that answers the documented Query API contract. The claim that needs a
real org is only "these rows are the org's records", and it is stated as owed
rather than met.

⚠️ The load-bearing test serves **more rows than fit in one page**. A fixture
that fits in one response cannot fail, which is exactly how core#823 survived —
and the negative control withholds the paginator against the same server, so a
server that quietly returned everything at once could not make the assertion
vacuous.
"""

import http.server
import json
import threading
import urllib.parse

import pytest

import datanika.services.dlt_runner as dlt_runner
from datanika.services.dlt_runner import DltRunnerService

#: Records the fake org holds. Must exceed PAGE_SIZE.
TOTAL_ACCOUNTS = 7
PAGE_SIZE = 3
MAX_REQUESTS = 10

API_VERSION = dlt_runner.SALESFORCE_API_VERSION


class _SalesforceHandler(http.server.BaseHTTPRequestHandler):
    """The documented Query API contract, and nothing else.

    A request to `sobjects/<Name>` answers **404** on purpose: the describe
    endpoints are what this issue is about, so reaching for one has to be a
    failure rather than a plausible-looking table.
    """

    records = [{"Id": f"001{i:015d}", "Name": f"Account {i}"} for i in range(TOTAL_ACCOUNTS)]
    requests_seen: list[str] = []

    def do_GET(self):  # noqa: N802 - stdlib callback name
        type(self).requests_seen.append(self.path)
        if len(type(self).requests_seen) > MAX_REQUESTS:
            self.send_error(508, "paginator did not terminate")
            return

        parsed = urllib.parse.urlparse(self.path)
        if "/sobjects/" in parsed.path:
            self.send_error(404, "describe endpoint - not a record list")
            return
        if not parsed.path.startswith(f"/services/data/{API_VERSION}/query"):
            self.send_error(404, f"unexpected path {parsed.path}")
            return

        # `/query/<locator>` is the continuation; `/query?q=` is page one.
        tail = parsed.path.split("/query", 1)[1].lstrip("/")
        start = int(tail.split("-")[-1]) if tail else 0

        page = self.records[start : start + PAGE_SIZE]
        nxt = start + PAGE_SIZE
        body = {
            "totalSize": len(self.records),
            "done": nxt >= len(self.records),
            "records": page,
        }
        if nxt < len(self.records):
            body["nextRecordsUrl"] = f"/services/data/{API_VERSION}/query/01g000000000000-{nxt}"
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


@pytest.fixture
def salesforce_api():
    _SalesforceHandler.requests_seen = []
    server = http.server.HTTPServer(("127.0.0.1", 0), _SalesforceHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def local_egress(monkeypatch):
    """The egress guard blocks loopback by design; it has its own tests."""
    monkeypatch.setattr(dlt_runner, "validate_egress_host", lambda url: None)
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
    )


def _accounts(instance_url, dlt_config=None):
    source = DltRunnerService()._build_saas_source(
        "salesforce",
        {"access_token": "not-a-real-token", "instance_url": instance_url},
        dlt_config or {},
    )
    return list(source.resources["accounts"])


class TestSalesforceLoadsRecords:
    def test_it_queries_records_and_never_touches_a_describe_endpoint(
        self, salesforce_api, local_egress
    ):
        """The defect, stated as a request shape.

        Against the unfixed connector every request is `sobjects/<Name>`, which
        this server refuses — so the assertion is on what was asked for, not on
        what came back. A test asserting only "rows landed" passes today, over
        `objectDescribe` and up to 20 `recentItems`.
        """
        rows = _accounts(salesforce_api)

        paths = _SalesforceHandler.requests_seen
        assert paths, "no request was made at all"
        assert not any("/sobjects/" in p for p in paths), (
            f"still calling the describe endpoint: {paths}"
        )
        assert all(f"/services/data/{API_VERSION}/query" in p for p in paths), paths
        assert rows, "the query returned nothing"

    def test_the_column_list_comes_from_fields_standard(self, salesforce_api, local_egress):
        """SOQL has no `SELECT *`, so the columns come from somewhere.

        🚨 `FIELDS(ALL)` and `FIELDS(CUSTOM)` are NOT interchangeable with
        `FIELDS(STANDARD)` here: both require `LIMIT 200` or less, which caps a
        full extract at 200 rows — core#823's silent truncation arriving through
        the query instead of the paginator. This asserts the one that carries no
        such limit, and asserts no LIMIT was added.
        """
        _accounts(salesforce_api)
        first = urllib.parse.parse_qs(
            urllib.parse.urlparse(_SalesforceHandler.requests_seen[0]).query
        )
        soql = first["q"][0]

        assert "FIELDS(STANDARD)" in soql, soql
        assert "FROM Account" in soql, soql
        assert "LIMIT" not in soql.upper(), f"a LIMIT caps the extract: {soql}"

    def test_all_seven_records_arrive_across_the_next_records_url_boundary(
        self, salesforce_api, local_egress
    ):
        """Spans a page boundary, because a fixture that fits cannot fail."""
        rows = _accounts(salesforce_api)

        assert len(rows) == TOTAL_ACCOUNTS, (
            f"loaded {len(rows)} of {TOTAL_ACCOUNTS}; requests: {_SalesforceHandler.requests_seen}"
        )
        assert {r["Id"] for r in rows} == {r["Id"] for r in _SalesforceHandler.records}

    def test_control_without_the_paginator_stops_at_page_one(self, salesforce_api, local_egress):
        """The negative control, and the assertion above is worthless without it.

        Same server, same code, the paginator overridden away. It has to
        reproduce the truncation — otherwise a server that quietly returned
        everything on page one would satisfy the test for the wrong reason.
        """
        rows = _accounts(salesforce_api, {"paginator": {"type": "single_page"}})

        assert len(rows) == PAGE_SIZE, (
            f"expected the unpaginated read to stop at {PAGE_SIZE}, got {len(rows)} — "
            "the paginated assertion above cannot distinguish anything"
        )


class TestTheConnectorIsNoLongerPaginationExempt:
    def test_salesforce_has_a_configured_paginator(self):
        assert "salesforce" in dlt_runner.SAAS_PAGINATORS
        assert "salesforce" not in dlt_runner.SAAS_PAGINATION_EXEMPT

    def test_the_paginator_points_at_next_records_url(self):
        assert dlt_runner.SAAS_PAGINATORS["salesforce"] == {
            "type": "json_link",
            "next_url_path": "nextRecordsUrl",
        }


class TestTheUserSuppliedApiVersionIsValidated:
    """`api_version` comes from `dlt_config`, so it is user input in a URL path.

    Found by bandit's S608 on the SOQL f-string beside it — the linter was right
    about the shape and pointed at the wrong half. The SOQL only ever
    interpolates a module-constant identifier; the *path* interpolates something
    a caller controls.

    The egress guard pins the host, so this is not SSRF. It is still a caller
    aiming the request at a path we did not choose.
    """

    def test_a_traversal_attempt_is_refused(self):
        from datanika.services.dlt_runner import DltRunnerError, _salesforce_default_resources

        with pytest.raises(DltRunnerError, match="api_version"):
            _salesforce_default_resources("../../../v59.0")

    def test_a_plausible_but_malformed_version_is_refused(self):
        from datanika.services.dlt_runner import DltRunnerError, _salesforce_default_resources

        with pytest.raises(DltRunnerError, match="api_version"):
            _salesforce_default_resources("59.0")

    def test_a_real_version_is_accepted(self):
        """Control — a validator that refuses everything is not a validator."""
        from datanika.services.dlt_runner import _salesforce_default_resources

        resources = _salesforce_default_resources("v61.0")
        assert resources[0]["endpoint"]["path"] == "services/data/v61.0/query"

    def test_the_default_version_passes_its_own_validator(self):
        from datanika.services.dlt_runner import (
            SALESFORCE_API_VERSION,
            _salesforce_default_resources,
        )

        assert _salesforce_default_resources(SALESFORCE_API_VERSION)
