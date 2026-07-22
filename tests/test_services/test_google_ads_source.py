"""Google Ads actually moves rows over a real socket (core#555).

The connector was withdrawn on the reading that a developer token was an
insurmountable gate. It is not — it is a string the user pastes, like a service
account JSON. What *was* true is that the form collected neither it nor a usable
OAuth credential, so nothing stored could authenticate.

**Transport was confirmed before building**, as the issue asked. The Ads API is
gRPC-first but ships a documented REST interface; the two alternatives (bundling
the dlt verified source, or the official `google-ads` client) both pull in
`grpcio`/`protobuf`, which is the dependency-conflict class the Dockerfile
deliberately avoids — and a builder importing a package the image lacks is
core#551's bug, not a fallback.

These tests drive the **real** builder against a **real** HTTP server standing in
for Google, because #545's finding was that a connector with only mocked tests is
a connector nobody has shown works. Two shapes here fail silently and both are
asserted directly:

* ``searchStream`` answers with a **JSON array of chunks**, unlike every other
  endpoint in the API — read as a single object it yields zero rows and a green
  run (the core#493 shape);
* each row is **nested**, so unflattened it lands as child tables and the user's
  first ``SELECT clicks`` fails.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import datanika.services.dlt_runner as dlt_runner
from datanika.services.dlt_runner import DltRunnerError, DltRunnerService

CUSTOMER_ID = "123-456-7890"

_CONFIG = {
    "customer_id": CUSTOMER_ID,
    "developer_token": "dev-token-abc",
    "client_id": "client-abc",
    "client_secret": "secret-abc",
    "refresh_token": "refresh-abc",
}


class _FakeGoogle(BaseHTTPRequestHandler):
    """Stands in for both the OAuth token endpoint and googleads."""

    seen: list[dict] = []
    status = 200

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length).decode() if length else ""
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    def do_POST(self):  # noqa: N802 - stdlib callback name
        body = self._read_json()
        type(self).seen.append({"path": self.path, "headers": dict(self.headers), "body": body})

        if self.path.endswith("/token"):
            payload = json.dumps({"access_token": "ya29.fake", "expires_in": 3599}).encode()
            code = 200
        elif type(self).status >= 400:
            payload = json.dumps({"error": {"message": "developer token is not approved"}}).encode()
            code = type(self).status
        else:
            # The array-of-chunks envelope, exactly as the API returns it.
            payload = json.dumps(
                [
                    {
                        "results": [
                            {
                                "campaign": {"id": "111", "name": "Brand"},
                                "segments": {"date": "2026-07-01"},
                                "metrics": {"clicks": "12", "impressions": "340"},
                            }
                        ],
                        "fieldMask": "campaign.id,campaign.name,segments.date",
                        "requestId": "req-1",
                    },
                    {
                        "results": [
                            {
                                "campaign": {"id": "222", "name": "Generic"},
                                "segments": {"date": "2026-07-02"},
                                "metrics": {"clicks": "7", "impressions": "90"},
                            }
                        ],
                        "requestId": "req-2",
                    },
                ]
            ).encode()
            code = 200

        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_google(monkeypatch):
    _FakeGoogle.seen = []
    _FakeGoogle.status = 200
    server = HTTPServer(("127.0.0.1", 0), _FakeGoogle)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_port}"

    monkeypatch.setattr(dlt_runner, "GOOGLE_ADS_API_HOST", base)
    monkeypatch.setattr(dlt_runner, "GOOGLE_OAUTH_TOKEN_URL", f"{base}/token")
    # The egress guard blocks loopback by design and has its own tests; the pin
    # resolves through `resolve_public_ip` since core#441, so both are needed.
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
    )
    try:
        yield base
    finally:
        server.shutdown()
        server.server_close()


def _rows(dlt_config=None):
    source = DltRunnerService()._build_google_ads_source(dict(_CONFIG), dlt_config or {})
    return list(source.resources["report"])


class TestItMovesRows:
    def test_rows_from_every_chunk_arrive(self, fake_google):
        """The array envelope is the whole point: a reader that takes the first
        object, or treats the response as a dict, loses the second campaign."""
        rows = _rows()
        assert {r["campaign_id"] for r in rows} == {"111", "222"}, rows

    def test_nested_rows_are_flattened_into_columns(self, fake_google):
        """Unflattened, `metrics` becomes a child table and `SELECT clicks`
        fails against a report table that has no such column."""
        row = next(r for r in _rows() if r["campaign_id"] == "111")
        assert row["campaign_name"] == "Brand"
        assert row["metrics_clicks"] == "12"
        assert row["segments_date"] == "2026-07-01"
        assert not any(isinstance(v, dict) for v in row.values())


class TestTheRequestIsWhatGoogleRequires:
    def test_developer_token_and_bearer_are_sent(self, fake_google):
        _rows()
        report = next(c for c in _FakeGoogle.seen if "googleAds:searchStream" in c["path"])
        assert report["headers"]["developer-token"] == "dev-token-abc"
        assert report["headers"]["Authorization"] == "Bearer ya29.fake"

    def test_the_refresh_token_is_exchanged_first(self, fake_google):
        _rows()
        token_call = next(c for c in _FakeGoogle.seen if c["path"].endswith("/token"))
        assert "grant_type=refresh_token" in token_call["body"]
        assert "refresh_token=refresh-abc" in token_call["body"]

    def test_customer_id_hyphens_are_stripped(self, fake_google):
        """Users copy `123-456-7890` off the Ads UI because it is the only form
        the UI shows. Sent verbatim it is a 404 blaming the customer ID."""
        _rows()
        report = next(c for c in _FakeGoogle.seen if "googleAds:searchStream" in c["path"])
        assert "/customers/1234567890/" in report["path"]
        assert "123-456-7890" not in report["path"]

    def test_login_customer_id_is_sent_only_when_configured(self, fake_google):
        _rows()
        report = next(c for c in _FakeGoogle.seen if "googleAds:searchStream" in c["path"])
        assert "login-customer-id" not in {k.lower() for k in report["headers"]}

        _FakeGoogle.seen = []
        source = DltRunnerService()._build_google_ads_source(
            {**_CONFIG, "login_customer_id": "999-888-7777"}, {}
        )
        list(source.resources["report"])
        report = next(c for c in _FakeGoogle.seen if "googleAds:searchStream" in c["path"])
        assert report["headers"]["login-customer-id"] == "9998887777"

    def test_the_default_query_is_sent_and_is_overridable(self, fake_google):
        _rows()
        report = next(c for c in _FakeGoogle.seen if "googleAds:searchStream" in c["path"])
        assert "FROM campaign" in report["body"]["query"]

        _FakeGoogle.seen = []
        _rows({"query": "SELECT customer.id FROM customer"})
        report = next(c for c in _FakeGoogle.seen if "googleAds:searchStream" in c["path"])
        assert report["body"]["query"] == "SELECT customer.id FROM customer"

    def test_api_version_is_overridable_because_versions_sunset(self, fake_google):
        """Google ships four majors a year and sunsets each after ~12 months, so
        a pinned version is a dated asset. Overriding it on a live connection is
        the difference between a config change and a release."""
        _rows({"api_version": "v26"})
        report = next(c for c in _FakeGoogle.seen if "googleAds:searchStream" in c["path"])
        assert report["path"].startswith("/v26/customers/")


class TestItFailsLegibly:
    def test_missing_credentials_are_named_before_any_request(self, fake_google):
        with pytest.raises(DltRunnerError) as exc:
            DltRunnerService()._build_google_ads_source({"customer_id": CUSTOMER_ID}, {})
        message = str(exc.value)
        for field in ("developer_token", "client_id", "client_secret", "refresh_token"):
            assert field in message
        assert not _FakeGoogle.seen, "must fail before touching the network"

    def test_googles_own_rejection_is_surfaced(self, fake_google):
        """Asserted on the message that reaches the caller, not on the class.

        dlt wraps anything raised inside a resource in `ResourceExtractionError`
        during extraction, so `pytest.raises(DltRunnerError)` would fail here
        even though the diagnosis is intact. What matters — and what
        `run_upload` stores as the run's `error_message` via `str(exc)` — is
        that Google's own explanation survives the wrapping.
        """
        _FakeGoogle.status = 403
        with pytest.raises(Exception) as exc:  # noqa: B017 - class is dlt's, message is ours
            _rows()
        message = str(exc.value)
        assert "developer token is not approved" in message
        assert "1234567890" in message, "name the customer, the usual second suspect"
        assert "v25" in message, "name the API version — sunsets are a failure mode here"

    def test_build_stays_offline(self, fake_google):
        """`build_source` must construct without credentials being valid or the
        network being reachable — the token is minted inside the resource."""
        DltRunnerService()._build_google_ads_source(dict(_CONFIG), {})
        assert not _FakeGoogle.seen
