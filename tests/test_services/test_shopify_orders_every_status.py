"""A Shopify upload loads orders in every status, not only the open ones (core#1337).

The Shopify branch of ``build_source`` runs the shared REST fallback, because no verified
``shopify_dlt`` module ships. Its default ``orders`` resource requested ``orders.json`` with no
query string. Shopify's REST Admin reference gives ``status`` a default of ``open``, so a run
loaded only the orders open at run time: no closed and no cancelled orders, on a green run with a
plausible row count. QA observed that request on a real store: ``GET
…/admin/api/2024-01/orders.json``, with no query string.

The witness runs the Shopify branch's OWN resource list, headers and paginator through the real
``execute()``, into DuckDB. Only the host is rewritten, to a local server that implements the
documented contract the fix relies on:

* Without ``status``, ``orders.json`` returns open orders only.
* Pages are linked with ``Link: <…page_info=…>; rel="next"``.
* A request carrying ``page_info`` may carry no other parameter except ``limit`` and ``fields``;
  Shopify answers 400. So the filter must ride on the first page only.

The rewrite is asserted, not assumed: the branch must still ask for
``https://{store}.myshopify.com/``.
"""

from __future__ import annotations

import http.server
import json
import threading
import urllib.error
import urllib.parse
import urllib.request

import duckdb
import pytest

from datanika.services.dlt_runner import DltRunnerService
from tests.test_services.test_rerun_lands_each_record_once import form_config

STORE = "probe-store"
PAGE_SIZE = 2
OPEN = {"id": 1001, "name": "#1001", "closed_at": None, "cancelled_at": None}
CLOSED = {"id": 1002, "name": "#1002", "closed_at": "2026-08-01T10:00:00Z", "cancelled_at": None}
CANCELLED = {"id": 1003, "name": "#1003", "closed_at": None, "cancelled_at": "2026-08-02T10:00:00Z"}
ORDERS = [OPEN, CLOSED, CANCELLED]


def _status(order: dict) -> str:
    if order["cancelled_at"]:
        return "cancelled"
    return "closed" if order["closed_at"] else "open"


class _ShopifyStub(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        parsed = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        self.server.paths.append(parsed.path)
        if parsed.path.endswith("/orders.json"):
            self.server.order_requests.append(query)
            self._orders(parsed.path, query)
        elif parsed.path.endswith("/products.json"):
            self._json({"products": [{"id": 1, "title": "probe"}]})
        elif parsed.path.endswith("/customers.json"):
            self._json({"customers": [{"id": 1, "email": "probe@example.com"}]})
        else:
            self.send_error(404)

    def _orders(self, path: str, query: dict) -> None:
        if "page_info" in query:
            if set(query) - {"page_info", "limit", "fields"}:
                # Shopify's documented rule for cursor pagination.
                self._json({"errors": {"page_info": "Invalid value."}}, status=400)
                return
            self._json({"orders": ORDERS[PAGE_SIZE:]})
            return
        wanted = query.get("status", ["open"])[0]
        matching = [o for o in ORDERS if wanted == "any" or _status(o) == wanted]
        headers = {}
        if len(matching) > PAGE_SIZE:
            host = self.headers["Host"]
            headers["Link"] = f'<http://{host}{path}?limit={PAGE_SIZE}&page_info=p2>; rel="next"'
        self._json({"orders": matching[:PAGE_SIZE]}, headers=headers)

    def _json(self, payload: dict, *, status: int = 200, headers: dict | None = None) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def shopify_stub():
    server = http.server.HTTPServer(("127.0.0.1", 0), _ShopifyStub)
    server.order_requests = []
    server.paths = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def allow_loopback(monkeypatch):
    """The three patches `test_source_builders_move_rows.allow_loopback` documents."""
    monkeypatch.setattr("datanika.services.dlt_runner.validate_egress_host", lambda url: None)
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
    )


@pytest.fixture
def host_rewritten_to_stub(monkeypatch, shopify_stub, allow_loopback):
    """Send the Shopify branch's fallback to the stub, keeping everything else the branch built."""
    seen: dict = {}
    original = DltRunnerService._rest_api_fallback.__func__
    stub_url = f"http://127.0.0.1:{shopify_stub.server_port}/"

    def fallback(cls, base_url, auth, resources, *, paginator, headers=None):
        seen["base_url"] = base_url
        return original(cls, stub_url, auth, resources, paginator=paginator, headers=headers)

    monkeypatch.setattr(DltRunnerService, "_rest_api_fallback", classmethod(fallback))
    return seen


def _order_ids(tmp_path) -> list[int]:
    con = duckdb.connect(str(tmp_path / "dest.duckdb"))
    try:
        rows = con.execute('SELECT id FROM "probe"."orders" ORDER BY id').fetchall()
    finally:
        con.close()
    return [row[0] for row in rows]


def _run(tmp_path) -> None:
    DltRunnerService(pipelines_dir=str(tmp_path / "dlt")).execute(
        pipeline_id=7,
        source_type="shopify",
        source_config={"store": STORE, "api_key": "shpat_probe"},
        destination_type="duckdb",
        destination_config={"path": str(tmp_path / "dest.duckdb")},
        dlt_config=form_config("shopify"),
        dataset_name="probe",
        run_id=1,
    )


class TestEveryOrderStatusLands:
    def test_open_closed_and_cancelled_orders_all_land(
        self, tmp_path, shopify_stub, host_rewritten_to_stub
    ):
        _run(tmp_path)

        assert host_rewritten_to_stub["base_url"] == f"https://{STORE}.myshopify.com/"
        assert _order_ids(tmp_path) == [1001, 1002, 1003], (
            "a Shopify upload must load orders in every status; Shopify's default is open only"
        )

    def test_the_filter_rides_on_the_first_page_and_the_next_page_still_loads(
        self, tmp_path, shopify_stub, host_rewritten_to_stub
    ):
        _run(tmp_path)

        first, *later = shopify_stub.order_requests
        assert first.get("status") == ["any"], first
        assert later, "the stub served a second page; the loader never asked for it"
        assert all(set(q) <= {"page_info", "limit", "fields"} for q in later), later


class TestTheDefaultResourcesShareOneApiVersion:
    def test_every_default_resource_requests_the_pinned_version(
        self, tmp_path, shopify_stub, host_rewritten_to_stub
    ):
        """One supported version for all three resources, read from one constant.

        Shopify serves the oldest version it still supports in place of a retired pin, with no
        error. A path left on an old version therefore changes the response shape without anyone
        choosing it.
        """
        from datanika.services import dlt_runner

        version = getattr(dlt_runner, "SHOPIFY_API_VERSION", None)
        _run(tmp_path)

        names = ("orders", "products", "customers")
        expected = {f"/admin/api/{version}/{name}.json" for name in names}
        assert version, "the Shopify API version is not pinned in one place"
        assert set(shopify_stub.paths) == expected, sorted(set(shopify_stub.paths))


class TestTheStubKeepsShopifysRules:
    def test_anti_vacuity_page_info_with_another_parameter_is_refused(self, shopify_stub):
        """Otherwise a loader re-sending `status` on page two would pass unnoticed."""
        url = (
            f"http://127.0.0.1:{shopify_stub.server_port}"
            "/admin/api/x/orders.json?page_info=p2&status=any"
        )
        with pytest.raises(urllib.error.HTTPError) as refused:
            urllib.request.urlopen(url)  # noqa: S310 - a loopback test server

        assert refused.value.code == 400

    def test_anti_vacuity_no_status_means_open_only(self, shopify_stub):
        url = f"http://127.0.0.1:{shopify_stub.server_port}/admin/api/x/orders.json"
        with urllib.request.urlopen(url) as response:  # noqa: S310 - a loopback test server
            payload = json.load(response)

        assert [o["id"] for o in payload["orders"]] == [1001]
