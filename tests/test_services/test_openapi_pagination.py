"""Spec-driven pagination detection (core#411, OpenAPI P2).

P1 emitted no paginator and leaned on dlt's *runtime* auto-detection. When that
guesses wrong the extract silently stops after page 1 — which surfaces as an
empty or short table, not an error. That is the same silent-green class as the
MCP deadlock (a 200 carrying an error string) and the e2e teardown (an opaque
setup failure): the pipeline reports success and the data is wrong.

So the load-bearing test here is **behavioural**, not structural:
``TestPaginationActuallyPagesThroughData`` serves two real pages over a real
socket and asserts a row that **cannot appear on page one** arrives. A test that
only asserts the emitted config "looks right" passes just as happily against a
paginator that never advances — which is precisely the bug being prevented.

Emitted configs are the shapes dlt actually accepts (verified against
``dlt.sources.rest_api.config_setup.PAGINATOR_MAP`` and the paginator
constructors), not invented ones.
"""

import http.server
import json
import threading

import pytest

from datanika.services.openapi_import import parse_openapi_spec


def _spec(*, params=None, response_props=None, headers=None, items_key="data") -> dict:
    """A minimal one-collection spec with pluggable pagination hints."""
    props = {items_key: {"type": "array", "items": {"$ref": "#/components/schemas/Widget"}}}
    props.update(response_props or {})
    response: dict = {
        "description": "ok",
        "content": {"application/json": {"schema": {"type": "object", "properties": props}}},
    }
    if headers:
        response["headers"] = headers
    return {
        "openapi": "3.0.0",
        "info": {"title": "t", "version": "1"},
        "servers": [{"url": "https://api.example.com"}],
        "paths": {
            "/widgets": {
                "get": {
                    "operationId": "listWidgets",
                    "parameters": params or [],
                    "responses": {"200": response},
                }
            }
        },
        "components": {
            "schemas": {
                "Widget": {
                    "type": "object",
                    "required": ["id"],
                    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
                }
            }
        },
    }


def _query(name: str, **schema) -> dict:
    return {"name": name, "in": "query", "schema": schema or {"type": "integer"}}


def _paginator_of(spec: dict) -> dict | None:
    parsed = parse_openapi_spec(spec)
    return parsed.resources[0]["endpoint"].get("paginator")


class TestDetection:
    def test_offset_limit_params(self):
        pag = _paginator_of(_spec(params=[_query("offset"), _query("limit")]))
        assert pag["type"] == "offset"
        assert pag["offset_param"] == "offset"
        assert pag["limit_param"] == "limit"

    def test_offset_limit_uses_the_specs_declared_maximum(self):
        """Respect the API's own ceiling instead of guessing a page size."""
        pag = _paginator_of(
            _spec(params=[_query("offset"), _query("limit", type="integer", maximum=250)])
        )
        assert pag["limit"] == 250

    def test_skip_top_are_recognised_as_offset(self):
        """OData-style names are the same pattern under different words."""
        pag = _paginator_of(_spec(params=[_query("skip"), _query("top")]))
        assert pag["type"] == "offset"
        assert pag["offset_param"] == "skip"

    def test_page_number_params(self):
        pag = _paginator_of(_spec(params=[_query("page"), _query("per_page")]))
        assert pag["type"] == "page_number"
        assert pag["page_param"] == "page"

    def test_json_link_beats_query_params(self):
        """A next-URL in the body is authoritative; params are inference."""
        pag = _paginator_of(
            _spec(
                params=[_query("page")],
                response_props={"next": {"type": "string"}},
            )
        )
        assert pag["type"] == "json_link"
        assert pag["next_url_path"] == "next"

    def test_link_header(self):
        pag = _paginator_of(_spec(headers={"Link": {"schema": {"type": "string"}}}))
        assert pag["type"] == "header_link"

    def test_cursor_param_with_matching_response_field(self):
        pag = _paginator_of(
            _spec(
                params=[_query("cursor", type="string")],
                response_props={"next_cursor": {"type": "string"}},
            )
        )
        assert pag["type"] == "cursor"
        assert pag["cursor_param"] == "cursor"
        assert pag["cursor_path"] == "next_cursor"

    def test_total_path_only_when_the_spec_declares_one(self):
        """OffsetPaginator defaults total_path='total'; a spec without that
        field must get None or dlt looks for a key that never arrives."""
        without = _paginator_of(_spec(params=[_query("offset"), _query("limit")]))
        assert without["total_path"] is None

        with_total = _paginator_of(
            _spec(
                params=[_query("offset"), _query("limit")],
                response_props={"total": {"type": "integer"}},
            )
        )
        assert with_total["total_path"] == "total"

    def test_no_hints_emits_no_paginator(self):
        """Silence beats a guess — dlt's runtime auto-detection still applies."""
        assert _paginator_of(_spec()) is None


class _PagedHandler(http.server.BaseHTTPRequestHandler):
    """Two pages of widgets, offset/limit style. Page 2 holds id=99."""

    def do_GET(self):  # noqa: N802 - stdlib callback name
        offset = 0
        if "offset=" in self.path:
            offset = int(self.path.split("offset=")[1].split("&")[0])
        rows = [{"id": 1, "name": "first"}] if offset == 0 else []
        if offset == 1:
            rows = [{"id": 99, "name": "only-on-page-two"}]
        body = json.dumps({"data": rows}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def paged_api():
    server = http.server.HTTPServer(("127.0.0.1", 0), _PagedHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


class TestPaginationActuallyPagesThroughData:
    def test_second_page_rows_arrive(self, paged_api, monkeypatch):
        """The row on page two cannot be produced by a paginator that never
        advances, so this fails on exactly the bug we are preventing.

        Driven through ``DltRunnerService._build_openapi_source`` — the real
        production path — rather than hand-assembling a dlt config. The parser
        emits a *catalog* (with our private ``columns``/``_source`` keys) that
        dlt rejects outright until the runtime strips them, so a test that
        built its own config would prove the parser works in a shape nothing
        actually uses.
        """
        import datanika.services.dlt_runner as dlt_runner

        # The egress guard blocks loopback by design; it has its own tests.
        monkeypatch.setattr(dlt_runner, "validate_egress_host", lambda url: None)
        monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)

        parsed = parse_openapi_spec(
            _spec(params=[_query("offset"), _query("limit", type="integer", maximum=1)]),
            base_url_override=paged_api,
        )
        source = dlt_runner.DltRunnerService()._build_openapi_source(
            {"base_url": paged_api, "resources": parsed.resources}, {}
        )
        rows = list(source.resources["widgets"])

        ids = {r["id"] for r in rows}
        assert 1 in ids, f"page one missing: {rows}"
        assert 99 in ids, f"never advanced past page one: {rows}"

    def test_control_without_hints_stops_at_page_one(self, paged_api, monkeypatch):
        """Proves the test above discriminates.

        If dlt's runtime auto-detection reached page two on its own, the
        assertion above would pass no matter what this parser emitted — a green
        that measures nothing. With no offset/limit params declared, no
        paginator is emitted and the extract must stop at page one.
        """
        import datanika.services.dlt_runner as dlt_runner

        monkeypatch.setattr(dlt_runner, "validate_egress_host", lambda url: None)
        monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)

        parsed = parse_openapi_spec(_spec(), base_url_override=paged_api)
        assert parsed.resources[0]["endpoint"].get("paginator") is None
        source = dlt_runner.DltRunnerService()._build_openapi_source(
            {"base_url": paged_api, "resources": parsed.resources}, {}
        )
        rows = list(source.resources["widgets"])

        assert {r["id"] for r in rows} == {1}, f"expected page one only, got {rows}"
