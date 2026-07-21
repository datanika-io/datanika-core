"""Endpoint tests for ``POST /api/v1/connections/openapi/parse`` (core#416).

The parser and the fetcher are well covered at the unit level; the *route* had
no tests at all. That matters most for the error contract: ``OpenApiImportError``
carries a typed ``code`` precisely so a client (usually an agent) can branch
without regex-matching prose — and nothing checked those codes ever reach the
wire with the right status. A typed contract that is never asserted end-to-end
is decoration.

Also pinned: the response projection. The endpoint reshapes parsed resources
into ``{name, path, method, data_selector, primary_key, columns, summary}``,
and a rename there breaks every agent reading it while every parser test stays
green.

Uses the shared harness lifted into ``conftest.py`` by this same issue.
"""

import pytest

from datanika.services.openapi_import import MAX_SPEC_BYTES, OpenApiImportError
from tests.test_services.conftest import api_headers, patch_api_auth

_URL = "/api/v1/connections/openapi/parse"

_SPEC = """
{
  "openapi": "3.0.0",
  "info": {"title": "Widgets", "version": "1"},
  "servers": [{"url": "https://api.example.com"}],
  "paths": {
    "/widgets": {
      "get": {
        "operationId": "listWidgets",
        "summary": "List widgets",
        "responses": {"200": {"description": "ok", "content": {"application/json": {
          "schema": {"type": "object", "properties": {
            "data": {"type": "array", "items": {"$ref": "#/components/schemas/Widget"}}}}}}}}
      }
    }
  },
  "components": {"schemas": {"Widget": {"type": "object", "required": ["id"],
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}}}}}
}
"""


@pytest.fixture
def authed(fake_api_key, rate_limit_ok, db_session):
    """Context manager putting a valid key behind the middleware."""

    def _cm(**kwargs):
        return patch_api_auth(fake_api_key, rate_limit_ok, db_session, **kwargs)

    return _cm


class TestAuth:
    def test_401_without_a_key(self, client):
        assert client.post(_URL, json={"spec_inline": _SPEC}).status_code == 401

    def test_401_when_the_key_does_not_authenticate(self, client, authed):
        """Unknown, expired, or out-of-scope all land here in the real service."""
        with authed(authenticated=False):
            resp = client.post(_URL, json={"spec_inline": _SPEC}, headers=api_headers())
        assert resp.status_code == 401


class TestTypedErrorCodesReachTheWire:
    """Each parse/fetch failure must arrive as 400 + its documented ``code``."""

    @pytest.mark.parametrize(
        ("payload", "code"),
        [
            ({}, "invalid_request"),
            ({"spec_inline": _SPEC, "spec_url": "https://x.test/s.json"}, "invalid_request"),
            ({"spec_inline": "not json or yaml: ["}, "invalid_spec"),
            ({"spec_inline": '{"swagger": "2.0", "paths": {}}'}, "unsupported_version"),
            ({"spec_url": "http://insecure.test/spec.json"}, "invalid_spec_url"),
            ({"spec_url": "https://127.0.0.1/spec.json"}, "invalid_spec_url"),
        ],
    )
    def test_code_and_status(self, client, authed, payload, code):
        with authed():
            resp = client.post(_URL, json=payload, headers=api_headers())

        assert resp.status_code == 400, resp.text
        assert resp.json()["code"] == code, resp.text

    def test_oversized_spec(self, client, authed):
        with authed():
            resp = client.post(
                _URL, json={"spec_inline": "x" * (MAX_SPEC_BYTES + 1)}, headers=api_headers()
            )
        assert resp.status_code == 400
        assert resp.json()["code"] == "spec_too_large"

    def test_fetch_failure_is_mapped_not_leaked(self, client, authed, monkeypatch):
        """A network failure must surface as the typed code, not a 500."""

        def _boom(url):
            raise OpenApiImportError("Could not fetch the spec: nope", "spec_fetch_failed")

        monkeypatch.setattr("datanika.services.openapi_fetch.fetch_openapi_spec", _boom)
        with authed():
            resp = client.post(
                _URL, json={"spec_url": "https://ok.test/spec.json"}, headers=api_headers()
            )

        assert resp.status_code == 400
        assert resp.json()["code"] == "spec_fetch_failed"


class TestSuccessProjection:
    def test_response_shape(self, client, authed):
        with authed():
            resp = client.post(_URL, json={"spec_inline": _SPEC}, headers=api_headers())

        assert resp.status_code == 200
        body = resp.json()
        assert body["base_url"] == "https://api.example.com"

        resource = body["resources"][0]
        # These key names are the contract an agent reads — pin them.
        assert set(resource) == {
            "name",
            "path",
            "method",
            "data_selector",
            "primary_key",
            "columns",
            "summary",
        }
        assert resource["name"] == "widgets"
        assert resource["method"] == "GET"
        assert resource["data_selector"] == "data"
        assert resource["primary_key"] == "id"
        assert resource["summary"] == "List widgets"
        assert {c["name"] for c in resource["columns"]} == {"id", "name"}

    def test_base_url_override_is_honoured(self, client, authed):
        with authed():
            resp = client.post(
                _URL,
                json={"spec_inline": _SPEC, "base_url": "https://staging.example.com"},
                headers=api_headers(),
            )
        assert resp.json()["base_url"] == "https://staging.example.com"
