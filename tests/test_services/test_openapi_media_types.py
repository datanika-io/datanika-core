"""A JSON response is JSON whatever parameters its media type carries (core#1345).

What QA measured, 2026-09-15
----------------------------
FakeRESTApi's published spec (Swashbuckle, OpenAPI 3.0.1, sha256 `c0367244…`) declares every
collection GET under `text/plain; v=1.0`, `application/json; v=1.0` and `text/json; v=1.0`. That
is what Swashbuckle emits when ASP.NET API versioning is on. `parse_openapi_spec` returned
**0 resources** and 12 warnings, each `Skipped GET … — no array/collection JSON response schema.`
A minimal spec with plain `application/json`, in the same container, returned 1.

The mechanism
-------------
The parser read a response's media-type map by the exact key `"application/json"`, in TWO places:
`_response_item_schema`, which finds the rows, and `_resource_from_get`'s envelope lookup, which
feeds pagination detection. So the tests below fail if either lookup is left behind: the resource
tests catch the first, and the `total_path` test catches the second.

Media types compare case-insensitively on type and subtype, and their parameters are not part of
the type (RFC 9110 §8.3.1). The `+json` structured-syntax suffix (RFC 6839) is JSON too.
"""

from __future__ import annotations

import pytest

from datanika.services.openapi_import import parse_openapi_spec

WIDGET = {
    "type": "object",
    "required": ["id"],
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
}

OFFSET_LIMIT = [
    {"name": "offset", "in": "query", "schema": {"type": "integer"}},
    {"name": "limit", "in": "query", "schema": {"type": "integer"}},
]


def _spec(content: dict, *, params: list | None = None) -> dict:
    """A one-collection spec whose 200 response declares exactly ``content``."""
    return {
        "openapi": "3.0.1",
        "info": {"title": "t", "version": "1"},
        "servers": [{"url": "https://api.example.com"}],
        "paths": {
            "/api/v1/Widgets": {
                "get": {
                    "operationId": "listWidgets",
                    "parameters": params or [],
                    "responses": {"200": {"description": "ok", "content": content}},
                }
            }
        },
    }


def _array() -> dict:
    return {"schema": {"type": "array", "items": WIDGET}}


def _names(spec: dict) -> list[str]:
    """Assert on the resource list, never on the absence of a warning: a parser that returns
    nothing also returns no resource-specific warning."""
    return [resource["name"] for resource in parse_openapi_spec(spec).resources]


class TestJsonMediaTypesAreRecognisedByTypeAndSubtype:
    @pytest.mark.parametrize(
        "media_type",
        [
            "application/json; v=1.0",
            "application/json; charset=utf-8",
            "application/json;v=1.0",
            "Application/JSON; v=1.0",
        ],
    )
    def test_a_parameterised_json_type_yields_the_resource(self, media_type):
        assert _names(_spec({media_type: _array()})) == ["widgets"]

    @pytest.mark.parametrize("media_type", ["application/vnd.api+json", "application/hal+json"])
    def test_a_structured_json_suffix_yields_the_resource(self, media_type):
        assert _names(_spec({media_type: _array()})) == ["widgets"]

    def test_the_swashbuckle_shape_qa_measured(self):
        """FakeRESTApi's shape: three media types, and none of them is bare application/json."""
        content = {
            "text/plain; v=1.0": _array(),
            "application/json; v=1.0": _array(),
            "text/json; v=1.0": _array(),
        }
        assert _names(_spec(content)) == ["widgets"]

    def test_json_is_chosen_over_a_non_json_type_listed_first(self):
        """Document order must not decide: a `text/plain` entry with a string schema comes first."""
        content = {
            "text/plain; v=1.0": {"schema": {"type": "string"}},
            "application/json; v=1.0": _array(),
        }
        assert _names(_spec(content)) == ["widgets"]

    def test_control_plain_application_json_still_yields_the_resource(self):
        assert _names(_spec({"application/json": _array()})) == ["widgets"]

    def test_control_a_non_json_type_alone_yields_nothing(self):
        """So the tests above cannot pass by accepting every content type."""
        assert _names(_spec({"text/plain; v=1.0": _array()})) == []


class TestTheEnvelopeLookupUsesTheSameRule:
    """`_resource_from_get` reads the response a second time, for pagination hints."""

    def test_total_path_is_found_under_a_parameterised_json_type(self):
        envelope = {
            "schema": {
                "type": "object",
                "properties": {
                    "data": {"type": "array", "items": WIDGET},
                    "total": {"type": "integer"},
                },
            }
        }
        spec = _spec({"application/json; v=1.0": envelope}, params=OFFSET_LIMIT)

        resource = parse_openapi_spec(spec).resources[0]

        assert resource["endpoint"]["paginator"]["total_path"] == "total", (
            "the rows were found under `application/json; v=1.0` but the envelope was read by the "
            "exact key, so dlt would look for no total and page by guesswork"
        )

    def test_control_the_same_envelope_under_plain_json(self):
        envelope = {
            "schema": {
                "type": "object",
                "properties": {
                    "data": {"type": "array", "items": WIDGET},
                    "total": {"type": "integer"},
                },
            }
        }
        spec = _spec({"application/json": envelope}, params=OFFSET_LIMIT)

        resource = parse_openapi_spec(spec).resources[0]

        assert resource["endpoint"]["paginator"]["total_path"] == "total"
