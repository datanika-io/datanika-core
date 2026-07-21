"""OpenAPI P2 — nested data-selector, incremental cursor, Swagger 2.0 (core#411).

The three slices left after pagination. Each is deterministic parser work, so
these are fixture-driven — but each one guards a *silent* failure, which is why
the assertions are about extracted data rather than "it didn't raise":

- a missed **nested** envelope yields zero rows from a healthy API;
- a missed **incremental** cursor re-extracts everything every run, which looks
  like success and quietly burns the customer's byte quota;
- a rejected **Swagger 2.0** spec is the one honest failure of the three — the
  user is told no — which is why it was the cheapest to leave until last.
"""

import pytest

from datanika.services.openapi_import import OpenApiImportError, parse_openapi_spec


def _openapi3(paths: dict, components: dict | None = None) -> dict:
    return {
        "openapi": "3.0.0",
        "info": {"title": "t", "version": "1"},
        "servers": [{"url": "https://api.example.com"}],
        "paths": paths,
        "components": components or {},
    }


_WIDGET = {
    "type": "object",
    "required": ["id"],
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "updated_at": {"type": "string", "format": "date-time"},
    },
}


def _get_op(response_schema: dict, params: list | None = None, extra: dict | None = None) -> dict:
    op = {
        "operationId": "listWidgets",
        "parameters": params or [],
        "responses": {
            "200": {
                "description": "ok",
                "content": {"application/json": {"schema": response_schema}},
            }
        },
    }
    op.update(extra or {})
    return {"get": op}


class TestNestedDataSelector:
    """A collection one level down is a common shape and yielded nothing."""

    def test_nested_envelope_produces_a_dotted_selector(self):
        schema = {
            "type": "object",
            "properties": {
                "response": {
                    "type": "object",
                    "properties": {"items": {"type": "array", "items": _WIDGET}},
                }
            },
        }
        parsed = parse_openapi_spec(_openapi3({"/widgets": _get_op(schema)}))

        resource = parsed.resources[0]
        assert resource["endpoint"]["data_selector"] == "response.items"
        # And the columns still come from the item schema, not the envelope.
        assert {c["name"] for c in resource["columns"]} == {"id", "name", "updated_at"}

    def test_top_level_array_still_needs_no_selector(self):
        parsed = parse_openapi_spec(
            _openapi3({"/widgets": _get_op({"type": "array", "items": _WIDGET})})
        )
        assert parsed.resources[0]["endpoint"].get("data_selector") is None

    def test_shallow_envelope_is_unchanged(self):
        schema = {"type": "object", "properties": {"data": {"type": "array", "items": _WIDGET}}}
        parsed = parse_openapi_spec(_openapi3({"/widgets": _get_op(schema)}))
        assert parsed.resources[0]["endpoint"]["data_selector"] == "data"


class TestIncrementalCursor:
    """Missing this re-extracts the full table every run and reports success."""

    def _parsed(self, params, extra=None):
        schema = {"type": "object", "properties": {"data": {"type": "array", "items": _WIDGET}}}
        return parse_openapi_spec(_openapi3({"/widgets": _get_op(schema, params, extra)}))

    def test_datetime_filter_param_maps_to_incremental(self):
        parsed = self._parsed(
            [
                {
                    "name": "updated_since",
                    "in": "query",
                    "schema": {"type": "string", "format": "date-time"},
                }
            ]
        )
        incremental = parsed.resources[0]["endpoint"]["incremental"]
        assert incremental["start_param"] == "updated_since"
        # The cursor must be a field the rows actually carry.
        assert incremental["cursor_path"] == "updated_at"

    def test_x_incremental_extension_wins(self):
        """An explicit spec declaration beats our inference."""
        parsed = self._parsed(
            [{"name": "since", "in": "query", "schema": {"type": "string", "format": "date-time"}}],
            extra={"x-incremental": {"cursor_path": "id", "start_param": "since_id"}},
        )
        incremental = parsed.resources[0]["endpoint"]["incremental"]
        assert incremental == {"cursor_path": "id", "start_param": "since_id"}

    def test_no_matching_row_field_emits_nothing(self):
        """A start_param with no cursor field to read would send a null filter."""
        schema = {
            "type": "object",
            "properties": {
                "data": {
                    "type": "array",
                    "items": {"type": "object", "properties": {"id": {"type": "integer"}}},
                }
            },
        }
        parsed = parse_openapi_spec(
            _openapi3(
                {
                    "/widgets": _get_op(
                        schema,
                        [
                            {
                                "name": "updated_since",
                                "in": "query",
                                "schema": {"type": "string", "format": "date-time"},
                            }
                        ],
                    )
                }
            )
        )
        assert parsed.resources[0]["endpoint"].get("incremental") is None

    def test_plain_query_params_are_not_mistaken_for_cursors(self):
        parsed = self._parsed([{"name": "name", "in": "query", "schema": {"type": "string"}}])
        assert parsed.resources[0]["endpoint"].get("incremental") is None


_SWAGGER2 = {
    "swagger": "2.0",
    "info": {"title": "Legacy", "version": "1"},
    "host": "api.legacy.example",
    "basePath": "/v2",
    "schemes": ["https"],
    "securityDefinitions": {
        "key": {"type": "apiKey", "name": "X-Api-Key", "in": "header"},
        "basicAuth": {"type": "basic"},
    },
    "paths": {
        "/widgets": {
            "get": {
                "operationId": "listWidgets",
                "parameters": [
                    {"name": "offset", "in": "query", "type": "integer"},
                    {"name": "limit", "in": "query", "type": "integer", "maximum": 200},
                ],
                "responses": {
                    "200": {
                        "description": "ok",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "data": {"type": "array", "items": {"$ref": "#/definitions/Widget"}}
                            },
                        },
                    }
                },
            }
        }
    },
    "definitions": {
        "Widget": {
            "type": "object",
            "required": ["id"],
            "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
        }
    },
}


class TestSwagger2Shim:
    """Swagger 2.0 was rejected outright — a large share of real specs."""

    def test_base_url_from_scheme_host_basepath(self):
        parsed = parse_openapi_spec(_SWAGGER2)
        assert parsed.base_url == "https://api.legacy.example/v2"

    def test_definitions_refs_resolve_to_columns(self):
        """`#/definitions/X` must be rewritten or every column silently vanishes."""
        parsed = parse_openapi_spec(_SWAGGER2)
        columns = {c["name"] for c in parsed.resources[0]["columns"]}
        assert columns == {"id", "name"}

    def test_response_schema_is_lifted_into_content(self):
        parsed = parse_openapi_spec(_SWAGGER2)
        assert parsed.resources[0]["endpoint"]["data_selector"] == "data"

    def test_security_definitions_convert(self):
        parsed = parse_openapi_spec(_SWAGGER2)
        kinds = {a["type"] for a in parsed.auth_schemes}
        assert kinds == {"api_key", "http_basic"}

    def test_parameter_types_reach_pagination_detection(self):
        """2.0 puts `type` on the parameter; 3.0 nests it under `schema`.

        Without normalising that, pagination detection sees no schema and the
        declared maximum is lost — the spec converts but pages 100 at a time
        against an API that allows 200.
        """
        parsed = parse_openapi_spec(_SWAGGER2)
        paginator = parsed.resources[0]["endpoint"]["paginator"]
        assert paginator["type"] == "offset"
        assert paginator["limit"] == 200

    def test_swagger_1_is_still_rejected(self):
        with pytest.raises(OpenApiImportError) as exc:
            parse_openapi_spec({"swagger": "1.2", "paths": {}})
        assert exc.value.code == "unsupported_version"

    def test_missing_version_is_still_rejected(self):
        with pytest.raises(OpenApiImportError) as exc:
            parse_openapi_spec({"paths": {}})
        assert exc.value.code == "unsupported_version"
