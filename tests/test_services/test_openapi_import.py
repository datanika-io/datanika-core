"""Tests for openapi_import — external OpenAPI 3.x spec → rest_api connector config.

P1 (paste-mode) of datanika-core#325. All in-process, no network. Fixtures are
compact but real OpenAPI 3.0/3.1 documents inline. Adversarial fixtures assert
the §7 caps trip with typed errors.
"""

import json

import pytest

from datanika.services.openapi_import import (
    MAX_RESOURCES,
    MAX_SPEC_BYTES,
    OpenApiImportError,
    parse_openapi_spec,
)

# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

PETSTORE = {
    "openapi": "3.0.3",
    "info": {"title": "Petstore", "version": "1.0"},
    "servers": [{"url": "https://api.petstore.io/v1"}],
    "components": {
        "securitySchemes": {"bearerAuth": {"type": "http", "scheme": "bearer"}},
        "schemas": {
            "Pet": {
                "type": "object",
                "required": ["id", "name"],
                "properties": {
                    "id": {"type": "integer", "format": "int64"},
                    "name": {"type": "string"},
                    "tag": {"type": "string"},
                },
            }
        },
    },
    "security": [{"bearerAuth": []}],
    "paths": {
        "/pets": {
            "get": {
                "operationId": "listPets",
                "summary": "List all pets",
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Pet"},
                                }
                            }
                        }
                    }
                },
            }
        },
        "/pets/{petId}": {
            "get": {
                "operationId": "getPet",
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Pet"}}
                        }
                    }
                },
            }
        },
    },
}

# Envelope response {data: [...], total} → data_selector = "data"
PAGINATED = {
    "openapi": "3.0.0",
    "info": {"title": "P", "version": "1"},
    "servers": [{"url": "https://api.example.com"}],
    "components": {
        "schemas": {
            "User": {
                "type": "object",
                "required": ["id"],
                "properties": {"id": {"type": "integer"}, "email": {"type": "string"}},
            }
        }
    },
    "paths": {
        "/users": {
            "get": {
                "operationId": "listUsers",
                "parameters": [
                    {"name": "limit", "in": "query", "schema": {"type": "integer"}},
                    {"name": "offset", "in": "query", "schema": {"type": "integer"}},
                ],
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": {
                                            "type": "array",
                                            "items": {"$ref": "#/components/schemas/User"},
                                        },
                                        "total": {"type": "integer"},
                                    },
                                }
                            }
                        }
                    }
                },
            }
        }
    },
}

APIKEY = {
    "openapi": "3.1.0",
    "info": {"title": "K", "version": "1"},
    "servers": [{"url": "https://api.key.io"}],
    "components": {
        "securitySchemes": {"apiKeyAuth": {"type": "apiKey", "in": "query", "name": "api_key"}},
        "schemas": {"Item": {"type": "object", "properties": {"id": {"type": "string"}}}},
    },
    "security": [{"apiKeyAuth": []}],
    "paths": {
        "/items": {
            "get": {
                "operationId": "listItems",
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Item"},
                                }
                            }
                        }
                    }
                },
            }
        }
    },
}

# Self-referential schema — must not infinite-loop
CIRCULAR = {
    "openapi": "3.0.0",
    "info": {"title": "C", "version": "1"},
    "servers": [{"url": "https://c.io"}],
    "components": {
        "schemas": {
            "Node": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "parent": {"$ref": "#/components/schemas/Node"},
                },
            }
        }
    },
    "paths": {
        "/nodes": {
            "get": {
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Node"},
                                }
                            }
                        }
                    }
                }
            }
        }
    },
}

PETSTORE_YAML = """
openapi: 3.0.3
info: {title: Petstore, version: "1.0"}
servers:
  - url: https://api.petstore.io/v1
paths:
  /pets:
    get:
      operationId: listPets
      responses:
        "200":
          content:
            application/json:
              schema:
                type: array
                items: {type: object, properties: {id: {type: integer}}}
"""


def _spec_with_n_paths(n: int) -> dict:
    paths = {}
    for i in range(n):
        paths[f"/r{i}"] = {
            "get": {
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {"type": "array", "items": {"type": "object"}}
                            }
                        }
                    }
                }
            }
        }
    return {
        "openapi": "3.0.0",
        "info": {"title": "big", "version": "1"},
        "servers": [{"url": "https://big.io"}],
        "paths": paths,
    }


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


class TestParseBasics:
    def test_base_url_from_servers(self):
        pc = parse_openapi_spec(PETSTORE)
        assert pc.base_url == "https://api.petstore.io/v1"

    def test_collection_resource_named_from_path(self):
        pc = parse_openapi_spec(PETSTORE)
        names = {r["name"] for r in pc.resources}
        assert "pets" in names

    def test_detail_endpoints_excluded(self):
        pc = parse_openapi_spec(PETSTORE)
        assert all("{" not in r["endpoint"]["path"] for r in pc.resources)
        assert any("petId" in w or "{" in w for w in pc.warnings)

    def test_columns_and_types_from_response_schema(self):
        pc = parse_openapi_spec(PETSTORE)
        pets = next(r for r in pc.resources if r["name"] == "pets")
        cols = {c["name"]: c for c in pets["columns"]}
        assert set(cols) >= {"id", "name", "tag"}
        assert cols["id"]["type"] == "bigint"  # integer + int64
        assert cols["id"]["nullable"] is False  # required
        assert cols["tag"]["nullable"] is True
        assert pets["primary_key"] == "id"

    def test_base_url_override(self):
        pc = parse_openapi_spec(PETSTORE, base_url_override="https://staging.petstore.io/v1")
        assert pc.base_url == "https://staging.petstore.io/v1"

    def test_data_selector_envelope(self):
        pc = parse_openapi_spec(PAGINATED)
        r = next(r for r in pc.resources if r["name"] == "users")
        assert r["endpoint"]["data_selector"] == "data"
        assert {c["name"] for c in r["columns"]} >= {"id", "email"}

    def test_yaml_input(self):
        pc = parse_openapi_spec(PETSTORE_YAML)
        assert pc.base_url == "https://api.petstore.io/v1"
        assert any(r["name"] == "pets" for r in pc.resources)

    def test_json_string_input(self):
        pc = parse_openapi_spec(json.dumps(PETSTORE))
        assert pc.base_url == "https://api.petstore.io/v1"


class TestAuth:
    def test_bearer_scheme(self):
        pc = parse_openapi_spec(PETSTORE)
        assert {"type": "bearer"} in pc.auth_schemes

    def test_api_key_query_scheme(self):
        pc = parse_openapi_spec(APIKEY)
        assert any(
            a["type"] == "api_key" and a["location"] == "query" and a["name"] == "api_key"
            for a in pc.auth_schemes
        )


class TestCapsAndErrors:
    def test_rejects_swagger_2(self):
        with pytest.raises(OpenApiImportError) as e:
            parse_openapi_spec({"swagger": "2.0", "paths": {}})
        assert e.value.code == "unsupported_version"

    def test_rejects_oversize(self):
        with pytest.raises(OpenApiImportError) as e:
            parse_openapi_spec("x" * (MAX_SPEC_BYTES + 1))
        assert e.value.code == "spec_too_large"

    def test_rejects_garbage(self):
        with pytest.raises(OpenApiImportError) as e:
            parse_openapi_spec("{not valid json or yaml: [[[")
        assert e.value.code == "invalid_spec"

    def test_circular_ref_terminates(self):
        pc = parse_openapi_spec(CIRCULAR)
        nodes = next(r for r in pc.resources if r["name"] == "nodes")
        cols = {c["name"]: c for c in nodes["columns"]}
        # nested self-ref collapses to a json column, no infinite recursion
        assert cols["parent"]["type"] == "json"

    def test_rejects_too_many_resources(self):
        with pytest.raises(OpenApiImportError) as e:
            parse_openapi_spec(_spec_with_n_paths(MAX_RESOURCES + 5))
        assert e.value.code == "too_complex"
