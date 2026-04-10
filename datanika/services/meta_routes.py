"""REST API discovery endpoints (`/api/v1/meta/*`).

These endpoints let AI agents (and human developers) discover what
connection types, dlt config options, dbt tests, and materializations
exist without reading source code or external docs.

Auth: any valid API key (no required scope) — discovery is read-only
and useful before any resources exist.
"""

from starlette.responses import JSONResponse
from starlette.routing import Route

from datanika.services.api_middleware import api_endpoint
from datanika.services.connection_schemas import (
    get_connection_type,
    list_connection_types,
)
from datanika.services.meta_schemas import (
    DLT_CONFIG_SCHEMA,
    list_dbt_tests,
    list_materializations,
)


@api_endpoint()
async def meta_connection_types(request, api_key, session):
    """List all connection types with their config JSON Schemas."""
    return JSONResponse({"items": list_connection_types()})


@api_endpoint()
async def meta_connection_type(request, api_key, session):
    """Get a single connection type's schema."""
    slug = request.path_params["type"]
    item = get_connection_type(slug)
    if item is None:
        return JSONResponse(
            {"error": {"code": 404, "message": f"Unknown connection type: {slug}"}},
            status_code=404,
        )
    return JSONResponse(item)


@api_endpoint()
async def meta_dlt_config_schema(request, api_key, session):
    """Return the JSON Schema for `Upload.dlt_config`."""
    return JSONResponse(DLT_CONFIG_SCHEMA)


@api_endpoint()
async def meta_dbt_tests(request, api_key, session):
    """Return all available dbt test types."""
    return JSONResponse({"items": list_dbt_tests()})


@api_endpoint()
async def meta_materializations(request, api_key, session):
    """Return all available dbt materialization types."""
    return JSONResponse({"items": list_materializations()})


meta_routes = [
    Route(
        "/api/v1/meta/connection-types",
        meta_connection_types,
        methods=["GET"],
    ),
    Route(
        "/api/v1/meta/connection-types/{type:str}",
        meta_connection_type,
        methods=["GET"],
    ),
    Route(
        "/api/v1/meta/dlt-config-schema",
        meta_dlt_config_schema,
        methods=["GET"],
    ),
    Route(
        "/api/v1/meta/dbt-tests",
        meta_dbt_tests,
        methods=["GET"],
    ),
    Route(
        "/api/v1/meta/materializations",
        meta_materializations,
        methods=["GET"],
    ),
]
