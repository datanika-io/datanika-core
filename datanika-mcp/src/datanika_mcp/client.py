"""Thin httpx wrapper for the Datanika REST API v1."""

from __future__ import annotations

import httpx


class DatanikaClient:
    """Authenticated HTTP client for the Datanika REST API.

    All methods return the parsed JSON body (dict/list) or raise
    ``httpx.HTTPStatusError`` on 4xx/5xx.
    """

    def __init__(self, base_url: str, api_key: str, timeout: float = 30.0) -> None:
        self._base = base_url.rstrip("/")
        self._http = httpx.Client(
            base_url=self._base,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        )

    def close(self) -> None:
        self._http.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, **params) -> dict:
        resp = self._http.get(path, params=params or None)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, json: dict | None = None) -> dict:
        resp = self._http.post(path, json=json)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Read-only — Tier 1: Discover & Introspect
    # ------------------------------------------------------------------

    def get_agent_tiers(self) -> dict:
        return self._get("/api/v1/meta/agent-tiers")

    def get_connection_types(self) -> dict:
        return self._get("/api/v1/meta/connection-types")

    def list_connections(self) -> dict:
        return self._get("/api/v1/connections")

    def get_connection(self, connection_id: int) -> dict:
        return self._get(f"/api/v1/connections/{connection_id}")

    def introspect_connection(self, connection_id: int, schema: str | None = None) -> dict:
        body = {}
        if schema:
            body["schema"] = schema
        return self._post(f"/api/v1/connections/{connection_id}/introspect", json=body)

    def preview_connection(
        self, connection_id: int, table: str, schema: str | None = None, limit: int = 100
    ) -> dict:
        body: dict = {"table": table, "limit": limit}
        if schema:
            body["schema"] = schema
        return self._post(f"/api/v1/connections/{connection_id}/preview", json=body)

    def query_connection(self, connection_id: int, query: str) -> dict:
        return self._post(f"/api/v1/connections/{connection_id}/query", json={"query": query})

    # ------------------------------------------------------------------
    # Read-only — Tier 3: Validate
    # ------------------------------------------------------------------

    def compile_transformation(self, transformation_id: int) -> dict:
        return self._post(f"/api/v1/transformations/{transformation_id}/compile")

    def preview_transformation(self, transformation_id: int, limit: int = 100) -> dict:
        return self._post(
            f"/api/v1/transformations/{transformation_id}/preview",
            json={"limit": limit},
        )

    # ------------------------------------------------------------------
    # Read-only — Tier 4: Control (read half)
    # ------------------------------------------------------------------

    def list_uploads(self) -> dict:
        return self._get("/api/v1/uploads")

    def list_pipelines(self) -> dict:
        return self._get("/api/v1/pipelines")

    def list_transformations(self) -> dict:
        return self._get("/api/v1/transformations")

    def list_runs(
        self,
        target_type: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> dict:
        params: dict = {"limit": limit}
        if target_type:
            params["target_type"] = target_type
        if status:
            params["status"] = status
        return self._get("/api/v1/runs", **params)

    def get_run(self, run_id: int) -> dict:
        return self._get(f"/api/v1/runs/{run_id}")

    def get_run_logs(self, run_id: int) -> dict:
        return self._get(f"/api/v1/runs/{run_id}/logs")

    def list_catalog(self) -> dict:
        return self._get("/api/v1/catalog")

    def get_catalog_entry(self, entry_id: int) -> dict:
        return self._get(f"/api/v1/catalog/{entry_id}")

    # ------------------------------------------------------------------
    # Write — Tier 2: Build (requires --allow-write)
    # ------------------------------------------------------------------

    def create_connection(self, name: str, connection_type: str, config: dict) -> dict:
        return self._post(
            "/api/v1/connections",
            json={"name": name, "connection_type": connection_type, "config": config},
        )

    def create_upload(
        self,
        name: str,
        source_connection_id: int,
        destination_connection_id: int,
        dlt_config: dict | None = None,
        description: str | None = None,
    ) -> dict:
        body: dict = {
            "name": name,
            "source_connection_id": source_connection_id,
            "destination_connection_id": destination_connection_id,
        }
        if dlt_config:
            body["dlt_config"] = dlt_config
        if description:
            body["description"] = description
        return self._post("/api/v1/uploads", json=body)

    def create_pipeline(
        self,
        name: str,
        destination_connection_id: int,
        command: str = "run",
        description: str | None = None,
    ) -> dict:
        body: dict = {
            "name": name,
            "destination_connection_id": destination_connection_id,
            "command": command,
        }
        if description:
            body["description"] = description
        return self._post("/api/v1/pipelines", json=body)

    def create_transformation(
        self,
        name: str,
        sql_body: str,
        materialization: str = "view",
        description: str | None = None,
        schema_name: str = "staging",
    ) -> dict:
        body: dict = {
            "name": name,
            "sql_body": sql_body,
            "materialization": materialization,
            "schema_name": schema_name,
        }
        if description:
            body["description"] = description
        return self._post("/api/v1/transformations", json=body)

    def bulk_import(self, payload: dict) -> dict:
        return self._post("/api/v1/import", json=payload)

    # ------------------------------------------------------------------
    # Write — Tier 4: Execute (requires --allow-write)
    # ------------------------------------------------------------------

    def trigger_upload(self, upload_id: int, wait: bool = False) -> dict:
        path = f"/api/v1/uploads/{upload_id}/run"
        if wait:
            path += "?wait=true"
        return self._post(path)

    def trigger_pipeline(self, pipeline_id: int, wait: bool = False) -> dict:
        path = f"/api/v1/pipelines/{pipeline_id}/run"
        if wait:
            path += "?wait=true"
        return self._post(path)

    def trigger_transformation(self, transformation_id: int, wait: bool = False) -> dict:
        path = f"/api/v1/transformations/{transformation_id}/run"
        if wait:
            path += "?wait=true"
        return self._post(path)
