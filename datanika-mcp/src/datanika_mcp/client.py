"""Thin httpx wrapper for the Datanika REST API v1.

**Fully async by design** (core#388). The hosted remote transport mounts this
tool surface into the Datanika backend itself, so a tool call is a *loopback*
request to the very process serving it. A blocking ``httpx.Client`` call would
hold the event loop that has to answer that request — the self-call could never
complete, and every hosted tool call died at the timeout below. Every request
method is therefore a coroutine, and every tool body awaits it, leaving the loop
free to serve the call it just made. ``tests/test_mcp/test_mcp_async_io.py``
pins this.
"""

from __future__ import annotations

import httpx


class DatanikaClient:
    """Authenticated HTTP client for the Datanika REST API.

    All methods are coroutines returning the parsed JSON body (dict/list), or
    raising ``httpx.HTTPStatusError`` on 4xx/5xx.
    """

    def __init__(self, base_url: str, api_key: str, timeout: float = 30.0) -> None:
        self._base = base_url.rstrip("/")
        self._http = httpx.AsyncClient(
            base_url=self._base,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _get(self, path: str, **params) -> dict:
        resp = await self._http.get(path, params=params or None)
        resp.raise_for_status()
        return resp.json()

    async def _post(
        self,
        path: str,
        json: dict | None = None,
        *,
        result_statuses: tuple[int, ...] = (),
    ) -> dict:
        """POST and return the parsed body, raising on 4xx/5xx.

        ``result_statuses`` names status codes that are **outcomes rather than
        transport failures** for this particular endpoint, and returns their
        bodies instead of raising.

        Only the three ``?wait=true`` trigger endpoints pass it, and only for
        ``(408, 422)`` (#663). Those codes carry the *run's* result on the status
        line — a failed pipeline, or a wait that timed out — and the body holds
        ``status`` and ``error_message``. Raising there tells an agent that
        something went wrong and throws away the one field saying what.

        ⚠️ **Deliberately opt-in per call, not a blanket rule in this method.**
        422 is not a run outcome anywhere else in the API; treating it as one
        everywhere would silently swallow real validation errors, which is the
        same defect one layer down.
        """
        resp = await self._http.post(path, json=json)
        if resp.status_code in result_statuses:
            return resp.json()
        resp.raise_for_status()
        return resp.json()

    #: Status codes the ``?wait=true`` trigger endpoints use to report the *run's*
    #: outcome. See ``_post`` and core#663.
    _RUN_OUTCOME_STATUSES = (408, 422)

    # ------------------------------------------------------------------
    # Read-only — Tier 1: Discover & Introspect
    # ------------------------------------------------------------------

    async def get_agent_tiers(self) -> dict:
        return await self._get("/api/v1/meta/agent-tiers")

    async def get_connection_types(self) -> dict:
        return await self._get("/api/v1/meta/connection-types")

    async def list_connections(self) -> dict:
        return await self._get("/api/v1/connections")

    async def get_connection(self, connection_id: int) -> dict:
        return await self._get(f"/api/v1/connections/{connection_id}")

    async def introspect_connection(self, connection_id: int, schema: str | None = None) -> dict:
        body = {}
        if schema:
            body["schema"] = schema
        return await self._post(f"/api/v1/connections/{connection_id}/introspect", json=body)

    async def preview_connection(
        self, connection_id: int, table: str, schema: str | None = None, limit: int = 100
    ) -> dict:
        body: dict = {"table": table, "limit": limit}
        if schema:
            body["schema"] = schema
        return await self._post(f"/api/v1/connections/{connection_id}/preview", json=body)

    async def query_connection(self, connection_id: int, query: str) -> dict:
        return await self._post(f"/api/v1/connections/{connection_id}/query", json={"query": query})

    # ------------------------------------------------------------------
    # Read-only — Tier 3: Validate
    # ------------------------------------------------------------------

    async def compile_transformation(self, transformation_id: int) -> dict:
        return await self._post(f"/api/v1/transformations/{transformation_id}/compile")

    async def preview_transformation(self, transformation_id: int, limit: int = 100) -> dict:
        return await self._post(
            f"/api/v1/transformations/{transformation_id}/preview",
            json={"limit": limit},
        )

    # ------------------------------------------------------------------
    # Read-only — Tier 4: Control (read half)
    # ------------------------------------------------------------------

    async def list_uploads(self) -> dict:
        return await self._get("/api/v1/uploads")

    async def list_pipelines(self) -> dict:
        return await self._get("/api/v1/pipelines")

    async def list_transformations(self) -> dict:
        return await self._get("/api/v1/transformations")

    async def list_runs(
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
        return await self._get("/api/v1/runs", **params)

    async def get_run(self, run_id: int) -> dict:
        return await self._get(f"/api/v1/runs/{run_id}")

    async def get_run_logs(self, run_id: int) -> dict:
        return await self._get(f"/api/v1/runs/{run_id}/logs")

    async def list_catalog(self) -> dict:
        return await self._get("/api/v1/catalog")

    async def get_catalog_entry(self, entry_id: int) -> dict:
        return await self._get(f"/api/v1/catalog/{entry_id}")

    # ------------------------------------------------------------------
    # Write — Tier 2: Build (requires --allow-write)
    # ------------------------------------------------------------------

    async def create_connection(self, name: str, connection_type: str, config: dict) -> dict:
        return await self._post(
            "/api/v1/connections",
            json={"name": name, "connection_type": connection_type, "config": config},
        )

    async def create_upload(
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
        return await self._post("/api/v1/uploads", json=body)

    async def create_pipeline(
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
        return await self._post("/api/v1/pipelines", json=body)

    async def create_transformation(
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
        return await self._post("/api/v1/transformations", json=body)

    async def bulk_import(self, payload: dict) -> dict:
        return await self._post("/api/v1/import", json=payload)

    # ------------------------------------------------------------------
    # Write — Tier 4: Execute (requires --allow-write)
    # ------------------------------------------------------------------

    async def trigger_upload(self, upload_id: int, wait: bool = False) -> dict:
        path = f"/api/v1/uploads/{upload_id}/run"
        if wait:
            path += "?wait=true"
        return await self._post(path, result_statuses=self._RUN_OUTCOME_STATUSES)

    async def trigger_pipeline(self, pipeline_id: int, wait: bool = False) -> dict:
        path = f"/api/v1/pipelines/{pipeline_id}/run"
        if wait:
            path += "?wait=true"
        return await self._post(path, result_statuses=self._RUN_OUTCOME_STATUSES)

    async def trigger_transformation(self, transformation_id: int, wait: bool = False) -> dict:
        path = f"/api/v1/transformations/{transformation_id}/run"
        if wait:
            path += "?wait=true"
        return await self._post(path, result_statuses=self._RUN_OUTCOME_STATUSES)
