"""Nightly live-connector smoke — Wave-1 drop (core#309 / core#312).

One probe per Wave-1 connector. Same contract as ``test_paid_connectors.py``:
the lightest auth + metadata call that catches (1) our credentials being
revoked / rotated / expired, (2) the vendor API shape changing in a way our
connector code cares about, (3) regional/network issues.

Coverage split (deliberate):
- **Correctness** of the connector code (URL building, auth-dict shape, IR
  membership) is covered offline by ``tests/test_services/test_wave1_connectors.py``.
- **This suite** is the *live* layer — it hits the real vendor endpoints /
  a real Oracle server, so a silent cred/driver/API breakage fails loud
  (Telegram) from the nightly workflow.

Env vars each probe needs (materialized in CI from the ``QA_WAVE1_CREDENTIALS``
secret + the Oracle service container; see ``.github/workflows/nightly-connector-smoke.yml``):

| Connector | Env vars |
|-----------|----------|
| Oracle    | ``ORACLE_{HOST,PORT,SERVICE,USER,PASSWORD}`` |
| Pipedrive | ``PIPEDRIVE_API_TOKEN`` |
| Freshdesk | ``FRESHDESK_API_KEY`` ``FRESHDESK_DOMAIN`` |
| Asana     | ``ASANA_ACCESS_TOKEN`` |

Skipped unless ``DATANIKA_CONNECTOR_SMOKE=1`` (see conftest.py). Locally:

    set -a && source secrets/pipedrive.env secrets/freshdesk.env secrets/asana.env && set +a
    # + an Oracle XE container on localhost:1521 (see the workflow for the image)
    DATANIKA_CONNECTOR_SMOKE=1 uv run pytest tests/test_connector_smoke/ -v
"""

from __future__ import annotations

import time

import httpx


def _log(msg: str) -> None:
    """Print with a stable prefix so the nightly log is scannable."""
    print(f"[smoke] {msg}")


# ---------- Oracle (SQL, source-only) ----------


def test_oracle_live_driver_and_connector(require_env):
    """Live Oracle probe against the XE service container.

    Two layers:

    1. **Positive control** — the oracledb thin driver + XE container + creds
       are healthy, addressed the *correct* way (service name / easy-connect).
       This is the real live-smoke value: catches driver break, XE image drift,
       network/creds issues. If it fails, the infra is broken — fail loud.
    2. **Our connector's path** (``_build_sa_url`` + ``ConnectionService.test_connection``)
       reaching the same service-name Oracle. Fixed in core#329 — ``_build_sa_url``
       now emits the ``?service_name=`` form (the URL path is SID-only).
    """
    env = require_env(
        "ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "ORACLE_USER", "ORACLE_PASSWORD"
    )

    from sqlalchemy import create_engine, text

    from datanika.models.connection import ConnectionType
    from datanika.services.connection_service import ConnectionService, _build_sa_url

    host, port, service = env["ORACLE_HOST"], int(env["ORACLE_PORT"]), env["ORACLE_SERVICE"]
    user, password = env["ORACLE_USER"], env["ORACLE_PASSWORD"]
    config = {
        "host": host,
        "port": port,
        "database": service,  # Oracle service name
        "user": user,
        "password": password,
    }

    # (1) Positive control — service-name easy-connect (the correct form).
    ctrl = create_engine(
        f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service}",
        connect_args={"tcp_connect_timeout": 5},
    )
    t0 = time.monotonic()
    try:
        with ctrl.connect() as conn:
            assert conn.execute(text("SELECT 1 FROM dual")).scalar() == 1
            table_count = conn.execute(text("SELECT COUNT(*) FROM all_tables")).scalar()
    finally:
        ctrl.dispose()
    _log(
        f"oracle: driver+container live, service={service} all_tables={table_count} "
        f"elapsed={int((time.monotonic() - t0) * 1000)}ms"
    )
    assert table_count is not None and table_count >= 0

    # (2) Our connector's own connect path (URL builder + connect_args + engine).
    url = _build_sa_url(config, ConnectionType.ORACLE)
    assert url.startswith("oracle+oracledb://"), f"unexpected Oracle URL scheme: {url}"
    ok, msg = ConnectionService.test_connection(config, ConnectionType.ORACLE)
    assert ok, msg  # core#329 fixed: connector reaches the service-name Oracle


# ---------- Pipedrive (SaaS REST — api_token query auth) ----------


def test_pipedrive_auth_and_current_user(require_env):
    """API-token auth + ``/users/me`` — the connector's exact auth shape.

    The Pipedrive connector authenticates with the token as an ``api_token``
    query param against ``https://api.pipedrive.com/v1/``. This probe mirrors
    that shape.

    Catches: token revoked/rotated, account suspended, API base moved.
    """
    env = require_env("PIPEDRIVE_API_TOKEN")

    t0 = time.monotonic()
    r = httpx.get(
        "https://api.pipedrive.com/v1/users/me",
        params={"api_token": env["PIPEDRIVE_API_TOKEN"]},
        timeout=15.0,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    assert r.status_code == 200, f"Pipedrive /users/me returned {r.status_code}: {r.text[:200]}"
    payload = r.json()
    assert payload.get("success") is True, f"Pipedrive success != true: {r.text[:200]}"
    data = payload.get("data", {})
    _log(
        f"pipedrive: user_id={data.get('id')} company={data.get('company_domain')} "
        f"elapsed={elapsed}ms"
    )
    assert data.get("id") is not None, "Pipedrive /users/me returned no user id"


# ---------- Freshdesk (SaaS REST — HTTP basic auth) ----------


def test_freshdesk_auth_and_current_agent(require_env):
    """HTTP-basic auth (api_key:X) + ``/agents/me`` — the connector's auth shape.

    The connector builds ``https://{domain}.freshdesk.com/api/v2/`` where
    ``domain`` is the *subdomain*. The provisioned secret may store either the
    bare subdomain or the full ``<sub>.freshdesk.com`` host — normalize both.

    Catches: API key revoked / API-key-access toggled off, account/trial
    expiry (Freshdesk free program lapses), subdomain change.
    """
    env = require_env("FRESHDESK_API_KEY", "FRESHDESK_DOMAIN")
    subdomain = env["FRESHDESK_DOMAIN"].strip().removesuffix(".freshdesk.com")

    t0 = time.monotonic()
    r = httpx.get(
        f"https://{subdomain}.freshdesk.com/api/v2/agents/me",
        auth=(env["FRESHDESK_API_KEY"], "X"),  # Freshdesk: api_key as user, any password
        timeout=15.0,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    assert r.status_code == 200, (
        f"Freshdesk /agents/me returned {r.status_code}: {r.text[:200]}. "
        "Likely the API key rotated, API-key access was disabled on the agent, "
        "or the free account lapsed (re-provision per secrets/freshdesk.env)."
    )
    agent = r.json()
    contact = agent.get("contact", {})
    _log(
        f"freshdesk: subdomain={subdomain} agent_id={agent.get('id')} "
        f"email={contact.get('email')} elapsed={elapsed}ms"
    )
    assert agent.get("id") is not None, "Freshdesk /agents/me returned no agent id"


# ---------- Asana (SaaS REST — bearer PAT) ----------


def test_asana_auth_and_current_user(require_env):
    """Bearer-PAT auth + ``/users/me`` — the connector's exact auth shape.

    The Asana connector sends the PAT as a bearer token against
    ``https://app.asana.com/api/1.0/``.

    Catches: PAT revoked, account deactivated, API version moved.
    """
    env = require_env("ASANA_ACCESS_TOKEN")

    t0 = time.monotonic()
    r = httpx.get(
        "https://app.asana.com/api/1.0/users/me",
        headers={"Authorization": f"Bearer {env['ASANA_ACCESS_TOKEN']}"},
        timeout=15.0,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    assert r.status_code == 200, f"Asana /users/me returned {r.status_code}: {r.text[:200]}"
    data = r.json().get("data", {})
    workspaces = data.get("workspaces", [])
    _log(
        f"asana: user_gid={data.get('gid')} name={data.get('name')!r} "
        f"workspaces={len(workspaces)} elapsed={elapsed}ms"
    )
    assert data.get("gid") is not None, "Asana /users/me returned no user gid"
