"""Post-deploy probes for the core Reflex app at `staging-app.datanika.io`.

Every probe asserts a **specific shape claim**, not just "HTTP 200 and
non-empty body". The point of smoke is to catch deploys that serve
200-with-wrong-payload (missing env var, stale cached HTML, plugin
unloaded). A 200-with-nothing-asserted is worse than no check at all —
it gives false comfort.
"""

from __future__ import annotations

import json

import httpx
import pytest


def _log_latency(response: httpx.Response) -> None:
    """Print elapsed time so pytest -v shows it.

    No hard threshold in v1 per core#133 scope. Collect a baseline from
    two weeks of green runs before tightening.
    """
    ms = response.elapsed.total_seconds() * 1000
    print(f"    elapsed={ms:.0f}ms url={response.request.url}")


def test_spa_root_loads(core_client: httpx.Client) -> None:
    r = core_client.get("/")
    _log_latency(r)
    assert r.status_code == 200
    # SPA shell is Reflex → React Router. Assert a marker that proves
    # the hydrated document shipped, not just Cloudflare's error page.
    body = r.text
    assert "reactRouter" in body or "Datanika" in body, (
        f"SPA root body did not contain expected markers; first 300 chars:\n{body[:300]}"
    )


def test_login_page_loads(core_client: httpx.Client) -> None:
    r = core_client.get("/login")
    _log_latency(r)
    assert r.status_code == 200
    # Same SPA shell, but assert at least one of the markers survives
    # the client-side route to /login.
    body = r.text
    assert "reactRouter" in body or "Datanika" in body, (
        "Login page body did not contain expected markers"
    )


def test_llms_txt_public(core_client: httpx.Client) -> None:
    r = core_client.get("/llms.txt")
    _log_latency(r)
    assert r.status_code == 200
    ctype = r.headers.get("content-type", "")
    assert ctype.startswith("text/"), f"/llms.txt returned unexpected content-type: {ctype}"
    body = r.text
    # Known-stable substring from the live content. If someone trims
    # the file so aggressively that "Tenant Isolation" drops, that's
    # worth a smoke failure — the agent-guide doc has been ~stable for
    # weeks and shouldn't shrink without notice.
    assert "Tenant Isolation" in body, "/llms.txt missing 'Tenant Isolation' section"


def test_openapi_json_public(core_client: httpx.Client) -> None:
    r = core_client.get("/api/v1/openapi.json")
    _log_latency(r)
    assert r.status_code == 200
    try:
        payload = r.json()
    except json.JSONDecodeError as e:
        pytest.fail(f"/api/v1/openapi.json was not JSON: {e}; first 300 chars: {r.text[:300]}")
    assert "openapi" in payload, "openapi.json missing top-level 'openapi' key"
    assert "paths" in payload, "openapi.json missing 'paths' key"
    # Sanity: at least one public meta path should be documented.
    assert any(p.startswith("/api/v1/") for p in payload["paths"]), (
        "openapi.json 'paths' is empty or doesn't include any /api/v1 routes"
    )


def test_agent_guide_md_public(core_client: httpx.Client) -> None:
    r = core_client.get("/api/v1/agent-guide.md")
    _log_latency(r)
    assert r.status_code == 200
    body = r.text
    assert len(body) > 500, f"agent-guide.md body is suspiciously short ({len(body)} chars)"
    # The guide should reference the golden path — if that section drops,
    # we broke the SoT generator.
    assert "golden" in body.lower() or "golden path" in body.lower(), (
        "agent-guide.md missing 'golden path' section"
    )


def test_agent_tiers_shape(core_client: httpx.Client) -> None:
    """Tier SoT — pinned to the current 5-tier / 7-capability layout.

    If Product adds or removes a tier, this test goes red first and
    forces a conscious update. That's the contract — `agent-tiers` is
    the SoT for landing's build-time agent page (closes #108) and for
    the /llms.txt generator.
    """
    r = core_client.get("/api/v1/meta/agent-tiers")
    _log_latency(r)
    assert r.status_code == 200
    payload = r.json()
    assert payload.get("tier_count") == 5, (
        f"tier_count changed from 5 to {payload.get('tier_count')} — "
        "update the smoke test AND landing's build-time snapshot in the same PR"
    )
    assert payload.get("capability_count") == 7, (
        f"capability_count changed from 7 to {payload.get('capability_count')} — "
        "update the smoke test AND landing's build-time snapshot in the same PR"
    )
    assert isinstance(payload.get("tiers"), list), "tiers should be a list"
    assert len(payload["tiers"]) == 5, "tiers list length doesn't match tier_count"
    # Every tier must have a number, name, summary, and capabilities list.
    for tier in payload["tiers"]:
        assert "number" in tier
        assert "name" in tier
        assert "summary" in tier
        assert isinstance(tier.get("capabilities"), list)
