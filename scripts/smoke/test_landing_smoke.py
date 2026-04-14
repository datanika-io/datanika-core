"""Post-deploy probes for the marketing site at `datanika.io`.

Runs against prod because there is no landing staging (landing deploys
straight from `main` to Aweb via `landing#137` workflow). Probes are
HTTP-only and hit the same Cloudflare → Aweb path real users do.

Like the core suite, every probe asserts a specific shape claim, not
just "HTTP 200 and non-empty body". The failure modes we're trying to
catch are: Astro build served stale, sitemap integration broken, pricing
page rewritten without one of the three plans, docs overview missing.
"""

from __future__ import annotations

import httpx


def _log_latency(response: httpx.Response) -> None:
    ms = response.elapsed.total_seconds() * 1000
    print(f"    elapsed={ms:.0f}ms url={response.request.url}")


def test_root_loads(landing_client: httpx.Client) -> None:
    r = landing_client.get("/")
    _log_latency(r)
    assert r.status_code == 200
    ctype = r.headers.get("content-type", "")
    assert ctype.startswith("text/html"), f"root returned unexpected content-type: {ctype}"
    body = r.text
    # Title is stable and has been the same for months. If it changes,
    # Growth rewrote the hero — surface that as a smoke failure so we
    # update the assertion consciously rather than silently drift.
    assert "<title>Datanika" in body, "root missing '<title>Datanika' — hero title changed?"


def test_pricing_page_has_all_plans(landing_client: httpx.Client) -> None:
    """Pricing SoT — Free / Pro / Enterprise must all appear.

    If Growth renames a tier (say 'Pro' → 'Team'), this goes red and
    forces a conscious update paired with the core `agent-tiers` snapshot.
    """
    r = landing_client.get("/pricing/")
    _log_latency(r)
    assert r.status_code == 200
    body = r.text
    for plan in ("Free", "Pro", "Enterprise"):
        assert plan in body, f"pricing page missing plan '{plan}'"


def test_docs_overview_loads(landing_client: httpx.Client) -> None:
    r = landing_client.get("/docs/")
    _log_latency(r)
    assert r.status_code == 200
    body = r.text
    # Docs overview is the canonical entry for /docs — title should say
    # so. If this drops, the Starlight/Astro docs build is broken.
    assert "Datanika Docs" in body, "/docs/ missing 'Datanika Docs' in body"


def test_sitemap_index_served(landing_client: httpx.Client) -> None:
    """Astro's sitemap integration publishes `/sitemap-index.xml`, NOT
    `/sitemap.xml`. Probe the right path — the index should reference
    at least one child sitemap file.
    """
    r = landing_client.get("/sitemap-index.xml")
    _log_latency(r)
    assert r.status_code == 200
    ctype = r.headers.get("content-type", "")
    assert "xml" in ctype, f"/sitemap-index.xml returned unexpected content-type: {ctype}"
    body = r.text
    assert "<sitemapindex" in body, "/sitemap-index.xml missing <sitemapindex> root element"
    assert "sitemap-0.xml" in body, (
        "/sitemap-index.xml doesn't reference sitemap-0.xml — "
        "Astro sitemap integration may be misconfigured"
    )
