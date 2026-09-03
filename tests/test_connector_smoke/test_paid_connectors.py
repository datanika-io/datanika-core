"""Nightly live-connector smoke matrix.

One test per verified paid-connector sandbox. Each probe is the
lightest auth + metadata call that catches:

1. Our credentials being revoked / rotated / expired
2. The vendor's API shape changing in a way our code cares about
3. Regional/network issues (DNS, TLS, firewall)

Failure → Telegram alert from the nightly workflow. Not wired into
PR CI (gated by ``DATANIKA_CONNECTOR_SMOKE=1``; see conftest.py).

## Status of the 9 paid connectors

| Connector      | Tested here? | Reason |
|----------------|:-:|-|
| BigQuery       | ✅ |   |
| Databricks     | ✅ | control plane only — see the warehouses probe |
| Stripe         | ✅ |   |
| Kafka/Redpanda | ⚠️ | pinned awaiting-provisioning — trial org reclaimed (cloud#160) |
| HubSpot        | ✅ |   |
| Shopify        | ✅ | provisioned 2026-07-21 |
| GA4            | ✅ | provisioned 2026-07-21 |
| Salesforce     | ⬜ | signup blocked on bot detection — one human click |
| Snowflake      | ⬜ | **parked** — trial denied by Snowflake's risk engine |

Salesforce and Snowflake are the last two gaps, tracked in the founder queue
(`human-locked` issues in `datanika-cloud`). Snowflake is *parked*, not pending:
signup returns `SUPPRESSED_GENERIC` from the vendor's risk engine and is probably
unwinnable from our egress IP, so treat it as an accepted coverage gap rather
than a to-do.

## 🚨 What each probe would FAIL on — the core#992 sweep

**Reachability of *some* endpoint is not liveness of the account.** A vendor
deliberately keeps read-only metadata serving after it suspends or expires a
tenant — which is precisely the moment our probe most needs to fail. Two probes
here were green on a workspace that could not start compute, and one asserted
something that could not be false.

So for every probe below: *name the vendor state it would fail on, and confirm
that state is distinguishable from the one it passes on.* Swept 2026-09-03.

**bigquery datasets** — fails on: key revoked · project renamed · API disabled ·
key/project crossed. Distinguishable ✅ (the cross-check reads the key file's own
`project_id`). ⚠️ **Not** billing — see the probe.

**databricks warehouses** — fails on: PAT revoked · BI-Tools scope dropped ·
warehouse deleted. Distinguishable ✅.
⚠️ **Not** trial expiry / compute gating — see the probe. A further check —
*`DATABRICKS_HTTP_PATH` must still name a warehouse that exists* — is measured
and correct but **not shipped here**: it needs a new per-key GitHub secret, which
is core#983 Phase B's lane, and landing the assertion ahead of the secret reds
the nightly with `Missing env vars` (exactly what
`test_nightly_smoke_env_parity.py` exists to catch). Tracked on core#992.

**databricks UC catalogs** — fails on: UC scope dropped from the PAT ✅.
⚠️ Same compute-gating gap.

**stripe livemode** — fails on: key revoked (401) · a **live** key replacing the
test key ✅. Test-mode keys do not expire and test mode has no billing state to
lapse, so there is no quiet-death state here.

**stripe charges scope** — fails on: `charges.read` narrowed (403) ✅.

**hubspot account** — fails on: token revoked · portal deleted · account type
downgraded ✅. `accountType == DEVELOPER_TEST` is itself a liveness statement.

**hubspot properties** — fails on: CRM scope dropped · API shape change ✅
(`>= 50` against a normal 300+).

**shopify shop.json / scopes** — fails on: token revoked or uninstalled · store
deleted · API version retired · token crossed · seeded data wiped ✅.
⚠️ **But see the residual below.**

**ga4 runReport** — fails on: key revoked · property deleted · Data API disabled
✅ (`metadata.time_zone` is populated regardless of traffic).

**ga4 metadata** — fails on: property misconfigured · API shape change ✅. GA4
returns its schema whatever the traffic, unlike a row count.

⚠️ **Unverified residual: a FROZEN Shopify development store.** Shopify freezes
dev stores for inactivity or when the partner account lapses, and the Admin API
is believed to keep serving `shop.json` when it does — which would be this exact
defect a third time. It is **not measured**, because measuring it means freezing
the store, and no read-only signal for it has been found. Recorded rather than
guarded: inventing an assertion for a state nobody has observed is how a check
that can only ever be green gets written. Tracked in the issue linked from
core#992.
"""

from __future__ import annotations

import time

import httpx


def _log(msg: str) -> None:
    """Print with a stable prefix so the nightly log is scannable."""
    print(f"[smoke] {msg}")


# ---------- BigQuery ----------


def test_bigquery_auth_and_list_datasets(require_env):
    """Service-account auth + list datasets in the QA project.

    Catches: key revoked, project ID changed, API disabled, IAM scope drift,
    and a key/project pair that has been crossed in the secret bundle.

    The real check is that ``list_datasets`` **did not raise** — an empty
    project is a healthy state, so the count proves nothing (same trap as
    Shopify's ``orders=0`` and GA4's zero rows below). The assertion that can
    be false is the credential cross-check: the key file carries its own
    ``project_id``, and it must be the project we configured.

    Does NOT catch: billing disabled on the project. Production run 9 failed
    with ``403 Billing has not been enabled for this project. DML queries are
    not allowed in the free tier`` (cloud#124) while this probe was green on
    the same project — metadata reads are free and only DML is billed, so no
    metadata call can see the difference. **Green here does not mean BigQuery
    pipelines work.** Registered in ``UNOBSERVABLE_STATES`` (conftest.py).
    """
    env = require_env("BIGQUERY_PROJECT_ID", "BIGQUERY_CREDENTIALS_FILE")

    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account

    t0 = time.monotonic()
    creds = service_account.Credentials.from_service_account_file(env["BIGQUERY_CREDENTIALS_FILE"])
    client = bigquery.Client(project=env["BIGQUERY_PROJECT_ID"], credentials=creds)
    # list_datasets is the cheapest call that exercises both auth and project access.
    datasets = list(client.list_datasets(max_results=5))
    _log(
        f"bigquery: email={creds.service_account_email} key_project={creds.project_id} "
        f"datasets={len(datasets)} elapsed={int((time.monotonic() - t0) * 1000)}ms"
    )
    assert creds.project_id == env["BIGQUERY_PROJECT_ID"], (
        f"The service-account key belongs to project {creds.project_id!r}, not the "
        f"configured {env['BIGQUERY_PROJECT_ID']!r}. The BigQuery and GA4 probes share "
        "one key file — secrets may have been crossed in qa-connectors.env."
    )


# ---------- Databricks ----------


def test_databricks_auth_and_list_warehouses(require_env):
    """PAT auth + list SQL warehouses (BI Tools scope).

    Catches: PAT revoked, workspace URL changed, BI-Tools scope downgrade, and
    the warehouse being deleted.

    Does NOT catch: trial expiry / compute gating. This probe reads the control
    plane, and on this workspace the control plane is **entirely intact while no
    compute can start**. Measured 2026-09-03 over 30 read-only endpoints:
    warehouses, UC catalogs, clusters, jobs, ``sql/config`` and SCIM (still
    granting ``allow-cluster-create``) all return 200, exactly as on a live
    workspace, while::

        POST /api/2.0/sql/warehouses/{id}/start
          -> 400 denyReason=INACTIVE domain=resource-gatekeeper

    The warehouse **row survives the expiry**, which is why the old assertion
    message here — *"No SQL warehouses found, trial workspace may have
    expired"* — could never fire for the reason it named (core#992).

    ⚠️ The discriminating call is a **write that starts billable serverless
    compute**, so a probe must not make it (WORKFLOW_RULES 7a). Databricks
    compute liveness is therefore not observable from here at all; it is tracked
    on cloud#124 and registered in ``UNOBSERVABLE_STATES`` (conftest.py).
    **Green here does not mean Databricks pipelines work.**
    """
    env = require_env("DATABRICKS_HOST", "DATABRICKS_TOKEN")

    t0 = time.monotonic()
    r = httpx.get(
        f"{env['DATABRICKS_HOST']}/api/2.0/sql/warehouses",
        headers={"Authorization": f"Bearer {env['DATABRICKS_TOKEN']}"},
        timeout=15.0,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    assert r.status_code == 200, (
        f"Databricks warehouses list returned {r.status_code}: {r.text[:200]}"
    )
    warehouses = r.json().get("warehouses", [])
    _log(f"databricks: warehouses={len(warehouses)} elapsed={elapsed}ms")
    assert warehouses, (
        "No SQL warehouses in the workspace. The PAT authenticated and the endpoint "
        "answered, so this is a deleted warehouse or a BI-Tools scope revoke — NOT a "
        "trial expiry, which leaves the row in place (core#992)."
    )


def test_databricks_auth_and_list_uc_catalogs(require_env):
    """Unity Catalog list (Other APIs scope).

    Catches: UC scope dropped from PAT. Second call so a partial scope
    revoke is caught specifically, not hidden behind warehouses.

    Does NOT catch: trial expiry / compute gating — see the warehouses probe
    above for the measurement. UC metadata is served by the metastore and keeps
    answering 200 after the workspace can no longer start compute.
    """
    env = require_env("DATABRICKS_HOST", "DATABRICKS_TOKEN")

    r = httpx.get(
        f"{env['DATABRICKS_HOST']}/api/2.1/unity-catalog/catalogs",
        headers={"Authorization": f"Bearer {env['DATABRICKS_TOKEN']}"},
        timeout=15.0,
    )
    assert r.status_code == 200, f"UC catalogs returned {r.status_code}: {r.text[:200]}"
    catalogs = r.json().get("catalogs", [])
    _log(f"databricks: uc_catalogs={len(catalogs)}")
    assert len(catalogs) >= 1, "No UC catalogs — workspace/scope may be broken"


# ---------- Stripe ----------


def test_stripe_auth_and_livemode_guard(require_env):
    """Restricted key auth + verify we're in test mode.

    Catches: key revoked, accidentally live-mode key replaces test-mode
    (we'd never want `livemode: true` in a smoke run — that would mean a
    production key was copied to the test secret).
    """
    env = require_env("STRIPE_API_KEY")
    assert env["STRIPE_API_KEY"].startswith("rk_test_") or env["STRIPE_API_KEY"].startswith(
        "sk_test_"
    ), "STRIPE_API_KEY must be a test-mode key (rk_test_ or sk_test_)"

    t0 = time.monotonic()
    r = httpx.get(
        "https://api.stripe.com/v1/balance",
        auth=(env["STRIPE_API_KEY"], ""),
        timeout=15.0,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    assert r.status_code == 200, f"Stripe balance returned {r.status_code}: {r.text[:200]}"
    payload = r.json()
    _log(f"stripe: livemode={payload.get('livemode')} elapsed={elapsed}ms")
    assert payload.get("livemode") is False, (
        "Stripe smoke hit LIVE MODE — rotate the key immediately; a live key "
        "has replaced the test-mode restricted key in qa-connectors.env"
    )


def test_stripe_list_charges_read_scope(require_env):
    """Exercise the `charges.read` scope on the restricted key.

    Catches: scope narrowing on the key (if someone rotated it with
    fewer scopes, this catches it before the smoke matrix runs blind).
    """
    env = require_env("STRIPE_API_KEY")
    r = httpx.get(
        "https://api.stripe.com/v1/charges",
        params={"limit": 1},
        auth=(env["STRIPE_API_KEY"], ""),
        timeout=15.0,
    )
    assert r.status_code == 200, f"Stripe charges returned {r.status_code}: {r.text[:200]}"
    data = r.json()
    _log(f"stripe: charges_has_more={data.get('has_more')} charges={len(data.get('data', []))}")
    # Empty list is fine — key must just be permitted to ask.


# ---------- Kafka / Redpanda ----------


def test_kafka_auth_and_list_topics(require_env, require_import):
    """SASL/SCRAM-SHA-256 handshake + admin list_topics on Redpanda Serverless.

    Catches: credentials rotated, cluster paused, network block,
    SASL mechanism change (Redpanda occasionally tightens defaults).

    Runs fine from GHA Ubuntu runners. Will **fail from networks that
    block outbound 9092** (some ISPs do — see PLAN_HUMAN_LOCKERS.md
    Kafka entry). If you see ENOTFOUND or timeout locally, that's an
    ISP issue, not a credential issue.
    """
    env = require_env(
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_SASL_USERNAME",
        "KAFKA_SASL_PASSWORD",
        "KAFKA_TOPIC",
    )

    # NOT try/except ImportError -> pytest.skip. kafka-python 2.0.x cannot
    # import on Python 3.12 (kafka.vendor.six.moves is gone), so a skip here
    # would have converted a completely broken Kafka probe into a passing
    # nightly. require_import fails instead — see conftest for the rationale.
    KafkaAdminClient = require_import("kafka.admin", "KafkaAdminClient")  # noqa: N806 (a class)

    t0 = time.monotonic()
    admin = KafkaAdminClient(
        bootstrap_servers=env["KAFKA_BOOTSTRAP_SERVERS"],
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=env["KAFKA_SASL_USERNAME"],
        sasl_plain_password=env["KAFKA_SASL_PASSWORD"],
        request_timeout_ms=20000,
        # No api_version_auto_timeout_ms: kafka-python's KafkaAdminClient rejects it
        # (it is a consumer/producer kwarg, not an admin one) — passing it raised
        # KafkaConfigurationError and broke the re-enabled nightly (core#331).
        client_id="qa-nightly-smoke",
    )
    try:
        topics = admin.list_topics()
        elapsed = int((time.monotonic() - t0) * 1000)
        _log(f"kafka: topics={topics} elapsed={elapsed}ms")
        assert env["KAFKA_TOPIC"] in topics, (
            f"Expected topic {env['KAFKA_TOPIC']!r} missing; topics visible to "
            f"qa-smoke user: {topics}. Topic may have been deleted, or the ACL "
            f"grant was removed."
        )
    finally:
        admin.close()


# ---------- HubSpot ----------


def test_hubspot_auth_and_account_details(require_env):
    """Private App PAT auth + account type check.

    Catches: token revoked, portal deleted, account type downgraded
    (a real account would break the smoke expectations silently; we
    want the smoke to fail loud).
    """
    env = require_env("HUBSPOT_ACCESS_TOKEN")
    assert env["HUBSPOT_ACCESS_TOKEN"].startswith("pat-"), (
        "HUBSPOT_ACCESS_TOKEN must be a Private App PAT (pat-*)"
    )

    t0 = time.monotonic()
    r = httpx.get(
        "https://api.hubapi.com/account-info/v3/details",
        headers={"Authorization": f"Bearer {env['HUBSPOT_ACCESS_TOKEN']}"},
        timeout=15.0,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    assert r.status_code == 200, f"HubSpot account details returned {r.status_code}: {r.text[:200]}"
    payload = r.json()
    _log(
        f"hubspot: portalId={payload.get('portalId')} accountType={payload.get('accountType')} "
        f"elapsed={elapsed}ms"
    )
    assert payload.get("accountType") == "DEVELOPER_TEST", (
        f"HubSpot accountType is {payload.get('accountType')!r}, expected DEVELOPER_TEST. "
        "A real/standard account may have replaced the test portal in the secret."
    )


def test_hubspot_contacts_schema_introspectable(require_env):
    """List contact property definitions — the introspect() coverage check.

    Catches: CRM scope dropped, property-definitions endpoint shape change.
    """
    env = require_env("HUBSPOT_ACCESS_TOKEN")
    r = httpx.get(
        "https://api.hubapi.com/crm/v3/properties/contacts",
        headers={"Authorization": f"Bearer {env['HUBSPOT_ACCESS_TOKEN']}"},
        timeout=15.0,
    )
    assert r.status_code == 200, f"HubSpot properties returned {r.status_code}: {r.text[:200]}"
    properties = r.json().get("results", [])
    _log(f"hubspot: contact_properties={len(properties)}")
    assert len(properties) >= 50, (
        f"Only {len(properties)} contact properties returned — HubSpot usually ships "
        "300+ by default. Scope or API-shape change?"
    )


# ---------- Shopify ----------
#
# Sandbox is the `datanika-qa-smoke` development store (Partner org
# `Datanika`), read via the legacy custom app `qa-smoke-readonly`.
#
# ⚠️ The store's generated test data seeds products, customers and
# locations but **zero orders**. Asserting non-empty orders here would
# fail against a perfectly healthy store, so the orders probe checks
# reachability (the scope works) and deliberately not the count.


def test_shopify_auth_and_shop_metadata(require_env):
    """Admin API token auth + shop identity check.

    Catches: token revoked/uninstalled, store deleted or reclaimed for
    inactivity, API version retired (Shopify sunsets versions yearly),
    and — via the token-shape guard — a write-scoped or production
    token silently replacing the read-only sandbox one.
    """
    env = require_env("SHOPIFY_SHOP_DOMAIN", "SHOPIFY_ACCESS_TOKEN", "SHOPIFY_API_VERSION")
    assert env["SHOPIFY_ACCESS_TOKEN"].startswith("shpat_"), (
        "SHOPIFY_ACCESS_TOKEN must be an Admin API access token (shpat_*). "
        "A storefront (shpss_/shpca_) or OAuth token means the wrong secret "
        "was copied into qa-connectors.env."
    )

    t0 = time.monotonic()
    r = httpx.get(
        f"https://{env['SHOPIFY_SHOP_DOMAIN']}/admin/api/{env['SHOPIFY_API_VERSION']}/shop.json",
        headers={"X-Shopify-Access-Token": env["SHOPIFY_ACCESS_TOKEN"]},
        timeout=15.0,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    assert r.status_code == 200, f"Shopify shop.json returned {r.status_code}: {r.text[:200]}"
    shop = r.json()["shop"]
    _log(
        f"shopify: domain={shop['myshopify_domain']} plan={shop.get('plan_name')} "
        f"elapsed={elapsed}ms"
    )
    assert shop["myshopify_domain"] == env["SHOPIFY_SHOP_DOMAIN"], (
        f"Token belongs to {shop['myshopify_domain']!r}, not the configured "
        f"{env['SHOPIFY_SHOP_DOMAIN']!r} — secrets may have been crossed."
    )


def test_shopify_read_scopes_cover_core_resources(require_env):
    """Exercise each granted read scope — the introspect() coverage check.

    Catches: an individual scope being dropped when the app is
    reconfigured. Separate from the auth probe so a partial scope
    revoke is reported specifically instead of hiding behind shop.json.
    """
    env = require_env("SHOPIFY_SHOP_DOMAIN", "SHOPIFY_ACCESS_TOKEN", "SHOPIFY_API_VERSION")
    base = f"https://{env['SHOPIFY_SHOP_DOMAIN']}/admin/api/{env['SHOPIFY_API_VERSION']}"
    headers = {"X-Shopify-Access-Token": env["SHOPIFY_ACCESS_TOKEN"]}

    def _get(path: str) -> dict:
        r = httpx.get(f"{base}/{path}", headers=headers, timeout=15.0)
        assert r.status_code == 200, (
            f"Shopify {path} returned {r.status_code}: {r.text[:200]}. A 403 here "
            f"means the corresponding read_* scope was dropped from qa-smoke-readonly."
        )
        return r.json()

    products = _get("products/count.json")["count"]
    customers = _get("customers/count.json")["count"]
    locations = len(_get("locations.json")["locations"])
    # read_orders: assert the scope *works*, not that orders exist. The dev
    # store's test-data generator creates none, so `== 0` is the healthy state.
    orders = _get("orders/count.json?status=any")["count"]

    _log(
        f"shopify: products={products} customers={customers} locations={locations} "
        f"orders={orders} (orders=0 is expected — test data seeds none)"
    )
    assert products >= 1, (
        f"Expected seeded products, got {products}. The dev store's generated "
        "test data may have been wiped, or read_products was revoked."
    )
    assert locations >= 1, f"Expected at least one location, got {locations}"


# ---------- Google Analytics 4 ----------
#
# Sandbox is property `QA Smoke Property` (546388994) on account
# `Datanika QA`, read by the shared `qa-smoke@…gserviceaccount.com`
# service account (same key as BigQuery).
#
# ⚠️ The property has **no data stream**, so every report legitimately
# returns zero rows. Row counts prove nothing here — the metadata probe
# below is what actually distinguishes "working" from "broken".


def test_ga4_auth_and_run_report(require_env):
    """Service-account auth + a runReport call against the QA property.

    Catches: key revoked, property deleted, SA's Viewer grant removed,
    Data API disabled on the GCP project (the failure mode that cost us
    an hour during provisioning — the Analytics UI shows the grant as
    fine while the API returns PERMISSION_DENIED).
    """
    env = require_env("GA4_PROPERTY_ID", "GA4_CREDENTIALS_FILE")

    from google.analytics.data_v1beta import BetaAnalyticsDataClient  # type: ignore[import-untyped]
    from google.analytics.data_v1beta.types import DateRange, Metric, RunReportRequest
    from google.oauth2 import service_account

    t0 = time.monotonic()
    creds = service_account.Credentials.from_service_account_file(env["GA4_CREDENTIALS_FILE"])
    client = BetaAnalyticsDataClient(credentials=creds)
    response = client.run_report(
        RunReportRequest(
            property=f"properties/{env['GA4_PROPERTY_ID']}",
            date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
            metrics=[Metric(name="activeUsers")],
        )
    )
    _log(
        f"ga4: property={env['GA4_PROPERTY_ID']} row_count={response.row_count} "
        f"elapsed={int((time.monotonic() - t0) * 1000)}ms"
    )
    # Neither `row_count > 0` NOR `metric_headers` may be asserted here: with
    # no data stream GA4 returns zero rows *and* an empty metric_headers list
    # (verified live 2026-07-21 — an earlier draft of this test asserted
    # metric_headers and failed against a perfectly healthy property, the same
    # trap as Shopify's orders=0 above).
    #
    # The response metadata block IS always populated — it carries the
    # property's real configuration — so that's what proves the call worked.
    assert response.metadata.time_zone, (
        "runReport returned no metadata.time_zone. The property answered but "
        "not with a well-formed report — API shape changed?"
    )


def test_ga4_property_metadata_introspectable(require_env):
    """List the property's dimension/metric definitions.

    This is the assertion with teeth: getMetadata returns GA4's full
    schema regardless of whether the property has any traffic, so —
    unlike a row count — a non-empty result genuinely proves auth,
    property access, and API enablement all work.
    """
    env = require_env("GA4_PROPERTY_ID", "GA4_CREDENTIALS_FILE")

    from google.analytics.data_v1beta import BetaAnalyticsDataClient  # type: ignore[import-untyped]
    from google.oauth2 import service_account

    creds = service_account.Credentials.from_service_account_file(env["GA4_CREDENTIALS_FILE"])
    client = BetaAnalyticsDataClient(credentials=creds)
    metadata = client.get_metadata(name=f"properties/{env['GA4_PROPERTY_ID']}/metadata")

    _log(f"ga4: dimensions={len(metadata.dimensions)} metrics={len(metadata.metrics)}")
    assert len(metadata.dimensions) >= 50, (
        f"Only {len(metadata.dimensions)} dimensions returned — GA4 ships 100+ by "
        "default. Property misconfigured or API shape changed?"
    )
    assert len(metadata.metrics) >= 50, (
        f"Only {len(metadata.metrics)} metrics returned — GA4 ships 100+ by default."
    )
