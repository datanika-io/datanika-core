# Spec: Pipeline Templates — Depth (Option B)

> **Status**: Scaffold. No priorities committed. Fill in after **2–3 weeks** of Plausible funnel data from PR #94 lands (timeline updated from "1 week" after Growth's traffic audit — see [Reality check](#reality-check-why-23-weeks-not-1)). Do not implement any section until the data is reviewed.
> **Owner**: Product
> **Related**:
> - PR #81 — Pipeline Templates MVP (3 launch templates, prefill-only)
> - PR #94 — Plausible instrumentation (`template_selected`, `template_prefill_applied`)
> - PR #119 — Docs → connector cross-links (feeds the "does the SEO path exist?" question)
> - Issue #93 — Follow-up for `template_first_run_triggered` (funnel step 3)
> - Issue #76 — Original MVP scope, explicitly out-of-scope for the marketplace/orchestration work this spec enumerates
> - Google Ads campaign `AW-18081528527` (conversion attribution for paid traffic)
> - Google Search Console property `datanika.io` (organic query → landing page attribution)
> - **[SPEC_PUBLIC_TEMPLATE_LANDING.md](https://github.com/datanika-io/datanika-landing/blob/main/docs/specs/SPEC_PUBLIC_TEMPLATE_LANDING.md) — Option C standalone spec.** Evaluates public `/templates/*` landing pages on their own architectural merits (SEO, shareability, cold-traffic measurability), not just as a fallback for thin in-app data. Three sequencing options (pre-B / post-B / parallel) enumerated. **Read before picking Option B axes** — Option C landing first changes the evidence inputs for Axis 2.
> **Date**: 2026-04-13 (drafted) · 2026-04-13 (extended with cross-source data framework + measurement preconditions + timeline reality check)
> **Decision framework**: Every section below is an axis. Each axis has options, not conclusions. When funnel data arrives, pick along each axis based on evidence — weighted by **which users matter commercially**, not raw click counts. Write the picks at the bottom under "Post-data picks".

## Why this exists

PR #81 shipped the Pipeline Templates MVP — 3 launch templates (Stripe→PG, PG→BQ, CSV→DuckDB), pure field prefill on the connection form, static data registry (no DB model). It's intentionally shallow: its job was to prove the conversion path from empty dashboard to first-run was viable, not to realize it.

The MVP leaves four real questions unanswered:

1. **How deep should the automation go?** Today it prefills one form field. Users still have to click through source creation, destination creation, dlt configuration, and first-run trigger manually. Each of those is a drop-off opportunity.
2. **Which templates should we have?** 3 templates covers the bare minimum. The keyword-research long-tail suggests 8–15 more candidates, but some will be duplicative of existing use-case pages and some will have no signal at all.
3. **What does "one-click" mean?** There's a spectrum from "prefill the form" (MVP) to "create the source, create the destination, configure the dlt load, schedule it, and run it — all from one click". Each step along the spectrum is more valuable but more expensive to build and more likely to break for users whose credentials or environment don't match our assumptions.
4. **How do we attribute conversions?** Without `template_first_run_triggered` from issue #93, we can't measure whether a template actually delivered value. With it, we can, but it requires schema changes.

The funnel data from PR #94 will tell us which of these questions matter most. This spec enumerates the options so that next session's decisions can be made on evidence, not re-derivation under time pressure.

---

## Data sources to consult

Before filling in the [Post-data picks](#post-data-picks) table, consult **all** of the following sources in the order listed. Each has a different shape and resolution — don't answer a commercial question with the wrong source, and don't use just one source where two tell a richer story together.

### Source inventory

| Source | What it tells you | What it does NOT tell you | Status (2026-04-13) |
|---|---|---|---|
| **Plausible** — `template_selected`, `template_prefill_applied` | Which of the 3 MVP templates gets clicks; what fraction of clicks land on the prefilled connections page; session-level funnel shape in raw numbers | Whether the user completed credential entry, whether first-run succeeded, whether they converted to paid — only click-through | ⏳ Wired in PR #94, **dormant** until Infra promotes core dev→master and sets `analytics_domain` + `analytics_script_src` in `.env.docker` and creates `app.datanika.io` site in Plausible CE |
| **Plausible** — `template_first_run_triggered` | Whether users actually reached "value delivered" — a first run of a template-sourced resource | See above + full attribution chain from click → run | ❌ Deferred as follow-up issue #93; requires `source_template_slug` schema column on `connections` |
| **Google Ads API** — `secrets/datanika-google-ads-acc.json`, campaign `AW-18081528527` | Which ad groups / keywords drove the signups; CPA per template; conversion value per template; ROAS of each template variant under paid spend | Anything about organic traffic; anything pre-signup; anything about what a user did *after* signup | ❌ **Engineering Phase 2 conversion tracking not yet shipped on signup.** The Ads side has conversion actions defined, but the signup flow isn't firing them yet. Until this is live, the Ads API can show *which campaigns drove signups* but not *which templates those signups used*. |
| **Google Search Console API** — `secrets/datanika-search-console-acc.json`, property `datanika.io` | Which queries drive organic traffic to template-adjacent landing pages (e.g., `/docs/pipelines`, `/docs/getting-started`, `/docs/connections`, `/connectors/{slug}`); impressions vs clicks by query | Anything about in-app behavior; anything behind auth | ✅ Verified property. Queryable now. |
| **Hetzner server logs** — SSH to `app.datanika.io`, raw nginx access logs | Raw HTTP request patterns, referrers, user agents; sanity check for Plausible undercount | Aggregates — the repo has no log pipeline. Useful only as a spot check, not a reporting surface. | ⚠ Accessible via SSH but **no aggregation infrastructure**. Use only to verify Plausible isn't missing events (e.g., ad-blockers). |

### Cross-referencing the sources

Plausible alone tells you **which template gets clicks**. Google Ads alone tells you **which keyword / ad group drove a signup**. Google Search Console alone tells you **which organic queries reach template-adjacent pages**.

The insight lives in the **intersection**. Examples:

- *Plausible × Google Ads*: "Stripe→PG got 40% of template clicks, but 100% of those clicks came from organic traffic. The Fivetran-alternative ad group drove 12 signups last week, and zero of them clicked any template card." → Interpretation: the ad copy is setting the wrong expectation. Fix the ad landing page, not the template catalog.
- *Plausible × GSC*: "CSV→DuckDB dominates template clicks, and the GSC top query driving `/docs/getting-started` is `duckdb local etl`." → Interpretation: the audience wants zero-credential quickstarts; prioritize file-based templates even if they look "toy" next to enterprise ones.
- *Google Ads × GSC*: "Search Console shows `stripe analytics pipeline` as the top organic query but the Google Ads campaign isn't bidding on it." → Out of Product's lane (tell Growth), but it influences which templates are commercially relevant vs just popular.

**Heuristic**: a template that is popular in Plausible but absent from Google Ads and GSC is a curiosity, not a commercial asset. A template that appears in all three sources, even with low absolute numbers, is the one to invest in.

### What "weighted by ad spend" actually means

A template that attracts 100 Plausible clicks but zero of them came from paid traffic is **less commercially valuable** than a template that attracts 20 Plausible clicks all of which came from a high-CPA keyword in the paid campaign.

Why: the first template scales with SEO (slow, 3–6 month payback, bounded by organic query volume). The second template scales by **increasing ad spend on the matching keyword** (fast, same-day, bounded by CAC/LTV economics). Both directions are legitimate; they imply different operational moves and different investment horizons.

Every Axis 2 (catalog expansion) decision **must** be weighted by this distinction, not by Plausible counts alone. See the updated Axis 2 decision criteria below.

### Reference: how to query each source

Quick-reference commands for next session, so the data pull doesn't eat into the decision time:

```python
# Plausible events — via the self-hosted instance dashboard at plausible.datanika.io
#   Filter: site = app.datanika.io, event = template_selected OR template_prefill_applied
#   Group by: custom prop "slug"
#   Timeframe: last 14d / 28d depending on when Infra flipped analytics on

# Google Ads conversions — via the google-ads-python SDK
from google.ads.googleads.client import GoogleAdsClient
client = GoogleAdsClient.load_from_storage(
    "D:/Projects/Datanika/secrets/datanika-google-ads-acc.json"
)
# Query: search_term_view + campaign_conversion + by ad_group
# See: plans/growth/README_ADS_API.md (if Growth has one) or the google-ads Python docs

# Google Search Console — via the webmasters API service account
from google.oauth2 import service_account
from googleapiclient.discovery import build
creds = service_account.Credentials.from_service_account_file(
    "D:/Projects/Datanika/secrets/datanika-search-console-acc.json",
    scopes=["https://www.googleapis.com/auth/webmasters.readonly"],
)
service = build("searchconsole", "v1", credentials=creds)
# Request: siteUrl="sc-domain:datanika.io", body with query dimensions + date range
```

Don't put these into Python files for real in this session — they're reference only. The point is: next session's query work is itself a few-hours task, not a "minutes from the spreadsheet" task. Budget it accordingly.

---

## What the MVP does today (recap)

See [datanika/data/pipeline_templates.py](../../datanika/data/pipeline_templates.py) and [datanika/ui/pages/pipeline_templates.py](../../datanika/ui/pages/pipeline_templates.py).

- **Data**: frozen `PipelineTemplate` dataclass, 3 instances, static Python. No DB model, no migration.
- **UI**: grid of 3 cards at `/pipelines/templates`, each linking to `/connections?template=<slug>`.
- **State**: `ConnectionState.load_template_from_query` reads the URL query param, looks up the template, copies `source_config_defaults` into matching `form_*` attributes. Never touches credentials.
- **Scope**: the user enters their own credentials, clicks save, then the rest of the flow (destination, dlt config, first run) is fully manual. The template is off their radar after that first prefill.

Every design axis below is about *going further than this* in a specific direction.

---

## Axis 1 — Multi-step orchestration depth

How far does clicking a template card carry the user before manual work resumes?

### Option 1A — Prefill only (MVP, current state)

Click card → navigate to `/connections` with one form pre-filled. User fills credentials, saves, manually navigates to uploads/pipelines, manually configures dlt, manually triggers a run.

- **Effort**: 0 (already shipped)
- **Drop-off surface**: maximum — user has to make every decision after the prefill
- **Best for**: users who know what they want and are blocked by "what do I type in the schema field"

### Option 1B — Two-step (source → destination prompt)

Click card → prefill source form. After the user saves the source connection, a modal or toast appears: *"Next: set up your Postgres destination for this template."* Clicking the CTA navigates to `/connections/new?template=<slug>&step=destination` with the destination form prefilled. After destination save, the flow returns the user to `/pipelines` with a "Template X is ready to configure" banner.

- **Effort**: ~2 days. Needs session-scoped state tracking the template through the flow, plus the destination form prefill path (already supported by the `source_config_defaults` / `destination_config_defaults` distinction on `PipelineTemplate`, but currently unused).
- **Drop-off surface**: reduced — two prompted steps instead of one ad-hoc navigation
- **Breakage risk**: low. No backend work; just UI state threading.
- **Open question**: How does the banner detect "template X is ready"? Session state lost on refresh. Needs either a `pending_template_slug` column on the user record, or a localStorage flag, or a URL param carried through.

### Option 1C — Three-step (source → destination → dlt config)

Everything in 1B, plus after destination save we auto-create an `Upload` record populated from `PipelineTemplate.dlt_config_defaults` (resources, write_disposition, target schema). The user sees a pre-configured upload in the uploads list. They just have to click **Run now**.

- **Effort**: ~4 days. Needs the `Upload` creation path to accept template defaults, permission to write Upload records from the template flow, potentially a migration to associate the Upload with its template for funnel attribution.
- **Drop-off surface**: reduced further — the user sees a concrete "thing to run" instead of an abstract "now configure"
- **Breakage risk**: medium. Upload config has many knobs; the template's guesses may not match the user's warehouse schema, leading to run failures on first trigger. That's a worse UX than "manual config hit Enter".

### Option 1D — Four-step (source → destination → dlt config → auto first-run)

Everything in 1C, plus we automatically trigger the first run on behalf of the user the moment the destination is saved. No "Run now" click.

- **Effort**: ~5 days. Needs safety rails around auto-run (what if credentials are wrong? what if the source has 10B rows and the user wasn't expecting it? what if billing/quota blocks it?).
- **Drop-off surface**: zero between card click and first run
- **Breakage risk**: high. An auto-run that fails on bad credentials is worse than a manual-run that fails on bad credentials because the user didn't consent to the attempt. Rate limiting + confirmation modal become necessary.
- **Cost implication**: for Datanika Cloud users, this burns quota on a user's behalf before they've had a chance to inspect. Needs opt-out.

### Decision criteria from funnel data

| If Plausible shows... | Pick... |
|---|---|
| High `template_selected` but low `template_prefill_applied` | Users click cards but bounce before landing on /connections — Axis 1 isn't the bottleneck, it's the templates themselves. Revisit Axis 2. |
| `template_selected` → `template_prefill_applied` ratio good, but no `template_first_run_triggered` (from #93 once shipped) | Users get to the prefill but fall off before running. **Option 1B or 1C** reduces that drop-off. |
| High `template_first_run_triggered` already on MVP | Option 1B/C/D is marginal — double down on Axis 2 instead. |
| Heavy tail — 1 template dominates, 2 are dead | Don't invest in deeper orchestration for the dead ones. Pick 1B for the live template only. |

---

## Axis 2 — Template catalog expansion

The MVP ships 3 templates. How many more should exist, and which?

### Candidate list (not prioritized)

All candidates are documented here for completeness. Post-data, pick the top 5–8 based on evidence. Candidates are derived from: (a) the Pipeline Templates MVP spec in issue #76, (b) the Tier 1 connector list, (c) the Google Ads keyword research that drove the connector-guide queue, (d) the use-case pages already shipped on landing.

| Candidate slug | Source | Destination | Rationale |
|---|---|---|---|
| `github-to-bigquery` | GitHub | BigQuery | Engineering analytics / PR cycle tracking. Matches an existing use-case page. |
| `hubspot-to-snowflake` | HubSpot | Snowflake | CRM analytics. Matches use-case. High CPC in keyword research. |
| `salesforce-to-bigquery` | Salesforce | BigQuery | Enterprise CRM to warehouse. Tier 1 connector, already has a connector guide. |
| `shopify-to-duckdb` | Shopify | DuckDB | E-commerce analytics quickstart. DuckDB matches the "lightweight" audience the CSV→DuckDB MVP template attracts. |
| `mongodb-to-snowflake` | MongoDB | Snowflake | NoSQL → SQL flattening, the one NoSQL template in the list. Matches use-case. |
| `mysql-to-bigquery` | MySQL | BigQuery | Second most common OLTP source. Symmetric to Postgres→BQ. |
| `s3-to-snowflake` | S3 | Snowflake | Data-lake → warehouse. Matches use-case. |
| `kafka-to-clickhouse` | Kafka | ClickHouse | Streaming → OLAP. Matches use-case. |
| `stripe-to-bigquery` | Stripe | BigQuery | Revenue analytics on BigQuery instead of Postgres. Variant of MVP template for users already on GCP. |
| `google-analytics-to-bigquery` | Google Analytics | BigQuery | Marketing analytics. High keyword volume in keyword research. |
| `postgresql-to-duckdb` | PostgreSQL | DuckDB | Dev / staging sync for local warehouse. Zero-cost destination. |
| `csv-to-postgresql` | CSV | PostgreSQL | Variant of MVP CSV template for users without DuckDB. |

### Coverage gaps to consider

- **No Databricks destination** in the candidate list. If the funnel data shows enterprise traffic, add one.
- **No Redshift destination**. Same caveat.
- **No destination-first templates.** Every candidate is source-to-destination. A "bring your existing BigQuery" template that prompts for *source* after the user has a destination is a different UX but possibly valuable.

### Decision criteria from cross-source data

The table below is the **upgraded** version of the single-source criteria that were here in v1. Each row combines Plausible popularity with **ad-spend weighting** from Google Ads, because popularity alone can't tell you which template to scale with paid traffic. Read the table left-to-right: Plausible signal, Ads signal, combined interpretation, pick.

| Plausible signal | Google Ads signal | Interpretation | Pick |
|---|---|---|---|
| `template_selected` dominated by 1 MVP template | That template's source/destination match a **paid** ad group's keyword with a reasonable CPA | Paid intent aligns with the dominant template → the winning template is genuinely commercial, not a fluke | Clone the winner across 2–3 destination variants (e.g., Stripe→PG → also Stripe→BQ, Stripe→Snowflake). Push Growth to bid harder on the matching keyword. |
| `template_selected` dominated by 1 MVP template | No paid intent for that template — all traffic is organic | Popular but not commercially scalable via ads. **Don't clone it** yet — organic traffic is bounded. First figure out *why* it's popular from GSC queries, then decide. | Hold on catalog expansion for that winner. Axis 2 investment goes elsewhere. Maybe Axis 1 (deepen the winner's orchestration) is the right move. |
| Traffic evenly distributed across the 3 MVP templates | Paid traffic concentrated on one Tier 1 connector (e.g., Salesforce, HubSpot) that isn't an MVP template | The ad campaign is driving users to an area the catalog doesn't cover. **Build the missing template first.** | Ship the template that matches the highest-spend ad group from the [candidate list](#candidate-list-not-prioritized). If it's not in the candidate list, add it. |
| Traffic evenly distributed | Paid traffic also evenly distributed (broad Fivetran-alternative ad groups) | Broad audience — invest in breadth, not depth. Ship 5–8 candidates covering different verticals. Measure the winners of *that* batch, then iterate. | Ship the top 5 candidates by Tier 1 connector presence: GitHub→BQ, HubSpot→Snowflake, Salesforce→BQ, MySQL→BQ, S3→Snowflake. |
| CSV→DuckDB dominates | Zero paid intent for CSV or DuckDB | Audience wants zero-credential quickstarts and they're finding them organically. Paid can't amplify this efficiently. | Ship 2–3 more credential-free templates (JSON→DuckDB, Parquet→DuckDB, CSV→PostgreSQL). Don't try to scale CSV→DuckDB via paid. |
| CSV→DuckDB dominates | Paid intent strong for `duckdb` or `local etl` keywords | Unusual but valuable: paid traffic wants the free-tier path. Optimize the CSV→DuckDB flow as the funnel's actual top-of-funnel. | Deepen CSV→DuckDB (Axis 1 1C or 1D) instead of expanding catalog. It's the winner of the free tier. |
| Low absolute traffic across the board (< 50 sessions/week) | Paid campaign still ramping, few conversions | **Data is too thin to decide anything.** Wait another measurement window. Do not commit to catalog expansion on < 50 data points. | No pick. Continue waiting. If ad spend is active and still no signal after 3 weeks, suspect a tracking gap (see [Measurement preconditions](#measurement-preconditions)) not a template problem. |
| GSC shows high impressions on `/docs/pipelines` / `/docs/getting-started` for `"stripe → bigquery"` style queries | — | Organic demand for specific source→destination pairs that aren't in the template catalog yet | Add those specific pairs as templates. SEO can pull them in at zero marginal cost. |

### Warning: Plausible-only decisions are biased

If you ignore the Ads column because the data isn't wired yet (Engineering Phase 2 not shipped — see [Measurement preconditions](#measurement-preconditions)), you'll end up optimizing for organic-traffic templates by default. That's fine *if* you accept the 3–6 month SEO payback. It's **not fine** if the commercial priority is scaling the paid campaign — in which case you should **wait** for the Phase 2 conversion tracking rather than pick templates with half the data.

When in doubt, default to the "low absolute traffic, no pick" row: extend the measurement window instead of forcing a decision.

---

## Axis 3 — Prefill vs one-click automation

Orthogonal to Axis 1 (which is about *how many steps the template carries you through*), this axis is about *how much the template does for you at each step*.

### Option 3A — Form prefill (MVP, current state)

Template fills form fields, user clicks save.

### Option 3B — Form prefill + auto-save

After the form is prefilled, if there are no required credential fields (e.g., CSV template, DuckDB destination), the template saves the form automatically. For credential-bearing forms (Stripe, Postgres), stays in prefill mode.

- **Effort**: ~1 day. Needs a per-template `auto_save: bool` flag and the `ConnectionState.save_connection` call from the load_template_from_query path.
- **Best candidates**: CSV, DuckDB, file-based templates where "configuration" is just a filename.

### Option 3C — Credential vault / pre-auth

Template asks the user to authenticate once via OAuth (for Stripe, HubSpot, Salesforce, Google Analytics), then auto-fills both the connection and the credential. Makes the template feel "one-click" even though a consent flow happened.

- **Effort**: ~1–2 weeks per OAuth provider. Current MVP stores encrypted credentials via Fernet but doesn't do OAuth for SaaS sources. Needs OAuth state machine per provider, token refresh handling, revocation UX.
- **Best for**: SaaS sources where the user already has an account but doesn't know where to find the API key.
- **Out of scope unless funnel data specifically implicates credential entry as the drop-off.**

### Option 3D — Guardrail-assisted prefill

Template prefills plus a "dry-run" check that validates credentials without saving. If the user pastes a bad Stripe key, the flow tells them immediately instead of waiting for the first run to fail.

- **Effort**: ~3 days. Needs a validation endpoint per connector (some already exist — `ConnectionService.test_connection` — but not all SaaS sources support a lightweight test).
- **Best for**: reducing drop-off between credential paste and first run.

---

## Axis 4 — Template persistence & attribution

MVP treats templates as ephemeral — once the user lands on /connections with a template slug, we prefill and forget. No DB record of "this connection came from a template".

This axis is tightly coupled to issue #93 (`template_first_run_triggered`). Decisions here gate decisions there.

### Option 4A — No persistence (MVP, current state)

Template slug lives only in the URL query param and `ConnectionState.selected_template_slug` session state. Cross-session attribution is impossible.

- **Cost**: we lose the "X% of first runs came from template Y" report forever.

### Option 4B — Column on `connections`

Add `source_template_slug: str | None` to the `Connection` model. `ConnectionService.create_connection` writes it when the caller passes it in. `RunState.trigger_run` reads it when firing `template_first_run_triggered`.

This is the Option A recommendation in issue #93. Full spec lives there; don't duplicate.

- **Effort**: Alembic migration + model + service + read paths in 3 state classes ≈ 4–6 hours
- **Best for**: accurate lifetime attribution

### Option 4C — Event log table

New `template_events` table: `(user_id, template_slug, event_type, occurred_at)`. Every funnel step inserts a row. Reports query the table directly without walking through `connections`.

- **Effort**: ~1 day. New model, new service, read/write paths at 3–4 call sites.
- **Best for**: rich funnel reports inside the app (not just in Plausible). Enables per-tenant template dashboards.
- **Trade-off**: duplicates what Plausible will already track. Only pick this if Growth specifically asks for in-app reporting.

### Decision criteria from funnel data

- If Plausible gives us enough attribution alone → **Option 4B** is sufficient (just to unblock `template_first_run_triggered`).
- If Growth needs in-app per-tenant funnel reports → **Option 4C**.
- If the data shows templates are noise → **Option 4A** is fine; don't pay for persistence we won't read.

---

## Out of scope for this spec (deliberate)

- **Community templates marketplace.** This was mentioned in the MVP spec as a *future* deferred thing. It's a substantially larger project — per-user / per-org publishing, moderation, review, discoverability. Not touched here. Revisit after Option B ships and proves the underlying template surface is worth extending.
- **Template versioning.** If a template's `dlt_config_defaults` changes (e.g., Stripe adds a new resource), does the template have a version number that affects existing users? Not decided. Probably "no versioning, latest always wins" for Option B.
- **Per-environment templates.** Dev vs staging vs prod templates. Not asked for. Not doing it.
- **Template parameters / variants.** A Stripe→Postgres template that asks the user "which Stripe resources?" as a configuration step. Possibly valuable, possibly overkill. Punt until data says users actually want this.
- **Revenue attribution on templates.** "This template generated $X in paid conversions." Needs a join across Plausible → signup table → paid plan table. Growth's territory, not Product's. Loop them in if it becomes relevant.

---

## Measurement preconditions

Before the **measurement window** starts (the clock for "how long until we have data to decide on"), every item in this checklist must be wired. An item being "✅ Code shipped" is not the same as being "✅ Live in production and generating events". The clock starts on live-in-production.

### Required for any decision at all

| Precondition | Status (2026-04-13) | Owner | Gate |
|---|---|---|---|
| `template_selected` event firing on `/pipelines/templates` card clicks | ⏳ Wired in PR #94, awaiting core dev→master promotion | Infra (promotion) | Core dev→master |
| `template_prefill_applied` event firing on `/connections?template=<slug>` page load | ⏳ Wired in PR #94, awaiting core dev→master promotion | Infra (promotion) | Core dev→master |
| `settings.analytics_domain = "app.datanika.io"` in Hetzner `.env.docker` | ❌ Not set | Infra | Manual env edit on Hetzner + container rebuild |
| `settings.analytics_script_src = "https://plausible.datanika.io/js/script.js"` in Hetzner `.env.docker` | ❌ Not set | Infra | Same env edit + rebuild |
| `app.datanika.io` site registered in Plausible CE dashboard at plausible.datanika.io | ❌ Not created | Infra (one-click in Plausible admin) | Manual step in the Plausible CE UI |
| **Any real user traffic actually clicking a template** | ⏳ Depends on paid campaign + organic indexing | Growth + Product | ≥ 50 unique sessions hitting `/pipelines/templates`, see [Reality check](#reality-check-why-23-weeks-not-1) |

### Required for cross-source decisions (Google Ads attribution)

Without these, the Axis 2 "weighted by ad spend" decision criteria above cannot be applied — you're stuck with Plausible-only signal, which (per the warning under Axis 2) biases the decision toward organic templates.

| Precondition | Status (2026-04-13) | Owner | Gate |
|---|---|---|---|
| Google Ads signup conversion action firing on the Datanika signup flow | ❌ **Engineering Phase 2, not shipped** | Engineering | Signup page must fire the conversion gtag on successful account creation |
| Google Ads conversion action for `template_first_run_triggered` (or a proxy for paid-attributed first runs) | ❌ Blocked on issue #93 + Engineering Phase 2 | Engineering + Product | #93 lands first, then the RunState code returning `rx.call_script` needs to fire a *second* script to the Ads gtag alongside the Plausible event |
| Ability to correlate an Ads click_id to a Datanika user session | ❌ Not wired; requires `gclid` capture on signup | Engineering | Signup page must read `?gclid=` from URL and persist it with the user record |

**Until all three of the above land, the Axis 2 decision can only be made from Plausible counts.** That's a material risk — it's exactly the decision where commercial weighting matters most.

### Required for SEO decisions (GSC cross-reference)

| Precondition | Status (2026-04-13) | Owner | Gate |
|---|---|---|---|
| GSC data available for template-adjacent pages (`/docs/pipelines`, `/docs/getting-started`, `/docs/connections`, `/connectors/{slug}`) | ✅ Already indexed, queryable | — | Just run the API query |
| GSC data for a public template landing page | ❌ **No public template page exists** | Product (future Option C — new landing page) | `/pipelines/templates` is behind auth in the Reflex app. GSC will never index it. If we want SEO signal on templates specifically, a public marketing landing page is a separate Option C scope. |

> **Clarification on a common mistake**: `/pipelines/templates` is the in-app template selection page in the Reflex app (behind authentication). It is not a public marketing page. Google Search Console cannot and will not index it regardless of how long we wait. If the commercial goal is to get organic search traffic flowing to a template landing experience, that requires a separate public page — see [SPEC_PUBLIC_TEMPLATE_LANDING.md](https://github.com/datanika-io/datanika-landing/blob/main/docs/specs/SPEC_PUBLIC_TEMPLATE_LANDING.md) for the full design.

### Reality check — why 2–3 weeks, not 1

The v1 of this spec assumed "1 week of Plausible data" would be enough to make the post-data picks. **That estimate is wrong** given what Growth's audit has surfaced.

**The facts (Growth audit, 2026-04-13)**:

- The Google Ads paid campaign `AW-18081528527` has been running with **zero ad-attributed conversions**. The conversion tracking is the missing link — ads are running, clicks are happening, but signups aren't being credited to campaigns.
- Organic traffic to `app.datanika.io` is tiny: **54 clicks in 48 hours**, all organic. That's a floor of ~20 sessions/day to the authenticated app across *all* pages, not just `/pipelines/templates`.
- The template page is one of ~15 app pages competing for that floor traffic. Optimistically, **5–10% of in-app sessions** might hit `/pipelines/templates` in the first few days after promotion, because the Getting Started checklist now points there (shipped in PR #81) — but that's still only **1–3 template visits per day** until the funnel warms up.

**Sample-size implication**:

- At 1–3 template visits per day, a 7-day window yields **7–21 total events**, which is well below any statistical floor for comparing 3 template variants. Stripe→PG vs PG→BQ vs CSV→DuckDB cannot be differentiated from random noise with that little data.
- A 14-day window yields **14–42 events**. Still marginal — a single power user clicking all 3 templates skews everything.
- A 21-day window yields **21–63 events**. Borderline usable for a coarse "one template is dead, two are live" split but not for fine-grained destination pair choices.
- A full month (~30 days) is the first point at which the "< 50 unique sessions" floor from the Axis 2 decision table can reliably be cleared.

**So the real timeline**:

- **Best case** (paid campaign starts converting + SEO ramp kicks in): 2 weeks. Unlikely — Engineering Phase 2 hasn't shipped yet.
- **Realistic case**: 3–4 weeks from when Infra promotes PR #94 and wires the envs. This is when the Post-data picks should start getting filled in.
- **Worst case**: indefinite. If the paid campaign never fires conversions and organic traffic to the in-app page stays in the 1–3/day range, there will never be enough in-app-only data. The fallback is to **create a public template landing page first** (see [SPEC_PUBLIC_TEMPLATE_LANDING.md](https://github.com/datanika-io/datanika-landing/blob/main/docs/specs/SPEC_PUBLIC_TEMPLATE_LANDING.md)) and measure organic signal there before deciding on Option B.

**What this means for next session**:

1. **Don't look at the dashboard after 7 days and expect a decision.** The data will be too thin.
2. **Look at 14 days as a health check**, not a decision point. If `/pipelines/templates` has < 20 sessions by then, the problem isn't templates — it's that no one's getting to the page. Investigate the upstream funnel (signup volume, Getting Started checklist link, onboarding flow) before touching the template catalog.
3. **Plan to decide around day 21–28**, not day 7. Update any cross-team expectations accordingly.
4. **Consider Option C (public template landing page) as a measurement unblock**, not as a parallel priority. A public page that ranks on any template-adjacent query could double the measurement volume overnight. That's a commercial argument for building it before Option B, not after.

### Measurement preconditions summary

> **Green-light condition**: all 6 items in the "Required for any decision" table are ✅ AND ≥ 50 unique sessions have hit `/pipelines/templates` in the measurement window. Both gates must clear before the Post-data picks table below gets filled in.

Until both gates clear, the only productive Option B-adjacent work is:

- Shipping issue #93 (`template_first_run_triggered`) so the funnel step 3 is wired and ready.
- Building a public template landing page (Option C) to generate measurable organic signal.
- Continuing the Tier 2/3 connector guide queue to drive SEO volume that *upstream* of the template page.

**Starting Option B implementation before both gates clear is a speculative move that this spec exists to prevent.**

---

## Post-data picks

> Fill this section after the [Measurement preconditions](#measurement-preconditions) gates have cleared AND **2–3 weeks** of cross-source data has accumulated. Each row is a decision the data drives. **Do not commit to anything until the data is actually in hand.**

| Axis | Option picked | Evidence from Plausible | Evidence from Google Ads | Evidence from GSC | Scope (hours/days) |
|---|---|---|---|---|---|
| Axis 1 — Orchestration depth | TBD | TBD | TBD | TBD | TBD |
| Axis 2 — Catalog expansion | TBD | TBD | TBD | TBD | TBD |
| Axis 3 — Automation depth | TBD | TBD | TBD | TBD | TBD |
| Axis 4 — Persistence | TBD | TBD | TBD | TBD | TBD |

> If the Google Ads column is still blank when filling this in (because Engineering Phase 2 conversion tracking hasn't shipped), **explicitly note that** in the evidence column rather than silently making the decision on Plausible-only data. The next agent needs to know the decision was partial-evidence, not complete-evidence.

## Open questions for next session

**Plausible gates (per [Measurement preconditions](#measurement-preconditions) table 1):**
- [ ] Is PR #94 merged to dev AND promoted to master AND deployed?
- [ ] Are `analytics_domain` + `analytics_script_src` set in Hetzner `.env.docker` and picked up by the running container?
- [ ] Is `app.datanika.io` a registered site in the Plausible CE admin at plausible.datanika.io?
- [ ] **Is the measurement window ≥ 21 days old?** (Per [Reality check](#reality-check-why-23-weeks-not-1), 7 days is too short given organic traffic floor.)
- [ ] How many unique sessions hit `/pipelines/templates` in the window? (Green-light: ≥ 50. Below that, extend the window or investigate the upstream funnel instead.)
- [ ] What's the distribution across the 3 MVP templates? Even / skewed / dominated?
- [ ] What's the `template_selected` → `template_prefill_applied` conversion rate?
- [ ] Is issue #93 (`template_first_run_triggered`) merged and live? If yes, what's the `template_prefill_applied` → `template_first_run_triggered` rate?

**Google Ads gates (per [Measurement preconditions](#measurement-preconditions) table 2):**
- [ ] Is the Engineering Phase 2 signup conversion tracking live? (Blocks every Axis 2 "weighted by ad spend" decision.)
- [ ] Is `gclid` being captured on signup and persisted with the user record?
- [ ] For each MVP template: how many signups that used the template came from paid traffic vs organic?
- [ ] For each paid-attributed signup: what's the CPA? Is any template's CPA meaningfully below campaign average?
- [ ] If the Google Ads data is missing or incomplete when filling in the Post-data picks table, **flag it in the evidence column** rather than silently picking on partial data.

**Google Search Console gates (per [Measurement preconditions](#measurement-preconditions) table 3):**
- [ ] What are the top 10 queries driving organic traffic to `/docs/pipelines`, `/docs/getting-started`, `/docs/connections`, and `/connectors/*`?
- [ ] Do any of those queries match a candidate template from the [Axis 2 list](#candidate-list-not-prioritized)? If yes, that candidate has organic demand before it's even shipped.
- [ ] **Is a public template landing page (Option C) shipped?** If no, note that GSC cross-reference is limited to docs pages. If yes, what's its indexing status and impressions?

**Process gates:**
- [ ] Has Growth been looped into the measurement review? Cross-source interpretation is better as a joint call, not unilateral Product.
- [ ] Is the decision record going into this spec (Post-data picks table) AND being mirrored in `plans/product/current_state.md` so other agents see it without reading the spec?
- [ ] If the data says "no clear winner", is the Axis 2 fallback (ship more templates at current depth) cheaper than starting Axis 1 work? Usually yes.

**Escape hatches:**
- [ ] If the data is still too thin after 4 weeks: consider Option C (public landing page) as a measurement unblock BEFORE continuing Option B.
- [ ] If the paid campaign never fires conversions: escalate to Engineering as a blocker on Product's ability to make commercial decisions, and note that Option B cannot be evidence-driven without that data stream.

## Process notes for next agent

- **This is a scaffold, not a plan.** Don't start implementing anything from Axis 1–4 until the [Post-data picks](#post-data-picks) table is filled in AND the [Measurement preconditions](#measurement-preconditions) gates have cleared.
- **If the user asks "which option should we pick for X", the answer is *"depends on the data — let me pull Plausible + Ads + GSC first"*.** Not "here's my gut feel". If the data isn't in yet, say that out loud, don't speculate.
- **Don't decide on Plausible alone.** Per the [warning in Axis 2](#warning-plausible-only-decisions-are-biased), Plausible-only decisions bias toward organic templates. Use the cross-source framework from [Data sources to consult](#data-sources-to-consult) or wait for the Ads data to become available.
- **If the data is ambiguous** (low traffic, even distribution, noisy signal), the right call is usually **Axis 2 first at low depth** — ship more templates at the current depth — because that's the cheapest experiment and generates more data for the deeper decisions. **Or** consider Option C (public template landing page) as a measurement unblock if in-app traffic is the bottleneck.
- **Cross-reference every decision** in the Post-data picks table with what's already in current_state.md (`plans/product/current_state.md`) and [PLAN_PRODUCT.md](https://github.com/datanika-io/datanika-core/issues/734) so handoff is clean.
- **Ad-spend weighting is a real constraint, not a nice-to-have.** A template that scales under paid traffic beats a template that merely gets clicks. Don't let Plausible popularity alone drive Axis 2 picks.
- **Respect the 2–3 week (realistically 3–4 week) timeline.** If you're reading this spec less than 21 days after PR #94 was promoted, the data is probably too thin — extend the window instead of forcing a decision.

---

## Session log

- **2026-04-13 (v1)** — Initial scaffold written. 4 axes (orchestration depth, catalog expansion, automation depth, persistence) enumerated with options but no priorities. 12 candidate templates listed. Post-data picks table empty. Decision framework assumed "1 week of Plausible data" as the measurement window. Single-source (Plausible only).
- **2026-04-13 (v2)** — Extended with:
  - New [Data sources to consult](#data-sources-to-consult) section enumerating Plausible, Google Ads, GSC, Hetzner logs and their cross-reference patterns
  - Axis 2 decision criteria rewritten as a cross-source table (Plausible × Google Ads) with the "weighted by ad spend" framework
  - Explicit warning about Plausible-only decision bias
  - New [Measurement preconditions](#measurement-preconditions) section with 3 checklists (Plausible / Google Ads / GSC) and a green-light condition
  - [Reality check](#reality-check-why-23-weeks-not-1) subsection updating the measurement window from 1 week to 2–3 weeks (realistically 3–4) based on Growth's traffic audit (paid campaign zero conversions, 54 organic clicks in 48h)
  - Post-data picks table expanded from 3 columns to 5 (added Google Ads + GSC evidence columns)
  - Open questions list restructured into Plausible / Google Ads / GSC / Process / Escape hatch groups
  - Process notes for next agent updated to reflect cross-source decision framework

> Future extensions: add a new entry to this session log at the top of each revision so future agents can see the diff history without running `git blame`. The spec lives outside any git repo, so this log is the only version history we have.
