# UX Spec — Dual-Mode Pipelines (ETL vs ELT) + Volume Billing

> **Status**: spec v3 — user approved all 6 specs 2026-04-15. Free-tier allowance locked at **10 GB** (was 1 GB in v2). UI implementation in flight.
> **Tracks**: plans/PRICING_PIVOT.md (`plans/PRICING_PIVOT.md`) → Product deliverable
> **Author**: Product agent, 2026-04-15 (v1 same day; v2 later same day after decisions locked; v3 2026-04-15 late PM post-approval)
> **Scope boundary**: UX only. Not implementation. Not migration mechanics (Eng owns via SPEC_ELT_IR_ARCHITECTURE). Not metering internals (Cloud owns via SPEC_VOLUME_METERING). Not pricing copy (Growth owns via SPEC_PRICING_V2).
>
> **v3 revisions (2026-04-15 late PM, per user approval block in PRICING_PIVOT_DECISIONS.md (`plans/PRICING_PIVOT_DECISIONS.md`))**:
> - **Free tier = 10 GB** (was 1 GB throughout v2 draft). Every "1 GB" Free-tier reference swapped to "10 GB". §5.5 disabled-state row and §8.3 modal copy updated.
> - **§8.3 modal example re-picked** — old "2.3 GB estimated / 0.3 GB remaining of 1 GB" was a tight squeeze that barely demonstrated the feature at the new 10 GB scale. New example: "~5 GB estimated / 2 GB remaining of 10 GB" — realistic 80%-full Free-tier org trying a moderate run; block fires cleanly, "Upgrade to Pro" CTA is the natural next action.
> - **§13.2 open-question cost example** updated: "Free 10 GB / Pro 100 GB / Enterprise 1 TB" (was 1 GB Free).
>
> **v2 revisions (2026-04-15 PM, per PRICING_PIVOT_DECISIONS.md (`plans/PRICING_PIVOT_DECISIONS.md`))**:
> - **Q3 SILENT** — removed Pro→Enterprise upgrade CTA from dashboard usage bar (§8.1) and from `volume_quota_*` notifications (§9.2). Pro/Enterprise orgs see color shifts and overage running-totals only, no in-app upsell. Only Free-tier orgs see upgrade CTAs.
> - **Q6 HARD SUNSET** — removed all legacy-plan migration UX. No opt-in dialog, no "you're on the old plan" banner, no 14-day window. Cutover flips server-side; V1 Plan rows deleted, V2 seeded. Existing Free signups silently land on V2 Free (same $0). §12 rewritten; §13.3 "legacy subscription path" question deleted.
> - **Q1 KEEP (mode-asymmetric billing)** — no change needed. §9.5 "Switch to ELT" nudge is already a dollar-savings framing; matches Growth's "ELT: lower cost for us, lower bill for you" talking point.
>
> **Dependencies on sibling specs** (not yet drafted — this spec declares contracts, Eng/Cloud/Infra fill in):
> - `plans/engineering/SPEC_ELT_IR_ARCHITECTURE.md` — IR dispatch, mode execution, auto-mode heuristic
> - `plans/engineering/SPEC_VOLUME_METERING.md` — GB counter, quota-enforcement API, usage ledger shape
> - `plans/growth/SPEC_PRICING_V2.md` — plan names, GB inclusions, $/GB overage rates
> - `plans/infra/SPEC_GB_THROUGHPUT_METRICS.md` — real-time usage readback for dashboard UX
> - `plans/qa/SPEC_VOLUME_METERING_TESTS.md` — acceptance criteria for metering correctness

---

## 1. Problem

Today, pipelines go through one implementation path (dlt ETL) and users are billed on three dimensions (seats, connections, run count). Two consequences:

1. **Negative unit economics at volume.** One user with a 1 TB pipeline pays $79 and costs us $50–150 to serve (see plans/product/price_insights.md §4 (`plans/product/notes/price_insights.md`)). Every large customer is a loss.
2. **No way to surface the cheaper path.** ELT (extract → raw → dbt transforms) costs us $0.01–0.03/GB vs $0.05–0.15/GB for ETL, but users can't opt into it because the mode doesn't exist in the product.

The UX question this spec answers: **how do we expose mode choice without burdening novice users, and how do we make cost visible before the bill arrives?**

## 2. Scope

**In scope**:
- Mode selector on the pipeline create/edit form
- Auto-mode heuristic (UX surface only — Eng owns the heuristic itself)
- Pre-run cost estimator
- Post-run cost display
- "Switch to ELT" migration nudge on existing pipelines
- Volume dimension in the dashboard usage bar
- Quota-at-100% blocking UI (predict-and-reject path A) and overage UI (allow-then-block path B)
- i18n keys across all 9 locales (en, ru, de, fr, es, el, zh, ar, sr)
- Interaction with Pipeline Templates, Notification Center, Getting Started checklist, Agent MCP API

**Out of scope** (deferred — separate follow-up specs if justified):
- Automatic ETL → ELT migration without user confirmation
- Per-pipeline billing attribution on the invoice PDF (Paddle handles invoice formatting)
- Per-user cost attribution within an org (all costs stay at org level)
- Historical run re-pricing when switching modes
- Marketing pricing-page copy (Growth's spec)
- On-boarding flow change for the first-pipeline case (see §9.3 — checklist stays as-is)

## 3. Users

Three personas, stack-ranked by frequency:

| Persona | Fraction | Knows ETL vs ELT? | Wants from this UX |
|---------|----------|-------------------|--------------------|
| **Novice** — technical founder or analyst building first pipeline | ~60% | No | "Just work. Tell me what it costs." |
| **Data engineer** — builds the team's data stack | ~30% | Yes, strongly preferred opinions | Mode control + clear cost impact. No "auto" surprises. |
| **Existing user** — has ETL pipelines, new billing dim applies | ~10% | Varies | "Will my stuff break? Will my bill change? How do I move?" |

**Design rule**: the novice path must finish successfully **without ever seeing the word "ELT"**. Expert controls exist behind a disclosure, not on the main surface.

## 4. Design principles

1. **Defaults do the work.** Mode defaults to `auto`. The novice never has to pick.
2. **Cost is visible before the money is spent.** Estimate shows on the form before the first run. Actual cost shows on the run detail immediately after completion.
3. **Nudges are informational, not manipulative.** The "Switch to ELT" banner explains savings in dollars, shows a dry-run preview, lets the user dismiss. No dark patterns.
4. **Back-compat for pipelines, not for plans.** Existing pipelines keep running with `mode=etl` until the user explicitly changes it — this is the "no silent mode changes" rule and it's absolute. **Plan migration is different**: per Q6 HARD SUNSET, V1 plans are deleted on cutover and any Free-tier signup silently lands on V2 Free. That's a silent plan migration, but only because (a) no paying customers exist, (b) the price stays $0, and (c) no user-visible UX changes without their action. At the pipeline level, nothing ever flips silently.
5. **Mode is a property of the pipeline, not the user.** Users can have an ETL pipeline and an ELT pipeline side by side. Changing mode re-runs the pipeline (full rebuild), doesn't transform in place.
6. **Volume is honest, not input-GB.** We meter *processed* GB (accounts for amplification, see price_insights.md §6). This spec assumes Cloud's SPEC_VOLUME_METERING delivers that number.

## 5. Mode selector

### 5.1 Location and default

Added to the pipeline form (`/pipelines/new`, `/pipelines/:id/edit`) **above the destination connection picker**. This is the first mode-aware decision; everything else depends on it.

**Default on the form**: `auto`. **Persisted in DB**: always a concrete mode (`etl` or `elt`). The UI form uses `auto` as a convenient default; the form **resolves auto → etl/elt at save time** based on Engineering's heuristic (SPEC_ELT_IR_ARCHITECTURE §5.2: sources ≥ 100 MB/run default to `elt`, smaller default to `etl`). The `Pipeline.mode` column stores only `etl` or `elt` — there is no persisted `auto` state. This keeps pipeline behavior stable between runs: once created, a pipeline's mode cannot silently flip.

> **Reconciled with Eng (2026-04-15)**: Eng's spec defines `mode` as binary (etl/elt). Treating `auto` as a UI-layer default that resolves at create-time keeps the DB model clean and satisfies the "no silent mode changes" design principle.

### 5.2 Three-state selector

```
┌─────────────────────────────────────────────────────┐
│  Pipeline mode                                  ⓘ   │
├─────────────────────────────────────────────────────┤
│  ◉ Auto   (recommended) — we pick the cheapest       │
│                           path for your data volume  │
│  ○ ETL    Classic — transform on the way in          │
│  ○ ELT    Streaming — transform after loading        │
└─────────────────────────────────────────────────────┘
```

Implementation: Reflex `rx.radio_group` with 3 options. Tooltip on the ⓘ icon explains mode choice in one line (see §10 copy).

When `auto` is selected, a read-only indicator below the selector shows what the heuristic **will resolve to on save**: `↳ Will save as: ELT (based on estimated 50 GB/run, your BigQuery destination supports streaming loads)`. This resolution updates reactively as the form changes. This line is **not** a form input — the user cannot click into it. If they want to override, they switch to the ETL or ELT radio. On save, the stored `Pipeline.mode` is the resolved concrete value.

### 5.3 Mode-specific help

When the user selects **ETL** or **ELT** (not `auto`), a one-line help text appears under the selector explaining what the user is committing to:

- **ETL**: "Datanika normalizes and types your data during extraction, then loads ready-to-query tables. Best for small-to-medium volumes where normalization cost is low."
- **ELT**: "Datanika loads raw data fast, then transforms in your warehouse using dbt. Best for large volumes and when your destination does the heavy lifting. Requires a warehouse that supports streaming loads."

The text is a single line; deeper explainers live in `/docs/pipelines` and are linked from a "Learn more" anchor.

### 5.4 Novice escape hatch

For users creating their first pipeline (checked against `users.onboarding_checklist_dismissed_at IS NULL` OR `pipelines.count == 0` for the org), the expert controls collapse behind a disclosure:

```
[ ▸ Advanced: pipeline mode ]
```

Expanded shows the radio group above. Collapsed uses `auto`. This reduces the first-pipeline form to the minimum set: name, destination, tables.

### 5.5 Disabled states and validation

Per Engineering SPEC_ELT_IR_ARCHITECTURE §5.3, ELT has native-destination adapters for **Postgres, Snowflake, BigQuery, ClickHouse, DuckDB** (5 destinations in P3). Non-native destinations fall back to dlt's loader in ELT shape via `_dlt_fallback.py` — the UX doesn't distinguish native vs fallback.

| Condition | Selector behavior |
|-----------|-------------------|
| Source is REST API or Kafka (no IR support in P3; arrives in P4 per ELT §9) | ELT option disabled with tooltip: "ELT for this source arrives in a later phase — currently ETL-only." Auto resolves to ETL. |
| Upload uses a merge strategy we can't express in dbt incremental (e.g. `scd2`, per ELT §6.3) | ELT option disabled with tooltip: "This pipeline's merge strategy isn't supported in ELT yet. ETL-only." |
| User is on the Free tier and the estimate exceeds the 10 GB/month Free allowance | Selector remains functional; quota banner appears below the form (see §8.3). |
| Editing an existing pipeline with runs | Changing mode requires explicit confirmation: "The next run will write to new locations (`raw.{table}` + `stg_{table}` for ELT vs `{dataset}.{table}` for ETL). Your existing data stays where it is — you can drop the old tables when confident." No silent rebuild, no data loss. |

> **Reconciled with Eng (2026-04-15)**: Earlier draft disabled ELT for DuckDB and local Postgres. Eng's ELT §5.3 confirms both are supported — DuckDB uses direct `COPY … FROM 'file.parquet'`, Postgres uses `COPY FROM STDIN BINARY`. Removed from disabled list.

## 6. Cost estimator (pre-run)

### 6.1 Where it appears

A **cost estimator panel** sits between the table-selection section and the submit button. It's not collapsible — it's the last thing the user sees before clicking Create.

Shape:

```
┌─────────────────────────────────────────────────────┐
│  Estimated cost                                      │
│                                                      │
│  Per run:   2.3 GB   ~$0.07                          │
│  Per month: 69 GB    ~$2.10       (daily schedule)   │
│                                                      │
│  ── Compared against Auto's alternative ──           │
│  ETL:  2.3 GB  ~$0.12  (currently selected)          │
│  ELT:  2.3 GB  ~$0.07  ↓ 42% cheaper                 │
│                                                      │
│  [ Estimate method ⓘ ]                               │
└─────────────────────────────────────────────────────┘
```

### 6.2 Inputs the estimator needs

The estimator is a thin UI client over the backend's `UploadService.predict_bytes()` / `PipelineService.predict_bytes()` (Cloud SPEC_VOLUME_METERING §5.4). The backend's prediction strategy (authoritative):

| Case | Strategy |
|------|----------|
| Upload (SQL source) | EWMA of last 5 successful runs' `bytes_processed` for this Upload |
| Upload (file source) | `file_size × 1.2` (parquet expansion heuristic) |
| Upload (SaaS source) | EWMA of last 5 runs |
| Pipeline (dbt) | Sum of `predicted_bytes` of input uploads |
| Transformation | `bytes_processed` of most recent successful run |

**First-run fallbacks** (no EWMA history yet) — in priority order:

1. **User hint** (optional) — if the user provides "Expected volume per run" in the form, use that as the seed estimate. Accepts GB with sensible bounds (0.01 GB – 100 TB). **This field is a first-run convenience, not a persisted primary source**: after the first real run, EWMA takes over and the user hint is ignored. This avoids the "user lied, EWMA is better" drift problem.
2. **Pipeline template default** — if the user came from a template, use the template's `typical_volume_gb` (§9.1).
3. **Source-type default** — each connector declares a typical GB/run figure in `datanika/data/connector_cost_profiles.py` (new file — Eng creates as part of SPEC_ELT_IR_ARCHITECTURE implementation).
4. **Genuinely unknown** (REST API without a `max_rows` hint, SaaS source without docs) — display "—" and label "Can't estimate — first run will calibrate."

The user-hint input lives in the **table-selection section** of the form as `form_volume_estimate_gb`. Small, optional, collapsed under "Advanced: expected volume" for novices.

> **Reconciled with Eng/Cloud (2026-04-15)**: Earlier draft treated `Pipeline.volume_estimate_gb` as a primary persisted input. Cloud's VOLUME §5.4 uses EWMA of run history as authoritative; user hint is only useful before the first run. Clarified that the field is a first-run seed, not a persistent override. Once `bytes_processed` history exists, EWMA wins.

### 6.3 Display rules

| State | Display |
|-------|---------|
| First-time create, no volume input, default connector profile | `2.3 GB  ~$0.07` with label "Estimated from source defaults" |
| User provided volume | Same numbers with label "Based on your estimate" |
| Editing with run history | Same numbers with label "Based on your last 10 runs (median 2.3 GB)" |
| Auto mode with fallback | Show both ETL and ELT numbers side by side with the winner highlighted |
| ELT mode unavailable (see §5.5) | Hide the ETL/ELT comparison row, show only the selected mode |
| Volume estimate is unknown (e.g., REST API with no `max_rows` hint) | Show `— / run` with label "Can't estimate — first run will calibrate" |

Cost ranges (low–high) are **not shown on the primary display**. The point estimate is the honest expected cost; ranges add noise for novices. Expert users who want bounds click the ⓘ icon to see a tooltip with p5–p95 bounds.

### 6.4 Scheduled-run extrapolation

If a schedule is set (cron or interval), the estimator multiplies per-run cost by expected runs-per-month and shows "Per month: N GB / ~$X (cadence: hourly)". If no schedule, shows only "Per run: ..." — never "Per month: $0" which is misleading.

### 6.5 Quota interaction

If the estimated monthly cost exceeds the org's included GB allowance (current-tier — see SPEC_PRICING_V2 for tier allowances), a warning appears above the cost panel:

> ⚠️ This pipeline's estimated monthly volume (**69 GB**) exceeds your Pro plan's 100 GB allowance by **nothing** — wait, let me recompute. Every **extra GB will cost $0.50** at your current rate.

(Example phrasing; exact copy in §10.)

## 7. Post-run cost display

### 7.1 Run detail page — summary header

After a run completes, the **run detail page** (`/runs/:id`) header gains a cost summary block next to the existing status/duration/rows-processed fields:

```
Status: ✓ Succeeded    Duration: 2m 14s    Rows: 1.2M    Data: 2.1 GB    Cost: $0.06
```

The `Cost:` field is new. Format: USD to two decimal places, or `< $0.01` if below threshold. Tooltip on hover: "Based on 2.1 GB processed × $0.03/GB (your Pro tier overage rate)."

### 7.2 Estimate vs actual callout

If the actual cost differs from the estimate by **>30%** in either direction, a callout appears below the summary header:

> 📊 This run cost **$0.06** — 40% higher than the $0.04 estimate. Likely cause: more data than expected (2.1 GB actual vs 1.5 GB estimated). Update the estimate on the pipeline edit page if this is the new normal.

The callout is dismissible and doesn't re-fire for the same pipeline on the same direction within 7 days (to avoid nag spam). If the user updates the estimate, the callout never re-fires.

### 7.3 Per-table breakdown

In the existing run logs section, each table row gains a GB and $ column:

| Table | Rows | Size | Cost |
|-------|------|------|------|
| customers | 10,000 | 12 MB | $0.00 |
| orders | 2,000,000 | 180 MB | $0.01 |
| line_items | 8,000,000 | 1.9 GB | $0.05 |

Sort order: by cost descending so the expensive tables are visible at the top. "$0.00" is displayed as "< $0.01" when the actual is below one cent but positive.

### 7.4 Dashboard run list

The existing runs table (dashboard + `/runs`) gains a "Cost" column, placed after "Duration". Same format as above. Column is sortable. For orgs on the Free tier (no overages possible), the column is hidden.

## 8. Volume billing UX

### 8.1 Dashboard usage bar

The existing "Plan Usage" card on the dashboard shows runs today. After the pivot, it extends to show both dimensions stacked:

```
┌─────────────────────────────────────────────┐
│  Plan Usage                          [Pro]  │
│                                             │
│  Runs     ▓▓▓▓▓▓▓▓▓░░░░░░░░░░  42%          │
│           6,300 / 15,000 runs this month    │
│                                             │
│  Volume   ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓░░░░  78%          │
│           78 / 100 GB processed this month  │
│                                             │
└─────────────────────────────────────────────┘
```

Color rules per dimension (reuse existing `runs_color` logic):
- Green <60%
- Yellow 60–79%
- Orange 80–99% — colored bar only, **no upgrade CTA** for Pro/Enterprise (per Q3 SILENT decision)
- Red ≥100% (shows "Overage: $X this month" for plans with overage allowed)

**Upgrade CTA** — shown **only for Free-tier orgs** when either dimension is ≥80%. For Pro/Enterprise, the bar color shift is the only signal; we do not nudge Pro users toward Enterprise in-app. Per PRICING_PIVOT_DECISIONS.md (`plans/PRICING_PIVOT_DECISIONS.md`) Q3: Pro customers past the Enterprise-crossover point (~740 GB/mo) find out from the invoice, not from an in-app banner.

### 8.2 Overage accrual display

For Pro + Enterprise plans (which allow overage), when Volume crosses 100%, the bar stays full-red and a running total appears:

```
Volume   ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  100%+
         115 / 100 GB processed this month
         Overage: 15 GB × $0.50 = $7.50 this month
```

Free tier has no overage — instead, runs are blocked once the quota hits 100% (see §8.3).

### 8.3 Quota-at-100% UX — Free tier only (Path A)

**Only Free-tier orgs see a pre-run quota modal.** Per Cloud SPEC_VOLUME_METERING §12.5, predict-and-reject applies only to `hard_cap_bytes=True` plans. Pro and Enterprise use Path B (allow-then-block, §8.4) — overage is inherently reactive; we don't pre-reject what we'd willingly bill for.

For **Free-tier orgs**, when a user clicks **Run now** and `predicted_bytes` would push the org over its 10 GB monthly allowance:

> **Volume quota reached**
>
> This run is estimated at **~5 GB**. You have **2 GB** remaining of your 10 GB Free allowance this month.
>
> **Options**:
> - Upgrade to Pro (100 GB included) → [Upgrade]
> - Cancel → [Cancel]

For Free, there is **no "Allow and run" option** — overage isn't offered because the plan doesn't allow it (`overage_bytes_price_cents_per_gb=NULL`).

For **Pro / Enterprise**, no pre-run modal is shown. The run proceeds; any overage accrues and shows up in the running-total display (§8.2), the post-run cost callout (§7.2), and the mid-run breach notification (§8.4) if it happened mid-run.

> **Reconciled with Eng (2026-04-15)**: Earlier draft showed a pre-run "Allow and run" modal for Pro users. Cloud's VOLUME §12.5 rules this out as a non-goal for P5 — Pro users should never be blocked pre-run, overage is an expected billing state. The cost-alert feature (user-configured dollar threshold) in the Notification Center covers the "heads up for a large run" case without the blocking UX.

### 8.4 Quota-breach mid-run (allow-then-block, Path B)

If a run starts within quota but the processed-GB counter crosses the threshold mid-run (Path B from cloud#18 hybrid), the run **completes** (partial block at the stream level is too complex for v1). Post-run:

1. A prominent notification fires via the Notification Center: "Run XYZ exceeded your monthly GB allowance by 5 GB. $2.50 overage accrued."
2. The dashboard usage bar updates immediately to reflect the overage.
3. Subsequent runs fall into the predict-and-reject path above until the next billing period.

### 8.5 Billing page (`/settings?tab=billing`)

The existing billing tab gains a **Volume usage** section below the existing Runs usage:

- Current month volume used / included
- Current month overage (if any) in GB and $
- Historical volume chart (last 6 months, stacked line: included vs overage)
- Link to Plans comparison with the GB dimension

Chart implementation: reuse existing `recharts` integration. No new chart library.

### 8.6 Invoice preview

When Paddle sends a webhook indicating the next invoice is generated, the billing tab shows a preview:

> **Next invoice (generates 2026-05-01)**:
> - Pro subscription: $79.00
> - Volume overage: 15 GB × $0.50 = $7.50
> - **Estimated total: $86.50**

Labeled clearly as "estimated" — final amount is Paddle's call. Updates live as overage accrues.

## 9. Integration with existing surfaces

### 9.1 Pipeline templates

Each `PipelineTemplate` dataclass in `datanika/data/pipeline_templates.py` gains a new field:

```python
@dataclass(frozen=True)
class PipelineTemplate:
    ...
    typical_volume_gb: float  # per-run estimate, used by the cost estimator
    recommended_mode: PipelineMode = PipelineMode.AUTO
```

The 3 current templates get reasonable defaults:

| Template | Typical volume/run | Recommended mode |
|----------|--------------------|------------------|
| stripe-to-postgres | 0.3 GB | Auto (resolves to ETL at low volume) |
| postgres-to-bigquery | 5 GB | Auto (resolves to ELT when BigQuery is the destination) |
| csv-to-duckdb | 0.1 GB | ETL (DuckDB doesn't support ELT) |

The cost estimator reads these when the user arrives from a `?template=<slug>` URL. Numbers are visible on the `/templates/[slug]` public landing page (Option C funnel) too — cold-traffic visitors see the volume-aware cost upfront (per PRICING_PIVOT.md "Watch for" note).

### 9.2 Notification Center

Three new notification types (Engineering + Cloud own emitting these — this spec defines the UX):

| Event type | Trigger | UI treatment |
|------------|---------|--------------|
| `volume_quota_warning_80` | Org hits 80% of monthly volume | Orange icon, one-line message. "Upgrade" CTA **only for Free tier** (per Q3 SILENT) |
| `volume_quota_exceeded` | Org hits 100%, run was blocked | Red icon, "X runs blocked in the last hour" summary. "Upgrade" CTA **only for Free tier** — Pro/Enterprise runs aren't blocked; this event only fires for Free |
| `cost_threshold_exceeded` | User-configured dollar threshold crossed (new setting) | Blue icon, shows current month-to-date spend. No upgrade CTA — this is informational |

The existing Notification Center UI (`/notifications`, bell icon) doesn't need structural changes — it's a new icon color + a couple of copy keys. Event copy is in §10.

### 9.3 Getting Started checklist

**No changes.** The 5-step checklist (add connection → create pipeline → run it → add transformation → schedule) stays unchanged. Volume awareness is a second-pipeline concern, not a first-pipeline concern. Adding it to the checklist would dilute the activation signal.

### 9.4 Agent MCP API

Per Engineering SPEC_ELT_IR_ARCHITECTURE §10, two new MCP tools land in core#153:

1. **`estimate_run_cost(pipeline_id: int)`** — predicts GB and USD for the next run of an existing pipeline. Returns:
   ```json
   {
     "mode": "etl|elt",
     "estimated_bytes": 2469606195,
     "estimated_gb_per_run": 2.3,
     "estimated_usd_per_run": 0.07,
     "estimated_monthly_usd": 2.10,
     "prediction_method": "ewma_last_5_runs|source_introspection|fallback"
   }
   ```

2. **`migrate_to_elt(pipeline_id: int)`** — triggers the same action as the UI "Switch to ELT" button (§9.5). Returns the new IR and the list of raw tables that will be created.

A third tool covers the pre-create case (not enumerated in Eng's spec but needed for the first-pipeline conversational flow):

3. **`estimate_cost_for_config(source_type, destination_type, tables[], mode?, volume_estimate_gb?)`** — estimates cost for a hypothetical pipeline config (used before an agent actually creates the pipeline). Returns the same shape as #1 plus `mode_will_resolve_to` if the caller passed `auto`.

These are listed in the MCP tool catalog after this UX spec lands. The cost estimator logic lives in a shared service (`datanika/services/cost_estimator.py`) consumed by both the UI and MCP — single source of truth for all cost predictions.

> **Reconciled with Eng (2026-04-15)**: Earlier draft named the tool `estimate_pipeline_cost` and required full config fields. Adopted Eng's `estimate_run_cost(pipeline_id)` shape for existing pipelines. Added a companion `estimate_cost_for_config` for the pre-create case — Eng's spec doesn't explicitly rule this out; it's additive.

### 9.5 "Switch to ELT" migration nudge

**Trigger condition** (AND gate):
- Pipeline is in ETL mode
- Pipeline has ≥5 successful runs in the last 30 days
- Destination supports ELT (BigQuery, Snowflake, Redshift, ClickHouse)
- Cumulative volume in last 30 days is ≥20 GB
- User has not dismissed this nudge for this pipeline in the last 30 days

**UI**: a dismissible banner at the top of the pipeline detail page:

```
┌─────────────────────────────────────────────────────────┐
│  💡 You could save ~$X/month by switching to ELT        │
│                                                         │
│  This pipeline has processed 47 GB in the last 30 days  │
│  at $2.35. ELT mode estimates the same data at $0.47.   │
│                                                         │
│  [ Preview what changes ]  [ Switch to ELT ]  [ × ]     │
└─────────────────────────────────────────────────────────┘
```

**Preview what changes** → modal showing:
- Estimated new cost
- New tables that will be created (`raw.{table}` and `stg_{table}` per Eng ELT §6.4). **Existing ETL tables are not touched** — they remain in their current location until the user drops them manually.
- dbt staging models that Datanika will auto-generate (one `stg_*.sql` per source table, typed via the IR's `cast()` macros — Eng ELT §5.4)
- Warning if existing downstream dbt models depend on the ETL tables (we check via the dbt manifest): "These models still reference your old ETL tables and will continue to work until you rewire them."

**Switch to ELT** → confirmation dialog: "The next run of this pipeline will write to new locations (`raw.{table}` + `stg_{table}`). Your existing ETL tables stay where they are — you can drop them when confident. Continue?" — Yes/No.

> **Reconciled with Eng (2026-04-15)**: Earlier draft said "existing data will be replaced" — this is wrong. Per ELT §6.4, ETL and ELT write to different tables and migration does not touch existing data. The design is deliberately two-phase: new tables land, user verifies, user drops old tables. Zero risk of silent schema drift or data loss during migration.

**Dismiss (×)** → no nudge for 30 days on this pipeline. User can re-trigger by clicking a "Review ELT savings" link in the pipeline settings.

Dismissal is per-pipeline, per-org, stored in a new `pipeline_prefs.elt_nudge_dismissed_at` column (cheap — Eng adds in SPEC_ELT_IR_ARCHITECTURE). Not a full user pref entity; just this one flag.

## 10. Copy & i18n keys

### 10.1 New keys (English; other 8 locales populated in a parallel i18n pass)

**Namespace: `pipelines.*` (extends existing 25 keys)**

| Key | English copy |
|-----|--------------|
| `pipelines.mode_label` | Pipeline mode |
| `pipelines.mode_tooltip` | Controls how Datanika processes your data. Auto picks the cheaper option. |
| `pipelines.mode_auto` | Auto (recommended) |
| `pipelines.mode_auto_hint` | We pick the cheapest path for your data volume. |
| `pipelines.mode_etl` | ETL |
| `pipelines.mode_etl_hint` | Classic — transform on the way in. Best for small-to-medium volumes. |
| `pipelines.mode_elt` | ELT |
| `pipelines.mode_elt_hint` | Streaming — transform after loading. Best for large volumes with a warehouse that supports streaming loads. |
| `pipelines.mode_resolved_prefix` | Resolved: |
| `pipelines.mode_advanced_toggle` | Advanced: pipeline mode |
| `pipelines.mode_change_confirm_title` | Change pipeline mode? |
| `pipelines.mode_change_confirm_body` | This will rebuild the full dataset on the next run. |
| `pipelines.mode_elt_requires_warehouse` | ELT requires a warehouse destination (BigQuery, Snowflake, Redshift, ClickHouse). |
| `pipelines.mode_elt_source_unsupported` | ELT is coming to this source — currently ETL-only. |
| `pipelines.volume_estimate_label` | Expected volume per run |
| `pipelines.volume_estimate_placeholder` | e.g. 2 GB |
| `pipelines.volume_estimate_hint` | Optional. We'll estimate from your source if you leave this blank. |

**Namespace: `cost.*` (new)**

| Key | English copy |
|-----|--------------|
| `cost.section_title` | Estimated cost |
| `cost.per_run` | Per run |
| `cost.per_month` | Per month |
| `cost.per_month_cadence` | (cadence: {cadence}) |
| `cost.compared_modes` | Compared against Auto's alternative |
| `cost.cheaper_by` | ↓ {percent}% cheaper |
| `cost.method_label` | Estimate method |
| `cost.method_source_defaults` | Estimated from source defaults |
| `cost.method_user_input` | Based on your estimate |
| `cost.method_history` | Based on your last 10 runs (median {median}) |
| `cost.method_unknown` | Can't estimate — first run will calibrate |
| `cost.actual_run_cost` | Cost |
| `cost.actual_vs_estimate_higher` | This run cost **{actual}** — {delta}% higher than the {estimated} estimate. |
| `cost.actual_vs_estimate_lower` | This run cost **{actual}** — {delta}% lower than the {estimated} estimate. |
| `cost.below_cent` | < $0.01 |

**Namespace: `quota.*` (extends existing 2 keys)**

| Key | English copy |
|-----|--------------|
| `quota.volume_title` | Volume |
| `quota.volume_usage` | {used} / {limit} GB processed this month |
| `quota.volume_overage` | Overage: {gb} GB × ${rate} = ${total} this month |
| `quota.volume_quota_reached_title` | Volume quota reached |
| `quota.volume_quota_reached_body` | This run would use ~**{needed} GB**. You have **{remaining} GB** remaining of your {plan} allowance this month. |
| `quota.allow_as_overage` | Allow this run as overage ({gb} GB × ${rate} = ${total}) |
| `quota.allow_as_overage_button` | Allow and run |
| `quota.upgrade_to_next_tier` | Upgrade to {plan} ({gb} included) — rendered only in Free-tier surfaces; never Pro→Enterprise per Q3 SILENT |

**Namespace: `billing.*` (new)**

| Key | English copy |
|-----|--------------|
| `billing.volume_usage_title` | Volume usage |
| `billing.volume_usage_history_title` | Last 6 months |
| `billing.next_invoice_preview_title` | Next invoice (generates {date}) |
| `billing.next_invoice_estimated_total` | Estimated total |
| `billing.overage_line_item` | Volume overage: {gb} GB × ${rate} = ${total} |

**Namespace: `nudge.*` (new)**

| Key | English copy |
|-----|--------------|
| `nudge.elt_migration_title` | 💡 You could save ~${savings}/month by switching to ELT |
| `nudge.elt_migration_body` | This pipeline has processed **{gb} GB** in the last 30 days at **${current_cost}**. ELT mode estimates the same data at **${new_cost}**. |
| `nudge.elt_preview_button` | Preview what changes |
| `nudge.elt_switch_button` | Switch to ELT |
| `nudge.elt_dismiss` | Dismiss |
| `nudge.elt_preview_title` | What changes if you switch to ELT |
| `nudge.elt_preview_new_cost` | Estimated new cost |
| `nudge.elt_preview_tables_rebuilt` | Tables that will be reloaded from scratch |
| `nudge.elt_preview_dbt_models_generated` | dbt models Datanika will auto-generate |
| `nudge.elt_preview_dbt_dependencies_warning` | ⚠ Existing dbt models depend on ETL's typed output |
| `nudge.elt_switch_confirm_title` | Rebuild all tables in ELT mode? |
| `nudge.elt_switch_confirm_body` | The next run will replace existing data in the destination. |

**Namespace: `notifications.*` (extends existing)**

| Key | English copy |
|-----|--------------|
| `notifications.volume_quota_warning_80` | You've used 80% of your monthly volume quota ({used} / {limit} GB). |
| `notifications.volume_quota_exceeded` | Volume quota reached — {count} runs blocked in the last hour. |
| `notifications.cost_threshold_exceeded` | Month-to-date spend has crossed your alert threshold of ${threshold}. |

### 10.2 Locale parity

All 59 new keys land in **all 9 locales** (`en, ru, de, fr, es, el, zh, ar, sr`) in the same PR as the UI code. Product drafts the English; a parallel translation pass handles the other 8. Test `tests/test_i18n/test_i18n.py::test_all_locales_have_same_keys` enforces parity.

### 10.3 Translation considerations

- **"ETL" / "ELT"** are acronyms; left as-is in all locales.
- **"$0.50", "$2.10"** — currency formatting stays in USD for v1 (Paddle bills in USD). Locale-specific number separators (e.g., `€2,10` for EU locales) are a future concern, not this spec.
- **RTL (Arabic)** — the usage bar's fill direction mirrors for RTL locales; numbers stay LTR per web convention.
- **Percentage formatting** — use locale's native decimal separator via `Intl.NumberFormat` (Reflex exposes this via `rx.format_number` helper).

## 11. State and service changes (Engineering owns implementation)

This spec declares the shape; Engineering's SPEC_ELT_IR_ARCHITECTURE defines implementation.

**Pipeline model** — aligned with Eng SPEC_ELT_IR_ARCHITECTURE §5.6:
```python
class PipelineMode(StrEnum):
    # Only two persisted values. `auto` is a UI-layer default that resolves
    # to one of these at pipeline-create time.
    ETL = "etl"
    ELT = "elt"

class Pipeline(TenantMixin, TimestampMixin, Base):
    ...
    mode: Mapped[PipelineMode] = mapped_column(
        Enum(PipelineMode, native_enum=False, values_callable=lambda e: [i.value for i in e]),
        default=PipelineMode.ETL,  # back-fill for existing rows
        nullable=False,
    )
    ir: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # Eng ELT §5.1
    elt_nudge_dismissed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

# `Upload.volume_estimate_gb` — user-hint column, used only before first run.
# After first successful run, EWMA of bytes_processed replaces this value in
# predictions. Keeping the column around is cheap and useful if all runs fail.
class Upload(TenantMixin, TimestampMixin, Base):
    ...
    volume_estimate_gb: Mapped[float | None] = mapped_column(Float, nullable=True)

# Run.bytes_processed added by Eng ELT §5.6 — not duplicated here.
```

**State additions on `PipelineState`**:
- `form_mode: str = "auto"` — UI-layer only; resolves to `"etl"` or `"elt"` on submit
- `form_mode_will_resolve_to: str = ""` — computed reactively; shown in the "Will save as: …" indicator (§5.2)
- `form_volume_estimate_gb: float | None = None` — optional first-run hint
- `cost_estimate: dict = {}` — populated reactively via the shared `cost_estimator.py` service (§9.4)
- `elt_nudge_visible: bool = False` — computed from pipeline history

> **Reconciled with Eng (2026-04-15)**: Dropped `PipelineMode.AUTO` from the persisted enum. Dropped `Pipeline.volume_estimate_gb` in favor of `Upload.volume_estimate_gb` (the actual source of variable volume — pipelines inherit from their uploads).

**New service: `PipelineCostService`** (or function within `PipelineService`):
- `estimate_cost(source_type, dest_type, tables, mode, volume_gb=None) -> CostEstimate`
- Returns `{mode_resolved, gb_per_run, usd_per_run, usd_per_month}` with a dependency on Eng's resolver for `mode_resolved`.

**Hooks** (extends `datanika/hooks.py`):
- `pipeline.before_run` — already exists. Cloud plugin's quota handler extends to check volume quota (predict-and-reject Path A).
- `run.volume_processed` — new event emitted after run completes. Cloud plugin handler records GB to usage ledger.

**Migrations**:
- One Alembic migration adds `pipelines.mode`, `pipelines.volume_estimate_gb`, `pipelines.elt_nudge_dismissed_at`. Back-fills `mode = 'etl'` for existing rows (back-compat — existing pipelines were ETL).
- One migration (in `datanika-cloud`) adds a `volume_gb_processed` column to `usage_ledger`.

## 12. Back-compat

Per PRICING_PIVOT_DECISIONS.md (`plans/PRICING_PIVOT_DECISIONS.md`) Q6 (HARD SUNSET): **zero paying customers at cutover means no plan migration UI.** V1 Plan rows get deleted on cutover day; V2 Plan rows seed fresh; any existing Free-tier signup silently lands on V2 Free (same $0 price). There is no "opt-in" dialog, no "you're on the legacy plan" banner, no 14-day grace period UI, no choice modal. Cutover is a server-side flip, invisible to users.

1. **Existing pipelines default to `mode = etl`** — confirmed above. Alembic back-fills on deploy. No change in execution path for anyone not interacting with the new selector.
2. **Billing dimension appears on cutover day** — on cutover, V2 Plan rows are active and the dual-dim usage bar renders immediately for all orgs. No opt-in UI. No migration banner. No legacy-plan fallback state the user can see.
3. **Cost estimator is always live post-cutover** — every org is on V2 Plan, so the estimator returns real numbers for everyone. No "—" fallback for "legacy plan" orgs, because no legacy plan orgs exist post-cutover.
4. **Notification Center events fire for every org** — `volume_quota_*` events are active uniformly. No conditional skip for "old plan" orgs.
5. **Defensive code path stays** — the `plan.bytes_included IS NULL` quota handler early-return (Cloud VOLUME §5.3) stays as a safety net for any stray V1 row that survives the cutover delete. It's a one-line no-op, not a UX state. The user never sees it.
6. **API contract**: no existing `/api/v1/*` endpoints change. New endpoints (cost estimate, volume usage) get `x-stability: experimental` until Engineering stabilizes per SPEC_ELT_IR_ARCHITECTURE.

> **Reconciled with PRICING_PIVOT_DECISIONS.md Q6 (2026-04-15)**: Earlier draft had "users on the old Pro plan stay on the old plan until they pick the new plan" + "cost estimator shows '—' for legacy-plan orgs" + 14-day opt-in window UX. All deleted. Zero paying customers at pivot time makes the entire legacy-subscription UX surface obsolete.

## 13. Open questions — updated 2026-04-15 against Eng sibling specs

### 13.1 Resolved by sibling specs

| Q | Answered by | Resolution |
|---|-------------|------------|
| Auto-mode heuristic | Eng SPEC_ELT_IR_ARCHITECTURE §5.2, §12.3 | **`≥100 MB/run → elt`**. Threshold is `DATANIKA_ELT_DEFAULT_THRESHOLD_MB` env var (default 100). Sources without IR support (REST API, Kafka in P3) always resolve to ETL. Destination capability also checked (native or `_dlt_fallback`). |
| Paddle GB-metering mechanics | Cloud SPEC_VOLUME_METERING §5.5, §11 | **Meter key `datanika_bytes_processed`, unit=bytes, aggregation=sum, reset=monthly aligned to sub period**. Idempotency key: `org_{id}_{YYYY-MM}_bytes_processed_rev_{n}`. Paddle dedupes by key; retries safe. Display math (bytes → GB) happens at render, not in the meter. |
| Paddle-unreachable fallback | Cloud SPEC_VOLUME_METERING §11 | **5xx → retry via Celery default policy; 4xx → alert oncall, leave row unflipped for manual inspection; 409 duplicate → treat as success**. No user-facing fallback needed for the §8.3 modal — quota check is local to cloud ledger, doesn't hit Paddle. |

### 13.2 Resolved 2026-04-16 — all open questions closed

| Q | Owner | Resolution |
|---|-------|------------|
| How fresh is the "78 GB / 100 GB" usage number? | Infra — SPEC_GB_THROUGHPUT_METRICS | **≤5 min staleness via cached API endpoint + manual refresh button.** The dashboard usage bar reads from `GET /api/v1/usage/volume` backed by `UsageLedger` (Postgres) — NOT Prometheus. Server-side 5-min cache (simple TTL on the service method). A "Refresh" icon-button on the bar forces a cache-bust for users who just finished a run and want instant feedback. No WebSocket subscription — the bar is not latency-sensitive enough to justify the complexity. Infra's Prometheus track (SPEC_GB_THROUGHPUT_METRICS §3) is for operational dashboards and anomaly alerts, not billing-grade UX. §8.1 implementation: poll on page load + 5-min `setInterval` + manual refresh. Show "Updated N min ago" footer when staleness > 60s. |
| What's the exact $/GB rate per plan? | Growth — SPEC_PRICING_V2 | **Locked in SPEC_PRICING_V2 v3 §2.1 (not illustrative).** Free = hard cap at 10 GB, no overage rate (block at limit). Pro = **$0.50 per extra GB** (`overage_bytes_price_cents_per_gb = 50`). Enterprise = **$0.25 per extra GB** (`overage_bytes_price_cents_per_gb = 25`). These are the production `Plan` seed values. §6 cost estimator: display `$0.50/GB` for Pro, `$0.25/GB` for Enterprise. §8 usage bar overage hint: "Each extra GB costs $0.50" (Pro) or "$0.25" (Enterprise). §10 i18n tokens: `cost.overage_rate_pro` = "$0.50/GB", `cost.overage_rate_enterprise` = "$0.25/GB". |
| Can a running pipeline show a live GB counter? | Infra — SPEC_GB_THROUGHPUT_METRICS | **No — not in V2 scope.** SPEC_VOLUME_METERING §3 explicitly declares "Real-time byte accounting" a non-goal; meter is batched per-run, synced hourly. §7 of this spec correctly shows cost only **after** run completion. The mid-run bytes counter from VA2 in PLAN_PRODUCT.md is **descoped** from V2 P3 — it would require a separate Prometheus scrape path or in-process byte accumulator that neither Engineering nor Infra specs provision. If demand surfaces post-V2, revisit as a Prometheus-backed experimental endpoint (`x-stability: experimental`). The remaining 3 VA2 surfaces (cost estimator, ELT nudge, billing preview) are unaffected. |
| Unit attribution for shared-destination runs | Engineering — SPEC_ELT_IR_ARCHITECTURE | **Deferred to P4.** Eng's current IR (§5.1) has one `target` per IR; shared destinations aren't in scope for P3. Re-open in P4 if use case appears. |

### 13.3 New questions raised by reconciliation

| Q | Owner | Note |
|---|-------|------|
| i18n namespace reconciliation: my `quota.*` vs Cloud's `billing.error.*` for quota-exceeded errors | Product + Cloud | Both namespaces are reasonable. Recommendation: keep `quota.*` for dashboard-surface UX (warning bars, usage display); use `billing.error.*` for API-level error translations that back-map from `QuotaExceededError(metric=…)`. Cloud's §6 defines 3 `billing.error.*` / `billing.warning.*` keys; my §10 defines 5 `quota.*` keys. They don't collide — they serve different call sites. **No change needed; document the split.** |

> **Removed 2026-04-15 per PRICING_PIVOT_DECISIONS.md Q6**: The "Legacy subscription path — when does an existing Pro subscriber see the new UX?" question is obsolete. Zero paying customers at cutover = no migration window, no legacy-plan UX state, no opt-in dialog. Cutover is a server-side flip; users see the V2 experience on day one.

**All open questions resolved 2026-04-16.** No remaining blockers from sibling specs. V2 P3 UI implementation (VA2) can proceed without coordination latency on any of the 3 previously-open questions.

## 14. Non-goals (deferred)

- **Per-pipeline billing alerts.** Use Notification Center with `cost_threshold_exceeded`; per-pipeline alerts are follow-up work if this isn't granular enough.
- **Automatic ETL → ELT migration.** Users explicitly click "Switch to ELT". Auto-migration on cost grounds is a trust issue we don't want to take on in v1.
- **Historical re-billing.** When a user switches from the old plan to the new plan, we don't retroactively meter their old runs. They start on volume billing from the transition date.
- **Sub-org (project/workspace) cost attribution.** Multi-tenant is at org level. Sub-org hierarchies are not in scope.
- **Pipeline-level cost caps** ("block this pipeline at $10/month"). Use the Notification Center threshold alert; hard caps at pipeline level are a separate feature if customers ask.
- **BYOD (bring-your-own-destination) pricing** — where the customer's warehouse cost isn't on Datanika's bill anyway. Keep it simple for v1.
- **Estimator for custom REST API sources** — falls into the "unknown" bucket (§6.3). Improving this is a follow-up with live calibration after first run.

## 15. Acceptance criteria (implementation phase)

When Engineering + Cloud are ready to ship, these are the UX acceptance tests this spec demands:

1. **Novice first-pipeline flow** — user with no pipelines can create and run a pipeline end-to-end without seeing the word "ELT" anywhere on the screen.
2. **Expert mode control** — user can pick ETL or ELT explicitly; the choice persists; the next run uses the selected mode.
3. **Cost pre-visibility** — on the pipeline form, the cost panel shows a number (or "—" if genuinely unknown) before the first run.
4. **Cost post-visibility** — on any run detail page after a successful run, the Cost field is populated and the value is within 30% of what the usage ledger records for that run.
5. **Quota block at 100% — Free tier only** (Path A per §8.3) — a Free-tier org whose `predicted_bytes` would exceed the remaining quota sees the modal before the run starts. Pro/Enterprise orgs do **not** see this modal; their runs proceed and accrue overage.
6. **Quota breach mid-run** (Path B) — a run that exceeds mid-way completes, notifies via Notification Center within 60 seconds, and updates the dashboard bar.
7. **No Pro→Enterprise upgrade CTAs anywhere** (per Q3 SILENT) — dashboard usage bar shows color shift only (no CTA) for Pro/Enterprise orgs at 80%+; `volume_quota_warning_80` notification has no Upgrade button for Pro/Enterprise recipients.
8. **ELT nudge shows** — a test pipeline with ≥5 runs and ≥20 GB in the last 30 days shows the banner on the pipeline detail page. Banner text is savings-oriented per Q1 ("save ~$X/month"), not tax-neutral.
9. **ELT nudge dismissal** — after clicking ×, the banner does not reappear for 30 days on the same pipeline.
10. **i18n parity** — all 59 new keys exist in all 9 locales; the parity test passes.
11. **Pipeline back-compat** — on deploy day, existing pipeline rows back-fill to `mode = 'etl'` and run identically to pre-deploy. Post-cutover, the dashboard shows the dual-dimension bar for all orgs (no legacy single-dim bar state). No in-app "you were migrated" banner appears anywhere — cutover is invisible to users.
12. **Agent MCP tool** — `estimate_run_cost(pipeline_id)` returns a non-null estimate for any pipeline with ≥1 successful run; `estimate_cost_for_config(...)` returns a non-null estimate for the stripe-to-postgres template inputs.

## 16. Cross-team handoff

**To Engineering (core)**: this spec defines the form fields and state shape; your SPEC_ELT_IR_ARCHITECTURE defines the execution path. Coordinate on the `Pipeline.mode` enum name (we proposed `PipelineMode.AUTO/ETL/ELT`).

**To Engineering (cloud)**: this spec defines the billing UX; your SPEC_VOLUME_METERING defines the counter. We need `/api/v1/usage/volume` returning current-month GB used with freshness ≤5 min for §8.1.

**To Growth**: this spec defines the in-app copy; your SPEC_PRICING_V2 defines the marketing copy + plan tiers. **Resolved 2026-04-16**: $/GB rates locked — Pro $0.50/GB, Enterprise $0.25/GB. Copy tokens in §10 use these values directly.

**To Infra**: SPEC_GB_THROUGHPUT_METRICS determines what per-tenant aggregation backs the dashboard bar. **Resolved 2026-04-16**: ≤5 min staleness via cached `UsageLedger` query + manual refresh button (§13.2). §8.1 shows "Updated N min ago" footer when staleness > 60s. No mid-run counter in V2 scope.

**To QA**: SPEC_VOLUME_METERING_TESTS should include the acceptance criteria in §15 as E2E test cases.

---

*End of spec. Review cycle: sibling specs land → full review → user sign-off → atomic implementation sequence owned by orchestrator per PRICING_PIVOT.md §Sequencing.*
