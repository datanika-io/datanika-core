# Specs

Design contracts for work that spans more than one session or more than one department. A spec is
read before the code is written and amended when the design changes — it is the contract, not a
description of what shipped. What shipped is described in [`DESIGN.md`](../../DESIGN.md) and in the
code.

## Engineering

| Spec | Governs | Status |
|---|---|---|
| [`SPEC_ELT_IR_ARCHITECTURE.md`](SPEC_ELT_IR_ARCHITECTURE.md) | The intermediate representation between every source and destination, and the ETL/ELT mode dispatch built on it. P1–P4 shipped; the file-source IR builder is not built. | Partially implemented |
| [`SPEC_OPENAPI_CONNECTOR.md`](SPEC_OPENAPI_CONNECTOR.md) | The parametric `openapi` connection type — an OpenAPI 3.x document in, a working `rest_api` source out. | Spec only, no code ([#310](https://github.com/datanika-io/datanika-core/issues/310)) |
| [`SPEC_REMOTE_MCP.md`](SPEC_REMOTE_MCP.md) | The hosted Streamable-HTTP MCP endpoint at `/mcp` and its OAuth 2.1 authorization server. | P1 + P2 shipped |
| [`SPEC_EXPAND_CONTRACT_MIGRATIONS.md`](SPEC_EXPAND_CONTRACT_MIGRATIONS.md) | Which schema changes a migration may make given that blue/green runs the **previously deployed code against the new schema**. Written by Infra, binds Engineering. | Policy, in force |
| [`SPEC_RELEASE_VERSIONING.md`](SPEC_RELEASE_VERSIONING.md) | The `0.x` SemVer scheme, the `v*` tag contract, and what a release is and is not. Written by Infra, binds whoever cuts a tag. | Policy, in force |

The first three are owned by Engineering; the last two are Infra-authored policy that constrains
engineering work, which is why they sit in the same index rather than in a separate one.

## Product

Product writes the spec; Engineering implements against it; the acceptance criteria in each spec are
what "done" means from the user's side.

> 🚨 **A status cell is a claim with an expiry date, and three of them here were stale on
> 2026-09-07** — one of them stating a measured zero that had become a five. Two specs were on disk
> and **absent from this table entirely**, which is worse than a stale cell: an unindexed spec is a
> contract nobody finds. **Re-derive a cell before relying on it**, and prefer *"merged to `dev`,
> not in production"* to *"shipped"* — on this project those are different facts and issues close on
> the second one.
>
> ⚠️ **`SPEC_VOLUME_METERING`, `SPEC_GB_THROUGHPUT_METRICS` and `SPEC_BILLING_SELF_SERVICE` are
> named below and are deliberately NOT in this repo** (see the footer). A completeness check that
> diffs "files on disk" against "names in this file" reports those three as dangling; they are not.

| Spec | Governs | Status |
|---|---|---|
| [`SPEC_LOCALE_REACHABILITY.md`](SPEC_LOCALE_REACHABILITY.md) | Nine locales, reachable where they matter and consistent within a screen. The switcher is **sidebar-only**, so **464 translated strings on the four pre-auth screens cannot be reached** by the people they were written for — and the locale persists nowhere, so a switcher alone would forget the choice by the time the emailed reset link is opened. Also decides Serbian = **Cyrillic** and Spanish = **tú**, and records why a key-parity test is structurally blind to both. | Spec only ([#696](https://github.com/datanika-io/datanika-core/issues/696), [#695](https://github.com/datanika-io/datanika-core/issues/695)) |
| [`SPEC_EARNED_VERDICTS.md`](SPEC_EARNED_VERDICTS.md) | The product may not report an outcome it did not measure. The contract behind the two fixes for [#821](https://github.com/datanika-io/datanika-core/issues/821) (a green Test Connection that made no request) and [#823](https://github.com/datanika-io/datanika-core/issues/823) (a green run that loaded one page of fifteen): what "not tested" must look like on every surface, and what a load may claim about its own completeness. 🚨 **§0 records that both defects are already fixed and live** — read it before working the issue bodies. | Spec only ([#821](https://github.com/datanika-io/datanika-core/issues/821), [#823](https://github.com/datanika-io/datanika-core/issues/823)) |
| [`SPEC_AUDIT_TRAIL.md`](SPEC_AUDIT_TRAIL.md) | What a mutating handler owes the audit record: one transaction, a valid action, a filterable resource type, a payload a human can read. 🚨 **§4 carries three test-design clauses that were falsified by measurement, two of which went red on correct code** — read the corrections before writing the tests. §6 is ruled *not resolved* while the fixes sit on `dev`. | Merged to `dev` ([#934](https://github.com/datanika-io/datanika-core/issues/934), [#1127](https://github.com/datanika-io/datanika-core/issues/1127), [#1128](https://github.com/datanika-io/datanika-core/issues/1128)); **not yet in production** |
| [`SPEC_LOCAL_FILE_CONNECTIONS.md`](SPEC_LOCAL_FILE_CONNECTIONS.md) | What Test Connection *means* on a DuckDB or SQLite file source — the question the two filed defects did not contain. Verified by calling `ConnectionService.test_connection` directly, with controls in both directions. | Contract ([#978](https://github.com/datanika-io/datanika-core/issues/978), [#979](https://github.com/datanika-io/datanika-core/issues/979)) |
| [`SPEC_PAGE_ENTRY.md`](SPEC_PAGE_ENTRY.md) | What happens when a browser arrives at a URL: who may enter, what runs on entry, and what is on screen while it runs. Covers the `/signup` **session substitution** (a signed-in user who submits the form is silently re-identified), the sign-in/sign-up affordance inversion, the loading state, and the unguarded dashboard loader. 🚨 **§0 retracts four claims I made on the issues themselves** — including *"blank screen"* (a spinner is already there) and *"17 protected pages"* (it is 14). Read §0 before the ACs. | 🔄 **All four sections merged to `dev`; not yet in production.** *(Was "Spec only" until 2026-09-07.)* ([#1081](https://github.com/datanika-io/datanika-core/issues/1081), [#1090](https://github.com/datanika-io/datanika-core/issues/1090), [#1097](https://github.com/datanika-io/datanika-core/issues/1097)) |
| [`SPEC_PII_SEPARATION.md`](SPEC_PII_SEPARATION.md) | Personal data into `<parent>_pii` tables with a shared PK/FK; erasure, org deletion, email change. Amended against two production column censuses (§2a–§2c) and, 2026-09-02, with **§0 — the soft-delete / hard-delete split**, which surfaced two gaps where a soft delete stood in for an erasure. | 🔄 **Partly built.** 🔴 *This cell said "**No feature code** — 0 hits for `redact_pii_payload`/`PII_PAYLOAD_KEYS`" until 2026-09-07. That measurement was taken on 2026-09-02 and is false now:* `redact_pii_payload` appears **5×** and `PII_PAYLOAD_KEYS` **3×** in `services/audit_service.py` on `dev`, live inside `log_action` — so a new audit writer inherits redaction at the chokepoint (`SPEC_AUDIT_TRAIL` §7). Still absent: `erase_user`, `delete_org`, email change. ([#655](https://github.com/datanika-io/datanika-core/issues/655)) |
| [`SPEC_SIGNUP_ENUMERATION.md`](SPEC_SIGNUP_ENUMERATION.md) | Bounding the `/signup` account-existence oracle by reusing the shipped `RateLimitService` pattern. The follow-through on `SPEC_PASSWORD_RESET` D7, which made reset opaque and said so. ⚠️ **A bound is not opacity** — targeted single-address enumeration stays open by decision. | Spec only ([#639](https://github.com/datanika-io/datanika-core/issues/639)) |
| [`SPEC_MUTATION_FEEDBACK.md`](SPEC_MUTATION_FEEDBACK.md) | A successful create must say so. The constructive mirror of the confirmation work in #804/#851: **10 destructive DB handlers, 9 acknowledge; 10 constructive DB handlers, 0 do.** Includes the tri-state loading that makes an honest empty table readable, and the D7 addendum that gave `leave_org`, `cancel_invitation` and `transfer_ownership` their confirmations. | 🔄 **Merged to `dev`** across PRs #960 (acknowledge every create), #1006 (tri-state loading) and #1023 (re-entrant save refused server-side); **not yet in production.** *(Was "Spec only" until 2026-09-07.)* ([#872](https://github.com/datanika-io/datanika-core/issues/872)) |
| [`SPEC_PASSWORD_RESET.md`](SPEC_PASSWORD_RESET.md) | Password change and account recovery — token shape, the non-consuming GET, what the copy may and may not claim about sessions. | Part B shipped ([#623](https://github.com/datanika-io/datanika-core/issues/623)) |
| [`SPEC_ORG_ROLES.md`](SPEC_ORG_ROLES.md) | The org permission model: who may change whose role, owner transfer, and why nobody may strand the last owner. | Decided, not built ([#658](https://github.com/datanika-io/datanika-core/issues/658)) |
| [`SPEC_RUN_CANCELLATION.md`](SPEC_RUN_CANCELLATION.md) | What cancelling a run promises — best-effort stop, partial data left in place, billing to the stop point — and the seven hand-maintained status lists that a new `cancelling` state has to reach. | Decided, not built ([#657](https://github.com/datanika-io/datanika-core/issues/657)) |
| [`SPEC_SIGNUP_SOCIAL_AUTH.md`](SPEC_SIGNUP_SOCIAL_AUTH.md) | Social auth on `/signup`, and the context (`template`, `invite_token`, `next`) that the OAuth path currently drops. | Spec only ([#624](https://github.com/datanika-io/datanika-core/issues/624)) |
| [`SPEC_MONGODB_TLS_SRV.md`](SPEC_MONGODB_TLS_SRV.md) | `tls` + SRV on the MongoDB connection form — the first dependent field pair in the connection form. | Spec only ([#626](https://github.com/datanika-io/datanika-core/issues/626)) |
| [`SPEC_NOTIFICATION_DELIVERY.md`](SPEC_NOTIFICATION_DELIVERY.md) | What an alerting channel must record and show. 🚨 **Six independent silences, not one** — wiring the missing `email_service` argument closes exactly one of them, and three of the six affect Slack/Telegram/webhook too. | Spec only ([#652](https://github.com/datanika-io/datanika-core/issues/652)) |
| [`SPEC_DUAL_MODE_UX.md`](SPEC_DUAL_MODE_UX.md) | The ETL/ELT mode selector, cost estimator, and dual-dimension volume-billing UX. | Shipped behind `datanika_dual_mode_ux_enabled` (default off) |
| [`SPEC_PIPELINE_TEMPLATES_DEPTH.md`](SPEC_PIPELINE_TEMPLATES_DEPTH.md) | How far the curated template catalog should go, and the measurement that decides it. | Deferred ([#735](https://github.com/datanika-io/datanika-core/issues/735)) |
| [`SPEC_NOTIFICATION_CENTER_API.md`](SPEC_NOTIFICATION_CENTER_API.md) | The in-app notification service interface and its five REST routes. | Shipped |
| [`SPEC_CONTEXTUAL_TOOLTIPS.md`](SPEC_CONTEXTUAL_TOOLTIPS.md) | The onboarding tooltip component and where it may appear. | Shipped |
| [`SPEC_WAVE1_CONNECTOR_FIELDS.md`](SPEC_WAVE1_CONNECTOR_FIELDS.md) | Config fields for the Wave-1 connectors, plus a "Shipped reality" section recording where the implementation diverged. | Shipped |
| [`SPEC_SOC2_ROADMAP.md`](SPEC_SOC2_ROADMAP.md) | The SOC 2 Type I readiness programme and its control inventory. ⚠️ **Its dates have expired and the public claim was withdrawn 2026-08-30** — the programme is parked, not cancelled, and nothing on datanika.io may state a status or a quarter for it. | Parked |

---

Sixteen of these moved here on 2026-08-31 from a local planning directory outside any git repository,
under [`SPEC_PLANS_CONSOLIDATION`](https://github.com/datanika-io/datanika-core/issues/724) —
Engineering's and Infra's first, Product's with [#734](https://github.com/datanika-io/datanika-core/issues/734).

**Where the cross-references went.** Moving a file changes what its relative links mean, and a broken
markdown link fails silently, so every one was resolved against the filesystem after the move. Three
kinds of target could not stay links:

- **Documents that deliberately did not move.** `plans/WORKFLOW_RULES.md` and everything under
  `plans/security/` stay outside git — the second on purpose, because this repository is public. They
  appear as plain paths, never as links.
- **The pricing-pivot root document**, which sets the commercial context for the ELT/IR work and the
  per-GB cost targets it cites. The published outcome of that pivot is on
  [datanika.io/pricing](https://datanika.io/pricing).
- **Specs that moved to a *different* repository.** [`SPEC_VOLUME_METERING.md`](https://github.com/datanika-io/datanika-cloud/blob/dev/docs/specs/SPEC_VOLUME_METERING.md)
  and [`SPEC_GB_THROUGHPUT_METRICS.md`](https://github.com/datanika-io/datanika-cloud/blob/dev/docs/specs/SPEC_GB_THROUGHPUT_METRICS.md)
  are the billing interpretation of the bytes the IR layer emits, so they govern the `datanika-cloud`
  plugin and live in that private repository. Nothing in them is required to read the specs here:
  core's side of that contract is one integer, `bytes_processed`, emitted on the `run.*_completed`
  hooks. [`SPEC_BILLING_SELF_SERVICE.md`](https://github.com/datanika-io/datanika-cloud/blob/dev/docs/specs/SPEC_BILLING_SELF_SERVICE.md)
  is Product's and went the same way, for the same reason.
