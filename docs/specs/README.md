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

> 🚨 **A status cell is a claim with an expiry date.** Three were stale on 2026-09-07, one of them
> stating a measured zero that had become a five, and two specs on disk were missing from this table
> entirely, which is worse: an unindexed spec is a contract nobody finds. **On 2026-09-17 at least
> eleven of the 24 cells below were false** ([#1455](https://github.com/datanika-io/datanika-core/issues/1455)). Ten called work *spec only*, *not built*
> or *not yet in production* when it was on `master`, and one named two functions as absent that exist.
>
> **Why they go stale:** a promotion, or another department's PR, makes a cell false without touching
> this file. So a cell records a **dated reading** (*"on `master` by content, read 2026-09-17"*,
> *"closed 2026-09-08"*) and links the issue for whatever is still open. A present-tense status such
> as *"not yet in production"* stops being true when a promotion lands, and nothing edits this file
> when one does. **Re-derive a cell before relying on it.** Issues close on production evidence, not
> on a merge to `dev`.
>
> ⚠️ **`SPEC_VOLUME_METERING`, `SPEC_GB_THROUGHPUT_METRICS` and `SPEC_BILLING_SELF_SERVICE` are
> named below and are deliberately NOT in this repo** (see the footer). A completeness check that
> diffs "files on disk" against "names in this file" reports those three as dangling; they are not.

| Spec | Governs | Status |
|---|---|---|
| [`SPEC_SERVICE_AUTHORIZATION.md`](SPEC_SERVICE_AUTHORIZATION.md) | The rule `SPEC_ORG_ROLES` §4 already decided, applied to the other eight subsystems. **31 handlers declare a role; 25 call a service that enforces nothing** — and the *"there is no other surface"* argument is false: **26 mutating REST endpoints** already cover exactly those eight, authorized by **scope with no role at all**. Derives one rule plus three exception families rather than 25 judgements. | Implementation on `master`: PRs #1234 (the mechanism), #1246, #1254, #1425 and #1427, by content, read 2026-09-17. Completeness: [#681](https://github.com/datanika-io/datanika-core/issues/681). *(Said "Spec only" until 2026-09-17.)* |
| [`SPEC_LOCALE_REACHABILITY.md`](SPEC_LOCALE_REACHABILITY.md) | Nine locales, reachable where they matter and consistent within a screen. The switcher is **sidebar-only**, so **464 translated strings on the four pre-auth screens cannot be reached** by the people they were written for — and the locale persists nowhere, so a switcher alone would forget the choice by the time the emailed reset link is opened. Also decides Serbian = **Cyrillic** and Spanish = **tú**, and records why a key-parity test is structurally blind to both. | Spec only ([#696](https://github.com/datanika-io/datanika-core/issues/696), [#695](https://github.com/datanika-io/datanika-core/issues/695)) |
| [`SPEC_EARNED_VERDICTS.md`](SPEC_EARNED_VERDICTS.md) | The product may not report an outcome it did not measure. The contract behind the two fixes for [#821](https://github.com/datanika-io/datanika-core/issues/821) (a green Test Connection that made no request) and [#823](https://github.com/datanika-io/datanika-core/issues/823) (a green run that loaded one page of fifteen): what "not tested" must look like on every surface, and what a load may claim about its own completeness. 🚨 **§0 records that both defects are already fixed and live** — read it before working the issue bodies. **§4.6 (2026-09-16):** `/models` must name an upload whose tables the catalogue could not read, not only when the whole catalogue is empty; **corrected 2026-09-17**, because one of its sentences claimed data the verdict does not record. **§4.7 (2026-09-17):** a SQL-source run that finds nothing it was configured to read fails before loading, as a file source already does. | §3.2–§4.5 shipped (each section says so). §4.6 in production since `master` `da634df5` (PR #1436, deploy run 35228187079 green). Its wording correction is PR #1446, merged to `dev` on 2026-09-17 and not on `master` when read that day. §4.7: implementation tracked on [#1445](https://github.com/datanika-io/datanika-core/issues/1445). ([#821](https://github.com/datanika-io/datanika-core/issues/821), [#823](https://github.com/datanika-io/datanika-core/issues/823), [#1398](https://github.com/datanika-io/datanika-core/issues/1398)) |
| [`SPEC_INCREMENTAL_UPLOADS.md`](SPEC_INCREMENTAL_UPLOADS.md) | An upload with an incremental cursor resumes from where the last successful run stopped. Decided on QA's measurement that **every run restarts the cursor** (under `append`, 13 rows for 8 ids after two runs), against the alternative of no longer calling the mode incremental. Defines the cursor's key (what restarts it), that a failed run does not move it, and what overlapping runs may do. | Spec only ([#1404](https://github.com/datanika-io/datanika-core/issues/1404)) |
| [`SPEC_AUDIT_TRAIL.md`](SPEC_AUDIT_TRAIL.md) | What a mutating handler owes the audit record: one transaction, a valid action, a filterable resource type, a payload a human can read. 🚨 **§4 carries three test-design clauses that were falsified by measurement, two of which went red on correct code** — read the corrections before writing the tests. §6 is ruled *not resolved* while the fixes sit on `dev`. | Implementation on `master`: PRs #1144, #1146 and #1250, by content, read 2026-09-17. [#934](https://github.com/datanika-io/datanika-core/issues/934), [#1127](https://github.com/datanika-io/datanika-core/issues/1127) and [#1128](https://github.com/datanika-io/datanika-core/issues/1128) closed 2026-09-07. *(Said "not yet in production" until 2026-09-17.)* |
| [`SPEC_LOCAL_FILE_CONNECTIONS.md`](SPEC_LOCAL_FILE_CONNECTIONS.md) | What Test Connection *means* on a DuckDB or SQLite file source — the question the two filed defects did not contain. Verified by calling `ConnectionService.test_connection` directly, with controls in both directions. | Contract ([#978](https://github.com/datanika-io/datanika-core/issues/978), [#979](https://github.com/datanika-io/datanika-core/issues/979)) |
| [`SPEC_PAGE_ENTRY.md`](SPEC_PAGE_ENTRY.md) | What happens when a browser arrives at a URL: who may enter, what runs on entry, and what is on screen while it runs. Covers the `/signup` **session substitution** (a signed-in user who submits the form is silently re-identified), the sign-in/sign-up affordance inversion, the loading state, and the unguarded dashboard loader. 🚨 **§0 retracts four claims I made on the issues themselves** — including *"blank screen"* (a spinner is already there) and *"17 protected pages"* (it is 14). Read §0 before the ACs. | Implementation on `master`: PRs #1118, #1121 and #1142, by content, read 2026-09-17. [#1081](https://github.com/datanika-io/datanika-core/issues/1081), [#1090](https://github.com/datanika-io/datanika-core/issues/1090) and [#1097](https://github.com/datanika-io/datanika-core/issues/1097) closed 2026-09-07. *(Was "Spec only" until 2026-09-07, then "not yet in production" until 2026-09-17.)* |
| [`SPEC_PII_SEPARATION.md`](SPEC_PII_SEPARATION.md) | Personal data into `<parent>_pii` tables with a shared PK/FK; erasure, org deletion, email change. Amended against two production column censuses (§2a–§2c) and, 2026-09-02, with **§0 — the soft-delete / hard-delete split**, which surfaced two gaps where a soft delete stood in for an erasure. | 🔄 **Partly built.** On `master`, by content, read 2026-09-17: PR #942 (release N: extraction, erasure, org deletion); `erase_user` and `delete_org` in `services/user_service.py`; and `redact_pii_payload` called inside `log_action`, so a new audit writer inherits redaction at the chokepoint (`SPEC_AUDIT_TRAIL` §7). No email-change flow is on `master`: see `SPEC_EMAIL_CHANGE`. Not re-derived: whether §0's two gaps are closed. *(Said "No feature code" until 2026-09-07, and "Still absent: `erase_user`, `delete_org`" until 2026-09-17.)* ([#655](https://github.com/datanika-io/datanika-core/issues/655)) |
| [`SPEC_SIGNUP_ENUMERATION.md`](SPEC_SIGNUP_ENUMERATION.md) | Bounding the `/signup` account-existence oracle by reusing the shipped `RateLimitService` pattern. The follow-through on `SPEC_PASSWORD_RESET` D7, which made reset opaque and said so. ⚠️ **A bound is not opacity** — targeted single-address enumeration stays open by decision. | Implementation on `master`: PR #956, by content, read 2026-09-17. [#639](https://github.com/datanika-io/datanika-core/issues/639) closed 2026-09-08. *(Said "Spec only" until 2026-09-17.)* |
| [`SPEC_MUTATION_FEEDBACK.md`](SPEC_MUTATION_FEEDBACK.md) | A successful create must say so. The constructive mirror of the confirmation work in #804/#851: **10 destructive DB handlers, 9 acknowledge; 10 constructive DB handlers, 0 do.** Includes the tri-state loading that makes an honest empty table readable, and the D7 addendum that gave `leave_org`, `cancel_invitation` and `transfer_ownership` their confirmations. | Implementation on `master`: PRs #960 (acknowledge every create), #1006 (tri-state loading) and #1023 (re-entrant save refused server-side), by content, read 2026-09-17. [#872](https://github.com/datanika-io/datanika-core/issues/872) closed 2026-09-07. *(Was "Spec only" until 2026-09-07, then "not yet in production" until 2026-09-17.)* |
| [`SPEC_PASSWORD_RESET.md`](SPEC_PASSWORD_RESET.md) | Password change and account recovery — token shape, the non-consuming GET, what the copy may and may not claim about sessions. | Part B shipped ([#623](https://github.com/datanika-io/datanika-core/issues/623)) |
| [`SPEC_ORG_ROLES.md`](SPEC_ORG_ROLES.md) | The org permission model: who may change whose role, owner transfer, and why nobody may strand the last owner. | [#658](https://github.com/datanika-io/datanika-core/issues/658), the defect behind it, closed 2026-08-31, and `UserService.transfer_ownership` is on `master` (read 2026-09-17). Not re-derived: whether every rule in the spec is enforced. *(Said "Decided, not built" until 2026-09-17.)* |
| [`SPEC_RUN_CANCELLATION.md`](SPEC_RUN_CANCELLATION.md) | What cancelling a run promises — best-effort stop, partial data left in place, billing to the stop point — and the seven hand-maintained status lists that a new `cancelling` state has to reach. | Implementation on `master`: PRs #1212, #1217 and #1411, by content, read 2026-09-17. Completeness: [#657](https://github.com/datanika-io/datanika-core/issues/657). *(Said "Decided, not built" until 2026-09-17.)* |
| [`SPEC_SIGNUP_SOCIAL_AUTH.md`](SPEC_SIGNUP_SOCIAL_AUTH.md) | Social auth on `/signup`, and the context (`template`, `invite_token`, `next`) that the OAuth path currently drops. | Implementation on `master`: PR #1339, by content, read 2026-09-17. Completeness: [#624](https://github.com/datanika-io/datanika-core/issues/624). *(Said "Spec only" until 2026-09-17.)* |
| [`SPEC_MONGODB_TLS_SRV.md`](SPEC_MONGODB_TLS_SRV.md) | `tls` + SRV on the MongoDB connection form — the first dependent field pair in the connection form. | Spec only ([#626](https://github.com/datanika-io/datanika-core/issues/626)) |
| [`SPEC_NOTIFICATION_DELIVERY.md`](SPEC_NOTIFICATION_DELIVERY.md) | What an alerting channel must record and show. 🚨 **Six independent silences, not one** — wiring the missing `email_service` argument closes exactly one of them, and three of the six affect Slack/Telegram/webhook too. | Implementation on `master`: PR #971, by content, read 2026-09-17. [#652](https://github.com/datanika-io/datanika-core/issues/652) closed 2026-09-08. *(Said "Spec only" until 2026-09-17.)* |
| [`SPEC_DUAL_MODE_UX.md`](SPEC_DUAL_MODE_UX.md) | The ETL/ELT mode selector, cost estimator, and dual-dimension volume-billing UX. | Shipped behind `datanika_dual_mode_ux_enabled` (default off) |
| [`SPEC_PIPELINE_TEMPLATES_DEPTH.md`](SPEC_PIPELINE_TEMPLATES_DEPTH.md) | How far the curated template catalog should go, and the measurement that decides it. | Deferred ([#735](https://github.com/datanika-io/datanika-core/issues/735)) |
| [`SPEC_NOTIFICATION_CENTER_API.md`](SPEC_NOTIFICATION_CENTER_API.md) | The in-app notification service interface and its five REST routes. | Shipped |
| [`SPEC_CONTEXTUAL_TOOLTIPS.md`](SPEC_CONTEXTUAL_TOOLTIPS.md) | The onboarding tooltip component and where it may appear. | Shipped |
| [`SPEC_WAVE1_CONNECTOR_FIELDS.md`](SPEC_WAVE1_CONNECTOR_FIELDS.md) | Config fields for the Wave-1 connectors, plus a "Shipped reality" section recording where the implementation diverged. | Shipped |
| [`SPEC_SOC2_ROADMAP.md`](SPEC_SOC2_ROADMAP.md) | The SOC 2 Type I readiness programme and its control inventory. ⚠️ **Its dates have expired and the public claim was withdrawn 2026-08-30** — the programme is parked, not cancelled, and nothing on datanika.io may state a status or a quarter for it. | Parked |
| [`SPEC_EMAIL_CHANGE.md`](SPEC_EMAIL_CHANGE.md) | Changing your email address — [core#655] AC4. Needs no migration; §3 carries the invariant that keeps [core#700]'s resend control from becoming an open relay, and §3a records why that is a design constraint rather than a disclosure. | Contract |
| [`SPEC_FIELD_REQUIREDNESS.md`](SPEC_FIELD_REQUIREDNESS.md) | How a form says a field is required — the convention [core#1311] is blocked on. Markers are derived, never authored into translated strings. | Contract |
| [`SPEC_BUTTON_CONTRAST.md`](SPEC_BUTTON_CONTRAST.md) | The accent is `violet`, and `variant="solid"` is reserved for scales that clear 4.5:1 at step 9. Decides the `color-contrast` class of [#1409](https://github.com/datanika-io/datanika-core/issues/1409), the last of six blocking [#720](https://github.com/datanika-io/datanika-core/issues/720). ⚠️ **Reflex's `rx.App` default is `accent_color="blue"`** — blue was never an absence. 🚨 §5: *"just make it soft"* creates **new** failures on amber/yellow/green/blue/orange; §10: `var(--gray-9)` is a text colour at 29 sites at 3.32:1, and dialog copy is not in the DOM so axe never scores it. | Contract, decided 2026-09-23. Not implemented: no theme is passed to `rx.App` on `dev`, read 2026-09-23 ([#1409](https://github.com/datanika-io/datanika-core/issues/1409)) |

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
