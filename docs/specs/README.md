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

| Spec | Governs | Status |
|---|---|---|
| [`SPEC_PII_SEPARATION.md`](SPEC_PII_SEPARATION.md) | Personal data into `<parent>_pii` tables with a shared PK/FK; erasure, org deletion, email change. **Live and being implemented** — §2c is amended against a production column census. | In flight ([#655](https://github.com/datanika-io/datanika-core/issues/655)) |
| [`SPEC_PASSWORD_RESET.md`](SPEC_PASSWORD_RESET.md) | Password change and account recovery — token shape, the non-consuming GET, what the copy may and may not claim about sessions. | Part B shipped ([#623](https://github.com/datanika-io/datanika-core/issues/623)) |
| [`SPEC_ORG_ROLES.md`](SPEC_ORG_ROLES.md) | The org permission model: who may change whose role, owner transfer, and why nobody may strand the last owner. | Decided, not built ([#658](https://github.com/datanika-io/datanika-core/issues/658)) |
| [`SPEC_SIGNUP_SOCIAL_AUTH.md`](SPEC_SIGNUP_SOCIAL_AUTH.md) | Social auth on `/signup`, and the context (`template`, `invite_token`, `next`) that the OAuth path currently drops. | Spec only ([#624](https://github.com/datanika-io/datanika-core/issues/624)) |
| [`SPEC_MONGODB_TLS_SRV.md`](SPEC_MONGODB_TLS_SRV.md) | `tls` + SRV on the MongoDB connection form — the first dependent field pair in the connection form. | Spec only ([#626](https://github.com/datanika-io/datanika-core/issues/626)) |
| [`SPEC_DUAL_MODE_UX.md`](SPEC_DUAL_MODE_UX.md) | The ETL/ELT mode selector, cost estimator, and dual-dimension volume-billing UX. | Shipped behind `datanika_dual_mode_ux_enabled` (default off) |
| [`SPEC_PIPELINE_TEMPLATES_DEPTH.md`](SPEC_PIPELINE_TEMPLATES_DEPTH.md) | How far the curated template catalog should go, and the measurement that decides it. | Deferred ([#735](https://github.com/datanika-io/datanika-core/issues/735)) |
| [`SPEC_NOTIFICATION_CENTER_API.md`](SPEC_NOTIFICATION_CENTER_API.md) | The in-app notification service interface and its five REST routes. | Shipped |
| [`SPEC_CONTEXTUAL_TOOLTIPS.md`](SPEC_CONTEXTUAL_TOOLTIPS.md) | The onboarding tooltip component and where it may appear. | Shipped |
| [`SPEC_WAVE1_CONNECTOR_FIELDS.md`](SPEC_WAVE1_CONNECTOR_FIELDS.md) | Config fields for the Wave-1 connectors, plus a "Shipped reality" section recording where the implementation diverged. | Shipped |
| [`SPEC_SOC2_ROADMAP.md`](SPEC_SOC2_ROADMAP.md) | The SOC 2 Type I readiness programme and its control inventory. ⚠️ **Its dates have expired and the public claim was withdrawn 2026-08-30** — the programme is parked, not cancelled, and nothing on datanika.io may state a status or a quarter for it. | Parked |

---

All sixteen moved here on 2026-08-31 from a local planning directory outside any git repository,
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
