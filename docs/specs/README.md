# Engineering specs

Design contracts for work that spans more than one session or more than one department. A spec is
read before the code is written and amended when the design changes — it is the contract, not a
description of what shipped. What shipped is described in [`DESIGN.md`](../../DESIGN.md) and in the
code.

| Spec | Governs | Status |
|---|---|---|
| [`SPEC_ELT_IR_ARCHITECTURE.md`](SPEC_ELT_IR_ARCHITECTURE.md) | The intermediate representation between every source and destination, and the ETL/ELT mode dispatch built on it. P1–P4 shipped; the file-source IR builder is not built. | Partially implemented |
| [`SPEC_OPENAPI_CONNECTOR.md`](SPEC_OPENAPI_CONNECTOR.md) | The parametric `openapi` connection type — an OpenAPI 3.x document in, a working `rest_api` source out. | Spec only, no code ([#310](https://github.com/datanika-io/datanika-core/issues/310)) |
| [`SPEC_REMOTE_MCP.md`](SPEC_REMOTE_MCP.md) | The hosted Streamable-HTTP MCP endpoint at `/mcp` and its OAuth 2.1 authorization server. | P1 + P2 shipped |
| [`SPEC_EXPAND_CONTRACT_MIGRATIONS.md`](SPEC_EXPAND_CONTRACT_MIGRATIONS.md) | Which schema changes a migration may make given that blue/green runs the **previously deployed code against the new schema**. Written by Infra, binds Engineering. | Policy, in force |
| [`SPEC_RELEASE_VERSIONING.md`](SPEC_RELEASE_VERSIONING.md) | The `0.x` SemVer scheme, the `v*` tag contract, and what a release is and is not. Written by Infra, binds whoever cuts a tag. | Policy, in force |

The first three are owned by Engineering; the last two are Infra-authored policy that constrains
engineering work, which is why they sit in the same index rather than in a separate one.

All five moved here on 2026-08-31 from a local planning directory outside any git repository.
The three Engineering specs previously cross-referenced two documents that did **not** move and are
not public:

- **The pricing-pivot root document**, which sets the commercial context for the ELT/IR work (the
  per-GB cost targets it cites). The published outcome of that pivot is on
  [datanika.io/pricing](https://datanika.io/pricing).
- **`SPEC_VOLUME_METERING.md`**, the billing interpretation of the bytes the IR layer emits. It
  governs the `datanika-cloud` plugin and therefore lives in that repository, which is private.
  Nothing in it is required to read the specs here: core's side of that contract is one integer,
  `bytes_processed`, emitted on the `run.*_completed` hooks.
