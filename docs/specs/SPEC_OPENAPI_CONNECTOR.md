# SPEC — OpenAPI Source-Generator Connector

> **Provenance.** Migrated on 2026-08-31 from the local planning directory
> (`plans/engineering/`), which is outside every git repository and therefore has no history,
> no review and no recovery path. A spec is a contract amended across sessions, so it belongs
> in the repository it governs. Content is unchanged apart from link paths; a few links still
> point at internal planning documents that are not part of this repository.


> Status: **draft, spec-first** — no code until the user signs off (per [core#310](https://github.com/datanika-io/datanika-core/issues/310)). Implementation is a follow-on issue gated on this doc.
> Owner: Engineering. Cross-dep: Product (wizard UI), QA/Infra (live smoke, egress policy).
> Related: [SPEC_ELT_IR_ARCHITECTURE.md](SPEC_ELT_IR_ARCHITECTURE.md), wave-1 connectors ([core#309](https://github.com/datanika-io/datanika-core/pull/323)).

---

## 1. TL;DR

Add one new connection type, **`openapi`** — a *parametric* connector. Instead of us hand-coding a source per SaaS (the wave-1 pattern: 4 PRs for 4 APIs), the user hands Datanika an **OpenAPI 3.x spec** (paste or URL) and we derive a working `rest_api` connector from it: base URL, auth scheme, the list of readable endpoints, and — from the spec's declared response schemas — the columns each endpoint yields.

The insight that makes this cheap: **the runtime already exists.** `DltRunnerService._build_rest_api_source` ([dlt_runner.py:324](../../datanika/services/dlt_runner.py#L324)) already turns a `{client, resources}` config into a live `rest_api_source`. An OpenAPI connection is just a **spec-derived `rest_api` config**. So the entire feature is (a) a spec → config translator, (b) a small wizard/parse endpoint, and (c) IR columns from the spec's response schemas. One `ConnectionType`, infinite catalog.

## 2. Goals

- **G1.** Turn any valid OpenAPI 3.0/3.1 spec into a usable read connector with **zero per-API code**.
- **G2.** Auto-derive: `base_url` (from `servers`), auth scheme (from `securitySchemes`), the readable-resource list (GET collection endpoints), and per-resource **columns from the spec's response schema** (no sample fetch needed).
- **G3.** Reuse the existing `rest_api` runtime — an `openapi` source is built through the same `rest_api_source(...)` path, not a new engine.
- **G4.** Agent-native: expose a stateless `POST /api/v1/connections/openapi/parse` so an AI agent (or the UI) can preview the derived connector shape before creating a connection. This is the "connect to any documented API" story the VC memo wants made real.
- **G5.** Auto-propagate the new type through the existing machinery: `CONFIG_SCHEMAS["openapi"]` → OpenAPI spec + agent-tiers via [`openapi_inline.py`](../../datanika/services/openapi_inline.py); picker; IR sets — exactly like every other connector.

## 3. Non-goals

- Not GraphQL / SOAP / gRPC / AsyncAPI (REST + OpenAPI only).
- Not **write-back** to the API (read/extract only, consistent with every Datanika source).
- Not OpenAPI **2.0 (Swagger)** in P1 — user converts externally (or a P2 shim). 3.0.x + 3.1.x only.
- Not an interactive **OAuth2 authorization-code** flow in P1 (no browser redirect dance). Bearer / API-key / HTTP-basic / static token only; OAuth2 *client-credentials* deferred to P3.
- Not auto-refreshing specs / detecting upstream API drift automatically (manual re-parse; see §9 P3).
- Not a general HTTP scraper — the spec is the contract; endpoints not described by the spec are not reachable.

## 4. Background — what we build on

Three existing pieces do most of the work:

**4.1 The generic `rest_api` connector (the runtime target).** [`_build_rest_api_source`](../../datanika/services/dlt_runner.py#L324) reads `config.base_url` / `config.headers` / `config.auth` and `dlt_config.resources` / `paginator` / `resource_defaults`, then calls dlt's `rest_api_source({"client": {...}, "resources": [...]})`. Today the user must **hand-write the `resources` list** — that's the pain OpenAPI removes. The output of our translator is exactly this config shape, so we inherit pagination, auth, retries, and incremental support from dlt's `rest_api` for free.

**4.2 `openapi_inline.py` (the schema-translation base).** It walks our SoT dicts (`CONFIG_SCHEMAS`, `DLT_CONFIG_SCHEMA`) and **emits** OpenAPI 3.0.3 component schemas (config schema, create-body, `oneOf` + discriminator) so the spec is fully typed. The OpenAPI connector is the **reverse direction**: **consume** an external OpenAPI spec into our config. Both directions need the same primitives — `$ref` resolution, `components/schemas` walking, JSON-Schema → column-type mapping. We factor those into a shared `openapi_schema.py` so there is one implementation, and the connector's own `CONFIG_SCHEMAS["openapi"]` entry propagates to our spec through `openapi_inline` with no extra work.

**4.3 `dlt-init-openapi` (prior-art heuristics).** dlt ships a CLI (`dlt-init-openapi`) that scaffolds a `rest_api` source from a spec, with heuristics for pagination, primary keys, parent-child relationships, and response data-selectors. It emits a **Python package**, not a config dict — so we cannot drop it in as a runtime library. We treat it as a **heuristics reference** (see §5.4): P1 ships an in-house parser for the common cases; P2+ borrows/vendors its detection logic for pagination and relationships.

**4.4 Wave-1 connectors** ([core#309](https://github.com/datanika-io/datanika-core/pull/323)) proved the per-connector wiring surface (enum, `CONFIG_SCHEMAS`, IR sets, UI fields, picker). The OpenAPI connector touches the **same** surface but only **once** — the per-API variance moves out of code and into the stored config.

## 5. Proposed design

### 5.1 One parametric connection type

Add `ConnectionType.OPENAPI = "openapi"`. Unlike wave-1's fixed types, a single `openapi` type serves every API; the specificity lives in `Connection.config`. Direction: **source only** (add to `SOURCE_TYPES` + `_NON_DB_TYPES`; not a destination; IR: a new spec-schema branch, see §5.7).

### 5.2 Config contract — the connection *is* its derived config

`Connection.config` (Fernet-encrypted at rest, like all configs):

```jsonc
{
  "spec_source": { "kind": "inline" | "url", "value": "<raw spec text | https URL>" },
  "base_url": "https://api.example.com/v1/",     // from servers[0].url; user-overridable
  "auth": { "type": "bearer", "token": "..." },  // shape mirrors dlt rest_api auth (bearer|api_key|http_basic)
  "headers": { "X-Api-Version": "2024-01-01" },  // optional, static headers
  "paginator": { "type": "offset", "limit": 100, ... },  // optional; spec-derived or user default
  "resources": [                                  // the DERIVED catalog (what the API offers)
    {
      "name": "customers",
      "endpoint": { "path": "customers", "method": "GET", "data_selector": "data" },
      "primary_key": "id",
      "columns": [ { "name": "id", "type": "text", "nullable": false }, ... ],  // from response schema
      "_source": { "operation_id": "listCustomers", "summary": "List all customers" }
    }
  ]
}
```

- **`resources`** is the full **catalog** derived from the spec (analogous to the tables a SQL connection exposes). It carries both the dlt-`rest_api` fields (`name`, `endpoint`, `primary_key`) *and* the derived `columns` (for IR, §5.7) *and* provenance (`_source`).
- **`Upload.dlt_config`** then **selects** a subset per load: `{ "resource_names": ["customers", "invoices"], "incremental": {...}, "mode": "full_database" }`. This mirrors the SQL "connection exposes tables → upload picks tables" model and keeps the connector reusable across many uploads.
- At runtime, `columns`/`_source` are stripped before handing `resources` to dlt (they are ours, not rest_api's) — see §5.5.

### 5.3 Spec ingestion + translation — `openapi_import.py`

New module `datanika/services/openapi_import.py`. Pure, dependency-light, deterministic. Entry point:

```python
def parse_openapi_spec(raw: str | dict, *, base_url_override: str | None = None) -> ParsedConnector
```

Steps:
1. **Load + validate** — JSON or YAML → dict. Assert `openapi` starts with `3.`. Enforce size/complexity limits (§7) before deep-walking.
2. **base_url** — `servers[0].url`, resolving relative/templated server URLs; `base_url_override` wins. If `servers` is absent, require the user to supply it.
3. **auth** — read `components.securitySchemes` + top-level `security`. Map: `http`+`bearer` → `{"type":"bearer"}`; `http`+`basic` → `{"type":"http_basic"}`; `apiKey` (header/query) → `{"type":"api_key","name":...,"location":...}`; `oauth2` client-credentials → deferred (P3), surfaced as "unsupported, supply a static token". Credential *values* are never in the spec — the wizard/agent fills them.
4. **resources** — for each `paths[path]` with a **GET** operation that returns a collection (heuristic: `200` response schema is an array, or an object whose primary property is an array — the data-selector), emit a resource: `name` from `operationId`/path tail, `endpoint.path` (relative to base_url), `data_selector`, `primary_key` (from an `id`-like required field or spec `x-primary-key`), and `columns` from the response item schema (§5.7). Path-templated endpoints (`/users/{id}`) are **detail** endpoints — excluded from the P1 catalog (they need a parent; P3 relationships).
5. **paginator** — P1: leave unset (dlt `rest_api` auto-detects common paginators at runtime) or apply a single spec-hinted default if `limit`/`offset`/`page` query params are declared. P2: full detection (§9).
6. Return `ParsedConnector(base_url, auth_schemes, resources, warnings)` — `warnings` lists skipped/ambiguous endpoints so the UI/agent can show what was and wasn't imported (no silent truncation, per workflow rules).

### 5.4 dlt-init-openapi vs in-house parser — decision

| | In-house `openapi_import.py` (P1) | `dlt-init-openapi` library |
|---|---|---|
| Output | config **dict** in our shape (what we need) | generates a **.py package** (codegen) |
| Dep weight | none (stdlib + existing yaml) | heavy, and API is CLI-oriented / not a stable lib |
| Heuristics | we own; start simple | best-in-class pagination/relationship detection |
| Control/security | full (limits, allowlists) | opaque |

**Recommendation:** **in-house parser for P1** (covers the ~80% case: GET-collection endpoints, bearer/api-key/basic auth, spec-declared response schemas). For **P2+ pagination and P3 relationships**, *vendor the specific heuristics* from `dlt-init-openapi` (it's OSS) into `openapi_import.py` rather than taking a runtime dep — keeps the "no forks, no heavy deps" boundary from [SPEC_ELT_IR_ARCHITECTURE §dlt separation]. Revisit if dlt ships a stable programmatic `spec → rest_api config` API.

### 5.5 Runtime — reuse `rest_api`, add a thin adapter

New `DltRunnerService._build_openapi_source(config, dlt_config, batch_size)`:
1. `base_url = config["base_url"]`, `auth = config.get("auth")`, `headers`, `paginator`.
2. `catalog = config["resources"]`; `selected = dlt_config.get("resource_names")`; filter catalog to `selected` (or all).
3. **Strip our private keys** (`columns`, `_source`) from each resource → leave clean dlt-`rest_api` resource dicts.
4. Delegate: assemble `{"client": {...}, "resources": <cleaned>}` and call the **same** `rest_api_source(...)` path (factor the tail of `_build_rest_api_source` into a shared `_rest_api_from_parts(...)` so both connectors share it).

Wire into `build_source` dispatch ([dlt_runner.py:214](../../datanika/services/dlt_runner.py#L214)) with `SUPPORTED_OPENAPI_TYPES = {"openapi"}` before the SQL fallthrough. Add `"openapi"` to `INTERNAL_CONFIG_KEYS` handling for `resource_names`. **No new destination.**

### 5.6 Relationship to `openapi_inline.py` (the named base)

- **Shared utility** `datanika/services/openapi_schema.py`: `resolve_ref(spec, ref)`, `walk_schema(...)`, `json_schema_to_column_type(...)`. `openapi_inline` (emit) and `openapi_import` (consume) both import it → one JSON-Schema/`$ref` implementation, tested once.
- **Free propagation**: `CONFIG_SCHEMAS["openapi"]` (the connector's *own* config: spec_source, base_url, auth, etc.) flows into our published spec via `openapi_inline.build_connection_inlined_schemas()` unchanged — the `openapi` type shows up in `ConnectionCreate`'s `oneOf` like any other.

### 5.7 IR integration — columns from the spec, not a sample

This is the differentiator. Other SaaS sources need `resource.compute_table_schema()` (may require a sample fetch); an **OpenAPI spec already declares its response shapes**. So:
- `build_ir` ([ir/builder.py](../../datanika/services/ir/builder.py)) gets a new branch: `source_type == "openapi"` → read the connection's stored `resources[*].columns` (produced in §5.3 step 4 by mapping the response item schema through `openapi_schema.json_schema_to_column_type`) and emit `IRColumn`s directly. **No live call, richer types than a JSON sample.**
- Add `"openapi"` to a dedicated set (not plain `SAAS_TYPES`, since it bypasses `_build_ir_saas`'s dlt-instantiation path). Fall back to `compute_table_schema()` only if a resource lacks a declared schema.

### 5.8 Proposed module layout

```
datanika/services/
  openapi_schema.py     # NEW — shared $ref/type-mapping utils (used by inline + import)
  openapi_import.py     # NEW — parse_openapi_spec(): spec → ParsedConnector (base_url/auth/resources+columns)
  openapi_inline.py     # (existing) — now imports openapi_schema
  dlt_runner.py         # + _build_openapi_source(), + _rest_api_from_parts() shared tail, dispatch + set
  connection_schemas.py # + CONFIG_SCHEMAS["openapi"]
  connection_service.py # + "openapi" in SOURCE_TYPES + _NON_DB_TYPES; test_connection = light probe or skip
  ir/builder.py         # + "openapi" spec-schema branch
  api_v1_routes.py      # + POST /connections/openapi/parse (stateless preview)
datanika/ui/…           # Product-owned wizard (contract in §6); Eng ships the parse endpoint + CONFIG_SCHEMAS
tests/…                 # fixtures: 2–3 real OpenAPI specs (petstore + a paginated + an api-key one)
```

## 6. UX flow (contract for Product)

Engineering owns the **parse endpoint + config contract**; Product owns the **wizard UI** (this is a multi-step form, unlike the flat per-type forms — a cross-dep like the Notification Center backend was).

1. User picks **"OpenAPI (any REST API)"** in the connector picker.
2. **Paste spec** (textarea) or **enter spec URL**. (P1 recommends *paste*; URL fetch is gated by §7 SSRF guards.)
3. UI calls `POST /api/v1/connections/openapi/parse` → gets `{ base_url, auth_schemes, resources: [{name, path, method, summary, column_count}], warnings }`.
4. UI renders: editable **base_url**, an **auth section** matching the detected scheme (fill token/key/basic creds), and a **checklist of endpoints** (pre-checked = collection GETs). Warnings shown inline.
5. User selects endpoints + fills creds → UI assembles `Connection.config` and calls the normal `POST /api/v1/connections` with `connection_type: "openapi"`.
6. **Test Connection**: optional light probe — call one selected endpoint with `?limit=1` (or `HEAD`) to validate auth; on non-2xx, surface a typed error. (May be skipped like other SaaS if egress policy §7 forbids arbitrary hosts from the web tier.)
7. The connection is now a normal source in Uploads/Pipelines; the upload's `dlt_config.resource_names` selects which endpoints to load.

## 7. Security — the load-bearing section

Fetching + calling **user-supplied URLs** is the whole risk surface. Non-negotiable for P1:

- **SSRF on spec fetch (URL mode).** Resolve the host; **reject** loopback/private/link-local/ULA ranges (incl. `169.254.169.254` cloud-metadata, `::1`, `10/8`, `172.16/12`, `192.168/16`, `fc00::/7`), `https` scheme only, **no redirects** to private ranges (follow-and-recheck each hop), 10 s timeout, **5 MB** size cap. Reuse/centralize with any existing outbound-fetch guard. **P1 may ship paste-only** and defer URL fetch to P2 behind these guards — decided in §13 open questions.
- **SSRF on data fetch (runtime).** `base_url` + spec paths are called from the **Celery worker** at extract time. This egress-to-arbitrary-host property is **already true of the generic `rest_api` connector** — OpenAPI inherits, doesn't invent it. Apply the same private-range block to `base_url`. Infra decides whether workers get an **egress allowlist / proxy** for third-party sources (handshake item).
- **Spec-bomb / DoS.** Caps *before* deep-walk: max spec bytes (5 MB), max `$ref` resolution depth (e.g. 50), **circular-`$ref` detection**, max resources (e.g. 300), max columns/resource. Exceed → typed 400, no partial parse.
- **Credentials.** Auth values are Fernet-encrypted like all configs; **never** taken from the spec; spec text is scrubbed of any embedded `example` secrets before storage; never logged.
- **Injection.** Paths/params are URL-encoded via dlt `rest_api`; we never `eval`/`exec` spec content (contrast with `dlt-init-openapi` codegen, which we deliberately do **not** run at runtime).

## 8. DB / storage

- **No schema migration** — `openapi` is another `ConnectionType`; the derived catalog + spec ref live in the existing `Connection.config` JSON (encrypted). Slug length ≤ 30 (`Enum(..., length=30)`) ✓.
- **Store the derived config, plus `spec_source`.** Storing `spec_source` (inline text or URL) lets the user **re-parse** later (API added endpoints) without re-pasting. For URL mode we store the URL, not a cached copy, to avoid staleness (re-fetch on explicit re-parse). Raw inline specs can be large → covered by the 5 MB cap; consider a `config` size guard.
- **Re-parse is explicit** (a button / `POST …/openapi/parse` again + update) — never automatic (avoids surprise column changes mid-pipeline). Column drift between re-parses is surfaced like a schema change.

## 9. Phased rollout

- **P1 (MVP — the follow-on impl issue).** `openapi` type end-to-end: `openapi_schema.py` + `openapi_import.py` (GET collections, `servers` base_url, bearer/api-key/basic auth, columns from response schema), `_build_openapi_source` (delegates to `rest_api`), IR from spec schema, `POST /connections/openapi/parse`, `CONFIG_SCHEMAS` + picker, paste-mode. dlt's runtime paginator auto-detection covers most pagination. **Ship gate:** petstore + one paginated + one api-key fixture parse → real `rest_api_source` builds → IR columns correct.
- **P2.** Spec-driven **pagination detection** (offset/page/cursor/link-header) + **incremental cursor** mapping (spec `x-incremental` or a datetime query param) + response **data-selector** (unwrap `{data:[…]}`/`{results:[…]}`); **URL-fetch mode** behind SSRF guards; Swagger 2.0 → 3.0 shim.
- **P3.** **Parent-child relationships** (detail endpoints resolved from a parent list, dlt `resolve`), **OAuth2 client-credentials**, POST-based search/list endpoints, vendor `dlt-init-openapi` heuristics. Optional agent flow: `/parse` → `/connections` → `/uploads` fully autonomous.

## 10. API surface changes

- **New:** `POST /api/v1/connections/openapi/parse` — `{spec_url?, spec_inline?, base_url?}` → `200 {base_url, auth_schemes, resources[], warnings}` / typed `400 {invalid_spec|spec_too_large|unsupported_version|ssrf_blocked}`. **Stateless** (no DB write) so agents/UI preview before committing. `@api_endpoint()` (auth + rate-limited), `x-stability: experimental`.
- **Unchanged:** connection create/list/etc. — `openapi` flows through `POST /api/v1/connections` with the assembled config; `openapi_inline` types the `config` in the spec automatically.
- **IR/introspection:** `POST /api/v1/connections/{id}/introspect` returns the stored resource catalog for an `openapi` connection (no live call).

## 11. Interactions with current work

- **Wave-1 connectors** ([#309](https://github.com/datanika-io/datanika-core/pull/323)): shares the `_build_saas_source`/`rest_api` fallback lineage; `_rest_api_from_parts` refactor should land without disturbing them (same output).
- **ELT/IR** ([SPEC_ELT_IR_ARCHITECTURE.md](SPEC_ELT_IR_ARCHITECTURE.md)): OpenAPI is a SaaS-class source → **ELT streaming** path applies; byte metering (V2) counts processed GB unchanged.
- **`openapi_inline.py`**: gains a shared `openapi_schema.py` import; behavior identical (guard with existing `test_openapi.py`).

## 12. Test strategy (handshake with QA)

- **Unit (Eng, mocked):** `openapi_import.parse_openapi_spec` against **checked-in fixtures** — Swagger Petstore 3.0, a cursor-paginated spec, an api-key-in-query spec, a `$ref`-heavy spec, and adversarial fixtures (circular `$ref`, 6 MB spec, 500-endpoint spec) asserting the §7 caps trip. Assert derived base_url/auth/resources/columns. `_build_openapi_source` builds a **real** `rest_api_source` from a parsed fixture (like the wave-1 smoke). IR-from-schema column mapping.
- **Contract:** `test_openapi.py` stays green (new type in `ConnectionCreate`), picker-coverage, meta-routes, i18n parity (wizard labels — Product).
- **Live smoke (QA/Infra, follow-on):** parse a public spec (e.g. Petstore live) + one real API with a test key from `secrets/` → extract a few rows. Mirrors [#311](https://github.com/datanika-io/datanika-core/issues/311)/[#312](https://github.com/datanika-io/datanika-core/issues/312).
- **Security tests:** SSRF fixtures (private IPs, metadata endpoint, redirect-to-private) → assert `ssrf_blocked`.

## 13. Decisions — ✅ SIGNED OFF 2026-07-17 (user accepted all recommended defaults)

> Each recommendation below is now the **decision**. P1 = paste-only, in-house parser, store `spec_source`, skip Test Connection, worker-egress flagged to Infra as a non-blocking handshake.


1. **Paste-only in P1, or URL-fetch from day one?** URL is nicer UX but adds the full SSRF surface. Recommendation: **paste-only P1**, URL in P2 behind §7 guards. *(Your call.)*
2. **Runtime egress policy.** Do Celery workers call arbitrary third-party hosts directly, or through an Infra egress proxy/allowlist? Pre-existing for `rest_api`, but OpenAPI makes it prominent. *(Infra handshake.)*
3. **Vendor `dlt-init-openapi` heuristics vs keep fully in-house** for P2 pagination/relationships? Recommendation: vendor specific functions, no runtime dep.
4. **Store raw inline spec on the connection** (enables re-parse, but bloats encrypted config up to 5 MB) **vs** store only the derived config (smaller, but re-parse needs a re-paste)? Recommendation: store `spec_source` with the cap.
5. **Test Connection semantics** — light live probe (needs egress from web tier) or skip like other SaaS? Recommendation: skip in P1; validate on first run.

## 14. Risks

- **Spec quality varies wildly** — many real specs are incomplete/wrong (missing `servers`, untyped responses). Mitigation: graceful degradation + `warnings[]` + user overrides; never hard-fail a whole spec on one bad endpoint.
- **SSRF is the headline risk** — a mis-implemented URL fetch or unguarded `base_url` is a real vuln. Mitigation: §7 is a ship gate; security-review the fetch path; consider a `SECURITY.md` note. P1 paste-only sidesteps the fetch half entirely.
- **Scope creep toward a full dlt-init-openapi reimplementation** — bounded by the phased plan; P1 is deliberately the 80% case.
- **Column drift on re-parse** surprising a live pipeline — mitigated by explicit (never auto) re-parse + drift surfacing.

## 15. What we explicitly do NOT commit to in this spec

- Any pagination beyond dlt's runtime auto-detection **in P1** (P2 owns spec-driven pagination).
- Parent-child relationship resolution in P1 (P3).
- OAuth2 flows beyond "supply a static token" in P1.
- Swagger 2.0 support until the P2 shim.
- A specific SSRF/egress implementation — §7 sets the requirements; Infra + a security review pick the mechanism.
- Write endpoints, GraphQL, or non-OpenAPI API description formats — ever, under this connector.
