# SPEC — Remote / Hosted MCP Server (Streamable HTTP + OAuth 2.1)

> **Provenance.** Migrated on 2026-08-31 from the local planning directory
> (`plans/engineering/`), which is outside every git repository and therefore has no history,
> no review and no recovery path. A spec is a contract amended across sessions, so it belongs
> in the repository it governs. Content is unchanged apart from link paths; a few links still
> point at internal planning documents that are not part of this repository.


> Status: **SIGNED OFF 2026-07-19 (CEO).** All 5 §11 open questions resolved as recommended (verdicts stamped in §11). Implementation unblocked — file the P1 issue off this doc and start. **P1 = read-only + bearer-is-API-key, no OAuth build.**
> Owner: Engineering. Cross-dep: Infra (Apache `/mcp` proxy + SSE, deploy), Product (consent UI), Growth (MCP-directory listing).
> Related: [SPEC_OPENAPI_CONNECTOR.md](SPEC_OPENAPI_CONNECTOR.md), [`datanika-mcp/`](../../datanika-mcp/), agent docs ([#353](https://github.com/datanika-io/datanika-core/issues/353)), API-key scopes ([#297](https://github.com/datanika-io/datanika-core/issues/297)), SSRF/egress ([#338](https://github.com/datanika-io/datanika-core/issues/338)).

---

## 1. TL;DR

`datanika-mcp` today is **stdio-only** — a local `uvx` process a user runs on their machine and wires into Claude Desktop by hand. In 2025–26 the ecosystem moved to **remote MCP**: one-click "Add Datanika" inside **Claude.ai, ChatGPT, and Cursor** with no local install. This spec adds a **hosted Streamable-HTTP MCP endpoint at `https://app.datanika.io/mcp`**, authenticated with **OAuth 2.1** (the MCP remote-auth spec) mapped onto Datanika's existing **API-key scope model**.

The load-bearing design choice: the remote server is a **thin authenticated REST client of its own API** — it forwards the caller's org-scoped credential to `127.0.0.1:8000/api/v1/...`. That means org isolation, scope enforcement, rate limits, and V2 byte-metering are **inherited from `api_middleware`, not re-implemented**. The MCP layer only adds transport + auth + the tool surface (which is shared, verbatim, with the stdio server).

## 2. Goals

- **G1. One-click remote MCP** in Claude.ai / ChatGPT / Cursor — no local install, no `uvx`, no pasting a config file.
- **G2. One tool surface, two transports.** The 25 `@mcp.tool()` definitions in [`datanika_mcp/server.py`](../../datanika-mcp/src/datanika_mcp/server.py) serve **both** stdio and remote — no fork, no duplication.
- **G3. Auth = Datanika's scope model.** OAuth 2.1 grants map to a read-only or read-write **API key** (per [#297](https://github.com/datanika-io/datanika-core/issues/297)); "remote with write" ≡ today's `--allow-write`, gated by consent.
- **G4. Free multi-tenancy.** Per-org isolation + rate limits + metering come from `api_middleware` because the MCP server calls the REST API with the caller's key — zero new isolation code.
- **G5. Distribution.** Be listable in the Claude/MCP connector directories — a durable, agent-native acquisition channel (the VC "AI-agent-native" thesis made real, beyond `/llms.txt`).

## 3. Non-goals

- Not replacing the stdio server — it stays for local/offline/self-host and power users.
- Not a general OAuth **identity provider** for third parties — the AS role exists only to authorize MCP clients against Datanika accounts.
- Not new **tools** — same 25. New tools are separate work.
- Not `SSE`-transport (the deprecated MCP transport) — Streamable HTTP only.
- Not solving arbitrary-host **egress** here — that's [#338](https://github.com/datanika-io/datanika-core/issues/338); this spec documents the interplay and inherits its guardrails (§10).
- Not self-hoster OAuth-AS turnkey config in P1 (cloud endpoint first).

## 4. Background — what we build on

**4.1 The stdio server (the tool surface to reuse).** [`datanika_mcp/server.py`](../../datanika-mcp/src/datanika_mcp/server.py): a **`FastMCP("Datanika")`** instance with 25 `@mcp.tool()`s — 17 read-only, 8 write. Each tool calls `_get_client().<method>()`; the client is a **process-global** `DatanikaClient(url, api_key)` (an httpx wrapper over the REST API), and writes are gated by a **process-global** `_allow_write` via `_require_write()`. `main()` reads `--url/--api-key/--allow-write`, sets the globals, and calls `mcp.run(transport="stdio")`. **The globals are the thing a multi-tenant remote server cannot keep** — see §5.1.

**4.2 The REST API + scope model.** Every endpoint is `@api_endpoint(required_scope="connections:read"|"connections:write"|…)`; `api_middleware` authenticates `Authorization: Bearer etf_<key>`, resolves `api_key.org_id`, enforces the key's scopes, applies the per-key rate limit, and (cloud) records byte-metering. An unscoped key has full access; a `*:read`-only key is rejected on writes (proven by `e2e/tests/rbac.spec.ts`). **This is the entire authz/isolation/limits stack the MCP layer will inherit by calling the API.**

**4.3 How the app serves extra routes.** Reflex 0.8.x runs on Starlette; Datanika appends plain `starlette.routing.Route`s to `app._api.routes` (OAuth/SSO/webhooks/health/`/llms.txt` all do this). The `/mcp` endpoint and the OAuth endpoints mount the same way.

**4.4 Hosting.** Prod is the Datanika backend (`:8000`) behind **Apache** on pointer.gr; Apache proxies `/api/`, `/_event` (WebSocket), etc. to `:8000`. A new `/mcp` location must be proxied too, with **response buffering off** for the server→client SSE stream (same class of handling as `/_event`).

**4.5 The MCP SDK.** `mcp>=1.0` is already a core dep (from the [#153](https://github.com/datanika-io/datanika-core/pull/153) MCP stub). FastMCP exposes `mcp.streamable_http_app()` (a mountable Starlette app) and an auth/token-verifier hook. We'll pin whatever version ships stable Streamable-HTTP + resource-server auth (a lib bump — no vendor cost).

## 5. Proposed design

### 5.1 One tool surface, two transports — de-globalize the client

The tools must resolve **`(client, allow_write)` per request**, not from module globals, so one `@mcp.tool()` body serves a single stdio user *and* thousands of concurrent remote orgs.

- Replace `_get_client()` / `_require_write()` with lookups on FastMCP's request-scoped **`Context`**: `ctx = mcp.get_context()` → `session = ctx.request_context` → a `DatanikaSession(client, allow_write)` attached at auth time.
- **stdio** supplies a single fixed `DatanikaSession` (from the CLI key) for the whole process — behaviourally identical to today.
- **remote** supplies a **per-request** `DatanikaSession` derived from the caller's OAuth token (§5.3).
- Tool bodies change only from `_get_client()` → `_session().client` and `_require_write("x")` → `_session().require_write("x")`. The 25 tool signatures/docstrings are untouched (they *are* the product surface).

### 5.2 Transport — Streamable HTTP at `/mcp`

- Mount `mcp.streamable_http_app()` at `https://app.datanika.io/mcp` as a Starlette sub-app appended to `app._api.routes`.
- Streamable HTTP = a single endpoint: **POST** for JSON-RPC requests/notifications, **GET** to open the optional server→client SSE stream (progress, sampling). Session correlation via the `Mcp-Session-Id` response/request header.
- **Stateful vs stateless:** run **stateful** (a session per client) so long-running tool calls (`trigger_*` with `wait=true`) can stream progress and so auth is established once per session. Session state is in-memory per backend worker; with `GRANIAN_WORKERS>1` behind Apache, pin sessions to a worker via sticky routing **or** run stateless-JSON mode in P1 to avoid the affinity problem (§11 open question).

### 5.3 Auth — OAuth 2.1 → Datanika API key

Follow the MCP remote-auth spec (2025-06-18): the MCP endpoint is an **OAuth 2.1 Resource Server**; Datanika also plays the **Authorization Server** (it already owns accounts, orgs, JWT login).

Flow (one-click):
1. Client `POST /mcp` with no token → `401` + `WWW-Authenticate` pointing at **Protected Resource Metadata** `/.well-known/oauth-protected-resource` (lists the AS + the `mcp` resource identifier).
2. Client reads **AS metadata** `/.well-known/oauth-authorization-server`, **dynamically registers** (`POST /oauth/register`, RFC 7591), then runs **authorization-code + PKCE**: `GET /oauth/authorize` → Datanika login + a **consent screen** ("Datanika: allow *Claude.ai* to **read** / **read & write** your **<org>** data?") → redirect back with a code → `POST /oauth/token` → **access token** (+ refresh).
3. Every `POST /mcp` carries `Authorization: Bearer <token>`; FastMCP's token verifier validates it (audience-bound to the `mcp` resource — see §10) and attaches the `DatanikaSession`.

**Token → credential mapping (the key decision).** On consent, Datanika **mints an org-scoped API key** carrying the granted scopes (read-only = `*:read`; read-write = full) and the access token maps 1:1 to it. The remote server presents that key as `Authorization: Bearer` to its **own REST API at `127.0.0.1:8000`**. Consequences:
- **read-only vs read-write** is just the key's scopes → §4.2 enforces it; "remote write" ≡ `--allow-write`, per **consent**.
- **Revocation** = revoke the key (existing UI) or the OAuth grant.
- The MCP server holds **no bespoke authz** — it forwards a credential the API already understands.

### 5.4 Isolation, rate limits, metering — inherited, not rebuilt

Because §5.3 makes the MCP server call `127.0.0.1:8000/api/v1/...` with the caller's org key:
- **Org isolation** — `api_key.org_id` filters every query (TenantMixin). An agent can only ever see its own org.
- **Rate limits** — the per-key RPM limit throttles a runaway agent for free (Free 30 / Pro 120 / Ent 300 RPM).
- **V2 byte-metering** — `trigger_*` runs meter GB-processed on the caller's plan exactly as REST-triggered runs do.
- **Audit** — every tool call is an authed API call → already in the audit log, now with an `mcp` client marker.

This is why "thin REST client of its own API" beats calling the service layer in-process: in-process would **bypass** all of the above and force re-implementation.

### 5.5 SSRF / egress interplay (with #338)

The write tools expose two egress-adjacent capabilities to a **remote** agent: `create_connection` (store an arbitrary `rest_api`/`openapi` `base_url`) and `trigger_upload` (make the worker fetch it). This is the **same** surface the REST API + the `openapi`/`rest_api` connectors already expose ([#338](https://github.com/datanika-io/datanika-core/issues/338), SPEC_OPENAPI_CONNECTOR §7) — the remote MCP **adds no new egress path**, it just makes the existing one reachable by remote AI agents holding a user's write grant. Therefore: **#338's private-range/allowlist guardrails fix it for MCP too**, and P1/P2 ship **read-only**, deferring the write tools until write-consent + #338 land (§7).

### 5.6 Proposed module layout

```
datanika-mcp/src/datanika_mcp/
  server.py        # refactor: tools resolve DatanikaSession from Context (not globals);
                   # add create_streamable_http_app() returning the mountable Starlette app
  session.py       # NEW — DatanikaSession(client, allow_write) + require_write()
datanika/services/
  mcp_routes.py    # NEW — mounts create_streamable_http_app() at /mcp; FastMCP token
                   #        verifier -> DatanikaSession(DatanikaClient("127.0.0.1:8000", key))
  mcp_oauth.py     # NEW (P2) — OAuth 2.1 AS: metadata, DCR, /authorize (login+consent), /token,
                   #        token<->API-key mapping, refresh, revoke
datanika/datanika.py  # append the /mcp + /.well-known + /oauth routes to app._api.routes
# Dockerfile: `uv pip install ./datanika-mcp` so the core app can import the shared tool surface
```

## 6. Deployment (Infra handshake)

- **Apache**: add a `/mcp` proxy to `127.0.0.1:8000` with **`ProxyPass ... flushpackets=on` / buffering off** so the SSE stream isn't buffered (same rationale as `/_event`). The `/.well-known/*` + `/oauth/*` paths route to the backend like `/api/`.
- **TLS/host**: reuse the existing `*.datanika.io` cert; the MCP resource identifier is `https://app.datanika.io/mcp`.
- **Workers**: resolve the session-affinity question (§5.2) before turning on stateful mode with `GRANIAN_WORKERS>1`.
- **No new server** — it runs in the existing backend process; `datanika-mcp` becomes an install dep of the image.

## 7. Phased rollout

- **P1 — remote transport, bearer = API key (no OAuth AS yet).** De-globalize the tool surface (§5.1); mount `/mcp` (Streamable HTTP); **read-only tools only**; auth = the user pastes an existing Datanika **API key** as a bearer/custom header (works today in Cursor + clients that allow it). Ships + proves the transport, the shared tool surface, and the "REST-client-of-itself" model without the OAuth build. **Ship gate:** an MCP client lists + calls the 17 read tools against a real org; isolation holds.
- **P2 — OAuth 2.1 one-click.** The AS (`mcp_oauth.py`): protected-resource + AS metadata, DCR, PKCE authorize+consent+token, token↔API-key. → one-click "Add Datanika" in Claude.ai / ChatGPT. Still read-only by default; consent can grant read-write.
- **P3 — write tools GA + directory listing.** Enable the 8 write tools behind write-consent (gated on #338 egress guardrails); submit to the MCP/Claude connector directory; per-tool scope granularity; refresh-token rotation.

## 8. API / endpoint surface (new)

| Endpoint | Purpose |
|---|---|
| `POST/GET /mcp` | Streamable-HTTP MCP endpoint (JSON-RPC + optional SSE), bearer-authenticated |
| `GET /.well-known/oauth-protected-resource` | Points MCP clients at the AS + resource id (P2) |
| `GET /.well-known/oauth-authorization-server` | AS metadata (P2) |
| `POST /oauth/register` | Dynamic Client Registration, RFC 7591 (P2) |
| `GET /oauth/authorize` | Login + **consent** (scope + org selection) → auth code (P2) |
| `POST /oauth/token` | Code→token, refresh; mints/binds the org-scoped API key (P2) |

Unchanged: the REST API — the MCP server is a client of it.

## 9. Interactions with current work

- **#353** (`/llms.txt` + agent-guide already advertise the MCP path): update the MCP section to add the **remote** URL once P1 lands.
- **#297** (API-key scopes): the read/write consent maps directly onto these — no new authz vocabulary.
- **#338** (SSRF/egress): §5.5 — MCP inherits the fix; write tools wait on it.
- **openapi/rest_api connectors**: reachable via the write tools; same egress posture.
- **Cloud metering**: `trigger_*` from MCP meters like any run — the plan enforcement already flows through.

## 10. Security — the load-bearing section

- **Least privilege + consent.** Default **read-only**. Write requires an explicit read-write grant on the OAuth consent screen; the token→key mapping carries only granted scopes. No silent escalation.
- **Confused-deputy / token passthrough.** The `/mcp` resource server accepts **only** tokens it issued, **audience-bound** to `https://app.datanika.io/mcp`. It never forwards a client-presented third-party token upstream. (This is the canonical MCP-auth pitfall — call it out in the impl.)
- **Prompt injection → write tools.** A malicious source row could try to make the agent call `create_connection`/`trigger_*`. Mitigations: write off by default; **rate limits + byte quotas cap blast radius**; `trigger_*` (compute/$ spend) is P3 and may require a per-call confirmation elevation; every call is audit-logged and attributable to the grant.
- **Egress** — §5.5 / #338.
- **Revocation & expiry** — short-lived access tokens + refresh; revoke = kill the key/grant; surfaced in the same API-keys UI.
- **Consent UI is Product** — the `/oauth/authorize` screen (app name, requested scopes, org picker) is a Product cross-dep.

## 11. Open questions — RESOLVED 2026-07-19 (CEO sign-off)

All five accepted as recommended. Verdicts:

1. **Stateful vs stateless in P1 → stateless-JSON P1.** No multi-worker session affinity to solve for a read-only P1; go stateful in P2 when streaming / per-session consent needs it.
2. **Own AS vs delegate → own AS, endorsed as the P2 *direction*.** We already own accounts/orgs/JWT and a vendor AS violates the no-cost-raises-pre-10-paid rule. **Caveat:** this is the biggest, most security-sensitive P2 build — **re-validate own-AS vs a managed AS (e.g. WorkOS) at the point P2 actually starts**, not now. P1 needs no AS, so nothing is blocked today.
3. **Token → minted org-scoped API key.** The load-bearing choice: reuses revocation + the API-keys UI; the MCP server stays a pure forwarder. Confirmed.
4. **Write tools → P3**, gated on #338 egress guardrails + a per-call confirmation model for `trigger_*`. A remote agent spending compute/money must not ship early. Confirmed.
5. **Self-hosters → cloud-only P2**, document self-host in P3. Confirmed.

**P1 scope (start here):** de-globalize the tool surface (§5.1) → mount `/mcp` Streamable-HTTP (stateless-JSON) → **read-only tools only** → bearer = an existing Datanika API key (no OAuth build). Ship gate per §7 P1. File the implementation issue off this doc and begin.

## 12. Risks

- **OAuth 2.1 AS is real work** and security-sensitive — the P1 "bearer API key" step deliberately de-risks by shipping the transport first, AS second.
- **Streamable-HTTP + Apache SSE buffering** can silently break streaming — validate with a real client early (like the WS `/_event` work).
- **Remote write = an agent spending compute/money** — the phasing (write in P3, quotas, consent, confirmation) is the mitigation; shipping write early would be the mistake.
- **MCP spec churn** (auth revision moved fast in 2025) — pin to a spec revision + SDK version; keep the surface small.
- **Session state under multiple workers** — §11.1; getting it wrong = flaky sessions.

## 13. Test strategy (handshake with QA)

- **Unit**: the de-globalized tool surface — a `DatanikaSession` fake proves each tool routes through the session client and that `require_write` blocks writes for a read-only session. One suite, exercised by both transports.
- **Transport integration**: boot the `/mcp` app in-process with the MCP SDK client; `initialize` → `tools/list` (25) → call a read tool → assert it hit the REST API with the session key; assert an unknown/absent token → 401 with the resource-metadata pointer.
- **Isolation**: two orgs' tokens; each `list_connections` sees only its own (rides §5.4).
- **OAuth (P2)**: metadata discovery, DCR, PKCE happy-path + audience-mismatch rejection (confused-deputy).
- **Live smoke (QA/Infra, follow-on)**: a real remote client (Claude.ai / `mcp-remote`) against staging — connect, list tools, read. Mirrors the connector-smoke posture.

## 14. What we explicitly do NOT commit to in this spec

- New tools (same 25).
- The deprecated `SSE` transport.
- Write tools before #338 + a confirmation model (P3).
- Datanika as a general-purpose IdP.
- Self-hoster OAuth turnkey in P1/P2.
- A specific SDK/spec revision — pinned at implementation time to the current stable Streamable-HTTP + resource-server-auth release.
