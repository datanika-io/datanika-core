# SPEC — Wave-1 Connector Config Fields (Product → Engineering handoff)

> **Status**: Draft handoff, 2026-07-17. Product deliverable for [landing#227](https://github.com/datanika-io/datanika-landing/issues/227).
> **Consumer**: Engineering [core#309](https://github.com/datanika-io/datanika-core/issues/309) — `datanika/services/connection_config_fields.py` + `dlt_runner.py`.
> **Scope**: The UI connection-form field contract (labels, help text, sensitive flags, direction) for the four Wave-1 connectors: **Oracle, Pipedrive, Freshdesk, Asana**. Field **keys** here are the source of truth — the landing docs guides ([`src/content/connectors/{oracle,pipedrive,freshdesk,asana}.md`](https://github.com/datanika-io/datanika-landing/blob/main/src/content/connectors/)) and the `/connectors` catalog ([`src/data/connectors.ts`](https://github.com/datanika-io/datanika-landing/blob/main/src/data/connectors.ts)) already ship these exact keys. Keep them aligned so the docs match the real form.

---

## ⚠️ Shipped reality (verified against live app.datanika.io, 2026-07-17)

**Engineering (core#309, live in prod) shipped a MINIMAL field set that diverges from the contract in sections 1–4 below.** The drift was not flagged back to Product, so Product caught it during screenshot/label verification and corrected the docs to match reality. Sections 1–4 are the *original ask*; the table below is *what actually renders* in the shipped form.

| Connector | Shipped form fields (verified in UI) | Divergence from the spec below | Verdict |
|-----------|--------------------------------------|--------------------------------|---------|
| **Oracle** | `Connection Name`, `Host *`, `Port *` (auto-`1521`), `User`, `Password`, `Database *` | Routed through the generic `db_fields()` (grouped with postgres/mysql). **No `service_name` field** — the **`Database`** field is used as the Oracle **SID**. | ⚠️ Real limitation — service-name DBs (PDB/RAC/Autonomous) can't connect (`ORA-12505`). Already tracked as **core#329**. |
| **Pipedrive** | `Connection Name`, `API Key (optional) *` | Routed through shared `saas_api_key_fields()`. **No `company_domain` field**; label is `API Key`, not `API token`. | ✅ Acceptable — personal token works on the global `api.pipedrive.com` host; company domain not required. |
| **Freshdesk** | `Connection Name`, `Freshdesk Domain *`, `API Key (optional) *` | Dedicated `freshdesk_fields()`. Label is **`Freshdesk Domain`** (not "subdomain"); `api_key` label is `API Key`. | ✅ Matches intent (domain + key). |
| **Asana** | `Connection Name`, `API Key (optional) *` | Routed through shared `saas_api_key_fields()`. **No `workspace` field**; label is `API Key`, not `Access token`. | ✅ Acceptable — omitting workspace means "sync all accessible workspaces" (the field was optional anyway). |

**Other shipped facts** (verified in UI, corrected in the guides):
- The **Test Connection** button renders for **every** connector type (it lives in the form action row, outside the type-specific fields). For the three HTTP-API sources it returns *"Test not applicable for this type."* — so the guides' original "no Test-connection button" claim was wrong; the *intent* (validated on first run) was right.
- The type dropdown shows the **lowercase type key** (`oracle`, `pipedrive`, …), not a title-cased name.
- The **Connection Name** field strips non-alphanumerics (hyphens/underscores) — guide example names updated accordingly.
- Submit button is **`Create Connection`**; a **`Use raw JSON config`** escape-hatch checkbox is available for advanced config.
- Live code paths: `datanika/ui/components/connection_config_fields.py` (`type_fields()` routing) + `datanika/i18n/en.json` (`connections.*` labels), both on `origin/master`.

**Follow-ups for Engineering to consider** (not blocking the docs): Oracle service-name support (core#329, open). Pipedrive `company_domain` and Asana `workspace` were intentionally dropped and are fine as-is; re-add only if a user needs host pinning / workspace scoping.

## Conventions

- **Sensitive** = store encrypted at rest (Fernet), render as a password input, never echo back in the UI.
- **Test connection button**: DB sources (Oracle) validate synchronously → show the button. HTTP-API sources (Pipedrive, Freshdesk, Asana) validate on first run → **no** Test-connection button (mirror the existing Zendesk / Stripe / HubSpot pattern).
- All four are **source-only** in Wave 1 (`direction = "source"`; not in `DESTINATION_TYPES`). This matches the catalog (`direction: "source"`), which keeps them out of the `/docs/architecture` (11 destinations) and `/docs/transformations` (6 warehouses) link-count tests on the landing side.
- Where a dlt verified source already exists, prefer it over hand-rolling `rest_api` (noted per connector).

---

## 1. Oracle — `oracle`

- **Category**: Database · **Direction**: source · **Test-connection button**: yes
- **Driver**: SQLAlchemy `oracle+oracledb://` (python-oracledb *thin* mode — no Instant Client needed). Route through the existing generic `sql_database()` path in `dlt_runner.py` (same path MySQL/MSSQL use). Add `"oracle"` to `SUPPORTED_SOURCE_TYPES` and `SOURCE_TYPES`; `infer_direction()` → `"source"`.
- **Connection string**: `oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={service_name}`. If a user supplies a SID rather than a service name, the alternate DSN form is `.../{sid}` — support service_name first; SID is the documented fallback.

| Key | Label | Type | Required | Sensitive | Default | Help text |
|-----|-------|------|----------|-----------|---------|-----------|
| `host` | Host | text | ✅ | — | — | Database hostname or IP address. |
| `port` | Port | number | ✅ | — | `1521` | Oracle listener port. |
| `service_name` | Service name | text | ✅ | — | — | Oracle service name (or SID) of the database / PDB. |
| `username` | User | text | ✅ | — | — | Read-only database user. |
| `password` | Password | password | ✅ | ✅ | — | Database password. |

**Notes for Eng**: 12c+ PDBs are addressed by service name, not SID — the guide tells users to grab the service name. Oracle identifiers are UPPERCASE unless quoted; table discovery should surface them as stored. `NUMBER` without precision/scale lands as high-precision decimal — no action needed on the source side, just a known downstream cast (documented in the guide).

## 2. Pipedrive — `pipedrive`

- **Category**: SaaS & API · **Direction**: source · **Test-connection button**: no
- **dlt source**: reuse the **`pipedrive`** verified source if available; otherwise `rest_api` against `https://{company_domain}.pipedrive.com/api/v2` (the token also works on the global `https://api.pipedrive.com` host, but the company-domain host is preferred).
- **Auth**: `api_token` as the `api_token` query param (Pipedrive's token auth), not a Bearer header.

| Key | Label | Type | Required | Sensitive | Default | Help text |
|-----|-------|------|----------|-----------|---------|-----------|
| `api_token` | API token | password | ✅ | ✅ | — | Personal API token from Pipedrive → Personal preferences → API. |
| `company_domain` | Company domain | text | ✅ | — | — | Subdomain of `*.pipedrive.com` (e.g. `acme`). |

**Resources** (for `configure pipeline` discovery): `deals, persons, organizations, activities, pipelines, stages, users, notes, products`. Primary key `id`; `merge` cursor = `update_time`. **Custom fields** come back keyed by 40-char hashes — leave them raw (map in dbt); do not try to resolve labels in the extractor.

## 3. Freshdesk — `freshdesk`

- **Category**: SaaS & API · **Direction**: source · **Test-connection button**: no
- **dlt source**: `rest_api` against `https://{subdomain}.freshdesk.com/api/v2`.
- **Auth**: HTTP Basic — **`api_key` as the username, any placeholder as the password** (Freshdesk convention; use `"X"`). The user only ever supplies the key.

| Key | Label | Type | Required | Sensitive | Default | Help text |
|-----|-------|------|----------|-----------|---------|-----------|
| `subdomain` | Freshdesk subdomain | text | ✅ | — | — | Subdomain of `*.freshdesk.com` (e.g. `acme`). |
| `api_key` | API key | password | ✅ | ✅ | — | Agent API key from Freshdesk → Profile settings. |

**Resources**: `tickets, contacts, companies, agents, groups, conversations, satisfaction_ratings, time_entries`. Primary key `id`; `merge` cursor = `updated_at`. **Pagination gotcha for Eng**: the ticket list endpoint caps at 300 pages and only returns the last 30 days unless `updated_since` is supplied — the incremental cursor must drive `updated_since`, not naive offset paging, or older tickets are silently unreachable. Honor `Retry-After` on `429`.

## 4. Asana — `asana`

- **Category**: SaaS & API · **Direction**: source · **Test-connection button**: no
- **dlt source**: reuse the **`asana_dlt`** verified source if available; otherwise `rest_api` against `https://app.asana.com/api/1.0`.
- **Auth**: Bearer — `Authorization: Bearer {access_token}` (personal access token).

| Key | Label | Type | Required | Sensitive | Default | Help text |
|-----|-------|------|----------|-----------|---------|-----------|
| `access_token` | Access token | password | ✅ | ✅ | — | Personal access token from Asana → developer console (`app.asana.com/0/my-apps`). |
| `workspace` | Workspace GID | text | ❌ (optional) | — | — | Workspace to sync. Blank = all workspaces the token can access. |

**Resources**: `workspaces, projects, sections, tasks, users, teams, tags, stories`. Every object keyed by `gid`; `merge` cursor = `modified_at`. **Extraction gotcha for Eng**: there is no "all tasks in a workspace" endpoint — tasks must be iterated **per project** (or per assignee+workspace). The extractor must fetch `projects` first, then loop them to assemble `tasks`. Tasks in no project are only reachable via assignee queries (documented as a limitation in the guide).

---

## Handoff checklist

- [x] Field keys locked and mirrored in landing catalog + docs guides (landing#228, merged to dev + promoted).
- [x] Eng: implement fields in `connection_config_fields.py` + wire `dlt_runner.py` source paths (core#309, live in prod).
- [~] Eng: confirm final labels/keys against this spec; **flag any drift back to Product**. — **Drift was NOT flagged.** Product caught it during 2026-07-17 verification (see "⚠️ Shipped reality" above). Eng shipped a minimal field set; docs corrected to match.
- [x] Product: capture real screenshots + exact field labels for each guide, flip `verified_by` from `draft-pending-verification`. — **Done 2026-07-17** (this task): 4 `02-add-connection.png` shots from the live app, every field label verified in-UI, `verified_by: product-ui` / `verified_date: 2026-07-17`. Ships in the follow-up landing PR.
