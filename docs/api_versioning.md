# API Versioning Policy

All Datanika REST endpoints live under `/api/v1/`. This document defines
the rules for evolving that surface without breaking integrations.

## Breaking-Change Rules

A **breaking change** is any modification that can cause a working
client to fail. The following are breaking:

| Category | Examples |
|----------|----------|
| Removal | Deleting an endpoint, field, or enum value |
| Rename | Changing an endpoint path, field name, or query param |
| Type change | Changing a field from `string` to `integer`, or from nullable to non-nullable |
| Semantic change | Altering the meaning of a status code or error code |
| Auth tightening | Requiring a new scope or elevating the required role |

The following are **non-breaking** and may ship without notice:

- Adding a new endpoint, field, query parameter, or enum value
- Adding a new optional request-body property
- Widening a constraint (e.g., raising a `maximum`)
- Adding new error codes alongside existing ones
- Relaxing auth requirements

## Deprecation Lifecycle

1. **Announce** — the endpoint or field is marked `deprecated: true` in
   the OpenAPI spec and a `Sunset` HTTP header is added to responses.
   The minimum notice period is **12 months** from the announcement date.
2. **Warn** — during the notice period the endpoint continues to work.
   Responses include `Deprecation: true` and `Sunset: <date>` headers.
   Release notes and changelog entries call out the deprecation.
3. **Remove** — after the sunset date the endpoint may return
   `410 Gone`. The schema entry is removed from the OpenAPI spec.

For critical security fixes, the minimum notice period may be shortened
to **30 days** with explicit communication to affected users.

## Stability Tiers

Every operation in the OpenAPI spec carries an `x-stability` extension
so consumers (and enterprise procurement checklists) can assess risk
at a glance.

| Tier | Meaning | Commitment |
|------|---------|------------|
| **stable** | Production-ready, fully supported | 12-month deprecation notice before breaking changes |
| **beta** | Functionally complete, schema may evolve | 3-month notice before breaking changes; additive changes ship freely |
| **experimental** | Early access, may change or be removed at any time | No notice required; not recommended for production integrations |

### Current Assignments

| Endpoints | Tier |
|-----------|------|
| `/api/v1/connections`, `/api/v1/uploads`, `/api/v1/pipelines`, `/api/v1/transformations`, `/api/v1/schedules` (CRUD + trigger) | stable |
| `/api/v1/runs`, `/api/v1/notifications/channels` | stable |
| `/api/v1/transformations/{id}/compile`, `/api/v1/transformations/{id}/preview` | stable |
| `/api/v1/catalog`, `/api/v1/catalog/{id}` | beta |

### Promotion Path

`experimental` -> `beta` -> `stable`. Promotion happens via a PR that
updates the `x-stability` value in `datanika/services/openapi.py` and
adds a changelog entry. Demotion (e.g., `stable` -> `beta`) is treated
as a breaking change and follows the deprecation lifecycle above.

## Version Lifecycle

| Phase | Description |
|-------|-------------|
| **Active** | `/api/v1/` — current and only version. Receives features and fixes. |
| **Deprecated** | When `/api/v2/` is introduced, v1 enters a 12-month sunset window. |
| **Retired** | After the sunset date, v1 endpoints return `410 Gone`. |

There are no plans to introduce `/api/v2/` at this time. When that
happens, this document will be updated with a concrete timeline.
