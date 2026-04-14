# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.x     | Latest minor only  |

Datanika follows a rolling-release model. Security patches are applied
to the most recent `0.x` release on the `master` branch and deployed
to `app.datanika.io` immediately. Older minor versions do not receive
backports. Self-hosted operators should always run the latest release.

## Reporting a Vulnerability

**Email**: [security@datanika.io](mailto:security@datanika.io)

Please include:

1. A description of the vulnerability and its potential impact.
2. Steps to reproduce or a proof of concept.
3. The affected version(s) and component (core, cloud plugin, landing,
   REST API, UI).

### What to expect

| Milestone          | SLA            |
| ------------------ | -------------- |
| Acknowledgement    | 48 hours       |
| Initial assessment | 7 calendar days|
| Patch release      | 90 calendar days (critical: best effort for 30 days) |

We follow **coordinated disclosure**: we ask reporters to keep details
private until a patch is released or the 90-day window expires,
whichever comes first. We will credit reporters in the release notes
and the Hall of Fame below unless they prefer to remain anonymous.

## Scope

### In scope

- Authentication and authorization (JWT, API keys, RBAC, SSO/SAML/OIDC)
- Multi-tenant isolation (org_id boundary, schema separation)
- Credential encryption (Fernet) and secret management
- REST API `/api/v1/*` endpoints
- Webhook signature verification (Paddle HMAC)
- SQL injection in user-provided queries (SQL Editor, transformations)
- Cross-site scripting (XSS) in the Reflex UI
- Server-side request forgery (SSRF) via connector configuration
- Dependency vulnerabilities with a known exploit path

### Out of scope

- Denial-of-service attacks against `app.datanika.io` infrastructure
- Social engineering or phishing
- Vulnerabilities in upstream dependencies without a demonstrated
  exploit path in Datanika's usage
- Rate limiting thresholds (these are configurable, not a vulnerability)
- Self-hosted misconfiguration (e.g., running without TLS, exposing
  the database port)

## Security Architecture

Datanika's security model is documented in the codebase:

- **Multi-tenancy**: all tables use `org_id` column filtering
  (`TenantMixin`). 25 cross-tenant boundary tests cover every
  `/api/v1/*` mutation route.
- **Encryption**: credentials stored via `Fernet` symmetric encryption;
  passwords hashed with `bcrypt` (no passlib).
- **Authentication**: JWT tokens for UI sessions, API keys for REST API,
  SSO via SAML 2.0 and OIDC.
- **Authorization**: role-based access control (owner / admin / editor /
  viewer) enforced at the state and service layers.
- **Audit logging**: all mutations logged with user, org, action, and
  timestamp.

## Hall of Fame

We thank the following researchers for responsibly disclosing
vulnerabilities:

*No reports yet. Be the first!*

## PGP Key

For encrypted communication, use the PGP key published at:

```
Key ID:       (to be generated on first report)
Fingerprint:  (to be generated on first report)
```

Alternatively, email `security@datanika.io` and request an encrypted
channel — we will respond with a key within 48 hours.
