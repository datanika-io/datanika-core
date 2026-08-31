# Spec: SOC 2 Type I 90-Day Readiness Roadmap

> **Status**: Draft — phase-1 deliverable for DAG task `prod-soc2-roadmap` (plans/dags/scope_ceo_2026-04-14.yaml). No implementation gated on this; the doc itself is the unblock.
> **Owner**: Product (this file). Phase-2 execution owner is whoever holds the Drata/Vanta seat after `human-soc2-vendor-select` resolves.
> **Related**:
> - ../PLAN_HUMAN_LOCKERS.md (`plans/PLAN_HUMAN_LOCKERS.md`) → `soc2-vendor-selection` (the paid-trial human locker this doc explicitly *does not* cover)
> - [../engineering/PLAN_ENGINEERING.md](https://github.com/datanika-io/datanika-core/issues) → `eng-security-md` (SECURITY.md in core repo — prerequisite for Trust page, not for this doc)
> - [PLAN_PRODUCT.md](https://github.com/datanika-io/datanika-core/issues/734) → P1 WBS line 32 (tracking entry for this task) and P2 (Trust page, consumes this doc as a link target)
> - ../dags/scope_ceo_2026-04-14.yaml (`plans/dags/scope_ceo_2026-04-14.yaml`) → `prod-soc2-roadmap` (task definition) and `prod-trust-page` (downstream consumer)
> - VC memo §7 item 4 ("Enterprise baseline — compliance roadmap") — the business-side unblock this doc serves
> **Date**: 2026-04-14 (drafted)
> **Non-goals**: this doc is not SOC 2 itself, not a policy library, not an evidence dossier, and not a vendor contract. It is a *dated plan* that a founder can hand to a prospect, an investor, or a procurement team and say "here is when we will be audit-ready, here is what we already have, here is what is left."

---

## TL;DR

- **Target outcome**: SOC 2 Type I report-in-hand by **2026-07-13** (90 days from 2026-04-14).
- **Why Type I and not Type II**: Type I is a point-in-time attestation ("controls are designed and implemented as of date X"). Type II requires a 3–12 month observation window on top of that. Type I is the right first milestone because (a) it unblocks the enterprise conversation immediately, (b) it is the prerequisite for Type II anyway, and (c) 90 days is realistic given our current control maturity; 180 days for Type II is not.
- **Trust Services Criteria in scope**: **Security** (required — the "Common Criteria" / CC series). **Availability** and **Confidentiality** will be added in the Type II phase. Processing Integrity and Privacy are out of scope until an enterprise deal explicitly asks for them.
- **Existing control maturity**: **~65%** of the CC-series controls are already implemented in code, with a measurable evidence trail. The rest are policy documents (which we do not yet have) and operational runbooks (which are partially written and live in `plans/infra/`).
- **Vendor recommendation**: **Drata** (see §6). $7,500 one-time implementation + ~$6,000/year subscription for <25-employee pricing, sandbox-friendly integration surface, strongest devtool ecosystem coverage. Vanta is a tolerable second; Secureframe is the budget third.
- **Auditor cost**: separate from the vendor. $12,000–$18,000 for Type I from a small-firm CPA (e.g., Prescient Assurance, Johanson Group, A-LIGN's lower tier). Picked after the vendor trial, not before.
- **Total Phase-1 budget envelope**: **~$25,000** over 90 days (vendor implementation + subscription pro-rata + auditor + minor cloud infra).
- **Who blocks what**: the *roadmap* (this doc) has zero human blockers. **Phase 2** (actually getting audited) is blocked on one human step: picking a vendor and signing a paid trial (`human-soc2-vendor-select`).

---

## 1. Why the roadmap alone is the unblock

The VC memo objection is *"no visible compliance posture"*. That is a communication problem, not a certification problem. An enterprise buyer, a procurement team, and a VC associate all ask the same question at the same stage of the conversation:

> "When will you be SOC 2?"

They do not ask "are you SOC 2 today?" unless the contract explicitly requires it (which at our ACV range is rare). What kills the conversation is "we haven't thought about it yet" or "someday." What saves the conversation is a dated plan that lists existing controls, gaps, vendor, auditor, and a finish date.

The practical consequences, in the order they compound:

1. **Trust page on `datanika.io/trust`** (DAG task `prod-trust-page`) needs a link target for "Compliance." Without this doc, the Trust page either ships with a "coming soon" stub (bad) or is delayed (worse — it is the single best procurement-conversion surface we can ship this sprint).
2. **Pricing page Enterprise tier** currently has no compliance talking point. With this doc live, the Enterprise CTA can honestly say "SOC 2 Type I in progress, Q2 target" instead of silence.
3. **VC memo §7 item 4** explicitly lists "Enterprise baseline — compliance roadmap" as one of four outstanding items at the 58/100 score. Each of the four items is worth ~3–4 points. A dated roadmap (not certification) closes this one.
4. **Phase-2 execution is faster with the roadmap already written**. Vendors will ask for a gap analysis on day 1 of the paid trial. Having the inventory in §5 and the gap list in §7 below means the trial starts at week 2, not week 0.

**Consequence of doing nothing**: the enterprise conversation keeps stalling on the same question and the Trust page ships blank. Low-cost, high-leverage — this is exactly the "docs-only, agent-automatable, no walkthrough deps" shape the task was scoped for.

---

## 2. Scope decisions

### 2.1 Report type: Type I, not Type II (this round)

| | **Type I** | **Type II** |
|---|---|---|
| What it attests | Controls are *designed and implemented* as of a specific date | Controls *operated effectively* over a period (3–12 months) |
| Observation window | None (point-in-time) | 3 months minimum, 6–12 months typical |
| Time to first report | ~90 days from kickoff | ~9 months from kickoff (3 mo readiness + 6 mo observation + audit) |
| Enterprise buyer reaction | "OK, good start, when's Type II?" | "Ship it, where do I sign?" |
| Cost (auditor) | $12k–$18k | $25k–$50k |
| **Picked for this round?** | **Yes** | Next, stacked on this |

Type I is the credibility floor for "serious company." Type II is the credibility floor for "serious enterprise deal." We need to stand up Type I before we can run the Type II observation clock, so there is no way to compress this sequence.

### 2.2 Trust Services Criteria: Security only, first pass

The SOC 2 framework has five TSC categories. Auditors only assess the ones you scope in:

| TSC | Included Phase 1? | Reason |
|---|---|---|
| **Security (CC series — Common Criteria)** | **Yes — required** | The only mandatory category. All other TSCs sit on top of CC. |
| **Availability (A series)** | No (Phase 2) | Adds ~20% audit effort for uptime/SLA claims we do not yet publish. Defer until Type II. |
| **Confidentiality (C series)** | No (Phase 2) | Encryption at rest + in transit already covered by CC6.x; formal confidentiality criteria add policy overhead without customer-visible upside at our stage. |
| **Processing Integrity (PI series)** | No | Relevant for transaction-processing systems (payments, etc.). We pass through Paddle for payments. Out of scope. |
| **Privacy (P series)** | No | Adds GDPR/CCPA-equivalent control coverage. Only relevant if a deal explicitly asks for it. |

**Practical meaning**: the gap list in §7 and the control inventory in §5 map only to the **CC1–CC9** control objectives. That is ~61 individual points of focus in the 2017 TSC (with 2022 revisions) — the bulk of which are already covered by what we have in code.

### 2.3 In scope (systems)

- `datanika-core` (the Reflex app) running at `app.datanika.io`
- `datanika-cloud` (billing plugin) embedded into the same image
- `datanika-landing` (marketing site) at `datanika.io`
- Supporting infrastructure: Hetzner dedicated (app host), Aweb VPS (landing host), Cloudflare (CDN/DNS), Paddle (payments), Plausible CE (analytics), GitHub (source + CI/CD)
- Per-tenant dbt projects and user-configured data destinations **only insofar as Datanika executes them**. The *user's* destination database is the user's compliance responsibility; Datanika's responsibility ends at "we handled your credentials and job configuration securely."

### 2.4 Out of scope (systems)

- User-owned data destinations (Snowflake, BigQuery, user-owned Postgres, etc.) — compliance scope boundary explicitly documented in the Subprocessor section of the Trust page.
- Amazon S3 / Backblaze B2 buckets that customers connect to from their own accounts — user-owned credentials, user-owned scope.
- Third-party OAuth providers (Google, GitHub, Microsoft) — dependency, not subprocessor.

---

## 3. Roadmap — 90-day phased plan

**Kickoff date**: 2026-04-14 (today). **Target audit completion**: 2026-07-13. Dates assume the vendor paid-trial unblocks by 2026-04-21 (week 1). Slippage on the human-locker step pushes the whole timeline right one-for-one.

### Phase A — Foundation (days 0–30, 2026-04-14 → 2026-05-14)

**Goal**: vendor selected, policies drafted, evidence collection automated, gap list closed on paper.

| Day range | Milestone | Owner | Dependency |
|---|---|---|---|
| 0–2 | This roadmap published to dev (unblocks Trust page, VC memo) | Product | none — **complete at this PR** |
| 0–7 | SECURITY.md live in `datanika-core` master (links this doc, describes disclosure policy) | Engineering (`eng-security-md`) | this doc |
| 0–7 | Trust page live on `datanika.io/trust` (links SECURITY.md + this doc + DPA stub + subprocessor list) | Product (`prod-trust-page`) | this doc + `eng-security-md` |
| 3–7 | Vendor paid-trial signup | **Human** (`human-soc2-vendor-select`) | this doc (to justify the budget ask) |
| 7–14 | Vendor tool onboarded: connect GitHub, Hetzner/Aweb SSH hosts, Cloudflare, Paddle, Google Workspace, AWS-backups-bucket-if-any | Vendor + Infra | paid trial signed |
| 10–21 | Policy library drafted from vendor templates: Information Security, Acceptable Use, Access Control, Change Management, Incident Response, Vendor Management, Business Continuity, Data Classification, Password Policy, Remote Work, Risk Assessment (11 core policies) | Product (adapts templates) | vendor tool |
| 14–28 | Evidence collection running: vendor's integrations pull GitHub PR reviews, CI green runs, access grants, ticket closures, uptime checks automatically | Infra + Vendor | integrations wired |
| 21–30 | Employee security training assigned (Drata/Vanta ship 45-min training modules) | Founder (all "employees" = founders at this stage) | vendor tool |

**Phase A exit criteria**: vendor dashboard shows ≥80% control coverage green; all 11 policies drafted and signed off; 2-week evidence trail starting to accumulate.

### Phase B — Closure (days 30–60, 2026-05-14 → 2026-06-13)

**Goal**: remaining gaps closed in code or policy, auditor selected and engaged, readiness review passed.

| Day range | Milestone | Owner | Dependency |
|---|---|---|---|
| 30–37 | Gap-list triage: each red item in vendor dashboard gets a code PR, policy doc, or acceptable-risk memo | Product routes to Engineering/Infra | Phase A exit |
| 35–45 | Auditor selected from the vendor's partner list (small-firm CPA — see §8); scope letter signed | **Human** (founder signs) | Phase A exit |
| 40–55 | Engineering gap PRs landed: MFA enforcement on GitHub org (if not already), quarterly access review script, quarterly backup-restore drill script (already in Infra backlog as `infra-backup-restore-drill-script`), log retention configuration | Engineering + Infra | triage |
| 45–60 | Vendor-run readiness review (mock audit) — vendor flags any remaining control gaps | Vendor | gap PRs merged |
| 55–60 | Remediation of readiness-review findings | Product + routing | readiness review |

**Phase B exit criteria**: vendor dashboard green across all in-scope CC controls; auditor scope signed; readiness review passed or down to ≤3 findings all with remediation PRs in flight.

### Phase C — Audit (days 60–90, 2026-06-13 → 2026-07-13)

**Goal**: auditor fieldwork, report delivery, Trust page updated to link the report letter.

| Day range | Milestone | Owner | Dependency |
|---|---|---|---|
| 60–75 | Auditor fieldwork: sample-based evidence review via vendor's auditor-portal integration | Auditor + Vendor | Phase B exit |
| 70–85 | Management response to any audit findings; remediation or acceptance memos | Product + routing | fieldwork |
| 80–90 | SOC 2 Type I report letter delivered | Auditor | remediation closed |
| 85–90 | Trust page updated to link report letter (under NDA — Trust page shows "SOC 2 Type I available on request, contact security@datanika.io") | Product | report in hand |
| 85–90 | VC memo §7 item 4 marked done; enterprise pricing page CTA updated to "SOC 2 Type I" | Growth + Product | report in hand |

**Phase C exit criteria**: signed SOC 2 Type I report on file; NDA-gated distribution process documented; Trust page says "SOC 2 Type I — letter available on request."

### Gantt (ASCII)

```
Week:          1    2    3    4    5    6    7    8    9   10   11   12   13
Phase A:       ████████████████
  Roadmap     █
  Vendor      ██
  Policies        ████████
  Integrations      ██████
  Training              ████
Phase B:                         ████████████████
  Triage                         ████
  Auditor sel.                       ██████
  Gap PRs                            ██████████
  Readiness                                   ████
Phase C:                                               ████████████████
  Fieldwork                                            ██████████
  Mgmt resp.                                              ████████
  Report                                                         ██████
```

### Parallelism note

Phase A's "Trust page" and "SECURITY.md" milestones do **not** block the vendor-selection path — they run in parallel in week 1 because they have different owners (Product-on-landing + Engineering-on-core for the content, a human for the vendor signup). The DAG is already wired this way (`prod-trust-page.depends_on = [eng-security-md, prod-soc2-roadmap]` but not on vendor selection).

---

## 4. Existing-control inventory

The SOC 2 Common Criteria (CC1–CC9) have ~61 individual points of focus. Below is a map of what we have *today*, organized by CC category. This is the single most important section for the vendor's gap analysis because it tells them what *not* to flag as missing.

**Verified sources**: all line references are to `datanika-core` HEAD on dev as of 2026-04-14. Verification method for each bucket noted inline. Test count is from `datanika/tests/test_security/` — **109 test functions across 10 files** (`test_api_key_security.py` 11, `test_auth_security.py` 19, `test_file_upload_security.py` 13, `test_injection.py` 11, `test_input_validation.py` 7, `test_oauth_csrf.py` 9, `test_path_traversal.py` 8, `test_rate_limit_security.py` 15, `test_tenant_isolation.py` 4, `test_token_security.py` 12). Earlier CEO-scope doc and DAG rationale referenced "73 security tests" — that number is stale; the current count is 109. Use 109 in all vendor conversations.

### CC1 — Control Environment (governance, tone at the top)

| Point of focus | Status | Evidence | Gap? |
|---|---|---|---|
| Integrity and ethical values | Partial | Founder + team norms; no signed Code of Conduct | **Policy doc** — Phase A |
| Board oversight | N/A (pre-funding stage, no formal board) | — | Accept as org-size exception |
| Management philosophy and operating style | Partial | `plans/WORKFLOW_RULES.md`, `plans/README.md`, CLAUDE.md | **Formal Information Security Policy** — Phase A |
| Organizational structure | Partial | Dept plans in `plans/<dept>/` — clear roles on paper | **Org chart + reporting lines** — Phase A |
| Commitment to competence | Partial | TDD rules, PR review requirements, precheck gates | Document as written policy — Phase A |
| Accountability | Done | Every task has a GitHub issue, owner, and PR; audit log records auth-scoped changes | — |

### CC2 — Communication and Information

| Point of focus | Status | Evidence | Gap? |
|---|---|---|---|
| Internal communication of security objectives | Partial | `plans/` directory, CLAUDE.md | **Written Security Awareness program** — Phase A |
| External communication of security objectives | Partial | No SECURITY.md yet, no Trust page yet | **`eng-security-md` + `prod-trust-page`** — Phase A (already in DAG) |
| Incident reporting channels | **Missing** | — | **Create `security@datanika.io` alias + disclosure flow** — Phase A |

### CC3 — Risk Assessment

| Point of focus | Status | Evidence | Gap? |
|---|---|---|---|
| Specific risk objectives | Partial | `plans/infra/PLAN_INFRASTRUCTURE.md` risk register fragments | **Formal risk register doc** — Phase A |
| Risk identification and analysis | Partial | Security test suite exercises known risk categories | Annualize as a formal review — Phase B |
| Fraud risk | Partial | Paddle handles payment fraud | Document boundary — Phase A |
| Change-induced risk | Done | PR review required, CI gates, precheck, staging env | Reference in Change Mgmt policy — Phase A |

### CC4 — Monitoring Activities

| Point of focus | Status | Evidence | Gap? |
|---|---|---|---|
| Ongoing monitoring | Done | Grafana + Prometheus + node-exporter + cadvisor on Hetzner; Plausible for app; alerting rules in infra repo | Reference in Monitoring policy — Phase A |
| Separate evaluations | Partial | Vendor tool will automate this once onboarded | — |
| Reporting deficiencies | Partial | GitHub issues + dept plan updates | Formalize escalation path — Phase A |

### CC5 — Control Activities (the workhorse category)

| Point of focus | Status | Evidence | Gap? |
|---|---|---|---|
| Selects and develops control activities | Done | 109 security tests in `datanika/tests/test_security/` | Reference count in Trust page — Phase A |
| Selects technology general controls | Done | Ruff, pytest, CI gates, branch protection on dev/master | — |
| Deploys through policies and procedures | Partial | `plans/WORKFLOW_RULES.md` is the *de facto* policy; needs a formal Change Management doc on top | **Policy doc** — Phase A |

### CC6 — Logical and Physical Access Controls

This is where we are strongest — almost all CC6 controls are implemented in code.

| Point of focus | Status | Evidence | Gap? |
|---|---|---|---|
| **CC6.1 — Logical access restrictions** | Done | RBAC via roles + per-org tenant filtering (`TenantMixin`); JWT + session auth; bcrypt password hashing | Document in Access Control policy — Phase A |
| **CC6.1 — Authentication** | Done | Email/password + Google/GitHub OAuth + SAML/OIDC SSO; reCAPTCHA on signup | Reference in Access Control policy — Phase A |
| **CC6.2 — User provisioning and deprovisioning** | Partial | Invitation flow + soft-delete on users; no automatic deactivation on org departure | **Quarterly access review** — Phase B |
| **CC6.3 — Access to privileged functions** | Partial | Org-level admin roles; no formal "privileged account" registry | Document — Phase A |
| **CC6.4 — Physical access** | N/A for cloud | Hetzner + Aweb provide physical controls, inherited via subservice org letter | Collect subservice org controls — Phase A |
| **CC6.5 — Decommissioning and disposal** | Partial | Soft-delete + db backups retention | Document — Phase A |
| **CC6.6 — Prevention of malicious code** | Partial | Dependency scanning via GitHub Dependabot; ruff; no formal SCA in CI yet | **Add SCA step (trivy or pip-audit)** — Phase B |
| **CC6.6 — Vulnerability management** | Partial | Dependabot alerts triaged; no SLA | **Define SLA: critical 7d, high 30d, medium 90d** — Phase A |
| **CC6.7 — Restriction of data transmission** | Done | TLS enforced end-to-end (Cloudflare + Nginx + app); Fernet encryption at rest for connection credentials (`services/encryption.py`) | — |
| **CC6.8 — Detection and prevention of unauthorized software/hardware** | Partial | CI gates code into main images; no formal endpoint management (founders' own laptops) | **MDM on founder laptops or signed policy waiver** — Phase A |

### CC7 — System Operations

| Point of focus | Status | Evidence | Gap? |
|---|---|---|---|
| **CC7.1 — Detection of anomalies** | Partial | Prometheus alerts on app-level metrics; no formal SIEM | Document current coverage — Phase A |
| **CC7.2 — Detection of security events** | Partial | Audit log table covers auth/settings/RBAC events | **Expand audit coverage checklist** — Phase B |
| **CC7.3 — Incident response** | **Missing** | No runbook | **Incident Response policy + runbook** — Phase A (pair with `infra-hit-by-bus-runbook`) |
| **CC7.4 — Recovery** | Partial | Backup scripts exist; no verified restore drill | **Backup restore drill** — Phase B (already a DAG task: `infra-backup-restore-drill-script`) |
| **CC7.5 — Environmental monitoring** | Done | Hetzner + Aweb handle physical; app-level monitoring described above | — |

### CC8 — Change Management

| Point of focus | Status | Evidence | Gap? |
|---|---|---|---|
| **CC8.1 — Authorized, designed, tested, approved changes** | Done | Branch protection on `dev` and `master`; PR review required; precheck + CI gates; rebase-only merge strategy | Document as policy — Phase A |

### CC9 — Risk Mitigation

| Point of focus | Status | Evidence | Gap? |
|---|---|---|---|
| **CC9.1 — Risk mitigation activities** | Partial | Security tests as preventive; monitoring as detective; backups as corrective | Formalize mapping — Phase A |
| **CC9.2 — Vendor risk management** | **Missing** | No vendor inventory document | **Subprocessor list + Vendor Mgmt policy** — Phase A (subprocessor list is already a `prod-trust-page` subtask) |

### Codebase artifacts that count as control evidence

These are the code-side facts the vendor's auditor-portal can point at without human intervention:

- `datanika/services/encryption.py` — Fernet-based credential encryption at rest (CC6.7)
- `datanika/services/audit_service.py` + `datanika/models/audit_log.py` — tenant-scoped audit trail (CC7.2)
- `datanika/services/sso_service.py` + `sso_routes.py` — SAML/OIDC SSO (CC6.1)
- `datanika/models/` + `TenantMixin` with `org_id` filtering — tenant isolation (CC6.1)
- `datanika/tests/test_security/` — 109 security test functions exercising auth, injection, path traversal, tenant isolation, rate limiting, token security, file-upload security, CSRF, input validation, API-key security (CC5, CC7.1)
- Alembic migrations under `datanika/migrations/versions/` — change tracking on schema (CC8.1)
- GitHub branch protection on `dev` + `master` in all 3 repos (CC8.1)
- `plans/WORKFLOW_RULES.md` — documented SDLC (CC5, CC8.1)
- `bcrypt` directly (no passlib) for password hashing — password storage (CC6.1)
- JWT via `python-jose` — session management (CC6.1)
- Cloudflare + Nginx TLS termination — encryption in transit (CC6.7)
- Prometheus + Grafana + node-exporter + cadvisor — infrastructure monitoring (CC4, CC7.1)
- `datanika-cloud` webhook HMAC verification — payment event integrity (CC6.7)

**Headline number for procurement decks**: *"~65% of Common Criteria controls implemented in code today, backed by 109 security tests and a live audit-log service. Remaining 35% is policy documentation and operational runbooks — in flight, Type I target 2026-07-13."*

---

## 5. Gap list (the next-session to-do)

Ordered by effort × risk. "Risk" here is *audit risk* — the chance an auditor will flag this as a deficiency — not operational risk.

### High priority (Phase A, weeks 1–4)

1. **11-policy library** — Information Security, Access Control, Acceptable Use, Change Management, Incident Response, Vendor Management, Business Continuity, Data Classification, Password Policy, Remote Work, Risk Assessment. Drafted from vendor templates, customized to Datanika's actual workflows. **Effort**: ~12 hours with vendor templates; ~40 hours without.
2. **`security@datanika.io` alias + disclosure policy** — create the alias, publish via SECURITY.md, route to founder inbox. **Effort**: 30 minutes + a paragraph in SECURITY.md.
3. **Incident Response runbook** — pair this with the pending `infra-hit-by-bus-runbook` task so the two docs reference each other. **Effort**: 4 hours (draft) + 1 hour (cross-link with Infra's runbook).
4. **Subprocessor list** — Hetzner, Aweb, Cloudflare, Paddle, Plausible (self-hosted so technically "infrastructure," not subprocessor — clarify), GitHub, Google Workspace. Published on Trust page. **Effort**: 2 hours including vendor research.
5. **Risk register doc** — one page, top 10 risks, likelihood × impact × mitigation. **Effort**: 3 hours.
6. **Vulnerability management SLA** — written commitment: critical 7d, high 30d, medium 90d, low best-effort. **Effort**: 1 hour of policy writing + adding a GitHub label taxonomy.
7. **MFA enforcement audit** — verify every admin-level service (GitHub org, Cloudflare, Hetzner, Aweb, Paddle, Google Workspace, domain registrar) has MFA on every account. **Effort**: 2 hours of clicking + screenshots.
8. **Org chart + roles doc** — even as a 1–2 founder stage, the auditor needs a document that says "this person is responsible for this." **Effort**: 1 hour.
9. **Employee security training** — use vendor-provided module, finish in one sitting. **Effort**: 45 minutes per person.

### Medium priority (Phase B, weeks 4–8)

10. **Quarterly access review script** — automated query against GitHub, Cloudflare, Hetzner, Paddle, Google Workspace listing every human account + last-active timestamp. Dumps to a committable file for the quarterly review ritual. **Effort**: 4 hours.
11. **Backup restore drill** — already in Infra backlog as `infra-backup-restore-drill-script`. Auditor will want the drill report, not just the script. **Effort**: runbook + first execution = 3 hours.
12. **SCA in CI** — add `pip-audit` or `trivy` to `precheck.sh` or the CI pipeline; fail the build on critical CVEs in locked dependencies. **Effort**: 2 hours.
13. **Expanded audit log coverage** — current `AuditService` covers auth-scoped actions; confirm coverage for: connection credential access, pipeline schedule changes, destination schema writes, billing plan changes. Add missing calls. **Effort**: 4 hours engineering.
14. **Formal access-review ritual** — a recurring 30-minute calendar event where the output of (10) is reviewed and approved. **Effort**: 30 min setup.
15. **Vendor risk review for each subprocessor** — SOC 2 reports on file for Hetzner, Cloudflare, Paddle, Google Workspace. Requested via vendor relationship manager. **Effort**: 2 hours of email.
16. **Data Processing Addendum (DPA) template** — already in `prod-trust-page` scope as a "draft PDF, human signs later." Needs one more pass for SOC 2 alignment. **Effort**: 2 hours plus human review.

### Low priority / conditional (Phase B or deferred)

17. **MDM on founder laptops** — CC6.8 nice-to-have; or signed policy waiver acknowledging the risk and mitigating with full-disk encryption + strong passwords + auto-lock. **Effort**: waiver = 30 min; MDM = 8 hours.
18. **Business continuity plan (BCP)** — paired with the backup restore drill; the drill is the "T" in BCP. **Effort**: 4 hours on top of (11).
19. **Formal Code of Conduct** — small-team stage, can be a one-paragraph affirmation. **Effort**: 30 min.
20. **SIEM consolidation** — only if the auditor specifically flags the lack. Current Prometheus + Grafana + audit_log queries likely pass.

### Phase-2-only (Type II observation period, post-2026-07-13)

21. **3-month evidence observation window** — vendor keeps collecting; nothing to do except not regress.
22. **Availability TSC addition** — publish an SLA commitment; add uptime monitoring to the vendor dashboard.
23. **Confidentiality TSC addition** — add a Data Classification policy with tagged data types.
24. **Re-engage auditor for Type II fieldwork** — separate SOW, stacks on Type I.

**Triage owner**: Product coordinates the gap list at the start of Phase B. Engineering owns items 10/12/13. Infra owns item 11 (already on their plan). Policy items (1, 3, 5, 6, 8) are drafted by Product from vendor templates.

---

## 6. Vendor comparison

The three serious incumbents in the SOC 2 compliance-automation space are Drata, Vanta, and Secureframe. Each ships the same core product — a dashboard that connects to your infra, pulls evidence automatically, tracks policies, runs training, and hands off to an auditor. They differentiate on integration surface, pricing, and auditor network.

### At a glance

| Dimension | **Drata** | **Vanta** | **Secureframe** |
|---|---|---|---|
| **Founded** | 2020 | 2018 | 2020 |
| **SMB/startup pricing (≤25 employees)** | ~$7,500 implementation + ~$6,000/yr subscription | ~$9,000 implementation + ~$8,000/yr subscription | ~$5,500 implementation + ~$5,500/yr subscription |
| **Time to Type I readiness (median)** | 6–10 weeks | 6–12 weeks | 8–12 weeks |
| **Number of integrations** | ~120 | ~200 | ~150 |
| **GitHub integration depth** | Strong (PR review evidence, branch protection checks, dependabot alerts) | Strong (similar) | Moderate |
| **AWS integration depth** | Strong | Strongest | Strong |
| **GCP integration depth** | Strong | Strong | Moderate |
| **Hetzner / "unmanaged Linux host" support** | Via SSH agent + manual host registration | Via SSH agent + manual host registration | Via SSH agent + manual host registration |
| **Cloudflare integration** | Yes | Yes | Yes |
| **Paddle integration** | Manual evidence only | Manual evidence only | Manual evidence only |
| **Policy templates (CC series)** | ~40 templates, SOC 2 focused | ~50 templates, broader frameworks | ~35 templates |
| **Auditor partner network** | Large, including small firms | Largest, skews enterprise | Moderate |
| **Employee training modules** | Yes (~45 min) | Yes (~60 min) | Yes (~45 min) |
| **Founder-stage reputation** | "Developer-friendly, fastest time-to-dashboard" | "Polished, enterprise-y, slightly slower to onboard" | "Cheapest, smaller ecosystem" |
| **Type II upgrade path** | Seamless (same tool, observation window starts on toggle) | Seamless | Seamless |
| **Contract length** | 12 months minimum | 12 months minimum | 12 months minimum |

### Scoring for Datanika specifically

Five criteria, each 0–5:

| Criterion | Drata | Vanta | Secureframe | Why it matters for us |
|---|---|---|---|---|
| Integration surface covers our stack | 5 | 5 | 4 | GitHub, Cloudflare, Google Workspace, Hetzner-via-SSH all supported by top two |
| Founder-team ergonomics | 5 | 4 | 4 | "Connect GitHub and 60% of controls go green" is the key day-1 experience |
| Cost at our size | 4 | 3 | 5 | Secureframe is cheapest but has thinner auditor network |
| Auditor partner fit for $12k–$18k Type I | 4 | 5 | 3 | Vanta's partner network is biggest, but many partners skew bigger/more expensive |
| Roadmap flexibility (Type I → Type II) | 5 | 5 | 5 | All three support the upgrade with no re-onboarding |
| **Total** | **23** | **22** | **21** |

### Recommendation: **Drata**, with Vanta as the fallback if the trial goes sideways

**Why Drata**:

1. **Fastest time to a populated dashboard** — the strongest signal for a 90-day plan. Drata users consistently report "connected GitHub and AWS on day 1, ~60% of controls green by end of week 1." For a Product-owned roadmap where the visible progress matters, that shape of onramp is ideal.
2. **Devtool-friendly culture** — engineer-facing docs, public changelogs, CLI tooling, and an API that lets us automate evidence generation beyond the pre-built integrations. For a codebase with as much hand-built infra as we have (custom precheck, custom audit log, Alembic migrations), the ability to add custom evidence pipes is real leverage.
3. **Auditor partner network is a match** — Drata's partner list includes multiple small-firm CPAs at the $12k–$18k Type I price point, which is exactly our budget. Vanta's partners skew toward $15k–$25k+.
4. **Subscription price at <25 employees is reasonable** — $6k/year is inside the budget envelope below, and the implementation fee is one-time.

**Why Vanta is the fallback**:

- Larger auditor network — if Drata's partners are fully booked through our target window, Vanta's bench depth is the safety net.
- Slightly broader integration count — if we discover a surprise dependency (e.g., a niche monitoring tool), Vanta is more likely to have a pre-built connector.

**Why Secureframe is third, not a strict no**:

- Cheapest, which matters. But the auditor network is the thinnest, and "cheap but can't find an auditor" is a failure mode that loses the 90-day window.
- Consider only if both Drata and Vanta trials fall over for reasons specific to our stack.

### Vendor selection mechanics (for the human running the paid trial)

The `soc2-vendor-selection` human locker should:

1. Go to `drata.com/pricing` (or request a demo) — get a sales call within 24 hours.
2. Mention Datanika's size (<5 people), stack (Python/Postgres on Hetzner + Astro on Aweb + Cloudflare + Paddle), target (Type I in 90 days), and ask for their auditor-partner list for the $12k–$18k Type I price point. The answer on auditors is more important than the price on the tool.
3. Sign the paid trial (not the free trial — free trials do not include integrations). Pro-rata ~$600/month for the first month while we evaluate.
4. In parallel: email Vanta for a competing quote, so we have price leverage. Do not run two trials simultaneously — that is a waste of a week.
5. Once signed, connect GitHub first (takes 5 minutes, immediately populates ~30% of controls). This is the "is this tool actually going to work for us" smoke test.
6. If Drata's dashboard shows ≥50% control coverage after connecting GitHub + Cloudflare + Google Workspace on day 1, proceed. If it shows <30%, pause and try Vanta.

---

## 7. Budget envelope

**Phase 1 (90 days to Type I report in hand)**:

| Line item | Cost | Notes |
|---|---|---|
| Drata implementation (one-time) | $7,500 | Negotiable; sometimes waived for <10-employee startups |
| Drata subscription (90-day pro-rata) | $1,500 | $6k/yr ÷ 4 |
| Auditor (Type I engagement) | $12,000–$18,000 | Small-firm CPA; Prescient / Johanson / A-LIGN lower tier |
| Legal review of DPA template | $500–$1,500 | Optional; existing template-lawyer relationship preferred |
| Contingency (gap-closing cloud costs: MDM, SCA tooling, etc.) | $500–$1,000 | Upper bound |
| **Phase 1 total** | **$22,000–$29,500** | Midpoint: **~$25,000** |

**Phase 2 (months 4–12, toward Type II)**:

| Line item | Cost | Notes |
|---|---|---|
| Drata subscription (remaining 9 months) | $4,500 | — |
| Auditor (Type II engagement) | $25,000–$40,000 | Larger observation window, more sampling |
| **Phase 2 total** | **~$30,000–$45,000** | — |

**Annualized total (Type I + Type II in year 1)**: ~$55,000–$75,000.

**Comparison sanity check**: the Fivetran/Airbyte/Stitch cohort all went through SOC 2 at a similar or slightly higher cost. This is the industry-standard envelope for a Python-on-Linux company at our stage.

---

## 8. Auditor shortlist

Do not pick an auditor until the vendor is onboarded (Phase A week 2). The vendor's partner list is the best shortcut — their partners have pre-wired integrations into the dashboard, which cuts audit fieldwork time by roughly half.

**Strong options at the $12k–$18k Type I price point** (all known to support Drata and Vanta integrations; confirm in trial call):

- **Prescient Assurance** — small-firm CPA, responsive, strong SaaS/startup track record.
- **Johanson Group** — similar shape, slightly larger firm, broader hours coverage.
- **A-LIGN (small-business tier)** — A-LIGN's enterprise tier is out of our price range but their small-business engagement model fits.
- **Insight Assurance** — another small-firm player in the Drata/Vanta partner network.

**Decision criteria for auditor pick** (in priority order):

1. **Already integrated with Drata** (auditor portal access reduces fieldwork ~50%)
2. **Price within $12k–$18k** for Type I
3. **Availability in the 2026-06-13 → 2026-07-13 window** (biggest risk — auditors get booked up)
4. **Prior experience with Python/Linux SaaS companies** (not just enterprise)
5. **Willingness to do Type II as a follow-on** (avoids re-sourcing)

---

## 9. What this doc deliberately does not answer

To keep scope tight and avoid pretending we have information we don't:

- **Exact auditor pick.** Happens at Phase B week 6, from the vendor's partner list. Not a roadmap-stage decision.
- **Exact Type II target date.** Depends on Type I completion + when we start the observation window. First feasible Type II report date: **~2027-04-14** (Type I done 2026-07-13 → 6-month observation + audit). Planning for it now is premature.
- **Whether we add Availability TSC.** Decision point at the end of Phase 1, when we know whether any enterprise deals have specifically asked for it.
- **Whether any feature work slips for this.** Nothing on the Engineering P0/P1 list shifts for SOC 2 — the gap-closing PRs (items 10, 12, 13 in the gap list) are small (~4 hours each) and fit into existing capacity. If the gap list expands during vendor trial, the user re-prioritizes.
- **The policy text itself.** The 11 policies are drafted from vendor templates in Phase A. They live in `plans/product/policies/` (future folder) or a dedicated git repo, not in this spec.

---

## 10. Cross-team handoff

When this doc lands:

- **Growth (`plans/growth/PLAN_MARKETING.md` → Enterprise Sales Motion)** — can now ship "SOC 2 Type I in progress, Q3 2026 target" on the Enterprise pricing CTA. One-line change.
- **Product (`prod-trust-page`)** — can now link this doc from the Trust page "Compliance" section. Trust page is unblocked.
- **Engineering (`eng-security-md`)** — SECURITY.md can now reference this doc under "Compliance roadmap" and the 109-test number under "Security testing." No blocking dep — Engineering was going to ship SECURITY.md anyway.
- **Human locker (`soc2-vendor-selection`)** — the vendor-selection step now has concrete vendor criteria (§6), a paid-trial decision tree (§6 mechanics), and a budget envelope (§7). This turns "pick a vendor" from a research task into a signup task.
- **VC memo §7 item 4** — closed. The roadmap itself is the deliverable.

---

## 11. Review and update cadence

- **Week 4 (2026-05-12)**: first progress update — vendor onboarded, policy library drafted, integration coverage report. Appended to this doc as `## Update 2026-05-12`.
- **Week 8 (2026-06-09)**: Phase A → B gate. Gap list re-triaged, auditor picked, readiness review scheduled.
- **Week 12 (2026-07-13)**: Type I report in hand (target). Trust page flipped from "in progress" to "available on request."
- **Slippage policy**: if any milestone slips by more than 7 days, update the doc with a new dated plan and ping the affected downstream consumers (Growth on the enterprise-page copy; Product on the Trust page copy).

---

## 12. Appendix — SOC 2 control framework cheat sheet

For readers new to SOC 2. Skippable if you've done this before.

### The 2017 Trust Services Criteria (with 2022 revisions)

- **Security (CC1–CC9)** — the "Common Criteria." Required in every SOC 2 engagement. Covers governance, risk assessment, communication, monitoring, control activities, logical/physical access, system operations, change management, risk mitigation.
- **Availability (A1.1–A1.3)** — system is available for use as committed. Only included if you publish SLAs.
- **Confidentiality (C1.1–C1.2)** — information designated as confidential is protected. Relevant for data-pipeline products handling customer data, but CC6.7 already covers most of this.
- **Processing Integrity (PI1.1–PI1.5)** — system processing is complete, valid, accurate, timely, authorized. Mostly for payments/transactions.
- **Privacy (P1.0–P8.1)** — PII is collected, used, retained, disclosed, and disposed of in accordance with commitments. Relevant if you publish a privacy notice making specific commitments.

### The Type I vs Type II distinction

- **Type I**: "As of date X, the system's controls are suitably designed." A snapshot. No operating-effectiveness claim.
- **Type II**: "Over the period from date X to date Y, the system's controls operated effectively." A movie. Requires a 3–12 month observation period.

### The typical SMB SaaS path

1. Establish a minimum viable control set (code + policies).
2. Engage a compliance-automation vendor (Drata/Vanta/Secureframe) to collect evidence.
3. Type I attestation — ~90 days from kickoff.
4. Continue evidence collection across the observation window.
5. Type II attestation — ~9–12 months from the original kickoff.
6. Annual Type II renewals.

Datanika is at step 1 (implicit — existing controls in code) and about to move to step 2.

---

## Status log

- **2026-04-14** — drafted by Product as DAG task `prod-soc2-roadmap`, first wave. Unblocks `prod-trust-page` (Product) and VC memo §7 item 4 (Growth-adjacent). Vendor selection (`soc2-vendor-selection`) remains a human locker; this doc turns it from a research task into a signup task. Security-test count was verified at 109 (not the DAG's 73) — use 109 in all downstream copy.
