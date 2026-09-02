# SPEC — PII separation, account erasure, org deletion, email change

**Author**: Product · **Date**: 2026-08-30 · **Status**: contract, ready for Engineering
**Tracking**: [core#655](https://github.com/datanika-io/datanika-core/issues/655)
**Implementation**: Engineering (core + a small cloud change). Product owns this spec and the acceptance criteria.
**Verified against**: `origin/dev` @ `11ab292` for the **2026-09-02** amendment (§0, D5 steps 1a/7, D14.4 item 3, criteria 0e / 8d / 8e / 17c / 17d / 17f); `cfbb0c7` for the 2026-08-30 night amendment; `ef76067` for the earliest sections. Plus `origin/main` @ the live `datanika.io/privacy` and `/trust` pages, and `plans/infra/scripts/backup-offsite.sh`.
⚠️ **Nothing of this feature is implemented yet** — re-measured 2026-09-02 on `origin/dev`: `PII_PAYLOAD_KEYS` and `redact_pii_payload` have **0 occurrences** anywhere in `datanika/`, and there is no `erase_user`, `delete_user`, `delete_account`, `delete_org` or `change_email` on any surface. The spec, the executable guard (`tests/test_services/test_audit_pii_redaction.py`) and this amendment are the whole of what exists.
**Founder architecture decision, 2026-08-30**: *"Separate PII from internal data. GDPR-sensitive info goes into separate tables with a FK to our internal IDs. Soft-delete users, hard-delete the sensitive info."* This spec implements that and nothing else; where it makes a call the decision did not cover, it is marked as a decision.
**Founder addition, 2026-08-30 night**: *"On deletion, flush the PII properties in `audit_logs` for that user"*, and *"GDPR allows deletion in 30 days or so, so maybe it should be a procedure launched once a week on weekends that deletes preliminarily prepared users from all the PII tables."* Answered in **D12.4** (kept, re-scoped) and **D14** (the phase boundary moves; the architecture does not).

> ### 🆕 Read §0 first (2026-09-02) — it fixes two steps, and both let a soft delete stand in for an erasure
>
> **§0 states the soft-delete / hard-delete split as one findable decision** instead of leaving it
> implicit across D1, D5, D7 and D14.1 — and writing it down surfaced **two gaps in D5 that four
> sessions of review had missed**. Both are now fixed in the step table: a **sole-member org is
> soft-deleted still carrying the erased person's name** (§0.1 — `organizations.name` holds a live
> `users.full_name` in 5 of 5 prod rows), and **erasure ships in release N while N is still
> dual-writing the legacy columns**, so every erasure until N+2 deleted the copy and left the original
> (§0.2). Criteria **8d** and **8e** are the tests; both are written so that *every other erasure
> criterion passes on the broken implementation.*
>
> 🆕 **Two other things changed under this spec and neither is in its body's history:**
> **[core#726] is DONE** — the value-level migration round-trip and the `one_way` marker shipped in
> [PR #874]; criterion **0e** says what that means for this chain, and one consequence is that the
> N migration turns an existing suite **red** until its seeder covers the four new tables.
> **[core#895]** invalidates D14.4's proposed completion-record mechanism — a Prometheus counter
> incremented in the Celery worker is never scraped (17c).
>
> ### ⚠️ Read §2c, then §2b and D12–D14, before writing the expand migration
>
> 🆕 **§2c (census, 2026-08-31) is the newest and it deletes a step.** *"116 of 116 populated"* was a
> `count()` counting the JSON literal `null`: **30 / 18** rows hold an object and **zero** hold an
> email. **There is no audit-payload backfill.** The design is unaffected — the five call sites that
> would write an address are live code and fire on the first invitation — but with the backfill gone
> **D12.2's guard is the only thing that can ever notice a redactor regression**, and a guard written
> as an assertion over the table would pass against a no-op. §2c carries the revised acceptance
> criteria. ⚠️ **The `user_pii` backfill is a different backfill and it survives** — `users.email` is
> populated in 5 of 5.
>
> **§2b** is still the section that changes the *answer*: **nothing reads `old_values`/`new_values`**,
> so the forensic value the key-level/blanket trade-off was protecting does not currently exist, and
> neither does any signal that would catch a redactor bug. **D12** decides redaction (both mechanisms,
> key-level, and **not** the key set that was proposed — it contains no PII key). **D13** is the only
> part with a deadline. **D14** moves the founder's phase boundary and shows why no `/privacy` change
> is needed. **Release count is still four** — it is set by blue/green code overlap, not by row count
> (§2c).

---

## 0. 🆕 Is this a soft delete or an erasure? Both, on different objects — and the split is the whole design

*(Added 2026-09-02. The question was put to Product as *"soft-delete is our pattern, and a soft delete
is not a GDPR erasure — decide explicitly which this is."* It is the right question, the answer was
already implicit across D1, D5, D7 and D14.1, and being implicit is what let **two gaps** survive four
sessions of review. Both are named below and both are now fixed in D5.)*

**The decision, in one line:** *a row that identifies a **person** is hard-deleted; a row that
identifies a **record** is soft-deleted. Nothing personal is ever soft-deleted, and nothing structural
is ever hard-deleted.*

| Object | Treatment | Why |
|---|---|---|
| `user_pii`, `invitation_pii`, `notification_channel_pii` rows | **HARD delete** | they *are* the personal data. D1 deliberately withholds `TimestampMixin` from these tables so a `deleted_at` cannot exist to be mistaken for erasure |
| `api_keys`, `password_reset_tokens`, `oauth_grants`, `oauth_tokens` | **HARD delete** | live credentials. A soft-deleted API key that still authenticates is a backdoor (D5 step 2) |
| `users` row (the integer identity) | **SOFT delete** | after N+2 it carries no personal data at all — an anonymous integer holding FK integrity for audit rows and memberships |
| `memberships`, `organizations`, connections/pipelines/uploads/… | **SOFT delete** | records, not people. Org-scoped and person-free once D4/D5 step 7 have run |
| `audit_logs` rows | **kept, never deleted** | append-only behavioural trail pointing at an integer. D11 is what keeps a person out of it |

🚨 **The failure mode this section exists to prevent: a soft delete standing in for an erasure because
the personal datum is sitting on the soft-deleted row.** A soft-deleted row is still a row in Postgres.
`deleted_at` hides it from the application and from nobody else — not from `pg_dump`, not from a
backup, not from a regulator, not from anyone with SQL access. **Two places in this spec had exactly
that shape.** Neither was a wrong decision; both were steps that were never written down, and in both
cases §7 criterion 10 would have caught them *after* implementation, which is the expensive end.

### 0.1 · Gap A — a sole-member org is soft-deleted **carrying the erased person's name**

D5 step 7 renames *"each **surviving** org"* whose `name` or `slug` still contains the erased name.
D5 step 6 sends a sole-member org to D6, which **soft-deletes** it. So the org that is deleted
*because* it belonged only to the erased person is the one org the rename never reaches — and by §2c's
own measurement, **`organizations.name` holds a live `users.full_name` in 5 of 5 production rows and
`.slug` in 5 of 5**. The erasure would leave the person's name in a row it had just "deleted".

**Fix (D5 step 7, rewritten): rename *every* org the user was a member of, including the ones being
deleted, and rename **before** the soft delete.** Renaming after is not equivalent: an
`UPDATE … WHERE deleted_at IS NULL` — the shape every org-scoped query in this codebase uses — would
skip it.

### 0.2 · Gap B — erasure ships in release **N**, while N is still dual-writing the legacy columns

This is the more serious of the two, because it is invisible for exactly as long as the expand/contract
chain takes to finish, and §8 step 5 already predicted its shape without connecting it to `erase_user`:
*"the legacy columns keep accumulating personal data that the erasure sweep does not clear."*

Walk it. §4 puts **erasure, org deletion and email change in release N**. N's code **dual-writes**
`users.email` / `users.full_name` / `invitations.email` alongside the new PII tables, and the legacy
columns are not dropped until **N+2**. D5 step 1 deletes the `user_pii` row and nothing else. So an
erasure performed at any point in N or N+1 — which is *every* erasure, until the chain finishes —
deletes the copy and leaves the original, in the very column the data was extracted from.

**Every downstream check still passes**, which is why this needs to be a step rather than a note:
`get_user_by_email` reads through the join and returns `None`; login is structurally impossible; the
UI shows nothing; criterion 8 (0 rows in `user_pii`) is satisfied. Only criterion 10 — a grep across
all 26 tables — can see it, and criterion 10 runs on prod after promotion, two releases late.

**Fix (D5 gains step 1a):** in N and N+1, `erase_user` **also NULLs the legacy columns** it has already
copied — `users.email`, `users.full_name`, `users.oauth_provider_id`, `invitations.email`,
`invitations.token`. ⚠️ **This is possible only because N drops their NOT NULL constraints** (§4), which
that migration does for a different reason. The step is **deleted in N+2**, when the columns go.

> 🔑 **The general form, worth carrying past this spec:** *an expand/contract chain has a window in
> which the same datum exists twice, and any deletion written against the new shape silently spares the
> old one.* The window is not an edge case — it is the whole point of the pattern, and it is where every
> feature that deletes must be written twice. Ask of any deletion landing in an expand release: **what
> is the second copy, and who removes it?**

---

## 1. The gap, in one line

There is no account deletion, no org deletion and no email change on any surface — and the personal
data that erasure would have to reach is spread across **more tables than anyone has yet counted
correctly**. `plans/product/AUDIT_LAUNCH_READINESS.md` finding B6 has the absence evidence; this spec
is about what to build.

⚠️ **This line used to assert "7 tables in core and 2 in cloud". It is deliberately no longer a
number.** The count has been revised three times in one evening by three different methods (§2a), it
did not match §2's own table when written, and the gate in §2a means it is not final until every
candidate column has a production non-null count. **A headline figure that the body contradicts is
worse than no figure** — read §2 and §2a, which carry per-column evidence.

## 2. ⚠️ The surface is larger than the routing measurement

The coordinator measured **3 tables / 4 columns** (`users.email`, `users.full_name`,
`invitations.email`, `audit_logs.ip_address`) and asked for it to be re-derived rather than trusted.
Re-derived it is wrong in both directions: **four columns are missing, and one of the four named holds
no data.** Every row below was read on `origin/dev` and confirmed by a second grep.

| # | Column | Why it is personal data | Evidence |
|---|---|---|---|
| 1 | `users.email` | direct identifier | `models/user.py:32` — `String(320)`, NOT NULL, **unique** |
| 2 | `users.full_name` | a natural person's name | `models/user.py:34` |
| 3 | 🆕 **`users.oauth_provider_id`** | OAuth: the provider subject (a pseudonymous identifier, still Art. 4(1)). **SSO: the email address, verbatim** | `models/user.py:38`; `services/sso_routes.py:260` passes `oauth_provider_id=email` |
| 4 | `invitations.email` | address of a person who may never become a user | `models/invitation.py:24` |
| 5 | 🆕 **`invitations.token`** | a `String(500)` **plaintext JWT whose payload contains `{"email": <invitee>}`** — `base64 -d` reads it out of any `pg_dump` | minted at `services/invitation_service.py:58` via `create_email_verification_token(user_id=…, email=email, …)`, `services/auth.py:70-81` |
| 6 | 🆕 **`notification_channels.config`** (JSON) | `{"email": …}` for EMAIL channels, `{"chat_id": …}` for Telegram — **in the same column as the Slack webhook URL and the Telegram bot token** | written `ui/state/notification_state.py:80-90`, read `services/notification_service.py:143` |
| 7 | 🆕 **`organizations.name` and `organizations.slug`** | **every signup writes the user's name into both** | `ui/state/auth_state.py:286-288` (password signup) *and* `services/user_service.py:475-477` (OAuth/SSO): `name = f"{full_name}'s Org"`, `slug = f"{slugify(full_name)}-{user.id}"` |
| 8 | ~~`audit_logs.ip_address`~~ | ⚠️ **SUPERSEDED — see §2a, then §2c.** Empty in **0 of 117** prod rows and deferred to [core#670]. The real PII risk in this table is **`old_values` / `new_values`** — ⚠️ **not because they hold personal data today (§2c: they hold none) but because five live call sites can put it there.** D11 stops them carrying it instead of extracting them to a sidecar. | `models/audit_log.py:33` |
| 9 | cloud `subscriptions.paddle_customer_id` | `ctm_…` resolves to a named, addressed billing person inside Paddle | `datanika_cloud/billing/models.py:81` |
| 10 | cloud `subscriptions.card_last_four` (+ `card_brand`) | financial identifier in combination | `datanika_cloud/billing/models.py:101-102` |

⚠️ **`audit_logs.ip_address` has never held a value.** The column, the `AuditService.log_action(...,
ip_address=None)` parameter and the rendered table column (`ui/pages/audit_logs.py:19`) all exist, and
**all three `log_action` call sites in the repo omit it** (`ui/state/auth_state.py:224`, `:374`,
`ui/state/base_state.py:65`). It is NULL in every row in production. A `services/client_ip.py` helper
exists and is wired to nothing here. That is another instance of the audit's *"machinery exists, entry
point does not"* pattern — filed separately, **not** fixed by this spec (§9).

**Two findings are worth more than their line in the table.** #7 means erasing a user's name from
`users` leaves it in an org name *and in a unique, URL-bearing slug*. #5 means our own repo already
knows better: `models/password_reset.py:13-20` documents why reset tokens are stored as SHA-256
precisely so a `pg_dump` yields nothing — and `invitations` does the opposite, in the same database,
with an email inside.

**Deliberately not in scope, and the boundary matters:**

| Category | Examples | Why it is not PII extraction |
|---|---|---|
| **Secrets** | `connections.config_encrypted`, `oauth_grants.encrypted_api_key`, `sso_configs.oidc_client_secret_encrypted`, Slack webhook URLs, Telegram bot tokens | Fernet-encrypted org property, not personal data. **Folding these in makes the deletion contract wrong in both directions**: it implies erasure must decrypt secrets (it must not), and it implies one member leaving destroys an org's pipelines (it must not). Founder instruction, and correct. |
| **Legal-retention records** | cloud `subscriptions.*`, `charges.*`, `usage_ledger.*` | `datanika.io/privacy` §6 already promises *"Billing records are retained for 7 years as required by tax law."* These outlive an erasure request **by law**, and the confirmation dialog must say so (D7). |
| **Third-party data inside customer pipelines** | `runs.logs`, `runs.error_message`, warehouse tables | Our customer is the controller for that data; we are the processor. A different contract, a different retention rule, not this spec. |

## 2a. ⚠️ CORRECTION 2026-08-30 late — the audit-log design was wrong, and the list is not final

**Provenance**: [core#676], filed by Growth against production. `audit_logs.ip_address` holds a value
in **0 of 117** rows; `old_values` and `new_values` were not in the table above at all.
**The one audit column I listed is the empty one; the two that carry every payload were missing.**

> 🆕 **§2c corrects the second half of this paragraph.** It used to read *"`old_values` and
> `new_values` hold one in 116 of 116"*. They do not, and never did: `count()` on a `json` column
> counts the literal `null` as a value. **30 / 18 rows hold an object and zero hold an email.** The
> *design* finding below survives intact — the columns are still where a `users` update would put an
> address — but every sentence in this section that treats them as *already* holding personal data is
> superseded by §2c. Read §2c before acting on any number here.

This is the **third** correction to this list in one evening, each by a different method — the
coordinator grepped field names (3 tables / 4 columns), I read semantics (7 / 10), Growth counted rows
in production. Each pass found what the previous one could not see. **Treat the surface as unproven,
not as patched.** *(A fourth followed on 2026-08-31, §2c, and it corrected the counting method rather
than the list.)*

### The finding is a design break, not two table rows

`old_values` / `new_values` are `JSON` diffs of a changed record, so what they contain depends on what
the caller put there. Enumerating every call site, **five write personal data, and four of those write
an email address belonging to _somebody other than the row's author_**:

| call site | payload | whose data |
|---|---|---|
| `ui/state/settings_state.py:201` and `:240` | `{"email": self.invite_email, "role": …}` | the **invitee's** address (`:201` invite, `:240` add-existing-user) |
| `ui/state/settings_state.py:304` (built `:294`) | `{"email": member_info.email, "role": …}` | the **removed member's** address |
| `ui/state/settings_state.py:334` (built `:331`) | `{"email": inv_info.email, "role": …}` | the **cancelled invitee's** address |
| `ui/state/settings_state.py:146-147` | `{"name": org_name, "slug": org_slug}` | the owner's **full name**, by §2 finding #7 |

⚠️ **Line numbers re-derived at `cfbb0c7` (2026-08-30 night). They had drifted by ~22 lines from the
`:179/:218/:282/:312/:124` this table carried when written, in under a day** — which is the argument
for D12.2's derived guard rather than a positional one, made by this table against itself. The
**count** also moved: 34 payload-passing call sites now, not 37; the *four* PII sites and the *three*
third-party addresses are unchanged, and those are the load-bearing figures.

⚠️ Two of the four have **no `resource_id`** (`:201`, `:240`), so D11's *"store the internal ID
instead"* needs one captured there: `create_invitation` returns the `Invitation` at `:191`, and
`_add_existing_user` already holds `user` at `:229`. This is the only call site work D11 needs that
is not a straight substitution.

**The arithmetic, shown so it can be audited rather than trusted** (`git grep -nE "old_values=|new_values=" -- datanika/`, at `cfbb0c7`, excluding `audit_service.py`'s own signature):

| | |
|---|---|
| matching lines | **34** |
| − `base_state.py:72-73` — the `_audit` helper *forwarding* the payload, not a call site | −2 |
| − two call sites that span two lines each (`settings_state.py:146`+`:147`, `:274`+`:275`) | −2 |
| **= distinct call sites passing a payload** | **30** |
| of which write personal data (`:201`, `:240`, `:304`, `:334`, `:146-147`) | **5** |
| **= remaining, carrying user-chosen labels** | **25** |

Those 25 carry connection / pipeline / schedule / upload names, types, commands and cron strings — not
systematically personal data. ⚠️ `notification_channels.config` is **not** audited, so the address in
§2 row 6 does not also land here.

**Why this breaks D1 as written.** A 1:1 `audit_log_pii` sidecar keyed on `audit_log_id` models *"this
audit row has a personal attribute."* That is not the shape. Here the personal data is **a reference to
a different person, embedded in a diff, on a row authored by somebody else.** Erasing user X would mean
scanning JSON payloads across rows belonging to other actors and other resources — unbounded, and it
must be re-solved for every audit call site anyone adds later.

### D11 · Audit diffs store internal IDs, not personal data

**This is the founder's own architecture applied to the audit log** — *"GDPR-sensitive info goes into
separate tables with a FK to our internal IDs."* The audit log should hold the FK.

- The five call sites above (four table rows) change to identifiers: `{"membership_id": …, "role": …}`,
  `{"invitation_id": …, "role": …}`, `{"org_id": …}`. The address is then resolvable through
  `invitation_pii` / `user_pii` **while those rows exist**, and unresolvable once erased. Erasure
  becomes automatic — nothing sweeps `audit_logs` at all, and the security trail stays complete.
- **A guard test, derived not enumerated**: no `log_action` payload may contain a PII key. That fails
  on the *next* call site someone writes, which is the only version of this that survives.
  ⚠️ **The key set written here — `{"email", "full_name", "ip_address", "recipient"}` — is a hand list
  and is SUPERSEDED by D12.2**, which derives it from the `*_pii` table columns. This list was already
  short by two keys (`oauth_provider_id`, and `pending_email`, which D8 adds in the same release).
  A hand list inside the fix for a hand list is the mistake writing itself out.
- ~~**Backfill**: 116 rows, four known shapes, one key (`email`).~~ 🆕 **DELETED — §2c.** There is
  **no backfill**. Zero existing rows carry personal data; all five PII-writing call sites have never
  fired. The expand release ships the call-site change and the redactor with **no data-repair step**.
  ⚠️ Deleting it also deletes the only witness a redactor bug would have had besides the guard — see
  §2c's revised acceptance criteria before writing that guard.
- **`audit_log_pii` is NOT built in this release.** It would protect a column that has never held a
  value. It belongs to [core#670], the decision to *start collecting* client IPs — and when that
  ships, the column lands in a sidecar from day one rather than being migrated later. This removes one
  table from the migration.

### The gate that stops a fourth correction

**No column enters or leaves this spec's list on a name.** Every candidate needs a production
non-null count first — *a column that holds nothing is not PII in practice, and a column holding JSON
diffs of other rows is PII whatever its name suggests.* Both errors above are that same mistake in
opposite directions.

**Task for Infra — read-only, ~5 minutes, blocks nothing else** *(Product cannot run it: prod DB access
is outside my permission set, and the browser path needs a login credential I will not put in a
transcript)*:

```sql
-- non-null count for every text/JSON column in public, next to its table's row count
SELECT c.table_name, c.column_name, c.data_type,
       (xpath('/row/cnt/text()',
         query_to_xml(format('select count(%I) as cnt from %I.%I',
           c.column_name, c.table_schema, c.table_name), false, true, '')))[1]::text::int AS non_null
FROM information_schema.columns c
WHERE c.table_schema = 'public'
  AND c.data_type IN ('character varying', 'text', 'json', 'jsonb')
ORDER BY non_null DESC, c.table_name, c.column_name;
```

Hand the output back to Product. ⚠️ **A zero count is evidence about *this* deployment, not about the
column** — `invitations.*` is empty here only because nobody has been invited yet, and it is
unambiguously PII. Zero means *"check whether anything writes it"*; that is what made
`ip_address` different, and `git grep` is what settles it. **The count narrows the question; it does
not answer it.**

⚠️ **This correction is cheap only now.** The contract step that drops columns lands a release *behind*
the expand, so a wrong list costs two releases. That is why Growth filed it before the migration was
written rather than after.

## 2b. ⚠️ AMENDMENT 2026-08-30 night — the audit table, measured; and the read side, which nobody checked

**Provenance**: the coordinator measured `audit_logs` on production rather than reading the model, and
the founder proposed a mechanism (*"on deletion, flush the PII properties in `audit_logs` for that
user"*). Both are answered here. §2a established *that* the JSON is the exposure; this section
establishes *what can be done to it*, and it changes the design.

### The table, as it actually is

| Property | Value | Confirmed in code |
|---|---|---|
| Partitioning | **none** — ordinary table, `relkind = r`, 0 partitions | no partition DDL anywhere in `datanika/migrations/` |
| Size | **117 rows, 88 kB** *(116 the previous evening — it grew by one between two measurements, and nothing purges it)* | — |
| Indexes | **`id` (PK) and `org_id` only** | `org_id` is `TenantMixin`'s, declared `index=True` (`models/base.py:23-26`) |
| `user_id` | FK to `users.id`, **no index** — Postgres does not create one | `models/audit_log.py:26` has no `index=True`. `WHERE user_id = X` is a sequential scan |
| `old_values` / `new_values` | **`json`, not `jsonb`** | `models/audit_log.py:31-32` uses SQLAlchemy's generic `JSON`; the original DDL agrees (`a1b2c3d4e5f6_add_all_tables_to_public.py:359-360`) |
| `ip_address` | populated in **0 of 117** | §2 row 8 |
| `old_values` / `new_values` **content** | 🆕 **§2c: 30 / 18 rows hold an object, 0 hold an email.** The `116 of 116` this table used to imply was `count()` counting the JSON literal `null` | §2c |

The prod measurement and the model agree exactly, in both directions. That is worth one line because
it is not the usual outcome in this codebase, and it means the rest of this section can be reasoned
from the model.

⚠️ **Nothing purges `audit_logs`.** `run_maintenance_task` purges runs
(`maintenance_run_retention_days = 90`), orphaned dlt dirs, dbt targets, archives and spent reset
tokens. **Audit rows are in none of those sweeps**, and `AuditLog` is the one model with no
`deleted_at`. The table grows without bound, by design. This is what turns "expensive later" from a
hypothetical into a schedule (D13).

### 🆕 The finding that changes the decision: **nothing reads the payload**

`old_values` and `new_values` are written by **30 call sites** through two chokepoints — and read by
**zero production code paths**. Verified by grepping the whole of both repos, not the directories I
expected them in:

- **Not rendered.** `ui/pages/audit_logs.py:15-21` renders `created_at`, `action`, `resource_type`,
  `resource_id` and **`ip_address`**. The audit page shows the one column that has never held a value
  and omits the two that hold every value — §2's original error, inverted, in the UI.
- **Not in `AuditState`**, not in `services/api_v1_routes.py`, not in the MCP tool surface, not in any
  export envelope, not in any script.
- The **only** read in either repo is `tests/test_services/test_audit_service.py:65-66`, asserting
  that what was stored comes back.

**This is the ninth instance of *"machinery exists, entry point does not"*** and the second where the
orphan is a security control. Filed as **[core#694]** — it is a feature (build the reader), not an
erasure fix, and folding it in here would let a new data-*exposure* surface ride into production
inside a privacy change, which is the same reason §9 keeps `client_ip.py` out.

**Why it changes the decision rather than just being a curiosity.** The trade-off as posed was
*"blanket redaction guts the audit trail's forensic value."* There is no forensic value being gutted
today, because there is no reader. That removes the argument **against** blanket redaction — and it
simultaneously removes every guard-rail that would have caught a redactor bug. **No test, no page and
no user would notice if redaction silently emptied every payload in the table.** Green would prove nothing, in
the purest available form. D12 is written around that fact: the guard has to assert what *survives*,
not only what is removed.

## 2c. 🆕 CENSUS 2026-08-31 — the backfill is deleted, and the guard is now the only witness

**Provenance**: Infra's read-only column census, the task §2a asked for. It arrived before Engineering
wrote the expand migration, which is the only moment it was cheap.

### The correction, and it is a correction of a *method*, not of a digit

**`count(new_values) = 117` counts the JSON literal `null`, which is not SQL `NULL`.** A `json` column
holding the four bytes `null` is non-`NULL` as far as `count()` is concerned. So *"116 of 116
populated"* — written into §2, §2a and §2b, and relayed to me as a measurement — was a fact about
**nullness**, never about **content**.

| | Claimed | Measured |
|---|---|---|
| `old_values` holding an object | 116 of 116 | **30** |
| `new_values` holding an object | 116 of 116 | **18** |
| audit rows carrying an email | assumed "the exposure" | **0** |

⚠️ **This is the same error as `ip_address`, in the opposite direction.** §2 counted a column as PII
because of its name; §2b counted a column as populated because of its `count()`. Both times the
instrument answered a question next to the one being asked. `count(col)` answers *"is the column
set?"*; the question was *"does the payload contain anything?"* — which on `json` needs
`json_typeof(col) = 'object'`, or the `jsonb` containment operators D13 exists to provide.

### The rest of the census

- **The complete production key set across every audit payload** is `name` · `connection_type` ·
  `target_id` · `target_type` · `destination` · `source` · `cron` · `is_active`. Intersected with
  D12.2's candidate PII key set it is **empty**.
- **All five PII-writing call sites have never fired.**
  `resource_type IN ('organization','membership','invitation','user')` returns **0 rows**. Every
  payload in the table was written by the 25 label-carrying sites.
- ✅ **§2's finding #7 is confirmed by join rather than by pattern**: `organizations.name` holds a live
  `users.full_name` in **5 of 5**, and `.slug` in **5 of 5**. This was inferred from two write sites;
  it is now measured.
- ✅ **`users.email` is the only column in the entire schema holding an address** — regex across all
  **121** text and JSON columns. `audit_logs.ip_address` remains **0 of 117**.

### What this deletes

| Removed | Where it was |
|---|---|
| **The one-time backfill of existing audit payloads** | D11 bullet 3, D12.4 item 1, and the redaction step in release **N** of §4 |

There is nothing to backfill. The expand release ships the redactor and the call-site change with no
data-repair step at all.

### ⚠️ What this does NOT change, and the reason is §2a's own gate

**The design is untouched.** The census measures **history**, not **reachability**. All five call
sites are live code on ordinary paths — invite a member, remove a member, cancel an invitation, rename
an org, change an email. They have never fired because this deployment has never had a second member.
**They fire on the first invitation**, which is a product goal, not a hypothetical.

> §2a already wrote the rule and then had to be reminded of it: *"A zero count is evidence about
> **this deployment**, not about the column."* It said so about `invitations.*`. It is equally true of
> `audit_logs`. **The census narrows the question; it does not answer it.** A column that holds nothing
> today is not thereby safe — it is untested.

`users.email` being populated in **5 of 5** is the discriminating case: *"there is no data to carry"*
is true of the **audit payload** and false of the **PII tables**. N's backfill into `user_pii` /
`organizations` still has rows to move.

### 🚨 The consequence that matters: the guard is now the sole detector, and it can pass vacuously

§2b established that **nothing reads the payload**, so a redactor bug has no user-visible witness.
The backfill was the second witness — redacted rows you could open and look at. **Deleting it leaves
exactly one: D12.2's guard.** That raises the guard's importance rather than lowering it, and it
introduces a failure mode the guard did not have before.

🚨 **A guard shaped as *"no payload in `audit_logs` contains a PII key"* is TRUE TODAY, against no
redactor at all** — 0 of 30 payloads contain one, and the five sites that could write one have never
run. That test goes green against an empty function, on a fresh clone, forever. It would be the tenth
"green that proves nothing" in this spec's own subject matter, shipped by the fix for the ninth.

**Acceptance criteria, revised (these supersede the corresponding items in §5):**

1. **The redactor guard CONSTRUCTS its input.** It calls `AuditService.log_action` with a payload
   carrying each derived PII key and asserts the stored value is the marker — it must never assert
   over whatever the table happens to contain. **Shown red against a no-op redactor** is a required
   artifact, not a nicety: a test that has never failed has never been shown to be able to.
2. **The same for the D12.4 residual sweep.** *"Finds zero"* is also its result before the feature
   exists, so a clean run is not evidence. Its acceptance evidence is a run against a **deliberately
   planted** PII-bearing row: the sweep finds it, logs a count, and increments the metric.
3. **The derived key set's cardinality is pinned** (already D12.2) — and now doubly load-bearing,
   because an empty `frozenset()` and a correct set produce **identical** results on production data.

### Release count: still four. Row count was never what set it.

The coordinator asked whether the sequencing shrinks now that there is nothing to carry. **No**, and
the reason is worth stating because it will be asked again:

- **N → N+1 → N+2 is set by *code overlap under blue/green***, not by data volume. During the N+2 swap
  the still-serving **N+1** code runs against the contracted schema. That is true of a table with zero
  rows exactly as it is of one with a billion. See the expand/contract policy in `CLAUDE.md`.
- **N₀ still stands.** `jsonb` is a **capability**, not an optimization: the residual sweep's
  containment query has to be *expressible* to return zero. A query that cannot run does not return
  zero — it errors. And `CREATE INDEX CONCURRENTLY` on `user_id` is instant now and an online build
  later, on a table nothing purges.

## 3. Decisions

### D1 · One PII sidecar table per parent, named `<parent>_pii`. Not one polymorphic table.

| Table | Columns |
|---|---|
| `user_pii` | `user_id` **PK and FK** → `users.id` · `email` `String(320)` NOT NULL **unique** · `full_name` `String(255)` NOT NULL · `oauth_provider_id` `String(255)` nullable · `pending_email` `String(320)` nullable *(D8)* |
| `invitation_pii` | `invitation_id` **PK and FK** → `invitations.id` · `email` `String(320)` NOT NULL |
| `notification_channel_pii` | `channel_id` **PK and FK** → `notification_channels.id` · `recipient` `String(320)` NOT NULL |
| ~~`audit_log_pii`~~ | ⚠️ **NOT BUILT in this release — see §2a / D11.** It would protect a column empty in every production row. Moves to [core#670], which decides whether to start collecting client IPs at all; the sidecar lands with that feature, not before. **Three tables ship, not four.** |

Shared PK/FK, so the relationship is 1:1 and the FK *is* the identity — no surrogate key, no way to
have two PII rows for one user. `TimestampMixin` is **not** applied: a `deleted_at` on a table whose
whole purpose is hard deletion is a trap waiting to be read as "erased" when the row is still there.
No `TenantMixin` — these are person-scoped, like `users` itself. All **three** go into `PUBLIC_TABLES`
(`migrations/helpers.py`).

**Why per-parent rather than one `personal_data` table.** A polymorphic table cannot express
`email UNIQUE` (the login constraint), cannot keep per-column types, and turns every lookup into a
key-name string match. The naming convention is doing real work in exchange: **`*_pii` is greppable**,
which is what makes the export guard in §8 mechanical rather than another hand-maintained list.

### D2 · `users.email` moves, uniqueness moves with it, and the address is freed on erasure.

The coordinator flagged both consequences and asked for them to be stated rather than discovered.

**Extraction cost is smaller than "every auth lookup becomes a join" suggests.** There are 32 textual
references to `User.email`, but only **five are SQL-level reads of the column**; the other 27 are
attribute reads on an already-loaded `User`, and the UI already copies email into DTOs (`UserInfo`,
`MemberItem`). The five:

- `services/user_service.py:106` — `get_user_by_email`, the single chokepoint (4 internal callers: `:33`, `:57`, `:88`, `:439`)
- `services/invitation_service.py:34` and `:92`
- `services/email_routes.py:77`
- `scripts/e2e_seed.py:264` (seed only)

⚠️ **`get_user_by_email` must join `user_pii` *and* filter `users.deleted_at IS NULL`.** Without the
filter a soft-deleted user still authenticates, which converts this whole feature into a security
regression. There is a compensating property worth knowing: an **erased** user has no `user_pii` row
at all, so the join returns nothing and login becomes *structurally* impossible rather than
policy-impossible. Belt and braces: erasure also sets `is_active = False`.

**Decision: hard-deleting the PII row frees the address for re-registration. That is correct, and it
is not a side effect we tolerate — it is the point.** Retaining a tombstone (even a hash) to block
re-registration would mean retaining a pseudonymous identifier that re-identifies the person on
lookup, which is the thing they asked us to stop doing.

**So, explicitly, what happens when the same person signs up again:** they get a new `users` row, a
new integer id, and a new organization. Nothing links them to the old account, and **nothing can** —
not by us, not by support, not by a database query. The old `users` row survives as an anonymous
integer holding FK integrity for audit rows and memberships. Three consequences to state in the UI and
the docs, because each will otherwise arrive as a support ticket:

1. **We cannot restore an erased account.** There is no undo, ever.
2. **We cannot tell a returning user what their previous organization was**, or re-add them to it.
3. **Their old organization's data is not returned to them.** If they were the sole member, D6 deleted the org with them; if they shared it, the org and its connections belong to the remaining members.

### D3 · `invitations.token` becomes `token_hash`. This is free today and expensive later.

Mirror `PasswordResetToken`: `token_hash` `String(64)`, unique, indexed, SHA-256 of the value in the
emailed link, looked up by hash. The email then lives only in `invitation_pii.email`, and a `pg_dump`
yields nothing readable.

**The migration has no clean path for tokens already outstanding** — a hash cannot be derived from a
JWT we no longer hold in the clear at rest, so every pending invitation would have to be invalidated
and re-sent. Today there are effectively none. **At the first real cohort this becomes a change that
emails strangers "your invitation link no longer works."** Do it now.

Note this is a *storage* fix, not an extraction — but the personal datum is inside the token, so it
belongs to this spec rather than to a future security ticket.

### D4 · `organizations.slug` stops being derived from a person's name. The display name may stay.

Two different problems wearing one bug.

- **`slug` is an identifier**: unique-constrained, in URLs, and read by the SSO callback (`sso_routes.py` matches `Organization.slug == org_slug`). A name-derived slug is a person's name published in a durable key. **Decision: new orgs get `slug = f"org-{user.id}"`.** Change both generators (`ui/state/auth_state.py:288` and `services/user_service.py:475`).
- **`name` is display text** inside the tenant. `"{full_name}'s Org"` is friendly and may stay. The erasure sweep rewrites it (D5).

⚠️ **Existing orgs are not renamed proactively.** Renaming a slug that an SSO config points at breaks
that org's IdP entry point until someone updates the IdP. The erasure sweep renames the specific org
it is erasing (D5) and states this consequence to whoever runs it.

### D5 · Erasure: hard-delete the PII, hard-delete every credential, soft-delete the identity.

`UserService.erase_user(session, user_id)`, one transaction. ⚠️ **Synchronous — see D14.1**, which answers the founder's weekly-batch proposal and shows why the phase boundary sits after all eight steps below rather than before them.

The **nine** steps *(was eight — step 1a added 2026-09-02, §0.2; step 7 rewritten, §0.1)*:

| Step | Action | Why |
|---|---|---|
| 1 | **DELETE** `user_pii` row | the erasure itself |
| 🆕 **1a** | **NULL the legacy columns** already copied into the PII tables: `users.email`, `users.full_name`, `users.oauth_provider_id`, `invitations.email`, `invitations.token`. **Live in releases N and N+1; DELETED in N+2**, when the columns are dropped | 🚨 **§0.2.** Erasure ships in **N**, which still dual-writes these. Without this step every erasure until N+2 deletes the copy and leaves the original — and criterion 8, `get_user_by_email`, login and every UI surface all still read as erased. Possible **only because N drops their NOT NULL** (§4) |
| 2 | **DELETE** `api_keys`, `password_reset_tokens`, `oauth_grants`, `oauth_tokens` for that `user_id` | live credentials pointed at a person who no longer exists. Not soft — a soft-deleted API key that still authenticates is a backdoor |
| 3 | **DELETE** `invitation_pii` for invitations this user sent that are still `PENDING`, and mark those invitations `REVOKED` | an invitee's address is *their* personal data, and the invitation cannot complete anyway |
| 4 | Soft-delete `memberships` (`deleted_at`) | removes them from every member list through the existing filters |
| 5 | `users.deleted_at = now()`, `is_active = False` | the anonymous integer identity survives for FK integrity |
| 6 | For each org where this user was the **sole member**: D6 | |
| 7 | 🆕 **REWRITTEN.** For **every** org this user belonged to whose `slug` or `name` contains the erased name — **including the ones step 6 is about to delete** — rename `name` to `Organization {id}` and `slug` to `org-{id}`. **Order: rename first, then D6's soft delete.** | 🚨 **§0.1.** This previously read *"each **surviving** org"*, which skipped precisely the org that existed only for the erased person. §2c measured `organizations.name` carrying a live `users.full_name` in **5 of 5** prod rows and `.slug` in **5 of 5**, so the skipped case is the common one. The ordering is load-bearing too: after the soft delete, an `UPDATE … WHERE deleted_at IS NULL` — the shape every org-scoped query here uses — no longer matches the row |
| 8 | Write **one** audit row: `action="user.erased"`, `user_id` = the erased id, `new_values` = `{}` | ⚠️ **`new_values` must not name the user.** An erasure that logs the erased email defeats itself |

**`audit_logs` rows are kept, and this is deliberate.** They are append-only (`AuditLog` is the one
model with no `deleted_at`) and hold only `user_id`, an action string, and resource labels — a
behavioural trail pointing at an integer with no way back to a person. That is exactly the shape the
founder decision describes, and it is the pattern the rest of this spec follows: **the record
survives, the personal datum goes.**

⚠️ **Corrected — see §2a/D11.** This paragraph used to say `audit_log_pii` was the mechanism for
the one column that does not fit. It is not: the column it named is empty in every production row,
and the columns that are full (`old_values`/`new_values`) carry **other people's** email addresses.
D11 stops them carrying personal data at the five call sites that write it, so there is nothing here
to sweep and no sidecar to build.

**Not touched:** connections, pipelines, uploads, transformations, schedules, runs, catalog entries.
All are `TenantMixin` (org-scoped), **there is no `created_by` column anywhere in either repo**, and
so no data asset is orphaned by removing a person. This is worth checking rather than assuming — it is
what makes erasure cheap here and expensive in most products.

### D6 · Org deletion, and the two things soft-delete does not reach.

- **Sole-member org** → deleted with the user, no prompt.
- **Shared org where the user is the last owner** → refused, reusing the existing last-owner guard (`user_service.py:489-501`). The user must transfer ownership or delete the org first; the dialog says which, and links to the transfer control. **Route around that guard and an org becomes permanently unadministrable.**
- **Shared org where others remain** → untouched, apart from D5 step 7.

`OrganizationService.delete_org(session, org_id)` soft-deletes the org **and every row carrying that
`org_id`**. ⚠️ **Soft-deleting only the `organizations` row is not enough**: `Organization.deleted_at`
is read in exactly one place in the codebase (`services/sso_service.py:81`) and written nowhere, so
org-scoped queries do **not** filter on it. Setting it alone hides nothing.

Three things live outside that transaction and must be handled explicitly, or an org deletion is a
deletion in name only:

1. **`dbt_projects/tenant_{org_id}/`** — on disk, outside the database. Nothing soft-deletes a directory.
2. **Warehouse schemas** the org's pipelines created — in the **customer's own** warehouse, under their credentials. **Decision: we do not delete these, and the dialog says so** in one line. They are the customer's data in the customer's account; silently dropping schemas in someone else's warehouse is a far worse failure than leaving them.
3. **The Paddle subscription** — an org deleted with a live subscription keeps being charged. **Decision: `delete_org` cancels it first, and refuses to proceed if the cancellation call fails.** This is the cross-link to [SPEC_BILLING_SELF_SERVICE.md](https://github.com/datanika-io/datanika-cloud/blob/master/docs/specs/SPEC_BILLING_SELF_SERVICE.md) D2, which builds `BillingService.cancel_subscription`. Order matters: cancel, then delete. The reverse leaves a subscription with no org to attribute it to.

### D7 · Retention: PII is deleted immediately. The 30 days in the policy is the backup tail.

`datanika.io/privacy` §6 says *"After account deletion, personal data is removed within 30 days."*

**Decision: no grace period, no soft-delete of PII, no queued job.** The delete happens inside the
request transaction. A grace period would mean retaining personal data after the person asked us to
stop, which is the opposite of the thing being built.

The 30 days is therefore **entirely** the backup tail, and it fits exactly:

| Copy | Retention | Source |
|---|---|---|
| live DB | 0 — deleted in the transaction | this spec |
| `/opt/datanika/backups/` on the app box | **7 days** | `backup-offsite.sh:21` `LOCAL_KEEP_DAYS=7` |
| Aweb `185.226.65.96:/opt/datanika-backups/` | **30 days** | `backup-offsite.sh:22` `REMOTE_KEEP_DAYS=30` |

**We do not rewrite backups**, and should not — an archive you edit is not an archive. The published
promise is satisfiable **only because `REMOTE_KEEP_DAYS` is exactly 30**.

> 📌 **Cross-team constraint for Infra.** Raising `REMOTE_KEEP_DAYS` above 30 breaks a published legal
> promise on `datanika.io/privacy`. If backup retention needs to grow, the privacy policy changes in
> the same week. This constraint has no mechanical enforcement; it is written here and in
> [PLAN_INFRASTRUCTURE.md](https://github.com/datanika-io/datanika-core/issues/724) because a number in a shell script and a
> sentence on a marketing page have nothing linking them.

### D8 · Email change: confirmed at the new address, refused if taken.

The `#655` case *"a typo'd signup email locks a user out permanently"* is more likely to be met by a
real user than an erasure request, and it is much the smaller build once `user_pii` exists.

1. User enters a new address in the Settings account card. If they have a password, they re-enter it.
2. `user_pii.pending_email` is set. **The live `email` does not change.**
3. Refused if the address matches any non-deleted `user_pii.email` **or any other `pending_email`** — otherwise two people race for one address and the second confirmation fails at the unique constraint with an unexplainable error.
4. A confirmation link goes to the **new** address. Token: a new `email_change_requests` table — `user_id`, `token_hash` `String(64)` unique indexed, `expires_at`, `used_at`. **Same shape as `password_reset_tokens`, and PII-free by construction** because the address it refers to lives in `user_pii.pending_email`, not in the token.
5. On confirmation: `email` ← `pending_email`, `pending_email` ← NULL, `email_verified` stays as it was for OAuth accounts and is set True here for password accounts.
6. A **notice** goes to the **old** address saying the change was requested, with no link to approve — informational only. An attacker with a live session should not be able to move an account silently.
7. Requesting a new change invalidates any outstanding one.

### D9 · Where the controls live, and how they are confirmed.

Both go in the **`account_card()`** on `/settings` that [SPEC_PASSWORD_RESET.md](SPEC_PASSWORD_RESET.md) D1
introduced — it is already the one user-scoped card on an otherwise org-scoped page. Deletion sits at
the bottom, visually separated, and is the only destructive control on the page.

**Confirmation, and it must be typed, not a second button.** Reuse [core#623]'s discriminator exactly:
`password_changed_at IS NULL` means the account has never had a password.

| Account | Confirmation |
|---|---|
| has a password (`password_changed_at` is not NULL) | re-enter the current password |
| OAuth/SSO only (`password_changed_at IS NULL`) | type the organization name exactly |

The dialog states, before the confirm button is enabled: what is deleted, what is kept (billing
records, 7 years, by law), that warehouse schemas in their own account are untouched, and that it
cannot be undone. **WORKFLOW_RULES §7b (`plans/WORKFLOW_RULES.md`) applies to the implementation too**:
the confirm control must be scoped to the dialog, and no destructive action may be reachable without
the dialog open.

### D10 · Two entry points, one service method — and neither ships without the other.

`erase_user` and `delete_org` are the work; entry points are trivial. Ship **both**:

1. **`datanika/scripts/erase_user.py`** — operator-run, for a request arriving by email from someone who cannot reach the UI (locked out by a typo'd address, or a former member).
2. **The Settings control** in D9.

The audit's own recurring finding is *"machinery exists, entry point does not"* — [core#623],
`email_service`, `send_quota_warning_email_task`. **A merged `erase_user` with no caller is a fourth
instance of it, and this spec does not accept that as done.**

> ℹ️ **D11 is not missing — it lives in §2a**, where the correction that produced it was written.
> Numerically the decision list reads D1–D10 here, D11 above, then D12–D14. D12.1 depends on it.

### D12 · Redact on write **and** scrub on delete. Key-level, never blanket. The key set is derived from the `*_pii` tables — **not** from `SECRET_CONFIG_KEYS`.

Three questions were put to Product. All three are answered here, and the first answer corrects the
mechanism that was suggested.

#### D12.1 · Both — and they are not redundant, they reach different things

| Mechanism | What only it reaches |
|---|---|
| **D11** — the five call sites store internal IDs | `organizations.name` / `slug`. **No key-name rule can reach this**: the key is `name`, and `{"name": "My Postgres"}` on the 25 label-carrying sites is not personal data while `{"name": "Anna's Org"}` on one site is. The discriminator is `(resource_type, key)`, not `key` — so this case is fixed at the call site or not at all |
| **Redact-on-write** at `AuditService.log_action` | PII in call sites **nobody has written yet**, and PII in rows belonging to *other* actors. D11 fixes five known sites; the redactor is what makes the fifth site safe before anyone reviews it |
| **Scrub** (D12.4) | 🆕 **§2c: nothing that already exists** — the existing rows carry no PII, so the scrub's only remaining job is the **canary** in D12.4 item 2: anything the other two miss |

⚠️ **The next PII-writing call site is in this spec.** D8 (email change) is the first genuine `users`
update in the product's history. The obvious implementation audits
`{"email": old, "new_email": new}` — two personal values, in a payload, on a row the user authored.
D11 does not prevent it, because D11 is a list of five sites that predates it. The redactor does.

#### D12.2 · ⚠️ The key set must NOT be `SECRET_CONFIG_KEYS`. It contains no PII key at all.

The routing message asked that the schema-derived key set Engineering built for [core#651] be reused,
*"rather than a hand-maintained list."* **The principle is right and the object is wrong**, and the
failure would be silent, so it is stated here rather than left to be discovered.

`connection_service.SECRET_CONFIG_KEYS` (`connection_service.py:171-196`) is a **connector-credential**
set — 17 keys: `password`, `token`, `api_key`, `access_token`, `refresh_token`, `client_secret`,
`developer_token`, `aws_secret_access_key`, `aws_access_key_id`, `keyfile_json`,
`service_account_json`, `credentials`, `secret`, `api_token`, `auth_password`, `auth_token`,
`security_token`. **Not one is a PII key.** It derives from `CONFIG_SCHEMAS`, which describes
**connection types** — a universe with no email addresses in it. Redacting an audit payload against
that set removes nothing from any of the 30 call sites.

A redactor built on it would be derived, superset-tested
(`tests/test_services/test_secret_key_coverage.py`), green, and would redact **zero** personal data —
in a table whose payloads nothing reads, so nothing would contradict it. That is the exact defect
shape this spec exists to close, arriving through the door marked "do it the derived way."

**The right derivation source is the `*_pii` tables D1 creates** — the same greppable convention §6's
export guard already uses. One convention, two consumers, rather than a second mechanism:

```python
PII_PAYLOAD_KEYS = frozenset(
    col.name
    for table in Base.metadata.tables.values()      # NOT models.__all__ — see §6
    if table.name.endswith("_pii")
    for col in table.columns
    if not col.primary_key and not col.foreign_keys
) | {"ip_address"}
```

Today that is **`{email, full_name, oauth_provider_id, pending_email, recipient, ip_address}`** — a
strict superset of §2a's hand list, which it supersedes. That list omitted `oauth_provider_id` and
`pending_email`; `pending_email` did not exist when it was written, which is the whole argument.
The derived set grows on its own when a fourth PII table or a fifth PII column lands.

- **`ip_address` is the one hand-added key, and it has a stated expiry.** It is not a `*_pii` column
  because §2a/D11 declines to build `audit_log_pii`. It belongs in its own column and never in a
  payload. When [core#670] creates that sidecar, the derivation picks it up and this literal becomes
  redundant — harmless, and it should be deleted then.
- ⚠️ **Derive the set, then pin its cardinality in a test.** `Base.metadata.tables` is populated only
  for models that have been *imported*. A redactor whose module loads before the PII models does not
  raise — it silently gets an **empty** set and redacts nothing. §6 already names the sibling trap
  (`models/__init__.py` does not export `Invitation`, `SSOConfig`, `Notification` or
  `NotificationChannel`). So the guard asserts the exact expected contents, not merely that the
  derivation ran. **This is the one place a hand-written literal is correct: as the assertion, never
  as the source.**
- 🆕 **Pinned name, 2026-08-31: the set is exported as
  `datanika.services.audit_service.PII_PAYLOAD_KEYS`.** The spec describes behaviour, but the guard
  in `tests/test_services/test_audit_pii_redaction.py` needs a handle, and a contract a test cannot
  address is not a contract. Renaming it is fine — rename it in the guard in the same commit.
- ⚠️ **The set is nominal.** It matches key names, so it cannot see PII stored under a non-PII key —
  precisely the `organizations.name` case. That residual is D11's, and it is why D12.1 is *both*.

#### D12.3 · Key-level. What the audit trail must still be able to answer.

The constraint asked for, stated as the six questions the trail must answer — and **honestly**: today
it answers none of them from the payload, because §2b established that nothing reads the payload.
These are therefore a **forward commitment**: what must still be answerable when the reader is built.

| # | Question | After D11 + D12 |
|---|---|---|
| 1 | Who did what, to which resource, when? | ✅ `user_id`, `action`, `resource_type`, `resource_id`, `created_at` — no redaction touches these |
| 2 | **What was this connection / pipeline / schedule / upload called before it was renamed or deleted?** | ✅ the 25 label-carrying call sites, untouched. **Key-level keeps them. Blanket destroys them.** |
| 3 | What type, command or cron did it have? | ✅ same payloads |
| 4 | Did a membership role change, and from what to what? | ✅ `{"role": …}` — not personal data, kept verbatim |
| 5 | Was an invitation sent or cancelled, and for whom? | ⚠️ **"for which invitation id"**, not "for whom". Resolvable through `invitation_pii` while that row exists, unresolvable after erasure. That is the intent, not a loss |
| 6 | **What was this email address before it changed?** | ❌ **Deliberately not answerable — this is the decision** |

**Question 2 is why key-level wins, and it is not hypothetical.** The five production connections
deleted in one session by a page-wide `.last()` (WORKFLOW_RULES §7b (`plans/WORKFLOW_RULES.md`), and the
Product skill's own safety rules) were deleted through the UI, so `connection_state.py:1285` wrote
their `old_values`. Those payloads are the **only** surviving record of what those five connections
were. 🆕 **§2c confirms this positively rather than by assumption**: the complete production key set
across every payload is `name · connection_type · target_id · target_type · destination · source ·
cron · is_active`, and `name` + `connection_type` on a delete is exactly that record. They are among
the **30** rows holding an object. Blanket redaction destroys them — and, because nothing reads the
column, destroys them invisibly. **The census strengthens this argument; it is the one place the
smaller numbers make the case for key-level *harder*, not softer.**

**Question 6 is the decision, and it runs against the intuition.** *"What was this email before it
changed"* is genuinely what an account-takeover investigation wants. It is still refused:

1. **Retaining a former address after erasure re-creates the exposure erasure exists to remove.**
   Art. 17 has no forensic exception for a general-purpose audit trail, and a retained prior address
   is a direct identifier, not a pseudonym.
2. **The takeover defence does not depend on it.** D8 step 6 mails the **old** address at request
   time; the live address is in `user_pii.email` and the in-flight one in `user_pii.pending_email`.
   The audit log is not, and must not become, the notification path.
3. **The trail still records that the address changed, when, and by whom** — the part that identifies
   the incident. What it stops recording is the value.

> **The constraint, in one line:** *the audit log records **that** a value changed and **who** changed
> it; it is never the store of record for **what the value was**, when that value is personal data.*
> Everything above follows from that sentence — including why blanket is wrong and why 6 is refused.

**Redaction is replacement, not deletion.** A redacted key keeps its key and takes a fixed marker
(reuse `backup_service.REDACTED`), so the trail still shows *an email was here*. A dropped key is
indistinguishable from a call site that never wrote one.

#### D12.4 · The founder's scrub keeps a job. Three of them, and none is the primary mechanism.

*"On deletion, flush the PII properties in `audit_logs` for that user."* Kept, and scoped:

1. ~~**One-time backfill of the existing rows.**~~ 🆕 **DELETED — §2c.** Zero existing rows carry
   personal data; the org-update row this item was written for has never been written, because
   `resource_type IN ('organization','membership','invitation','user')` returns **0 rows**. **The
   scrub now has two jobs, not three.**
2. **A residual sweep inside `erase_user`** — after D11 and D12 this must find **zero rows**, and that
   is the point. It is a **canary, not a cleanup**: if it ever redacts something, the guard failed.
   It logs a **count**, never a value, and increments a metric, so the failure is visible rather than
   quietly repaired.
   🚨 🆕 **Its acceptance evidence cannot be a clean run.** "Finds zero" is also what it returns
   **before the feature exists**, and now also what it returns on a correct run against real data — so
   a green tells you nothing. Prove it against a **deliberately planted** PII-bearing row: the sweep
   finds it, logs the count, increments the metric. §2c criterion 2.
3. **Not the primary mechanism**, for the reason given in routing and one more:
   - `WHERE user_id = <erased>` catches rows the user **caused** and misses rows where their address
     sits in someone else's entry. Confirmed — and it is the *majority* case: **three of the four
     PII-writing sites store the *subject's* address on a row whose `user_id` is the *actor***
     (`settings_state.py:201`, `:304`, `:334` — invite, remove-member, cancel-invitation).
   - Correct scope is therefore every row in every org the user has ever belonged to (**including
     soft-deleted memberships**) plus rows they authored — which needs D13's `user_id` index to be
     affordable, and D13's `jsonb` to be **expressible at all**.
   - **An erasure procedure that has to *find* things is one that can miss things.** Redaction at the
     chokepoint has nothing to find.

#### D12.5 · Where it lives, and the way it must fail

`AuditService.log_action` (`services/audit_service.py:11-33`) is the **single true chokepoint** — both
`BaseState._audit` (the 30 payload call sites) and the three direct callers in `auth_state.py` pass
through it. The
redactor goes there, so no new call site can bypass it. It **recurses** into nested dicts and lists:
today every payload is a flat dict of scalars, and that is not a property anything enforces.

🆕 **Pinned name and call shape, 2026-08-31: `datanika.services.audit_service.redact_pii_payload(payload)`,
called by `log_action` through a MODULE-GLOBAL lookup, not inlined into the method body.** This is not
style. §2c leaves this guard as the sole detector, and the only way to prove the guard is *sensitive to
the redactor* is to substitute a no-op and watch the guard fail. A redactor inlined into `log_action`
cannot be substituted, so the negative control silently stops being a control while still passing —
which would be the same defect one level up. `test_negative_control_a_no_op_redactor_fails_this_guard`
enforces this.

⚠️ **It must not raise.** `BaseState._audit` ends in `except Exception: pass` — *"Audit logging should
never break the main operation."* A redactor that throws therefore **silently deletes the audit row**,
turning a PII bug into a missing-trail bug with no signal at all. On any internal error it replaces
the payload with a marker (`{"__redaction_failed__": true}`) and lets the row be written: who/what/when
survives, and the marker is greppable. Failing open (pass the payload through) leaks; failing hard
(raise) loses the row. This is the only third option.

### D13 · Index `user_id`, convert the payload columns to `jsonb`, and ship both **ahead** of the expand as a standalone code-free migration (release N₀). This is the only part of the spec with a deadline.

**Tracked as [core#693]**, split out so it can ship without waiting for the rest of this spec.

Both operations are free today and neither is free later. At **117 rows / 88 kB** they complete
faster than the transaction that wraps them.

| Change | Why it is needed, not merely tidy | Cost at 88 kB | Cost at a million rows |
|---|---|---|---|
| `CREATE INDEX CONCURRENTLY ON audit_logs (user_id)` | D12.4's sweep, `erase_user`'s per-user reads, and `AuditService.list_logs(user_id=…)` — which already accepts the filter — are **sequential scans** today. Postgres does not index a FK | instant | a long online build, and every erasure until then scans the table |
| `ALTER … TYPE jsonb` on `old_values`, `new_values` | ⚠️ **Not an optimization — a capability.** On `json` there is no GIN index and **no containment operators at all** (`?`, `?|`, `@>` are jsonb-only). The payload cannot be searched, so **the one-time backfill and the residual sweep cannot be expressed** except by casting every row | instant | a full table rewrite under `ACCESS EXCLUSIVE` — a genuine outage |

⚠️ **The deadline is real and it is not "eventually".** §2b established that **nothing purges
`audit_logs`** — it is in none of `run_maintenance_task`'s five sweeps and `AuditLog` is the one model
with no `deleted_at`. The table grows monotonically for the life of the product. There is no future
point at which this gets cheaper, and the first real cohort is what makes it expensive.

**Ship them alone, before the expand, in a migration that changes no application code (release N₀).**
Nothing in the spec depends on N₀ having landed except the operations that cannot be written without
it, and isolating it means the one operation in the plan whose blue/green tolerance is *not obvious*
carries its own tiny revert.

⚠️ **The blue/green question, which must be answered by a probe and not by reasoning.** Under
blue/green the **previously deployed** code — whose model still declares
`mapped_column(JSON, …)` — serves against the converted schema for the length of the swap. Whether
SQLAlchemy's generic `JSON` bind and result processing round-trip cleanly against a `jsonb` column on
this driver is **not asserted here**, because it is the kind of claim that reads as obviously true and
is dialect- and driver-specific.

- **Gate**: extend the existing `migration-roundtrip` required check to run a write **and** a read
  through the *pre-migration* model definition against the *post-migration* column. Green means N₀
  ships alone; red means the model change (`JSON().with_variant(JSONB, "postgresql")`) must ship in
  the same release, and N₀ merges into N.
- **Rollback**: `ALTER … TYPE json` — instant at this size, and *only* at this size. The cheap
  rollback is itself part of the deadline argument.

⚠️ **The model change needs a dialect variant.** Tests run on SQLite, which has no `JSONB`. A bare
`postgresql.JSONB` on the model breaks the whole suite. Use
`JSON().with_variant(postgresql.JSONB, "postgresql")` — there is **no precedent for this in either
repo**, so it will not be copied from a neighbouring model.

Ordering note for whoever writes it: `CREATE INDEX CONCURRENTLY` cannot run inside a transaction
block, so it needs its own Alembic revision or `autocommit_block()`. That is a mechanical detail, but
it is the one that turns a two-line migration into a failed deploy.

### D14 · Erasure timing: the founder's phase boundary moves. Phase 1 is synchronous; the batch job survives with a different job.

**The founder's design**: *"GDPR allows deletion in 30 days or so, so maybe it should be a procedure
launched once a week on weekends that deletes preliminarily prepared users from all the PII tables."*
Mark on request, batch hard-delete weekly.

The shape is sound and the reasoning that produced it is sound **for the design it was imagining** —
scrub-as-primary, an unbounded JSON scan per user, something you would obviously want batched. D12
removes that work. What is left to batch is three deletes keyed by primary key.

#### D14.1 · Decision: everything in D5 runs **inside the request transaction**

| Phase | What | When |
|---|---|---|
| **1 — synchronous** | All of D5: the three PII rows, every credential (`api_keys`, `password_reset_tokens`, `oauth_grants`, `oauth_tokens`), pending `invitation_pii`, membership soft-delete, `users.deleted_at` + `is_active = False`, org handling (D6), the D5 step-7 rename, the `user.erased` audit row | in the request, as D5 already specifies |
| **2 — scheduled** | D12.4's **residual sweep**, which must find zero, plus its completion record | the founder's job, re-scoped below |

Four reasons, in the order they bind:

1. **It is the only version that keeps the promise we published four hours ago** (D14.2).
2. **The work is not expensive.** Three deletes by PK/FK plus four credential deletes by an indexed
   `user_id`. Batching buys operational simplicity that is not needed and costs a legal sentence.
3. **§9a(1)'s refusal must be synchronous or it is useless.** A sole owner who cannot be erased has to
   learn that *at the moment they click*, with the two exits named. Discovering a week later that
   nothing happened — via no notification, because none is specified — is worse than not offering the
   control.
4. **A failed batch erasure and a clean one are indistinguishable at every signal we have** (D14.4).

⚠️ **What is *not* being declined**: the founder's separate-tables + FK + soft-delete-user +
hard-delete-PII architecture is unchanged and is what D1/D5 implement. Only the *scheduling* of the
hard delete moves, and only for the stated reasons.

#### D14.2 · The collision with `/privacy`, resolved by arithmetic

Live on `datanika.io/privacy` §6 (`src/pages/privacy.astro:124-127`, and **test-locked** at
`tests/legal-pages-facts.test.ts:311-328`, whose comment names *this spec* as the thing it protects):

> *"After account deletion, personal data is removed within 30 days. The 30 days is the off-site
> backup retention window described in section 5 — a backup taken before the deletion request ages out
> within that period, which is what makes the promise deliverable rather than aspirational."*

The sentence asserts a **mechanism**, not just a number, and the mechanism holds only if the live purge
is prompt.

| Design | Worst case from request to last copy expiring | Verdict |
|---|---|---|
| **Synchronous (chosen)** | purge at T; last contaminated backup is the one taken before T; expires ≤ **T+30** | ✅ holds exactly |
| Weekly batch | purge at T+7; a backup at T+6 expires at **T+36** | ❌ breaks it by 6 days |
| Daily batch | purge at T+1; a backup at T+1 (if taken before the job) expires at **T+31** | ❌ breaks it by 1 day |
| Daily batch **ordered before the 03:00 backup** | last contaminated backup is T's; expires T+30 | ⚠️ works, and is rejected — see below |

**The daily-before-backup variant is rejected even though the arithmetic works.** It makes a published
legal promise depend on the *relative order of two cron schedules in two different systems* — Celery
Beat inside the app image, and a shell cron on the box invoking `backup-offsite.sh` — with nothing
linking them, no test that can see both, and no alert if one moves. This spec already names that exact
trap twice (D7's `REMOTE_KEEP_DAYS`, and the run-log retention numbers in [landing#343]). Choosing it
here would be the third instance, introduced deliberately.

**Shortening off-site retention to ~23 days is also rejected**: `REMOTE_KEEP_DAYS=30` is
simultaneously a **restore-window** commitment, and trading it away has a cost the privacy page cannot
see. That is Infra's number, not Product's, and it should not be spent to buy scheduling convenience.

✅ **Net: no change to `/privacy` is required, and none should be made.** The test lock stays green and
D7 stands as written.

> ✅ **CLOSED — founder decision, 2026-08-30 night: no grace period.** The **change-of-mind window**
> was put to the founder as the one thing a deferred purge would buy, and it was **declined**. D2
> stands as written: *"there is no undo, ever."*
>
> So the synchronous design in D14.1 is now the decided behaviour and not merely the cheaper option,
> and the `/privacy` sentence above needs **no re-word** — it was written against exactly this
> behaviour. ⚠️ **Do not re-open this as an implementation convenience.** A "pending deletion" state
> is not a smaller version of the same feature; it is the declined one, and it would drag a
> test-locked legal sentence with it.

#### D14.3 · ✅ [core#653] was a hard prerequisite for **any** scheduled component — and it is now SATISFIED

> 🆕 **CORRECTED 2026-08-31.** This section asserted *"Celery Beat has never run in
> production"* and that *"the string `beat` appears nowhere in `docker-compose.yml` or the
> `Dockerfile`"*. **Both are now false.** [core#653] closed **2026-08-30T23:58Z**; `beat` is a
> real service on `origin/dev` (`container_name: datanika-beat`, with the load-bearing
> `beat_state` volume) and has been live in production since 2026-08-30, where it fired the
> first `run_maintenance` in the project's history.
>
> ⚠️ **This drift pointed the expensive way.** A stale blocker parks work that is not
> blocked — §8 step 4 and criterion 17a both still read as gated. Left alone it would have
> deferred the scheduled component indefinitely, for a reason that had stopped being true.

**The original reasoning stands, which is why this is corrected rather than deleted.** A GDPR
erasure job on a scheduler that does not run is *a compliance promise that silently does not
happen*, and unlike a failed deploy nobody learns until a regulator asks. What changes is *what
to verify*: not "is [core#653] closed" — an issue closing is a different claim from a process
running — but **is beat producing, at ship time**. That is now directly measurable: since
2026-08-31 `celery-exporter` scrapes the worker event stream as `job=celery`, so `celery_worker_up`
and the `celery_task_*` counters answer it.

⚠️ **Two traps on that exporter, both of which make a dead pipeline look healthy**, and they
matter here because this is the signal the criterion now rests on: the worker must run with
**`-E`** (without it the exporter still serves `celery_worker_up` and **zero** `celery_task_*`,
which is indistinguishable from "no task has run yet"), and `--purge-offline-worker-metrics 0`
is load-bearing (the default deletes a vanished worker's series after 10 minutes, and
`increase()` over a vanished series yields no series, which reads as healthy at exactly the
wrong moment).

A GDPR erasure job on that scheduler is *a compliance promise that silently does not happen*, and
unlike a failed deploy nobody learns until a regulator asks. Under D14.1 no erasure depends on it — but
the **canary** does, and that is the subtler failure: **a canary that never runs reports nothing, and
nothing reads exactly like clean.** So:

- **[core#653] must be closed before the scheduled component ships.** Not before the erasure ships.
- **Idempotency by construction.** [core#648] is about *APScheduler* (a different scheduler), so it is
  not directly this job's risk — but `celery_app.conf` sets `task_acks_late=True`, which means a Beat
  task **can be redelivered** after a worker crash regardless. A `DELETE … WHERE` and a redaction
  `UPDATE` are naturally idempotent; a counter increment or an appended report row is not. Write the
  job so a double delivery is indistinguishable from one.
- ⚠️ **Do not add the sweep to `run_maintenance_task`.** Its DB block is wrapped in
  `except Exception: logger.exception(...)` and then sets its result counters to **0**
  (`maintenance_tasks.py:50-52`). A total failure and a clean run return the same shape. Riding that
  task would make a broken erasure canary report exactly what a working one reports.

#### D14.4 · What proves the job ran

**An erasure job whose only evidence is the absence of data is unfalsifiable.** `celery_tasks_total`
(`services/metrics.py:41`) already exists and is **not sufficient**: it records that a task executed,
and for the reason above a swallowed failure increments it identically. The completion record must
therefore carry:

1. **A row count per class of work** — rows examined, rows redacted, users purged. Zero redactions is
   the expected value and must be *reported*, not inferred from silence.
2. **A distinguishable failure state**, so "ran and found nothing" and "raised and was swallowed" are
   different readings.
3. **A durable location.** Celery's Redis result backend is ephemeral and no person or alert reads it.
   🚨 🆕 **CORRECTED 2026-09-02 — and the correction matters more than the original item, because the
   mechanism this line proposed cannot work and would have looked like it did.** This read *"Prometheus
   (already scraped, already alerted on by the 30 Grafana rules) is the cheapest surface that a human
   and an alert can both see."* **A Prometheus counter incremented by this sweep would never be
   scraped.** Verified in source on `origin/dev`:
   - `services/metrics.py` builds every metric on `prometheus_client`'s **default process-local
     `REGISTRY`** (imported at `:14`), and `/metrics` is a Starlette **`Route`** (`:200`) served by the
     **app** process.
   - The sweep runs in the **Celery worker**. A counter it increments lives in the worker's registry,
     which nothing serves — **exactly the defect that left `celery-task-failures` watching an empty
     metric for the life of the project** ([core#704]): `celery_tasks_total` is incremented by Celery
     signals in the worker (`metrics.py:155/163/169`) and read from the app's registry.
   - 🆕 **[core#895] is the second half**: the app's own metrics are per-process behind
     `GRANIAN_WORKERS=4` with no `PROMETHEUS_MULTIPROC_DIR`, so even an app-side counter is one
     worker's arbitrary share (measured: the same counter read 3, 7, 6, 7, 7, 6).
   **So an "increment a counter" completion record is unobservable in the worker and unreliable in the
   app**, and — this is the part that makes it worse than no record — a metric that is never served
   returns *no series*, which under `noDataState: OK` reads as healthy. The alert in 17d would be
   green forever.
   **What to use instead**, in order of preference:
   - **A durable row** the sweep writes (its own small table, or a `user.erasure_sweep` audit row with
     counts and no values). It survives restarts, it is queryable, and it is the only option that a
     human can read *after* the fact rather than only while a scrape window is open.
   - **The `celery-exporter` event stream** for liveness only. It is a single process reading the
     broker (`job=celery`, `--purge-offline-worker-metrics 0`), so it is not subject to either defect
     above — but it can only tell you the **task ran**, never what it found. ⚠️ It requires the worker
     to run with **`-E`**; without it the exporter still serves `celery_worker_up` and zero
     `celery_task_*`, which is indistinguishable from "no task has run yet."
   **Do not add a metric to `services/metrics.py` for this** until [core#895] is fixed and the worker's
   registry is actually exported. Both are somebody else's issue and neither is a prerequisite for the
   erasure itself (D14.1) — only for the canary.
4. **Never a value.** Counts only — an erasure log that names what it erased defeats itself, the same
   trap D5 step 8 already guards on the `user.erased` audit row.

**Acceptance is the alert, not the job**: a rule that fires when the sweep has not reported in over its
period. Anything less means the first person to learn it stopped is whoever asks for their data.

## 4. Migration plan — ⚠️ this is FOUR releases, not two

The routing message said *"expand and the code migration ship in release N; dropping the old columns
waits for N+1."* **That is two releases short.** One of the missing ones is where prod breaks (N+1,
below); the other is **N₀**, which is not required by the expand/contract policy but is required by
the clock (D13).

Under blue/green the new container migrates while the old one still serves
([SPEC_EXPAND_CONTRACT_MIGRATIONS](SPEC_EXPAND_CONTRACT_MIGRATIONS.md)). Walk the two-release
version: in N+1 the columns are dropped, but the still-serving **N** code writes `users.email` on every
registration — so the drop breaks the previous release *while it is serving*. And you cannot fix that
by having N stop writing the legacy column, because `users.email` is **NOT NULL**, so an N-code INSERT
that omits it fails immediately.

The sequence that works:

| Release | Migration | Code |
|---|---|---|
| 🆕 **N₀** — the two that expire (**D13**) | `CREATE INDEX CONCURRENTLY ON audit_logs (user_id)` *(own revision or `autocommit_block()` — it cannot run in a transaction)* · `ALTER audit_logs ALTER old_values, new_values TYPE jsonb` | **none** — deliberately code-free, so its revert is one line. ⚠️ Gated on the `migration-roundtrip` probe in D13; if that goes red, this merges into N with the `with_variant` model change |
| **N** — expand | `CREATE TABLE user_pii, invitation_pii, notification_channel_pii, email_change_requests` *(no `audit_log_pii` — §2a/D11)* · ~~redact the existing `audit_logs` diff payloads~~ 🆕 **no audit backfill — §2c: zero rows carry PII** · `ADD COLUMN invitations.token_hash` (nullable) · batched backfill from the legacy columns · **`ALTER … DROP NOT NULL` on `users.email`, `users.full_name`, `invitations.email`, `invitations.token`** | **dual-write** legacy + PII tables · **read from the PII tables** · `get_user_by_email` joins · **the D12 redactor at `log_action`** · D11's five call sites move to internal IDs · erasure, org deletion and email change all ship here |
| **N+1** — stop writing | none | stop writing the legacy columns |
| **N+2** — contract | `DROP COLUMN users.email, users.full_name, users.oauth_provider_id, invitations.email, invitations.token` *(**not** `audit_logs.ip_address` — it stays, unused, until [core#670] decides whether to fill it)* · drop `organizations` name-derived slug backfill if any | none |

Every N operation is on the "safe now" list: `CREATE TABLE`, nullable `ADD COLUMN`, a batched
backfill, and **dropping** a NOT NULL (a widening — the old code keeps writing a value, and nothing
notices the constraint is gone). The **new UNIQUE on `user_pii.email` is safe** despite the general
rule, because it is on a table created empty in the same migration and backfilled from a column that
already carries a UNIQUE — there is no value that can violate it.

⚠️ **Dropping NOT NULL in N is what makes N+1 possible at all.** Leave it on and N+1's code cannot
stop writing.

⚠️ **N₀ is the one release in this plan that is NOT on the "safe now" list, and that is why it is
alone.** `ALTER … TYPE` is a full table rewrite under `ACCESS EXCLUSIVE`, and a type change is on
[SPEC_EXPAND_CONTRACT_MIGRATIONS](SPEC_EXPAND_CONTRACT_MIGRATIONS.md)'s *never in the same
release as the code needing it* list. It is proposed anyway, now, because the operation is instant at
88 kB and unbounded later (D13), and because isolating it means the one uncertain step carries its own
one-line revert instead of dragging the expand back with it. **The probe decides, not this paragraph.**

## 5. Copy and i18n

New user-visible strings, `en.json` first then all **9** locales (`test_all_locales_have_same_keys`
gates it). Per WORKFLOW_RULES §6 (`plans/WORKFLOW_RULES.md`), dynamic error messages are exempt.

| key | English |
|---|---|
| `account.delete_heading` | Delete your account |
| `account.delete_body` | This removes your name, email address and sign-in details permanently. It cannot be undone. |
| `account.delete_button` | Delete my account |
| `account.delete_confirm_heading` | Delete your account? |
| `account.delete_confirm_password` | Enter your password to confirm |
| `account.delete_confirm_org_name` | Type the organization name to confirm |
| `account.delete_what_goes` | Deleted immediately: your name, email address, sign-in details, API keys and personal notification settings. |
| `account.delete_what_stays` | Kept: billing records, for 7 years, as tax law requires. Anything already written to a data warehouse in your own account is untouched — we never delete from your warehouse. |
| `account.delete_backups_note` | Backups are kept for up to 30 days, then expire. We do not edit backups. |
| `account.delete_last_owner` | You are the only owner of {org}. Transfer ownership or delete the organization first. |
| `account.delete_org_too` | {org} has no other members, so it will be deleted with your account. |
| `account.change_email` | Change email address |
| `account.new_email` | New email address |
| `account.email_change_sent` | Confirm the change from the link we sent to your new address. Until you do, keep signing in with your current one. |
| `account.email_change_pending` | Change pending: {email} |
| `account.email_change_done` | Your email address has been updated. |
| `account.email_change_notice_old` | Someone requested a change of the email address on your Datanika account. If that was not you, change your password now. |
| `org.delete_heading` | Delete this organization |
| `org.delete_body` | Connections, pipelines, uploads, schedules and run history are removed. Data already loaded into your warehouse is not touched. |
| `org.delete_button` | Delete organization |
| `org.delete_confirm` | Type the organization name to confirm |
| `org.delete_subscription_note` | The subscription is cancelled first. Billing records are kept for 7 years. |

**22 keys.** `account.` and `org.` prefixes both already exist. Reuse `auth.current_password` from
SPEC_PASSWORD_RESET rather than declaring a second key for the same field.

## 6. 🔗 What this changes about [core#651] — flag to Engineering, who are fixing it now

The routing message asks that #651 be told, because PII separation is supposed to make its redaction
structural. **The transferable part is the principle, not the mechanism**, and mixing them up would
send Engineering the wrong way.

**What does not transfer.** #651's defect is *secrets inside a single encrypted JSON column*
(`connections.config_encrypted`), masked against a 4-key literal (`SENSITIVE_KEYS`) while the canonical
set next door holds 12. You cannot "exclude a table" to fix that — there is no table to exclude. **The
fix stays what `plans/security/BACKUP_EXPORT_SECRETS_2026-08-30.md` recommends**: derive from the
canonical set, plus a test that fails when the two lists diverge.

**What does transfer, concretely and worth building in #651.** The export is a **table-level**
allowlist (4 tables today: connections, uploads, pipelines, transformations). After this spec, "a PII
table is never exported" becomes mechanically checkable:

> Enumerate `Base.metadata.tables`; assert no table whose name ends in `_pii` appears in any export
> envelope. The test **fails on the day someone adds a fifth PII table**, which no allowlist assertion
> can do.

That is the durable half of the lesson: a hand-maintained field allowlist is how the export came to
mask `password` while writing live OAuth refresh tokens in the clear.

⚠️ **One trap specific to writing that guard.** `datanika/models/__init__.py` does **not** export
`Invitation`, `SSOConfig`, `Notification` or `NotificationChannel`, though all four are real tables in
`PUBLIC_TABLES`. **Three of the four carry PII.** Any walker built over `datanika.models.__all__` —
a GDPR exporter, an erasure sweep, a redaction guard — silently skips them and reports success.
**Enumerate `Base.metadata.tables`, never `__all__`.** This is the same defect shape as the other four
hand-maintained-list-pair bugs already on the board ([core#651], [core#654], [core#659], [core#638]).

## 7. Acceptance criteria

Product verifies these on prod after promotion. Several are written so a plausible half-implementation
fails; that is deliberate.

**Schema (release N₀ — D13)**
0a. `audit_logs.old_values` and `new_values` are `jsonb` in prod (`information_schema.columns.data_type`), and `SELECT count(*) FROM audit_logs WHERE new_values ? 'email'` **executes** — today that query is a syntax error, which is the whole point of the conversion.
0b. An index on `audit_logs (user_id)` exists, and `EXPLAIN` on `WHERE user_id = <id>` no longer shows `Seq Scan`.
0c. **`migration-roundtrip` proves the blue/green case**, not just the forward one: a write **and** a read issued through the *pre-migration* model definition (`mapped_column(JSON, …)`) succeed against the converted column. *(Green here is what allows N₀ to ship without a code change; red means it merges into N.)*

0e. 🆕 **The data-preservation harness this chain needs ALREADY EXISTS — use it, do not commission it (2026-09-02).** [core#726] was carried in this spec's routing as an open QA gap (*"the migration round-trip asserts schema only, and this is the first migration where that is not enough"*). **Both of its scope items shipped** in [PR #874] (`dev 2ddc92b`), verified by content on `origin/dev`:
    - **`tests/test_migrations/test_data_preservation_roundtrip.py`** — seeds one identifiable row into **every one of the 26 `PUBLIC_TABLES`** (asserted by *set equality* against `PUBLIC_TABLES`, so a table added by this spec cannot arrive uncovered and silently), then compares **every column of every row by value, keyed by primary key**, across `downgrade -1` → `upgrade head` on a real Postgres. Row loss, row appearance and value change are three distinct findings. It was shown red against a real destructive head, and the first version of that control was **rejected as too weak** because it only proved a NULL→value change.
    - **A `one_way = True` module marker**, read by **AST** (a non-literal *raises* rather than reading as absent, because a classifier that guesses produces a *skip*, and a skip is the same colour as a pass), paired with a required `one_way_reason` that the skip line prints.
    **Three consequences for whoever writes this chain, and they change the work rather than merely informing it:**
    1. 🚨 **`user_pii`, `invitation_pii`, `notification_channel_pii` and `email_change_requests` are added to `PUBLIC_TABLES` in release N (criterion 1), so that suite's set-equality assertion turns RED the moment the migration lands and stays red until the seeder covers all four.** That is not a defect to work around — it is the harness demanding coverage of exactly the tables this spec creates. **Extend the seeder in the same PR as the migration.**
    2. **N+2 is the first migration in this repo that may legitimately need `one_way`.** It drops columns whose values have moved to the PII tables, so a `downgrade` cannot restore them from anywhere. Decide deliberately: either `downgrade` re-derives the legacy columns *from* the PII tables (preferred — it makes the rollback real), or the migration declares `one_way = True` with a reason. **Do not leave it undeclared** — an undeclared destructive downgrade fails the value comparison and the failure names a column, not a decision.
    3. **The N backfill is exactly the shape [core#726] was filed about** — *"it passes a backfill that moves zero rows; it passes a backfill that moves rows into the wrong tenant."* Criterion 2 below is the count check; the harness is the value check. Both, not either.
    ⚠️ **`plans` is the one table whose seeding is not load-bearing** — the migrations seed the priced tiers themselves, so deleting the fixture's `plans` insert is invisible where deleting any of the other 25 is not. Recorded by QA on [core#726]; do not conclude the guard is broken on finding it.
0d. 🆕 **CORRECTED 2026-08-31 — this cited 116, and was weak in the dangerous direction.** The table holds **117 rows**, and per §2c only **30 / 18** of them hold an *object*; the rest hold the JSON literal `null`. A criterion asserting *"all 116 payloads survive"* therefore compares mostly nulls, and **would pass while a bad `USING` clause mangled the 30 rows that actually carry anything** — the exact failure it exists to catch. **Restated:** immediately before the migration record `count(*)`, `count(*) FILTER (WHERE json_typeof(old_values) = 'object')` and the same for `new_values`; after it, all three are **unchanged** and every object-holding payload is **byte-equal as a parsed value**. The two object counts are the load-bearing half — they are the only figures a silent drop can move. *(Nothing reads this column, §2b, so no other signal exists.)*

**Extraction (release N)**
1. `user_pii`, `invitation_pii`, `notification_channel_pii` and `email_change_requests` exist, are in `PUBLIC_TABLES`, and `migration-roundtrip` is green. **`audit_log_pii` must NOT exist** (§2a/D11).
1a. **D11 guard**: no `log_action` payload may contain a key in the **derived** set of D12.2 — `{email, full_name, oauth_provider_id, pending_email, recipient}` ∪ `{ip_address}` today. ⚠️ **Supersedes the hand list this line used to carry** (`{"email", "full_name", "ip_address", "recipient"}`), which omitted `oauth_provider_id` and `pending_email`. Written so it fails on the *next* call site added, not on the five that exist today — and red first against the current `settings_state.py:201/240/304/334`. *(Line numbers re-derived at `cfbb0c7`; they had already moved from the `:179/218/282/312` this line previously cited, which is why the guard must be derived rather than pinned to positions.)*
1aa. **The derived set is not empty and not short.** A test asserts its exact contents against a literal, so a redactor whose module loads before the PII models — and therefore silently derives `frozenset()` — fails loudly instead of redacting nothing (D12.2).
1ab. **Redaction is key-level, proven in both directions.** Given a payload `{"email": "a@b.c", "name": "My Postgres", "role": "editor"}`, the stored row has `email` **replaced with the marker** and `name` and `role` **byte-identical**. ⚠️ **The second half is the load-bearing one**: nothing reads this column (§2b), so a blanket redactor would pass every other criterion here and every test in the suite.
1ac. **The redactor cannot throw.** With a payload that breaks it (a non-serializable value, a cycle), the audit row is **still written**, carrying the `__redaction_failed__` marker. *(`BaseState._audit` swallows exceptions — a raising redactor deletes audit rows silently, D12.5.)*
1ad. **It is installed at the chokepoint, not at the call sites**: a `log_action` call made directly, bypassing `BaseState._audit`, is redacted too. *(Three such callers already exist in `auth_state.py`.)*
1b. 🆕 **REPLACED — §2c.** ~~"The 116 existing prod audit rows carry no email after the backfill."~~ There is no backfill and the existing rows never carried one, so that criterion was **satisfied before any code was written** — a green that proves nothing, sitting in the acceptance list of the spec about greens that prove nothing. **Replaced by a criterion that can fail:** exercising the org-update path at `settings_state.py:146-147` — the site a key-name rule cannot find (D12.1) — produces an audit row whose payload contains **neither the organization's display name nor its slug**, and the test is **shown red against the pre-D11 call site**.
1c. 🆕 **The guard constructs its own input and is never an assertion over `audit_logs` as it stands.** A query-the-table test passes today against a **no-op redactor** (§2c: 0 of 30 payloads contain a PII key, and the five sites that could write one have never fired). Required artifact: the guard **red against a redactor stubbed to return its argument unchanged**.
1d. 🆕 **The guard is already written and committed: `tests/test_services/test_audit_pii_redaction.py`** (Product, 2026-08-31), and its behaviour has been **measured in both directions** rather than asserted:
    - against `dev` as it stands (no redactor): **2 passed, 6 xfailed** — CI green, nobody blocked;
    - against a throwaway reference redactor: **all 6 XPASS(strict) → reported as failures**, and the 11 existing `test_audit_service.py` tests stayed green.
    Both markers matter. `strict=True` means the day the redactor lands, CI goes **red until the markers are deleted** — so "shown red, then green" becomes a step of the implementing PR instead of a sentence in its description. `raises=AssertionError` means a broken harness (an `ImportError`, a renamed fixture) is reported as an **error**, not silently absorbed as the expected failure — which is the [core#709] trap, where a strict xfail was satisfied by an `IndentationError` and the assertion never ran.
    ⚠️ It also carries `test_a_table_shaped_guard_passes_vacuously_today`, which is **expected to pass** and exists to demonstrate the forbidden shape going green with no redactor in the tree. If anyone proposes replacing the constructed-input guards with a query over `audit_logs`, that test is the counter-example, and it will still be passing.

**Migration safety — the [core#809] hazard, which lands hardest on exactly this chain**
1e. 🆕 **Every migration in this chain is checked against an explicit table manifest before review, not against reviewer instinct.** `alembic revision --autogenerate` was emitting `op.drop_table()` for four live tables — including `invitations` and `notification_channels`, both of which this spec touches. The root cause is fixed on `dev`, but **this is a four-release expand → migrate → contract chain, and the contract release is *supposed* to contain drops**, so a spurious one reads as the step doing its job. The manifest, from §4:

    | release | may CREATE | may ALTER | may DROP |
    |---|---|---|---|
    | **N₀** | — | `audit_logs` | — |
    | **N** | `user_pii`, `invitation_pii`, `notification_channel_pii`, `email_change_requests` | `users`, `invitations` | — |
    | **N+1** | — | — | — *(no migration at all)* |
    | **N+2** | — | `users`, `invitations` | **columns only** — `users.email`, `users.full_name`, `users.oauth_provider_id`, `invitations.email`, `invitations.token` |

    🚨 **The discriminating rule, and it is the whole value of the manifest: `op.drop_table(` must appear ZERO times in every migration of this chain, including N+2.** The contract release drops **columns**, never tables. That is what makes the [core#809] failure mode mechanically detectable here — the "but drops are expected in this release" defence covers `drop_column` and never covers `drop_table`, so the two must not be read as one category. A generated migration naming any table outside its row above, or containing `op.drop_table` at all, is rejected without further reading.
    ⚠️ `audit_log_pii` must not appear in any of them (§2a/D11), and `notification_channels` appears in **no** row — it is the parent of a new sidecar, not itself altered, which is precisely the table [core#809] proposed dropping.
2. `SELECT count(*) FROM users WHERE deleted_at IS NULL` equals `SELECT count(*) FROM user_pii` after N's backfill. No user loses their email. *(This backfill is real and survives §2c — `users.email` is populated in 5 of 5. Only the **audit-payload** backfill was deleted; do not conflate them.)*
3. Sign-in works for a password account **and** an OAuth account, and `get_user_by_email` returns `None` for a soft-deleted user. *(A join written without the `deleted_at` filter passes every other test here.)*
4. A new invitation stores a `token_hash`; `SELECT token FROM invitations` yields nothing decodable. The emailed link still accepts.
5. A new signup's `organizations.slug` is `org-<id>` and contains no part of the user's name.

**Erasure**
6. A user with a password deletes their account after re-entering it; a wrong password leaves **every** row byte-identical.
7. An OAuth-only account is offered the org-name confirmation and **no password field**. *(A test covering only the password variant passes on the broken implementation — the [core#623] D6 trap, repeated here on purpose.)*
8. After erasure: 0 rows in `user_pii` for that id; 0 `api_keys`, 0 `password_reset_tokens`, 0 `oauth_grants`, 0 `oauth_tokens`; `users.deleted_at` set; `is_active` false.
8a. 🆕 **All of that is true the moment the request returns** — no job, no queue, no wait (D14.1). *(Query `user_pii` for that id in the same second the confirm request completes. An implementation that marks-and-defers passes criterion 8 an hour later and breaks the `/privacy` sentence, and nothing else here would notice.)*
8b. 🆕 **The sole-owner refusal is synchronous too** (§9a(1)): the user is told at the moment they click, with both exits named. *(A deferred refusal is discovered a week later through no notification at all — see D14.1 reason 3.)*
8c. 🆕 **The residual audit sweep runs and finds ZERO**, and reports the zero (D12.4). *(Expected value is zero because D11 + D12 already prevented it. A sweep that reports nothing and a sweep that never ran are the same reading — this criterion is about the report, not the deletion.)*

8d. 🆕 **§0.2 — the legacy columns are cleared too, and this is asserted DURING the dual-write window, not after it.** Erase a user while release **N** is deployed, then read `users.email`, `users.full_name`, `users.oauth_provider_id` and any `invitations.email`/`invitations.token` for that person **directly, with SQL, not through the ORM's join**. All NULL. 🚨 **Every other erasure criterion passes on the broken implementation** — `user_pii` is empty (8), `get_user_by_email` returns `None` (3), login is impossible, no surface renders anything (14). **A test written through the application cannot see this failure; only a query against the legacy columns can.** *(And it stops being testable at all once N+2 drops them — so if this criterion is not exercised in N or N+1, it is never exercised.)*

8e. 🆕 **§0.1 — a sole-member org is renamed BEFORE it is soft-deleted.** After erasing the sole member of an org, `SELECT name, slug FROM organizations WHERE id = <org>` — **without a `deleted_at IS NULL` filter**, because the row is soft-deleted and the natural query would skip it — returns `Organization {id}` and `org-{id}`, containing no part of the erased person's name. *(§2c measured `organizations.name` carrying a live `users.full_name` in 5 of 5 prod rows. An implementation that renames only surviving orgs passes criterion 11 and fails only this one and criterion 10.)*
9. The erased address can immediately be used to register a new account, and the new account has no visibility of anything from the old one.
10. `grep` the whole database for the erased email and full name returns **zero rows across all 26 tables** — including `organizations.name`, `organizations.slug`, `invitations.email`, `notification_channels.config` and `audit_logs.new_values`. *(This is the criterion that catches a partial implementation. Run it as a query over every text and JSON column, not over the tables you remembered.)*
11. The last owner of a shared org is refused, and the org remains administrable afterwards.
12. A sole-member org is deleted with the user, and `dbt_projects/tenant_{org_id}/` is gone from disk.
13. Deleting an org with a live subscription cancels it at Paddle **first**; a failed cancellation aborts the deletion and nothing is deleted.
14. No surface renders a deleted user's name or address — member lists, audit log page, notification center, invitation lists.

**Email change**
15. Changing to an address already in use is refused, and so is one already pending for another user.
16. The change takes effect only after the link at the **new** address is followed; the old address keeps working until then.
17. The old address receives a notice with no approve link in it.

**Scheduled component (D14.3 / D14.4) — gated on [core#653]**
17a. 🆕 **Celery Beat is demonstrably PRODUCING in production at ship time**, verified on the running system rather than from an issue's state. ⚠️ **Updated 2026-08-31**: this previously read *"[core#653] is closed and..."* with a parenthetical saying `beat` appears in neither `docker-compose.yml` nor the `Dockerfile`. [core#653] **closed 2026-08-30** and `beat` is a live service, so the gate as written is already satisfied — and, read literally, would have blocked. *(An issue's closure is not evidence that a process is running. Ask the exporter: `celery_worker_up` and the `celery_task_*` counters, scraped as `job=celery`. See D14.3 for the two ways that exporter reads healthy while measuring nothing.)*
17b. 🆕 **The job is idempotent under double delivery.** Run it twice against the same state; the second run changes nothing and its counts are zero. *(`task_acks_late=True` means a Beat task can be redelivered after a worker crash, independently of [core#648]'s APScheduler duplication.)*
17c. 🆕 **A completion record carries row counts**, is durable beyond Celery's Redis result backend, and **distinguishes "ran and found nothing" from "raised and was swallowed."** *(`celery_tasks_total` alone cannot: `run_maintenance_task` catches its own DB exceptions and returns zeroed counters, so the two are identical at that metric.)*
    🚨 🆕 **AMENDED 2026-09-02 — the record is a DURABLE ROW, not a Prometheus counter, and the reason is measured (D14.4 item 3).** `services/metrics.py` uses the default process-local `REGISTRY` and `/metrics` is a route in the **app** process; this sweep runs in the **Celery worker**, whose registry nothing serves. A counter incremented here produces **no series**, and no series under `noDataState: OK` reads as **healthy**. Accepting a metric would mean shipping a canary whose failure signal is indistinguishable from success — which is the exact defect this whole section exists to avoid, arriving through the door marked "use the observability we already have."
17d. 🆕 **An alert fires when the job has not reported within its period.** *(Without it, the first person to learn the erasure job stopped is whoever asks for their data.)*
    ⚠️ **Its input is the durable row's timestamp, not a counter** (17c). If the alert is expressed over a metric emitted from the worker it can never fire, which is worse than no alert — it is an alert that reports health it cannot observe.
17f. 🆕 **The completion record is shown able to report a FAILURE, not only a zero.** Force the sweep to raise mid-run; the record must distinguish that from a clean zero-row pass. *(Both the "ran and found nothing" and "raised and was swallowed" states produce the same output at every signal we have today — D14.4 item 2 states the requirement, and this is the artifact that proves it was met.)*
17e. 🆕 **The sweep is NOT inside `run_maintenance_task`** (D14.3, third bullet).

**Cross-cutting**
18. All 22 keys in all 9 locale files. ⚠️ **D14 adds none** — synchronous erasure needs no "pending deletion" copy, and `account.delete_body`'s *"It cannot be undone"* stays literally true. A design that defers the purge would need new keys **and** a re-word of a test-locked sentence on `/privacy` (D14.2).
19. Regression tests written red-first (WORKFLOW_RULES §5 (`plans/WORKFLOW_RULES.md`)) and shown red against the pre-fix code.
20. No email, name or token value appears in any log line or audit-log payload — **including the `user.erased` audit row itself**.

## 8. Ship order

0. 🆕 **Release N₀ first, and it can go today** — the `user_id` index and the `jsonb` conversion (D13). Code-free, one revert, and **the only item in this spec with a deadline**: 88 kB now, a full-table `ACCESS EXCLUSIVE` rewrite on a table nothing ever purges later. It does not block N, but N's audit backfill cannot be *written* without it.
1. **Release N**, one PR per table group but one release: PII tables + backfill + dual-write + `get_user_by_email` join + **the D12 redactor and D11's five call sites**. Nothing user-visible ships yet; this is the foundation both features stand on.
2. **Erasure + org deletion**, same release, **synchronous** (D14.1). This is what closes the launch blocker and makes `datanika.io/privacy` §6 true — and D14.2 shows that the *synchronous* version is the only one that leaves that sentence true.
3. **Email change**, same release if it fits, next otherwise. It is the smaller build and the one a real user hits first, but it does not block launch.
4. 🆕 **The founder's scheduled job ships LAST and separately** (D14.3). ✅ **Its prerequisite is met** — [core#653] closed 2026-08-30 and beat is live in production; verify it is *producing* at ship time rather than re-reading the issue. It is the D12.4 canary plus its completion record — not the erasure. Nothing above waits for it, which is deliberate: **no compliance obligation in this spec may depend on a scheduler that has never fired.**
5. **N+1** and **N+2** are bookkeeping releases and should be scheduled deliberately, not left to drift. A half-finished expand/contract is worse than either end of it: the legacy columns keep accumulating personal data that the erasure sweep does not clear.

**Steps 0-3 are blocked on nothing.** No new credential, no Infra change, no vhost change. The
cross-repo pieces are D6's subscription cancellation, which lands with
[SPEC_BILLING_SELF_SERVICE.md](https://github.com/datanika-io/datanika-cloud/blob/master/docs/specs/SPEC_BILLING_SELF_SERVICE.md), and §9a(1)'s transfer-ownership, which
must land before or with account deletion ([SPEC_ORG_ROLES.md](SPEC_ORG_ROLES.md) §3).
✅ **Step 4 is NO LONGER BLOCKED** — [core#653] closed 2026-08-30 and beat has been live in
production since. **Nothing in this spec is now blocked on another team.** It stays listed last
because it is the least urgent, not because it is gated. ⚠️ It was kept out of the sequence
originally so nothing above inherited the block; that separation is still right, for the
different reason that **no compliance obligation here may depend on a scheduler**, however
healthy it looks on the day it ships.

## 9. Filed separately by this spec

- **`services/client_ip.py` is wired to nothing.** `AuditService.log_action` accepts `ip_address` and no caller passes it, so `audit_logs.ip_address` has been NULL for the life of the product while the UI renders a column for it. That is a feature (capture the client IP for the security trail), not an erasure fix, and folding it in here would let a new data-collection behaviour ride into production inside a privacy change. ⚠️ **Corrected — §2a/D11: `audit_log_pii` is NOT created by this spec.** Building a sidecar for a column that has never held a value protects nothing; it lands with [core#670] instead, when there is data to protect.
- 🆕 **Nothing reads `audit_logs.old_values` / `new_values`** — **[core#694]** (§2b). 30 call sites write them; the audit page renders `ip_address` instead, and the only read in either repo is one round-trip test. The audit trail cannot answer a single one of D12.3's six questions today. That is a feature — build the reader — and it deliberately does **not** ride in here: it is a new data-*exposure* surface (those payloads carry a third party's email until D11 lands), and the same rule that keeps `client_ip.py` out keeps it out. **Ninth instance of *"machinery exists, entry point does not"*, and the second where the orphan is a security control.** ⚠️ It should ship *after* D11 + D12, never before.
- **Legal-page drift** — `datanika.io/privacy` and `/trust` name Hetzner/Germany six weeks after production moved to pointer.gr in Athens, contradict each other on run-log retention, omit the off-site backup host and the actual email processor, and promise deletion routes that do not exist. Filed against the landing repo and handed to Growth; **the 30-day number in §6 must not be edited**, because D7 is built to satisfy it.

## 9a. ⚠️ Two constraints added 2026-08-30 by the polish re-triage — read both before implementing

**(1) Owner-count invariant. Account deletion is a role-count-reducing operation.**
[SPEC_ORG_ROLES.md](SPEC_ORG_ROLES.md) R5 is **binding on this spec**. A user who is the *sole owner*
of an org must not be able to erase their account and leave the org ownerless — that is [core#658]'s
end state arriving from the other direction, and nothing else in the system would catch it. D5's
erasure path must refuse, with copy naming the two exits: **transfer ownership**, or **delete the org**
(D6). Transfer ownership does not exist today; it is specified in SPEC_ORG_ROLES §3 and **must land
before or with this spec's account deletion**. Refusing a sole owner's erasure request with no route
out is worse than the current state, not better.

**(2) GDPR portability (Art. 20) is deliberately out of scope, and stays safe only if we do not claim
otherwise.** This spec builds erasure (Art. 17). Portability is a separate obligation, and it is
genuinely deferrable here: the personal data we hold about a user is small enough that after D1 it is
literally the `users_pii` row plus `invitations_pii`, a manual response is lawful, and self-service is
not required. **What makes it unsafe is a claim, not a gap** — audit item P9 records that the backup
export covers 4 config tables (connections, uploads, pipelines, transformations), contains no personal
data at all, and **must never be described as a data-portability or GDPR export** in the UI, the docs,
`datanika.io/privacy`, or a support reply. Growth owns the pages ([landing#343]); this note exists so
the phrase is not introduced from the product side. If we ever want it self-service, D1's tables make
it near-free — a reason to defer it, not a reason to build it now.

## 10. Docs

Extend the **Authentication** section of `datanika-landing/src/pages/docs/organizations.astro`, as
SPEC_PASSWORD_RESET did — consolidate, do not scatter. Cover: deleting your account and what survives,
deleting an organization and what we do *not* touch in your warehouse, changing your email address,
and the operator route for someone locked out of their address. Cross-link from the Settings account
card. The privacy policy's "email info@datanika.io" route stays as the fallback for people who cannot
sign in at all.

[core#623]: https://github.com/datanika-io/datanika-core/issues/623
[core#648]: https://github.com/datanika-io/datanika-core/issues/648
[core#704]: https://github.com/datanika-io/datanika-core/issues/704
[core#726]: https://github.com/datanika-io/datanika-core/issues/726
[core#895]: https://github.com/datanika-io/datanika-core/issues/895
[PR #874]: https://github.com/datanika-io/datanika-core/pull/874
[core#653]: https://github.com/datanika-io/datanika-core/issues/653
[core#670]: https://github.com/datanika-io/datanika-core/issues/670
[core#693]: https://github.com/datanika-io/datanika-core/issues/693
[core#694]: https://github.com/datanika-io/datanika-core/issues/694
[core#676]: https://github.com/datanika-io/datanika-core/issues/676
[core#638]: https://github.com/datanika-io/datanika-core/issues/638
[core#651]: https://github.com/datanika-io/datanika-core/issues/651
[core#654]: https://github.com/datanika-io/datanika-core/issues/654
[core#655]: https://github.com/datanika-io/datanika-core/issues/655
[core#659]: https://github.com/datanika-io/datanika-core/issues/659
[core#658]: https://github.com/datanika-io/datanika-core/issues/658
[landing#343]: https://github.com/datanika-io/datanika-landing/issues/343
