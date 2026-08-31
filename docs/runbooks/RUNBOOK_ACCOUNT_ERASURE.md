# Runbook — Account erasure on request (operator route)

**Status: this is the interim route.** It exists because `datanika.io/privacy` promises one:

> *"There is no self-service delete button in the product yet — account and organization deletion is
> handled by us on request, by email."* … *"To exercise any of these rights, email info@datanika.io.
> We will respond within 30 days."*

That promise is live in production. Until [core#655] ships the in-product route, **this document is
the only thing that makes it true.** A documented request route that nobody can actually execute is
not a compliance position; it is a claim.

**Retire this file when [core#655] ships.** The shipped path is tested, transactional and audited;
hand-written SQL is none of those. See [`docs/specs/SPEC_PII_SEPARATION.md`](../specs/SPEC_PII_SEPARATION.md)
for the design, whose §7 acceptance criteria this runbook deliberately mirrors so the two cannot drift
into disagreeing about what "erased" means.

---

## 0. Authorization — read before anything else

**Executing this runbook is a manual mutation of the production database, and that is a founder
decision, not an agent's.** An agent may do every read-only step here — §3's precondition queries and
the *before* half of §5's sweep — and prepare the transaction; an agent must not execute §4 without
the founder saying so for that specific request.

If the 30-day clock allows and [core#655] is close, **prefer waiting for the shipped route.** A
tested code path beats a hand-written statement, and the request is answered either way inside the
window. Say so to the requester; "we will do this on <date> via the feature we are shipping" is a
lawful response, and a better one.

---

## 1. What a request must contain before you touch anything

1. **The request is from the account holder.** Default check: reply to the address **on the account**
   and require the confirmation to come back from it. An erasure performed on the wrong account is
   unrecoverable in exactly the way that matters, because the whole point is that the data is gone.

   ⚠️ **That check cannot be the only one, and assuming it can is a live contradiction.**
   `/docs/organizations` — *"If you cannot receive mail at your account's address"* — documents the
   case of a person whose signup address was a typo or is now lost. Demanding a reply from an address
   they do not control makes their request unanswerable, which is the same *"the product says one
   thing and behaves as though it were true"* shape this runbook exists to avoid. Use the fallbacks
   that page commits us to, in this order:
   - **They can still sign in** → ask them to make a small, specific change inside the account (rename
     a connection to a string you supply) and observe it. Control of the session is the proof.
   - **They cannot sign in either** → details only an account holder knows: organization name, names
     of connections or pipelines they created, billing details on a paid plan. Slow is correct here.
2. **What they are asking for.** Erasure of a personal account is not the same as deleting an
   organization, and not the same as leaving one. Ask which if it is ambiguous.
3. **Record the date.** The 30-day clock in the policy runs from receipt, not from when you start.

---

## 2. What is in scope, and what must NOT be touched

The personal-data surface was re-derived three times and corrected four; the current list is
`SPEC_PII_SEPARATION.md` §2 + §2a + §2c. Do not re-derive it from field names — two of the three
earlier passes did exactly that and both were wrong.

### In scope — erase

| # | Location | Note |
|---|---|---|
| 1 | `users.email` | **NOT NULL and UNIQUE** — overwrite, you cannot null it |
| 2 | `users.full_name` | **NOT NULL** — overwrite |
| 3 | `users.oauth_provider_id` | nullable. For **SSO** accounts this holds the email address verbatim (`services/sso_routes.py` passes `oauth_provider_id=email`), so it is not merely a pseudonymous id |
| 4 | `users.password_hash` | **NOT NULL** — overwrite with an unusable value, do not null |
| 5 | `invitations.email` | NOT NULL, no unique constraint. Invitations *they sent*, addressed to other people, are that third party's data — see the caution below |
| 6 | `invitations.token` | 🚨 a **plaintext JWT whose payload contains the invitee's email**. Clearing `invitations.email` alone leaves the address readable with `base64 -d` out of any `pg_dump`. **UNIQUE and NOT NULL** — needs a per-row replacement |
| 7 | `notification_channels.config` | JSON. **Key-level edit only** — see the caution below |
| 8 | `organizations.name`, `organizations.slug` | every signup writes the user's name into **both**; the slug is URL-bearing |
| 9 | `api_keys`, `password_reset_tokens`, `oauth_grants`, `oauth_tokens` | delete the rows outright — credentials, not records |
| 10 | `audit_logs.old_values` / `new_values` | verify rather than assume; see §5 |

### Out of scope — do NOT touch

- **`connections.config_encrypted`, `sso_configs.*_encrypted`, Slack webhook URLs, Telegram bot
  tokens.** These are Fernet-encrypted **organization** property, not personal data. Deleting them
  because one member left destroys the org's pipelines for everyone else. The deletion contract is
  wrong in both directions if you fold them in.
- **Billing records** — cloud `subscriptions`, `charges`, `usage_ledger`. `/privacy` §6 promises
  *"Billing records are retained for 7 years as required by tax law."* These outlive an erasure
  request **by law**. Do not delete them, and tell the requester they are kept and why.
- **Anything in the customer's own warehouse.** We are the processor there, not the controller. We
  never delete from a customer's warehouse — that is also what `/privacy` says.
- **`runs.logs` / `runs.error_message`** — third-party data inside customer pipelines, same reason.

> 🚨 **`notification_channels.config` is one JSON column holding an email address *next to* a Slack
> webhook URL and a Telegram bot token.** Do not delete the row and do not null the column. Remove the
> personal key and leave the rest byte-identical. This is the single easiest place in this runbook to
> destroy something you were not asked to destroy.

> ⚠️ **Invitations they *sent* are a judgement call, not a rule.** The invitee's address is that
> person's personal data, and the requester is not its subject. Erase invitations that are **pending
> and now pointless** (the inviter is gone); leave **accepted** ones alone — the invitee is a user in
> their own right. If in doubt, leave it and say so in the reply.

---

## 3. Preconditions and refusals — check before promising anything

Run these read-only first. Two of them can turn the request into a different request.

```sql
-- Is this user the SOLE OWNER of an org that has other members?
SELECT m.org_id,
       o.name,
       count(*) FILTER (WHERE m.role = 'owner')  AS owners,
       count(*)                                  AS members
FROM memberships m
JOIN organizations o ON o.id = m.org_id
WHERE m.org_id IN (SELECT org_id FROM memberships WHERE user_id = :uid)
  AND m.deleted_at IS NULL
GROUP BY m.org_id, o.name;
```

- **`owners = 1` and `members > 1`** → **REFUSE the account erasure as asked** and reply with the two
  exits: transfer ownership to another member, or delete the organization. Erasing here leaves the org
  ownerless and unadministrable, which is a worse outcome than the current state, and nothing in the
  system would catch it. (`SPEC_ORG_ROLES.md` R5 is binding here.)
- **`members = 1`** → the org is sole-member and is deleted **with** the account. Say so in the reply
  before doing it, not after.
- **Any org with a live subscription** → cancel it at Paddle **first**. If the cancellation fails,
  **abort the whole erasure and change nothing** — a deleted org that still bills is the one failure
  here that costs the requester money.

---

## 4. The mutation

**One transaction. Read §2's "do NOT touch" list again first.**

```sql
-- Pin the ids ONCE, outside the transaction, from the §3 queries. Never match
-- on the address inside the statements below: if the address is wrong you want
-- zero rows, not a different person's.
\set uid    <user id>
\set org_id <org id from §3, ONLY if that org is sole-member and is going too>

BEGIN;

-- 1. Identity. email, full_name and password_hash are all NOT NULL, so these
--    are overwrites — you cannot null them.
--    The tombstone address is per-user (so it satisfies the UNIQUE index and
--    frees nothing for anyone else) and unroutable: `.invalid` is reserved by
--    RFC 2606 and can never resolve.
UPDATE users
   SET email             = 'erased-' || id || '@erased.invalid',
       full_name         = 'Deleted user',
       oauth_provider_id = NULL,
       password_hash     = '!erased',       -- see the caution below
       is_active         = false,
       deleted_at        = now()
 WHERE id = :uid;

-- 2. Credentials — delete outright.
DELETE FROM api_keys              WHERE user_id = :uid;
DELETE FROM password_reset_tokens WHERE user_id = :uid;
DELETE FROM oauth_tokens          WHERE user_id = :uid;
DELETE FROM oauth_grants          WHERE user_id = :uid;

-- 3. Invitations they sent that are still pending. BOTH columns — the token
--    carries the address independently of the email column.
--    🚨 `invitations.token` is UNIQUE and NOT NULL, so a constant ('' or NULL)
--    violates the index on the SECOND row. Use a per-row value.
--    The status enum is `pending|accepted|expired|cancelled`; there is no
--    `accepted_at` column to test against.
UPDATE invitations
   SET email = 'erased@erased.invalid',
       token = 'erased-' || id
 WHERE invited_by_user_id = :uid
   AND status = 'pending';

-- 4. Notification channels: remove ONE key, keep the rest of the JSON object.
--    `config` is `JSON`; `-` and `?` are jsonb-only, hence the cast round-trip.
--    In psql `?` needs no escaping (`??` is a JDBC-ism and is wrong here).
--    The WHERE clause restricts this to EMAIL channels, so nothing else in the
--    org's alerting is touched.
UPDATE notification_channels
   SET config = ((config::jsonb) - 'email')::json
 WHERE org_id IN (SELECT org_id FROM memberships WHERE user_id = :uid)
   AND (config::jsonb) ? 'email';

-- 5. Org name and slug, IF they were derived from this person's name.
--    🚨 Look before you leap: signup writes `<full name>'s Org` and
--    `slugify(<full name>)-<id>`, but an org that was RENAMED by hand must not
--    be clobbered back to a generated name. Run this SELECT first and only
--    proceed for rows where the person's name is visibly in them:
--       SELECT id, name, slug FROM organizations WHERE id = :org_id;
UPDATE organizations
   SET name = 'Organization ' || id,
       slug = 'org-' || id
 WHERE id = :org_id;

COMMIT;
```

### Three cautions on the statements above

- ⚠️ **If any statement fails, the whole thing rolls back** — that is the point of the single
  transaction, and it is why you must not run these one at a time in separate sessions. The most
  likely failure is step 3 hitting the `invitations.token` UNIQUE index. `ROLLBACK`, fix the
  statement, start again from the top. **Never patch forward mid-erasure**: a half-applied erasure is
  the one state where you can no longer tell what you have and have not removed.
- ⚠️ **`password_hash = '!erased'` is not a bcrypt hash, and that is deliberate — but do not rely on
  it alone.** Nothing can hash to it, and `is_active = false` plus `deleted_at` are what actually stop
  the account authenticating. If you would rather not leave a non-bcrypt value where bcrypt might one
  day be handed it, substitute a real hash of 32 random bytes; the erasure outcome is identical.
- ⚠️ **Telegram channels are a judgement call and are deliberately not in step 4.** A Telegram
  `chat_id` may be the erased person's private chat *or* a shared group the whole org relies on, and
  the two are indistinguishable in the column. Look at the org's channels, decide, and act separately.
  Blanket-removing `chat_id` would silently switch off an organization's alerting.

Then, **outside** the transaction: remove `dbt_projects/tenant_{org_id}/` from disk if the org was
deleted. It is not in the database and no SQL above reaches it.

> ⚠️ **The audit row you write for this must not contain the address or the name.** Log the user id
> and the action. An erasure that records what it erased has not erased it.

---

## 5. Verification — and why the obvious query is not evidence

The acceptance test is *"grep the whole database for the address and the name, get zero rows across
every table."* A sweep that returns zero because it is **broken** reads identically to one that
returns zero because the data is gone. This has already happened elsewhere in this codebase often
enough to have its own note in `CLAUDE.md`.

🚨 **So run the sweep BEFORE the mutation and require it to find the address.** That is the negative
control, and it is not optional here:

1. **Before §4** — run the sweep. It must return **> 0 rows**. If it returns zero, your query is
   wrong; fix it now, while you still have a positive case to test against. Once the data is gone,
   you can never distinguish a working sweep from a broken one again.
2. **After §4** — run the identical query. It must return **0**.

```sql
-- Generates a UNION ALL over every text-ish and JSON column in the public
-- schema, so it cannot miss a table you forgot. Run the OUTPUT of this query,
-- don't eyeball the column list.
\set needle 'the.address@example.com'

SELECT string_agg(
  format('SELECT %L AS tbl, %L AS col, count(*) FROM public.%I WHERE %I::text ILIKE %L',
         table_name, column_name, table_name, column_name, '%' || :'needle' || '%'),
  E'\nUNION ALL ')
FROM information_schema.columns
WHERE table_schema = 'public'
  AND data_type IN ('character varying','text','json','jsonb','character');
```

Run it once with the **email** as `needle` and once with the **full name**. The name matters
separately: it is the one that hides in `organizations.name` and in the slug.

> 🚨 **`audit_logs.old_values` and `new_values` are `json`, not `jsonb`, until [core#693] ships.** The
> containment operator you would reach for — `WHERE new_values ? 'email'` — is a **syntax error** on a
> `json` column, so a verification written that way does not return zero, it fails to run at all. The
> sweep above casts to `text` deliberately, which works on both. This is exactly the capability
> [core#693] adds, and it is why that migration is the one item in the spec with a deadline.

Also confirm, per `SPEC_PII_SEPARATION.md` §7:

- `user_pii`-equivalent data gone: 0 rows for that id in every table in §2's "in scope" list.
- `users.deleted_at` set and `is_active` false.
- **The erased address can immediately be used to register a new account**, and the new account sees
  nothing from the old one. (This is what the per-user tombstone buys — a shared constant would keep
  the UNIQUE index occupied and silently block re-registration.)

---

## 6. What this runbook cannot reach — and what to tell the requester

Say these plainly in the reply. All three are already what `/privacy` says, so the reply and the
policy agree:

- **Backups.** Nightly `pg_dump`, 7 days local and 30 days off-site. **We do not edit backups** — the
  copies expire, they are not surgically altered. A restore inside that window would reintroduce the
  data, and the restore procedure must re-apply this runbook. Say "within 30 days", because that is
  what the policy commits to and what the backup tail actually is.
- **Your own warehouse.** Anything a pipeline already loaded into the customer's warehouse is theirs
  and is untouched. We never delete from it.
- **Billing records**, kept 7 years — see §2.

---

## 7. Record keeping

Append one line to the erasure log — request date, account id (**not** the address), what was refused
if anything, date executed, who authorized it. Keep the id only: a log of erasure requests that
records the addresses is a list of exactly the people who asked to be forgotten.

---

## 8. When [core#655] ships

Delete this file and point `/privacy` at the in-product route. Until then it stays, and it stays
accurate: **if you change what "erased" means in the spec, change it here in the same batch**, because
this document and the shipped feature are two implementations of one promise, and the promise is
published.

Three places say the same thing today and must be changed together:

| where | what it says |
|---|---|
| `datanika.io/privacy` §6 and §7 | the rights, the 30 days, and *"no self-service delete button yet"* |
| `datanika.io/docs/organizations` | *"Deleting your account or your organization"* + the locked-out routes |
| this file | how it is actually done |

[core#655]: https://github.com/datanika-io/datanika-core/issues/655
[core#693]: https://github.com/datanika-io/datanika-core/issues/693
