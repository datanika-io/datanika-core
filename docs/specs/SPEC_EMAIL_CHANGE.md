# SPEC — Changing your email address

**Status:** contract, ready to build
**Owner:** Product
**Closes:** [core#655] AC4 (the last open acceptance criterion on that issue)
**Depends on nothing.** No migration, no new table, no new column — see §2.

[core#655]: https://github.com/datanika-io/datanika-core/issues/655
[core#700]: https://github.com/datanika-io/datanika-core/issues/700

---

## 1. Why this is the remainder, and why it is small

[core#655] was filed as three problems: erasure, org deletion, and email change. Re-measured
against `origin/master` (`ada0987`) on 2026-09-12, the first two have shipped:

| capability | witness | state |
|---|---|---|
| account erasure | `services/user_service.py:1086` `erase_user`, `:1068` `erasure_preconditions`, dialog `ui/pages/settings.py:21` mounted at `:372` | shipped |
| last-owner guard respected | `user_service.py:916` `_check_last_owner`, called from `:521`, `:592`; `account_state.py:211` documents that the dialog does **not** keep a second copy of the rule | shipped |
| org deletion / leaving | `def delete_org`; `leave_org` family, 87 references | shipped |
| **email change** | `EmailChangeRequest` — **0 references in `services/`, 0 in `ui/`** | **not shipped** |

`EmailChangeRequest` is a table whose only producer today is the test that proves it gets
deleted (`tests/test_scripts/test_e2e_seed.py:778-794`, via `scripts/e2e_seed.py:300`).

> ⚠️ **Instrument note, because this table was nearly written the other way round.** The first
> pass at the measurement above used `git grep -cE` with `\|` between alternatives and returned
> **0** for erasure — under `-E`, `\|` is a *literal pipe*, not alternation, so the pattern searched
> for one long literal string. The correct pattern returns 51. A negative control (a name that
> should appear zero times) did **not** catch it and could not have: it only proves the instrument
> can return zero. Every count in this spec was taken with a **positive** control alongside it.

## 2. The schema already exists — do not write a migration

Shipped as part of the PII-separation work (`migrations/versions/e7f2a9c4b1d8_pii_separation_expand.py`):

- **`email_change_requests`** (`models/pii.py:106`) — `user_id`, `token_hash` (sha256 hex, unique,
  indexed), `expires_at`, `used_at`. Already listed in `migrations/helpers.py:42` `PUBLIC_TABLES`.
- **`user_pii.pending_email`** (`models/pii.py:74`, `String(320)`, nullable) — where the requested
  address is held while it is unconfirmed.

Same shape as `PasswordResetToken`, for the reason its docstring gives: the nightly `pg_dump` ships
off-box and is retained 30 days, so what is stored must not be replayable. **Store the sha256 of
the emailed value, never the value.**

The model is deliberately not named with a `_pii` suffix and contributes no key to
`PII_PAYLOAD_KEYS`: the address lives in `user_pii.pending_email`, and reaches that set through
`user_pii`. Preserve that — putting the address on the token row changes the erasure surface.

**Consequence:** this feature is service + UI only. Expand/contract does not apply.

## 3. The invariant this feature must not break

🚨 **`users.email` must not be written until the new address is confirmed.**

This is not stylistic. [core#700] shipped a resend-verification control, and the argument that made
it safe to ship is that **the destination is not an input**: `AccountState.resend_verification`
re-reads the address from the `User` row and never from a state var, so the worst a signed-in
caller can do is mail themselves. `test_the_address_comes_from_the_user_row` pins it.

Writing a user-supplied pending address into `users.email` would convert that control into an open
relay aimed at an arbitrary address, and it would do so **without touching the resend code** — the
test above would still pass, because the address would still be coming from the `User` row. It
would just be a row an attacker now controls.

`user_pii.pending_email` exists precisely so the pending value has somewhere to live that is not
`users.email`. Use it.

### 3a · Is §3 a disclosure? — Product ruling, 2026-09-12

`WORKFLOW_RULES` §4 requires that a finding describing how the product leaks a credential, bypasses
an authorization check, or discloses another tenant's data carry a **neutral title**, with the
mechanism in the private `plans/security/` — **for life**, not until a fix ships. §3 was reviewed
against that rule because this spec sits in a **public** repository, and a spec can disclose as
readily as an issue can.

**Ruling: §3 is a design constraint, not a disclosure. It stays here, in full.**

The rule's own test is whether the text describes *"deployments the finding describes that are
running right now on a released tag."* §3 does not:

- **The flow it constrains does not exist.** `EmailChangeRequest` has **0** references in
  `services/` and **0** in `ui/`, and the only assignments to `users.email` anywhere in `services/`
  + `ui/` are `= None` on the erase paths. No shipped code path writes a user-supplied value to that
  column, so there is no deployment for §3 to describe.
- **What §3 says about shipped code is a safety property**, not a way around one: that
  `resend_verification` reads its destination from the `User` row. Publishing the reason a control
  is safe is not publishing a way past it.
- The hazard it names is an outbound mail relay — **none of the three classes the rule lists.**
  That is not the reason for the ruling, and it would not carry it alone; it is recorded so the fit
  is known to have been checked rather than assumed.

**And the reason that decides it: §3 is preventive.** It exists so this defect is never created,
and Engineering builds from this spec. Removing it would make the vulnerability **more** likely, not
less. The rule's own logic — *"the map outlives the vulnerability"* — presupposes a vulnerability
to map. Applied to a constraint whose entire purpose is to prevent one, it inverts.

🚨 **The condition that flips this ruling, and it has a reader.** The moment the email-change
flow ships, §3 stops describing hypothetical code and starts describing deployed code:

- If **AC7**'s `users.email` assertion exists and has been **seen to fail**, §3 describes a
  *guarded* property and remains a constraint.
- If the flow ships **without** that assertion, §3 becomes a precise, indexed description of where
  to find a live open relay. **It must then move to `plans/security/`, and this section must be
  replaced by a neutral pointer.**

The reader who must notice is **whoever reviews the implementing PR**, because AC7 is already on
their checklist. That is the whole trigger — no separate watcher, and nothing to remember.

⚠️ **The sentence in §3 about the existing regression test still passing is kept deliberately.**
It reads the most like attacker guidance of anything here, and it is the single load-bearing reason
AC7 exists: without it a reviewer reasonably concludes the existing test already covers this case.
Cutting it would remove the justification for the control that keeps this section a constraint.

This ruling is Product's, and is **reversible on the coordinator's or the founder's call.** A
neutral filing is the cheap and reversible direction, and I would not argue against being overruled.

## 4. Rate limiting: a second bucket, not the existing one

The [core#700] limiter is `verify-resend:{user_id}`, 3 per 3600 s, via
`RateLimitService.check_window` — Redis-backed, so shared across all Granian workers, and it
**fails closed** (on a limiter exception the resend is refused, not admitted).

**It does not cover this flow, on two counts:**

1. It is a different call site. Nothing about a new `request_email_change` inherits that bucket.
2. It is keyed by `user_id`, which bounds the **rate** but not the **target**. That was sufficient
   only while the destination was fixed at signup. Once a user names the address, one user id can
   aim its whole allowance at a fresh victim every hour.

**Required:** the change-confirmation send carries its own bucket keyed on **both** the actor and
the target — actor id plus a hash of the new address — and a second, coarser per-user bucket so
that rotating the target does not buy an unbounded total. Fail closed, matching its neighbour.

## 5. Acceptance criteria

**AC1 — Reachable by the user it is for.** The control is available to an account whose email is
**unverified**. [core#655]'s second user problem is "a typo'd signup email locks a user out
permanently"; a control gated on verification would be unreachable by exactly that user and would
not solve the problem it was filed for. *Verified safe to require: `email_verified` gates no
authentication path — the only two references in `auth_service.py` / `auth_state.py` /
`api_v1_routes.py` are the `?verified=1` display var and the confirm call at `auth_state.py:642` —
so a user who never received their mail can still sign in with their password and reach `/settings`.*

**AC2 — Confirmed at the new address before it takes effect.** Submitting the form writes
`user_pii.pending_email` and an `email_change_requests` row, and sends the link to the **new**
address. `users.email` changes only when that link is followed. Single-use via `used_at`, expiring
via `expires_at`.

**AC3 — An address that already has an account is refused, visibly.**
*Decision, and the trade behind it.* A visible refusal is an account-existence oracle, which is why
the password-reset endpoint is deliberately opaque. We accept it here, because: the actor is
**authenticated**, so the enumeration is attributable to a user id we can suspend; §4 rate-limits
it; and the opaque alternative reproduces the exact defect [core#700] was filed about — a user who
typo'd into an address that happens to exist would wait forever for a mail that will never come,
with failure indistinguishable from success. Opacity is the right default and the wrong choice
*here*; state that in the code comment so it is not "corrected" later.

**AC4 — Every outcome is rendered, and they are distinguishable.** Follow [core#700]'s shape:
`queued` / `no_relay` / `failed` / `rate_limited` / `address_taken`, each its own branch.
**`no_relay` renders nothing alarming** — a self-hosted deployment with no SMTP relay is a normal
deployment, and warning that operator would be a false alarm.

**AC5 — On success, the new address is verified.** The user proved control of it by following the
link, so `email_verified` is set `True` in the same transaction that applies the change. A flow that
confirms an address and then asks the user to confirm it again is a bug.

**AC6 — Cancellable.** A pending change is visible in `/settings` and can be abandoned, clearing
`pending_email` and marking the outstanding row used. Without this, a mistyped *second* address
leaves a user staring at a pending state they cannot clear.

**AC7 — Armed regression tests.** At minimum: `users.email` is unchanged after a request and before
confirmation (this is §3, and it is the one that must never go green vacuously); the token is
stored hashed; a second use of the same link fails; an expired link fails; the limiter refuses at
`limit + 1` and the refusal renders. Each assertion must be seen to fail before it is trusted.

## 6. Out of scope

- Changing an email via the REST API or MCP. There is no `/api/v1/me` resource at all; adding one
  is a separate decision.
- Notifying the **old** address that a change was requested. Worth doing and deliberately deferred:
  it is the account-takeover mitigation, and it needs a decision about what that mail says when the
  old address is the typo'd one that never worked. **Trigger that un-defers it:** the first support
  request about an unrecognised email change, or any authenticated-session compromise.
