# SPEC — Bounding the `/signup` account-existence oracle

**Author**: Product · **Date**: 2026-09-02 · **Status**: contract, ready for Engineering
**Tracking**: [core#639](https://github.com/datanika-io/datanika-core/issues/639)
**Implementation**: Engineering. Product owns this spec and the acceptance criteria.
**Verified against**: `origin/dev` @ `11ab292`.
**Predecessor**: [SPEC_PASSWORD_RESET.md](SPEC_PASSWORD_RESET.md) **D7**, which chose to make
`/forgot-password` opaque, said out loud that the opacity was therefore *partial*, and filed
[core#639] rather than pretending the inconsistency away. **This spec is that follow-through.**

---

## 1. What is true today, re-measured

`/signup` answers *"does this address have a Datanika account?"* in one request, for anyone, without
limit.

- `services/user_service.py:35` — `raise UserServiceError("Email already exists")`.
- `ui/state/auth_state.py:465` surfaces it **verbatim**, with the rationale in a comment (#128):
  *"Surface them verbatim so users can recover instead of bouncing off a generic toast."*
- **The production CAPTCHA is a no-op.** `CaptchaService.verify()` returns `True` unconditionally when
  `enabled` is false, and `enabled` is `bool(site_key and secret_key)`. Production `.env.docker` has no
  `RECAPTCHA_*` keys, so both are `""`. Unchanged since the issue was filed.
- **There is still no rate limit on any auth route.** `grep` for `RateLimitService` in
  `auth_state.py` and `user_service.py`: **zero hits.**

🚨 **The #128 reasoning is good and this spec does not throw it away.** A signup form that refuses
without saying why is genuinely worse for the user, and it is a *primary funnel*. What is being fixed
is that the disclosure is **unbounded**, not that it exists.

### 1a. 🆕 What changed since [core#639] was filed, and it changes the recommendation's cost

The issue recommended *"option 1, and not before we have users"*, reasoning that option 1
*"reuses machinery #623 is building anyway."* **That machinery is now built, shipped and in production
use**, two files away:

| | |
|---|---|
| `ui/state/password_reset_state.py:35` | `_limiter = RateLimitService()` |
| `:44-51` | `_allow(bucket, limit)` → `_limiter.check_window(bucket, limit, window_seconds=_WINDOW)` |
| `:38-41` | `_EMAIL_LIMIT = 3`, `_IP_LIMIT = 10`, `_CONSUME_LIMIT = 20`, `_WINDOW = 3600` |
| `:70-78` | `_client_ip()` → `resolve_client_ip(dict(self.router.headers.raw_headers))` — **the answer to "how does a Reflex event handler get the client IP", which is the only non-obvious part of this work** |
| `password_reset_service.py:55-71` | `email_bucket` (**address hashed** — keying on plaintext *"would turn the Redis keyspace into a readable list of accounts, an enumeration oracle reachable by anyone who can read Redis"*), `ip_bucket`, `consume_bucket` |

So the estimate in the issue is stale in the cheap direction: the half of option 1 that does not need
a credential is now **a copy of a pattern, not a build**. `account_state.py` is a second in-production
consumer.

---

## 2. Decisions

### D1 · Option 1 — keep the disclosure, bound it. Ship the rate limit now; the CAPTCHA is not a dependency.

Confirming the issue's own recommendation, with one change: **do not wait for users, and do not wait
for reCAPTCHA keys.**

- **Not for users**, because the founder's [core#623] decision — *"do everything right before we
  launch"* — means correctness debt no longer rides into launch on the pre-traffic ship-immediately
  rule. The first real signup is exactly the moment there is finally something to enumerate, and it is
  the worst moment to be starting this.
- **Not for reCAPTCHA**, because those keys are **human-locked** and the founder queue is not a
  schedule. Option 1 has two halves and only one of them needs a credential. **Ship the half that does
  not.** ⚠️ Gating the whole of option 1 on the CAPTCHA half is how a cheap fix inherits an expensive
  block — the shape [SPEC_PII_SEPARATION](SPEC_PII_SEPARATION.md) D14.3 got wrong in the other
  direction, where a stale blocker parked work that was no longer gated.

### D2 · ⚠️ Say what this buys, precisely — because it is less than "fixed"

A rate limit makes **bulk** enumeration expensive. It does **nothing** against a **targeted** query:
*"does alice@bigcorp.com have a Datanika account?"* is one request, and one request is always under
any limit worth having.

**Decision: that residue is accepted, and it is recorded rather than closed.** At 0 users the answer
leaks nothing of value. It stops being acceptable the moment we have a customer whose *use of
Datanika* is itself sensitive — a competitor checking whether a named company is a customer, or an
attacker confirming a target before phishing them.

**Only option 2 (accept the submission either way, and send a "someone tried to sign up with your
address" email) closes the targeted case**, and it remains correctly deferred: it lands on the primary
signup funnel, it is a real feature with its own email path, and it defends against an attack that
presupposes a user list worth stealing.

🚨 **This must be written into the issue and the code comment, not left in a spec nobody re-reads.**
The failure mode is somebody later reading "signup is rate-limited" and concluding enumeration is
solved. **A bound is not opacity.**

### D3 · The limits, and why they are not the reset limits

Reset is 3/hour per address and 10/hour per IP. Signup is a different distribution — a person signs up
**once, ever** — so the per-address dimension is nearly meaningless and the per-IP dimension carries
the weight.

| bucket | limit | reasoning |
|---|---|---|
| **per IP**, `signup:ip:<ip>`, 1 hour | **10** | matches the reset IP limit deliberately. Enough for a shared office or a NAT'd network, far below anything useful for enumeration |
| **per address**, `signup:email:<sha256>`, 1 hour | **3** | catches the retry loop of a person who mistypes, and stops one address being used to probe timing. **Hash the address**, for the reason `password_reset_service.py:55-61` gives: a plaintext key turns the Redis keyspace into a readable account list, which is a *lower* bar than reading the database |

⚠️ **The per-IP limit is the one that will need revisiting**, and the trigger is corporate NAT, not
attack volume: ten colleagues signing up from one office in one hour is a plausible good outcome and
an indistinguishable one. **Do not raise it pre-emptively** — record the trigger and let the first
support ticket move it. *(Recorded here so a future reader does not treat 10 as measured. It is not; it
is copied from a route with a different usage shape.)*

### D4 · Fail **closed** on a Redis failure — and this is the opposite of a generic answer

`_allow` propagates Redis exceptions deliberately: *"a limiter that fails open is not a limiter."*
Applying that to signup means **Redis down ⇒ nobody can register**, which for a signup funnel is
normally an unacceptable trade.

**Decision: fail closed anyway, and the reason is specific to this deployment rather than a
principle.** Since [core#646], Redis holds **Reflex session state** (`REFLEX_REDIS_URL`), not just
Celery queues. With Redis down, a user who registered could not stay logged in — `AuthState` is
per-process, `check_auth` redirects on an empty token, and the app is already unusable. So failing
closed on signup **forfeits nothing that is not already lost**, and it avoids the far worse outcome of
the limiter silently disappearing during exactly the incident an attacker would pick.

⚠️ **Show the user the same generic "temporarily unavailable" message the reset page uses
(`unavailable`), never a limiter error.** A distinct message is itself a signal.

### D5 · The refusal message must not become a second oracle

When the limit is hit, `/signup` must return the **same** message regardless of whether the address
exists. Getting this wrong recreates the leak at one remove: *"too many attempts"* for a known address
and *"email already exists"* for an unknown one is still an answer.

**Order of operations, and it is load-bearing: check the limit BEFORE the existence lookup.** If the
lookup runs first, a limited request still consumes a database read and — depending on where the
message is built — can still branch on the result.

### D6 · The CAPTCHA no-op must stop being invisible

Separate from the rate limit, and cheap. `CaptchaService.verify()` returning `True` when unconfigured
is a **green that proves nothing**: the class exists, the call sites exist, `/login` and `/signup` both
call it, and it has never checked anything in production.

**Decision: do not change the fallback** — returning `True` when unconfigured is right for
self-hosters, who must not be forced into a Google dependency. **Make the state legible instead:**

1. A single `WARNING` at startup when `CaptchaService.enabled` is `False`, naming the two settings.
2. A test asserting `enabled is False` under the default settings, so the no-op is a **documented,
   asserted** property rather than a discovery.

⚠️ **Do not make it fail the deploy.** That converts an open-source default into a hard dependency on
a Google account.

### D7 · Out of scope, deliberately

- **`InvitationService.create_invitation`'s leak** (*"{email} is already a member of this
  organization"*) — only reachable inside an org you already belong to, where the member list is
  visible anyway. Materially weaker; not worth the copy churn.
- **`/login`** — a login form that distinguishes "no such account" from "wrong password" is the same
  class, and it is not measured here. **If Engineering finds it does, file it; do not fix it inside
  this change** — it has its own funnel cost and its own copy.
- **Option 2 in full** (§D2).

---

## 3. Copy and i18n

`en.json` first, then all **9** locales. The existing "Email already exists" text is a *dynamic error
message* and therefore i18n-exempt under WORKFLOW_RULES §6; the new refusal is a fixed string and is
not.

| key | English |
|---|---|
| `auth.signup_rate_limited` | Too many sign-up attempts from this network. Try again in an hour. |

**1 key.** ⚠️ Reuse the reset page's existing `unavailable` copy for the D4 Redis-failure case rather
than adding a second key for the same condition.

---

## 4. Acceptance criteria

1. **The 11th signup attempt from one IP within an hour is refused**, and the 10th succeeds. *(Assert
   both ends. A limiter tested only at its refusal has not been shown to permit.)*
2. 🚨 **The refusal is byte-identical for a registered and an unregistered address.** *(This is the
   criterion that stops the fix becoming a new oracle — D5. Compare the rendered strings, not the
   status codes.)*
3. **The limit is checked before the existence lookup**, shown by the refused request performing **no**
   `get_user_by_email` query. *(D5. A test that only reads the response passes on the wrong order.)*
4. **The Redis bucket key contains no plaintext address.** Read the key back from Redis and assert the
   address does not appear in it. *(D3 — and this is exactly how `password_reset_service.py` justifies
   its own hashing, so the negative control is a working example one file away.)*
5. **With Redis unreachable, signup is refused with the generic unavailable message** — not the
   limiter's error, and not a success. *(D4.)*
6. **A successful signup is unaffected** — the happy path, the #128 verbatim "Email already exists"
   for a genuine duplicate under the limit, and the funnel all behave as they do today. *(The whole
   point of option 1 over option 2 is that the funnel does not change; a criterion that does not
   assert this cannot tell the two options apart.)*
7. **`CaptchaService.enabled` is `False` under default settings, asserted by a test**, and a startup
   WARNING names the two missing settings. *(D6.)*
8. **The key is in all 9 locale files.**
9. 🚨 **[core#639] is updated to say what remains open** — targeted single-address enumeration, and
   option 2 as the only thing that closes it. *(D2. Closing the issue on the rate limit alone records
   "signup enumeration: fixed", which is false in the direction that matters later.)*

---

## 5. Ship order

1. **D1 + D3 + D4 + D5** — the limiter, both buckets, fail-closed, order of operations, one i18n key
   in nine locales. This is a copy of `password_reset_state.py:35-51` and `:70-78`.
2. **D6** — the CAPTCHA legibility. Two lines and a test; independent of the above.

**Blocked on nothing.** ⚠️ **Specifically not blocked on the reCAPTCHA keys**, which are human-locked
and are *not* on the path of either step.

[core#128]: https://github.com/datanika-io/datanika-core/issues/128
[core#623]: https://github.com/datanika-io/datanika-core/issues/623
[core#639]: https://github.com/datanika-io/datanika-core/issues/639
[core#646]: https://github.com/datanika-io/datanika-core/issues/646
