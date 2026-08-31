# SPEC — Password change and password reset

**Author**: Product · **Date**: 2026-08-30 · **Status**: contract, ready for Engineering
**Tracking**: [core#623](https://github.com/datanika-io/datanika-core/issues/623)
**Implementation**: Engineering (core). Product owns this spec and the acceptance criteria.
**Verified against**: `origin/dev` @ `f6851eb`, the production `.env.docker`, and the live app.

---

## 1. The gap, in one line

`password_hash` is written in exactly two places — `register_user()` and
`find_or_create_oauth_user()` — and read in exactly one, `authenticate()`. **Nothing in Datanika can
change it.** A user who forgets their password has no recovery path; the only remedy is a manual
`UPDATE` on the prod box by whoever holds SSH. Full evidence is on [core#623]; it is not repeated here.

This spec covers both halves. They share `UserService` surface and one column, so they are designed
together; they can ship in two PRs.

---

## 2. Decisions

These are the calls I am making so Engineering does not have to make them inside an implementation.
Each one is a decision, not a preference — if you disagree with any, change *this file* first.

### D1 · Placement: a Settings card and two public Reflex pages. No new route for the change form.

**Change password** goes in a new **`account_card()`** on `/settings`, rendered **first**, above
`org_profile_card()`.

Every existing card on that page is org-scoped (Organization Profile, Members, Invite, Notifications,
API Keys, Backup & Import). This is the first *user*-scoped control on the page, and the issue's DOM
walk found the same asymmetry — "every user-level control is org-scoped". So the card carries a
subtitle saying so in one line (`account.subtitle`), and sits at the top rather than being buried
between two org cards. A separate `/profile` route would mean a new page and a new sidebar entry for
one form; the sidebar already routes people to `/settings` and that is where they will look.

**Reset** gets two **public Reflex pages**: `/forgot-password` and `/reset-password`. Registered with
`app.add_page(...)` and, like `/login` and `/signup`, **without** `AuthState.check_auth` in `on_load`.

> ⚠️ **Not backend Starlette routes, and this is load-bearing.** The Apache vhost forwards an explicit
> list (`/_event`, `/api/`, `/mcp`, `/ping`, `/healthz`, `/readyz`, `/_upload`, the OAuth AS paths) to
> `:8000` and **everything else to `:3000`**. A new backend route outside `/api/` silently serves the
> Reflex SPA instead of itself — that exact failure hit `/mcp` and every OAuth discovery document, and
> both needed an Infra vhost change to fix. Reflex pages need **no vhost change and no Infra
> involvement**; the state handlers reach the backend over `/_event`, which is already proxied.
> `/oauth/consent` is the precedent: a Reflex page that does real backend work.

The existing email routes (`/api/verify-email`, `/api/accept-invite`) are backend routes because they
*act and redirect*. Reset must render a **form**, so it is a page. See D3 for why that distinction is
also a security requirement and not just a layout one.

### D2 · Token: 32 bytes of `secrets`, SHA-256 at rest, 60-minute TTL, single-use, one live per user.

| Property | Decision |
|---|---|
| Value | `secrets.token_urlsafe(32)` — opaque, **not a JWT** |
| At rest | `token_hash` = SHA-256 hex, `String(64)`, `unique=True` |
| TTL | **60 minutes**, in an `expires_at` column, checked against the column |
| Single use | `used_at` timestamp, set in the same transaction as the password write |
| Concurrency | Requesting a new link **invalidates every outstanding one** for that user |

**Why not a JWT.** The two existing email flows disagree with each other, and only one of them is
right for this. `verify_email` uses a bare `email_verify` JWT with no DB row — it is replayable until
`exp` and cannot be revoked. `accept_invite` uses an `Invitation` row with a `status` state machine —
single-use, revocable. **A reset token is a full account-takeover primitive, so it must be the second
kind.** Single-use is a property of stored state; a signed string cannot have it.

**Why hashed, unlike `Invitation.token`.** `Invitation` stores its JWT verbatim in a `String(500)`
column and looks it up by equality. Do not copy that here. Nightly `pg_dump` ships off-box to Aweb
(`185.226.65.96`) and is retained 30 days — a plaintext reset token in a dump is a live key to an
account, sitting on a second server. An invitation token only grants membership of one org.

We already do this correctly elsewhere, and the codebase already states the principle:
`models/mcp_oauth.py` keeps `OAuthGrant.code_hash` as SHA-256 hex, `String(64)`, unique, **NULLed on
redemption**, and its docstring says *"kept as SHA-256 hashes, so a database read yields nothing
replayable."* **Mirror `OAuthGrant`, not `Invitation`.**

SHA-256, not bcrypt: the token already carries 256 bits of entropy, so there is nothing to stretch,
and the lookup has to be an indexed equality on the hash.

**Why 60 minutes.** `email_verify` is 24 h and invites are 7 days; both are far too long for this, and
copying either would be inheriting a TTL chosen for a much weaker capability. 15 minutes is too short
— relay latency plus a user who checks mail on a phone means frequent failure, and every failure
sends them back to the request form, which *increases* the number of live tokens. 60 minutes is the
smallest window that does not manufacture retries. The TTL is stated in the UI copy and in the email,
so the user is never guessing.

### D3 · The GET that renders `/reset-password` must not consume the token.

The page load may only check that the token **exists, is unexpired and is unused**, in order to decide
between rendering the form and rendering "this link is no longer valid". It must **not** set
`used_at`. Consumption happens only on the explicit submit event that carries the new password.

**This is not theoretical.** Corporate mail security (Defender for Office 365 Safe Links, Proofpoint,
Barracuda) fetches every URL in an inbound message before the recipient sees it. A GET that consumes
the token means the scanner burns it and the user's own click always lands on "already used" — a bug
that reproduces only for users at companies with mail scanning, i.e. exactly our target customer, and
never for us.

Also on page load: read the token into state, then
`rx.call_script("window.history.replaceState({}, '', '/reset-password')")` to drop it from the visible
URL, browser history and any `Referer`. The page must contain **no external links** while the token is
in the URL.

### D4 · Sessions after a successful change or reset — say what is true, not what sounds reassuring.

**Datanika has no durable session to invalidate, and the copy must not claim otherwise.**

The facts, all verified: `AuthState` is a plain `rx.State`; `access_token` is a server-side Reflex
state var, **not a cookie or `localStorage`**; `is_authenticated` is just `access_token != ""`.
`rxconfig.py` configures no `redis_url` and the production `.env.docker` contains **no `REFLEX_*`
keys**, so the state manager is **in-memory, per-process**. Consequences: a hard reload already drops
the session ([core#472]), and every blue/green deploy discards every session on the box.

So do the part that is enforceable and state the part that is not:

1. **Add `users.password_changed_at`** (nullable `DateTime(timezone=True)`). Set it on both change and
   reset.
2. **Reject any refresh token whose `iat` predates it.** Both JWTs already carry `iat`, so this needs
   no new claim and no new table. This is real revocation of the only long-lived credential we mint
   (7 days). It is currently latent — nothing redeems a refresh token today — so implement the check
   **in `AuthService.decode_token`'s caller at the point of redemption**, and it starts protecting the
   moment a redemption path exists.
3. **Do not add a DB read to the access-token path.** Access tokens live 15 minutes. Buying a
   worst-case 15-minute window costs a database round trip on every authenticated event; that is a bad
   trade at any traffic level.
4. **Do not auto-sign-in after a reset.** Redirect to `/login?reset=1` with a success callout. An
   emailed link that produces a live session makes the email itself a bearer credential, and it skips
   the one moment where the user demonstrates they know the password they just set. It also matches
   what `verify_email` and `accept_invite` already do — both land on `/login?<flag>=1`.
5. **Do not revoke API keys.** They are org-scoped machine credentials with their own revocation UI;
   killing an org's pipelines because one member forgot a password is worse than the residual risk.
   Instead, the success state links to Settings → API Keys with `account.review_api_keys`.

**Copy consequence:** never write "you have been signed out everywhere." Write what happens — this
browser is signed out and must sign in again with the new password.

### D5 · Rate limiting: reuse `RateLimitService`, two buckets, and get the client IP right.

**There is no rate limiting on `/login`, `/signup` or any auth route today, and the CAPTCHA that looks
like it covers them does not.** `CaptchaService.verify()` returns `True` unconditionally when
`enabled` is false, and production's `.env.docker` contains **no `RECAPTCHA_*` keys at all**. The
signup captcha is a no-op in prod. Do not design this form assuming a captcha exists.

`RateLimitService` (Redis sliding window, already used by `/api/v1/*`) is the right mechanism. Its
`api_key_id: int` parameter is used for nothing but building the Redis key — **generalise it to take a
`bucket: str`** and keep the existing call site passing `f"{api_key_id}"`. Do not build a second
limiter.

| Endpoint | Bucket | Limit |
|---|---|---|
| request a link | `pwreset:email:{sha256(normalised_email)}` | 3 / hour |
| request a link | `pwreset:ip:{client_ip}` | 10 / hour |
| submit a new password | `pwreset:consume:{client_ip}` | 20 / hour |

Hash the email in the key so the Redis keyspace does not become a readable list of accounts.

**Over-limit on the email bucket returns the identical generic response as success.** A visible "too
many requests" scoped to an email address is an oracle: send four, watch the fourth differ, and you
have learned the address is real. The IP bucket may surface a visible "try again later" — it says
nothing about any account.

> ⚠️ **`request.client.host` is `127.0.0.1` for every request in production.** Traffic arrives through
> Cloudflare → Apache → `127.0.0.1:8000`. A per-IP limiter reading `request.client.host` collapses the
> entire internet into one bucket, so the tenth password-reset request *from anyone* locks out
> *everyone* — a global outage that cannot reproduce in dev. Read `CF-Connecting-IP`, falling back to
> the **last** hop of `X-Forwarded-For` (the one our own Apache appended), and never the client-supplied
> leftmost value. If Engineering cannot establish the real client IP with confidence, **ship the email
> bucket alone** — a limiter keyed on the wrong thing is worse than no limiter.

Reflex handlers do not receive a `Request`. Getting headers into a state handler is real work
(`self.router.headers`); if it turns out not to be available, that is the case for putting *only* the
consume step behind a small `/api/` route. Flag it rather than guessing.

### D6 · OAuth-only accounts: "Set a password", never a "current password" they cannot satisfy.

`find_or_create_oauth_user` writes `hash_password(secrets.token_urlsafe(32))` — a hash no human can
produce. Showing such a user a "Current password" field is showing them a field they can never fill.

- **Settings card**: if the account has never had a password set, render **"Set a password"** — new +
  confirm only, no current-password field, with `account.set_password_hint` explaining why. This is a
  feature, not a workaround: today an OAuth user who loses their Google account loses Datanika.
- **Reset flow**: **yes, OAuth-only accounts get reset emails.** Refusing would create a
  distinguishable response that enumerates *which* accounts are OAuth-backed, and would strand exactly
  the users who most need a second route in. Setting a password does not unlink the provider; they
  keep both.

> ⚠️ **The obvious discriminator is wrong.** `oauth_provider IS NOT NULL` does **not** mean "has no
> password": `find_or_create_oauth_user` backfills `oauth_provider` / `oauth_provider_id` onto a
> **pre-existing password account** on first social login. Gating on it would drop the
> current-password re-verification for users who *do* have a password — a real weakening, since it
> lets anyone holding a hijacked live session change the password without knowing the old one.
>
> **The discriminator must be a stored fact about whether a password was ever set by a human.** The
> cheapest correct version reuses D4's column: set `password_changed_at` in `register_user()` as well,
> backfill existing password-created rows to `created_at`, and leave it NULL for OAuth-created rows.
> `password_changed_at IS NULL` then means exactly "never set a password". Engineering may pick a
> different column; it may not pick an inference from `oauth_provider`.

### D7 · Enumeration: opaque here, and file the `/signup` leak as its own work. ⭐

This is the one genuine trade-off in the spec, so here is the reasoning rather than the conclusion.

**We already enumerate, deliberately, and on a bigger surface.** `register_user` raises
`UserServiceError("Email already exists")`, and `AuthState.signup` surfaces `str(exc)` **verbatim** —
a decision recorded in the code as #128: *"Surface them verbatim so users can recover instead of
bouncing off a generic toast."* One request per address, no captcha in prod, no rate limit on that
route. `InvitationService.create_invitation` leaks similarly, though only within your own org.

So a generic `/forgot-password` response buys **nothing today**. The honest options are:

1. **Make the whole surface opaque** — `/signup` stops saying "email already exists" and instead sends
   an "someone tried to sign up with your address" email. Strictly larger work, and it degrades our
   primary signup funnel to defend against an attack that presupposes we have a user list worth
   stealing. We have zero users.
2. **Stay consistently transparent** — accept that existence is discoverable and defend the things
   that matter: rate limits, no token in any response, short single-use tokens.
3. **Make the reset form opaque now, and track the signup leak separately.**

**Decision: (3).** The deciding factor is cost asymmetry, not principle. Generic reset copy is
*free* — it is what you get by writing the response once and not branching. Retrofitting it later
means rewriting copy across 9 locales and re-teaching users who have learned the form tells them
things. Meanwhile fixing signup is a genuine feature. So: build the reset form for the end state,
ship the free half now, and track the `/signup` disclosure separately — **filed as [core#639]** — so
the inconsistency is recorded instead of pretended away. Until that lands, the opacity here is partial
and this spec says so out loud.

**The generic response must still be actionable.** Most complaints about opaque reset forms are
really complaints about dead ends. Remove the dead end with copy rather than with disclosure:

- **Echo the submitted address back**, prominently. It is the user's own input, so it leaks nothing —
  and it is what catches the typo that caused the problem.
- **State the TTL** ("expires in 60 minutes") so waiting is bounded.
- **Offer the exit**: reuse the existing `auth.no_account` + `auth.sign_up` pair to link to `/signup`.
  A person with no account gets told what to do next without being told they have no account.

Timing is not a meaningful side channel here: email dispatch is `.delay()`'d to Celery, so no SMTP
round trip happens on the request path, and bcrypt never runs on this path.

### D8 · Password rules: minimum 8, maximum 72 bytes, no composition rules — one validator, three call sites.

There is **no password validation today** beyond `if not password`. No minimum length anywhere,
server or client. We are building the form, so close it.

- **Minimum 8 characters.** Per NIST SP 800-63B: length only, no character-class requirements, no
  forced rotation, no hints.
- **Maximum 72 bytes, enforced explicitly with a clear message.** bcrypt silently ignores everything
  past 72 bytes, so a 100-character passphrase is really a 72-character one — and if we ever change
  algorithm, those users' passwords change meaning. Reject rather than truncate.
- **One validator (`AuthService.validate_password_strength` or equivalent), called from all three of**
  `register_user`, change, and reset. Three places that must agree is how they stop agreeing.
- **Confirm-password is client-side only.** It catches typos; it is not a security control and must
  not be re-checked server-side as though it were.

### D9 · Self-hosters with no SMTP get told, not stranded.

`EmailService.send()` returns `False` when `smtp_host` is empty — which is the **default**. On such an
instance the reset flow would show "check your inbox" forever.

- **Hide the "Forgot your password?" link on `/login` entirely when `settings.smtp_host` is empty.**
- If `/forgot-password` is reached directly on such an instance, say plainly that reset-by-email is
  unavailable because no mail server is configured, and to contact the administrator. This is
  instance-level, not account-level, so it discloses nothing.
- **The Settings change-password card is unaffected** and works with no SMTP at all. That makes Part A
  the more valuable half for the open-source edition.

---

## 3. Part A — change password (in `/settings`)

**Card**: `account_card()`, first card on the page, title `account.title`, subtitle
`account.subtitle`.

**Two variants, chosen by whether a password was ever set (D6):**

| Variant | Fields | Button |
|---|---|---|
| Has a password | Current · New · Confirm | `account.update_password` |
| Never had one | New · Confirm, plus `account.set_password_hint` | `account.set_password` |

> ⚠️ **Use `rx.form` + `on_submit`, not the controlled `value=` / `on_change=` pattern the rest of
> `/settings` uses.** Every other card binds inputs to state vars, which for a password field means
> the plaintext is shipped to the server on **every keystroke** and then sits in server-side Reflex
> state for the life of the session. `rx.form` + `on_submit` sends it once and keeps it out of state —
> which is what `/login` and `/signup` already do. This also composes with the autofill work in
> [core#618] / [core#630]: real password fields on a page that also renders connector credential
> fields is precisely the context Chrome mis-targets.

**Behaviour**

1. Wrong current password → error, **hash unchanged**, no partial write. Fail closed.
2. New password fails D8 → error naming the actual rule.
3. New == current → reject ("choose a different password"). Cheap, and it catches an accidental
   no-op that a user would otherwise believe worked.
4. On success: clear all three fields, show `account.password_updated`, write
   `password_changed_at = now()`, write an audit-log entry via the existing `self._audit(...)`
   pattern — **event only, never the password or the hash**.
5. The user stays signed in. Their current session is the one that just proved knowledge of the old
   password; there is nothing to protect it from.

## 4. Part B — password reset (email round trip)

```
/login  ──"Forgot your password?"──▶  /forgot-password
                                            │ submit email
                                            ▼
                              generic confirmation + echoed address
                                            │  (Celery → SMTP → Resend)
                                            ▼
      email  "Set a new password"  ──▶  {FRONTEND_URL}/reset-password?token=…
                                            │ GET: validate only, never consume (D3)
                                            ▼
                                 new + confirm  ──submit──▶  password written,
                                                             token used_at set,
                                                             password_changed_at set
                                            │
                                            ▼
                                   /login?reset=1  (success callout)
```

**Screen 1 — `/forgot-password`.** The `/login` card shell verbatim (360px, 32px padding, 12px radius,
`1px solid var(--gray-a5)`). Heading `auth.forgot_password_heading`, body
`auth.forgot_password_intro`, one email input, `auth.send_reset_link`, and `auth.back_to_sign_in`
below.

**Screen 2 — confirmation.** Replaces the form in place. Heading `auth.reset_link_sent_heading`, the
**submitted address on its own line in bold** (raw user data, no i18n key), body
`auth.reset_link_sent_body`, hint `auth.reset_link_sent_hint`, then the existing `auth.no_account` +
`auth.sign_up` link pair. Identical whether or not the account exists, and identical when the email
bucket is over its limit (D5, D7).

**The email.** New `EmailService.send_password_reset_email(to: str, token: str) -> bool` +
`datanika.send_password_reset_email` Celery task, mirroring `send_invitation_email` /
`send_invitation_email_task` exactly — `autoretry_for=(OSError, ConnectionError, TimeoutError)`,
`retry_backoff=30`, `retry_backoff_max=300`, `max_retries=3`. Link is
`{frontend_url}/reset-password?token={token}` — a **frontend** path, unlike the two existing emails
which point at `/api/` routes, for the reasons in D1 and D3. Body states the 60-minute expiry and
that an unrequested email can be ignored because nothing has changed.

> **Two notes on the email, neither a defect in this spec.** (a) Email bodies are **outside the i18n
> system** — all three existing templates are hardcoded English `string.format` constants, so this one
> is too; locale-aware email is separate work, not an omission here. (b) `EmailService.send` builds a
> `multipart/alternative` with only an HTML part and no plaintext alternative. That is pre-existing and
> mildly deliverability-unfriendly; worth a one-line fix while the file is open, not a blocker.
>
> The plaintext token necessarily transits the Celery task argument (Redis broker, JSON serializer).
> Acceptable: Redis is bound to `127.0.0.1` and is not backed up off-box. The **database** only ever
> holds the hash. Do not log task arguments for this task.

**Screen 3 — `/reset-password?token=…`.** On load: validate (never consume), stash the token in state,
`replaceState` the URL. Valid → heading `auth.reset_password_heading`, new + confirm,
`auth.set_password`. Invalid, expired or already used → `auth.reset_link_invalid_heading` +
`auth.reset_link_invalid_body` + `auth.request_new_link` → `/forgot-password`. **One message for all
three failure causes** — distinguishing "expired" from "already used" from "never existed" tells an
attacker which tokens were real.

**Screen 4 — `/login?reset=1`.** Green callout `auth.password_reset_done`, plus
`account.review_api_keys` linking to `/settings`.

---

## 5. i18n — 26 new keys × 9 locales

Existing keys reused, no duplicates: `auth.email`, `auth.ph_email`, `auth.password`,
`auth.ph_password`, `auth.no_account`, `auth.sign_up`, `auth.sign_in`, `common.save`, `common.cancel`.

`auth.*` for the public pages (siblings of `/login`, `/signup`); `account.*` for the Settings card,
matching the `api_keys.*` / `notifications.*` precedent for a self-contained Settings sub-feature.
Password field labels are shared across both surfaces rather than duplicated under two prefixes.

| Key | `en` |
|---|---|
| `auth.forgot_password` | Forgot your password? |
| `auth.forgot_password_heading` | Reset your password |
| `auth.forgot_password_intro` | Enter your account's email address and we'll send you a link to set a new password. |
| `auth.send_reset_link` | Send reset link |
| `auth.reset_link_sent_heading` | Check your inbox |
| `auth.reset_link_sent_body` | If that address has a Datanika account, a link to set a new password is on its way. The link expires in 60 minutes. |
| `auth.reset_link_sent_hint` | Nothing after a few minutes? Check your spam folder, and check the address above for typos. |
| `auth.reset_password_heading` | Set a new password |
| `auth.new_password` | New password |
| `auth.confirm_password` | Confirm new password |
| `auth.set_password` | Set password |
| `auth.reset_link_invalid_heading` | This link is no longer valid |
| `auth.reset_link_invalid_body` | Reset links can only be used once and expire after 60 minutes. Request a new one. |
| `auth.request_new_link` | Request a new link |
| `auth.password_reset_done` | Your password has been updated. Sign in with your new password. |
| `auth.back_to_sign_in` | Back to sign in |
| `auth.reset_unavailable` | Password reset by email isn't available on this instance — no mail server is configured. Contact your administrator. |
| `account.title` | Your account |
| `account.subtitle` | These settings apply to you personally, not to this organization. |
| `account.change_password` | Change password |
| `account.set_password_hint` | You signed in with Google or GitHub, so you don't have a password yet. Setting one gives you a second way into your account. |
| `account.current_password` | Current password |
| `account.update_password` | Update password |
| `account.password_updated` | Password updated. |
| `account.password_rules` | At least 8 characters. |
| `account.review_api_keys` | If someone else may have had access to your account, review your API keys. |

**26 rows, 26 distinct keys, no duplicates across the two prefixes.** `auth.set_password` does double
duty as the Settings button label for the never-had-a-password variant, and `auth.new_password` /
`auth.confirm_password` are shared by both surfaces rather than re-declared under `account.` — nine
locales is nine translators' worth of reason not to say the same words twice.

Per WORKFLOW_RULES §6, **dynamic error messages are exempt** — `AuthState.auth_error` is already raw
English throughout, and this spec does not change that convention.

---

## 6. Data model and migration

**Expand-only. Nothing here is destructive, so it is safe under
[SPEC_EXPAND_CONTRACT_MIGRATIONS](SPEC_EXPAND_CONTRACT_MIGRATIONS.md) in a single release.**

```
ALTER TABLE users ADD COLUMN password_changed_at TIMESTAMPTZ NULL;   -- nullable ADD COLUMN
CREATE TABLE password_reset_tokens (...);                            -- CREATE TABLE
UPDATE users SET password_changed_at = created_at WHERE oauth_provider IS NULL;  -- batched backfill
```

All three are on the "safe now" list: the currently-deployed container tolerates an unread nullable
column and an unreferenced table for the whole blue/green window.

`password_reset_tokens` — mirror `OAuthGrant`, not `Invitation`:

| column | type | notes |
|---|---|---|
| `id` | PK, autoincrement | |
| `user_id` | `BigInteger` FK → `users.id`, not null | non-PK FK ⇒ `BigInteger` |
| `token_hash` | `String(64)`, unique, indexed, not null | SHA-256 hex |
| `expires_at` | `DateTime(timezone=True)`, not null | |
| `used_at` | `DateTime(timezone=True)`, nullable | the single-use marker |
| + `TimestampMixin` | | `created_at` / `updated_at` / `deleted_at` |

**No `TenantMixin`** — this is user-scoped, not org-scoped, exactly like `users` itself. Add the table
to `PUBLIC_TABLES` in `migrations/helpers.py`.

A short retention sweep (delete rows `expires_at < now() - 30 days`) belongs on the existing
maintenance task rather than as new infrastructure.

---

## 7. Acceptance criteria

Product will verify these on prod after promotion. They restate [core#623]'s list with this spec's
decisions made concrete.

**Part A**
1. A signed-in user changes their password from `/settings`; the new one works on the next sign-in and
   the old one is rejected.
2. A wrong current password produces an error and **`password_hash` is byte-identical afterwards**.
3. An OAuth-created account sees "Set a password" with **no** current-password field; a
   password-created account that later linked Google still sees the current-password field. *(This is
   the D6 trap; a test that only covers the first half passes on the broken implementation.)*
4. A password under 8 characters, or over 72 bytes, is rejected at signup, change **and** reset, with
   the rule named.

**Part B**
5. A signed-out user requests a reset, receives the email, and completes it from the link.
6. The link is single-use: the second visit shows the invalid-link state.
7. The link expires: a token older than 60 minutes shows the same invalid-link state, with **the same
   wording** as the used one.
8. `/forgot-password` renders a byte-identical response for a registered and an unregistered address,
   including when the email rate-limit bucket is exhausted.
9. **A `curl` GET of the reset URL — no submit — does not consume the token**; a subsequent real click
   still works. *(The mail-scanner case in D3.)*
10. Requesting a second link invalidates the first.
11. After a successful reset the user lands on `/login?reset=1` **signed out**, and signs in with the
    new password.
12. On an instance with `smtp_host` empty, `/login` shows no "Forgot your password?" link.

**Cross-cutting**
13. All 26 keys present in all 9 locale files; `test_all_locales_have_same_keys` green.
14. Regression tests written red-first (WORKFLOW_RULES §5), and **run against the pre-fix code** to
    prove they discriminate.
15. No password, token, or hash appears in any log line or audit-log payload.

---

## 8. Ship order and dependencies

1. **Part A first.** Smaller, unblocks the parked Docs-QA rotation without a manual production
   `UPDATE`, and is the half that works for self-hosters with no SMTP (D9).
2. **Part B second.** Reasonable to land after or alongside the OAuth email-trust fix
   (`plans/security/OAUTH_EMAIL_TRUST_2026-08-30.md` (`plans/security/OAUTH_EMAIL_TRUST_2026-08-30.md (local only)`)).
   Part B makes the **mailbox the root authority** for every account including OAuth-created ones;
   that finding is about whether a provider-asserted email proves identity. Same question, two
   surfaces — worth resolving in one direction. **This is a sequencing preference, not the hard gate
   that finding places on [core#624].**
3. **Not blocked on anything.** SMTP is live in prod (Resend relay, `datanika.io` verified, all seven
   `SMTP_*` keys populated), Celery runs, Redis is reachable from the web process, and `FRONTEND_URL`
   is set. No Infra change, no vhost change, no new credential.

**Filed separately by this spec** (see D7): **[core#639]** — `/signup` discloses account existence
verbatim, unbounded, because the production CAPTCHA is a no-op and no auth route is rate-limited. Not
fixed here; tracked so the surface can be made consistent later. Its recommended fix reuses the
generalised `RateLimitService` this spec introduces.

## 9. Docs

Extend the existing **Authentication** section of `datanika-landing/src/pages/docs/organizations.astro`
rather than adding a page — consolidate, don't scatter. Cover: changing your password, resetting it,
what the 60-minute single-use link means, that OAuth accounts can set a password, that self-hosted
instances need SMTP configured, and that API keys are not revoked by a reset. Cross-link from the
Settings card.

> ⚠️ **Pre-existing drift found while writing this, unrelated to the feature.** That same page claims
> *"On the cloud edition, new accounts registered via email must verify their email address before
> accessing the platform."* `email_verification_required` defaults to `False` and the production
> `.env.docker` sets no such key, so it is false today. Growth's page, Growth's call — flagged, not
> changed.

[core#472]: https://github.com/datanika-io/datanika-core/issues/472
[core#618]: https://github.com/datanika-io/datanika-core/issues/618
[core#623]: https://github.com/datanika-io/datanika-core/issues/623
[core#624]: https://github.com/datanika-io/datanika-core/issues/624
[core#630]: https://github.com/datanika-io/datanika-core/pull/630
[core#639]: https://github.com/datanika-io/datanika-core/issues/639
