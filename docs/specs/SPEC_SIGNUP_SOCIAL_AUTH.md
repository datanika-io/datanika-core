# SPEC — Social auth on `/signup`

**Author**: Product · **Date**: 2026-08-30 · **Status**: contract, ready for Engineering
**Tracking**: [core#624](https://github.com/datanika-io/datanika-core/issues/624)
**Implementation**: Engineering (core). Product owns this spec and the acceptance criteria.
**Verified against**: `origin/dev` @ `7165ad1` (= deployed `master`) **and** the live production app.

---

## 1. What is actually wrong

`/login` offers *Continue with Google / GitHub*. `/signup` offers email + password only.

**This is not a missing feature. It is a missing entry point.** Social signup already works end to end
today: `UserService.find_or_create_oauth_user` creates the user, creates their org, creates the owner
membership, sets `email_verified=True`, and returns `is_new`, which `oauth_callback` already forwards
to `/auth/complete?…&is_new=1`. Everything needed to sign up with Google exists and ships. The only
page that never mentions it is the one for people who do not yet have an account.

Confirmed on prod 2026-08-30: `/signup` renders exactly one button (`Create Account`), three inputs,
one link (`Sign In`), and no divider or provider row.

### Why it is worth fixing at 0 signups

The perverse part is the copy. A new user who wants to sign up with Google has to ignore the page
titled *"Create your account"*, follow a link that reads *"Already have an account? Sign In"* — which
tells them they are in the wrong place — and use the page headed *"Sign in to your account"*. We are
steering people away from our fastest signup path with our own labels.

There is also a second reason, new as of today: **we cannot reset a password** ([core#623] — no
change-password and no reset-password path exists anywhere in the product). Until that ships, every
email+password signup is an account that can never be recovered. Social auth is the only signup path
we currently offer that has a working recovery story, because the provider owns it.

---

## 2. The part that makes this more than a copy change

Adding the buttons **without** the rest of this spec would make two funnels worse than they are today.

`AuthState.signup()` and `AuthState.login()` both end at `_post_auth_redirect_target()`, which honours
`?next=` then `?template=<slug>`; `signup()` additionally consumes `?invite_token=` and pre-fills from
`?email=`. **The social path bypasses all of it.** `oauth_login` carries nothing but `state`; the
callback redirects to `/auth/complete?token=…&refresh=…&is_new=…`. Nothing else survives.

| Context | Email path today | Social path today | Consequence once the buttons exist |
|---|---|---|---|
| `?template=<slug>` | → `/connections?template=<slug>`, form prefilled | dropped | Growth's public `/templates/[slug]` pages are a cold-traffic entry point. The visitor clicks *Try this template*, takes the fastest-looking button, and lands on an empty dashboard with no template. |
| `?invite_token=` | invitation accepted, switched into the inviting org | dropped | **The bad one.** The invitee gets a brand-new personal org instead of joining the team that invited them. Silently wrong, not merely unhelpful, and it leaves an orphan org behind. |
| `?next=` | resumes the interrupted flow | dropped | Already a known gap (the MCP consent bounce, `PLAN_PRODUCT.md` → Remote-MCP P2 follow-ups). Becomes load-bearing here. |

Today these funnels are accidentally safe: there is no social button on `/signup`, so everyone arriving
with context is forced down the path that preserves it. Adding the button removes that accident.

**Therefore context propagation is a ship gate, not a follow-up.**

### Mechanism

`oauth_login` already sets an HMAC-signed, `httponly`, `samesite=lax`, 600-second `oauth_state`
cookie. Carry the context in that same signed cookie rather than inventing a channel:

- `oauth_login` reads `template`, `invite_token`, `next`, `email` from its own query string and stores
  them alongside the state value, covered by the existing `_sign_state` HMAC.
- `oauth_callback` verifies as it does now, then appends the surviving context to the
  `/auth/complete?…` redirect.
- `/auth/complete` applies it through the **same** helpers the email path uses — `_safe_next_path()`
  for `next`, the existing slug pattern for `template`, `accept_invitation` for `invite_token`.

Two constraints, both non-negotiable:

1. **Nothing attacker-controllable may reach a redirect unchecked.** `_safe_next_path()` already
   rejects `//evil`, `/\evil`, absolute URLs and whitespace splitting — it must be applied on the way
   out of `/auth/complete`, not merely on the way in. The signed cookie protects integrity, not the
   value's shape: a user can put anything in their own `?next=` and have it faithfully signed.
2. **`invite_token` acceptance must be idempotent and must fail closed.** If the token is expired or
   already used, the user is still signed in (their OAuth identity is valid) but must land with a clear
   message, not silently in a fresh personal org.

If Engineering wants to split delivery, the only acceptable split is: **ship the buttons hidden when
`invite_token` or `template` is present**, and drop that condition when propagation lands. Shipping the
buttons unconditionally without propagation is not an acceptable intermediate state.

---

## 3. Ship gate — the auth finding

**Blocked on `plans/security/OAUTH_EMAIL_TRUST_2026-08-30.md` (`plans/security/OAUTH_EMAIL_TRUST_2026-08-30.md (local only)`).**

`_fetch_github_email` falls back to `emails[0]["email"]` in exactly the case where the preceding loop
established that no address is both primary and verified; Google's branch never reads `email_verified`;
and `find_or_create_oauth_user` links an OAuth identity to an existing password account on email match
alone, with no confirmation and without comparing `oauth_provider_id`. Latent today (0 users), and this
spec's whole purpose is to route the majority of new signups through that code.

Fix that first. It is mostly deletions. **Do not open a public GitHub issue with the detail** — public
AGPL repo, founder chooses disclosure, per the SAML precedent and the standing decision in
`plans/current_state.md`.

---

## 4. Design decisions (Product's call — implement these, don't re-litigate)

**Placement: identical to `/login` — below the email form, after a divider.** The tempting alternative
is social-first on signup, on the usual "fewest fields wins" reasoning. Rejected for v1: we have zero
signup data, so choosing a layout for conversion would be decoration dressed as optimisation, and an
asymmetry between two adjacent pages is a real maintenance cost for an imagined gain. Revisit when
there is a funnel to read. **Do not A/B at 0 traffic.**

**Reuse `_social_login_button` from `login.py` verbatim** — lift it to a shared component, do not copy
it. It carries the [core#605] fix (`flex="1 1 0"`, `min_width="0"`) and the [core#418] fix
(`window.location.assign`, because `rx.link`/`rx.el.a`/`rx.redirect` all treat a same-origin absolute
URL as an in-app route and would swallow the click in production while working in dev). Both are
non-obvious and both have already been re-learned once. A copy is a second place to regress them.

**Copy: reuse `auth.or_continue_with`** ("or continue with"), which exists in all 9 locales. Provider
names stay untranslated per WORKFLOW_RULES §6. **The minimal change therefore needs zero new i18n
keys.** Any string added for the invite/template edge cases needs all 9.

**Order: Google, then GitHub** — same as `/login`.

**Adjacent, cheap, include it:** the three `/signup` inputs set no `autocomplete` attribute (`null` on
all of them, verified live). Set `autocomplete="name" / "email" / "new-password"`. `new-password` is
what stops Chrome offering a *saved existing* password on an account-creation form. This is the same
class as [core#618] but a different surface and far smaller; it is in scope here because it is three
attributes on the form this spec already touches.

---

## 5. Acceptance criteria

From the user's side. Each must be demonstrated in a running app, not asserted from code.

1. `/signup` shows *Continue with Google* and *Continue with GitHub*, laid out like `/login`: both
   inside the card, equal width, neither overflowing. Re-measure per [core#605] — row
   `scrollWidth == clientWidth`.
2. A visitor with no Datanika account completes Google signup from `/signup` and lands signed in, with
   an org created and owner membership. `is_new` is `1` on that callback.
3. Signing up via `/signup` social with `?template=<slug>` lands on `/connections?template=<slug>` with
   the form prefilled — the same destination the email path reaches.
4. Signing up via `/signup` social from an invite link joins **the inviting org**, not a new personal
   one, and the invitee appears in that org's Members list.
5. An expired or already-used `invite_token` on the social path signs the user in and tells them the
   invitation could not be accepted. It does not fail silently into a fresh org.
6. `?next=` survives the social path and is honoured only when `_safe_next_path()` accepts it; a
   crafted `next` (`//evil.example`, an absolute URL, a newline-split value) redirects to the dashboard
   instead.
7. An existing Google-linked user signing in from `/signup` is signed into their existing account — not
   given a second org.
8. Chrome does not offer a saved existing password on the `/signup` password field.
9. All 9 locales render the divider text; no untranslated key appears.
10. Regression tests written red-first (WORKFLOW_RULES §5), including one that fails on the current
    code for each of criteria 3, 4 and 6.

## 6. Out of scope

- New providers beyond Google/GitHub. SAML/OIDC SSO is Enterprise and separate.
- Password change / reset — [core#623], its own issue, referenced here only as motivation.
- Any change to `/login`'s layout beyond extracting the shared button component.
- Conversion experiments on placement or button copy. Nothing to measure yet.

## 7. Files Engineering will touch

`datanika/ui/pages/signup.py` · `datanika/ui/pages/login.py` (extract only) · a shared component under
`datanika/ui/components/` · `datanika/services/oauth_routes.py` (context in the signed state cookie) ·
`datanika/services/oauth_service.py` + `datanika/services/user_service.py` (§3 gate) ·
`datanika/ui/state/auth_state.py` (`/auth/complete` applying context through the existing helpers).

[core#605]: https://github.com/datanika-io/datanika-core/issues/605
[core#618]: https://github.com/datanika-io/datanika-core/issues/618
[core#623]: https://github.com/datanika-io/datanika-core/issues/623
[core#418]: https://github.com/datanika-io/datanika-core/issues/418

---

## 8. Amendment, 2026-09-03 — four corrections, measured against `origin/dev` `1b1c134`

§1–§7 were written on 2026-08-30 and re-verified on 2026-09-02. Everything structural in them still
holds: `/signup` has **zero** references to `oauth`/`google`/`github`/`_social_login_button`,
`_social_login_button` is still private to `login.py:18` (used at `:268` and `:269`), the three
`/signup` inputs still carry no `autocomplete`, and the `oauth_state` cookie is still HMAC-signed,
`httponly`, `samesite=lax`, `max_age=600`.

**What does not hold is §2's picture of the email path**, and since §2 asks Engineering to make the
social path match it, that matters more than any of the above. Four corrections. The first two change
what "done" means; the third would have cost an implementation attempt; the fourth is a hazard this
work creates rather than inherits.

### 8a. 🚨 The email path also creates a spurious personal org — the orphan is not a social-path defect

§2's table says the email path *"invitation accepted, switched into the inviting org"* and blames the
social path for *"a brand-new personal org … it leaves an orphan org behind"*. Measured by AST over
`AuthState.signup`:

```
create_org calls in signup()      : 1
guards enclosing create_org       : NONE -- unconditional
create_org at line 525, accept_invitation at line 567  -> org created FIRST
```

So **`svc.create_org(...)` runs for every signup, invited or not**, and `accept_invitation` runs 42
lines later and *appends* the invited org (`self.user_orgs.append(invited)`) before switching to it.
An invited user who signs up by email today ends in **two** orgs: `{full_name}'s Org`, which they own
and never asked for, and the team they were actually invited to.

**Consequence for this spec: "make the social path match the email path" is the wrong contract.**
Copying it reproduces the spurious org into the funnel this work exists to make the primary one. The
orphan org is a defect of *both* paths and has to be fixed in the shared behaviour, not mirrored.

### 8b. 🚨 §2's own constraint 2 is already violated — by the reference implementation

Constraint 2 reads: *"If the token is expired or already used, the user is still signed in … but must
land with a clear message, not silently in a fresh personal org."* Measured on the email path:

```
invite except handler:
  statements                     : 1
  assigns anything user-visible  : False
  body                           : logger.exception('Invitation acceptance failed during signup and was dropped: ...')
```

One statement, a log line, nothing reaches the user. The handler's own comment says *"this one is
user-visible when it fails (they sign up and are not in the org they were invited to), so it is
exactly the thing support needs a log line for"* — support gets the log; **the user gets nothing**.
Landing silently in a fresh personal org is not the hazard this spec is guarding against on the social
path. It is what ships today on the email one. Filed separately so it is not gated behind this work.

⚠️ **And there is no test.** `grep -rln invite_token tests/` returns exactly one file,
`test_services/test_email_service.py`, which covers the invitation *email*. **The invited-signup flow
has no coverage at all**, on either path. So §5's criteria 4 and 5 are not "extend the existing
tests" — they are the first tests this flow has ever had, and nothing currently protects the email
path from regressing either.

### 8c. 🚨 `/auth/complete` cannot read the cookie — the third mechanism bullet is not implementable as written

§2's Mechanism says *"`/auth/complete` applies it through the **same** helpers the email path uses"*.
That is right about **policy** and wrong about **transport**, in two independent ways:

1. **`/auth/complete` is a Reflex frontend page**, not a backend route — `datanika.py:236`,
   `route="/auth/complete"`. Its state reads `self.router.page.params`, i.e. the query string. The
   cookie is `httponly`, so the browser never exposes it to JS and a client-side `rx.Cookie` var
   cannot see it.
2. **The cookie is already deleted by then.** `oauth_callback` calls
   `response.delete_cookie(_OAUTH_STATE_COOKIE)` on the *same* redirect that sends the browser to
   `/auth/complete`. It is gone before the page loads.

Either one is decisive; together they make the reading unavailable rather than merely awkward. **The
second Mechanism bullet is the whole transport**: the callback — a backend Starlette route, which
*can* read the cookie — verifies, then puts the surviving context into the `/auth/complete?…` query
string. `/auth/complete` then applies it from **its query string**, through the same helpers.

Corrected bullet 3, replacing the original:

> `/auth/complete` reads the context **from its own query string** and applies it through the same
> helpers the email path uses — `_safe_next_path()` for `next`, the existing slug pattern for
> `template`, `accept_invitation` for `invite_token`. It never reads the `oauth_state` cookie: that
> cookie is `httponly` and is deleted by the callback that redirects here.

⚠️ Note what this does **not** relax. Constraint 1 becomes *more* load-bearing, not less: the context
arrives at `/auth/complete` as ordinary query parameters, so `_safe_next_path()` on the way out is the
only thing standing between a crafted `?next=` and an open redirect. The signed cookie protects the
value between the two backend hops; it says nothing once the value is back in a URL.

### 8d. ⚠️ One cookie name, two tabs — a hazard this work creates

`_OAUTH_STATE_COOKIE` is a single fixed name, so two concurrent OAuth flows in one browser overwrite
each other's state. The first callback's `state` parameter then fails to match the surviving cookie
and is rejected.

Today that is a rare annoyance, because only `/login` offers social auth and the two flows would be
interchangeable anyway. **This work changes both halves of that.** Once `/signup` carries context, two
tabs is the *expected* shape of the traffic — an invitation email open in one tab, a `/templates/<slug>`
page in another — and the flows are no longer interchangeable, because each carries a different
invite or template.

🚨 **Named because the tempting repair is the dangerous one.** Loosening the state comparison to make
the mismatch go away would let the first tab's callback complete against the second tab's cookie — and
therefore against the second tab's **invite context**, joining a user to an org they were not invited
to. The state check is an auth boundary; it does not become a UX nuisance because we added a payload
to it. Acceptable resolutions: key the cookie per-state (`oauth_state_<state>`), or keep the single
cookie and make the failure message accurate about what happened. **Not** a weaker comparison.

### 8e. Decision — an invited signup does not get a personal org

Product's call, and it applies to **both** paths, closing 8a and 8b together:

1. **Try the invitation first.** If `invite_token` is present and valid, the user joins the inviting
   org and **no personal org is created**. `create_org` becomes conditional on there being no valid
   invitation — today it has no enclosing branch at all.
2. **The personal org is the fallback, not the default.** No token, or a token that is expired,
   already used, or for a different address → create the personal org, exactly as now.
3. **The fallback is announced.** When a token was supplied and could not be applied, the user lands
   signed in, in their personal org, **and sees a message saying the invitation could not be applied
   and that they can ask for a new one**. That is §2 constraint 2, made true for the first time.
4. **Never zero orgs.** Every user finishes signup as a member of at least one org. Ordering the
   invitation first must not create a window where a failure leaves them with none — attempt the
   invitation, and create the personal org if and only if that attempt did not produce a membership.

Rationale for 1, since it is the change of behaviour: the invitee never asked for a second workspace.
It clutters the org switcher, it makes `current_org` ambiguous immediately after signup, and with
quota enforcement live since 2026-08-31 it is a second org carrying its own Free-plan limits. The
argument for keeping it — *"a user should always own something"* — is answered by 2 and 4.

### 8f. Additions to §5's acceptance criteria

Numbered from 9 so §5's eight stay unambiguous.

9.  **An invited signup produces exactly one membership and exactly one org.** Assert the *count*, on
    both the email and the social path. *(Criterion 4 as written — "the invitee appears in that org's
    Members list" — is satisfied by today's two-org behaviour, so it cannot catch 8a.)*
10. **An expired or already-used token produces a visible message**, on both paths. Exercise the
    failing branch; a test that only walks the happy path passes on the current silent-swallow.
11. **The context reaches `/auth/complete` in its query string**, and the `oauth_state` cookie is
    absent by the time that page loads. Assert the absence — it is what stops someone reintroducing
    the cookie read that 8c rules out.
12. **Two overlapping OAuth flows fail closed and say so.** Start a second flow before completing the
    first; the first must be rejected, and must not complete against the second's context. 🚨 Assert
    that it did **not** join the org named by the second flow's token — not merely that an error
    appeared.
