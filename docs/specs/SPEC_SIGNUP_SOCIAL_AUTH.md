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
