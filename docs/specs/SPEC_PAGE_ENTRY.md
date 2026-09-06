# SPEC — Page entry: who may enter, what runs, and what the user sees while it runs

**Author:** Product · **Status:** contract, ready for Engineering · **Written:** 2026-09-06
**Binds:** Engineering. **Source of truth for:** [core#1081], [core#1090] remedy 1, [core#1097].

Every anchor below was read on `origin/dev` at **`9b9810a`**. Line numbers are from that commit;
if one does not match your checkout, suspect the checkout before the citation.

This spec exists because three open issues turned out to be one question asked at three moments of
the same event — a browser arriving at a URL:

| moment | question | issue |
|---|---|---|
| before anything runs | **may this visitor be here?** | [core#1081] AC1 |
| while the entry handlers run | **what is on screen?** | [core#1090] remedy 1 |
| inside the entry handlers | **what work is legitimate?** | [core#1097] |

Answering them separately is how the three defects arrived separately. §1 is the contract; §2–§5 are
the changes; §6 is how Engineering proves each one **without a production credential**.

---

## §0 — Three claims of mine that this spec RETRACTS, and the control for each

Read this section first. Two of the three are claims I filed on the very issues below, and an
implementer who works from the issue text rather than from here will build the wrong thing.

**A correction is a claim and needs the control the original needed** (`PRODUCT_RULES` §15). Each
retraction below carries the artifact that settles it.

### 0a. 🔴 RETRACTED — *"a signed-out visitor to `/` waits on a **blank screen**"* ([core#1090] title)

**`page_layout` already renders a loading state, and has all along.** `layout.py:394-403`, the false
arm of `rx.cond(AuthState.is_authenticated, …)`:

```python
rx.cond(
    AuthState.session_expired,
    signed_out_panel(),
    rx.center(
        rx.spinner(size="3"),
        height="100vh",
    ),
),
```

with the comment immediately above it naming this exact case: *"Not signed in. Two very different
reasons land here: the page is still hydrating (spinner), or a handler just ended the session."*

Both routes I measured go through it — `dashboard.py:260` and `pipeline_templates.py:83` both call
`page_layout`, and **17 page modules** import it.

**Why my measurement could not have told me.** I used `first-contentful-paint` as the proxy for
*"the user sees something"*. A Radix `Spinner` is CSS-animated `<span>` elements: **no text node, no
image, no SVG, therefore not a contentful paint.** FCP is structurally blind to the one element that
was on screen. The `first-paint` figure in my own filing — **388 ms**, against FCP 3656 ms — is what
a spinner painting early looks like, and I read it as "background only".

⚠️ **Scoped honestly, and NOT overshot into the opposite claim.** I have established that *the code
path which renders during the window is a spinner*. I have **not** photographed production during
the window, so I am not asserting the spinner is pixel-visible for its whole duration. The
discriminating measurement is a DOM read or screenshot taken **inside** the window and it has not
been taken. What is settled is narrower and sufficient: **remedy 1 as filed — "add a loading state" —
would ship nothing, because one is already there.** §4 re-scopes it accordingly.

### 0b. 🔴 RETRACTED — *"`rx.App(…)` sets no `overlay_component`; Reflex's own hook for exactly this is unused"* ([core#1090] comment)

The fact is right and the inference is wrong. `datanika.py:83` is `rx.App(head_components=…)` with
no `overlay_component` — true. But `overlay_component` is **not a hydration-aware loading hook**. Its
default is `connection_pulser() + connection_toaster()` (`reflex/app.py:208-228`) — a global overlay
rendered on every page at all times, in no way conditioned on load state. Building the loading state
there would put it on `/login` and `/signup` too, which are the two pages that are already fast.

**Report a mechanism's status only from the field that records that mechanism.** I named a hook by
what its absence looked like rather than by what it does.

### 0c. 🟠 CORRECTED — *"the affordance is grey"* ([core#1081] AC2 as filed)

The `Sign Up` link renders `rgba(0, 109, 203, 0.95)` — Radix accent blue. `color="gray"` sits on the
enclosing `rx.text` (`login.py:273-281`), and I attributed the wrapper's colour to the link inside it.

**The problem is size, not colour: 50 × 20 px against a 141 × 40 px `Google` button and a 294 × 40 px
`Sign In`.** A restyle that darkens the text satisfies the sentence I originally wrote and leaves the
defect exactly where it was. Superseded by §3.

### 0d. ✅ NOT retracted — and one new source-level corroboration

The measurement that matters is unchanged: **the cost is entry into the backend event path at all,
and it does not scale with what the handlers do.** Five samples, production, signed out, same box,
~1 hour, `gap` = `loadEventEnd` → FCP:

| route | `on_load` handlers | gap |
|---|---|---|
| `/` | 4 | 2575 / 6167 / 1933 ms |
| `/pipelines/templates` | **1** (a `check_auth` that does no I/O when signed out) | **9499** / 1528 ms |
| `/login` direct | **0** | **57 ms** |

🚨 **There is no "the 3.7 seconds".** The quantity ranges **1933–9499 ms**, and the slowest sample of
the session was **the control page with a single no-op handler**. The `3.7s` in [core#1090]'s title
was **n=1** and must not be quoted as a measured figure.

**The new corroboration is in Reflex's own source**, and it is why the split falls where it does —
`reflex/state.py:2666-2678`:

```python
load_events = app.get_load_events(self.router.url.path)
if not load_events:
    self.is_hydrated = True
    return None  # Fast path for navigation with no on_load events defined.
self.is_hydrated = False
return [*fix_events(load_events, …), State.set_is_hydrated(True)]
```

The fast path is keyed on **`if not load_events`** — a boolean about *whether any handler exists*,
never about what one costs. One no-op handler and four heavy ones take the same branch. That is the
measured shape, stated by the framework.

⚠️ **It does not explain the 6.2x variance between two samples of the same route**, and nothing here
should be read as if it did. The discriminating test remains **N rapid vs N spaced samples against
`GRANIAN_WORKERS`**, and it is unclaimed.

⚠️ **`is_hydrated` does NOT gate rendering.** Frontend-side it gates the `on_hydrated_queue` and the
application of client-storage deltas (`reflex/.templates/web/utils/state.js:669-673, 772-785`), and
nothing else. Do not build the loading state by testing it, and do not repeat the guess that Reflex
withholds the DOM until hydration — it does not.

### 0e. 🔴 RETRACTED — *"all **17** protected pages pay this"*

**It is 14.** Derived from `datanika.py`'s AST rather than from a grep: 20 `app.add_page` calls, of
which **14 carry `AuthState.check_auth`** and 6 do not (`/login`, `/signup`, `/forgot-password`,
`/reset-password`, `/auth/complete`, `/oauth/consent`).

I have asserted **17** on [core#1090], on [core#1097] and in my own handoff. Two ways it went wrong,
and both are traps this project has already written down:

1. **`grep -c 'AuthState.check_auth' datanika/datanika.py` returns 16, not 14.** Lines **108** and
   **241** are *comments explaining why a page does **not** carry it*. This is `WORKFLOW_RULES` §4's
   rule — *"a guide corrected to deny an old behaviour still contains the old phrase; count the
   instruction, not the phrase"* — arriving in source rather than in docs.
2. **17 is a real number about a different set**: the page modules importing `page_layout`. I counted
   the breadth of the layout and called it the size of the protected set. A denominator taken from
   the wrong set.

**Neither number changes any decision in this spec** — §4 is still the widest remedy and §5 still
needs a sweep. It is corrected because both figures appear in acceptance criteria, and *"sweep the
other 16"* sends someone looking for three routes that do not exist.

---

## §1 — The entry contract

Every route belongs to exactly one class. The class determines all three answers.

| class | routes | signed-out visitor | signed-in visitor | entry work |
|---|---|---|---|---|
| **public** | `/forgot-password`, `/reset-password` | serve | serve | may run; must not need a session |
| **credential** | `/login`, `/signup` | serve | **redirect to `/`** — §2 | may run; must not need a session |
| **protected** | the **14** routes carrying `AuthState.check_auth` — §0e | **redirect to `/login`** | serve | **must not run without a resolved org and user** — §5 |

⚠️ **`/auth/complete` and `/oauth/consent` are in none of these three classes and are correctly
outside this spec.** They are mid-flow OAuth pages carrying their own handlers
(`AuthState.handle_oauth_complete`, `McpConsentState.load_consent`). Do not "fix" their missing
`check_auth` — a comment at `datanika.py:241` records why.

Three rules follow, and they are the contract:

1. **A credential page is not a protected page with the sign flipped.** `check_auth` sends the
   *unauthenticated* case to `/login`; on `/signup` that returns every prospect to the wall this work
   exists to remove. The credential class needs its **own** handler with the opposite polarity.
2. **An entry handler on a protected page may assume nothing.** `check_auth` is registered *first* in
   every `on_load` list, and that ordering buys nothing: Reflex dispatches the list, and a later
   handler does not learn that an earlier one returned a redirect. Each loader guards itself. §5.
3. **Every protected page shows the same thing while its handlers run**, from one place. The window
   is 1.5–9.5 s and unpredictable; there is no per-page version of this problem. §4.

---

## §2 — [core#1081] AC1: the credential pages must refuse a signed-in visitor

### The defect is a session substitution, not a confusing page

`/signup` is registered with `on_load=[AuthState.prefill_invite_email]` and nothing else
(`datanika.py:103-106`); `/login` carries no `on_load` at all (`datanika.py:97-100`). Neither bounces
an authenticated visitor.

`AuthState.signup` (`auth_state.py:548`) **never asks whether a session already exists**, and ends by
overwriting the live one in place (`auth_state.py:646-661`):

```python
self.access_token  = result["access_token"]
self.refresh_token = result["refresh_token"]
self.current_user  = UserInfo(...)
self.current_org   = OrgInfo(id=org_id, name=org_name, slug=org_slug)
self.user_orgs     = [self.current_org]      # every existing membership discarded
self.current_role  = signup_role
```

A signed-in user who completes that form is **silently re-identified**: new user row, new org, tokens
replaced, `user_orgs` clobbered to a single-element list. No sign-out, no confirmation, no notice.
Their real memberships survive in the database; the session no longer knows about them, so the next
page renders an empty new tenant and their work reads as gone.

Since landing PR #513, **"Get Started Free" in the marketing nav points at `/signup`** — so the
signed-in population reaches this form through the most prominent control on the site.

⚠️ **Scope it honestly: this is not privilege escalation.** The resulting session is a brand-new org
containing only the new user, so nothing becomes reachable that was not before. The damage lands on
the person who was signed in. It is a self-inflicted session substitution, and it is fixed by a
guard — not by hardening `signup`.

### AC1.1 — a credential-page guard

A new handler on `AuthState`, registered as `on_load` on **both** `/login` and `/signup`:

- a **valid** session ⇒ `rx.redirect("/")`;
- no session, or one that fails revalidation ⇒ **return `None`**, render the page. It must not clear
  the session, must not set `auth_error`, and must not redirect anywhere.

Reuse `_revalidate_session()` (`auth_state.py:843`) — do not write a second definition of "valid".
Two definitions of a session's validity is how [core#671] happened.

⚠️ **`/signup` already has `prefill_invite_email` and must keep it.** Add to the list; the guard goes
first.

⚠️ **This adds an `on_load` to `/login`, which today has none.** Per §0d that moves `/login` off
Reflex's fast path — it is the one page measured at a **57 ms** gap, and it will acquire the same
1.5–9.5 s window as every other page. **That cost is accepted and it is not negotiable away**, because
the alternative is leaving a form that silently discards a user's org memberships. State it in the PR
body so nobody later "optimises" the guard off. `/signup` already pays it and is unaffected.

### AC1.2 — a defence in depth inside `signup`, and it is not optional

The guard is a page-load check; the form can still be submitted from a tab that was signed in after
the page loaded. `AuthState.signup` must refuse when a valid session already exists, before the
CAPTCHA check and before any database read.

The refusal is a **redirect to `/`**, not an error message. A signed-in user who lands on a signup
form has not made a mistake worth explaining — they clicked a nav link.

🚨 **Do not implement this by clearing the session first.** "Sign the user out, then sign them up" is
the same substitution with a friendlier name.

### AC1.3 — the regression test names the discarded memberships

Not *"signup redirects"*. The property is that **an existing session's `user_orgs` survives**. A test
asserting only the redirect passes against an implementation that redirects *after* clobbering.

### AC1.4 — what is NOT in scope

- `/forgot-password` and `/reset-password` keep no guard. Their comment at `datanika.py:107-113`
  records why, and it is right: a signed-out user is the only kind that can need them.
- The OAuth callback pages keep their existing handlers untouched.

---

## §3 — [core#1081] AC2: account creation lives on the sign-in page and is absent from the sign-up page

This is a product decision, not a copy tweak, and it is the reason a restyle cannot close AC2.

`login.py:266-270` renders `auth.or_continue_with` above `_social_login_button("Google", "google")`
and `("GitHub", "github")`. Those start `/api/auth/login/<provider>`, which lands in
`UserService.find_or_create_oauth_user` — whose own docstring is *"Find existing user by OAuth
identity or email, **else create**."* It returns `is_new` because **creating an account is a normal,
expected outcome of that path.**

Meanwhile `ui/pages/signup.py` carries **zero** social controls: `grep -ciE
'google|github|continue_with|social'` returns **0** across 149 lines.

So on a page headed *"Sign in to your account"*:

- the two largest controls after `Sign In` **will create an account** for a visitor who has none, and
  nothing on the page says so;
- the only control that *names* signing up is a **50 × 20 px** link beneath them;
- and the page actually called **Sign Up** cannot do the one-click thing at all.

> **The inversion is the defect.** `/login` is the product's fastest signup path and says it is not.

### AC2a — make the sign-up affordance a real secondary control

Keys `auth.no_account` and `auth.sign_up` already exist in all nine locales (`i18n/en.json:18,26`),
so **no new strings.** The bar is comparative, not aesthetic:

> The control that names signing up must be of the same **order** as the social buttons — a button or
> button-like control, not body text. Target: no smaller than half a social button's area
> (141 × 40 = 5,640 px²). It is 1,000 px² today.

⚠️ **Do not restyle by colour.** §0c: it is already accent blue. A darker link is a change that
measures as done and fixes nothing.

### AC2b — label the social block for what it does for a visitor who has no account

`auth.or_continue_with` ("or continue with") is true for a returning user and **silent** for a new
one. The block must say that these controls also create an account.

⚠️ **A new string ⇒ all 9 locales** (`i18n/{en,ru,el,de,fr,es,zh,ar,sr}.json`), enforced by
`tests/test_i18n/test_i18n.py::test_all_locales_have_same_keys`.

### AC2c is [core#624] and MUST NOT be absorbed here

Putting social buttons **on `/signup`** is [core#624]'s job — it has its own spec
([SPEC_SIGNUP_SOCIAL_AUTH.md](SPEC_SIGNUP_SOCIAL_AUTH.md)) and carries the template/invite context
propagation that this issue does not touch. Two failure modes to avoid, in both directions:

- AC2b must not be written up as having delivered #624;
- #624 must not be reported as blocked on this spec. It is not.

### AC3 of [core#1081] is a recorded decision and is NOT reopened

`/` continues to send a signed-out visitor to `/login`, never to `/signup`. The three reasons are on
the issue. [core#1090]'s measurements do not disturb it — the window is paid regardless of which page
the redirect targets.

---

## §4 — [core#1090] remedy 1, re-scoped: the loading state exists and is inadequate

### Why this is still the remedy to ship, ahead of the other four

The reasoning is the durable part and it survives §0a intact:

1. **It is the only remedy whose benefit does not depend on the number being small.** Every other
   candidate is an argument about shaving a duration. The wait is **1933–9499 ms** and unpredictable,
   and the user-visible failure — *"the site is broken"* — is caused by **not knowing whether anything
   is happening**, not by seconds. A good loading state is right at 1.5 s and right at 9.5 s.
2. **It is the widest.** `page_layout` is imported by **17 page modules** and is the single place all
   14 protected routes pass through. One change covers them all, and the authenticated majority
   benefits at least as much as the bare-origin visitors the issue was filed about.
3. **It cannot be wrong.** No auth surface, no browser storage, no build coupling, no legal page. Its
   worst case is cosmetic. Every other remedy on the list has a way to be silently wrong and two of
   them have a way to be wrong in an auth-shaped direction.

**Two of my own candidates collapsed under measurement and stay dead:**

- **R2, preload the login chunk — do not do it.** Measured **2 ms** warm (chunk requested 6702 ms,
  arrived 6704 ms). The 433 ms I filed was a cold-cache single sample. And it rots silently: the
  hash moved `_login_._index-BBuK70zD.js` → `-CJsve78t.js` **inside 24 hours**, and Reflex 0.8.26
  exposes no route-prefetch API, so this would be a hardcoded `modulepreload` against generated
  output — dead in one deploy, page still green, nobody's check red.
- **R3, client-side negative auth check — its premise is false.** `access_token: str = ""`
  (`auth_state.py:130`) is a plain **server-side** Reflex var; `rx.Cookie` / `rx.LocalStorage` /
  `rx.SessionStorage` are **0 occurrences package-wide**; `document.cookie` is empty on production.
  The browser holds no auth artifact and cannot. R3 is not "let the client short-circuit", it is
  *"introduce a client-visible authentication artifact where none exists"* — and it needs §2's AC1
  first, or a false-negative hint drops an authenticated user on an unguarded `/login`.

### The actual defect, and our own codebase already argued it

The hydrating branch is **a bare centred spinner, held for up to 9.5 seconds, with no text**.

`signed_out_panel()`'s docstring (`layout.py:262-271`) makes the case against exactly this, for a
different instance of it:

> *"…previously a bare spinner, and **a spinner forever is indistinguishable from a hang**. This says
> what happened and offers the way back."*

[core#673] accepted that argument for the session-ended branch and left the hydrating branch beside it
unchanged. §4 finishes the job.

### AC4.1 — replace the bare spinner with an app-shell skeleton

In `page_layout`'s hydrating branch only (`layout.py:399-403`) — the `session_expired` branch is
[core#673]'s and is correct.

Render the **application chrome**: the sidebar silhouette and content-block placeholders.

🚨 **Chrome only. It must be content-neutral, and this is a deliberate product decision, not
minimalism.** The visitor's destination is not yet known — that is the entire problem. An
authenticated user is about to see this chrome filled in; a signed-out user is about to be moved to
`/login`. **A branded shell is honest in both branches; a dashboard-shaped skeleton with fake stat
cards is honest in one and a lie in the other.** Do not render placeholder rows, counts, or chart
shapes.

### AC4.2 — it must be contentful

At least one real text node — the product name is sufficient and needs no new i18n key.

Two reasons, and the second is the one that lasts:

1. A text node is what a user reads as *"this is loading"* rather than *"this is stuck"*.
2. 🔑 **It repairs the instrument.** FCP is what every future measurement of this page will use, and
   §0a is the record of FCP being blind to the element that was actually on screen. A contentful
   loading state makes FCP mean *"the user saw something"* again.

### AC4.3 — no new i18n keys if it can be avoided

A non-textual skeleton plus the product name costs zero locale work. If any user-visible string is
added, it is **all nine locales**.

### AC4.4 — `/login` and `/signup` must not regress

They are `page_layout`-free today and paint in ~404 ms. After §2's AC1.1 gives `/login` an `on_load`,
confirm it still paints its own card promptly — the guard returns `None` for a signed-out visitor,
which is the overwhelmingly common case, and nothing about it should defer the form.

### AC4.5 — what this explicitly does NOT claim

It removes **0 ms**. Do not write a PR body, changelog entry or blog post saying the page got faster.
[core#1090] stays open for R4 — *"why does entering the event path vary 6.2x"* — which is unclaimed
and may be Infra's rather than Engineering's.

---

## §5 — [core#1097]: `load_dashboard` runs unguarded for signed-out visitors

### The defect

`/` registers four `on_load` handlers (`datanika.py:133-140`). Two of the three loaders refuse to run
without an org and a user, in the same two-line idiom
(`onboarding_state.py:36-41`, `notification_center_state.py:37-41`):

```python
auth = await self.get_state(AuthState)
org_id  = auth.current_org.id or 0
user_id = auth.current_user.id or 0
if org_id == 0 or user_id == 0:
    return
```

`DashboardState.load_dashboard` (`dashboard_state.py:123`) has no such guard. It goes straight to
`_get_org_id()` and then unconditionally opens a session and runs five service calls
(`dashboard_state.py:133-142`), then emits `usage.get_summary` (`dashboard_state.py:187`), which the
cloud plugin answers by **opening a second database session of its own**.

So every signed-out visitor to the bare origin — whose `check_auth` has already decided to send them
to `/login` — costs a session, five queries and a plugin hook against `org_id=0`, returning nothing.

### 🚨 This is NOT the cause of §4's window, and must not be closed as if it were

I tested that hypothesis and it failed. `/pipelines/templates`, whose `on_load` is
`[AuthState.check_auth]` alone and which therefore does none of the above, produced the **slowest**
sample of the session (9499 ms) while `/` produced 1933 ms. **My best story was killed by its own
control, and the control was slower.**

Fixing this will not make any page faster in a way a user notices. It is worth doing because it is
**unbounded work executed on behalf of an unauthenticated request, on the most-hit route in the
product**, and the guard is an idiom already used in the two files registered next to it.

### AC5.1 — guard `load_dashboard` with the existing idiom

Match `onboarding_state.py` and `notification_center_state.py` exactly. **Do not invent a third
spelling** — three spellings of one predicate is how the next loader gets missed.

### AC5.2 — assert the session, not just the queries

No database session opened **and no hook emitted** on the guarded path. The `emit` is the half that
is easy to leave behind, and it opens its own session downstream.

### AC5.3 — prove it red against today's code first

The current implementation is the negative control and it is free.

### AC5.4 — do NOT guard `BaseState._get_org_id`

[core#673] AC5 leaves it unguarded deliberately: it runs during rendering, so guarding it puts a
session decision and a token-minting write inside template evaluation. **The guard belongs in the
loader.**

### AC5.5 — sweep the other 13, and report the number

`ui/state/` makes **113 `get_sync_session` calls across 21 files**. Each protected route pairs
`check_auth` with at least one loader; I have read three. A loader that does not guard is the same
defect on a less-visited route.

⚠️ **Start with `/settings`.** It registers **six** `on_load` handlers — more than any other route,
and more than twice `/`'s four. It is the largest unexamined surface of exactly this defect.

⚠️ **Report the count either way.** *"I swept and found none"* is a result; an absent sentence and a
correct sweep look identical.

---

## §6 — How Engineering verifies all of this WITHOUT the production credential

**Answer: verification does not need the founder, and it does not need a browser.** This section
exists because I said on [core#1081] that I could not establish the authenticated case, and that
statement has been read as a blocker. It is not one — it was a statement about *my* instrument.

### 6a. Why my probe failed, so nobody rebuilds it

I tried to answer *"does `/signup` have an authenticated guard?"* credential-free by fetching every
JS asset production serves and grepping for the handler names.

🚨 **Its positive control returned zero.** `check_auth` appears **0 times across 51 files** — measured
on `/`, *during the same page load in which `/` redirected me to `/login`*. The guard demonstrably
ran and its name is in no client artifact. Same for `prefill_invite_email` on `/signup`: **0**, for a
handler that **is** registered.

`on_load` handler identity is dispatched from the backend over the websocket and is absent from the
bundle entirely. Any *"no guard found in the shipped chunk"* finding would have been **true by
accident and unfalsifiable**. Do not rebuild this probe.

Separately, I declined to put the prod-verify password into a session transcript (`WORKFLOW_RULES`
§7). The account is low-value and purpose-made; the discipline is not conditional on that.

### 6b. The authenticated case is an in-process test, and the pattern already exists

**`tests/test_ui/test_handler_session_revalidation.py`** builds precisely this: a real user via
`UserService.register_user`, a real `Organization` and `Membership`, a real token from
`AuthService`, and calls the handler directly. Copy its shape.

| AC | test | proven red by |
|---|---|---|
| **AC1.1** guard redirects a valid session | mint a real access token onto `AuthState`, call the guard, assert `rx.redirect("/")` | today's code — there is no guard, so it returns `None` |
| **AC1.1** guard passes a signed-out visitor | empty `access_token`, call the guard, assert `None` and that `access_token` is still `""` | inverting the guard's polarity |
| **AC1.2** `signup` refuses a live session | seed a session with **two** `user_orgs`, submit the form, assert redirect | today's code — it completes the signup |
| **AC1.3** memberships survive | same fixture; assert `user_orgs` still holds **both** entries | an implementation that redirects *after* the assignment at `auth_state.py:655` |
| **AC5.1/5.2** loader guards | patch `get_sync_session` and `hooks.emit`; drive `load_dashboard` with an unauthenticated `AuthState`; assert **0** calls to each | today's code |
| **AC4.2** loading state is contentful | construct `page_layout`, assert the hydrating branch contains a text node | a spinner-only branch, i.e. today |

Two traps this project has already paid for, both live here:

- 🚨 **Do not stand in a bare `MagicMock` for `AuthState`.** It answers every attribute truthily, so
  `if org_id == 0` is never taken and the test measures nothing. `test_handler_session_revalidation.py`
  says so in its own module docstring, and `session_expired` is exactly the kind of bool it eats.
- 🚨 **Do not mock the module whose surface is the claim.** If a test asserts *"the guard redirects"*,
  the assertion must reach the real handler.

### 6c. What genuinely cannot be tested this way — and what to do instead

The **rendered pixel geometry** of §3's AC2a (is the control actually of the same order as the social
buttons?) is a browser measurement. It does not need a credential: `/login` is a **public page** and
every figure in §3 was read signed out.

So: assert the *structure* in the suite (the affordance is a button-like component, not an
`rx.text`), and take the geometry read on production signed out, exactly as the "before" numbers were
taken. **Nothing in this spec requires an authenticated production session.**

### 6d. The one thing that IS a production read, and it is not blocking

Whether the deployed container carries any of this. Cloud and core both ship on the core `master`
push, so a `dev` merge is not a deploy. That read belongs to Infra at promotion time and gates
**issue closure**, not implementation.

---

## §7 — Out of scope

| topic | owner |
|---|---|
| Social buttons on `/signup`; template + invite context through OAuth | [core#624] / [SPEC_SIGNUP_SOCIAL_AUTH.md](SPEC_SIGNUP_SOCIAL_AUTH.md) |
| Why entering the event path varies 6.2x | [core#1090] R4 — unclaimed, possibly Infra |
| The spurious personal org on an invited signup | [core#981] |
| `/signup` email-existence disclosure; the no-op prod CAPTCHA | [core#639] / [SPEC_SIGNUP_ENUMERATION.md](SPEC_SIGNUP_ENUMERATION.md) |
| Ten more one-click destructive controls | [core#851] |
| Any analytics proving any of this worked | [cloud#192] — `app.datanika.io` has emitted **none** since May 2026 |

🚨 **Nothing in this spec is measurable in production analytics.** Do not let a green deploy read as
a validated funnel.

---

## §8 — Suggested shipping order

Each step is independently mergeable; the ordering is about not building on a retracted premise.

1. **§5** (`load_dashboard` guard) — smallest, no dependency, and the negative control is free.
2. **§4** (the skeleton) — needs §0a read first, or the implementer adds a second loading state
   beside the one already there.
3. **§2** (the credential-page guard) — the only user-visible-harm item. It gives `/login` an
   `on_load`, so land it **after** §4, or the one page with a 57 ms gap acquires the window with no
   loading state to cover it.
4. **§3** (the affordance) — copy-and-layout, and the only step with a possible new i18n key.

⚠️ **Step 3 after step 2 is a real constraint, not a preference.** Reversed, `/login` spends a release
with the window and a bare spinner.

[core#624]: https://github.com/datanika-io/datanika-core/issues/624
[core#639]: https://github.com/datanika-io/datanika-core/issues/639
[core#671]: https://github.com/datanika-io/datanika-core/issues/671
[core#673]: https://github.com/datanika-io/datanika-core/issues/673
[core#851]: https://github.com/datanika-io/datanika-core/issues/851
[core#981]: https://github.com/datanika-io/datanika-core/issues/981
[core#1081]: https://github.com/datanika-io/datanika-core/issues/1081
[core#1090]: https://github.com/datanika-io/datanika-core/issues/1090
[core#1097]: https://github.com/datanika-io/datanika-core/issues/1097
[cloud#192]: https://github.com/datanika-io/datanika-cloud/issues/192
