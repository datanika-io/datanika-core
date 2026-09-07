# SPEC — Locale: reachable where it matters, and consistent within a screen

**Author:** Product · **Status:** contract, ready for Engineering · **Written:** 2026-09-07
**Binds:** Engineering. **Source of truth for:** [core#696], [core#695].
**Verified against:** `origin/master` @ `fe0a9823` (production) and `origin/dev` @ `fe0a9823`,
fetched 2026-09-07. Every count below was measured on those trees.

> 🚨 **This spec exists because a passing test proves the wrong thing.**
> `test_all_locales_have_same_keys` proves every locale has an **entry** for every key. It cannot
> see whether the entry is in the right **script**, the right **register**, or reachable by the
> person it was written for. All three are wrong today, and the suite is green.

---

## §1 — What was measured

**742 keys × 9 locales = 6,678 translated strings.** The investment is made. Three findings:

| # | finding | measurement |
|---|---|---|
| **A** | The locale switcher is **sidebar-only**, so no visitor can change language before signing in | `language_switcher()` has **exactly one call site**: `layout.py:38`. `login.py` / `signup.py` / `forgot_password.py` / `reset_password.py` do not use `page_layout` |
| **B** | The locale **does not persist at all** | `I18nState.locale` is a plain `rx.State` var defaulting to `"en"`. **Zero** `rx.Cookie` / `rx.LocalStorage` anywhere in `datanika/ui/`; no `locale` column on any model |
| **C** | Two locales are internally inconsistent | `sr.json`: **14 screens render both scripts**. `es.json`: **4 screens mix tú and usted** |

**58 keys sit on pre-auth screens** (`auth.` / `login.` / `signup.` prefixes). Translated into
8 non-English locales, that is **464 strings that exist and cannot be reached** by the audience
they were written for — the people deciding whether to try us at all.

### 1a · 🔑 A and B are one defect, and fixing A alone produces a fix that mostly works

This is the part to get right, because the obvious change is a half-fix that will look complete.

Put a switcher on `/login` and a visitor can pick Serbian — for **that state session**. There is
nothing to write the choice to. So:

- close the tab, come back tomorrow → **English**;
- open the **emailed password-reset link**, which is a fresh session and often a different browser
  from the one that made the choice → **English**;
- the documented Redis restart that logs everyone out → **English**.

🚨 **The password-reset link is the worst case and it is the most likely one.** It is the pre-auth
screen a user is *sent* to rather than one they navigate to, so it is exactly where a
session-scoped preference cannot survive. A switcher without persistence would be shipped, demoed,
and quietly useless on the screen that motivated the issue.

**So B is in scope and is not a nice-to-have.**

---

## §2 — [core#696]: the contract

### 2.1 · AC1 — the switcher renders on all four pre-auth screens

`/login`, `/signup`, `/forgot-password`, `/reset-password`. They share no layout with the
authenticated app, so this is a placement decision, not a `page_layout` change.

**Do not solve it by giving the pre-auth pages `page_layout`.** That would drag the sidebar,
`check_auth` and the navigation onto pages whose whole property is that they are reachable signed
out — and [core#1090]'s hydrating skeleton is now in that layout, so it would put an app shell
behind a login form.

### 2.2 · AC2 — the choice survives a new browser session

**The property, not the mechanism:** a visitor who picks a language on `/login`, closes the
browser, and later opens an emailed reset link **on the same browser** sees that language.

`rx.Cookie` is the obvious candidate and needs no schema change, no auth and no DB read on a public
page. Engineering picks; the AC is the property. ⚠️ Whatever is chosen must work **before** there
is a user row — a `users.locale` column cannot serve `/signup`, which is the screen with no user.

⚠️ **A cookie is a stored preference, and `/privacy` enumerates what we store.** If the mechanism
sets a cookie, the privacy page's cookie statement is part of this change, not a follow-up. That is
the standing rule in `CLAUDE.md` about legal pages being representations rather than copy.

### 2.3 · AC3 — the signed-in preference and the pre-auth preference are the same preference

A user who has set Serbian inside the app and then signs out must not land on an English `/login`.
Today `I18nState.locale` is one var for both, which is right; AC2's persistence must not fork it
into two.

### 2.4 · AC4 — `Accept-Language` as the initial value only

A first-time visitor whose browser says `sr` should not have to find a control to discover we speak
Serbian. Use `Accept-Language` **only** when no explicit choice is stored; an explicit choice always
wins and is never overwritten by a header.

⚠️ **Scoped as separately shippable.** AC1–AC3 deliver the issue. If AC4 slips, say so rather than
folding it in silently — but do not invert them: **a header default without a switcher is worse
than neither**, because a user shown a language they did not choose has no way to leave it.

### 2.5 · AC5 — i18n for the control itself

The switcher's own affordance must not be English-only. `SUPPORTED_LOCALES` should present each
language **in its own language** (`Deutsch`, `Español`, `Српски`), which is the one labelling that
needs no translation and is legible to the person looking for it.
⚠️ `language_switcher.py:17` currently passes `search_placeholder="Language..."` — a raw English
literal inside the very control this issue is about.

---

## §3 — [core#695]: the contract

### 3.1 · The measurement, and two corrections to my own first pass

**`sr.json` — 14 screens render both scripts.** 427 Cyrillic values, 138 Latin-script Serbian:

| screen | cyrillic | latin | | screen | cyrillic | latin |
|---|---|---|---|---|---|---|
| `pipelines` | 26 | **23** | | `quota` | 2 | **8** |
| `auth` | 34 | **22** | | `dag` | 9 | 7 |
| `connections` | 78 | **20** | | `schedules` | 12 | 6 |
| `transformations` | 23 | 11 | | `api_keys` | 10 | 6 |
| `notifications` | 34 | 11 | | `uploads` | 41 | 4 |
| `account` | 18 | 9 | | `common` | 18 | 1 |
| `settings` | 40 | 9 | | `models` | 8 | 1 |

🔴 **Two counts my first pass produced are artefacts and must not be used.**

1. *"99 values mix scripts within one string."* **61 of those are correct** — Cyrillic prose
   containing a brand or technical token (`Datanika`, `URL`, `JSON`, `GCP`). Of the remaining 38,
   nearly all are interpolation placeholders (`{arg}`, `{terms}`) or code examples (`customers`,
   `updated_at`, `public`). **Within-string mixing is effectively zero; the defect is between
   strings on one screen.**
2. *"74 Serbian values are untranslated."* They are placeholders (`you@example.com`, `localhost`,
   `mydb`) and the brand name — which `WORKFLOW_RULES` §6 lists under **Skip**. Correct, not a gap.

**`es.json` — 4 screens mix register**, 75 tú keys against **9** usted keys, and **0** strings carry
both. 🔴 My first pass said 7 carried both; that used `puede`/`debe`/`tiene` as formal markers, and
those are equally **third person** — *"Qué puede hacer la aplicación"* is "what the app can do", not
usted. **Modals cannot discriminate register in Spanish; imperatives and possessives can.**

### 3.2 · 🔑 The root cause is chronological, and it is why a cleanup alone will not hold

The Latin-script Serbian keys are not scattered. On the `auth` screen they are **exactly the
password-reset flow** — `forgot_password*`, `reset_link_sent_*`, `reset_password_heading`,
`new_password`, `set_password`, `reset_link_invalid_*`, `back_to_sign_in`, `signed_out_body` —
i.e. **the batch [`SPEC_PASSWORD_RESET`](SPEC_PASSWORD_RESET.md) added.** The strings written before
it are Cyrillic; the ones written during it are Latin.

The Spanish outliers are the same shape: three of the nine are the *same sentence* under
`{uploads,pipelines,transformations}.destination_invalid`, and two more are
`notifications.*.body` — batches, not individuals.

> **Each batch of new keys is translated to whatever convention its author picked, because no
> convention is recorded anywhere.** The parity test forces a key to *exist* in all nine files and
> says nothing about what goes in it.

So a one-time sweep fixes 147 strings and the next feature reintroduces the defect. **§3.3 is the
part that lasts.**

### 3.3 · AC1 — record the conventions, in the repo, next to the files they govern

A short `datanika/i18n/CONVENTIONS.md` stating, per locale, the decisions below. It is the artifact
a contributor adding nine keys reads; today there is nothing to read.

### 3.4 · AC2 — **Decision: Serbian is CYRILLIC**

`sr` unsuffixed conventionally means Cyrillic (BCP-47 spells the other `sr-Latn`), it is already
the majority here (427 vs 138), and it makes the cleanup the smaller of the two directions.

⚠️ **The decision that matters is *a* choice, not *this* choice** — mixing is the defect, and either
consistent answer beats it. Recorded so it is not re-litigated per batch.
⚠️ **Not in scope: offering both.** A `sr-Latn` locale is a tenth locale and a product decision with
a cost; this spec picks one and says so.

### 3.5 · AC3 — **Decision: Spanish is TÚ**

75 against 9, and informal is the register this product's English already uses (*"Check your
inbox"*, not *"Please check your inbox"*). **Nine strings change**, all named:

`connections.or_enter_path` · `models.no_models_after_load` · `uploads.select_endpoints` ·
`uploads.destination_invalid` · `pipelines.destination_invalid` ·
`transformations.destination_invalid` · `notifications.quota_warning.body` ·
`notifications.charge_failed.body` · `api_keys.revoke_irreversible`

⚠️ **`{uploads,pipelines,transformations}.destination_invalid` are three keys holding one sentence.**
Fix all three or the screens disagree with each other — which is this defect one level up.

### 3.6 · AC4 — a guard that can fail

The parity test is not extendable to this; it compares key **sets**. A second guard, and it must be
shown red against today's tree before the sweep:

- **`sr.json`:** every value that is translated (differs from `en`) and contains a Latin run of 2+
  letters, **excluding** interpolation placeholders, code examples and an allowlist of brand and
  technical tokens, is a violation. **Today: 138.** After the sweep: 0.
- **`es.json`:** no value carries a formal-imperative or `usted` marker. **Today: 9.** After: 0.
  🚨 **Do not implement this with modal verbs.** `puede`/`debe`/`tiene` are third person as often as
  they are usted; a matcher built on them reports correct strings as violations, which is the
  failure direction that gets a guard deleted.

⚠️ **The allowlist is the hazard.** It is a hand-maintained list of spellings — the shape this
project keeps paying for. Keep it short, derive what you can (the connector type names already
exist as an enum), and make an unrecognised Latin token a **failure**, so adding a brand is a
deliberate edit rather than a silent pass.

---

## §4 — What this spec does not decide

- **Machine-translation quality generally.** These two locales were measured because [core#695]
  named them. **The other six are unaudited**, and §3.6's guard covers script and register only —
  it says nothing about whether a Cyrillic sentence is *good Serbian*. Say that plainly rather than
  letting a green guard read as a quality claim.
- **A tenth locale (`sr-Latn`), or dropping any locale.**
- **RTL layout for `ar`.** Present in the file, unaudited as a rendering question, out of scope.
- **Translating placeholders and status enums.** `WORKFLOW_RULES` §6 says skip; the 74 "untranslated"
  Serbian values are that rule working.

---

## §5 — Test design

**Product states the property; Engineering chooses the assertion and shows it red.**
[`SPEC_EARNED_VERDICTS.md`](SPEC_EARNED_VERDICTS.md) §5.1, earned the hard way in
[`SPEC_AUDIT_TRAIL.md`](SPEC_AUDIT_TRAIL.md) §4, where three of my test-design clauses were wrong
and two went red on correct code.

Two properties above have free negative controls — **§3.6 must be red at 138 and 9 today** — and
they are the ones to run first, because a guard that is green on arrival has not been shown to work.

🚨 **§2's ACs have no free control**, and that is the risk in this spec. Nothing today can produce a
persisted locale, so "the choice survives a new session" cannot be shown red against the current
tree; it can only be shown green against the new code. **Arm it the other way**: after implementing,
delete the persistence and require the test to fail. A test that has only ever passed has not been
shown able to fail.

⚠️ And assert the property a **user** has, not the storage: *"a second, cold state session renders
Serbian"*, never *"a cookie named `locale` exists"*. The second passes for a cookie nothing reads —
which is [core#646]'s defect exactly, a value set under a name the consumer never asked for.

[core#646]: https://github.com/datanika-io/datanika-core/issues/646
[core#695]: https://github.com/datanika-io/datanika-core/issues/695
[core#696]: https://github.com/datanika-io/datanika-core/issues/696
[core#1090]: https://github.com/datanika-io/datanika-core/issues/1090
