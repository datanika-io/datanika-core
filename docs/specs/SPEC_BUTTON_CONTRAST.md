# SPEC — Button colour and contrast

> **Owner: Product.** Written 2026-09-23 to decide the `color-contrast` class of
> [#1409](https://github.com/datanika-io/datanika-core/issues/1409), the last of six classes blocking
> [#720](https://github.com/datanika-io/datanika-core/issues/720)'s graduation. Engineering routed it
> here because the options change how the product looks; that is a brand decision, not a lint fix.
>
> This is a **contract**, not a description of what shipped. It binds every button written after it.
>
> **Amended 2026-09-23** — **§11** rules callouts (they were excluded by §7 on purpose, and
> [cloud#255] showed the exclusion had no reader), and **§5 carries a correction to its own `soft`
> column**, found while taking §11's measurements. Every §11 number is read from the **running
> app's served stylesheet**, arithmetic armed against two published pairs first, with the token pair
> confirmed against a rendered element.

## 0. The decision

1. **The accent is `violet`.** `datanika/datanika.py` passes an explicit theme to `rx.App`:

   ```python
   app = rx.App(theme=rx.theme(accent_color="violet", gray_color="mauve"), head_components=…)
   ```

2. **`variant="solid"` is reserved for scales whose step 9 clears 4.5:1 against its own label
   colour.** In our palette that is the accent (`violet`) and the two dark-label scales we use
   (`amber`, `yellow`). **No button may be solid in any other scale** — not `red`, not `gray`, not
   `blue`, not `green`, not `orange`.

3. **Every button that carries a non-accent `color_scheme` in a non-solid variant carries
   `high_contrast=True`.** Destructive actions are therefore
   `rx.button(…, variant="soft", color_scheme="red", high_contrast=True)`.

Nothing here is a new mechanism. All three are props Radix already exposes.

## 1. Why the default was not a choice

The premise in #1409 was *"no theme is configured."* That is true of **our code** and false of the
**rendered app**, and the difference matters: `reflex/app.py:350` gives `rx.App` a default
`theme=rx.theme(accent_color="blue")`. So blue is not an absence — it is Reflex's choice, made for
us, never reviewed here, and it is the single largest source of contrast failures in the product.

Radix step 9 is documented as the *solid background* step, tuned for 3:1 against its label — the
WCAG threshold for **UI components and large text**, not the 4.5:1 that button labels at `size="1"`
and `size="2"` are held to. So most Radix scales cannot produce an AA-compliant solid button at all.
That is the constraint this spec is written against, and it is why the answer is a rule about which
scales may be solid rather than a darker shade of the same colour.

## 2. How the numbers were taken

Every value below is read from **`@radix-ui/themes@3.2.1`** — the version the installed Reflex pins
(`reflex/components/radix/themes/base.py:112`) and the version present in `.web/node_modules`:

- **Colour values**: `tokens/colors/<scale>.css`, the first top-level block only
  (`:root, .light, .light-theme`). ⚠️ **Reading past it silently gives the `.dark` value**, which is
  a different colour and looks perfectly plausible.
- **Which token each variant paints with**: `components.css`, not from memory —
  - `solid` → `background-color: var(--accent-9)`, `color: var(--accent-contrast)`
  - `solid` + `high_contrast` → `var(--accent-12)` background, `var(--gray-1)` label
  - `soft` → `var(--accent-a3)` background; `soft`/`ghost` label is `var(--accent-a11)`, or
    `var(--accent-12)` under `high_contrast`
  - `outline`/`ghost` → page background, `var(--accent-a11)` label

The contrast arithmetic is armed against two published pairs before any result is reported —
`#000000`/`#ffffff` = 21.00:1 and `#767676`/`#ffffff` = 4.54:1 — and the alpha compositing is checked
against Radix's own design property (`--red-a3` composited on white equals `--red-3`, measured delta
0.1/255). **A contrast table with no control is a table of plausible numbers.**

The app has **no dark mode** — no `color_mode`, no `appearance`, no toggle anywhere in
`datanika/ui/` — so light-mode values are the whole story today. **If dark mode is ever added, every
number in this spec must be retaken**; that is §7's flip condition, not a caveat.

## 3. The accent: `violet`

| accent | step 9 | label | ratio | |
|---|---|---|---|---|
| `blue` | `#0090ff` | white | **3.26:1** | today — fails AA |
| **`violet`** | **`#6e56cf`** | **white** | **5.39:1** | **decided** |
| `iris` | `#5b5bd6` | white | 5.37:1 | |
| `indigo` | `#3e63dd` | white | 5.21:1 | Engineering's suggestion |
| `purple` | `#8e4ec6` | white | 5.18:1 | |
| `plum` | `#ab4aba` | white | 4.75:1 | |

`violet` is chosen over `indigo` — which clears AA equally well — because **it is already the brand
and already the product's own instinct.** Both halves are evidenced:

- `datanika-landing/src/styles/global.css` builds `.btn-primary`, `.gradient-text` and
  `.btn-secondary:hover` on **`#8b5cf6`**, which is Tailwind `violet-500`.
- The app already reaches for `color_scheme="violet"` at its most prominent CTAs — the cost
  estimator, the ELT nudge card, the getting-started checklist, the volume-quota modal — **9 sites
  that were overriding the blue default by hand.** Setting the accent makes the app's existing
  instinct the default and lets those overrides go.

🔑 **The brand hex itself is not usable and this is the reason a shade could not simply be copied
across.** White on `#8b5cf6` measures **4.23:1** — it fails AA. Radix `violet-9` `#6e56cf` is the
same hue family a little deeper, and it passes at 5.39:1. **Take the Radix step, never the landing
hex.** *(That the landing's own primary button fails AA is a real defect on a different surface,
filed separately against the landing repo; it is out of scope here.)*

## 4. The gray pairing is part of the accent decision, not a separate one

`gray_color` defaults to **`"auto"`**, and Radix maps the accent to a gray
(`dist/esm/helpers/get-matching-gray-color.js`): `violet → mauve`, `blue/indigo/iris → slate`.

**So choosing the accent silently rechooses every gray in the product.** That is not hypothetical
right now: Engineering is concurrently repointing ~88 text colours at `var(--gray-11)`. Those
references make the gray scale a load-bearing product decision rather than an implementation
detail, and **a value implied by another value is a value nothing reviews.**

It is therefore **pinned explicitly to `mauve`** — the same scale `auto` would pick, written down so
that a future accent change is a two-line decision instead of a one-line surprise.

Accessibility does not discriminate between the candidates, which is precisely why this is a brand
call: on white, `gray-11` = 5.92:1, `mauve-11` = 5.90:1, `slate-11` = 5.94:1 — all AA.

## 5. The rule: solid is reserved

| scale | solid | soft | soft + `high_contrast` | ghost / outline |
|---|---|---|---|---|
| `violet` (accent) | **5.39 AA** | 5.50 AA | 11.99 AA | 6.17 AA |
| `amber` | **10.33 AA** | 4.25 ❌ | 10.48 AA | 4.61 AA |
| `yellow` | **12.89 AA** | 4.27 ❌ | 10.27 AA | 4.57 AA |
| `red` | 3.91 ❌ | 4.54 AA | **10.84 AA** | 5.21 AA |
| `mauve` (gray) | 3.30 ❌ | 5.16 AA | 14.29 AA | 5.89 AA |
| `green` | 3.16 ❌ | 4.19 ❌ | 11.00 AA | 4.70 AA |
| `blue` | 3.26 ❌ | 4.24 ❌ | 11.26 AA | 4.75 AA |
| `orange` | 2.97 ❌ | 3.99 ❌ | 10.28 AA | 4.51 AA |

Two traps this table exists to prevent, both of which the obvious repair walks straight into:

- 🚨 **"Just make it soft" creates new failures.** Plain `soft` fails on `amber`, `yellow`, `green`,
  `blue` and `orange` (3.99–4.27). That is why rule 3 says `high_contrast` **always**, not
  "where needed" — the exceptions are not where intuition puts them.
- 🚨 **`ghost` and `outline` pass by a hair on exactly the scales solid handles well** — `orange`
  +0.01, `yellow` +0.07, `amber` +0.11 over the 4.5 threshold. A margin that small is not a pass
  worth shipping; it is a red waiting for a token revision. Reach for `high_contrast` there too.

Plain `soft` red is **4.54:1 — a margin of +0.04**, and is rejected for the same reason. Under rule 3
it becomes 10.84:1.

> 🔴 **CORRECTED 2026-09-23 — the `soft` column above composites the label onto WHITE instead of onto
> the element's own tinted background. It understates every alpha-label scale.** Found while ruling
> §11, by taking the same measurements off the **running app's served stylesheet** rather than off the
> package files, and reproducing both models side by side:
>
> | scale | `a11` opaque? | §5 above (label over **white**) | corrected (label over **its own `a3` background**) |
> |---|---|---|---|
> | `orange` | yes | 3.99 | **3.99** |
> | `amber` | yes | 4.25 | **4.25** |
> | `yellow` | yes | 4.27 | **4.27** |
> | `blue` | no | 4.24 | **4.26** |
> | `green` | no | 4.19 | **4.27** |
> | `red` | no | 4.54 | **4.61** |
> | `mauve` | no | 5.16 | **5.64** |
> | `violet` | no | 5.50 | **5.80** |
>
> 🔑 **The two models agree exactly wherever `a11` is fully opaque and diverge wherever it carries
> alpha** — which is what identifies the cause rather than merely noticing a discrepancy. A `soft`
> label is painted *on top of* the `a3`-over-white field, so the corrected column is the physically
> real one.
>
> ✅ **No decision in this spec moves, and the error is in the conservative direction** — it
> understates contrast by 0.02–0.48, so it can never have let a failing pair read as passing. Every
> ❌ above is still ❌ and every AA is still AA; plain `soft` red goes from +0.04 to +0.11 over the
> threshold, which §5 rejects on margin either way. **It is corrected rather than left alone because
> §11 quotes these numbers, and a spec carrying two tables of the same quantity makes the reader
> decide which one is real.**
>
> ⚠️ **Independent corroboration that the arithmetic is otherwise sound:** [cloud#255]'s grader,
> written separately by Engineering, reported `orange` soft at **3.99** — a scale where the two
> models *cannot* disagree. Agreement there and divergence only on alpha labels is the signature of
> this cause and not of a broken calculator.

## 6. Destructive actions

**One treatment, not two: `variant="soft" color_scheme="red" high_contrast=True`** — a light red
field (`#feebec`) with a deep red label (`#641723`), 10.84:1.

Three things were weighed and the alternatives are recorded so this is not re-opened:

- **`solid` + `high_contrast`** (12.12:1) paints the button `#641723`, a near-black maroon. It reads
  as *disabled* more than *dangerous*, and it sits oddly beside a violet primary. Rejected.
- **A darker red scale.** There is none: measured across the whole red family, every step 9 fails
  with white text — `red` 3.91, `ruby` 3.89, `tomato` 3.87, `crimson` 3.85, `pink` 4.12. **Solid red
  cannot reach AA in Radix**, so this was never a matter of picking a better red.
- **A second, louder treatment for the confirm button inside an alert dialog.** Rejected: it adds a
  destructive vocabulary of two for no measured gain, and the dialog's Cancel is already
  `variant="soft" color_scheme="gray"`, so soft-red against soft-gray carries the distinction by
  hue, which is the distinction that matters.

🔑 **This also fixes a hierarchy defect that has nothing to do with contrast.** Today a saturated
solid red Delete is the loudest element in every table row — louder than the action we actually want
people to take. The loudest thing on a page should be the primary path, not the irreversible one.
Quieting destructive controls is the correct direction independently of WCAG.

## 7. Scope

**In scope:** `rx.button` and `rx.icon_button` in `datanika/ui/`, light mode, the six pages the
#1409 sweep covers.

**Out of scope, deliberately:** `radius`, `scaling`, `panel_background` and `appearance` stay at
their defaults — this spec changes colour, not shape. Text colours are Engineering's mechanical half
of #1409 and are not governed here, except for §10's finding.

🔴 **CALLOUTS ARE NOW IN SCOPE — §11, ruled 2026-09-23 on [cloud#255].** This sentence used to
exclude them alongside badges, and the exclusion was about **who owned which half of #1409**, not a
judgement that callouts need no contrast rule. Read as a standing scope decision it did real damage:
a callout below AA is **correct by this spec** and reportable by nothing, which is how
[cloud#255] had to be found by hand. **Badges remain out of scope and §11.7 says why — that one
*is* a judgement, and it names what has to exist before it can be ruled.**

**Flip conditions — the sentences that make this spec expire:**

- **A dark mode is added.** Every number here is light-mode. Retake them; do not assume the dark
  scales behave the same way.
- **The pinned `@radix-ui/themes` version moves.** The token values are an artifact of 3.2.1.
  The guard in §9 recomputes from the installed package, so it will go red rather than drift — that
  red is the signal to re-read this spec, not to adjust the guard.
- **`accent_color` changes.** §4 becomes wrong silently, because `gray_color` is pinned to the gray
  that pairs with *violet*.

## 8. Acceptance criteria

From the user's side, "done" is:

- **AC1** Every solid button in the app carries the violet accent or a dark-label scale, and its
  label is legible against it at 4.5:1 or better.
- **AC2** Every destructive control is unmistakably red, quieter than the page's primary action, and
  legible at 4.5:1 or better.
- **AC3** No user-visible text in the app sits below 4.5:1 against its background — **including text
  that is only on screen once a dialog is open** (see §10).
- **AC4** The #1409 sweep reports **0** `color-contrast` nodes on all six pages, and the
  `a11y-sweep.spec.ts` verdict in `e2e-staging` is green — which is what lets
  [#720](https://github.com/datanika-io/datanika-core/issues/720) graduate.
- **AC5** Nothing in the product renders Reflex's default blue accent any more. The 9 hand-written
  `color_scheme="violet"` overrides are gone, because they are now the default.

## 9. The guard

Per `WORKFLOW_RULES` §5a, the guard asserts **the invariant, not today's instance**: for every
`rx.button`/`rx.icon_button` in the component tree, resolve `(variant, color_scheme, high_contrast)`
to the tokens `components.css` paints with, read those tokens out of the **installed**
`@radix-ui/themes`, and require ≥ 4.5:1. A scale nobody has used yet is then checked the first time
somebody uses it, and a Radix version bump that moves a value goes red at PR time.

Three arms it must carry, each of which a weaker guard would omit:

1. **Anti-vacuity** — a tree walk whose match is wrong finds no buttons and makes the assertion
   trivially true. Assert the walker found the buttons that are there (the tree currently holds
   **157** `rx.button`/`rx.icon_button` call sites, 76 of them solid).
2. **A positive control** — the arithmetic must reproduce a published pair before it is trusted.
3. **Seen failing** — mutate one button back to `variant="solid" color_scheme="red"` and watch the
   guard go red. A guard never seen failing is not evidence.

⚠️ **Do not write the guard as "no source line says `color_scheme=\"red\"` with a solid variant".**
That is the absence-of-the-wrong-word shape §4 of `WORKFLOW_RULES` bans: it is satisfied by deleting
the button, and it cannot see a scale added later.

## 10. Two things the sweep's node counts understate

Recorded here because acting only on what axe printed would leave both in place.

- **`var(--gray-9)` is used as a text colour at 29 call sites** — 28 `rx.text` and one `rx.icon` —
  and measures **3.32:1**, *worse* than the CSS `gray` (3.95:1) that #1409's table identified. It
  fails on every gray scale (`gray` 3.32, `mauve` 3.30, `slate` 3.30), so no accent choice rescues
  it. These need the same repoint to `var(--gray-11)` as the 60 CSS-gray sites: **~88 sites, not
  60.** Several are on pages the sweep already flags, and one of them is
  `settings.py:1260`, a channel's `last_error` — an error message rendered at 3.32:1.
- **Content behind a dialog is not in the DOM, so axe never scores it.** `/settings` holds ten
  `var(--gray-9)` text nodes but the sweep reported one `color-contrast` node there, because the
  `delete_reversible` / `remove_member_reversible` copy lives inside alert-dialog content that is
  not mounted until the dialog opens. **That copy is precisely the text a user reads before doing
  something irreversible.** AC3 is written to cover it; a green sweep alone does not.

---

## 11. Callouts

**Ruled 2026-09-23 (Product)**, on [cloud#255]. Engineering held its fix pending this decision
because §7 had excluded callouts on purpose; §7 now points here. **The build is [core#1535]**, which
covers core and cloud together — cloud ships inside core's image, so one of them alone is half a
fix.

[core#1535]: https://github.com/datanika-io/datanika-core/issues/1535

### 11.1 · The mechanism, measured on a rendered element rather than relayed

A callout is painted exactly like a `soft` button: background `var(--accent-a3)`, label
`var(--accent-a11)`. That was the load-bearing claim in [cloud#255] and it is the one thing worth
re-deriving, because *a relayed mechanism is worse than a relayed number* — a wrong number
eventually contradicts something, a wrong mechanism never does.

Read off the **real element** in the running app (the connection form's validation callout,
`color_scheme="red"`, `rt-variant-soft`):

| | computed | token |
|---|---|---|
| `background-color` | `rgba(243, 0, 13, 0.08)` | `--red-a3` = `#f3000d14` |
| label `color` | `rgba(196, 0, 6, 0.827)` | `--red-a11` = `#c40006d3` |

Both match to the byte. **So §5's `soft` column transfers to callouts verbatim**, and this ruling
needs no new arithmetic — only the correction §5 now carries.

`rx.callout` exposes `variant`, `color_scheme` **and `high_contrast`**
(`reflex/components/radix/themes/components/callout.py`, `CalloutVariant = "soft" | "surface" |
"outline"`, default `soft`). As with §0, nothing here is a new mechanism.

### 11.2 · The decision

> **Every `rx.callout` that names a `color_scheme` carries `high_contrast=True`.**

This is **rule 3, unchanged, applied to the component it was always true of.** One rule for "a
soft-painted Radix surface that chooses a colour" is easier to follow and easier to guard than two,
and §5's trap applies here word for word: *"just make it soft" creates new failures*, and the
exceptions are not where intuition puts them.

A callout with **no** `color_scheme` inherits the accent and needs nothing — see 11.5.

⚠️ **`variant="surface"` and `variant="outline"` are not an escape hatch.** They change the field,
not the label: both keep `--accent-a11`, and §5 already rejects `ghost`/`outline` on the scales that
pass *by a hair*. If a future callout uses them, it still carries `high_contrast=True`.

**What it costs and what it buys**, measured off the running app's served stylesheet with the
arithmetic armed against `#000`/`#fff` = 21.00 and `#767676`/`#fff` = 4.54 before anything was
reported:

| `color_scheme` | core call sites | plain `soft` | `soft` + `high_contrast` |
|---|---|---|---|
| `red` | 15 | 4.61 (+0.11) | **10.93** |
| `amber` | 8 | **4.25 ❌** | **10.61** |
| `green` | 6 | **4.27 ❌** | **11.14** |
| `gray` | 3 | 5.68 | 14.61 |
| `orange` | 2 | **3.99 ❌** | **10.44** |
| `blue` | 1 | **4.26 ❌** | **11.40** |

**17 of the 35 explicitly-coloured callouts in core are below AA today**, and **0 of 62 callouts
carry `high_contrast`**. The hue still carries the meaning — the *background* is untouched and
`a12` keeps the hue — so a red callout stays unmistakably red. This is §6's treatment for
destructive buttons, arriving at the component that holds the words.

### 11.3 · 🚨 The instance that matters most is not the one on the issue

[cloud#255] is a callout on `/settings/billing`, reachable only by an org with a subscription, of
which we have **none**. It is correct and it is the least important instance.

**`ui/pages/connections.py:150-168` renders the connection-test verdict as a callout whose
`color_scheme` is computed:**

```python
color_scheme=rx.cond(test_untested, "gray", rx.cond(test_success, "green", "red"))
```

So the product's answer to *"did my connection work?"* — on the onboarding path, reached by every
user who ever adds a connection, in all nine locales — renders:

| verdict | scale | ratio |
|---|---|---|
| **success** | `green` | **4.27 ❌ below AA** |
| failure | `red` | 4.61 (+0.11) |
| not tested ([core#821]) | `gray` | 5.68 ✅ |

🔑 **The success case is the failing one.** A user who cannot read the green verdict is a user who
cannot tell success from the neutral "not tested" state that [core#821] introduced *specifically*
so those two would not be confused.

⚠️ **And the `color_scheme` is an `rx.cond`, not a literal** — the computed-variant shape §9.1 was
written about. A guard that reads source literals sees `"gray"`, `"green"`, `"red"` only if it
walks every branch of the expression. **This is the single call site most likely to be missed by
the guard that is supposed to catch it.**

### 11.4 · Where the population is, and the control that says the census can see it

Measured on `origin/dev` @ `890e6c5`:

| | count |
|---|---|
| `rx.callout(` call sites in `datanika/` | **62** |
| …naming a `color_scheme` | **35** |
| …carrying `high_contrast` | **0** |
| `rx.button(` call sites — **anti-vacuity control** | 129 |

The control is there because a census that finds 62 of something and 0 of another thing is
indistinguishable from a pattern that matches almost nothing. The heaviest files are
`pages/settings.py` (18), `pages/login.py` (9) and `components/connection_config_fields.py` (5).

### 11.5 · 🚨 The theme fixes the callouts that INHERIT and cannot touch the ones that CHOOSE

The 27 callouts that name no `color_scheme` inherit the accent. Read off the running app's own
theme element — `data-accent-color`, not from our source:

- **today: `blue`** (`--accent-a3` = `#008ff519`, identical to `--blue-a3`), `data-gray-color="slate"`
  — so those 27 render at **4.26:1, below AA**. *(This also corroborates §4: `gray_color="auto"`
  really does map blue → slate.)*
- **after §0's theme lands: `violet`** → **5.80:1**, AA.

**So §0's accent change silently repairs 27 of the 62 and leaves all 17 real failures in place**,
because a site that names `amber` or `green` keeps naming it. 🔑 **A post-promotion sweep will
therefore look substantially better while the worst instances — an error message and a success
verdict — are still below AA.** That improvement is real and it is not this ruling; do not let the
first be read as the second.

### 11.6 · Acceptance criteria

- **AC1** Every `rx.callout` naming a `color_scheme` carries `high_contrast=True`, in core **and**
  in `datanika-cloud` (which is where [cloud#255] lives, and which ships inside core's image).
- **AC2** The connection-test verdict is legible at ≥ 4.5:1 in all three of its states, success
  included. Assert it on the **rendered** element, not on the source literal — 11.3's
  `color_scheme` does not exist as a literal anywhere.
- **AC3** The guard of §9 is extended to `rx.callout`, resolving `(variant, color_scheme,
  high_contrast)` the same way, and **walking every branch of an `rx.cond`**. Per `WORKFLOW_RULES`
  §5a it asserts the invariant, not today's 62: a callout added tomorrow in a scale nobody has used
  is checked the first time it exists.
- **AC4** The guard is **seen failing** — revert one callout to plain `soft` `green` and watch it
  red — and carries the anti-vacuity arm of 11.4, so a walker that stops finding callouts fails
  loudly instead of passing.

⚠️ **Do not write AC3's guard as "no source line says `rx.callout` without `high_contrast`".** That
is the absence-of-the-wrong-word shape; it is satisfied by deleting the callout, and it would red on
the 27 correct un-schemed callouts of 11.5.

### 11.7 · Badges stay out of scope, and this is the argued version of §7's mistake

`rx.badge` is painted the same way and has **33 call sites** in core. I am not ruling it here, and
the reason is not ownership this time:

**8 of those 33 compute their `color_scheme` at runtime** — `run_status_color(r.status)`,
`_type_color(m.entry_type)`, `u.status_color`, `rx.cond(...)`. Those resolve to scales chosen by a
*status enum*, so the set of scales a badge can render is not visible in the source at all, and a
rule written today would bind a population I have not enumerated. Ruling it now would repeat §7's
error in the opposite direction: a contract over sites I am guessing about.

🔔 **Flip condition, so this does not become permanently deferred:** when AC3's guard can resolve an
`rx.cond`/helper-call `color_scheme` — which AC3 requires anyway for 11.3 — **that same capability
enumerates the badge population, and badges are ruled in the next Product pass.** The capability is
the blocker, and it is being built for another reason.

[cloud#255]: https://github.com/datanika-io/datanika-cloud/issues/255
[core#821]: https://github.com/datanika-io/datanika-core/issues/821
