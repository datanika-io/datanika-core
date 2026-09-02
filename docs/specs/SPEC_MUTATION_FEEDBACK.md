# SPEC — Mutation feedback: a successful create must say so

**Author**: Product · **Date**: 2026-09-02 · **Status**: contract, ready for Engineering
**Tracking**: [core#872](https://github.com/datanika-io/datanika-core/issues/872)
**Implementation**: Engineering. Product owns this spec and the acceptance criteria.
**Verified against**: `origin/dev` @ `11ab292`, by an AST walk over `datanika/ui/state/*.py` rather
than by grep — the numbers below are derived and the derivation is reproducible (§2).

---

## 1. The problem, and why it is the exact mirror of work already shipped

[core#804] and [core#851] fixed the **destructive** direction: a delete used to remove a row on the
first click, with no confirmation and no acknowledgement. Ten confirmation dialogs and nine success
toasts later, deleting is instrumented.

**Creating is not.** Measured on production 2026-08-31 while creating a BigQuery connection: the
create **succeeded**, and the page showed no toast, no inline confirmation, no error, the same four
old rows for at least five seconds, and an apparently unchanged form. Every signal available to the
user said *nothing happened*. The recovery action a user reaches for is **repeating the mutation**.

🚨 **Why this is worse than untidiness, and worse than it was a month ago.** Connection quota
enforcement went live in production on 2026-08-31. On a Free org at 4 of 5 connections, an invisible
first create **spends the last slot**, and the user's *second* click is the one that gets refused.
The success and the failure arrive in the opposite order from the one the user perceives — so the
error message they eventually read describes the wrong event.

⚠️ **The same shape has a second face**: `/connections` and `/models` render **zero rows for 5–17
seconds** after navigation while websocket data arrives. **An empty table and a not-yet-loaded table
are pixel-identical.** One `/models` poll stayed empty for a full 30 s and *that* one was a real
emptiness ([core#869]) — which is the whole problem: the honest empty and the still-loading empty are
the same picture, so neither can be read.

---

## 2. The measurement

Derived with an AST walk over `datanika/ui/state/*.py`: every public handler classified by what it
*does* (does its body open a session or commit?) rather than by what it is called, then asked whether
it can emit a toast.

| | handlers that write to the database | acknowledge the user |
|---|---|---|
| **Destructive** (`delete_*`, `remove_*`, `revoke_*`, `cancel_invitation`) | **10** | **9** |
| **Constructive** (`create_*`, `save_*`, `add_*`) | **10** | **0** |

Same denominator, opposite result. The nine go through `BaseState._deleted_toast`
(`base_state.py:155`), added by [core#804] and generalised by [core#851]; **there is no constructive
twin of that helper anywhere in the codebase.**

The ten constructive handlers, all confirmed to write: `ApiKeyState.create_api_key` ·
`ConnectionState.save_connection` · `DagState.add_dependency` · `ModelDetailState.save_model_detail` ·
`NotificationState.save_channel` · `PipelineState.save_pipeline` · `ScheduleState.save_schedule` ·
`SettingsState.add_member_by_email` · `TransformationState.save_transformation` ·
`UploadState.save_upload`. Nine open a session inline; `add_member_by_email` writes through a helper.

### 2a. ⚠️ Five handlers were excluded **by argument**, and the exclusions are the interesting part

A name-based sweep gets this wrong in both directions, so each exclusion is recorded with its reason
rather than dropped:

| excluded | why |
|---|---|
| `PipelineState.add_model` · `ModelDetailState.add_custom_test` | edit the **unsaved form's** in-memory list. No session, nothing persisted. Toasting them would train the user to ignore toasts — the same argument [core#851] used to keep a confirmation dialog off `remove_model` |
| the seven `cancel_edit` / `cancel_restore` / `cancel_custom_test_form` handlers | form dismissals. My first pass matched on the prefix `cancel_` and swept all seven in as "destructive with no feedback", which is false |
| `TransformationState.save_sql_and_return` | 🚨 **it is a bare `rx.redirect("/transformations")` and saves nothing.** The name says `save_` and the body is a navigation. A guard keyed on names would demand a success toast for an operation that does not exist |

🔑 **The rule this produces, and it is what the guard in D2 must encode: classify a handler by whether
it touches a session, never by its name.** Both directions of the naming heuristic are wrong here, and
[core#851]'s own derived list is the proof — its sweep used the verbs `delete|revoke|remove|purge`, so
it never enumerated **`SettingsState.cancel_invitation`**, which is a real one-click irreversible
database mutation on `/settings` with **neither a confirmation dialog nor a toast**, sitting between
`_remove_member_dialog` and `_delete_channel_dialog`, which have both. *(Reported on [core#851]; it is
that issue's eleventh site, not this spec's work.)*

---

## 3. Decisions

### D1 · A symmetric helper on `BaseState`, not toasts added to ten pages

`_deleted_toast(key, fallback)` exists, is called from nine handlers, reads the translated string
through the reactive `I18nState` dict, and carries a docstring that already cites this issue. **Add
its twin**, and the two should sit next to each other so a reader sees the pair:

```python
async def _saved_toast(self, key: str, fallback: str): ...
```

**Why a helper rather than ten repairs**: ten repairs is ten chances to forget the eleventh, and the
eleventh is always the handler nobody has written yet. This is the same argument [core#887] settled
for `error_message` — *"not 'render it on ten more pages' — that is ten chances to forget the
eleventh"* — and the resolution there was a derived guard, which is D2 here.

⚠️ **The toast must be `yield`ed, not returned.** The nine destructive handlers do
`yield await self._deleted_toast(...)`; the constructive handlers currently end in
`await self.load_connections()` and return. A toast that is returned instead of yielded from a
generator handler does not reach the user, and nothing fails.

### D2 · A guard that fails on the eleventh handler, modelled on `test_error_message_is_rendered.py`

[core#887] shipped `tests/test_ui/test_error_message_is_rendered.py` — an AST walk with an allowlist
that **shrinks**, both directions asserted, and four negative controls each produced by mutating the
real artifact. **Copy that shape.** Specifically:

1. **Classify by behaviour**: a handler is in scope if its body opens a session or commits. Never a
   name list (§2a).
2. **The allowlist ships with the five §2a exclusions in it, each with its reason in a comment beside
   the entry** — and a **stale entry must fail by name and refuse to pass until deleted**, exactly as
   `UNRENDERED_ERROR_MESSAGE_STATES` does. A one-direction allowlist stays green over a stale claim
   forever.
3. 🚨 **The negative control must be produced by mutating a real handler**, not by writing a synthetic
   one. [core#887]'s own audit reported ten offenders when there were two, because its control was
   written from the same mental model as the check and agreed with it including where it was wrong.
   The control that matters here: **delete the `yield` from one of the nine shipped delete toasts and
   confirm the guard names that handler.**

⚠️ **Do not write this guard as "every handler contains the string `toast`."** `save_sql_and_return`
would satisfy it by being renamed, and `add_model` would fail it for doing the right thing.

### D3 · A table that has not loaded must not look empty. This is the load-bearing half.

D1 and D2 stop the *duplicate create*. **D3 is what makes an honest empty readable**, and it is the
half [core#869] needed and did not have — a `/models` page that is genuinely empty after a successful
load is indistinguishable today from one that is still fetching.

**There is no loading flag anywhere in `datanika/ui/state/`** — measured, zero occurrences. So this is
a build, not a call.

**Decision: a three-state, not a boolean.** `is_loading: bool` is the obvious shape and it is wrong,
because its initial value has to be a guess: `False` renders "no rows" before the first fetch, and
`True` renders a spinner forever on a page whose fetch never fires. Use an explicit tri-state — *not
requested* / *in flight* / *settled* — and render:

| state | render |
|---|---|
| not requested · in flight | a skeleton or spinner. **Never the empty-state text** |
| settled, 0 rows | the empty state, with its call to action |
| settled, n rows | the table |

🚨 **"Settled with 0 rows" must be reachable in a test**, because it is the state that carries the
product's honest answer and the one that is currently unreachable by inspection.

### D4 · The submit control is disabled between submit and outcome

Cheapest of the four and it closes the double-click race directly, independently of whether the toast
renders. It is **not** a substitute for D1: a disabled button that re-enables on completion still
leaves the user with no evidence *which* outcome occurred.

### D5 · What must NOT be treated as the acknowledgement

- **The table repopulating.** It is asynchronous, it is the thing observed to lag 5–17 seconds, and on
  a slow render it arrives after the user has already clicked again. It is the *symptom*, not the fix.
- **The form resetting.** `save_connection` calls `_reset_form_fields()` on success today. The
  production observation was *"the form apparently unchanged"* — an empty form and a freshly-reset form
  look identical, and a user who filled in four fields does not read a blank form as confirmation.
- **The absence of an error.** That is precisely the reading that produces the second click.

### D6 · Fix the two untranslated run toasts in the same change

`pipeline_state.py:478` and `upload_state.py:736` both do
`yield rx.toast("Run triggered", position="top-right")` — a **hardcoded English string**, bypassing
the `I18nState` lookup that `_deleted_toast` was built to route through. Eight of nine locales show
English. They are the only two toasts in the product that do not go through a helper, they are two
lines, and they are in the files this work touches anyway.

---

## 4. Copy and i18n

`en.json` first, then all **9** locales (`test_all_locales_have_same_keys` gates it). Follow the
existing `<page>.deleted_toast` convention exactly.

| key | English |
|---|---|
| `connections.created_toast` | Connection created |
| `uploads.created_toast` | Upload created |
| `pipelines.created_toast` | Pipeline saved |
| `schedules.created_toast` | Schedule saved |
| `transformations.created_toast` | Transformation saved |
| `notifications.created_toast` | Channel saved |
| `api_keys.created_toast` | API key created |
| `dag.created_toast` | Dependency added |
| `settings.member_added_toast` | Member added |
| `models.saved_toast` | Model saved |
| `common.run_triggered_toast` | Run triggered |
| `common.loading` | Loading… |

**12 keys.** ⚠️ `common.run_triggered_toast` replaces the two hardcoded strings in D6 — it is
deliberately in `common.` rather than duplicated per page, because both call sites say the same thing.

⚠️ **A create and an update are the same handler** in seven of the ten (`save_*` covers both). The copy
above says *"created"* / *"saved"* accordingly; where one handler does both, the message must match
what actually happened — `editing_conn_id` already discriminates in `save_connection`. **A create that
says "saved" is acceptable; an update that says "created" is not**, because it tells the user a new row
exists.

---

## 5. Acceptance criteria

Product verifies these on prod after promotion. Several are written so a plausible
half-implementation fails.

1. **Creating a connection renders a visible confirmation before the table updates.** Measured by
   creating one and observing the toast while the table still shows the old row set. *(If the
   acknowledgement only appears once the table has refreshed, D5's first bullet has been implemented
   instead of D1.)*
2. **All ten constructive handlers acknowledge**, and the guard in D2 fails when any one of them stops.
3. 🚨 **The guard is shown red by deleting the `yield` from an existing, shipped delete toast**, and it
   names that handler. *(A guard validated only against a synthetic handler agrees with itself. This is
   [core#887]'s own lesson, applied to its successor.)*
4. **The guard's allowlist fails on a stale entry**: add one of the ten fixed handlers to it and the
   test fails naming that entry, refusing to pass until it is removed.
5. **A table that has not yet received data renders a skeleton, and a settled-empty table renders the
   empty state.** Both states are reachable in a test. *(Criterion 5 is the one [core#869] needed: an
   honest empty must be readable **as** honest.)*
6. **The submit control is disabled between submit and outcome**, and a double-click produces exactly
   one row. Verify by row count, not by the absence of a second toast.
7. **An update says "saved", never "created."** *(Exercise the edit path on a page whose one handler
   does both — a test covering only the create path passes on the broken implementation.)*
8. **All 12 keys in all 9 locale files**, and `grep -rn 'rx.toast("' datanika/ui/` returns **zero**
   hardcoded strings. *(D6. The count is the check; the two known sites are not.)*
9. **No toast is emitted on a failed mutation.** Force a quota refusal and confirm the user sees the
   error and **no** success toast. 🚨 *(This is the [core#872] failure mode running backwards, and it
   has a known shape in this codebase: a success toast placed at the end of a `try` body reports a
   caught failure as a success unless the `except` returns first. The `except` must return.)*

---

## 6. Ship order and scope

1. **D1 + D6** — the helper, its ten call sites, the two untranslated toasts, 12 i18n keys. Smallest,
   highest value, and it is what stops a user creating a connection twice on a quota-enforced org.
2. **D4** — the disabled control. Two lines per form.
3. **D2** — the guard. After D1, so it ships with an allowlist holding only the five §2a exclusions
   rather than fifteen entries.
4. **D3** — the tri-state loading. Largest of the four, touches every list page, and is the one that
   closes [core#869]'s readability half. It is separable and should be its own PR.

**Blocked on nothing.** No new credential, no Infra change, no migration.

⚠️ **Do not fold [core#851]'s eleventh site (`cancel_invitation`, §2a) into this work.** It is a
missing *confirmation dialog* on a destructive control, which is that issue's contract, not this one's.
Reported there.

[core#804]: https://github.com/datanika-io/datanika-core/issues/804
[core#851]: https://github.com/datanika-io/datanika-core/issues/851
[core#869]: https://github.com/datanika-io/datanika-core/issues/869
[core#872]: https://github.com/datanika-io/datanika-core/issues/872
[core#887]: https://github.com/datanika-io/datanika-core/issues/887
