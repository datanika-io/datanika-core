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

---

## 7. Addendum, 2026-09-03 — the five handlers Engineering handed back

Engineering shipped D1, D2 and D6 in [PR #960]. Building D2's classifier by **behaviour** rather than
by name — the thing §2a insisted on — selected **40** committing handlers where §2 had measured ~20.
Twenty of the extra are mutations that legitimately do not toast, and the ratchet records the reason
beside each. **Five were handed back undecided**, correctly: which of them owes the user an
acknowledgement is a product question, and a guard cannot answer it.

This section decides all five. It is written as a decision, not a proposal: the ratchet lists in
`tests/test_ui/test_mutations_acknowledge.py` are the executable form of it, and each entry there
points here.

### D7 · The decision, per handler

| handler | dialog | toast | why |
|---|---|---|---|
| `SettingsState.leave_org` | **yes** | **no** | the outcome is a navigation; see D7a |
| `SettingsState.transfer_ownership` | **yes** | **yes** | irreversible *by the actor*; see D7b |
| `SettingsState.cancel_invitation` | **yes** | **yes** | [core#851]'s eleventh site; see D7c |
| `SettingsState.change_member_role` | **no** | **yes** | see D7d |
| `SettingsState.update_org` | **no** | **yes** | see D7e |

Two of the five get **no dialog**, and that is a decision rather than an omission. §D5 of this spec
and [core#851]'s own argument for excluding `remove_model` are the same argument: a confirmation on
something that does not warrant one teaches the user to click through the ones that do. A dialog is
spent capital.

#### D7a · `leave_org` — the dialog **is** the acknowledgement, and a toast is not available

This is [core#851]'s **twelfth** site and the highest-consequence one-click control in the product.
Three properties separate it from every other entry on that list:

- It is deliberately **not role-gated** (`SPEC_ORG_ROLES` R6 — leaving is the one action every member
  has), so *every* member sees it, including the ones with no way back.
- Every other entry deletes a **row**. This one removes the actor's access to all of them. The
  membership is soft-deleted, so an operator can restore it — but the user cannot, cannot see that it
  is restorable, and has been ejected from the surface that would have said so.
- Its terminal statement is `auth_state.switch_org(...)` **or** `auth_state.logout()`, and the button
  gives the user no way to tell which they are about to get.

**No toast, for two independent reasons — and the first is mechanical.** `leave_org` ends in
`return <event>`. Adding `yield await self._saved_toast(...)` makes it an async generator, and
`return` *with a value* inside an async generator is a **`SyntaxError`**, not a runtime subtlety:

```
async gen + `return value`        -> SyntaxError: 'return' with value in async generator
plain coroutine + `return value`  -> compiles          (control)
async gen + `yield event`         -> compiles          (the available route)
```

The available route — yielding both terminal events instead of returning them — works, and is still
wrong: the event is a **navigation**. A toast racing a logout redirect renders on `/login` if it
renders at all. **The dialog before the act is the acknowledgement; the destination is the outcome.**

So `leave_org` stays in `ACKNOWLEDGED_ELSEWHERE`, but with an argument in place of the placeholder
comment that said it was "arguably a defect".

**What the dialog must contain**, and this is the product half rather than the widget half:

1. The **organization it is about to leave, by name**. The button sits in a members table where the
   only other red control confirms by id *and* email; leaving names nothing at all today.
2. 🚨 **Which of the two outcomes will happen.** *"You'll be signed out"* and *"You'll be switched to
   `<other org>`"* are materially different events and the control is identical for both. A dialog
   that says only "are you sure?" leaves the more serious outcome undisclosed, which is the failure
   this whole class is about.
3. That an admin can re-invite them — the honest limit of the undo, stated the way
   `settings.remove_member_reversible` states it for the mirror action.

#### D7b · `transfer_ownership` — irreversible by the person clicking it

Owner-only, the **only** route to `MemberRole.OWNER`, and it demotes the actor in the same
transaction (`_load_current_role` re-reads the role immediately afterwards). Only the new owner can
transfer it back, so the undo lives in somebody else's hands — `leave_org`'s shape applied to control
rather than to access.

Today it is a select plus a button, with no confirmation and no acknowledgement. The failure case is
already visible (`error_message` renders); **success is silent**, so the two most likely readings of
a successful transfer are "nothing happened" and "it failed quietly".

`settings.transfer_ownership_help` already states the consequence honestly. That is help text on a
card, read before the select is touched — not a confirmation at the moment of the act. Both.

#### D7c · `cancel_invitation` — the lightest dialog of the three, and it still earns one

Persists, writes an audit row with `action="delete"`, commits. [core#851] found it as the eleventh
site and rated it *"low, and lower than the ten already listed"* — which is right, and is the reason
its dialog is a plain confirm rather than a warning: re-inviting fully restores the state and nobody
loses access they already had.

It earns one anyway because it sits **between two controls on the same card that both confirm**
(`_remove_member_dialog` above it, `_delete_channel_dialog` below). On a page that has established
the pattern, the absence of a dialog reads as a statement that this action is safe.

The toast is the load-bearing half here: the only evidence a cancellation happened is a row leaving a
table — and [core#872] is the measurement that a table in this product can show a stale row set right
after your own successful mutation.

#### D7d · `change_member_role` — a toast, and deliberately no dialog

**No dialog.** The control is an `rx.select`. A confirmation on every option change fires on
`viewer → editor` exactly as it fires on `editor → admin`, and a dialog that fires on harmless
changes is how a user learns to dismiss dialogs unread. That is [core#851]'s `remove_model` argument,
and it applies with more force here because the control is not destructive in most of its range.

**A toast, without qualification.** This handler changes *another person's* privileges and says
nothing. Promoting to admin hands over every destructive control [core#851] enumerates; demoting
takes them away mid-session. The actor currently gets no confirmation that the grant landed, and the
member it affects gets none either — the second half is out of scope here and belongs with
notification work, but the first half is one line.

#### D7e · `update_org` — a toast, and deliberately no dialog

**No dialog.** There is an explicit Save button; the click is the declared intent, and a settings
form is the canonical place where a confirmation buys nothing.

**A toast**, because success and failure are pixel-identical. The handler writes
`self.org_name = self.edit_org_name` and returns; the form goes on displaying exactly what the user
typed whether the write landed or not. It also carries `default_dbt_schema`, which decides where
future transformations write — a consequential change currently made with no receipt.

### D8 · Closing the census, and four ways it was blind

Deciding D7a exposes that [core#851]'s guard could not see the control it was deciding about. Both
gaps that issue names are real, and measuring turned up two more. All four are in
`tests/test_ui/test_delete_confirmation_and_blocked_uploads.py`.

| # | gap | consequence |
|---|---|---|
| 1 | the predicate is a list of **spellings** (`delete_ remove_ revoke_ purge_`) | misses `cancel_invitation` and `leave_org` |
| 2 | the matcher only walks `ast.Call` | misses **every** handler taking no arguments — a Reflex handler with no row id is referenced without parentheses, so there is no `Call` node |
| 3 | a confirmation is recognised **only** as `alert_dialog.action` | `delete_account` confirms through a form's `on_submit` inside `alert_dialog.content` |
| 4 | 🚨 the role check is a **substring over the handler source**, which includes its docstring | see below |

**Gap 2 is the one that matters most**, because it is not fixed by any amount of predicate work and
because of what it was hiding. Measured on `origin/dev`: the sweep sees **43** `ast.Call`-shaped
handler references and **205** `ast.Attribute`-shaped ones. The invisible set includes
`AccountState.delete_account` — account erasure, no grace period, the most destructive control in the
product. It is correctly implemented today, behind a typed confirmation. **The guard has been green
about it without being able to see it**, so an unwiring of that dialog tomorrow would not be caught.

**Gap 4 is the one worth carrying beyond this file.** `test_every_persisted_destructive_handler_checks_a_role`
tests `"_check_role" not in source`, where `source` is `ast.get_source_segment` of the handler — which
includes the docstring. `leave_org`'s docstring **explains why it deliberately has no role check**,
and that explanation contains the literal `_check_role`. Measured, with `remove_member` as the
positive control:

| handler | substring in source | in docstring only | AST: really calls it |
|---|---|---|---|
| `SettingsState.leave_org` | ✅ | ✅ | ❌ |
| `SettingsState.remove_member` | ✅ | ❌ | ✅ |
| `AccountState.delete_account` | ❌ | ❌ | ❌ |

So the instant gap 2 is closed and `leave_org` becomes visible, the guard reports it as role-checked
**because of a sentence saying it is not.** 🔑 *A substring check over source is satisfied by prose
about the code, and the prose most likely to contain the token is the comment explaining why the
token is absent.*

`leave_org` and `delete_account` both have a real refusal — `_require_live_session` plus a
service-level invariant (the owner-count check; the sole-owner refusal) — and neither is a role
check. That is declared, per handler, and checked by AST rather than by substring.

### D9 · Copy — 16 further keys, all nine locales

| key | English |
|---|---|
| `settings.org_saved_toast` | Organization settings saved |
| `settings.role_changed_toast` | Role updated |
| `settings.ownership_transferred_toast` | Ownership transferred |
| `settings.invitation_cancelled_toast` | Invitation cancelled |
| `settings.leave_org_title` | Leave this organization? |
| `settings.leave_org_body` | You lose access to everything in this organization immediately — connections, pipelines, uploads and their history. |
| `settings.leave_org_signs_you_out` | This is your only organization, so you will be signed out. |
| `settings.leave_org_switches_you` | You will be switched to your other organization. |
| `settings.leave_org_reversible` | An admin can invite you back, but you cannot undo this yourself. |
| `settings.leave_org_confirm` | Yes, leave |
| `settings.transfer_ownership_title` | Transfer ownership? |
| `settings.transfer_ownership_body` | They become the owner and you become an admin, immediately. |
| `settings.transfer_ownership_irreversible` | Only the new owner can transfer it back. |
| `settings.transfer_ownership_confirm` | Yes, transfer ownership |
| `settings.cancel_invitation_title` | Cancel this invitation? |
| `settings.cancel_invitation_reversible` | The link stops working. You can send a new invitation at any time. |

⚠️ **`settings.leave_org_signs_you_out` and `settings.leave_org_switches_you` are mutually exclusive
and exactly one must render.** Rendering both, or neither, is the defect D7a names — a dialog that
does not disclose which outcome it is about to produce.

### D10 · Acceptance criteria for this addendum

Numbered from 10 so §5's nine are unambiguous.

10. **Leaving names the organization and states the outcome.** Open the dialog as a member of two
    orgs and as a member of one; the two renders differ, and each says which thing will happen.
    *(A dialog that renders the same text in both cases fails, however correct its buttons are.)*
11. **All four new toasts fire on success and none fires on failure.** Exercise each failing branch —
    a role refusal for `change_member_role`, a duplicate slug for `update_org`, a successor who is
    not a member for `transfer_ownership` — and assert the error is visible and **no** success toast
    was yielded. *(§5 criterion 9, applied to the four handlers this addendum adds.)*
12. 🚨 **The widened census sees `leave_org` and `delete_account`.** Assert both by name, not by a
    count. *(A count rises for the wrong reason; these two are the ones gap 2 was hiding, and
    `delete_account` is the control whose correctness nobody could previously check.)*
13. **The role assertion is shown red by deleting a real `_check_role` call**, and green is not
    obtainable by writing `_check_role` in a comment. *(Both directions. The second is the actual
    defect — mutate a real handler to mention it only in prose and confirm the guard still fails.)*
14. **Each of the three new dialogs is shown to mutate nothing on the trigger** by rewiring its
    handler onto `alert_dialog.trigger` and confirming the guard names that site.
15. **Every disagreement between the verb census and the audit census is declared**, and adding an
    undeclared one fails. *(A silent union hides class B — a handler that persists and writes no
    audit row is a finding, and that is how [core#934] was found.)*

[PR #960]: https://github.com/datanika-io/datanika-core/pull/960
[core#934]: https://github.com/datanika-io/datanika-core/issues/934

[core#804]: https://github.com/datanika-io/datanika-core/issues/804
[core#851]: https://github.com/datanika-io/datanika-core/issues/851
[core#869]: https://github.com/datanika-io/datanika-core/issues/869
[core#872]: https://github.com/datanika-io/datanika-core/issues/872
[core#887]: https://github.com/datanika-io/datanika-core/issues/887
