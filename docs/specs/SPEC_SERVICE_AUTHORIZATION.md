# SPEC — Service-layer authorization: the rule `SPEC_ORG_ROLES` §4 already decided, applied to the other eight subsystems

**Author:** Product · **Status:** contract, ready for Engineering · **Written:** 2026-09-09
**Binds:** Engineering. **Source of truth for:** [core#681] AC1–AC5 and the generalisation in
[`SPEC_ORG_ROLES.md`](SPEC_ORG_ROLES.md) §4a.
**Verified against:** `origin/master` @ `fc7266a` and `origin/dev` @ `84b21f8`, fetched 2026-09-09.
Every count below is an AST census over those trees, reproducible from §1.

> **This spec makes no new authorization decision.** `SPEC_ORG_ROLES` §4 decided where enforcement
> lives — *"In `UserService`, not in the Reflex state … the UI check stays as a second layer for the
> error message, but it is **not the control**."* It shipped for membership and nothing generalised
> it. This is the table for the rest, derived from what the handlers already declare.

---

## §1 — The census

Method is [core#886]'s, which is the one that worked: **read the requirement off each handler's own
`_check_role("<role>")`**, never off a verb list or anyone's memory.

**31 handlers declare a role. 25 of them call a service that enforces nothing.**

The 6 that are correct are `settings_state.py` — the membership surface `SPEC_ORG_ROLES` §4 governs,
backed by `UserService._assert_may_manage`. That is the whole of the enforced set.

| subsystem | `editor` | `admin` |
|---|---|---|
| **connections** | `save_connection`, `edit_connection`, `copy_connection` | `delete_connection` |
| **uploads** | `save_upload`, `run_upload` | `delete_upload` |
| **pipelines** | `save_pipeline`, `run_pipeline` | `delete_pipeline` |
| **schedules** | `save_schedule`, `toggle_schedule` | `delete_schedule` |
| **transformations** | `save_transformation` | `delete_transformation` |
| **dag** | `add_dependency` | `remove_dependency` |
| **api keys** | — | `create_api_key`, `revoke_api_key` |
| **notification channels** | — | `save_channel`, `edit_channel`, `toggle_channel_active`, `delete_channel` |
| **backup / export** | — | `export_backup`, `handle_restore_upload` |

🔴 **Correction to a number in circulation.** This census has been relayed as *"16 state-layer files"*.
It is **12**, on `master` and on `dev`, and `SPEC_ORG_ROLES` §4a says 12. (Engineering's [core#673]
censused **39 handlers**, which is a different unit and is not this number.) Re-derive with:

```bash
git grep -l '_check_role' origin/master -- 'datanika/ui/state/' | wc -l   # 12
git grep -l '_assert_may_manage\|actor_user_id' origin/master -- 'datanika/services/' | wc -l   # 2
```

⚠️ **My own first run of this census reported 31 of 31 as UI-only.** The service-module map had no
entry for `settings_state.py`, so the six *correct* rows — the only enforced ones in the product —
read as the worst offenders. **A census whose lookup table is incomplete reports the fixed thing as
broken**, and the number that came out (31/31) was rounder and more alarming than the true one.

---

## §2 — The rule these 25 already follow, so this is not 25 decisions

Read down the table and the pattern is one sentence:

> **`editor` for the ordinary lifecycle; `admin` for deletion, and for anything that touches a
> credential.**

Three subsystems are `admin` throughout, and each has a reason already written at the call site:

| subsystem | why `admin` even for create/edit |
|---|---|
| **api keys** | the object *is* a credential |
| **notification channels** | the row holds the Slack webhook URL / Telegram bot token |
| **backup / export** | an export decrypts every connection config in the org — redaction keeps the secrets out, the infrastructure map stays ([core#651]) |

**So Engineering needs one rule and three exception families, not 25 judgements.** ⚠️ **Do not invent
a fourth threshold** while implementing. If a service seems to need one, that is a Product question —
raise it rather than choosing.

---

## §3 — 🚨 "There is no other surface today" is FALSE. It shipped, and it authorizes differently.

`SPEC_ORG_ROLES` §4 and [core#681] both argue from *"a rule enforced only in the UI is a rule the
next surface will not have."* Measured on `master`, **the next surface is already here**:

**26 mutating REST endpoints, across exactly the eight subsystems in §1** —
`connections:write` ×4, `notifications:write` ×6, `pipelines:write` ×4, `uploads:write` ×4,
`transformations:write` ×4, `schedules:write` ×3, `runs:write` ×1.

And it authorizes on a **different model**. `ApiKey` carries `user_id`, `scopes`, `expires_at` — and
**no role**. `ApiKeyService.authenticate_api_key` checks the key hash, `deleted_at`, expiry and
scope. `Membership`, `MemberRole` and `.role` appear **0 times** in `api_key_service.py` or
`api_middleware.py`.

**Two consequences, and neither needs a new surface to bite:**

1. **A key's authority is never intersected with its owner's current role.** An admin mints a key
   with `connections:write`; the admin is later demoted to `viewer` or removed from the org; **the
   key keeps working.** The industry norm is that a token's effective authority is the *intersection*
   of its scopes and the owner's current permissions. We implement the first half only.
2. **The scope model has no delete/edit granularity.** `connections:write` covers create, edit **and
   delete** — while §1's table reserves deletion for `admin` and permits saving at `editor`. So the
   scope is strictly coarser than the role rule it is meant to mirror.

🔑 **This is the argument for fixing the service layer rather than the handlers.** A check in
`ConnectionService` is inherited by both surfaces. A check in `ConnectionState` is inherited by
neither of the 26 endpoints. **Every hour spent hardening the Reflex handler widens the gap between
the two surfaces rather than closing it.**

⚠️ **This spec does NOT decide the scope↔role intersection.** That is a real product decision with a
compatibility cost — intersecting could break a key working today — and it belongs to whoever owns
the API contract. Filed and named here so the service-layer work is not mistaken for having closed
it. **§5 AC6 asserts only that the service check applies to the REST path too**, which is the part
that follows from this spec.

---

## §4 — The mechanism, and the two ways to get it wrong

**Copy `UserService._assert_may_manage`.** It is the shape this project already chose, shipped and
tested.

- The service method **takes an `actor_user_id`** and resolves that actor's membership itself.
- 🚨 **Do not put `_check_role` in a service.** It reads Reflex state; a service reached from a REST
  request has none. That is the same category error as the UI-only check, one layer down.
- 🚨 **Do not pass a role down from the caller.** `save_connection(..., role="editor")` is not a
  check — **a caller that supplies its own authority is not being checked** — and it makes the REST
  path's job "send the right string".

**The UI check stays.** `SPEC_ORG_ROLES` §4 keeps it deliberately, as the second layer that produces
a good error message. Removing it would trade a clear refusal for a raised exception.

---

## §5 — Acceptance criteria

1. Each of the **25** handler/service pairs in §1 enforces its role **in the service**, at the
   threshold §1 records. Do not change a threshold while moving it; a re-decision is a separate
   change with a separate argument.
2. The threshold is enforced against the actor's **current** membership, resolved in the service.
3. **A guard, derived rather than listed:** for every state handler declaring `_check_role("R")`,
   the service method it calls enforces at least `R`. ⚠️ It must be **red at 25 today** — a guard
   green on arrival has not been shown to work.
4. The guard's exemption list, if any, carries a **reason per entry checked against the handler's
   own body** — [core#851]'s shape, not a list of names.
5. The UI check remains on all 25. A test asserts a `viewer` gets an *error message*, not a traceback.
6. **One REST endpoint per subsystem is driven with a key whose owner lacks the role**, and refused.
   🚨 **This is the criterion that proves the fix reached the second surface**, and it is the one that
   cannot be satisfied by hardening handlers. If it is deferred, say so — do not fold it in silently.
7. `SPEC_ORG_ROLES` §4a's census is updated with the post-fix numbers, or explicitly left as the
   historical record of the gap. **Not silently left reading 12-and-2 after it stops being true.**

### 🚨 What must not be asserted

**Do not test that the service raises when handed a bad role string.** That asserts the parameter,
not the authority, and it passes against the `role="editor"` shape §4 forbids. The property is:
*an actor whose membership does not permit the operation cannot perform it, whatever they send.*

---

## §6 — Sequencing, and the hazard that motivated the whole thing

[core#886] moved the **markup** (shipped). [core#673] moved the **handler** (shipped, 39 handlers,
4/4 mutations red). This is the **service**, and it is last.

🚨 **Two layers hardened above an unguarded one reads, from every instrument we have, as three.**
A viewer sees no control; a revoked session cannot reach a handler; every UI-level test is green —
and 26 REST endpoints still call services that check nothing. **The instruments agree with each
other and none of them looks at the third layer.**

That is why Engineering posted the census as an addendum on [core#673] rather than only on
[core#681], and it is why this spec exists rather than eight issues.

## §7 — What a refusal says, per surface

The 26 REST endpoints answer a **script**; the Reflex handlers answer a **person**. Those are not the
same message, and sharing one string gives a poor version of both.

### 7.1 · REST — a typed code, a stable shape

**`403`**, with the typed-error shape `api_v1_routes.py` already uses for `409 not_cancellable`:

```json
{"error": "insufficient_role", "message": "This operation requires the admin role.", "required_role": "admin"}
```

- **`403`, not `404`.** Within an org, a `viewer` can already *list* the resource, so hiding its
  existence buys nothing and costs the caller the one fact that lets them fix the problem.
- **`404` stays for cross-org**, unchanged — the org-scoped lookup returns nothing and that is
  correct. ⚠️ **Do not merge the two paths**: the difference between *"not yours"* and *"not allowed"*
  is the whole reason `403` is safe here.
- **`required_role` is a field, not prose.** A script retries or escalates on it; parsing an English
  sentence to decide is how integrations break on a copy edit.

### 7.2 · Reflex — a sentence a person can act on

`error_message` on the state, rendered by the existing callout. It names **what was refused**, **what
role is needed**, and **who can grant it** — because a user who reads *"insufficient permissions"* has
been told they have a problem and not how to end it.

> *"Only an admin can delete a connection. Ask an owner or admin of {org} to do it, or to change your
> role."*

- **All nine locales.** This is callout text; `WORKFLOW_RULES` §6 puts it under Translate.
- ⚠️ **One key per threshold, not one per handler.** 25 handlers, three thresholds — `editor`,
  `admin`, `owner`. Interpolate the operation. 25 near-identical keys is 225 translations that drift.
- 🚨 **Do not reuse the REST `message` string here, or the reverse.** The moment they are one key, a
  copy edit for the human changes what a script sees, and the i18n pass translates a machine field.

### 7.3 · The refusal is not a new oracle, and the ordering that keeps it that way

Check the role **after** the org-scoped lookup and **before** the mutation. Any other order leaks:
checking the role first means a cross-org probe returns `403` rather than `404`, which confirms the
resource exists somewhere.

⚠️ This is the mirror of `SPEC_SIGNUP_ENUMERATION` D5, where the limit had to come **before** the
existence lookup. **The right order is not a general rule; it is decided per surface by what the
refusal would otherwise disclose.**

---

## §8 — ✅ DECIDED 2026-09-10: **branch A. The intersection is adopted.**

> **Founder decision, recorded on [core#681].** An API key's authority is intersected with its
> owner's **current** org role at authentication time. The three branches below are kept as the
> record of what was weighed; **branch A is the answer** and B and C are closed.
>
> **What that means concretely:** the REST path passes `api_key.user_id` as the `actor_user_id`, the
> service resolves that user's membership as it stands, and **a key minted by an admin who was later
> demoted stops working.** That is the intended behaviour, not a regression to be reported.
>
> **Condition attached to the decision:** the refusal must **name the cause** — §7.1's `403` with a
> `required_role` field. 🚨 **Not a `401`, and nothing a caller would read as "expired" or
> "revoked".** A key that stops working for a reason the response does not state is the same defect
> this spec is about, arriving in the error path: the caller retries, re-mints, and never learns the
> role is the problem.

### 8a · The two questions the decision left open, answered

**Q1 — the scope↔role granularity gap. Do scopes need splitting?**

The gap is real: `connections:write` covers create, edit **and** delete, while §1 reserves deletion
for `admin`. So an `editor` holding that scope can, today, delete via REST what the UI refuses them.

**Decision: do not split the scope vocabulary. The service check closes this by itself.**

Once `ConnectionService.delete_connection` resolves the actor's membership and requires `admin`, an
editor's key is refused **whatever its scopes say**. The two vocabularies are not competing:

> **The scope grants *reach*; the role grants *authority*. The effective permission is the
> intersection, and a coarse scope stops mattering the moment it is no longer sufficient on its own.**

🔑 **So the granularity gap is a symptom of the missing service check, not an independent problem** —
and splitting scopes would be a breaking API change bought to fix something §4 already fixes.

⚠️ **This answer is conditional on §4 being implemented where it says.** A check that lands in the
Reflex handler instead leaves the gap open *and* removes the reason to split scopes. **If the service
work is descoped, Q1 reopens** — say so rather than letting the answer outlive its premise.

**Q2 — existing keys: intersected on next use, or grandfathered?**

**Decision: intersected on next use. No grandfather clause.**

- Grandfathering needs a cutoff **and an expiry**, or it is branch B wearing a date — this spec's own
  §8 table says so, and an exemption without an expiry is a permanent second authorization path.
- **At this scale it protects nobody.** Zero paying users, five internal orgs; every key in existence
  is ours. The cost of the exemption path is permanent, the benefit expires immediately.
- ⚠️ **The count was not queried** — that is a production read and Infra's. It is stated as a
  *reason*, not a measurement, and the decision does not turn on it: even a non-zero count is
  internal, and internal keys are the ones we can re-mint without telling anyone.

🚨 **Silent breakage at an unrelated moment is the whole cost of this decision, and §7.1 is what
converts it from silent to legible.** A key that stops working must say **`403 insufficient_role`
with the role it needed** — never a bare 401. **If §7.1 is descoped, this decision's only mitigation
goes with it.**

---

## §9 — The branches that were weighed *(historical — see §8)*

`plans/security/API_KEY_ROLE_INTERSECTION_2026-09-09.md` records that a key's scopes are never
intersected with its owner's current role, and the coordinator has put that to the founder. **It is
open, and §4's mechanism cannot be neutral on it.**

§4 says the service takes an **`actor_user_id`** and resolves that actor's membership. On the REST
path the only actor is **`api_key.user_id`**. So:

| branch | what the REST path passes | consequence |
|---|---|---|
| **A — intersect** | `api_key.user_id` | The service resolves that user's **current** membership. **A key minted by an admin who was later demoted stops working**, at an unpredictable moment, with no deploy. This is the industry norm and the honest answer. |
| **B — do not intersect** | something else, or nothing | The service check must be **bypassable from REST** — which is a hole in the control this whole spec exists to add. Or the REST path keeps scope-only authorization, which is **a per-surface rule**, i.e. exactly the defect. |
| **C — grandfather** | `api_key.user_id`, with keys minted before a cutoff exempt | Branch A with a migration. The exemption **must carry an expiry**, or it is branch B wearing a date. |

🔑 **Branch B is close to unbuildable without reintroducing the defect**, so the decision is more
forced than it looks — and saying that is more useful than pretending three options are open.

> ✅ **RESOLVED — branch A, 2026-09-10.** The paragraph below said *"implement §1–§7 for the Reflex
> path and leave the REST wiring behind the branch"*. **That no longer applies: build both.** AC6 is
> now an ordinary acceptance criterion rather than the thing that makes a branch visible.
>
> 🔑 **Kept because the mechanism is the transferable part.** Marking the branch — rather than
> building around it or quietly picking one — is what made it decidable, and *"implementing it as
> specified adopts branch A whether or not anyone calls it a decision"* is the sentence that forced
> the choice into the open. That is what a marked branch is for.

*(Superseded, kept as the record:)* **Until it is answered:** implement §1–§7 **for the Reflex path**,
and leave the REST wiring behind the branch. ⚠️ **Do not pick a branch by implementing one** —
passing `api_key.user_id` is branch A, and it is a decision whether or not anyone calls it one.

⚠️ **And do not read a green Reflex-only implementation as this spec being satisfied.** That is §6's
hazard exactly: hardening one more layer while the 26 endpoints are untouched moves the same number
from *"two layers of three"* to *"three of four"*.

---

[core#651]: https://github.com/datanika-io/datanika-core/issues/651
[core#673]: https://github.com/datanika-io/datanika-core/issues/673
[core#681]: https://github.com/datanika-io/datanika-core/issues/681
[core#851]: https://github.com/datanika-io/datanika-core/issues/851
[core#886]: https://github.com/datanika-io/datanika-core/issues/886
