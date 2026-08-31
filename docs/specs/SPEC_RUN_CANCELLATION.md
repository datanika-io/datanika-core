# SPEC — Run cancellation: what "stop" promises, and what it must not promise

> **Author**: Product, 2026-08-31. **Status**: decision of record for [core#657].
> **Verified against** `origin/dev` @ `1261554` via `git cat-file -p origin/dev:<path>` — no checkout,
> no working-tree mutation. Every `file:line` below was read there, not recalled.
>
> **Why this exists.** [core#657] reports that `POST /api/v1/runs/{id}/cancel` returns `200` and
> cancels nothing. The issue leaves the mechanism to Engineering, and that is still right — but three
> product decisions and one structural hazard have to be settled *before* code, because each of them
> changes what gets built. The hazard is the reason this is a spec and not a comment: **the obvious
> fix introduces a new run status, and this codebase reads the set of run statuses from seven
> hand-maintained lists.** Two of those produce a wrong answer that looks correct.

---

## 1. What is broken today, measured

`ExecutionService.cancel_run` (`datanika/services/execution_service.py:118-127`) writes a row and
returns. It does not revoke the Celery task, and nothing on the worker side ever asks whether it
should stop:

```
git grep -nE "CANCELLED|revoke|is_cancel" origin/dev -- datanika/tasks/
```

→ **0 hits**, re-run against `1261554`.

The task therefore runs to completion and calls `complete_run`
(`execution_service.py:39-54`), which at **`:49`** sets `RunStatus.SUCCESS` **unconditionally**. The
cancellation is cosmetic and transient: the row flips back. The API layer
(`services/api_v1_routes.py:1012-1028`) is otherwise careful — `required_scope="runs:write"` at
`:1012`, a typed `409 not_cancellable` at `:1019-1024` — which is exactly what makes the success path
read as trustworthy.

Two further defects found while writing this spec, neither in the issue:

**1a. The `409` guard is duplicated, and the duplicate turns an ordinary race into a `500`.**
`api_v1_routes.py:1019` checks `run.status not in (PENDING, RUNNING)` and returns `409`; then
`cancel_run` (`execution_service.py:122`) checks *the same condition* and returns `None`; the route
maps `None` to **`_error(500, "Failed to cancel run")`** at `:1026-1027`. So the only way to reach
that `500` is for the run to finish **between the two checks** — a normal race on a busy worker,
reported to the caller as a server fault. It should be the same `409`.

**1b. `finished_at` is set at request time.** `cancel_run:125` stamps `finished_at = now()` while the
worker is still running. Any duration derived from that field is wrong for exactly the runs a user
cares most about.

---

## 2. Product decisions

These are the contract. If implementation finds one of them unbuildable, change the spec and say so —
do not resolve it silently in code.

### D1 — Cancellation is best-effort, and the UI must never claim more than happened

A running extract cannot always be stopped instantly. **Do not promise instant.** The user-visible
contract is: *the run stops as soon as it safely can, and stops being billed from that point.*

A status that reads `cancelled` while the warehouse is still being written is the same lie this issue
is about, moved one layer up. So the request and the outcome are **different states** — see §3.

### D2 — Usage already consumed is billed; nothing further accrues

Under V2 bytes metering a cancelled run does **not** zero its usage. We really did read those bytes
and the destination really did the work. Recording zero creates a cancel-to-avoid-billing hole on the
one control we hand the user for free. **Meter what was processed up to the stop; meter nothing
after.** If partial usage cannot be measured at the point of stopping, say so in the PR — that changes
the answer, and I would rather decide it than have it default.

### D3 — Partially loaded data stays where it is, and we say so plainly *(new)*

A run stopped mid-load has already written rows to the destination. We do **not** attempt to roll
them back: dlt's load is not transactional across a whole run, and compensating on the destination
side is a feature we do not have and should not fake. So the contract is:

> **Cancelling stops further loading. Data already written to your destination stays there.**
> **Re-running the pipeline reloads from the source according to the pipeline's write disposition —
> `replace` overwrites it, `append` will duplicate the partial rows, `merge` reconciles on the primary
> key.**

That sentence is the deliverable, not a paraphrase of it. It goes in three places, worded the same:
the API reference, `/docs` on the landing site, and the confirmation dialog (short form). `append` is
called out by name because it is the one disposition where cancelling costs the user something they
must then clean up, and hiding that is how a stop button becomes a support ticket.

`rows_loaded` must carry the **partial** count on a cancelled run — it is the only honest answer to
"what did I get?", and it is already the field the UI shows.

### D4 — No new column; `updated_at` is the cancellation clock *(new)*

The reaper in §3.1 needs to know how long a run has been trying to stop. `TimestampMixin` already
updates `updated_at` on the status write, which answers it. **Do not add `cancel_requested_at`.**
Under blue/green, a migration that the previously-deployed code has never seen is a cost we are not
obliged to pay here — and `docs/specs/SPEC_EXPAND_CONTRACT_MIGRATIONS.md` would make it a two-release
change for a timestamp we can already read.

### D5 — Cancel is gated at `editor`, because that is the role that may start a run *(new)*

`AuthState.can_edit` (`ui/state/auth_state.py:201-209`) documents the existing gate:
*"create/edit/**run**/toggle"* → `_check_role("editor")`. **Stopping a run is not a higher privilege
than starting one.** So: `editor` and above may cancel; `viewer` is refused. The REST surface already
requires `runs:write` and is correct as it stands.

---

## 3. The state machine

One value is added to `RunStatus` (`models/run.py:11-16`):

```
CANCELLING = "cancelling"      # non-terminal: the stop was requested, the worker has not confirmed
```

| from | event | to |
|---|---|---|
| `pending` | cancel requested | **`cancelled`** — terminal immediately; no worker has started |
| `running` | cancel requested | **`cancelling`** |
| `cancelling` | worker reaches a checkpoint and exits | **`cancelled`** (terminal) |
| `cancelling` | worker finishes its work before noticing | **`cancelled`** — see below |
| `cancelling` | worker dies / never acknowledges | **`cancelled`** by the reaper (§3.1) |
| `cancelling` | cancel requested again | **`cancelling`** — idempotent, still `200` |
| `success` / `failed` / `cancelled` | cancel requested | **no change**, `409 not_cancellable` |

**A run that was asked to stop ends `cancelled`, even if the work happened to complete first.** This
is the crux of the fix: `complete_run` and `fail_run` must **refuse to overwrite a non-`RUNNING`
status**. The user asked us to stop; reporting `success` because we lost the race tells them their
cancel did nothing, which is today's bug with better timing. The rows that landed are still described
honestly by `rows_loaded`.

⚠️ **`length=20` on the column** (`models/run.py:28`) — `"cancelling"` is 10 characters and fits.
Stated because it is the kind of thing that is discovered in production.

### 3.1 A `cancelling` run must be reaped

A worker that dies mid-cancel leaves a run non-terminal **forever**. That is worse than the current
bug: `wait_for_run` polls until terminal, so every `?wait=true` client on that run burns its full
timeout, and the run never leaves the active set. The hourly maintenance sweep — which since
[core#653] genuinely runs in production for the first time — moves a run that has been `cancelling`
beyond a bounded window (**recommend 15 minutes**; Engineering may argue the number, not the
existence) to `cancelled`, and records in `logs` that the worker never acknowledged.

---

## 4. 🚨 The load-bearing section: seven lists say what a run status is

Adding a value to `RunStatus` is **not** a one-line change. Seven places enumerate these statuses by
hand, and nothing links them. This is the same defect shape as [core#651] (two secret-key lists),
[core#654], [core#659] and [core#638] — and the reason [core#651]'s accepted fix was *deriving one
list from the other* rather than correcting it.

| # | Location | What it enumerates | What a missing `cancelling` does |
|---|---|---|---|
| 1 | `services/run_waiter.py` `_TERMINAL` | terminal set | ✅ correct if left alone — `cancelling` is genuinely non-terminal |
| 2 | `services/api_v1_routes.py:578` | `("pending", "running")` — *string literals* | 🔴 **a cancelling run at timeout returns `422`, not `408`** |
| 3 | `services/api_v1_routes.py:1019` | cancellable states | 🔴 a second cancel returns `409` instead of being idempotent |
| 4 | `services/execution_service.py:122` | cancellable states — duplicate of #3 | 🔴 same, plus the `500` in §1a |
| 5 | `services/maintenance_service.py:40` | runs whose dlt dir is protected | 🔴 **deletes the working directory of a still-running worker** |
| 6 | `services/openapi.py:50` `_RUN_STATUSES` | the **published** API schema | 🟠 clients validate against a schema missing a value we return |
| 7 | `ui/pages/runs.py:18` | the status filter dropdown | 🟠 cancelling runs are unfindable by filter |

**Two of these are silent and produce a confident wrong answer**, which is why they are the reason
this spec exists:

- **#2 is the dangerous one.** `api_v1_routes.py:578` tests `status.value in ("pending", "running")`
  to mean *still going*. A `cancelling` run fails that test, falls through to
  `if status.value != SUCCESS` and returns **`422` — "terminal, not success"** — for a run that is
  still working. The docstring three dozen lines above it, at `api_v1_routes.py:554`, warns about
  precisely this class in the *terminal* direction: *"`RunStatus` already has `cancelled`, and a
  terminal status added later must not silently rejoin the 200 branch."* The **non-terminal**
  direction is the untested half of the same trap, and it is the one this change walks into.

- **#5 is a data hazard, and it is newly live.** `cleanup_orphaned_dlt_dirs` protects the working
  directories of runs that are `RUNNING` or `PENDING`. A `cancelling` run is in neither set, so its
  dlt directory becomes eligible for deletion **while the worker is still writing to it**. This was
  harmless for the project's whole history because `run_maintenance` had no beat process and had
  never once executed; [core#653] shipped `datanika-beat` to production on 2026-08-30, so the sweep
  now runs hourly. **A latent list defect became a live one nine days before this spec.**

### The requirement

**Define the sets once and derive every consumer from them.** Concretely: `NON_TERMINAL` and
`TERMINAL` (and `CANCELLABLE`) as module-level frozensets on `RunStatus` or beside it in
`models/run.py`, with #1–#7 reading them. `openapi.py:50` and `runs.py:18` derive from
`list(RunStatus)`.

**The guard that matters is a test that fails when a status exists in the enum and in none of the
sets** — not a test asserting the current membership, which is an eighth hand-maintained list and
would have passed on every one of the seven defects above.

⚠️ **Verify that test goes red before it goes green**, by adding a throwaway eighth status and
confirming the failure names it. A membership assertion that can only be satisfied by editing it is
this project's signature defect, and §4 is a list of what it costs.

---

## 5. Surfaces

### 5.1 Storage — no migration

`runs.status` is `sa.Enum(..., native_enum=False, length=20)`
(`migrations/versions/a1b2c3d4e5f6_add_all_tables_to_public.py:231-244`, `name="runstatus"` at
`:239`). SQLAlchemy's `Enum.create_constraint` defaults to **`False`**, so **no CHECK constraint is
emitted and no PostgreSQL enum type exists** — the column is a plain `VARCHAR(20)`. Verified by
compiling the DDL against the postgres dialect on the pinned SQLAlchemy 2.0.46:

```
CREATE TABLE probe (
        status VARCHAR(20) NOT NULL
)
```

**Adding `cancelling` therefore requires no migration at all**, and the expand/contract policy is not
engaged. ⚠️ Confirm once against the real database (`\d+ runs`) before relying on it — the claim is
derived from the migration source and the library default, and production is the only authority on
what the column actually is.

### 5.2 REST API

- `POST /api/v1/runs/{id}/cancel` returns `200` with the serialized run, whose `status` is
  `cancelling` **or** `cancelled` — never `cancelled` while work continues.
- The `409 not_cancellable` shape is kept for terminal runs. **Cancelling an already-`cancelling` run
  is `200`, not `409`** — it is the same request arriving twice.
- The `500` at `:1026-1027` becomes the same `409` (§1a).
- The response body carries the D3 sentence about partial data, or a documented link to it.
- `openapi.py` publishes the new value.

### 5.3 UI — `/runs`

`ui/pages/runs.py` can already filter by `cancelled` (`:18`) and has no control that produces one.
**A user with a runaway run currently has no way to stop it without an API key.** That is the reason
this is a launch blocker rather than an API tidy-up, and the button ships in the same PR.

- A **Cancel** control on rows whose status is `pending` or `running`, rendered under
  `rx.cond(AuthState.can_edit, …)` — reuse the existing var, do not invent a second one.
- 🚨 **The affordance and the enforcement are separate requirements and both are mandatory.**
  Hiding the control is not authorization; the handler gates with `_check_role("editor")`
  independently. Rendering it for a `viewer` who will then be refused is the exact defect [core#681]
  and [core#658] R6 are open about, on this same page.
- Confirmation dialog, **scoped to the dialog** — `getByRole('dialog')`. Copy: name the run's target,
  carry the short form of D3, and label the buttons *Stop this run* / *Keep running*. Never a bare
  "Are you sure?".
- While `cancelling`: the badge reads **Stopping…**, and the control is disabled rather than removed —
  a control that vanishes reads as a failed click.
- `_status_color` (`runs.py:37-46`) needs a branch for `cancelling`; it currently falls through to
  `gray`, which is the same colour as `pending` and says the wrong thing.
- **i18n ×9** for the button, the dialog title and body, the confirm/cancel labels and *Stopping…*.
  Status enum values rendered raw in the badge (`runs.py:70`) stay untranslated, consistent with the
  standing rule.

### 5.4 Docs

A `/docs` page section on stopping a run, carrying D3 verbatim. Cross-linked from the UI per the
standing documentation rule.

---

## 6. Out of scope, deliberately

- **Revoking the Celery task** (`app.control.revoke(terminate=True)`). Killing a worker mid-`dlt`-load
  leaves partial state with no checkpoint and no record of where it stopped. Cooperative
  cancellation — the run row is already the shared state — is the honest shape. If Engineering
  concludes revocation is needed *in addition*, it needs its own answer for "what state is the
  destination in afterwards", and D3 is that answer's contract.
- **Rolling back partially loaded data.** See D3.
- **Cancelling a schedule** — stopping a run is not pausing its schedule. A user who cancels a nightly
  run gets another one tomorrow, and that is correct. Say so in the docs.
- **Automatic cancellation on quota exhaustion.** Related, differently owned, not this.

---

## 7. Acceptance criteria

Supersedes the criteria in the [core#657] comment of 2026-08-30 where they differ; 1–7 there are
carried forward and renumbered here.

1. **A run cancelled *mid-flight* ends terminal-`CANCELLED`.** Cancelling a `pending` run proves
   nothing — the bug is that `complete_run` overwrites the cancellation, so the test must cancel a
   task that is **already executing**, let the worker run to its natural end, and then assert the
   final row. *If that test is hard to write, that difficulty is the bug.*
2. **The worker actually stops.** Assert on an effect the run *would* have had and did not — the
   destination table stops growing, or the cancellation checkpoint is reached. **Not** on the API
   response: a `200` is precisely what we get today while nothing happens.
3. **Negative controls: an ordinary run still ends `SUCCESS`, and a failing one still ends `FAILED`.**
   The fix touches the completion path every run takes.
4. **The status-set guard of §4 exists and was demonstrated red** against a throwaway eighth status.
5. **Each of the seven consumers in §4 reads a derived set.** Specifically provable: a `cancelling`
   run at `?wait=true` timeout returns **`408`**, and `cleanup_orphaned_dlt_dirs` does **not** remove
   its directory.
6. **Idempotent and safe.** Cancel twice → `200`, no error. Cancel a `success` run → `409`, status
   unchanged. The `500` path of §1a is gone.
7. **Authorization, behaviourally.** A `viewer` is refused at the handler — assert the runtime denial,
   not the source. An API key without `runs:write` is refused. Cross-org cancel returns `404`.
8. **A stuck `cancelling` run is reaped** (§3.1) and the reaper's window is asserted.
9. **The UI control ships in the same PR**, with §5.3's affordance/enforcement pair and i18n ×9.
10. **D3's sentence appears in the API reference, the docs page and the dialog**, worded the same.
11. **Metering (D2), if the cloud plugin is loaded**: a cancelled run records its partial usage and no
    more.

---

## 8. Traps carried from the audit

- **`rows_loaded` on a cancelled run must be the partial count, not `0` and not `NULL`.** `0` reads as
  "nothing happened", which is the one thing we know is false.
- **Do not ship the button before the mechanism.** Shipping a Cancel control onto today's behaviour
  spreads the lie from API callers to every user. §7.9 sequences it *within* the PR, not before it.
- **`finished_at` means finished** (§1b) — set it when the run reaches a terminal status, not when the
  request arrives.
- **A test that asserts `notify`/`cancel` was *called* is not a test that it worked.** The defect
  class here is a function that returns cleanly having done nothing.

---

## References

- [core#657] — the issue.
- [`SPEC_ORG_ROLES.md`](SPEC_ORG_ROLES.md) §4 — the role model D5 is consistent with, and the
  rule-above-the-thing-it-protects defect §5.3 repeats.
- [`SPEC_EXPAND_CONTRACT_MIGRATIONS.md`](SPEC_EXPAND_CONTRACT_MIGRATIONS.md) — why D4 avoids a column.
- [`SPEC_VOLUME_METERING.md`](https://github.com/datanika-io/datanika-cloud/blob/dev/docs/specs/SPEC_VOLUME_METERING.md)
  — D2's billing surface. In the private `datanika-cloud` repo, per the cross-repo convention in
  [`README.md`](README.md).

[core#638]: https://github.com/datanika-io/datanika-core/issues/638
[core#651]: https://github.com/datanika-io/datanika-core/issues/651
[core#653]: https://github.com/datanika-io/datanika-core/issues/653
[core#654]: https://github.com/datanika-io/datanika-core/issues/654
[core#657]: https://github.com/datanika-io/datanika-core/issues/657
[core#658]: https://github.com/datanika-io/datanika-core/issues/658
[core#659]: https://github.com/datanika-io/datanika-core/issues/659
[core#681]: https://github.com/datanika-io/datanika-core/issues/681
