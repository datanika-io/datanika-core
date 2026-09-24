# SPEC — The audit trail: what a mutating handler owes the record

**Author:** Product · **Status:** implemented on `dev`, **not yet in production** · **Written:** 2026-09-06
**Amended:** 2026-09-07 — §4 (three clauses falsified by measurement), §4.4 (the mutation table
restated them), §6 (a branch-status ruling).
**Amended:** 2026-09-23 — §2 (its scope heading silently excluded the API surface) and new **§8**,
ruling what a mutating API route owes the record. §8's measurements are `origin/dev` @ `890e6c5`.
**Binds:** Engineering. **Source of truth for:** [core#934] and — via §8 — the audit half of
[core#657].
**Verified against:** `origin/dev` @ `e9e5b51` (fetched 2026-09-06), re-verified against
`origin/dev` @ `dc92f45` and `origin/master` @ `5726b8f` on 2026-09-07 for every production claim.

> ⚠️ **This spec decides two things and refuses three others.** It states the contract every audit
> writer is held to, then applies it to the one persisted mutating surface in the product that has
> never had one (§3), and — since 2026-09-23 — to the **API door**, which the contract's own scope
> heading had been excluding without saying so (§8). It does **not** decide [core#670] (whether to
> start collecting client IPs),
> [core#694] (giving `old_values`/`new_values` a reader), or [core#693] (index + `jsonb`). Two
> further defects were found while writing it; both are filed separately and are named in §6 so an
> implementer does not absorb them into this one.

---

## §1 — What the audit log is for, and the two ways it fails

The audit log answers exactly one question, asked after the fact: **did somebody do this, and who?**
It is reached when something is wrong and nobody remembers changing anything.

It therefore has two failure modes, and they are **symmetrical and indistinguishable from the
outside**:

| failure | what the reader sees | live instance |
|---|---|---|
| **A. The action leaves no record** | an empty table | [core#934] — this spec |
| **B. The record exists and the only instrument for reading it says it does not** | an empty table | §6.2, filed separately |

🔑 **Both produce the same screen, and that screen says "nobody did it."** An audit log that under-reports
is worse than an absent one, because an absent one is not consulted and a lying one is believed. Every
clause in §2 exists to close one of these two.

---

## §2 — The contract: five clauses, binding on every `_audit` call site

`BaseState._audit` → `AuditService.log_action` is the single chokepoint (`audit_service.py:141-166`).
These clauses are what a call site owes it.

> 🆕 **This heading names a scope, and for two weeks that scope quietly excluded half the product.**
> `_audit` is a method on `BaseState`, so *"every `_audit` call site"* means **every Reflex state
> class** — the UI door. A Starlette route has no `BaseState` and therefore no `_audit`, so a reader
> checking a route against this contract finds the contract does not reach it, and concludes
> correctly that nothing is owed. **§8 rules the API surface and is binding in the same way.** The
> five clauses below are unchanged and apply to both; what §8 adds is *which* routes owe a row.

### 2.1 · The audit row and the mutation are in **one transaction**

`_audit` takes the session as its first argument for this reason. The write is `session.add` +
`flush` — it becomes durable on the caller's `commit()` and disappears with the caller's rollback.

> **An audit write outside the mutation's transaction is a log of things that did not happen.**

There is no exception. A handler that opens a second session to "make sure the audit lands" has
built exactly the defect the log exists to rule out.

### 2.2 · The `action` string must be an `AuditAction` member

`_audit` does `AuditAction(action)` and **swallows the `ValueError`** (`base_state.py:212-246`) — by
design, because an audit failure must never break the operation it describes. The consequence is
that **a misspelled action is a silently dropped row**, visible only as a log line nobody is
watching. The six valid values are `create` · `update` · `delete` · `login` · `logout` · `run`
(`models/audit_log.py:12-18`).

⚠️ **This is not hypothetical — it is live in production.** See §6.1.

### 2.3 · The `resource_type` must be a value the reader can filter for

A row written under a type the filter does not offer is in the table and unreachable through the
only UI that reads the table.

🆕 **§6.2 is done, so the operative instruction has changed** ([core#1128], 2026-09-07). This
clause used to read *"`/audit-logs` filters against a **hardcoded list** (`pages/audit_logs.py:43-50`)
… adding a new type means adding it to the filter **in the same PR**, until §6.2 makes the list
derived."* The list **is** derived now, and leaving that sentence standing would have kept sending
implementers to hand-edit a list that no longer exists.

**What a new resource type costs today:** add the member to `AuditResourceType`
(`models/audit_log.py`) in the same PR as the writer. The filter picks it up with no second edit.
`tests/test_services/test_audit_call_site_vocabulary.py` fails if a writer and the enum disagree in
**either** direction — a written type nothing can filter for, or an option no call site writes.

⚠️ **The call sites still pass plain strings**, deliberately: the enum is what the *reader* derives
from, and the guard is what binds the writers to it. Binding 36 literals to enum members would be a
wider diff for the same guarantee, and would not catch the one thing neither shape catches — a typo
blessed into the enum alongside its writer.

### 2.4 · The payload is flat scalars under keys that are not PII keys

`redact_pii_payload` is live and is called inside `log_action`, so redaction is not the call site's
job — but **key naming is**. `PII_PAYLOAD_KEYS` is derived from the `*_pii` tables and is **nominal**:
it matches key *names*, so `{"email": …}` is redacted and `{"contact": <an address>}` is not
(`audit_service.py:26-63`). Two consequences for a new writer:

- Never put personal data under a non-PII key name. The redactor cannot see it.
- Prefer **prefixed, specific key names** (`upstream_name`, not `name`). A bare `name` becomes a
  redaction target the day any `*_pii` table gains a column called `name`, and the payload would
  start writing `[REDACTED]` in a table nothing reads, so nothing would contradict it.

Nesting deeper than 20 levels or containing a cycle raises inside the redactor
(`audit_service.py:74-90`) — and per 2.2 that raise is swallowed. Keep payloads flat.

### 2.5 · The payload must identify the thing to a human, not just to the database

`resource_id=12` identifies a row to Postgres and nothing to the person reading the table after an
incident. The payload carries the **names as the actor saw them**, alongside the ids.

⚠️ **State the honest property: a name is a label at a point in time; the id is what stays
resolvable.** Both belong in the row, and neither substitutes for the other.

🔑 **Our own codebase already made this argument, for the dialog on the very handler this spec is
about.** `_remove_dependency_dialog`'s docstring (`pages/dag.py:241-249`):

> *"So the dialog names both ends of the edge, since `#12` identifies an edge to nobody."*

[core#851] accepted that for the **question** and left the **record** with nothing but the id. §3
finishes the job — the same sentence, applied to the row instead of the prompt.

---

## §3 — [core#934]: `DagState` has no audit call of any kind

### 3.1 · Why this surface and not another

`dag_state.py` contains **0** occurrences of `_audit` while both of its handlers commit
(`add_dependency:278`, `remove_dependency:331`). Every other mutating state class in the product
audits — ten of them, 33 call sites (AST census, 2026-09-06).

The dependency graph is the one object in the product whose corruption is **silent by design**, and
`pages/dag.py`'s own dialog docstring says so:

> *"nothing breaks, nothing errors, and no row disappears from any other page. The downstream job
> simply stops waiting for the upstream one and starts running against whatever data happens to be
> there — a silently wrong result rather than a failure."*

So the failure mode is *"why is this model wrong?"* asked days later, and the audit log is precisely
the instrument you reach for to ask *"did somebody change the graph?"* It has never had an answer.

⚠️ **Not a security finding.** `remove_dependency` has required `admin` since [core#851] and
`add_dependency` `editor`. Nothing here is unauthorised; this is a missing record of authorised
actions.

### 3.2 · AC1 — `add_dependency` writes a `create` row

`DependencyService.add_dependency` already returns the flushed `Dependency`
(`dependency_service.py:105-107`), so its `id` is available inside the transaction. The handler
currently discards the return value.

| field | value |
|---|---|
| `action` | `"create"` |
| `resource_type` | `"dependency"` |
| `resource_id` | the returned `Dependency.id` |
| `new_values` | `upstream_type`, `upstream_id`, `upstream_name`, `downstream_type`, `downstream_id`, `downstream_name`, and — when the form supplied one — `check_timeframe_value`, `check_timeframe_unit` |

`old_values` is `None`: nothing existed before.

### 3.3 · AC2 — `remove_dependency` writes a `delete` row, **and only if a row was removed**

| field | value |
|---|---|
| `action` | `"delete"` |
| `resource_type` | `"dependency"` |
| `resource_id` | the `dep_id` argument |
| `old_values` | the same six keys as AC1, plus the timeframe pair if the removed row carried one |
| `new_values` | `None` |

🚨 **The condition is load-bearing and it is not a detail.** `DependencyService.remove_dependency`
returns `False` when the row does not exist, is already soft-deleted, or belongs to another org
(`dependency_service.py:109-115`). **The handler discards that return today** and yields
*"Dependency removed"* unconditionally.

**So the toast and the audit row must move together, and this is one AC, not two.** Writing the
audit row conditionally while leaving the toast unconditional produces the worse state of the two:
the user is told the edge was removed and the record says it was not. Either both fire or neither
does.

- `True` → audit row + success toast, as today.
- `False` → no audit row, and the user is told the dependency was **not** removed (see AC5 on
  strings).

### 3.4 · AC3 — where the names come from

Ids and node types come from the **persisted row** — `svc.get_dependency(session, org_id, dep_id)`
before the removal for AC2, and the returned object for AC1. That is the authoritative half.

The two names are resolved from the handler's already-loaded state — `self.dependencies`, whose
`DependencyItem` carries `upstream_name` / `downstream_name` (`dag_state.py:16-26`), or
`self._name_to_id` reversed. **No new queries.** Per §2.5 they are recorded as what the actor saw;
if a node has since been renamed, the ids still resolve and the name is the historical label, which
is what an audit row should hold.

If a name cannot be resolved, write the key with an empty string rather than omitting it. A key
that is sometimes absent makes every future reader write a `.get()`, and [core#694]'s point is that
there are no readers yet — this is the cheapest moment to fix the shape.

### 3.5 · AC4 — the `dependency` type must be filterable

Add `"dependency"` to the resource-type filter list in `pages/audit_logs.py`. Per §2.3, a row
written under an unfilterable type is unreachable through the only screen that reads the table.

⚠️ **Do not fix the filter list's other defects here.** They are §6.2 and they have their own issue.
This PR adds one string.

### 3.6 · AC5 — i18n

**No new locale keys are required for the audit rows themselves.** `/audit-logs` renders
`log.resource_type` and `log.action` as raw strings (`pages/audit_logs.py:16-17`), and the filter
`searchable_select` takes raw option values. Nothing here is translated today, so `"dependency"`
adds no locale work. **Do not add nine keys for it** — that would make this one type inconsistent
with the eleven beside it.

AC2's failure message **is** user-visible and therefore **does** need all nine locales. Reuse the
existing failure idiom rather than inventing a new tone; `dag.` is the key namespace
(`dag.created_toast`, `dag.deleted_toast` already exist).

---

## §4 — Tests: what each one kills, and what it cannot

**Four tests.** They are not redundant — each closes a failure the others are satisfied by, and the
spec names which, because a test suite whose members overlap is one test with four names.

> 🔴 **THIS SECTION WAS WRONG IN THREE OF ITS CLAUSES, AND TWO OF THEM WENT RED ON CORRECT CODE.**
> Written 2026-09-06 by reasoning about the harness; falsified 2026-09-06/07 by Engineering
> *running* it, on [core#1127] and [core#934]. The corrections are inline below, each beside the
> clause it falsifies.
>
> 🚨 **Read the direction of the error, not just the fact of it.** A test that reds on *broken* code
> and a test that reds on *correct* code are not two grades of the same mistake. The second one
> tells an implementer their working implementation is wrong, and the cheapest way to make it pass
> is to change the implementation — so §4.2's clause, written to *prevent* a second-session audit,
> could have produced one. **A spec clause that cannot pass against a correct implementation is a
> defect in the spec with the failure signature of a defect in the code.**
>
> 🔑 **What generalises to every acceptance criterion I write, and the reason this warning is at the
> top of the section rather than in a footnote:** all three wrong clauses share one shape — they
> assert on **an intermediate state of the machinery** (`session.new`, `session.dirty`, "a row
> exists after `commit()`") rather than on **the property the user is owed** ("the record and the
> mutation stand or fall together"). Machinery states are the ones I cannot check by reading, and
> they are the ones a harness quietly changes underneath a spec. **Prefer the invariant; make the
> implementer choose the assertion that detects it, and require them to show the red.**
>
> This is `QA_RULES` §29 and [core#864] arriving in a Product artifact: *an AC that fails on correct
> code is worse than one that passes on broken code.* Recorded here rather than only on the issue,
> because the next person to read this section is the next person at risk of it.

### 4.1 · T1 — the happy path

After a successful `remove_dependency`, `audit_logs` holds exactly **one** matching row with the
exact shape in AC2. Same for AC1.

**Kills:** the audit call being absent.

**Cannot see:** an audit row written in its own session — that row exists too, so T1 is green.

> 🚨 **CORRECTED 2026-09-07, [core#934] — T1 does NOT kill the after-`commit()` placement**, which
> this clause claimed. Measured: really moving the `_audit` call below `session.commit()` in
> `add_dependency` left **all nine** other tests in the file green.
>
> The reason is the harness, and it applies to every audit harness in this repo. The shared
> `db_session` fixture owns the real transaction and rolls it back at teardown, so a handler's
> `commit()` has to be stubbed to a `flush()` — otherwise it releases the savepoint and rows leak
> into the next test. Under that stub a row added *after* `commit()` is still flushed into the same
> transaction, and any later query finds it. In **production** the same code writes nothing: the
> `with` block exits with no second commit and the row is discarded.
>
> **What does kill it** is a watermark on the stubbed `commit()` — how many audit rows were already
> in the transaction at the moment it was called (expect 1, get 0).
>
> ⚠️ **Count ROWS, not objects.** The first version of that watermark asked whether an `AuditLog`
> was in `session.identity_map` or `session.new`, and it read **False on correct code**:
> `identity_map` is a *weak* dict, `log_action` flushes the row and returns it, and every caller
> discards the return — so the object is collected while its row sits in the transaction. Isolated
> probe: `identity_map` size **0**, `session.new` empty, `SELECT count(*)` = **1**. *Presence of the
> object is not presence of the row.*

### 4.2 · T2 — one transaction, asserted structurally

Wrap the handler's session factory and assert that the audit row and the mutation rode the **same**
`Session`.

> 🚨 **CORRECTED 2026-09-07, [core#1127] — the original wording could not pass against correct
> code.** It said: *"At the moment the handler calls `commit()`, assert that the same `Session`
> holds both the pending `AuditLog` (in `session.new`) and the mutated `Dependency` (in
> `session.dirty`)."* The `AuditLog` **cannot** be in `session.new` at that moment:
> `AuditService.log_action` ends in `session.add(log)` followed by `session.flush()`
> (`audit_service.py:178-179`), so the row is already persistent before `commit()` runs. Nor is the
> mutation still in `session.dirty` — the service flushed it earlier. Measured while implementing
> [core#1127]: the first draft of this test failed on a **correct** implementation, reporting
> `samples=[(False, True)]`.
>
> ⚠️ **That is the dangerous kind of wrong.** A red on correct code invites the implementer to
> change the *implementation* until the test passes — so a clause written to prevent a
> second-session audit could have produced one.
>
> **What discriminates instead is session identity**, and it is cheaper: count the handler's entries
> into `get_sync_session` (exactly one) and assert the `AuditLog` was flushed on the session that
> single entry handed out. Verified to kill the mutant it is named for — see the table below.

**Kills:** the second-session implementation. **Measured, not predicted** ([core#1127] M3): against
a handler that audits in its own `get_sync_session()` block, both row-existence assertions stayed
**green** and only this one went red.

🔑 **Why this and not "assert the row exists afterwards": a row-exists assertion is satisfied by the
bug it is supposed to name.** This is the [core#1081] lesson restated — *the property is that both
changes ride one transaction, not that a row is present when the dust settles.* A redirect-only
assertion passes against code that redirects after clobbering; a row-exists assertion passes
against code that audits in a transaction of its own.

### 4.3 · T3 — the rollback

Patch `commit` on the **first** session the handler is handed so that it raises. Assert **zero**
audit rows *and* that the dependency is still live.

**Kills:** the mutation and the record coming apart under failure — a handler that leaves the edge
removed while the audit row is gone, or the reverse.

⚠️ **"The first session" is the precision that makes T3 work.** Patching `Session.commit`
class-wide breaks the second session too, and the mutant then passes.

> 🚨 **CORRECTED 2026-09-07, [core#934] — T3 does NOT kill the second-session implementation "from
> the other side", which this clause claimed.** The claim assumed the second session commits
> independently, so a rolled-back outer transaction would leave its audit row behind and T3 would
> find one. **In this harness it cannot**: `tests/conftest.py` gives the suite a single SQLite
> connection, so a "second" `get_sync_session()` inside a test *is* the same transaction and rolls
> back with it. T3 finds zero rows and stays green against the mutant.
>
> **T2 is the only thing that kills that mutant** — measured twice, on [core#1127] M3 and again on
> [core#934]. T3 remains worth having for the property restated above; it is simply not a second
> line of defence against the one failure §4.2 covers.
>
> 🔑 **The general form, which is why this is written down rather than quietly fixed:** *two tests
> that appear to attack a defect from opposite sides may both be reading the same instrument.* The
> redundancy was an illusion produced by the fixture, not by the tests — and a spec that promises
> two independent kills where one exists invites deleting "the redundant one", which here would have
> deleted the only one that works. **Before claiming two tests are independent, ask what shared
> fixture they both sit on.**

Per `PRODUCT_RULES` §15b and the fifteen controls in
`tests/test_ui/test_delete_confirmation_and_blocked_uploads.py`: **apply the mutation to the real
file, run the named test, and check it fails *for the stated reason*.** A red for an unrelated
reason is not a control.

> 🔴 **THIS TABLE WAS WRONG IN TWO OF ITS FOUR ROWS, and it stayed wrong for a day after the
> clauses above it were corrected.** The prose in §4.1 and §4.3 was fixed; **the table that an
> implementer actually runs was not.** Corrected 2026-09-07 against Engineering's measurements on
> [core#934].
>
> 🔑 **That gap is the lesson, not the two rows.** A correction applied to the *explanation* and not
> to the *checklist* leaves the artifact people execute still carrying the falsified claim — and it
> reads as more authoritative afterwards, because the section around it now looks freshly reviewed.
> **When a clause is corrected, grep the spec for every other place that clause is restated**, and
> fix the summary in the same edit. Both wrong rows here named a test that *cannot see* the
> mutation, so an implementer applying the mutation would watch the named test stay green and
> reasonably conclude their own implementation, not the table, was at fault.

| mutation on the real handler | must go red | ⚠️ notes |
|---|---|---|
| delete the `_audit` call | T1 | |
| move the `_audit` call into its own `get_sync_session()` block | **T2 only** | 🔴 was *"T2 and T3"*. T3 cannot see it — §4.3: this harness runs one SQLite connection, so the "second" session is the same transaction and rolls back with it. Measured twice, [core#1127] M3 and [core#934]. |
| move the `_audit` call below `session.commit()` | **T4** (the commit watermark) | 🔴 was *"T1"*. T1 cannot see it — §4.1: the fixture stubs `commit()` to `flush()`, so a row added after it is still found by any later query. Measured: the mutation left **all nine** other tests green. |
| drop the `if` on the service's return in AC2 | T1's delete case, seeded with a `dep_id` that does not exist | |

**T4 — the commit watermark.** Record how many `audit_logs` rows are already in the transaction at
the moment the stubbed `commit()` is called; expect 1, a below-`commit()` placement gives 0. It is
the only assertion that separates "the row is in the transaction the handler committed" from "the
row is in the transaction the *fixture* is holding open", and §4.1 has the full derivation including
why it must count **rows, not objects**.

---

## §5 — The census guard will go red, and that is the ratchet working

`tests/test_ui/test_delete_confirmation_and_blocked_uploads.py` carries a declared disagreement:

```python
"remove_dependency": (
    "verb-only, and this one is a defect rather than a design: it persists "
    "and writes no audit row at all. Filed as core#934 — the disagreement "
    "is what surfaced it, which is the argument for keeping both lists."
),
```

`test_each_declared_disagreement_still_disagrees` asserts that entry is *still true*. The moment
AC2 lands, the two census derivations agree about `remove_dependency` and **that test fails, by
design and with the right message.**

🚨 **Delete the `CENSUS_DISAGREEMENT["remove_dependency"]` entry in the same PR.** Do not silence
the test, do not add an exemption. A stale declaration is a hole with a reassuring comment over it —
the file says so itself about its sibling exclusions.

⚠️ **`add_dependency` disturbs nothing.** It is not in `DESTRUCTIVE_PREFIXES` (`add_` is not a
destructive verb) and writes `create`, not `delete`, so it never enters `AUDITED_DELETE`. Only AC2
moves the census. Do not go looking for a second entry to delete.

---

## §6 — Two defects found while writing this. **Neither is in scope here.**

Both were found by the §2 clauses, which is the argument for writing the contract down rather than
patching the one site that prompted it. Both are filed — **[core#1127]** and **[core#1128]** — and are named here so an
implementer of [core#934] does not silently absorb them, and so the next person to read this spec
does not re-derive them.

> ### 📌 Status, ruled 2026-09-07 (Product): **fixed on `dev`, still LIVE IN PRODUCTION. §6 is not
> resolved and must not be marked so.**
>
> Engineering asked whether §6 could be retired now that [PR #1144] has merged. **No — not yet**,
> and the reason is the same rule that governs issue closure: a fix on `dev` is not a fix a user
> has. Measured against `origin/master` (production) on 2026-09-07, not inferred from the PR:
>
> | | `origin/master` (live) | `origin/dev` |
> |---|---|---|
> | `models/audit_log.py` | **36 lines, no `AuditResourceType`** | 94 lines, `AuditResourceType` present |
> | `ui/state/dag_state.py` `_audit` calls | **0** | 2 |
> | `settings_state.py` ownership action | **the non-member string** | corrected, with the reason in a comment |
>
> **So every sentence in §6.1 and §6.2 is true of the product as shipped**, including the two that
> read worst: the highest-privilege action in the product still writes no audit row, and an admin
> asking *"who removed this person?"* still picks `membership` and still gets an empty table.
>
> **Retire §6 when the promotion that carries [PR #1144] and [PR #1146] verifies on `master`** —
> then rewrite both clauses in the past tense with the promotion SHA, rather than deleting them.
> §1 is the reason to keep the text at all: §6.2 *is* the worked example of failure mode B, and a
> spec that deletes its own worked example keeps the taxonomy and loses the evidence for it.
>
> ⚠️ **The §2.3 clause above is correct as written and is NOT in tension with this.** §2.3 binds an
> implementer, who works on `dev`, so it must describe `dev` — the filter list *is* derived there,
> and telling someone to hand-edit a list that no longer exists would be the worse error. §6
> describes what a user currently suffers. **Two sections of one spec may honestly describe two
> branches, provided each says which branch it means.** They did not, until this note; that
> ambiguity is what made "is §6 resolved?" a question with two defensible answers.

### 6.1 · [core#1127] — a `transfer_ownership` audit row has never been written (§2.2)

`SettingsState.transfer_ownership` (`settings_state.py:457`) passes `"transfer_ownership"` as the
action. It is not an `AuditAction` member, so `AuditAction(action)` raises and `_audit` swallows it.
**The single highest-privilege action in the product writes no audit row**, and every check is
green.

### 6.2 · [core#1128] — the resource-type filter and the writers disagree, in both directions (§2.3)

AST census over `datanika/`, 2026-09-06 — 13 `resource_type` values are written; the filter offers 7:

- **Written and not filterable (7):** `import`, `member`, `notification_channel`, `org`,
  `password`, `session`, `user`.
- **Filterable and never written (1):** `membership`.

🚨 **The two halves compose into the §1.B failure.** `member` carries **7** of the writes — every
membership change, invitation, role change and `leave_org`. An admin asking *"who removed this
person?"* picks the one filter option that looks right, `membership`, and gets an **empty table**.
The record is there; the only instrument reads zero.

The durable fix is to derive the list from the written set rather than hand-maintaining it — the
same correction `PII_PAYLOAD_KEYS` already made for the redactor, for the same reason.

---

## §7 — Out of scope for [core#934]

- **[core#670]** `audit_logs.ip_address` — never written; `client_ip.py` has no caller. A decision
  about *collecting* client IPs, with its own privacy surface. This spec adds no `ip_address`.
- **[core#694]** nothing reads `old_values` / `new_values` — 30 writers, 0 readers. §2.4 and §3.4
  shape the payload for a reader that does not exist yet, which is the cheapest time to shape it.
- **[core#693]** index `user_id`, convert the payload columns to `jsonb`. Purely a storage change;
  the payloads specified here are valid under both.
- **[core#655] / SPEC_PII_SEPARATION D11** — the erasure surface reaching `audit_logs`.
  ⚠️ **The N+1 question in [core#934]'s original AC2 is already answered and the answer is
  "nothing to do":** `redact_pii_payload` and the derived `PII_PAYLOAD_KEYS` are **live on
  `origin/dev` and inside `log_action`**, contradicting `SPEC_PII_SEPARATION`'s header note that
  they have "0 occurrences anywhere in `datanika/`" (that note is dated 2026-09-02 and is stale).
  A new writer inherits redaction at the chokepoint. The only obligation left on the call site is
  §2.4's key naming.
- **The audit page's own gaps** — no `old_values`/`new_values` column, no actor name, no date range.
  [core#694] and [core#735].

---

## §8 — The API surface: what a mutating route owes the record

**Ruled 2026-09-23 (Product)**, on Engineering's question *"does `POST /api/v1/runs/{id}/cancel`
write an audit row?"* — asked because starting with that one route is a decision about the API
surface rather than a bug fix. It is. Here is the decision.

**Measured against `origin/dev` @ `890e6c5`, 2026-09-23.** Every line and number below was read this
session, not carried from §1–§7's 2026-09-07 verification.

### 8.1 · The question is not whether to start recording. We already record this action.

| door | handler | audit row |
|---|---|---|
| **UI** | `ui/state/run_state.py:255` | **yes** — `update` / `run`, `old_values`/`new_values` carrying the status pair, conditional on a real transition, inside the mutation's own transaction |
| **API** | `services/api_v1_routes.py:1149` (`cancel_run`) | **none** |

Both doors call the same `ExecutionService.cancel_run`. So the same action, on the same run, in the
same org, by the same person, is recorded or not **according to which door it came through**.

🚨 **That is worse than recording neither door, and §1 says why in its own words:** *"an absent one
is not consulted and a lying one is believed."* An empty `audit_logs` prompts the question *"do we
even log this?"* A table that holds **some** cancels does not — so an admin asking *"who stopped
run 42?"* about an API cancel gets a well-formed, confident **silence**, and reads it as
*"nobody did."* This is failure mode A wearing failure mode B's clothes: the record under-reports,
and the instrument that reads it looks healthy because it is healthy.

🔑 **So this is a consistency decision, not a collection decision.** The asymmetry is the defect,
and it would be a defect in whichever direction it pointed.

### 8.2 · The decision

**`POST /api/v1/runs/{id}/cancel` writes an audit row.** The route writes it — **not** the service.

**The invariant, binding beyond this one route:**

> **A mutating API route owes the record whatever its UI twin already writes for the same action.**
> Where the two doors reach the same service, they must be **indistinguishable in `audit_logs`**
> except for facts that genuinely differ (§8.6).

**Why the route and not the service**, since putting it in `ExecutionService.cancel_run` would cover
both doors in one edit and is the obviously cheaper diff:

1. **It would double-write.** The UI handler audits *and* the service would audit, so every UI
   cancel files two rows. An over-reporting audit log is the same class of defect as an
   under-reporting one — §1 — and it is the harder one to notice, because nothing is missing.
2. **Removing the UI handler's call to compensate relocates a discrimination that lives in the
   handler for a measured reason.** `run_state.py:233-236` reads the status **before** the mutation
   and says why: *"`cancel_run` returns the run already changed and flushes, which clears the
   attribute history — so afterwards nothing can say what the stop actually did."* The service
   cannot see its own before-state at the point it would write. Moving the write there is a real
   refactor of a production path, and it is not what was asked.
3. **The existing design already puts the write at the caller that knows the actor.** `_audit` takes
   the session precisely so the handler — which knows *who* — writes the row. A route knows who; a
   service knows only an `actor_user_id` it was handed.

⚠️ **None of that is an architectural objection to services auditing** — see 8.3.

### 8.3 · Two premises in the question, both measured, one of them false

1. 🔴 **"The cheapest implementation would make a service an audit writer for the first time" —
   FALSE.** `services/user_service.py:1276` already calls `AuditService().log_action(...)` directly,
   passing the session, inside `erase_user`; it is deliberate, commented, and belongs to
   `SPEC_PII_SEPARATION` D11 / [core#655]. **A service is already an audit writer.** The precedent
   exists, so "this would be the first" is not among the reasons to hesitate. §8.2's reasons stand
   on their own and none of them is precedent.
2. ✅ **"No API route audits anything" — TRUE, and the grep that shows it has to be aimed
   correctly.** `datanika/api/` **does not exist**, so a count at that path returns `0` about
   nothing — the vacuous-zero shape `WORKFLOW_RULES` §4 and coordinator rule 26 both name. Aimed at
   the real file, with a live positive control in the same run:

   | | `_audit(` | `log_action` | `AuditService` |
   |---|---|---|---|
   | `services/api_v1_routes.py` — 1932 lines, **54 routes**, 55 `@api_endpoint` | 0 | 0 | 0 |
   | `ui/state/base_state.py` — **positive control** | — | **1** | **2** |

   The control is what makes the zeros a reading. Without it they are indistinguishable from three
   patterns that match nothing anywhere.

### 8.4 · The row

Mirror the UI handler exactly, so the two doors are one story in the table:

| field | value |
|---|---|
| `action` | `"update"` |
| `resource_type` | `"run"` |
| `resource_id` | the `run_id` path parameter |
| `old_values` | `{"status": <status before the call>}` |
| `new_values` | `{"status": <status after the call>, "api_key_id": <the acting key's id>}` |

- **`"update"`, not a new `"cancel"` member.** §2.2 binds: the action must be an `AuditAction`
  member, and a misspelling is a **silently dropped row**. `AuditAction` is
  `create · update · delete · login · logout · run`. 🚨 **Do not add `cancel` to the enum** — the
  same expand/contract hazard `AuditResourceType`'s own docstring spells out applies, and the UI
  door already writes `"update"` for this transition. A second vocabulary for one action splits the
  filter.
- **Same transaction as the mutation** (§2.1), on the session `@api_endpoint` already hands the
  handler. No second session.
- **Conditional on a real transition**, exactly as the UI door is: write the row only when the
  status actually changed. `CANCELLING` is itself in `CANCELLABLE_RUN_STATUSES`, so a second stop on
  an already-stopping run is idempotent, and an unconditional write files a
  `cancelling → cancelling` row asserting something that did not happen.

### 8.5 · 🚨 The trap that makes the row lie, and the route walks into it by default

`cancel_run` already holds `run` from its own pre-flight `get_run` (`api_v1_routes.py:1153`) and
receives `cancelled` from the service. **Under one session and one identity map these are very
likely the same object**, so reading `run.status` *after* the service call yields the **new** status
— and a row built that way records `old_values == new_values`: a transition that never happened,
filed under the name of the person who did something else.

**Capture the before-status into a local scalar before calling the service:**

```python
was = run.status                      # BEFORE. Not `run.status` read afterwards.
cancelled = _exec_svc.cancel_run(...)
```

This is not a new discovery — `run_state.py:233-236` documents it for the UI door and works around
it the same way. It is written here because **the route's existing shape makes the wrong version
look like the natural one**: `run` is already in scope, already fetched, and reads perfectly.

⚠️ **A test asserting only that a row exists is satisfied by this bug**, which is §4.2's lesson
arriving on a new surface. The assertion that discriminates is on the row's **contents**:
`old_values["status"] != new_values["status"]`.

### 8.6 · Which key acted: record the id, never the name

An API key acts *on behalf of* an absent user, which is the whole point of it — so the audit log's
question, *"did somebody do this, and who?"*, is **more** likely to be asked about an API cancel
than a UI one, not less. `user_id` alone answers *"Alice"* when the useful answer is *"Alice's CI
bot"*, and one user may own several keys.

- ✅ **Record `api_key_id`.** An integer the user chose nothing about; it resolves to the key, which
  resolves to its name and owner. §2.5's own formulation: *"the id is what stays resolvable."*
- ⛔ **Do not record the key's name.** It is user-chosen free text, so it can contain anything —
  including an email address. §2.4 is explicit that `redact_pii_payload` is **nominal**: it matches
  key *names*, so personal data arriving under `api_key_name` is invisible to it. A name in the
  payload would be exactly the *"never put personal data under a non-PII key name"* failure, made
  by the spec rather than by a careless call site.

**The UI door's row carries no `api_key_id`, deliberately, and that is a decision rather than an
oversight** (`PRODUCT_RULES` §12). No key acted, and writing `api_key_id: null` would assert that
one was involved and was empty. **The presence of the field is itself the signal for which door was
used** — which is the one fact the two rows should differ on.
🔔 **Flip condition:** when [core#694] gives `old_values`/`new_values` a reader, decide the payload
shape for both doors together. Until then there are 30 writers and 0 readers, and changing a live
production handler for a field nothing reads is the more expensive half of the trade.

### 8.7 · Scope: this rules ONE route. The other 27 are filed, not implied.

Measured this session: `api_v1_routes.py` declares **54 routes, 28 of them mutating**
(15 `POST`, 6 `PUT`, 6 `DELETE`, 1 `PATCH`), and **none writes an audit row** — while the UI door
carries **37** `_audit` calls across 13 state classes, so most of those 28 have a twin that already
records the action.

🚨 **This section deliberately does not rule the other 27.** I have not measured which have auditing
twins, what each one's before/after shape is, or which are idempotent — and a contract written over
a population I have not read is a contract for routes I am guessing about. Ruling them here would
be the mistake coordinator rule 20 names, pointed the other way: naming a superset nobody checked.

**The sweep is [core#1534], so that it is neither silently implied nor silently dropped.** §8.2's
invariant is what it will be measured against; §8.4–§8.6 are the worked example. The cancel route
itself is **[core#1533]**.

Three questions that survey has to answer per route, because each changes the verdict for some of
them: **does an auditing UI twin exist** (where it does, the invariant decides and there is nothing
to debate); **is there a no-op path** (§8.4's condition exists because an unconditional write files
transitions that never happened — `PATCH /notifications/{id}/read` is the obvious candidate); and
**is it bulk** (`POST /api/v1/import` and `POST /api/v1/pipelines/yaml` create many objects in one
call — one row each, or one for the import?).

[core#1533]: https://github.com/datanika-io/datanika-core/issues/1533
[core#1534]: https://github.com/datanika-io/datanika-core/issues/1534

[core#655]: https://github.com/datanika-io/datanika-core/issues/655
[core#670]: https://github.com/datanika-io/datanika-core/issues/670
[core#693]: https://github.com/datanika-io/datanika-core/issues/693
[core#694]: https://github.com/datanika-io/datanika-core/issues/694
[core#735]: https://github.com/datanika-io/datanika-core/issues/735
[core#851]: https://github.com/datanika-io/datanika-core/issues/851
[core#934]: https://github.com/datanika-io/datanika-core/issues/934
[core#1127]: https://github.com/datanika-io/datanika-core/issues/1127
[core#1128]: https://github.com/datanika-io/datanika-core/issues/1128
[core#1081]: https://github.com/datanika-io/datanika-core/issues/1081
[core#864]: https://github.com/datanika-io/datanika-core/issues/864
[PR #1144]: https://github.com/datanika-io/datanika-core/pull/1144
[PR #1146]: https://github.com/datanika-io/datanika-core/pull/1146
