# SPEC — The audit trail: what a mutating handler owes the record

**Author:** Product · **Status:** contract, ready for Engineering · **Written:** 2026-09-06
**Binds:** Engineering. **Source of truth for:** [core#934].
**Verified against:** `origin/dev` @ `e9e5b51` (fetched 2026-09-06), and `origin/master` @ `1bd1e5c`
where a production claim is made.

> ⚠️ **This spec decides one thing and refuses three others.** It states the contract every audit
> writer is held to, then applies it to the one persisted mutating surface in the product that has
> never had one. It does **not** decide [core#670] (whether to start collecting client IPs),
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

Three tests. **They are not redundant** — each closes a failure the other two are satisfied by, and
the spec names which, because a test suite whose members overlap is one test with three names.

### 4.1 · T1 — the happy path

After a successful `remove_dependency`, `audit_logs` holds exactly **one** matching row with the
exact shape in AC2. Same for AC1.

**Kills:** the audit call being absent, and the audit call being placed *after* `session.commit()`
inside the same `with` block — where the row is added to the session and then dropped on close.

**Cannot see:** an audit row written in its own session. That row exists too, so T1 is green.

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

**Kills:** the second-session implementation from the other side — its audit row commits before the
outer transaction fails, so T3 finds one row and goes red.

⚠️ **"The first session" is the precision that makes T3 work.** Patching `Session.commit`
class-wide breaks the second session too, and the mutant then passes.

### 4.4 · Prove each red before you believe it

Per `PRODUCT_RULES` §15b and the fifteen controls in
`tests/test_ui/test_delete_confirmation_and_blocked_uploads.py`: **apply the mutation to the real
file, run the named test, and check it fails *for the stated reason*.** A red for an unrelated
reason is not a control.

| mutation on the real handler | must go red |
|---|---|
| delete the `_audit` call | T1 |
| move the `_audit` call into its own `get_sync_session()` block | **T2 and T3** |
| move the `_audit` call below `session.commit()` | T1 |
| drop the `if` on the service's return in AC2 | T1's delete case, seeded with a `dep_id` that does not exist |

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
