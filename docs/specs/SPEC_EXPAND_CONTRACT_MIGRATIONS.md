# SPEC — Expand/Contract Migrations

> **Status:** policy, effective from the first blue/green swap (A3, [core#425]).
> **Owner:** Infra wrote it; **Engineering is who it binds** — it changes how migrations are written.
> **Related:** the blue/green swap this policy exists for is [core#425] (closed — `scripts/deploy-bluegreen.sh`);
> the promotion procedure that enforces it at review time is
> [`docs/runbooks/RUNBOOK_DEV_TO_MASTER.md`](../runbooks/RUNBOOK_DEV_TO_MASTER.md).

---

## Why this exists now

Today a deploy **stops** the app, then starts the new one. Exactly one version of the code
ever talks to the database, so a migration only has to be compatible with the version
shipping alongside it. That is the only reason we have got away without this policy.

Blue/green removes that guarantee. The new container is started **while the old one is
still serving**, and `alembic upgrade head` runs in its startup command — so:

```
t0  old code  +  old schema      ← serving
t1  old code  +  NEW schema      ← serving, new container migrating   ⚠️ the dangerous window
t2  new code  +  new schema      ← swap complete
```

**At `t1` the previous version is running against the migrated schema.** A migration that
renames or drops something breaks production *at that instant* — not at the swap, and not
in CI, which only ever runs one version.

Celery widens the window: worker containers restart separately, so old task code can meet
the new schema for longer than the HTTP swap takes.

**Therefore: every migration must be safe for the version that is still running.**

---

## The rule

> A migration may only make changes that the **currently deployed** application version
> can tolerate. Anything destructive waits for a **later** release, once no running
> version depends on it.

That splits every breaking change into phases across **separate releases**:

| Phase | Release | What it does | Safe because |
|---|---|---|---|
| **Expand** | N | Add the new shape. Additive only. | Old code ignores what it doesn't know about. |
| **Migrate** | N | New code writes both shapes / reads the new one. | Both shapes are present and populated. |
| **Contract** | **N+1 or later** | Remove the old shape. | Nothing still running reads it. |

**Contract never ships in the same release as expand.** If it does, `t1` breaks.

---

## Concretely

### ✅ Safe in a single release

- `ADD COLUMN` **nullable**, or with a `DEFAULT` (Postgres 11+ doesn't rewrite the table).
- `CREATE TABLE`.
- `CREATE INDEX CONCURRENTLY` (see the lock notes below).
- Adding a **nullable** FK.
- Backfilling data in batches, **with a commit between them** — see the lock notes.
- Widening a type (`varchar(50)` → `text`, `int` → `bigint`).

### ❌ Never in the same release as the code that needs it

| Change | Why it breaks `t1` | Do instead |
|---|---|---|
| `DROP COLUMN` | Old code still `SELECT`s it | Stop using it in release N, drop in N+1 |
| Rename column/table | Old code queries the old name | Add new → dual-write → backfill → drop old in N+1 |
| `SET NOT NULL` | Old code still inserts NULLs | Add nullable → backfill → enforce in N+1 |
| Narrowing a type | Old code writes wider values | Widen-only; narrow in a later release |
| `DROP TABLE` still referenced | Old code queries it | Orphan it first, drop later |
| Adding a `UNIQUE` constraint | Old code may write duplicates | Deduplicate + enforce in the app first |

### Renaming a column — the canonical worked example

```
Release N    expand   : add `new_name` (nullable); backfill from `old_name`
             migrate  : new code writes BOTH, reads `new_name`
                        (old code, still running at t1, keeps using `old_name` — fine)
Release N+1  contract : nothing reads `old_name` any more → drop it
```

Three deploys, zero downtime. The tempting one-line `ALTER TABLE ... RENAME COLUMN` is a
guaranteed `t1` outage.

---

## Locks matter as much as compatibility

A migration that is *logically* backward compatible can still take production down by
holding a lock while the old version serves traffic.

- Set a short `lock_timeout` so a migration **fails fast** instead of queueing behind a
  long transaction and blocking every subsequent query.
- `CREATE INDEX CONCURRENTLY` **cannot run inside a transaction** — Alembic wraps
  migrations in one by default, so it needs an autocommit block. Without that it errors;
  with a plain `CREATE INDEX` it takes an `ACCESS EXCLUSIVE` lock and stalls writes.
- Backfill in **batches with commits**, never one statement over a large table.

```python
with op.get_context().autocommit_block():
    op.execute("CREATE INDEX CONCURRENTLY ix_audit_logs_user_id ON audit_logs (user_id)")
```

> ✅ **This works as of [core#933] (fixed 2026-09-07).** It did not for the life of the
> project before that: `migrations/env.py` executed `SET search_path` on the connection,
> which autobegins a SQLAlchemy transaction alembic did not begin, and
> `autocommit_block()` refuses exactly that state with a bare, message-less
> `AssertionError`. The search path now arrives in libpq's startup packet, so nothing is
> executed on that connection at all.
>
> 🚨 **`CREATE INDEX CONCURRENTLY` can leave an INVALID index if it fails part-way**, and
> the next deploy then dies on `relation already exists`. That risk is why a plain
> `CREATE INDEX` is still the right call on a small table — state the row count in the PR
> and say which you chose. Concurrency is a cost you pay for a lock you actually have.
>
> ⚠️ **Do not hand-roll `connection.commit()` inside a migration instead.** Fifteen
> migrations in this tree do, and they were safe only while alembic owned no transaction.
> `env.py` now sets `transaction_per_migration=True` so each one's commit is confined to
> its own migration — without it, a hand-rolled commit ends alembic's transaction and a
> **later** migration's `autocommit_block()` dies with `InvalidRequestError: This
> transaction is inactive`. Use the block; it is the supported mechanism now.
>
> ⚠️ **Consequence of `transaction_per_migration`, worth knowing before you rely on it:**
> a failure part-way through `upgrade head` leaves earlier migrations **applied** and
> recorded, rather than rolling the whole chain back. Under expand/contract each migration
> is independently safe and a re-run resumes where it stopped — but plan the batch that
> way rather than assuming all-or-nothing.

---

## One migrator at a time

`alembic upgrade head` runs from the container start command. Alembic does **not** take a
distributed lock, so two containers starting together can race and corrupt the version
table. Blue/green starts exactly one new container per swap, which is what makes this
safe — **it is a property of the deploy procedure, not of Alembic.** If we ever start
replicas in parallel, migrations must move to a one-shot job or take a Postgres advisory
lock first.

---

## PR checklist

Any PR containing a migration answers these:

- [ ] Is every change **additive**? If not, which release does the contract phase land in?
- [ ] Would the **currently deployed** version still run against this schema? (That is the
      `t1` question — not "does the new code work".)
- [ ] Any `NOT NULL`, rename, drop, narrowing, or new `UNIQUE`? → must be phased.
- [ ] Long-held locks considered. `CREATE INDEX CONCURRENTLY` **is** available
      ([core#933]); say which you chose and why — for a small table a plain
      `CREATE INDEX` is still better, because a failed concurrent build leaves an
      INVALID index that wedges the next deploy. State the row count either way.
- [ ] Backfills batched, **with a commit between batches** inside an autocommit block
      if the table is large enough that holding locks to the end would matter. Say how
      long it runs.
- [ ] **Celery task code** also tolerant of the old *and* new shape — workers restart
      separately and lag the web swap.

---

## Testing the `t1` window

CI runs one version, so it cannot catch a `t1` break by construction. The gap is covered
by asserting the property CI structurally can't:

**Run the previous release's test suite against the new schema.** Migrate a database to
`head`, then execute the tests from the currently-deployed tag against it. A failure there
is exactly a `t1` production break, caught before the swap.

Until that job exists, the checklist above is the control — and its weakness (a human
answering a question) is the same weakness that made promotion-ref enumeration leak. Treat
it as an interim measure, not the destination.

### ✅ Shipped 2026-07-22 (Engineering, core#449) — `tests/test_migrations/test_expand_contract.py`

Runs in the `migration-roundtrip` CI job, against a real migrated Postgres.

**It does not literally run the previous release's test suite, deliberately.** Almost every
test here builds its schema with `Base.metadata.create_all` on SQLite and never touches a
migrated Postgres — so that job would pass while exercising virtually nothing, which is the
failure mode this repo spent the week removing. Instead it asserts the property the
checklist actually asks about (*"would the currently deployed version still run against this
schema?"*):

1. migrate Postgres to the branch's `head`;
2. read the **deployed** release's models straight out of git via AST (`DEPLOYED_REF`,
   default `origin/master`) — never imported, since two versions of `datanika.models` in one
   interpreter collide on the declarative registry;
3. assert every table and column those models reference still exists, and that nothing
   tightened to `NOT NULL` under them.

**Catches:** `DROP COLUMN` · rename (as the drop half) · `DROP TABLE` · `SET NOT NULL`.
**Does not catch, and says so rather than implying otherwise:** type narrowing, and a new
`UNIQUE` on an existing column. Both remain checklist-only — neither is reliably derivable
from the old models by AST.

Verified end-to-end, not merely written: with a temporary `op.drop_column("api_keys",
"name")` migration in the chain the guard goes red on exactly that assertion, and green
without it. It also asserts its own inputs (≥10 tables parsed, known columns present) so a
model-style refactor cannot make it pass while comparing nothing.

⚠️ Requires `origin/master` to be fetched — `actions/checkout` is shallow by default, so the
job does an explicit `git fetch --depth=1 origin master`.

[core#933]: https://github.com/datanika-io/datanika-core/issues/933
