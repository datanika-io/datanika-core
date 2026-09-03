# QA rules

Rules earned from incidents on this codebase. Every one of them cost a session, a bad promotion, or
a wrong diagnosis to learn. They are not general testing advice — each is here because the obvious
thing was tried first and was wrong.

> **Provenance.** Written into the repository on 2026-08-31, per `plans/SPEC_PLANS_CONSOLIDATION.md`
> (founder decision 2026-08-30). They previously lived in `plans/qa/current_state.md`, a file that
> project rules require be **rewritten from scratch** every session — so a rule kept only there was
> one rewrite away from gone. Git is the durable home.
>
> **Nothing here is a work item.** Do not convert a rule into an issue.

---

## 1. The core of the job: what a signal actually records

**Ask what a signal records, and whether it would look different had the thing failed.** If it would
look the same, it is not evidence. This is the single rule the rest of the document elaborates.

It has been violated in at least seven distinct shapes on this project, and it is worth knowing all
seven, because each looked like health at the time:

| Shape | The instance |
|---|---|
| A CI tick masked by `continue-on-error` | the informational-tier step's own ✓ is meaningless; only its printed `RESULT=` line counts |
| A job that reports success having done nothing | a fork PR gets no token, every step skips, the job goes green having probed nothing |
| A suite that mocks the unit under test | 140 tests, 103 `@patch` decorators; the file-source builder loaded a *directory listing* instead of file contents for months |
| An alert rule structurally unable to fire | two rules were green for weeks because their expressions could never evaluate true |
| A green about the wrong layer | the nightly Kafka smoke listed topics with a library **the workflow installs and production does not have** — a green about connectivity, never about rows |
| Silence read as health | zero `/_event` errors across 14 days of logs, because the socket carried ~9 connections in that window |
| A wait whose success condition **is** absence | `gotoReady` returned when the `/_event` socket had been quiet for 600ms — so a socket that had died satisfied it *sooner* than a healthy one, and one that never opened became a silent 12-second sleep. It could not fail. |
| A correct assertion about unreachable code | `has_permission("admin", "manage_members") is False` passes, and `has_permission` has zero production callers |

**Corollary — the deferral trap.** A note once claimed a gap was covered "by the nightly connector
smoke and the restore check". Both are API-level; neither could catch either of the two P0s that
were then found by a person clicking through production. *Check what the named coverage actually
exercises before accepting it as coverage.*

**Corollary — a readiness gate must be able to fail, and "quiet" is not a readiness signal.**
(core#744, 2026-08-31.) `golden-path` flaked ~50% on `dev` and held a promotion for a day. Two
rounds of triage went to the Celery worker, because the assertion text ended *"Is the Celery worker
running?"* — a hypothesis embedded in a failure string, and the worker was healthy and consuming
other tasks from the same queue throughout.

The actual defect was in the harness's own readiness helper. It waited for the Reflex `/_event`
socket to go **quiet** and then returned successfully whatever it had observed. Silence is what a
*dead* socket produces best, so its success condition was satisfied fastest by the exact failure it
existed to prevent — and, having no failure branch, it degraded to a 12-second sleep and said
nothing. Downstream, the run helper clicked *Run* and navigated away 30–60 ms later (measured in
the failing traces), which is inside the window where Reflex has not yet put the event on the wire
and the unload handler disconnects the socket. Reflex's own comment for that path:
*"otherwise we throw the event into the void."*

**Rules:**
1. **A wait must have a failure branch, and the failure must name what was missing.** If every
   outcome returns the same way, it is a sleep wearing a predicate's name.
2. **Wait on a positive signal the test reads, never on the absence of one.** Here the right signal
   was the framework's own `is_hydrated_rx_state_":true` on the wire — an open socket, an answering
   backend, and a page that says it is mounted, rather than "nothing happened recently".
3. **A click is not delivered when `click()` resolves.** That proves a DOM node was pressed. If the
   next thing you do can tear the page down, first wait until the event is observably on the wire.
4. **Split a multi-process assertion so each layer has its own message.** "No run row appeared"
   spans the browser, the web app and the worker; one sentence naming the worker sent two people to
   the wrong process on nights the worker was fine.

**Corollary — an empty result is three different facts wearing one face, and the reassuring one is
the default reading.** (core#869 / core#883, 2026-09-01.) `sqlalchemy` `get_table_names(schema=X)`
returns `[]` when `X` **does not exist**. It does not raise. Measured against real BigQuery, with a
positive control so `[]` was not equally explained by the method being broken:

```
EXISTS, has tables    bigqueryfirstrun                 -> ['_dlt_loads', ..., 'customers', 'orders']
DOES NOT EXIST        datanika_docs_demo               -> []
DOES NOT EXIST        datanika_no_such_dataset_qa869   -> []
```

So *"the dataset was deleted"*, *"the dataset is empty"* and *"the load produced no tables"* are one
signal — in the product and in your probe alike. Downstream, `CatalogService.introspect_tables`
returns `[]`, `_sync_catalog_after_upload` writes zero entries and returns `0` **as a success value**,
and core#494's warning lives on the `except` path only, so nothing fires.

What it cost: an engineer with a correctly-armed instrument, working on the right issue, produced two
confident hypotheses — *"the upload has not materialised yet"* and *"it wrote to a different
dataset"* — and **neither was right**. The dataset had been deleted by that issue's own documented
cleanup, announced two comments above the reading.

**Rules:**
1. **Any lister that answers a question about existence needs a positive control** — a case you know
   is non-empty, run in the same probe. Without one, `[]` everywhere is equally explained by your
   instrument being broken, and one possible answer is not a measurement.
2. **Before explaining a measurement with a code defect, check that the thing measured still
   exists.** Cheaper than every code hypothesis, and it is the one check neither hypothesis above
   would ever have reached. Read the issue's own recent comments first — a teardown is usually
   announced by the person who did it.
3. **Two readings of "the same" thing taken at different times are not one state.** A REST
   verification showing 19 rows and a reflection showing 0 tables straddled a deletion; read together
   they produce a contradiction that *invites* a code explanation. Timestamp every measurement and
   compare the timestamps before comparing the values.
4. **When a count of zero is a legitimate success value, say so out loud somewhere.** A function that
   returns `0` for "nothing found" and `0` for "nothing exists to find" needs the caller to
   distinguish them — here, `rows > 0 and tables == 0` is a contradiction the caller can see and the
   callee cannot.

## 2. Any green you have not personally forced red is unproven

And its twin: **a red you have not shown can go green may be asserting something unreachable.**

- Before a probe is allowed to close an issue, break the thing it guards and watch it fail.
- Ship the forced-red as a required artifact of the work, not as an afterthought.
- **Assert the mutation landed before trusting the run that follows it.** A `sed` whose delimiter
  also appears in the pattern fails silently — one such no-op produced a green run against
  *unmutated* code, which reads exactly like "my test is worthless".

**Mutation testing as a tool was evaluated and rejected here, with numbers**: it ranked our two real
suites backwards, scoring the blind one 92% and the one that catches a real defect 62%. The
replacement is *shown red as a required artifact*. "Make the guarded thing worse" catches all three
failure shapes; "mutate the source" catches one.

### 2a. A derivation that depends on import side effects is not a derivation

**(2026-08-31, Engineering finding, verified by QA.)** Several guards here derive their input set
rather than listing it — the `/api/v1` route table (core#719), the tenant-owned model set (core#732),
the compose services a deploy must name. Deriving is right: a hand-written list is stale the day
after it is written. But **where the derivation reads from decides whether it can silently cover a
subset.**

`SQLAlchemy`'s `Base.registry` is populated by whatever has been *imported*. Inside a full
`pytest tests/` run that is everything, because some other test module imported it. So a
registry-based model set returns all 17 models in CI and a handful when the file is run alone. The
guard in question **passed with its own import loop deleted entirely**, and standalone covered
**13 of 17** while reporting clean. Nothing about the result said "partial".

The fix is to read from something that cannot vary with import order — here, an **AST walk over
`datanika/models/*.py`** — and then to cross-check that against the runtime registry in a test that
does its own explicit import loop.

**Both halves of that cross-check must be shown red, and they fail differently.** Measured on
`tests/test_security/test_tenant_fk_boundary.py`:

| mutation | what it models | what fires |
|---|---|---|
| stop recognising the bare `TenantMixin` base | derivation returns **nothing** | `assert derived` — "the AST walk has stopped working" |
| walk `models/[a-r]*.py` instead of `*.py` | derivation returns a **subset** | the set comparison: `only in registry=['SSOConfig', 'Schedule', 'Transformation', 'Upload', 'UploadedFile']` |

The second is the one worth insisting on. An emptiness assertion is cheap and everyone adds it; the
realistic defect is *12 of 17*, which is non-empty, looks healthy, and is caught only by comparing
against an independently-derived set.

**Rules:**
1. **Ask what populates the thing you are deriving from.** If the answer is "imports", the answer is
   "whatever ran first".
2. **Assert the derivation is non-empty AND that it agrees with a second, independent derivation.**
   Neither alone is sufficient.
3. **Run the guard in its own pytest session**, not only as part of the suite. A guard that is only
   ever exercised alongside 4,000 other tests has never been observed in the state a bisect or a
   `-k` run puts it in.

## 3. Negative controls are what attribute a red

A red proves the test failed. It does not prove *why*. For a **permissive** defect — one where
merely exercising the code passes — a bare red could equally mean the harness never reached the
code at all.

**Controls green + defects red is the evidence.** Both sides must call the same helper and differ in
exactly one fact.

This is not ceremony. It has paid three times:
- A cross-tenant probe returned `3 failed, 1 passed`; the single **pass** was the control, and it is
  what proved the harness reached the handlers rather than falling over in setup.
- A defect suite went red on the *correct* change, because it asserted a predicate's return value
  rather than the behaviour.
- A guard-the-guard class caught a false positive in its own detector: `ast.walk` descends into
  `FunctionDef`, so a call inside `def boot():` was flagged — which would have failed every correct
  fix.

**The control most likely to be missed is the permissive one.** When the fix is "deny X", the test
that matters is *"legitimate Y still succeeds"* — a deny-everything implementation passes every
other assertion in the spec.

## 4. Prove a new test earns its place

**Find a break the existing tests miss.** Three of four candidate mutations for one new file were
already caught elsewhere; only the fourth justified the file. Record the ones that proved nothing,
so nobody re-derives them.

The strongest form: mutate the code so that **N existing tests pass and only the new one fails.**

## 5. `xfail` conventions

Use `pytest.mark.xfail(strict=True, raises=<Specific>)`.

- **`strict=True`** makes an XPASS a failure, so the marker cannot outlive the defect. A fix
  therefore *cannot land silently* — it forces the marker out in the same PR.
- **`raises=`** stops an `ImportError` or a signature change being absorbed into a silent xfail.
  This has already caught a real harness bug: a missing fixture surfaced as a loud `NoResultFound`
  instead of a meaningless pass.
- A `KNOWN_VIOLATIONS`-style allowlist must be `strict` too. One entry lasted about an hour before
  the defect it named was fixed elsewhere and the suite correctly failed. **An entry there cannot
  outlive its defect** — that is the point of it.
- **A bare `assert` in the SETUP of an `xfail(raises=AssertionError)` test is absorbed by the
  marker**, so a broken harness reports as a satisfied expected-failure — indistinguishable from the
  defect being present. Raise a non-`AssertionError` (a local `HarnessError`) for setup invariants,
  so only the assertion under test can xfail. Related: when you CLEAR an xfail, mutate the fix back
  and confirm the test goes red *for the stated reason* — one strict xfail on this project was being
  satisfied by an `IndentationError` inside `ast.parse(inspect.getsource(...))`, visible only on
  removing the marker.

## 6. Validate against the real consumer

A mocked test encodes *your model* of the thing in doubt, which is the one thing you cannot use it
to check.

- DB connector fixes go against a real database, not DSN-string unit tests. A "fix" once passed
  mocked tests and promoted a broken connector to production.
- Artifacts an external system ingests go through **that system's** validator.
- The migration graph goes through `alembic heads`; a green `pytest` says nothing about it.
- A tier count, a route list, a spec count: run the command the CI runs, with the env vars CI sets.
  The same `--list` command reports **62 tests** with one env var set and **5** without it.
- **Reading a spec is not running it.** A prediction about which tests would skip, made from
  reading them, was wrong about five of them.

## 7. Count the instruction, not the phrase

A guide corrected to *deny* an old behaviour still contains the old words. Grepping the phrase
re-finds work that is already done, and reports fixed content as broken.

The inverse also bites: `grep 429` on a CI log returned two hits, **both digits inside a timestamp**
(`22:11:56.4290894Z`). Read the hits before believing the count.

## 8. Severity

| | |
|---|---|
| **S1** | blocks production |
| **S2** | blocks a feature |
| **S3** | cosmetic |

**S1 and S2 require a regression test before closing.** The test goes red against the unfixed code
first, and **ships in the same PR as the fix** — not separately.

Severity is about the defect, not about the strength of your evidence. When the evidence is
staging-only, say *"filed S2 because the evidence is staging-only, which is a statement about the
evidence rather than the risk"* — do not silently discount the severity to match your confidence.

## 9. When an incident is caught late, file the paired TDD-gap ticket

A fix closes one bug. The **class** stays open until a CI-level probe catches it at PR time.

Typical shapes: realistic-size INSERTs for column-width drift · vendor-sandbox contract tests for
payload schemas · schema-width trip-wires · property-based invariant tests · a probe of the built
artifact rather than the source.

This has paid: a migration round-trip probe filed after two int32 overflows red-flagged a third at
PR time within 24 hours.

If anyone shrugs *"but the unit tests pass"* — unit tests passing is the **premise** of the bug, not
a defense.

## 10. The E2E tier policy — binding

| Tier | Marker | Red holds a promotion? | Runs? |
|---|---|---|---|
| **Gating** | *(default — no marker)* | **yes** | yes |
| **Informational** | `@informational` in the `describe` title | no | **yes** |

**New specs graduate into the gate; they do not enter it.** Turning the gate on coupled production
promotion to the stability of tests written that morning — and that inverts the incentive, because
the cheapest way to unblock a promotion becomes *loosening the assertion*. That was caught happening
in review: relaxing one step to `rows >= 1` would have gone green **against an open P0**, because
you get one row and it is the directory listing. **A gate that punishes honest new tests produces
dishonest ones.**

**Graduation is mechanical:** 3 consecutive greens on `dev`, read from the job's printed
`INFORMATIONAL_RESULT=success` line — **never the step's own tick, which `continue-on-error` masks.**
Then delete the marker and close the tracking issue.

- `empty` and `unknown` are neither green nor red and count toward nothing.
- **A cancelled run is neither green nor red.** `dev` is busy and the concurrency group cancels
  often. Re-read; do not assume.
- **A skipped job is not a flake.** Check the run's *event*: the push-only jobs do not run on a
  `pull_request`, so a promotion PR legitimately skips them.

**🚫 Demotion is not the inverse.** Moving a spec *out* of the gating tier requires an issue and is
only ever legitimate for a spec that has **never passed**. Demoting one that used to pass hides a
regression. Without this rule, "informational" becomes where tests quietly go to die, which is worse
than the problem the tier solves.

**`continue-on-error` quarantines the verdict, never the execution.** A quarantined test that stops
running stops telling you when it starts passing — that is a disabled test with better PR.

## 11. A skip is in neither tier and counts toward nothing

Print the tier split. **"46 passed" reads like full coverage until you look** and find 16 skips
proving nothing.

Skipping must never be the default on a missing dependency. Inverting that default — *missing
credentials or imports now **fail**; skipping is opt-in* — closed a systemic hole where dropping one
environment variable made an entire nightly suite skip and exit 0. **Dropping any env var should now
only make a suite stricter, never quieter.**


**🚨 And a skip does not skip the same way for everyone. `UV_NO_SYNC=1` makes a `skipif`-guarded
test RUN in your worktree and SKIP in CI** — so you see coverage that does not exist. (2026-09-01,
from core#684 / core#825.) We all export that flag so `uv run` cannot gut the venv mid-pytest; the
same flag guarantees the venv is a **superset** of the lock and never a subset. When `s3fs` was
dropped from `uv.lock`, `TestS3FileSourceMovesRows`'s four tests kept passing locally against a
leftover `s3fs 2025.12.0` while CI — which installs from the lock — skipped all four, and the
capability was absent from the shipped image entirely.

**The tell is that the predicate asks about the ENVIRONMENT, not the repo:** *"is package X
importable"*, *"is binary Y on PATH"*, *"is service Z reachable"*. Before believing a local green on
anything dependency-gated, check what CI actually did, and read the **skip count**:

```bash
grep -c '^name = "<pkg>"' uv.lock   # 0 => CI cannot have run it, whatever your box says
pytest <file> -q -rs                 # -rs prints skip REASONS; a bare -q hides them
```

🔑 **The structural version, which is the part worth carrying: the test that proves a capability
works is often disabled by the same condition that breaks it.** So the suite gets quieter at exactly
the moment it should get louder, and a deferral recorded in a marker is invisible to everyone who
does not open that file (core#885).
## 12. Retries

**A retry around container startup hides nothing. A retry around an assertion hides a product bug.
If you cannot say which you are retrying, it is the second — leave it failing.**

## 13. Reading CI and production signals

- **Green attached to the wrong commit.** Right after a force-push, `gh pr view --json
  statusCheckRollup` reports the *pre-rebase* SHA's verdict. Read
  `repos/<r>/commits/<HEAD_SHA>/check-runs` and compare against `git rev-parse HEAD`.
- **`403` vs `502`** identifies *who* refused: 403 is the edge, 502 is the origin. A red can point at
  the wrong layer.
- **A failure "on `<sha>`" means the run that surfaced it, never the commit that caused it.** One
  gating red originated in a *different repository* pulled in at build time.
- **Anchor a log grep to the job.** The same run log carries the same marker line from two different
  jobs; an unanchored grep returns a mixed verdict spanning both, and has twice produced a wrong
  diagnosis.
- **Staging jobs are `dev`-only.** They do not run on a production promotion, so a promotion carries
  no staging verdict at all — and *nothing there* reads exactly like clean.
- **A comment claiming a guard exists is not a guard.** Grep the other repository before believing a
  cross-repo handoff.
- **Configuration that is not deployed is not configuration.** A setting present only on the box, in
  no repository file, survives only until that box is rebuilt — and one already has been. Verify by
  asking the running container, not the manifest; then put it in the manifest.

## 14. Timing and rate limits

- **A fixed-window rate limiter makes a suite's verdict depend on the wall clock.** Measured here:
  the *same* over-limit traffic is rejected six times when it falls inside one minute and **zero**
  times when a boundary splits it — about a 1-in-12 chance of an undeserved green. **Never conclude
  "flaky, re-run it" on a 429**, and never judge a rate-limit fix by one green. Any fix that merely
  shifts timing buys a coin flip; the fix has to be a deterministic budget.
- **A 429 is a rejection before the handler runs.** Adding it to an accepted-status list makes a
  suite pass on requests that never reached the thing it is named after. This applies to every
  `expect([...]).toContain(status)` in the E2E suite.
- **A test that pins an external number fails on the *correct* change.** Decide deliberately whether
  you are pinning a contract or observing a value.

## 15. For anything with a written spec, diff the spec against the tests

Not the tests against the code. A suite can be complete with respect to the implementation and blind
to a clause the implementation happens to get right — and **no mutation pass can find that**,
because the shipped line is correct, so there is no mutant to generate.

## 16. Assert on the destination, never on the pipeline's own report

`result["rows_loaded"]` is a number the pipeline reports about itself. The defect that motivated this
rule is precisely the case where the count looked plausible while the contents were a directory
listing. **Read the rows back with a query.**

## 17. Process

> ⚠️ **This is not the last section — §18 and §19 follow.** They were appended after this one
> because renumbering would break the §-references in `ci.yml`, `e2e/scripts/`, and several tests.
> The pointer is here because a heading that reads like an ending is how
> `PLAN_HUMAN_LOCKERS.md` hid six live items below its "Completed" section: the document was not
> wrong, it just put live content where the reader had already stopped looking.

- Every QA task has a GitHub issue **in the repository it touches**; cross-repo test infrastructure
  goes in the repository holding the harness.
- Branch `<issue>-<slug>`; PR targets `dev`; title and commits carry `[QA]`.
- **Filter GitHub by the `[Dept]` title tag, not by author** — all departments share one identity, so
  `author:@me` returns everyone's work.
- **`closes #N` does not fire on a `dev` merge.** Issues close after deployment from the production
  branch, which is why release validation is a QA judgement rather than an automatic consequence.
- **Sign off content marked pending verification only after walking it through yourself.** That tag
  is QA's to remove and nobody else's. It has come off without a walkthrough once, and the content
  stayed wrong for months afterwards.
- **A security-sensitive finding does not go in a public issue.** This repository is public and at
  least two automated surfaces publish issue *titles*. Write it up locally and route it directly.

## 18. A published target with no instrument is indistinguishable from a target being met

Earned on [core#721], where `docs/slo_targets.md` carried 33 numeric commitments for four and a half
months and `git grep` found **zero** references to it in any repository. It could not be violated and
it could not be achieved, so it read as a commitment while committing to nothing.

This is the same family as a rule that cannot fire and a check that cannot fail, but it is worth
stating separately because it has no red state to notice. **There is nothing to see.** The five rules
below all come from building the instrument that finally read it.

### 18a. Report three states, and never let "unmeasured" render as a pass

`PASS` / `FAIL` / `NO_VERDICT`. An unmeasured commitment must be counted and printed, and the
process must not exit 0 on it. `scripts/slo_report.py` exits **1** for a missed target and **2** when
nothing could be measured, precisely so a scheduled job cannot go green while blind — the failure
mode of every `noDataState: OK` rule this project has shipped.

### 18b. A quantile over too few samples is not a number

Every measured indicator declares a `min_samples` floor, and below it the answer is `NO_VERDICT`. A
p95 computed from three requests is a beautiful figure that means nothing; scored as a pass it is
worse than no figure, because a pass gets acted on.

### 18c. An instrument you have MEASURED to be broken must never report PASS

Not "note the caveat and score it anyway". Print the number, refuse to score it, and name the issue.
Six SLOs are in this state behind [core#895]. The reasoning: at zero traffic a broken meter is
harmless, and the moment real traffic arrives it starts emitting *confident greens*. The transition
is silent and there is no moment at which anyone re-examines it.

### 18d. The threshold lives in exactly one file, and the checker may not restate it

If the registry that runs the checks also carries its own copy of each number, the numbers can be
relaxed to match whatever production happens to do and every check goes green without anyone editing
a commitment. `tests/test_slo/test_slo_coverage.py` forbids threshold keys in the registry
mechanically. **A target quietly revised to match current behaviour is the same defect as a guard
that passes because it looks at nothing.**

### 18e. A zero-valued target is satisfied by the thing never happening

`Webhook handler (Paddle) — HTTP 5xx: 0` is met perfectly by an endpoint nobody calls, and at zero
paying users nobody does. Any error-rate check must gate on a count of **successes**, not of
failures, or the greenest line in the report is the one proving least. Related: a target written
`< 0` is unsatisfiable by any real number, so a healthy system reports FAIL forever and people learn
to ignore the report — use inclusive bounds.

## 19. A total-count floor does not catch a missing section

The SLO document parser's first column lookup silently dropped **all six** Saturation rows, and the
total came to exactly **20** against a floor of **20**. The floor was satisfied; a sixth of the
document had vanished.

Assert the count **per section**, per file, per category — whatever the natural subdivision is. The
same shape has now bitten this project three times: the restore drill asserting `plans >= 5` while
`users` was empty, a row-total check that would have missed 196-vs-177 by 10%, and this one.
**Whenever you write a floor, ask what fraction of the thing could disappear beneath it.**

## 20. Check the artifact against what it represents, not against its own plausibility

Three departments hit this on the same day (2026-09-02) in three different shapes, which is what
makes it a rule rather than three incidents:

- **Engineering** retracted three findings, each *"describing a plausible artifact instead of
  checking what it represents."*
- **QA's own handoff file** asserted that a correction had been posted to [core#895]. It had not.
  The grep that "confirmed" it matched *the right word in the wrong sentence* — `six` in
  *"the six `Connection: close` reads"*.
- **Two acceptance criteria passed against the unfixed code** ([core#895] AC2, then [core#896] AC2
  one day later), because both were drafted from a *description* of the defect rather than from a
  measurement of it. An AC written that way tends to describe the part that already works.

The common move is that a representation was inspected and treated as the thing. The artifact is
always the cheaper thing to look at, it is usually consistent with itself, and self-consistency is
what makes it convincing.

**Rules:**

1. **A claim about an external artifact is verified against that artifact**, not against a file that
   says it was done, and not against a grep of it. Read the hits.
2. **Before writing an acceptance criterion, run it.** If it passes today, it is a control, not a
   criterion — label it as one and go find the case that fails. Both ACs above are now shipped as
   controls in the suites that replaced them, which is the honest place for them.
3. **A number in a summary must be copied from output, never tallied by hand.** The
   `docs/slo_baseline.md` NO_VERDICT breakdown read *14 / 6 / 3*; the measured answer is
   **13 / 4 / 6**, wrong in all three cells. The figures had been derived by arithmetic over
   *registry rows* and published as counts of *commitments* — 26 rows carry 33 commitments, and 4 of
   the 6 entries flagged `blocked_by` never reach that branch because a sample floor stops them
   first. Nothing in the report ever printed 14, 6 or 3. The fix is not care: it is making the tool
   print the breakdown, so there is nothing left to tally.
4. **Ask which population you are counting.** Rows and commitments, entries and verdicts, files and
   findings — a plausible number over the wrong population is indistinguishable from a measurement,
   and the arithmetic will check out.
5. **Prose in a docstring or a guide is a specification the next reader acts on.** Assert it.
   `_normalize_path`'s docstring claimed to replace *"(IDs, UUIDs)"* and handled only integers,
   which is what would have got [core#896] closed as stale by a reader who grepped `metrics.py` and
   found a normaliser. [core#673]'s coverage claim — *"~20 mutating handlers already route through
   it, so one guard covers them all"* — was false by eight handlers. **Derive the claim from the
   prose rather than hardcoding it**, so correcting the sentence is an equally valid fix; the defect
   is the mismatch, not any particular word.

**Corollary — an instruction in an issue is an artifact too.** [core#896] proposed reading
`scope["route"]` in the ASGI middleware. Measured against the starlette this project actually runs
(0.52.1), `"route"` is never used as a scope key anywhere in the package, so that fix reads `None`
on every request and buckets **every** path into `<other>` — satisfying both cardinality criteria
perfectly while blinding the two REST-API latency SLIs that select on `path`. A proposed fix is a
hypothesis; run it against the installed version before building to it.

---

## 21. Before triaging a red, ask what changed about the INSTRUMENT

**(2026-09-02 — the `Nightly Connector Smoke` triage.)** Three consecutive nightly reds looked like
an escalating incident. They were **one breakage roughly ten nights old**, becoming visible for the
first time because [core#827]'s fix stopped the job laundering pytest's exit code through a pipe to
`tee`. Nothing broke on the day the alerts started.

The decisive evidence is cheap and it is always available: **compare the failing run to the last
GREEN one at the level of the thing being measured, not at the level of the verdict.** The last
green run reported `12 failed, 9 passed` — the identical failure set, test for test.

Two corollaries that changed the answer here:

1. **Read the run's `event` and `head_branch`, not just its colour.** One of the three "reds" was a
   `workflow_dispatch` on `dev` at the fix commit's own SHA — i.e. an engineer executing the fix's
   acceptance criterion *"show it red before calling it fixed"*. **It was a successful verification
   filed as an alert.** Counting it as an incident inverts its meaning.
2. **Read the config at the commit each run used** (`git cat-file -p <sha>:.github/workflows/x.yml`),
   never at `HEAD`. That is what separates *"failures that predate the fix"* from *"the fix does not
   work"*, and the two call for opposite responses.

🔑 **The general form: a change in a signal is not a change in the system.** When an alarm starts
firing, the first question is whether the alarm changed, not what it is pointing at.

**And the sequel is the harder half.** Once the red is correct, ask whether it can ever go green
again. Four of these twelve failures are lapsed vendor trial accounts — nothing an engineer can fix.
A red that repeats nightly and forever destroys the channel exactly as a green that proves nothing
does, and it hides the nine probes that *are* live. Neither carries information. When a check's
failure is permanent and understood, tier it — **and pin the known-bad set by name, so the job still
goes red if the set changes in either direction.** Muting is what tiering becomes when you skip that.

---

## 22. A leaked credential's blast radius is what the ACCOUNT can still do

**(2026-09-02, while assessing the credentials found in public Actions logs.)** I ranked a Databricks
PAT as the most urgent rotation, reasoning from the credential *type* — a workspace token means
clusters, jobs, data, billable compute. The founder queue had already measured that workspace
**INACTIVE**: the token authenticates and lists catalogs, but no compute can start. The real
exposure was metadata only, and the genuinely live credential was a different one entirely.

The nightly's green Databricks probes did not contradict that finding, and reading them as
reassurance would have been a second error: those probes only ever exercise **auth + list**, which
is precisely the subset that still works on a dead account.

**Rules:**

1. **Reason from the account's measured state, not from the credential's type.** "It is a PAT" tells
   you the maximum; only the account tells you the actual.
2. **Check the founder/human-locked queue before assigning a severity.** The measurement that
   corrected this was already written down in another repo's issue.
3. **Overstating a severity spends the same credibility as understating one** — it aims a human at
   the wrong item first. Correct it in place, publicly, rather than quietly leaving the ranking.
4. Related and pointing the other way: *a credential existing is not the same as the service being
   usable.* Free-trial sandboxes decay silently and **the credential keeps working while the service
   stops** — which is why "the probe authenticates" is never evidence the connector works.

## 23. A per-pair regression test does not generalize; derive the guard from metadata

Twice now, a hand-maintained ordered list of table deletes in
`datanika/scripts/e2e_seed.py::_tear_down_fixture` has drifted from the schema and
**permanently wedged `e2e-staging`**:

| | added | wedged by |
|---|---|---|
| [core#415] | Remote-MCP P2 added `oauth_grants.api_key_id -> api_keys.id` | one completed OAuth consent |
| [core#951] | PII separation N (#655) added four tables | the migration's own `user_pii` backfill |

The response to #415 was a behavioural test for **that FK pair**. It was a good test and
it still passes — and it could not see #951, because a per-pair test only ever covers the
pair somebody already debugged. Writing a second per-pair test for `user_pii` would have
bought exactly the same non-coverage a third time.

**The guard has to be derived from the thing that actually changes.** Here that is
SQLAlchemy metadata: for every table the teardown deletes, ask the metadata which tables
carry an FK into it, and assert each is deleted earlier. A new table is then covered the
moment it is mapped, and nobody has to remember a list.
`tests/test_scripts/test_e2e_seed_teardown_fk_drift.py`. Same shape as §21's route-drift
census and the connector-parity script — three findings from the same move.

🚨 **Build the control into the guard.** A derived guard has a failure mode a hand-written
one does not: if the extractor silently returns nothing, the invariant holds *vacuously*
and the suite is green while the guard sees nothing at all. So the drift guard ships with
three controls that fail loudly instead — the parser found ≥20 deletes; it still follows
`_delete_oauth_chain` (those tables appear nowhere else); and no parsed name failed to map
to a table. Without them, deleting one line of the parser turns the guard into a
decoration that reports PASS forever. Cf. §18c.

⚠️ **This class of break is invisible at run level and at spec level.** It kills
`globalSetup`, so **zero specs run**: no Playwright tally, `INFORMATIONAL_RESULT=unknown`,
and the step exits in ~6 s against a 2.4 min green. Meanwhile `deploy-staging`,
`smoke-staging`, `Assert staging is running THIS commit` and all five required checks stay
green, so the run-level `failure` is indistinguishable from the standing `image-cve` +
`e2e-sso` reds. **Read per job, then per step, then per spec — and treat a missing tally as
louder than a failing test, not quieter.**

⚠️ **Fix every dependent the guard names, not the one in the traceback.** #951's stack named
`user_pii`; the guard named five. Fixing only `user_pii` would have surfaced
`email_change_requests` on the very next run, and `password_reset_tokens` — a latent gap
that predated the PII work and had simply never been exercised — on some later one.

[core#415]: https://github.com/datanika-io/datanika-core/issues/415
[core#951]: https://github.com/datanika-io/datanika-core/issues/951

## 24. Ask what the predicate READS, not how many files it opens

A guard's breadth is the reassuring number, and the defect is usually in its depth.

[core#943]'s masking guard was already **class-wide within this repo** — it globs every file
under `.github/workflows/`, not just the one that leaked. It still had a syntax-shaped
blind spot: it read only `step["env"]`. So **hoisting a secret from step level to job or
workflow level — the ordinary refactor, and the obvious move the instant two steps need the
same value — evaded the guard entirely while leaking identically.** Shown one probe at a
time against the shipped guard, with the injectable-`workflow_dir` plumbing held constant in
both arms so the red attributes to the predicate and not to the harness:

| probe | ORIGINAL | PATCHED | want |
|---|---|---|---|
| real tree only (arming baseline) | GREEN | GREEN | GREEN |
| secret in **step** `env:`, unmasked | RED | RED | RED |
| secret in **job** `env:`, unmasked | 🔴 **GREEN** | RED | RED |
| secret in **workflow** `env:`, unmasked | 🔴 **GREEN** | RED | RED |
| job secret, **unrelated** env write | GREEN | GREEN | GREEN |
| secret in job `env:`, correctly masked | GREEN | GREEN | GREEN |

The two rows that are green in **both** arms are the false-positive controls, and they are
the point: without them a guard that had simply been made to go red more often would score
identically on this table. Cf. §3.

**So when you write or review a guard, enumerate the shapes the thing it forbids can take,
and test the ones you did NOT just fix.** *"It scans every file"* answers a different
question from *"what does it look at inside each one?"*

🚨 **A guard has to be class-wide across REPOSITORIES, not merely across files.** A
cross-repo sweep found `datanika-cloud`'s own materializer **already masking, citing
[cloud#91]** — so the organisation knew this failure mode *before this repo acquired the
bug*, and the knowledge did not transfer. A per-repo guard would have been satisfied by
cloud and blind to core, which is the arrangement that produced an S1 live for ~89 days on a
public repo.

**When you fix something in one repo, sweep the others for the same shape in the same
session.** The sweep is minutes; the knowledge does not travel on its own. Record the result
either way — the same sweep found `datanika-landing` writes no secret to `$GITHUB_ENV` at
all, and *"checked, not applicable"* is worth as much as a fix, because it stops the next
person re-deriving it.

### 24a. A guard that reads raw text can be satisfied by a COMMENT

Same family, found the same way — by mutating the real artifact rather than a fixture.

Two assertions of the form `assert '-k "not oracle"' in step` stayed **GREEN** against a
workflow whose `pytest` line had been stripped of `-k`, because the flag is also *described*
in the comment above it. The guard was checking that we still **talk about** the fix, not
that we do it. Its own test suite passed throughout; only the mutation exposed it.

Strip comment lines before asserting on a command — in a `run:` block a line whose first
non-space character is `#` is a comment by definition, so nothing is lost — and keep a
control asserting the stripper still lets a real command through. Otherwise the next person
"fixes" a false positive by narrowing the stripper until the guard matches nothing.
`tests/test_deploy/test_nightly_smoke_tiers.py`.

[core#943]: https://github.com/datanika-io/datanika-core/issues/943
[cloud#91]: https://github.com/datanika-io/datanika-cloud/issues/91

## 25. A refusal-shaped assertion measures whichever validator runs FIRST

**(2026-09-03, the gating-suite discrimination sweep — [core#1026].)** Four findings in one pass,
and they are the same defect wearing four costumes. In each, a test asserted that something was
*refused*, the refusal happened, the test was green — and the mechanism it was named for never ran.

| test | what it claimed | what actually refused |
|---|---|---|
| `test_upload_create_rejects_org_b_connection` ([core#719]) | a cross-tenant boundary | `validate_upload_name` — the payload's hyphen |
| `TestFileUploadPathTraversal`'s two traversal tests | a path-traversal defence | the **extension allowlist** — `.env` is not an allowed type |
| `test_inactive_user_returns_none` | `not user.is_active` | `verify_password`, three lines later — the test used the **wrong password** |
| `test_none_algorithm_attack` | signature verification | its **own fixture**: `json.dumps(default=str)` sent `iat` as a string and jose refused the claim first |

**The discriminator is always one request, and it is cheap: send the same shape with data the
unrelated validator accepts, and check it SUCCEEDS.** For the traversal pair that is
`"../../evil.csv"` — same traversal, allowed extension. It is **accepted**, which settles it in one
line. **Every `assert <refused>` needs a paired `assert <accepted>`.**

Note the fourth row especially: the refusing validator does not have to be in the product. A fixture
that constructs a *malformed* attack has built a test that passes for a reason the attacker would
never reproduce.

### 25a. Rank before you sweep

Auditing everything costs ten times as much and finds the same things. Two cheap static signals cut
339 test functions in `tests/test_security/` to a shortlist that produced all six findings:

1. **refusal-only** — every assertion in the body is `raises` / non-2xx / `is None` / `not in` /
   `== 0` / `is False`. A refusal can be produced by anything that runs earlier.
2. **no acceptance control anywhere in the enclosing class** — nothing attributes the red.

149 of 339 were refusal-only. The intersection with signal 2 is where every confirmed instance sat.
`plans/qa/notes/sweep-1026/triage_refusal_only.py`.

### 25b. Disable ONE mechanism per mutation, and declare the GREEN set too

A mutation that reds a whole file attributes nothing to any individual test. Removing the org scope
from `get_org_connection` reds 19 tests — excellent as a *positive control* that the suite reaches
that boundary, useless as evidence about any one of them.

And declare what must stay **green**: a mutation that reddens its own control is measuring its own
breakage, not the guard. Both halves are in `plans/qa/notes/sweep-1026/arm_sweep_fixes.py`, which is
the arming artifact for that PR rather than a description of one.

### 25c. Ask where the defence actually lives before believing the test named after it

`FileUploadService.save_file` contains **no path handling whatsoever**. The upload path's only
traversal defence is `filter="data"` on `tar.extractall` in `extract_for_dlt`, one call away and in
a different process. Removing it left `tests/test_security` at **424 passed, 0 red** — the class
named `TestFileUploadPathTraversal` could not see the removal of the one thing that stops an escape.

A test named for a mechanism is a claim about where that mechanism is. Check the claim.

## 26. An absence assertion resolves on its FIRST poll

**(2026-09-03, [core#1008].)** `expect(locator).toHaveCount(0)` — and `toBeHidden()`,
`.not.toBeVisible()` — is satisfied immediately by a page that has not rendered yet. Placed before
the load it is about, it cannot distinguish *"the control is correctly absent"* from *"the table has
not arrived"*.

This is not a flake and it does not depend on how fast staging is: there is **no** timing window in
which such an assertion catches a broken gate. Measured against a page that renders the controls
1.2 s after hydration — a viewer gate that has broken:

| gate | order | verdict |
|---|---|---|
| BROKEN | absence first | **PASS** |
| BROKEN | positive artifact, then absence | FAIL |
| GOOD | absence first | PASS |
| GOOD | positive artifact, then absence | PASS |

The bottom two rows are the false-positive control; without them an order that merely failed more
often would score identically on the top two.
`e2e/scripts/probe-1008-absence-ordering.mjs`.

**Rule: wait for the artifact that would be present had the thing happened, then assert the
absence.** It is §1's *"wait on a positive signal"* applied to assertions rather than to waits, and
it bites hardest on security specs, because least-privilege assertions are assertions about absence.

Guarded class-wide by `tests/test_deploy/test_e2e_absence_assertion_ordering.py`, which derives the
rule from `e2e/tests/*.spec.ts` rather than from the one spec somebody already debugged (cf. §23).
The `// absence-ok: <reason>` opt-out exists because some absences *are* ready at hydration —
`rx.cond(AuthState.can_edit, ...)` renders with the page — and the reason is required so the opt-out
is a sentence somebody writes rather than a pragma somebody pastes.

## 27. Two tests flaking in one file is a fact about the FILE

**(2026-09-03, [core#927].)** `template-prefill.spec.ts` flaked at 2 of 18 `dev` pushes in two
different tests, which invites a search for a shared cause *inside the feature* — the `?template=`
param, the prefill state, the Plausible hook. It was none of those.

The shared property was structural: it is the **only** spec in the suite that uses the
`loggedInPage` fixture, and it was the only gating spec that then navigated with a bare `page.goto`
instead of `gotoReady`. One flake was in the spec body (a `click()` waiting out the whole 60 s
timeout for a locator a bare `goto` had not waited for); the other was **in the fixture**, at
`auth.ts`, where the login `toHaveURL` used Playwright's default while `signUp` retries that exact
case and `loginAsViewer` allows 15 s. The asymmetry between the three login paths was accidental,
not reasoned.

**Rules:**

1. **Read the traceback's FILE, not just the failing test's name.** One of these two failures was
   not in the spec at all, and triaging it as a template-prefill problem would have found nothing.
2. **Ask what this file does that its siblings do not** — `grep -c` across the suite answers it in
   one command, and here it named both the fixture and the navigation helper.
3. **The discriminator against "the environment was down" is usually free and already in the log:**
   the *other* test in the same file passed, against the same deploy, minutes apart. An unhealthy box
   fails both.

## 28. A job can misclassify its own failure, and say so confidently

**(2026-09-03, [core#1029].)** `e2e-staging` printed `verdict: gating_failed` and alerted *"the
gating specs themselves went red — not that some other step failed"* on a run where `globalSetup`
died on a seed FK violation and **zero specs executed**. There was no Playwright tally anywhere in
the log, and `detect_flaky_gating.py` had printed `GATING_FLAKY_STATUS=no-evidence` two steps
earlier. The classifier branched on the step's exit status and never read the tally.

A red that says the wrong thing is worse than a red that says nothing, because it is *actionable in
the wrong direction*: it aims a responder at twelve specs when the answer is a teardown-order drift.

The correct behaviour already existed **one job over** in the same workflow — `e2e-sso` distinguishes
*no verdict* from *specs went red*, fires the no-verdict alert and declines to file a spec-failure
issue. **When a job classifies badly, diff it against its siblings before designing anything.**

[core#719]: https://github.com/datanika-io/datanika-core/issues/719
[core#927]: https://github.com/datanika-io/datanika-core/issues/927
[core#1008]: https://github.com/datanika-io/datanika-core/issues/1008
[core#1026]: https://github.com/datanika-io/datanika-core/issues/1026
[core#1029]: https://github.com/datanika-io/datanika-core/issues/1029
