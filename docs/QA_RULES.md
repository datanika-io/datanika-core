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
| A correct assertion about unreachable code | `has_permission("admin", "manage_members") is False` passes, and `has_permission` has zero production callers |

**Corollary — the deferral trap.** A note once claimed a gap was covered "by the nightly connector
smoke and the restore check". Both are API-level; neither could catch either of the two P0s that
were then found by a person clicking through production. *Check what the named coverage actually
exercises before accepting it as coverage.*

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
