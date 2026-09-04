# Engineering rules

Rules earned from incidents on this codebase. Every one of them cost a session or a wrong diagnosis
to learn. They are not general engineering advice — each is here because the obvious thing was tried
first and was wrong.

> **Provenance.** Seeded 2026-09-01. Engineering was the only department without a rules file, while
> `INFRA_RULES.md`, `PRODUCT_RULES.md` and `QA_RULES.md` already existed — so an Engineering rule had
> nowhere durable to live and was one `current_state.md` rewrite away from gone (project rules require
> that file be rewritten from scratch every session). Git is the durable home.
>
> **This file is not comprehensive.** It starts with what one session could establish by measurement.
> Add to it when a rule is *earned*, not when it sounds true.
>
> **Nothing here is a work item.** Do not convert a rule into an issue.

---

## 1. A "hung" `git push` is the pre-push hook, until proven otherwise

`core.hooksPath = scripts/hooks`, so **`ls .git/hooks/pre-push` reports ABSENT while the hook is
live**. The obvious check actively misleads. The discriminating check is:

```bash
git config --get core.hooksPath        # -> scripts/hooks
```

🔴 **CORRECTED 2026-09-04 — the scope in the next sentence is no longer true, and a rules file that
contradicts the code is how the `UV_NO_SYNC` trap reached two departments.** Since **core#964** the
core hook runs **`pytest tests/test_deploy/`**, not the whole suite: measured **998 passed, 1
skipped, 2 xfailed in 109s** on 2026-09-04, against the ~15 minutes below. The *mechanism* of this
rule is unchanged and is why it stays — a push that looks hung is still the hook, the transport is
still opened first, and `ls .git/hooks/pre-push` still lies. What changed is only how long to expect
to wait, and therefore how alarming a two-minute silence should be. `datanika-cloud`'s hook is
**unchanged and still runs its full suite** (~70s), so both readings are live, one per repo.

The hook ran ruff plus the **entire pytest suite** before the pack was sent — measured at
**882.91s (14:42) for 4783 passed, 29 skipped, 10 xfailed** on 2026-09-01. Git opens the HTTPS
transport *before* running the hook, so `git-remote-https.exe` sits in the process list for the whole
run. A twelve-minute-old `git-remote-https` is therefore the **expected** appearance of a healthy
push, not evidence of a network stall.

**The wrong turn this rule exists to prevent** (taken 2026-09-01, in full): a push wedged for 12
minutes; its output file was 0 bytes; `git ls-remote` succeeded instantly. That was read as an
authentication or HTTP/2 problem, reasoning that `datanika-core` is public so *reads* are anonymous
while a *push* needs credentials. Both premises were true and the conclusion was still wrong.

Three things each looked like evidence and were not:

| Observation | Why it proved nothing |
|---|---|
| Output file 0 bytes after 12 min | the command ended in `\| tail -25`, which buffers until the stream closes. Expected, not death. |
| `git-remote-https` alive 12 min | the transport opens before the hook runs. Expected, not a stall. |
| A retry "fixed by" `-c http.version=HTTP/1.1` | it returned fast because `origin/dev` had moved and the hook hit its **auto-rebase-and-abort** arm *before* reaching the tests. The flag was irrelevant. |

That third row is the dangerous one: a coincidental fast return arrived immediately after a plausible
fix and would have been recorded as the cause. **Before crediting a fix, name the mechanism and check
the log says what the mechanism predicts.** Here the log said `pre-push: branch is behind origin/dev
— rebasing...`, which HTTP/1.1 does not explain.

**Corollary — the hook aborts rather than pushes when `dev` moved.** It rebases you and exits 1 with
`Push again (--force-with-lease if already pushed)`. That is by design and is not an error.

**Corollary — docs-only pushes are cheap.** `scripts/hooks/should-run-tests.sh` fails closed but
treats `*.md`, `docs/*` and images as inert, so a documentation change skips the suite and pushes in
seconds. It tests code extensions *first*, so `docs/conf.py` correctly still runs.

> 🚨 **And "inert" is decided by extension, which is wrong for at least one file.** Measured
> 2026-09-04 while shipping core#1076: a commit changing **only `.gitattributes`** reported
> `pre-push: pytest SKIPPED — every changed file is documentation or an image (checked 1 files)`.
> `.gitattributes` is not documentation — it decides the **working-tree bytes** of every file it
> matches, and that very commit changed five files' contents on disk. It is close to the *last*
> file that should skip the suite. Raised on core#1076; until it is fixed, **run the suite by hand
> when your diff touches `.gitattributes`, `.gitignore` or anything else that changes what other
> files are.**

---

## 2. Auto-merge does not update a `BEHIND` branch

> 🔴 **SUPERSEDED ON `datanika-core` AND `datanika-landing` (2026-09-03, core#904) — still live on
> `datanika-cloud`.** Both public repos now have a **merge queue** on `dev`: `gh pr merge <n> --auto`
> with **no method flag**, and the queue rebases the entry onto current `dev` before testing it, so
> `BEHIND` never arises. Measured 2026-09-04: PRs #1074, #1077 and #1078 each read green, enqueued,
> and merged with **no rebase treadmill at all**.
>
> **The finding below is still true and still applies to `datanika-cloud`**, which is a private repo
> on a free org and can therefore never have a queue (the rulesets API answers `403 Upgrade to
> GitHub Pro`). Keep reading it for cloud; ignore its instructions on core and landing.

`dev` is protected with `strict = true` (a PR must be rebased onto current `dev` to merge). Arming
auto-merge on a PR that is already `BEHIND` is **a no-op**: nothing wakes it up, and it will sit
indefinitely while every check reads green.

Measured 2026-09-01: PRs #902, #903 and #894 sat `BEHIND` with auto-merge armed for **40+ minutes**
across three `dev` movements and none self-updated — with `allow_update_branch: true` on the
repository, which governs whether the *button* is offered, not whether auto-merge presses it.

Auto-merge itself is **not** unreliable, and saying so was itself a wrong read. Once #899 was rebased
onto current `dev` and its required checks completed, it **auto-merged in about 15 seconds**.

> **Rebase first, then arm.** Or expect to merge by hand.

**Corollary — `UNSTABLE` does not block auto-merge.** #899 merged while `image-cve` was **failing**.
Only the five *required* contexts gate: `lint`, `test`, `helm-lint`, `migration-roundtrip`,
`image-probe`. `image-cve` and `oracle-smoke` are not required. Read the required list from the API
before concluding a red check is blocking anything:

```bash
gh api repos/datanika-io/datanika-core/branches/dev/protection \
  --jq '.required_status_checks | {strict, contexts}'
```

---

## 3. Never key a watch on a SHA you are about to rewrite

A background monitor was armed to exit when the remote branch reached `4035734` — the local HEAD at
arming time. The pre-push hook then rebased that branch, and `4035734` became unreachable. The push
succeeded; the watch would have reported "still not landed" for all 40 of its iterations.

**The success condition was invalidated by the watcher's own next action**, and its failure mode was
to report the true outcome's exact opposite, indefinitely, in a confident tone.

Key a watch on a **state** — PR `MERGED`, checks complete, a ref *differing* from a recorded value —
never on a specific SHA in a workflow that rebases. This repo rebases on essentially every push.

---

## 4. Serialize your own PRs deliberately under `strict = true`

> 🔴 **SUPERSEDED on core and landing by the same merge queue as rule 2** — the queue serialises
> entries for you and rebases each one before testing, so holding two PRs costs nothing and the
> livelock this rule exists to break cannot form. **Still live on `datanika-cloud`.**
>
> ⚠️ **Its last corollary is NOT superseded and is the most valuable part of the section**: after
> *any* rebase — server-side, queue-side or local — the checks API answers for the **old head** for a
> few seconds, and the stale answer is the reassuring one. That is true under a queue too.

With five departments merging into one protected `dev`, every merge makes every other open PR
`BEHIND`, and clearing that costs a **~15-minute local suite** per attempt (rule 1). `dev` moved
three times in 40 minutes on 2026-09-01 (`b819168` → `d4a49ff` → `dd4e9ad`).

So when you hold two PRs, rebase the second **after** the first merges, not before — otherwise the
first merge invalidates the second's rebase and you pay the suite twice. This is reasoning about
observed cadence, not a measured comparison of both strategies; the cadence is the part that was
measured.

**Corollary — break the livelock with a server-side rebase, not another local suite.** Measured the
same day: #902's local suite ran 1066s (17:45) and `dev` moved *during it* (QA's #901 landed three
commits), so the branch was `BEHIND` the moment it finished. Paying another 17 minutes invites the
same loss. Instead:

```bash
gh api -X PUT repos/datanika-io/datanika-core/pulls/<N>/update-branch -f update_method=rebase
```

This replays the branch onto current `dev` **on GitHub, in seconds**, and CI — the actual required
gate — then runs on the result. It is sound precisely when the content is unchanged and has already
passed the hook once: you are re-basing tested code, not shipping untested code. Do **not** reach for
it to skip the suite on a branch whose content you have edited; that is what the hook is for.

⚠️ `update_method` defaults to **`merge`**, which puts a merge commit on your feature branch. Pass
`rebase` explicitly — this repo keeps feature branches linear and merges them with `--rebase`.

**Corollary — after a rebase, the checks API answers for the *old* head for a few seconds.**
Observed at `10:12:00Z` on #906: `gh pr view --json statusCheckRollup` reported all five required
checks `SUCCESS` immediately after a server-side rebase had replaced the head. Those conclusions
belonged to the **previous** SHA; the new one had no runs yet. `mergeStateStatus` was `UNKNOWN`, not
`BEHIND`, so the stale-ness was not visible there either.

GitHub refused the resulting merge (`add the --auto flag … or --admin`), which is the only reason it
was safe. **An automation that passes `--admin` would have merged a tree no check had run against**,
and it would have looked like a green merge. So:

- Gate a merge on `mergeStateStatus == CLEAN` (or `UNSTABLE`) — never on the rollup alone.
- Treat `UNKNOWN` as *pending*, never as *ready*: it means GitHub is still computing.
- Re-read the rollup **after** confirming the head SHA is the one you expect.

This is rule 3 wearing different clothes: there, a watch was keyed to a SHA that got rewritten; here,
a *verdict* was keyed to one. Both times the stale answer was the reassuring one.

---

## 5. A metric *name* in scrape output is not evidence the metric has data

**Measured 2026-09-01 ([core#907]).** `prometheus_client` emits the `# HELP` and `# TYPE` lines for a
labelled metric with **zero children**, and no samples:

```
# HELP probe_labelled_total labelled, never incremented
# TYPE probe_labelled_total counter
                                     <- no sample line at all
```

So `curl -sf .../metrics | grep -E "bytes_processed"` returns **non-empty** for a counter that has
never recorded anything. A verification step written that way cannot fail.

**Why this is worse than the bug it hides.** The V2 P4 runbook gates the pricing cutover on exactly
that grep, and `/metrics` currently falls through to the SPA (`curl -sf` succeeds on 5 KB of HTML).
The obvious repair — add the Apache vhost entry — flips the check from **unpassable to unfailable**,
and the second is more dangerous, because it reads as the repair having worked.

**Rules:**
1. **Assert a sample line, never a metric name.**
   `grep -E '^datanika_cloud_bytes_processed_total\{org_id="[0-9]+"\} [0-9]'`
2. **An *unlabelled* metric behaves differently and is useful for exactly this** — it emits `x 0.0`,
   so zero is distinguishable from absent. That is why a collector's health flag should be an
   unlabelled gauge: with labels, "collector broken" and "no tenants yet" produce identical output.
3. `CounterMetricFamily("x_total", …)` and `CounterMetricFamily("x", …)` **both** emit `x_total`.
   Verify naming against the library before relying on it; a wrong guess publishes the series under
   a name nothing queries, silently.

## 6. A counter incremented in Celery cannot be served by the app

`/metrics` is a Starlette route in the **app** process. `run.*_completed` is announced from
`datanika/tasks/*.py` — the **Celery worker**, a different container. A `prometheus_client` counter
is process-local, so an increment in the worker is invisible to the app's registry, permanently,
with nothing to indicate it.

This has now happened twice: `celery_tasks_total` ([core#704]) returned **0 series for the life of
the project** and made `celery-task-failures` structurally unable to fire; [core#907] was the same
design written into a spec for the billing surface, caught before the increment was added.

🚨 **#704's remedy does not generalise, and reaching for it is the trap.** It reads the **broker
event stream** with a dedicated exporter, which works because the thing being measured *is* task
lifecycle — the only thing that stream carries. It carries nothing about how many bytes an org
processed. Nor does a shared `PROMETHEUS_MULTIPROC_DIR` help: separate containers, and the worker's
default **prefork pool runs task code in forked children**, so a metrics server inside the worker
would serve the parent's registry — the same bug one level down.

**The rule: when the value is already in durable shared state, derive the metric from that state at
scrape time instead of accumulating it in RAM.** A collector over `usage_ledger` is correct in
whichever process serves the scrape, cannot drift from the record it is derived from, and survives a
restart — where a counter resets to zero, on a box that deploys many times a day.

**Before adding any counter, ask which process increments it and which process serves `/metrics`.**
If they differ, the counter is decoration.

## 7. A negative control that exercises only the path you already believe in proves nothing

Twice in one session, both in *our own* instruments.

**[core#887].** An audit resolved "does a page render this state's `error_message`?" by matching
receiver **names**, and reported ten unrendered classes. Four were rendered — through
`error_or_quota_callout(state_cls)`, a shared component that renders the var for whatever class it
is handed, so a page renders it while containing the string `error_message` zero times. The audit
carried a stated negative control ("the five classes we know are rendered must come back rendered")
and it **passed**, because all five are *also* rendered directly. A control that never exercises the
indirect path agrees with the check including where the check is wrong.

**[core#830].** A probe claimed to drive five distinct `SamlValidationError` raise sites. Its
"unparseable" case used base64 of `<not-a-saml-response/>`, which python3-saml parses happily and
rejects as `Unsupported SAML version` — the *validation-failed* branch, not the *could-not-process*
one. Four sites, not five.

🔑 **And the headline assertion passed throughout.** `test_each_refusal_logs_a_different_reason`
checks that the reasons are **pairwise distinct**, and they were — the two validation-failed messages
carried different library text. **A difference assertion cannot tell you *which* things differed**,
so it was satisfied for a reason unrelated to the property it exists to check. Only a second test
pinning each expected reason fragment caught it.

**Rules:**
1. **A control must include the shape that would break the rule, expressed the way the real code
   expresses it** — not the shape you had in mind when you wrote the check.
2. **Beside any "these are all different" assertion, pin *what each one is*.** Distinctness is cheap
   to satisfy accidentally.
3. **Ask the library, do not guess twice.** The SAML case was settled in one 20-line probe that fed
   five candidate inputs to `OneLogin_Saml2_Auth` and printed which threw and which failed
   validation.

## 8. Two mechanical traps in mutation harnesses on this machine

Every fix worth trusting here is proved by re-breaking the shipped code and watching the named test
go red. Two things break the harness itself:

1. 🚨 **`subprocess.run([".venv/Scripts/python.exe", …], cwd=ROOT)` fails with `WinError 2`.**
   Windows `CreateProcess` resolves a **relative executable against the parent's cwd**, not against
   `cwd=`. The message is "The system cannot find the file specified", which reads as a missing or
   broken venv. Pass an absolute path: `str(ROOT / ".venv/Scripts/python.exe")`.
2. **Match the file's own line endings, and assert the anchor is unique before writing.** Tracked
   `.py` files here are `i/lf w/crlf` while `i18n/*.json` are `i/lf w/lf` — `git ls-files --eol` is
   the only honest reading. A `\n` anchor silently matches nothing in a CRLF file, and a harness
   that does not check reports its no-op mutation as "the test is fine". Restore in a `finally`,
   round-trip in **binary**, and verify with `git status`, never with the harness's own equality
   check.

## 9. A guard's coverage claim is prose until something enumerates what it covers

**[core#673], 2026-09-02.** `BaseState._check_role` was given session revalidation in #760, and the
module docstring of its test file stated the claim that made one guard sufficient:

> "~20 mutating handlers across 9 state modules already route through it, so one guard covers them
> all"

Every test in that file exercised the guard. **None enumerated the handlers.** Measured: of the
public handlers in `datanika/ui/state/` that reach a `session.commit()`, **25 route through
`_check_role` and 13 do not** — five legitimately (`login`, `signup`, `logout`, and the two password
reset handlers), and **eight were writing to the database for sessions that had ended.**

The guard was correct. The sentence about its reach was not, and the sentence is what everybody read.

**Rules:**
1. **A claim of the form "everything X routes through Y" is a test, not a comment.** Write the
   enumeration as an assertion with an explicit, reasoned allowlist — then the exemptions are
   re-argued when they change instead of inherited.
2. **The allowlist needs its own staleness check.** A renamed handler leaves a dead entry behind,
   quietly available to exempt some future handler that takes the old name.
3. **And it needs a negative control asserting the exempt cases stay exempt.** Otherwise the cheapest
   way to make the coverage test pass is to guard everything — which here would have broken sign-in,
   sign-up and sign-out.

🔑 **Pick a discriminator that is the thing, not a proxy for it.** A name heuristic
(`save`/`delete`/`update`/`add`) over the same package returned 20 candidates of which **12 were
false positives**: `existing.add(name)` on a Python set, `add_model()` appending to a form list, an
`@rx.var` named `grants_write`. `session.commit()` returned 38 with **none**. A commit is the write;
a verb in a method name is a guess about intent, and this codebase has already been burned by an
over-reported count ([core#887]: ten offenders, two real).

## 10. Reflex substitutes an invalid icon silently, so "the component constructs" proves nothing

An unknown lucide tag does **not** raise. Reflex prints

```
Warning: Invalid icon tag: arrow_up_circle. ... Using 'circle_help' icon instead.
```

to **stdout** and renders a question mark. In a container nothing is listening, so the only signal
this defect has ever produced goes nowhere. `quota_callout.py` shipped a help icon beside "Upgrade"
— the callout shown at the moment of highest purchase intent — for as long as it had existed.

⚠️ **Hyphens are not the rule, and assuming they are sends you the wrong way.** `triangle-alert` and
`triangle_alert` are *both* accepted. `help-circle` is rejected while `circle_help` is accepted,
because lucide **renamed** the icon. The rule is "the tag is a current lucide name".

**Rules:**
1. **Resolve the tag through `rx.icon(tag)` and capture stdout.** Probe the validator, never a
   hardcoded list of valid names — a list goes wrong in the silent direction.
2. **`redirect_stdout` is load-bearing and fragile in exactly one way**: if Reflex ever moves to
   `warnings.warn` or a logger, the assertion goes green forever with no code change. Pair it with an
   in-process control that asserts a *known-bad* tag still warns, so the day Reflex changes, the
   control fails instead of the guard silently passing.

## 11. Constructing a Reflex page is cheap; compiling one is not. Do not conflate them.

**[core#701].** The reason the "every page constructs" guard went unbuilt for months was a belief
that it would be slow. Measured on this tree: **58 page factories construct in 3.71 s**, zero raised,
against a full suite of ~940 s. That is 0.4%.

Full `reflex compile` (JS codegen) *is* slow. Building the component tree is a function call. The
class it catches — an invalid prop, or a Var that cannot resolve inside `rx.foreach` — raises at
**app startup**, which means it is otherwise found by a deploy CI has already passed.

**Rule: measure the cost before declining on cost.** The estimate that kept this out of CI was never
timed, and it was wrong by two orders of magnitude.

⚠️ **Discover the factories, do not list them** — a hand-maintained list goes stale silently — and
**assert a floor on the discovered count**, because a `parametrize` over an empty list passes as zero
tests.

## 12. Two more mechanical traps in mutation harnesses

Extending rule 8, both from 2026-09-02.

3. 🚨 **A defective mutation is indistinguishable from a blind control.** Proving `test_unauthenticated
   _entry_points_are_not_guarded` could fail, I inserted `self._check_role` into `logout` — a bare
   **attribute reference**, not a call. The extractor collects `ast.Call` nodes, so it saw nothing and
   the test stayed **GREEN**. The available reading was "the control is blind"; the truth was "the
   mutation guarded nothing", which is what a bare reference does. Re-run with `self._check_role("viewer")`
   and it went red immediately.
   **When a mutation fails to turn a test red, suspect the mutation first** — confirm the mutated
   source actually expresses the defect (here: parse it and print the calls the extractor now sees)
   before concluding anything about the test.
4. **Your own harness's paths are not checked by anything.** `parents[1]` of
   `ui/state/account_state.py` is `ui/`, not `datanika/` — an i18n assertion pointed at a directory
   that does not exist. It surfaced only because the failure named a reason that had to be read; had
   the keys genuinely been missing it would have looked like a legitimate red. Derive a package path
   from the package (`pathlib.Path(datanika.ui.__file__).parent`), never by counting `parents`.

## 13. Wiring up inert config is when the value starts mattering — measure it then

**[core#780], 2026-09-02.** Three instances now, and they are the same defect at three stages:

| stage | instance | what it looked like |
|---|---|---|
| **inert** | [core#772] — cloud hooks subscribed in no worker | metering never ran; `usage_ledger` had one row |
| **inert** | [core#713] — byte columns never seeded | enforcement skipped every row while the flag read `true` |
| 🔴 **wrong** | [core#780] — plan values never applied | Enterprise sold 20, served 5 |

Stage three is the hardest to see, and this project produced it *while fixing stage one*. The fix
wired `plans.max_parallel_runs` into the limiter. The limiter is correct. The column is wrong. **A
correct reader over wrong data is indistinguishable from a correct system** — nothing in the product
reports the discrepancy, exactly as before.

🔴 **And I asserted the data was fine from the migration source.** Two shipped docstrings said the
column was "seeded correctly in April and read by nothing." That was read off
`UPDATE plans SET max_parallel_runs = 20 WHERE slug = 'enterprise-monthly'` — which had matched
**zero rows**, because no migration creates that row and the from-scratch rebuild ran the UPDATE
against an empty table.

**Rules:**
1. **Connecting a value to a consumer is not the end of the job. It is the moment the value starts
   mattering — so measure what it now reads.** "The data was already correct" is a claim about a
   database, and a migration's source is not a database.
2. **A seeding `UPDATE ... WHERE <key> = ...` can match zero rows and succeed.** On any rebuilt
   database, ask whether the row existed at that point in the chain. If the row is created outside
   the migration chain — by a script, by hand, by a vendor sync — the answer is no, and every column
   added since holds its `server_default`.
3. **The tell is an asymmetry you can check from source**: the one row a migration *creates* is the
   one row its later UPDATEs reach. Here `free` was right and all four paid rows were wrong.
4. **Guard it mechanically.** `tests/test_migrations/test_plan_seed_updates_reach_real_rows.py`
   fails when a migration configures a slug no migration creates on a column whose `server_default`
   differs from the intent. Prose about the trap already existed in `CLAUDE.md` for [core#713] and
   did not stop the second instance.

## 14. A parser that accepts only your convention silently skips whatever does not follow it

**[core#780], 2026-09-02.** `tests/test_migrations/test_migration_coverage.py` matched
`^revision: str = "..."` — this repo's type-annotated convention. **One migration of 37**
(`e5f6a7b8c9d0_add_max_api_keys_to_plans`) writes the bare `revision = "..."` that
`alembic revision` emits by default.

That file was invisible to **both** graph checks. It could not have been reported as a duplicate id,
and it could not have been part of a detected two-head split — which is precisely what those two
tests exist to catch, and what `core#393` had already cost a red CI job.

🔑 **And the guard was green.** `test_exactly_one_head` passed **by luck**: the invisible migration
happened to *be* the head, so the visible graph's only head was its parent. Adding a correctly-chained
migration after it produced two visible heads and a red test — **the new migration looked like the
defect, and it was the thing that exposed one.**

The discriminator was asking the real consumer: `alembic heads` reported a single head throughout.

**Rules:**
1. **When a text parser feeds an assertion, assert that the parser read everything.** A count of
   parsed items against a count of files is one line, and it converts a silent skip into a failure.
   This is the same shape as an extractor floor, applied per-file instead of in aggregate.
2. **Accept every spelling the generating tool produces**, not just the one your codebase happens to
   use. Alembic emits the unannotated form; a house style that annotates does not stop that.
3. **A newly-red guard is not automatically your diff's fault.** Ask the authoritative tool
   (`alembic heads`, the DB, the registry) before changing your change. Here the correct response was
   to fix the guard, and the tempting one was to re-chain a migration that was already right.


## 15. Asserting a flag is `False` proves nothing unless your test made it `True`

**[core#626], 2026-09-03.** `test_clickhouse_secure_does_not_carry_into_mongodb` asserted
`stub.form_mongodb_tls is False` after a type switch. It never set that var to `True`. So the
assertion compared a default against itself, and **deleting the reset lines from `set_form_type`
left the test green** — found only because the negative control for that criterion was run against
the real file and came back **blind**.

It reads exactly like a test of the carry-over bug. It is a test of a constant.

**Rules:**
1. **Every "was it reset / cleared / disabled?" assertion needs the test to have put the thing in the
   opposite state first.** Otherwise it is a "for all" over a value that was never in the failing
   state, and no mutation of the code under test can reach it.
2. **Keep such a line if it is a genuine control — but label it as one.** Here `form_mongodb_tls`
   *must* stay `False` because D5.1 gave the two connectors separate vars, so it is worth asserting;
   it is simply not the assertion the test's name promises. The load-bearing one was `form_secure`,
   which the test did set.
3. **This is only detectable by mutation, not by review.** The test passes, reads correctly, and its
   name describes the right behaviour. Run the control.

Same family as the guards in this repo that could only ever return one answer — but arriving from a
new direction: here the *check* is fine and the *fixture* never armed it.

## 16. A guard's invariant outranks a spec's incidental wording — say so, don't quietly pick one

**[core#626], 2026-09-03.** `SPEC_MONGODB_TLS_SRV` D2 says that under a DNS seed list no port is
written "into the URI **or the config**". Implementing the second half made
`test_connection_config_roundtrip.py` fail on `mongodb: ['port']` — the ratchet from [core#638],
whose invariant is *every key declared in `CONFIG_SCHEMAS` survives a structured-form save*.

Three responses were available and two were wrong:

| response | why it fails |
|---|---|
| add `mongodb: {"port"}` to `_DROPPED_ON_SAVE` | the ledger is pre-existing debt tracked by [core#662]. A new entry converts a ratchet into a permanent exemption, and asserts something false — `port` is not dropped in the ordinary case |
| add `mongodb` to `_UNPROBEABLE` | stops probing the connector entirely, losing the coverage of `auth_source`/`tls`/`srv` that the same change just bought |
| **keep the port in the config, keep it out of the URI** | ✅ |

The spec's *purpose* — no port in an SRV URI — is met structurally by `build_connection_uri`
composing the authority as `host` alone when `srv` is set. The config clause bought nothing and cost
two things: the guard's invariant, and a port the user typed (tick SRV, save, untick: gone), which is
the same silent-loss shape [core#638] exists to catch.

**Rules:**
1. **When a spec clause collides with a shipped guard, work out what the clause is *for*.** If the
   purpose is satisfied elsewhere and more robustly, implement the purpose.
2. **Record the deviation on the issue and in the code**, with the reasoning. A deviation nobody
   wrote down is indistinguishable from an implementation error, and the next reader "fixes" it back.
3. **Never widen a ratchet to fit your change.** A ledger that only ever grows is a list of things
   nobody will ever fix.

## 17. Adding an indirection is adding an idiom — teach the key scanner in the same commit

**[core#872], 2026-09-03.** `tests/test_i18n/test_i18n.py` derives "which translation keys does the
code use?" from three regexes: `_t["..."]`, `_deleted_toast("...")`, `_translated("...")`. Adding
`BaseState._saved_toast` made **all thirteen new keys read as orphans**, and
`test_no_orphan_keys_in_json` says so — while the documented remedy for a false orphan is to
**delete the key**, which would have dropped the translation in all nine locales with every check
green.

🔑 **The file had already been bitten twice and said so in a comment**, once for `_deleted_toast` and
once for `_translated`: *"a key-usage scanner is only as wide as the idioms it knows... Add the
pattern when you add the helper."* This was the third instance. A warning written in the place it
will be read is worth more than one written where it was learned.

**A second, narrower trap in the same scanner:** the pattern captures **one** literal per call, so

```python
yield await self._saved_toast("x.saved" if editing else "x.created", ...)   # second key invisible
```

hides the `else` branch. Two call sites needed create/update discrimination and now use explicit
`if`/`else` branches instead — more lines, and visible to the tooling.

**Rules:**
1. **A new helper that takes a translation key is a scanner change.** Same commit, or the keys are
   orphans and the fix-as-documented destroys them.
2. **Do not compute a scanned literal.** Ternaries, f-strings and locals are all invisible to a
   regex-based key scanner; branch instead.
3. **When you get bitten by a derivation's blind spot, write the warning next to the derivation.**

---

## 18. Build the unit the way the app builds it, or your suite is a second implementation

**core#1035, 2026-09-04.** The issue specified an `endpoint → template` index *"built in
`PrometheusMiddleware.__init__` off `app.routes`"*. Production installs that middleware with
`app._api.add_middleware(PrometheusMiddleware)`, and Starlette's stack is
`ServerErrorMiddleware → user middleware → ExceptionMiddleware → Router`. So `__init__` is handed an
**`ExceptionMiddleware`, which has no `.routes`**:

```
AFTER stack build: ExceptionMiddleware has .routes = False len = 0
wrapped.app -> Router          has .routes = True  len = 2
```

The whole test file constructed `PrometheusMiddleware(Starlette(routes=ROUTES))` **directly**, and a
`Starlette` *does* have `.routes`. So the specified implementation is **empty in production and full
in every test.** Mutating the shipped file back to it gives **2 failed, 19 passed** — and the 19
include every acceptance criterion the issue lists.

> **A harness that constructs the unit differently from the way it is installed is not a harness, it
> is a second implementation — and it is the one your assertions describe.**

**Rules:**
1. **Write one test that builds the object the way production builds it**, even when it is clumsier.
   Here that is a `_production_shaped_app()` helper doing `add_middleware` + a real request.
2. **Ask what the framework hands your constructor**, not what you passed to the thing you think you
   wrapped. One `print(type(app).__name__)` answered this.
3. **Walk, don't assume.** Following `.app` for a bounded depth covers both shapes with one rule,
   instead of making the harness lie about the install.
4. Same family as rule 7's negative-control rule, one level out: there the *control* only exercised
   the believed path; here the *constructor* did.

⚠️ **A second, cheaper instance from the same afternoon:** every route in that file's `ROUTES` shared
one `_endpoint` function, so an endpoint-keyed index is *always* ambiguous there. The new code path
would have been unreachable from every test while working perfectly in production. **When a fixture
collapses a dimension the code under test keys on, the fixture is the bug.** The real table is 83
routes on 83 distinct endpoints; the test table now matches it.

---

## 19. A guard's failure message must name what to read and *where*

**cloud#177, 2026-09-04.** `test_the_charge_loop_does_not_meter_model_runs_at_all` correctly goes red
on the one-token edit that would start billing model runs. Its message says:

> *"confirm every plan's `overage_run_price_cents` is 0, and then update this assertion"*

**Every source an engineer can reach from the repo already said 0** — the model's Python default, the
seeder's `PUBLISHED_ENTITLEMENTS`, both `e2e_admin` branches, the AST guard requiring every
production `Plan(...)` to state a price. The one place that said `1` was **production**.

So an honest reader satisfies the instruction completely, in good faith, from the repo, and ships the
charge. The guard is right in shape and points at a **proxy**.

> **If the property is only knowable from the running system, the message must say so and give the
> command.** *"Confirm X is true"* is an invitation to confirm it wherever it is cheapest to look.

**Rules:**
1. **Name the artifact, not the property**: *"read it off the serving container with `docker exec …`,
   not off this repo"*.
2. **Name who can read it** when it is another department's lane. A message that asks for something
   the reader cannot do gets satisfied by something they can.
3. This is the mirror of rule 15 (*asserting a flag is `False` proves nothing unless your test made
   it `True`*): there the test could not distinguish two states, here the *reader* could not.

---

## 20. A check with only one possible answer says nothing — in either direction

Two instances the same day, failing opposite ways.

**A permanent red (core#1060).** `seed_paid_plans.py --apply` on a fresh build seeds every row it
owns **correctly** and exits **1** — and so does every run after it, for ever, because two annual
rows are legitimately absent and the script deliberately refuses to create them. Wired into a drill
as *"non-zero exit = finding"*, that is a red that is never a finding, and a permanent red trains its
reader to ignore the report. Fixed with a flag that excuses **only** the one condition the caller
knows it cannot satisfy — and pinned in both directions, because a flag that excuses everything is
the same defect wearing the opposite sign.

**A permanent green, locally impossible (core#1076).** A guard byte-compared two checked-in files
that are the **same git object**. `.gitattributes` gave one directory `text eol=lf` and not its twin,
`core.autocrlf` is `true` on the dev machines, so git materialised one CRLF and one LF. **Red on
every Windows push, and structurally unable to fail in Linux CI.** Since the pre-push hook runs
`tests/test_deploy/`, it blocked every department, and `--no-verify` is forbidden.

**Rules:**
1. **Before shipping a check, ask what its two answers are and construct both.** If you cannot make
   it say the other thing, it is not a check.
2. **Ask what it says on a correct system, forever.** A check whose steady state is red is a check
   nobody reads by week two.
3. 🚨 **Anything added under `tests/test_deploy/` gates every department's push.** Treat that
   directory as production tooling, not as tests.
4. **`git ls-files --eol` is the only honest reading of line endings here.** MSYS `sed`, `od` and
   `cat -A` normalise CRLF in flight and will tell you two files agree when the bytes do not.

---

## 21. Migrations: derive the target set, state the criterion both ways, and mean the backfill

**core#1069, 2026-09-04**, release N of a chain over seven tables.

1. **Derive the set you are fixing from the system, not from the issue.** The seven drifted tables
   are re-read from `information_schema` at the parent revision by the test, so a list retyped into
   the migration cannot silently go stale — a retyped list is what produced the defect.
2. **State the criterion in both directions.** Not *"the seven are fixed"* but *"exactly these seven
   before, and **none at all** after"*. The second half is what the *next* drifted table fails.
3. 🔑 **Check whether the model or the database is wrong before changing either.** The inherited plan
   was to relax `TimestampMixin` to `Mapped[datetime | None]`. Measured: **17 of 22** tables already
   agreed with the mixin, so that would have made 17 correct tables wrong to accommodate seven
   sloppy `create_table`s — and would have addressed one of **three** independent disagreements.
   Two tables get all three right, which is what proves the declaration achievable.
   **The majority is evidence about which side drifted.**
4. **Backfill from the row's own history, not from `now()`** — and decide by asking what the column
   *means*. `now()` asserts every untouched row was modified at migration time. ⚠️ The opposite
   conclusion was right in core#726, where backfilling `password_changed_at` from `created_at` moved
   a **session-revocation baseline backwards** and failed open. `updated_at` is read at seven API
   serialization sites and by nothing that makes an authorization decision; that difference, not a
   general preference, is the whole argument.
5. **Assert the contract half ABSENT.** `SET NOT NULL` and a type change are release N+1, and the
   AST policy guard cannot see them when they arrive as `op.execute`. A behavioural test reading the
   catalogue after the migration is the only mechanical thing between a correct chain and a
   two-releases-in-one deploy. **Show it red by smuggling the N+1 statement in.**

---

## 22. Read the jobs, not the run — a stale run-level read is indistinguishable from a running job

**2026-09-04.** `GET actions/runs/<id>` answered `status: in_progress, conclusion: null,
updated_at: 19:39:07Z` on **three** reads spread over ~15 minutes, for a deploy that had completed
`success` at **19:47:24Z**. `actions/runs/<id>/jobs` was correct throughout. On that basis I told a
live issue that a production deploy was still in flight, and used it to justify a hold.

QA's existing rule says *a run-level conclusion cannot answer a question about one job*. This is its
second edge: **it cannot reliably answer a question about the run either.**

**Rules:**
1. **Ask the jobs endpoint** for anything you are about to write down or act on.
2. **`updated_at` is the tell** — if it is older than the thing you are asking about, you are holding
   a snapshot.
3. **When a read you published turns out stale, correct it with a new comment, not by editing the
   old one.** The wrong reading and the reason it looked right are the reusable part.

---

## 23. `master` is not production, and a promoted cloud tree is not a deployed one

**2026-09-04.** Cloud promotion #191 merged at **20:29:40Z**; core promotion #1070 at **19:39:01Z**.
`deploy-pointer.yml` checks the cloud repo out at a pinned `ref: master` and tars both trees to the
box, so **a promoted cloud tree reaches production only on the next core `master` push.** Cloud `dev`
and `master` read `identical`, every check was green, and the change had not shipped.

Concretely, wiring a drill to a new cloud CLI flag that day would have got
`error: unrecognized arguments`, which argparse exits **2** for — and in a drill that treats non-zero
as a finding, *"the flag does not exist yet"* and *"the seed found drift"* are the same colour.

> **Gate on the artifact, not on the branch.**
> ```bash
> docker exec <serving-colour> /app/.venv/bin/python /cloud/scripts/<script>.py --help | grep -c <flag>
> ```

Same family as *ask the running artifact, not the manifest* (core#646) and *a scheduled workflow runs
the default branch's copy of itself* — in all three, a branch state was read as a deployment state.

🆕 **The `--help` form of that check is fragile and the fix is one line.** A parser built with
`description=__doc__` prints the module docstring, and ours carry emoji: on a non-UTF-8 stdout
`--help` **raises part-way through printing**, and a truncated help text greps to `0` — *the same
answer as a copy that genuinely lacks the flag.* Keep `--help` ASCII, or better, give the script a
machine-readable capability probe (rule 26).

---

## 24. Ask what your instrument EXCLUDES before reporting what it found

**2026-09-05, core#1069 / cloud#195.** Re-deriving how many tables agree with `TimestampMixin`, I ran
cloud's shipped AST nullability scanner over every model table and got:

```
agree: 0    DISAGREE: 3    not visible in the migrations: 23
```

*"Twenty-three core tables whose migrations never state nullability"* is a striking finding, and it
is **entirely an artifact of the instrument**: `_scan_core_nullability` carries
`if table not in _CLOUD_TABLES: continue`. Widened deliberately, the real answer is **18 agree,
7 disagree, 1 invisible** — and the invisible one is a genuine blind spot worth its own issue
(`uploads` is created by `rename_table`, which that scanner does not follow while its sibling in the
same file does).

> **A scope filter and an empty result are the same output. Read the predicate's exclusions before
> you read its findings — especially when you are borrowing somebody else's scanner.**

**Rules:**
1. **Print the scope with the result.** *"scanner scope widened: 3 → 30 tables"* on its own line is
   what made the second run interpretable.
2. **A borrowed instrument carries its owner's assumptions.** Reusing a guard's extractor is right —
   it stops your number disagreeing with the guard for a reason of your own making — but its
   *filters* were written for the guard's question, not yours.
3. **When two scanners read the same tree, assert they see the same set.** That assertion is what
   would have caught cloud#195, and it is one line.

---

## 25. A check whose input can be empty is vacuous — put a floor on the count

**2026-09-05, found in my own tooling, which is where this class always is.** A merge gate:

```bash
RUNS=$(gh api ".../check-runs" -q '...')
printf '%s\n' "$RUNS" | grep -qv '=completed:success' && { echo "not green"; exit 1; }
```

With **no checks at all** — a head whose runs have not been created yet, or an API hiccup —
`grep -qv` matches nothing, returns 1, the guard does not fire, and the merge proceeds. *"Nothing is
red"* and *"nothing was measured"* are the same output. The second version counts first:

```bash
N=$(printf '%s\n' "$RUNS" | grep -c '=completed:success')
[ "$N" -ge 2 ] || { echo "fewer than 2 green checks - refusing"; exit 1; }
```

**Rules:**
1. **Every list-shaped gate needs a floor**, and the floor is a *number you derived*, not `>= 1`.
2. **Absence is not a state you may pass through.** `NO-VERDICT` is a third outcome; collapsing it
   onto green is how a dead runner reads as a clean run (`landing#505`), and onto red is how a
   promotion window reads as a defect (rule 23).
3. 🆕 **Re-run every mutation sweep after `ruff format`.** Formatting rewraps string literals and
   moves byte anchors: one anchor in a sweep went `ANCHOR-0` post-format. It reported that loudly
   instead of going green — *only because the harness asserts its anchor matched exactly once.*
   Without that assertion a moved anchor is a mutation that silently never applied, i.e. a
   mutation-testing tool whose own breakage says *"your test is perfect"*.

---

## 26. An exit code cannot say "I did not run" — only a positive artifact can

**2026-09-05, core#1060.** `seed_paid_plans.py` had three conditions sharing one non-zero colour and
needing three different responses, one of which **spends real money** (`seed_annual_plans.py` calls
`client.create_price()` against the live Paddle account). Splitting the codes into a partition —
`0 CLEAN / 1 DRIFT / 3 INCOMPLETE / 4 REFUSED / 5 ERROR`, with `2` reserved for argparse — was
necessary and **not sufficient**, because the hardest case is not a condition at all:

> **A drill must tell *"the script refused"* from *"the script is not there yet"*, and both are
> non-zero. No number distinguishes them, because one of them is the body never having run.**

What does: **every terminating run prints one verdict line, last on stdout, and a run that never
reached the body prints none.** No line = *no verdict*, which is neither clean nor a finding. The
line also carries the whole outcome vector, so the code's precedence cannot hide a second finding.

**Rules:**
1. **Reserve the codes your framework owns.** `argparse` exits `2`; if your body can return `2`,
   *"this copy is older than your invocation"* and *"this copy found a defect"* are one reading.
2. **Give the script a capability probe that opens nothing** — ours prints `SEED-CONTRACT:` lines
   derived from the parser and touches no database, so it answers inside an image with nothing
   reachable. That is rule 23's *"gate on the artifact"* made cheap enough to run every time.
3. 🚨 **An uncaught exception exits `1`.** Whatever `1` means in your partition, a crash now means it
   too — here that was *"re-run with `--apply`"*, i.e. **an instruction to write, against a database
   that was not there.** Wrap `main` and return a distinct code, and render the counts as `?` rather
   than `0`: a zero is a measurement the run never made.
4. **Found by running it, not by reading it.** I designed the whole partition from the source and
   would have shipped it without case 3; the first real subprocess invocation returned
   `exit=1, no verdict line` four times in a row.

---

[core#704]: https://github.com/datanika-io/datanika-core/issues/704
[core#830]: https://github.com/datanika-io/datanika-core/issues/830
[core#887]: https://github.com/datanika-io/datanika-core/issues/887
[core#907]: https://github.com/datanika-io/datanika-core/issues/907
[core#673]: https://github.com/datanika-io/datanika-core/issues/673
[core#701]: https://github.com/datanika-io/datanika-core/issues/701
[core#713]: https://github.com/datanika-io/datanika-core/issues/713
[core#772]: https://github.com/datanika-io/datanika-core/issues/772
[core#780]: https://github.com/datanika-io/datanika-core/issues/780
[core#928]: https://github.com/datanika-io/datanika-core/issues/928
[core#872]: https://github.com/datanika-io/datanika-core/issues/872
[core#626]: https://github.com/datanika-io/datanika-core/issues/626
[core#662]: https://github.com/datanika-io/datanika-core/issues/662
[core#638]: https://github.com/datanika-io/datanika-core/issues/638
