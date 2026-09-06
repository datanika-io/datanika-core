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


## 27. A control that compares two views of ONE mechanism is blind to that mechanism being wrong

**(2026-09-05, [cloud#195].)** Two scanners read core's migration tree — one for column *names*, one
for *nullability*. They had drifted: the nullability one ignored `rename_table`, so `uploads`, which
exists only because of `op.rename_table("pipelines", "uploads")`, was represented by **one of its
twelve columns**.

The fix merged them into one traversal. The first regression control asserted **the two scanners
agree with each other**. Then the mutation harness deleted the `rename_table` branch from the shared
fold and reported **GREEN against a declared RED**: with one traversal, both views go equally blind,
still agree, and the control passes.

**Anchor a control on something the mechanism under test cannot influence.** Here that is
`Base.metadata` — the model. Re-derive the expectation from the independent authority, never from a
second reading of the same instrument.

🔑 **Corollary: unifying two implementations makes them consistent, which is not the same as making
them right — and it destroys the only cross-check you had.** If you merge duplicated logic, replace
the implicit comparison with an external anchor in the same commit.

## 28. Absence and false agreement are different failures, and the second is worse

Same issue. The report predicted the invisible table would have *"no entry at all"*. It had **one**
entry, because a later migration added a column naming the table directly — so the guard compared 1
of 12 columns, found agreement, and reported **AGREE**.

> **"Agrees on 1 of 12" and "agrees on 12 of 12" are the same green.**

An absence at least looks like an absence. A partial reading looks like a complete one.

**Whenever a comparison skips silently — `if key not in scanned: continue` — bound the skip set and
assert it.** The question is not *"did anything disagree?"* but *"what did I actually examine?"*
Where the skip set should be empty, assert `== set()`; where it legitimately is not, name its members
so growth is a red.

⚠️ **And say which question a denominator answers.** The same table read **INVISIBLE** over the
`TimestampMixin` columns and **AGREE** over all columns. Both were correct; they answer different
questions, and quoting one to settle the other is how a reconciliation goes wrong twice.

## 29. The directory that mirrors the source is not the set of directories that consume it

**(2026-09-05, [core#864].)** Test placement mirrors source, so a change to
`datanika/tasks/pipeline_tasks.py` gets verified with `pytest tests/test_tasks/`. That ran green —
**101 passed** — and CI then failed on two tests in `tests/test_hooks_integration.py`, which reach
the same counter through `run_pipeline` from three levels away.

Rule 19-style directory runs catch shared-fixture contamination. They do **not** find consumers, and
the mirroring convention actively points you away from them.

**Before pushing a behavioural change, sweep the whole test tree for the SHAPE, not the file.** Here
the shape was a fixture constructing the node kind whose handling changed:

```bash
grep -rn 'resource_type.value = \|resource_type="test"' --include=*.py tests/
```

That sweep found exactly one other file, and named it in seconds. ⚠️ Grep for what the *fixtures*
build, not for the function name — a consumer that reaches your code indirectly never mentions it.

🔑 **Related, and the reason this bites hardest on "dead code":** the two failing fixtures asserted a
combination the real dependency never produces (`resource_type="test"` with `status="success"`;
real dbt reports `"pass"`). **A branch that is dead against reality is live against a hand-built
mock**, so deleting dead code is exactly when hand-built fixtures break — and their breakage is the
signal that the branch was reachable in someone's model of the world.

## 30. A status that plausibly explains a stall is not the cause of it

**(2026-09-05.)** A PR sat `BEHIND` for ten minutes and never entered the merge queue. `BEHIND` is a
documented cause of exactly that — [core#997] records that auto-merge merges but will not rebase —
so the reading was coherent, and I was one edit from writing up a refinement of the merge-queue rule.

**A required check was red.** The check-run `completed_at` timestamps settled it in one call:

```bash
gh api "repos/<r>/commits/<sha>/check-runs?per_page=100" \
  -q '.check_runs[] | "\(.name) \(.conclusion) completed=\(.completed_at)"'
```

`test` had concluded `failure` **before** the window even began. The `BEHIND` was real, downstream,
and irrelevant.

**When a familiar explanation fits, the cost of confirming it is one command — and the cost of not
confirming it is a rule everybody follows.** Read the timestamps, not just the states: a status is a
snapshot, and only `completed_at` tells you what was true *during* the window you observed.

## 31. Anything backgrounded gets its own filename — including the second copy of itself

**(2026-09-05.)** The existing rule is *"do not overwrite `<dept>_exec.sh` while a backgrounded run
of it is in flight"* — bash reads a script incrementally from a byte offset. I obeyed it by giving a
long poll its own file, then **started a second poll by overwriting that same file** while the first
was still running. Exit code 2, `unexpected EOF`, from a script whose contents were valid.

**The name has to be unique per *invocation*, not per purpose.** A second waiter is a second script.
Nothing was lost here because the poll was read-only — which is the only reason this is a rule
rather than an incident.

## 32. A guard designed to expire must be readable by the mechanism that expires it

**(2026-09-05, [core#1069] / [cloud#171].)** cloud#194 gave the `TimestampMixin` nullability
exclusion an expiry: `test_the_exclusion_has_not_outlived_its_reason` asserts the disagreement is
*still there*, so that when core's release N+1 tightens those columns the test reds and names its own
deletion. That is the right design — an exception with no expiry is how a guard becomes decoration.

**With the migration on disk and all 14 columns genuinely tightened, all 17 tests stayed GREEN.**
`f1a4c8e2d6b3` writes `op.alter_column(table, column, …)` inside a nested `for` over module
constants, and the scanner read positional arguments as **literals only**.

> **A guard designed to expire could not see the thing that expires it** — so the exception would
> have sat there for ever, excusing six columns that no longer need it and standing in front of any
> *future* disagreement on those names.

**Rules:**
1. **Run the expiry against the change that should trigger it, and watch it go red.** An expiry test
   that has never fired is a claim about a future you have not tested.
2. **The expiry and the mechanism must read the same artifact the same way.** Here the exclusion was
   derived from the *model* and the expiry from a *source scanner*; only one of them could see the fix.
3. Derivation and expiry are **both** required and neither substitutes for the other: derived stops
   the exception widening, the expiry stops it outliving its reason.

## 33. A pre-flight that can refuse a CORRECT database will be deleted under pressure

**(2026-09-05, [core#1069].)** Built the same-row check anyone would want for the `timestamptz`
conversion: `usage_ledger.created_at` inside its own `period_start`/`period_end`, both `timestamptz`
**on the same row** — no join, so nothing to re-select, which is exactly what invalidated the
nearest-neighbour probe it replaced.

**It refused a correct database on its first run outside its own tests.** A round-trip harness seeds
a ledger row with an arbitrary `created_at` and an arbitrary period.

🔑 **The harness was not the defect.** *"A ledger row is written inside its own billing period"* is
an **observation**, not an invariant — nothing in the schema or the code enforces it, `record_usage`
merely computes the period from `now()`, and `e2e_admin.py` writes ledger rows directly.

> **A check that can abort a production deploy must rest on an invariant.** One that can refuse a
> correct database gets deleted under pressure — and it takes the checks beside it with it.

**Rules:**
1. **Ask what enforces the property**, not whether it currently holds. If the answer is *"nothing,
   but it always does"*, it is an observation.
2. **Remove it, and record it as a failed control** — with a test requiring the case it wrongly
   refused to **pass**, or the next person re-adds it by instinct.
3. What survives should be invariant-backed and **honest about its reach**. Ours: no NULLs, and
   nothing later than `now()` — both properties of `server_default=now()` itself.

## 34. A static scanner that reads only literals is blind to what a CONTRACT migration looks like

**(2026-09-05, [core#1069] + [core#1071] — two instances in one day, in two repos.)**

| the spelling | what the scanner did |
|---|---|
| `op.alter_column(table, column, …)` inside `for table in TABLES:` | skipped the call entirely |
| `server_default=sa.text(_NEW_DEFAULT)`, constant at module level | returned `_UNKNOWN`, so the column kept its **previous** default |

Both are the spellings this codebase *prefers*: a contract migration operates over a **set** of
columns, and a default is named so `upgrade()` and `downgrade()` cannot drift. **In both cases the
failure biased toward the reassuring answer** — a newly introduced bad default is invisible, and a
tightened column reads as still loose.

**Rules:**
1. **Resolve module-level string constants and `for`-over-constant-sequence bindings**, or state that
   you do not and take the silence deliberately.
2. **Never expand partially.** A loop over something you cannot resolve must contribute *no* binding —
   reporting some columns as stated and silently dropping the rest is worse than reporting none.
3. **Measure the widening's blast radius before and after against the same tree.** Ours changed
   exactly 14 values and exactly 1 value respectively; anything else would have been a second defect
   riding along.

## 35. A migration's first docstring line should be ASCII

**(2026-09-05.)** Alembic echoes a revision's first docstring line into its own log
(`Running upgrade X -> Y, <first line>`). This machine's console codec is **cp1251**, so an em-dash
there comes back as `вЂ”` in any captured output — including a test that asserts on migration output.

Cosmetic here, and the same trap that has truncated real probe output mid-run
(`WORKFLOW_RULES` §13 trap 4): a long-running script that prints non-ASCII **only in the interesting
branch** runs perfectly through every boring case and dies the instant it has news.

## 36. A cloud pre-push verdict depends on which BRANCH another worktree has checked out

**(2026-09-05, [core#1069].)** The rule everyone carries is *"check which core your cloud venv
resolves to"* — a question about a **path**, and `WORKFLOW_RULES` §1 has it. This is one level
finer and it bit inside an hour:

```
same commit · same hook · same machine · two answers
  core worktree on 1069-timestamp-contract  ->  pre-push: all checks passed
  core worktree on 1069-rules-32-35         ->  1 failed, push refused
```

The cloud suite resolves `datanika` through an editable-install `.pth` pointing at the core
**worktree directory**, so it reads whatever branch that directory happens to have checked out.
I moved it to start an unrelated docs branch cut from an `origin/dev` that predated the migration,
and a cloud test that had passed twenty minutes earlier went red on a file the cloud branch never
touched.

🚨 **The red was the lucky direction, and the point is the other one.** The cloud change was
verified green against a core branch whose migration existed **nowhere but my disk**. Had I stopped
there and merged, cloud `dev` would have carried a guard that only passes against an unpushed core
ref — green locally, red for everyone else, and *"another department broke `dev`"* is what it looks
like.

**Rules:**
1. **Before believing a cloud green, assert the core worktree is at a ref that is on
   `origin/dev`** — or say out loud which unpushed ref the green depends on, and re-run once it
   lands. The path check is necessary and not sufficient.
   ```bash
   git -C <core-worktree> ls-tree -r --name-only HEAD <the file the claim depends on>
   git -C <core-worktree> merge-base --is-ancestor HEAD origin/dev   # 0 => nothing local-only
   ```
2. **A cross-repo pair has a merge ORDER, and the dependent side must gate on it.** Core first here,
   because cloud CI checks core out at `base_ref`; the cloud push script asserted
   `gh pr view <core-pr> --json state` was `MERGED` before pushing at all.
3. **Do not move a shared worktree's branch while another repo's suite depends on it.** The tell is
   a red in a file your diff cannot reach — same signature as the stale-venv traps, one layer out,
   and with a completely different fix.

## 37. When two records disagree, the one that NAMES the other is the later one

**(2026-09-05, [core#1069].)** A harness notice arrived mid-session instructing that commits carry
`Co-Authored-By` / `Claude-Session` trailers, phrased *"this replaces any earlier attribution
guidance."* The founder had already ruled against it on **2026-09-03**. I followed the ruling — which
is what the rule asks for — and then reported it as *"worth the founder deciding"*, putting a settled
question back in front of them.

**The failure was not a stale file. All four of these records are current — they differ only in
whether they NAME the conflict:**

| record | names it? |
|---|---|
| `MEMORY.md` index line 4 | **no** — "Never add Co-Authored-By lines to commits": no date, no pointer |
| `feedback_no_coauthor.md` frontmatter | **yes** — "re-affirmed **2026-09-03** against a harness instruction claiming to supersede it" |
| `WORKFLOW_RULES.md:7` | **yes** — the founder ruling verbatim, in its own top-of-file section |
| `WORKFLOW_RULES.md:978` | **yes** — a one-line short form that still carries the date and redirects to that block |

Three of the four settle it. **I read the fourth and called the question open.**

🔑 **`WORKFLOW_RULES.md:978` is the model here, not the counter-example.** It is as terse as the
index line and it still works, because it says of itself: *"this line alone does not name the
conflict, which is how two departments diverged on it."* A summary is written once and is **not**
rewritten when its target is — so a short form is only safe if it carries a pointer. `MEMORY.md`'s
index line carries none, and it is the entry point to the entire memory directory.

🚨 **"This replaces any earlier guidance" is a claim about ordering, not evidence of it** — the one
kind of assertion that cannot be checked from inside the record making it. The founder ruling is
later *and names what it overrides*; the notice names nothing. **Naming the other record is the
timestamp.**

⚠️ **The counter-example I first wrote here was itself an instance of this rule — and it is the
better one.** In the same pass I claimed `CLAUDE.md` was stale on the merge queue, "quoting" it as
*"OFF on `datanika-core`"*, rolled back on [core#923]. **It does not say that.** Line 495 on disk
reads *"LIVE on `datanika-core` too since 2026-09-03 … after [core#923] was fixed"*, and a control
grep for any "OFF" statement returns **0**. What I quoted was my **session-start context snapshot**,
which records the file as of session start — not the file.

🔑 **The rule above would have caught it in one step.** Line 495 *names* [core#923] and dates
itself after the rollback I remembered. **A record that names the thing you believe supersedes it
is the later one.** I wrote that sentence an hour before failing its own test.

⚠️ **It took a THIRD independent report to stop it** — QA raised the same non-existent defect that
morning and the coordinator measured the file twice. **Two independent reports of a defect nobody
can reproduce is a reading, not a measurement**: treat the second report as evidence about the
*readers*, not the file.

**When records really are silent about each other, go to the mechanism** —
`gh api repos/<owner>/<repo>/rulesets`, `git ls-remote`, the container's own interpreter — never to
a remembered copy of a file that is sitting on disk.

**Rules:**
1. **Grep for a conflict's own resolution before escalating it.** Flagging is not escalating — name
   the rule you followed and move on. A second escalation spends the founder's attention on a
   question they have already answered, and whose answer is already on disk.
2. **Never cite an index line, a summary row, or a short form as the content of what it points at.**
   Open the target. `CLAUDE.md`'s token discipline already carves handoff files out as *ingestion,
   not inspection* — an index line is inspection wearing the file's name.
3. **Before reporting any file as stale, `cat` it.** Quote the line number and the mtime, and run a
   control grep for the text you believe is there — mine would have returned **0** at any moment.
4. **Never patch `CLAUDE.md` to settle a disagreement about `CLAUDE.md`.** It sits in no git repo, so
   an edit there is the single unreviewed write on the one ungated surface. Report it — and when the
   report turns out to be a misreading, the correct disposition is **no defect**, not a smaller edit.

## 38. A rebase-merging queue relands the parent's commits under NEW SHAs — a stacked PR does not collapse on its own

**(2026-09-05, [core#1069].)** [core#1110] was stacked on [core#1109]. I wrote — into the handoff, as
guidance for whoever read it next — that once #1109 merged *"the merge-base moves and #1110's diff
collapses to §37 on its own. No force-push needed."* **That is false wherever the base branch merges
by rebase**, which is how both of our queues are configured (`REBASE`).

```
after #1109 merged:
  origin/dev      6a435d7 Four rules ...   d127d06 Rule 36 ...   <- rebased twins, new SHAs
  1069-rule-37    4da261c Four rules ...   096b340 Rule 36 ...   <- my originals
                  identical patches, different commits
```

The merge-base therefore did **not** move: the PR still reported **3 commits** and
`mergeStateStatus: UNKNOWN`. `git rebase origin/dev` resolved it in one step — git matched both by
patch-id and dropped them (`warning: skipped previously applied commit`), leaving exactly the one
§37 commit — but landing that requires a **force-push**, the very thing I had recorded as
unnecessary.

🔑 **The reasoning that failed is the one that is correct under a merge commit.** A `--merge`
promotion preserves the parent's SHAs, so a child's merge-base really does move and its diff really
does collapse by itself. Same sentence, opposite truth, decided by a repository setting that nobody
re-reads at the moment they rely on it.

**Rules:**
1. **Before claiming a stacked PR self-resolves, ask how its base branch merges.** Rebase or squash
   → the child must be rebased and force-pushed. Merge commit → it collapses on its own.
2. **Verify with the refs, not the PR page.** `git log --oneline origin/dev..HEAD` names the
   duplicates outright; `mergeStateStatus: UNKNOWN` only says GitHub has not recomputed yet.
3. **Use `--force-with-lease`, never a bare `--force`** — a queue may have moved the branch.
4. **A convenience claim written into a handoff is an instruction.** Mine was pushed before it was
   tested, and was wrong; the next agent would have inherited it as fact.

## 39. Report a mechanism's status only from the field that records THAT mechanism

**(2026-09-05, [core#1069].)** §37 and §38 are instances of this rule, not siblings of it. Four times
in one session I reported a mechanism's state from an instrument that records a **different**
mechanism. Every one was confidently worded, and every one was wrong:

| what I claimed | what I read | what that actually records |
|---|---|---|
| "#1109's auto-merge is armed, it lands on its own" | a queue status line | the **queue**, not auto-merge |
| "`autoMergeRequest` is null, so it is not armed" | `autoMergeRequest` | **auto-merge**, not queue membership (`mergeQueueEntry`) |
| "#1110 needs no force-push, the merge-base moves" | merge-base behaviour under a **merge commit** | our queue merges by `REBASE` |
| "`CLAUDE.md` is stale on the merge queue" | my **session-start context snapshot** | the file *as of session start*, not the file |

🚨 **All four read as measurements.** Each named a real field or a real mechanism; none of them was
the field that answers the question being asked. The failure mode is never *"I did not check"* — it
is *"I checked, and the thing I checked was about something else."* That is why it survives review:
a wrong answer sourced from a real instrument looks exactly like a right one.

**Rules:**
1. **Before quoting an instrument, say out loud what it records.** If that sentence does not contain
   the noun in your claim, it is the wrong instrument. `autoMergeRequest` records auto-merge; the
   question was about the queue.
2. **A null field is not evidence a mechanism is off** — only that *this field* is unset. Enumerate
   the fields that could carry the state (`mergeQueueEntry` **and** `autoMergeRequest`) before
   concluding either way.
3. **Your context is not the filesystem.** Anything injected at session start records session start;
   the file is on disk, and `stat` costs one call.
4. **Behaviour claims carry their configuration.** "The merge-base moves" is true under a merge
   commit and false under rebase — name the setting the claim depends on, or do not make it.

---

## 40. Every `merge_group` run on `datanika-core` concludes `failure`, including the successful merges

**(2026-09-06.)** §22 says *read the jobs, not the run*. This is the instance that costs a whole
triage, because the wrong reading is not "stale" — it is a **row of red that describes nothing**.

Measured: the **last twelve** `merge_group` runs on `datanika-core` all conclude `failure`, and the
PR behind every one of the twelve is `MERGED` — #1091, #1093, #1095, #1098, #1100, #1102, #1103,
#1104, #1105, #1108, #1109, #1110. **Twelve reds, twelve successful merges.** The jobs for the most
recent, `33964199351` (`gh-readonly-queue/dev/pr-1110-…`, merged `11:58:36Z`):

```
success  test          success  lint            success  migration-roundtrip
success  core-only-image                        success  helm-lint
success  image-probe   failure  image-cve       skipped  staging / e2e-sso
```

**All five required checks green; the single red is `image-cve`, which is not required and is red
repo-wide.** The run-level conclusion aggregates *every* check; the queue gates on the *required*
ones. Those are two different questions, and only one of them decides whether your PR merges.

🚨 **So `gh run list --event merge_group` shows twelve consecutive failures and reads exactly like a
broken queue.** It is the shape you land on the moment you go looking for *"why did my entry leave
the queue?"* — the one investigation this view cannot answer and looks most qualified to.

**Rules:**
1. **Never read a `merge_group` run's conclusion.** Read its jobs
   (`gh api repos/<r>/actions/runs/<id>/jobs`), and read only the five required names.
2. **The queue's own verdict is not in Actions at all.** An ejection's reason lives on GraphQL's
   `RemovedFromMergeQueueEvent.reason`; the REST timeline's `removed_from_merge_queue` carries none.
   `docs/runbooks/RUNBOOK_MERGE_QUEUE.md` has the query.
3. **The generalisation is §39's:** a run conclusion records *"did every check pass"*, and the
   question is *"did every **required** check pass"*. A real instrument, honestly reporting something
   adjacent to what you asked.

---

## 41. A result that reproduces your prediction exactly is the least informative green

**(2026-09-06, [core#1097].)** I swept `datanika.py` for every protected route's `on_load` loaders,
predicted **eleven** that do database work without a guard, then wrote the test. First run:

```
11 failed, 10 passed
```

Row for row, route for route, exactly the eleven. It was measuring **none** of them. Every one of
those loaders opens `await self._get_org_id()`; the caller stand-in was a bare `MagicMock`, so that
returned a `MagicMock`, and awaiting one raises `TypeError`. The session counter read **0** in all
eleven cases. The guarded loaders passed because they returned before the await — for a reason that
had nothing to do with their guard either.

🚨 **A table that agrees with the hypothesis is the shape you stop checking.** A partial or surprising
result gets read line by line; a perfect one gets screenshotted into the PR body. The existing rules
say *show the check red first* — this one was red first, in exactly the predicted set, and still
proved nothing.

**Rules:**
1. **Read the failure MESSAGE, not the failure COUNT.** One `AssertionError` text would have said
   `TypeError` instead of `1 database session(s)`. The count is the part that agrees with you.
2. **Every "nothing happened" assertion needs a positive control that makes something happen** —
   here, the same handler driven with an *authenticated* stand-in, required to open >= 1 session and
   emit >= 1 hook. If that cannot go green, no zero above it is attributable.
3. **Bind the real method rather than mocking it**, wherever the mock's return value is on the path
   under test. `MagicMock` supplies what is missing (`WORKFLOW_RULES`), and awaiting one fails in a
   way that reads like the code being wrong.
4. 🆕 **`1 error` is not `1 failed`, and a mutation harness prints them in the same slot.**
   (2026-09-06, [core#915].) A sweep row read `RED` exactly as predicted; the summary line said
   `2 warnings, 1 error` — a **collection** error, because the mutation left a dangling expression and
   the module stopped importing. The assertion under test never ran. Reading the colour agreed with
   me; reading the word did not. Grep the harness output for `error` as well as `failed`, or assert
   the failing **node id** is the one you named.

---

## 42. An assertion that a side effect did NOT happen is satisfied by the code dying before it

**(2026-09-06, [core#1097], same file, opposite direction — and this one was the false GREEN.)**

Six of those eleven loaders build `EncryptionService(settings.credential_encryption_key)` **above**
their `with get_sync_session()`. The test config carries the insecure placeholder, so unpatched they
raise `ValueError: Fernet key must be 32 url-safe base64-encoded bytes` — with the session counter
still at **0**. `assert sessions == 0` would have **passed for six of the eleven offenders**, and the
PR would have shipped a guard test that certified six unguarded loaders.

The general form, and it is not about Fernet keys:

> **"X did not happen" is true when X was prevented, and equally true when execution never reached
> X.** Only one of those is the property you are asserting.

**Rules:**
1. **Require the handler to complete.** Return the exception rather than raising it, and assert it is
   empty. An exception is a red, not a pass — a loader that blows up has demonstrated that it ran far
   enough to break, not that it guarded.
2. **Patch the irrelevant collaborators on BOTH paths**, not only on the one where you noticed them.
   Patching `EncryptionService` only for the authenticated control is what leaves the negative path
   dying early and reading clean.
3. **Ask where in the function the instrumented call sits.** If anything above it can throw, your
   zero has two explanations.

---

## 43. A negative control proves your INSTRUMENT works. It says nothing about your PROPOSITION

**(2026-09-06, [core#830].)** Fixing SAML defect 1 (`sp_binding: "redirect"` returning the Response
in a URL) I also moved `idp_sso_url` to authentik's POST-binding endpoint, reasoning that *"the Issuer
and the SSO url are compared during validation."* Then I wrote a guard banning
`/sso/binding/redirect/` anywhere in the fixture — **and gave it a negative control**, because the
first version of the pattern had been too wide and the obvious repair is to narrow it until it sees
nothing:

```python
def test_the_endpoint_guard_can_see_a_real_redirect_reference(self):
    was_the_defect = '  \\"issuer\\": \\"${AUTHENTIK_URL}/…/sso/binding/redirect/\\",'
    assert re.findall(r"\S*sso/binding/redirect/\S*", was_the_defect)
```

It passed. The regex could see a redirect endpoint. **What nothing tested was whether banning one was
true** — and it was not. SP-initiated SAML crosses the wire twice and the two crossings choose
bindings **independently**: the AuthnRequest leaves as a 302 with `?SAMLRequest=`, and authentik's
POST-binding view reads `request.POST`. The next run stalled on authentik's own page —
*"Bad Request — The SAML request payload is missing."* — and the guard held the defect in place for
three days.

Only the **Issuer** is compared (`idp.entityId`, against `<saml:Issuer>`).
`idp.singleSignOnService.url` is dialled, never compared. One clause of that sentence was true and I
enforced the whole of it.

🚨 **This is §41's sibling and the more dangerous one.** §41 is a result that agrees with you. This is
a *control* that agrees with you — and a control carries the authority of having been sceptical, so it
buys the proposition a credibility the proposition never earned. Both guards written for this defect,
in two departments, had this shape.

**Rules:**
1. **Name the proposition separately from the pattern.** "Does the regex match?" and "should this
   string be absent?" are different questions; a control answers only the first.
2. **A ban is a claim about the whole file; the defect is usually about one FIELD.** Prefer
   *"`idp_sso_url` ends with `/binding/redirect/`"* over *"`binding/redirect` appears nowhere"* —
   file-scope enforcement of a per-field property is what pinned this.
3. **When a fix corrects one direction of a two-directional protocol, ask what the other direction
   does.** Request and response, read and write, encode and decode. The correction that is right for
   one is a defect in the other, and it ships looking like thoroughness.
4. **Settle it against the real consumer's own code when you cannot run it.** authentik 2024.12's
   `providers/saml/views/sso.py` — the tag pinned in `e2e/docker-compose.test.yml` — names the
   channel each view reads *and* shows the response binding chosen from `provider.sp_binding`,
   independent of the receiving view. Two `WebFetch` calls, and it closed a claim neither department
   could reach with a container.

---

## 44. Before starting an issue another department holds, list its open PRs

**(2026-09-06, [core#830] again, one hour later.)** QA and Engineering diagnosed and fixed the same
SAML binding defect **independently, within the hour**, from the same artifact. Two PRs, the same two
files. Nothing warned either of us. The **merge queue** did: [#1132] entered at position 5 reading
`UNMERGEABLE` while [#1129] sat at position 3, and `git merge-tree` confirmed the conflict in both
files.

⚠️ **The handoff file did not fail — it said `#830` was handed to another department.** That line is
the moment to check for an open PR, and I read it as the moment to assume there was none.

**Rules:**
1. `gh pr list --repo <r> --search "<issue number>" --state open` before the first edit. One command,
   and `--search` finds a PR whose title never mentions the number.
2. **Duplicate work is not settled by who is better; it is settled by who is first.** Dequeue yours,
   comment with what is additive, and re-land the remainder as a delta on top.
3. **What survives is what the other PR does not have.** Here: the `issuer` read-back, a compile guard
   over all fourteen inline `py "…"` blocks, and the authentik-source reading that turned their
   *predicted* half into a measured one. Say that in the comment rather than in a second PR.

---

## 45. A guard that encodes your THEORY OF THE CAUSE will bless any change that satisfies the theory

**(2026-09-07, [core#933].)** The stopgap guard for #933 asserted, against the real `env.py`, that
`first_execute < first_begin` — *"a statement is executed before alembic begins a transaction"*.
That is the **theory**. The **condition** is that a statement is executed on that connection at all.

Apply the issue's own option 1 — move `SET search_path` inside `context.begin_transaction()` — and
the theory is satisfied while the defect is untouched. Measured on the real file: the guard failed
with

> *"env.py no longer executes a statement before alembic begins its transaction … If that was
> deliberate, core#933 may be fixed"*

**while `autocommit_block()` was still refused.** The file's runtime arm said *broken*, its source
arm said *possibly fixed*, about the same tree — and the message a human reads was the wrong one.
**A guard can certify a non-fix**, and this one was one commit from doing it.

⚠️ **The control's NAME carried the theory too.** `test_it_works_when_alembic_owns_the_transaction`
— alembic owns nothing in that arm; `_transaction` is `None` there exactly as in the failing arms.
**A name is an assertion that nothing checks**, so it survives the mechanism being disproved and
teaches the next reader the disproved thing.

**Rules:**
1. **Write the condition, not your account of it.** *"Any statement touches this connection"*, not
   *"the statement comes first"*. If you cannot state the condition without narrating a mechanism,
   you do not yet know which one you are testing.
2. **Prove both halves against the real artifact**: the wrong fix must NOT read as fixed, and a
   right fix must. One without the other is satisfied by a guard that cannot fire.
3. **A failure message that speculates (`"may be fixed"`) is a liability.** Say what was measured
   and what to do; never hand the reader a conclusion the test did not reach.
4. This is §43 turned inward. §43 is a control that agrees with your proposition; this is a *guard*
   that encodes it — and the guard outlives the session that wrote it.

---

## 46. A spec clause naming a value from a CLOSED VOCABULARY is an instruction to write a defect

**(2026-09-07, [core#1127].)** `SettingsState.transfer_ownership` passed `"transfer_ownership"` as
its audit action. Not an `AuditAction` member, so `BaseState._audit`'s deliberate swallow dropped
the row: **the highest-privilege action in the product had no history at all**, and every check was
green.

The string was not a typo. `SPEC_ORG_ROLES.md` §3 item 4 said *"Audited as its own action
(`transfer_ownership`)"*. **The implementation was faithful to a clause that could not be
satisfied** — the code was compliant, the review was correct, the row was dropped anyway. Same shape
as [core#933]/[#1133]: *an unsatisfiable checklist, ticked.*

**Rules:**
1. **When a spec names a value, check it against the vocabulary that will receive it** — an enum,
   a filter's option list, a column's constraint. Prose does not typecheck.
2. **Fix it at both ends.** Correcting only the call site leaves the document ready to regenerate
   the defect the next time somebody implements from it.
3. **Keep the old wording visible as the cause.** Deleting it hides why the code was written that
   way, and the next reader re-derives the same "obvious" string.
4. **Sweep for siblings once, and say the number.** One `grep` over `docs/**` for values outside the
   vocabulary; here it found exactly one, which is worth stating precisely because it bounds the
   problem.
5. The durable fix is a derived guard —
   `tests/test_services/test_audit_call_site_vocabulary.py` fails at authoring time on the next one.

---

## 47. Presence of the OBJECT is not presence of the ROW — `identity_map` is weak

**(2026-09-07, [core#934].)** A test needed *"was an audit row already in this transaction when the
handler committed?"* The obvious instrument —

```python
any(isinstance(o, AuditLog) for o in session.identity_map.values()) or ... session.new
```

— reads **`False` on correct code**. `Session.identity_map` is a **weak** dict; `AuditService.
log_action` flushes the row and returns it, every caller discards the return, and the object is
garbage-collected out of the map while its row sits in the transaction. Isolated probe:
`identity_map` size **0**, `session.new` empty, `SELECT count(*)` = **1**.

🚨 **The danger is the direction of the error.** A test that reds on correct code invites changing
the *implementation* until it passes — so an instrument built to prove the audit rides the
transaction could have been "fixed" by moving it out of one.

**Rules:**
1. **Ask the store, not the session.** To assert a row exists in a transaction, `SELECT` it.
   Session collections describe object lifecycle, which is not what you are asserting.
2. `session.new` empties at flush and `identity_map` empties at garbage collection — **both are
   transient for reasons unrelated to your property.**
3. When an assertion fails on code you believe is correct, **suspect the instrument before the
   code**, and settle it with a probe outside the test harness (three lines here).

---

## 48. A mutant that does not mutate reports the system as perfect

**(2026-09-07, [core#934].)** The mutation for *"move the `_audit` call below `session.commit()`"*
was first written as a one-line replacement that appended a comment to the `commit()` line. It
returned **GREEN** — and read exactly like a real finding: *"the suite cannot see this defect."*
The conclusion happened to be true, reached by an instrument that measured nothing. Rewritten as a
genuine two-edit move, it went red against precisely one test.

**Rules:**
1. **A mutation must change behaviour, not text.** Before believing a GREEN row in a mutation
   matrix, confirm the mutant *does* the thing — parse it, and where it is cheap, assert the mutated
   symbol actually moved.
2. **GREEN in a mutation sweep is a claim about your harness first and your suite second.** RED
   proves the test can fire; GREEN proves nothing until the mutant is shown real.
3. Related and equally cheap to miss: **a setup or collection `ERROR` is not a red control**
   (§41 rule 4). The first run of `test_dag_audit_trail.py` produced **9 errors, not failures** —
   a fixture name tripped a validator and no assertion executed. Read `ERROR` vs `FAILED` before
   claiming a red.
4. Corollary for anchors: **assert the anchor matches exactly once before writing.** Three refusals
   in this session were all correct (0 matches — `git ls-files --eol` says `i/lf w/crlf`; 8 and 2
   matches — one file had eight `_audit` calls sharing a prefix). The repair is to narrow the
   *scope* to the function's byte span, never to widen the anchor.

[core#704]: https://github.com/datanika-io/datanika-core/issues/704
[core#915]: https://github.com/datanika-io/datanika-core/issues/915
[#1129]: https://github.com/datanika-io/datanika-core/pull/1129
[#1132]: https://github.com/datanika-io/datanika-core/pull/1132
[core#830]: https://github.com/datanika-io/datanika-core/issues/830
[core#1097]: https://github.com/datanika-io/datanika-core/issues/1097
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
[core#864]: https://github.com/datanika-io/datanika-core/issues/864
[core#997]: https://github.com/datanika-io/datanika-core/issues/997
[core#1069]: https://github.com/datanika-io/datanika-core/issues/1069
[core#1071]: https://github.com/datanika-io/datanika-core/issues/1071
[cloud#171]: https://github.com/datanika-io/datanika-cloud/issues/171
[cloud#195]: https://github.com/datanika-io/datanika-cloud/issues/195
[core#923]: https://github.com/datanika-io/datanika-core/issues/923
[core#1108]: https://github.com/datanika-io/datanika-core/issues/1108
[core#1109]: https://github.com/datanika-io/datanika-core/pull/1109
[core#1110]: https://github.com/datanika-io/datanika-core/pull/1110
[core#1127]: https://github.com/datanika-io/datanika-core/issues/1127
[core#934]: https://github.com/datanika-io/datanika-core/issues/934
[core#933]: https://github.com/datanika-io/datanika-core/issues/933
