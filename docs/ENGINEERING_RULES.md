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

The hook runs ruff plus the **entire pytest suite** before the pack is sent — measured at
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

---

## 2. Auto-merge does not update a `BEHIND` branch

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
