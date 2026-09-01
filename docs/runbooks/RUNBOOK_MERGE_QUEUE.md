# Runbook — the merge queue on `dev`

Covers `datanika-core` and `datanika-landing`. **`datanika-cloud` has no queue and must not
get one**: it is private, the org is on the free plan, and GitHub answers the ruleset API
with `403 — "Upgrade to GitHub Pro or make this repository public"`. That is a vendor spend,
and the standing rule is no spend before a paying user. The same gate already denies cloud
branch protection.

Filed from [core#904]. Everything below marked **measured** was observed on
`datanika-landing` on 2026-09-01, in the order the rollout happened.

---

## Why the queue exists

`dev` is `strict = true` and five departments merge into it. Base-branch moves ran
~10–15 minutes apart on 2026-09-01 against a required-check cycle of about the same length.
**When base cadence ≈ CI duration, a rebased PR goes `BEHIND` again before its own checks
finish, and never converges.** Both [PR #894] and [PR #898] needed a hand-run
`gh pr update-branch --rebase` inside one hour. An armed auto-merge on a `BEHIND` PR looks
exactly like progress while it waits.

The queue replaces "the author rebases and waits" with "the queue tests the merged result".

---

## 🚨 The prerequisite, and why it is the whole reason this was a rollout

**A workflow reporting a required check must trigger on `merge_group`.** GitHub does not
start a run for a queue entry otherwise. Nothing goes red — the entry sits until
`check_response_timeout_minutes` expires and is ejected. **That is strictly worse than the
livelock it replaces**, which a human clears in seconds.

Guarded, so it cannot regress:

| repo | guard |
|---|---|
| core | `tests/test_deploy/test_merge_queue_triggers.py` |
| landing | `tests/merge-queue-triggers.test.ts` (runs inside `npm test`, i.e. inside `build`) |

Both are derived from the workflows rather than from a list of check names: *any workflow
that can report a check on a PR into `dev`, and has no `paths:` filter, must trigger on
`merge_group` and must not cancel merge-group runs.* The `paths:` exemption is derived too —
`CLAUDE.md` already requires `dev`'s required checks to carry no `paths:` filter so each
always reports, so a paths-filtered workflow cannot supply one.

⚠️ **`cancel-in-progress` must exclude `merge_group`.** A cancelled queue-entry run is an
*absent* verdict and nothing recomputes it. **Measured:** the merge-group ref is
`refs/heads/gh-readonly-queue/dev/pr-<N>-<BASE sha>` — the base commit, **not** the PR head —
so two entries for one PR against an unchanged base share a `github.ref`. That is exactly the
re-queue case. The guard is load-bearing, not defensive.

---

## Daily use

```bash
gh pr merge <n> --auto            # adds to the queue. No method flag — the queue sets it.
```

🚨 **Two signals lie about whether that worked, and both were believed once before being
checked.**

| signal | what it does |
|---|---|
| `gh pr merge --auto` exit code | **0 whether or not it enqueued.** It may print `! The merge strategy for dev is set by the merge queue` and still enqueue, or print nothing at all |
| `autoMergeRequest` | **`null` for a queued PR.** A queue entry is not an auto-merge request |
| `gh pr merge --disable-auto` | **does not dequeue.** Prints `already queued to merge`, exits 0, entry stays |

**The only outcome check:**

```bash
gh api graphql -f query='query { repository(owner:"datanika-io", name:"<repo>") {
  mergeQueue(branch:"dev") { entries(first:20) { totalCount
    nodes { position state pullRequest { number } } } } } }'
```

To remove an entry, use GraphQL — there is no `gh` subcommand:

```bash
PRID=$(gh api graphql -f query='query { repository(owner:"datanika-io", name:"<repo>") {
  pullRequest(number:<n>) { id } } }' -q '.data.repository.pullRequest.id')
gh api graphql -f query="mutation { dequeuePullRequest(input:{id:\"$PRID\"}) {
  mergeQueueEntry { state } } }"
```

---

## 🚨 "My PR left the queue and nothing is red" — the diagnostic

This is the failure mode the queue introduces, and the obvious place to look does not
answer it.

**The REST timeline event carries NO reason.** `GET /repos/{o}/{r}/issues/{n}/timeline`
returns `removed_from_merge_queue` with `actor`, `created_at`, `id`, `commit_id: null` — and
nothing saying why. Reading it and concluding "GitHub doesn't say" is the wrong conclusion.

**GraphQL has the reason. Use this:**

```bash
gh api graphql -f query='query { repository(owner:"datanika-io", name:"<repo>") {
  pullRequest(number:<n>) {
    timelineItems(last:20, itemTypes:[ADDED_TO_MERGE_QUEUE_EVENT, REMOVED_FROM_MERGE_QUEUE_EVENT]) {
      nodes { __typename
        ... on AddedToMergeQueueEvent { createdAt actor { login } }
        ... on RemovedFromMergeQueueEvent { createdAt reason actor { login } beforeCommit { oid } }
      } } } } }'
```

⚠️ **Only one value has been observed: `reason: "manual"`**, from a deliberate dequeue. The
value space is **not** enumerated here, because guessing at the string a timeout produces is
how a diagnostic ends up matching nothing. Read whatever `reason` says; do not grep for a
value this runbook invented.

⚠️ **A timeout ejection has not been observed.** Its most likely cause — a required check
that no workflow reports — is exactly what the guard tests above prevent, so the residue is
a GitHub outage or a workflow file that fails template validation. If a PR leaves the queue
with no check ever having started, check that first:

```bash
gh api "repos/datanika-io/<repo>/actions/runs?event=merge_group&per_page=5" \
  -q '.workflow_runs[] | "\(.id) \(.head_branch) \(.status)/\(.conclusion)"'
```

**Zero merge-group runs for your entry is the signature.** It is not "still running".

---

## `strict` alongside a queue — MEASURED, and it is not what the docs left ambiguous

GitHub says the queue *"provides the same benefits as Require branches to be up to date
before merging, but does not require a pull request author to update their pull request
branch."* [core#904] recorded as **unsettled** whether leaving the `strict` checkbox ON
reintroduces the livelock at queue-entry time.

**It does not.** Measured on landing with `strict = true` and the queue active:

| | |
|---|---|
| `dev` head | `4306b601` |
| [PR #442] base | `bac02c24` — one commit behind |
| `required_status_checks.strict` | `true` |
| `mergeStateStatus` | **`CLEAN`**, not `BEHIND` |
| enqueue | accepted; entry reached `QUEUED` with the stale base |

So with a queue present GitHub stops enforcing the up-to-date requirement at merge time, and
`strict` can be left on without cost. **`strict` was therefore NOT changed on either repo.**
Leaving it on is the conservative option: if a queue is ever removed, the old guarantee is
still there rather than silently absent.

Corollary: **the `allow_update_branch` question is moot on a queued branch.** A PR with a
stale base reads `CLEAN`, so GitHub has no reason to update it and nothing needs it to.

---

## Promotions never enter a queue

- A queue governs **feature → `dev`**. Promotions are **`dev` → `master`/`main`** and are
  unaffected.
- 🚨 **Never put a queue on `master`/`main`.** Same reason auto-merge is banned there: one
  required review, unsatisfiable with a single `Timev` identity, so a queued promotion would
  wait forever looking like progress.
### 🚨 The resync is a direct push, and a `merge_queue` rule blocks those

The last step of every promotion — `git push origin origin/master:refs/heads/dev` — is a
**direct push to `dev`**. **Measured:** a branch carrying a `merge_queue` rule refuses one:

```
422  Repository rule violations found
     Changes must be made through the merge queue
```

⚠️ Reading `GET /rules/branches/dev` and seeing no `pull_request` rule is **not** evidence
that pushes are allowed. I reasoned exactly that way and it was wrong; the `merge_queue` rule
blocks pushes by itself.

⚠️ **`gh` exits 0 on the refused update.** It prints the 422 to stdout and returns success.
The only thing that distinguishes refusal from success is re-reading where the ref points.

**This is why both rulesets carry a bypass:**

```json
"bypass_actors": [
  { "actor_id": 5, "actor_type": "RepositoryRole", "bypass_mode": "always" }
]
```

Verified on a throwaway branch: covered-without-bypass refuses, covered-with-bypass succeeds,
and an uncovered branch succeeds as the control. **Do not create a merge-queue ruleset here
without it** — the failure surfaces as a 422 halfway through a promotion, in the step whose
whole purpose is to stop `dev` and `master` diverging, and an agent meeting that mid-promotion
is one keystroke from forcing something.

Two consequences, stated because they cut against the queue:

1. 🚨 **`--admin` bypasses the queue.** All agents share one `Timev` identity, which is a repo
   admin, so the bypass cannot be scoped to Infra alone. The standing rule *"never reach for
   `--admin` on `dev`; it is the promotion tool"* is **more** load-bearing under a queue.
2. ⚠️ **The queue does not close the "direct pushes to `dev` get no CI" hole.** Without the
   bypass it would have. That is a real benefit given up, deliberately, to keep promotions
   working. Closing it needs its own mechanism — a `pull_request` ruleset rule, or CI on
   `push` — decided on its own merits, not smuggled in here.

---

## Configuration, and how to turn it off

```bash
gh api repos/datanika-io/<repo>/rulesets            # list; note the id
gh api repos/datanika-io/<repo>/rules/branches/dev  # what actually applies to dev
```

Landing's ruleset is **id 22022738**, `merge-queue-dev`, `enforcement: active`, targeting
`refs/heads/dev`, with the admin bypass above. Parameters, and why each was chosen:

| parameter | value | why |
|---|---|---|
| `merge_method` | `REBASE` | feature PRs rebase into `dev`; squash is disabled repo-wide and merge commits are for promotions only |
| `min_entries_to_merge` | `1` | a single PR must not wait to be batched. With this at 1 the minimum is met immediately and `min_entries_to_merge_wait_minutes` never applies |
| `grouping_strategy` | `ALLGREEN` | every entry in a group must be green, not just the head |
| `check_response_timeout_minutes` | `30` | landing CI is ~60 s, so 30 min is generous; it also bounds how long a stall can masquerade as progress |
| `max_entries_to_build` / `to_merge` | `5` / `5` | |

**Rollback — one call, takes effect immediately:**

```bash
gh api --method DELETE repos/datanika-io/<repo>/rulesets/<id>
```

Then merge normally with `gh pr merge <n> --rebase`. Nothing else has to be undone: the
`merge_group` triggers are inert without a queue, because the event only fires for a
repository that has one.

---

## What a healthy queue run looks like

From landing's first entry, so the shape is recognisable:

```
gh pr merge 440 --auto
  -> entry AWAITING_CHECKS, position 1
  -> run 33523211167, event merge_group,
     branch gh-readonly-queue/dev/pr-440-8d93a238, sha bac02c24
  -> check `build` reports on bac02c24: success
  -> PR merged 15:03:13Z, merge commit bac02c24
  -> dev head == bac02c24
```

🔑 **The merge commit and the tested commit are the same object.** That is the guarantee
worth checking if you ever doubt the queue is doing anything: the thing that was tested is
exactly the thing that merged.

⚠️ It also means **the PR that adds the `merge_group` trigger bootstraps itself** — GitHub
reads triggers from the merge-group ref's own tree (base + PR), not from the base branch.
That is how #440 landed, and it is worth knowing before anyone concludes the trigger must be
on `dev` first.

[core#904]: https://github.com/datanika-io/datanika-core/issues/904
[PR #894]: https://github.com/datanika-io/datanika-core/pull/894
[PR #898]: https://github.com/datanika-io/datanika-core/pull/898
[PR #442]: https://github.com/datanika-io/datanika-landing/pull/442
