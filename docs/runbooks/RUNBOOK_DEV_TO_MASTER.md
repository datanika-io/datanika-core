# Runbook: Promote `dev → master` on datanika-core

> **Moved into this repository on 2026-08-31** from `plans/infra/`, a local directory outside every
> git repo. Same document, versioned. Companion rules that are *not* procedure live in
> [`../INFRA_RULES.md`](../INFRA_RULES.md) §7.

> **When to run**: whenever the `dev` work you intend to release is merged and green. Not on a
> calendar, and not "when Engineering is done" — five departments merge into `dev` independently
> since 2026-07-22, so run the pre-flight below rather than asking a team.
>
> **What deploys**: everything on `dev` that isn't on `master` yet. Enumerate it, don't assume:
> ```bash
> git fetch origin && git log --oneline origin/master..origin/dev
> ```
>
> **Impact**: the app swaps with **zero downtime** — `deploy-pointer.yml` starts the inactive
> colour, health-checks it, repoints Apache, and only then stops the old one. Two things are
> *not* covered by that and are still real:
> - **Celery restarts normally** (no blue/green for the worker), so in-flight tasks are interrupted.
> - **Migrations run from the container start command**, so the *previously deployed* code runs
>   against the *new* schema during the swap. This is the `t1` window — every migration in the
>   release must be expand-only. See [`SPEC_EXPAND_CONTRACT_MIGRATIONS.md`](../specs/SPEC_EXPAND_CONTRACT_MIGRATIONS.md);
>   core#449 landed a CI guard for it, but the guard has an escape hatch and CI cannot see a
>   two-release contract break, so read the migrations.
>
> The old "~30s downtime, check for low traffic first" advice is obsolete for the app tier.

---

## Pre-flight

- [ ] Confirm nothing you meant to include is still open. ~~check with Engineering~~ — since
      2026-07-22 all five departments merge into `dev` themselves, so no single team knows
      what's in flight. Ask GitHub, not a person:
  ```bash
  gh pr list --repo datanika-io/datanika-core --base dev --state open \
    --json number,title,mergeStateStatus
  ```
- [ ] **Verify CI is green for the exact SHA you are about to promote.** ⚠️ Do **not** use
      `gh run list --branch dev --limit 1` (what this step used to say). That returns
      whichever workflow finished last — often `promotion-pr-refs` or a docs build — and
      reports *its* conclusion, so a red `CI` run is invisible behind a green unrelated one.
      A green run from three commits ago is also not evidence about `dev`'s head. Pin the SHA:
  ```bash
  SHA=$(gh api repos/datanika-io/datanika-core/commits/dev --jq .sha)
  gh api "repos/datanika-io/datanika-core/actions/runs?branch=dev&head_sha=$SHA" \
    --jq '.workflow_runs[] | "\(.name)\t\(.status)\t\(.conclusion)"'
  ```
      Every row must read `completed  success`. If a row is missing entirely, CI has not run
      on that SHA yet — that is *not* a pass.
- [ ] **Confirm the staging jobs passed, specifically.** `dev`'s required checks are only
      `lint, test, helm-lint, migration-roundtrip`. `strict=true` catches a cross-PR
      interaction *only if one of those four can see it* (see WORKFLOW_RULES §2). Anything
      that surfaces solely in `deploy-staging` / `smoke-staging` / `e2e-staging` — which are
      push-only, so they never gate a PR — lands on `dev` first and is caught **here**.
      `master` has no required checks at all, so this checkbox is the only thing between a
      red `dev` and production:
  ```bash
  gh run list --repo datanika-io/datanika-core --branch dev --workflow CI --limit 1 --json databaseId \
    --jq '.[0].databaseId' | xargs -I{} gh run view {} --repo datanika-io/datanika-core \
    --json jobs --jq '.jobs[] | select(.name|test("staging")) | "\(.name)\t\(.conclusion)"'
  ```
- [ ] Ensure no active user sessions you'd want to avoid interrupting (check Plausible real-time)
- [ ] Verify Grafana is reachable: `ssh -i ~/.ssh/id_ed25519 -L 3001:localhost:3001 root@185.25.22.188`

## Promote

1. Create and merge the promotion PR:
   ```bash
   gh pr create --repo datanika-io/datanika-core --base master --head dev \
     --title "Promote dev → master: <list features>" \
     --body "Bundled release: <describe what's included>"
   # Wait for CI to pass
   gh pr merge <number> --repo datanika-io/datanika-core --merge --admin
   ```

2. Wait for the CD deploy to complete:
   ```bash
   gh run list --repo datanika-io/datanika-core --branch master --limit 1
   gh run watch <run_id> --repo datanika-io/datanika-core --exit-status
   ```

## Post-deploy (SSH to prod — pointer.gr, **not** Hetzner)

3. SSH in and verify containers:
   ```bash
   ssh -i ~/.ssh/id_ed25519 root@185.25.22.188   # Windows OpenSSH
   docker ps --format '{{.Names}} {{.Status}}' | grep datanika
   ```
   All containers should be `Up` and `(healthy)` within 60s. Under blue/green exactly one
   of `datanika-app` / `datanika-app-b` is expected to be running — two is the normal
   *transient* state mid-swap, but two an hour later means the swap never finished stopping
   the old colour. Confirm which one Apache is actually pointed at:
   ```bash
   cat /etc/apache2/conf-enabled/datanika-prod-active.conf   # 8000/3000 = blue, 8010/3010 = green
   ```
   ⚠️ **Shared box** — the founder's `olcrtc` VPN and an Apache `:80` `webdav` vhost belong
   to co-tenants. Never restart Apache outright; `apachectl configtest && apachectl graceful`.

4. Create the pg_stat_statements extension (only needed once, idempotent):
   ```bash
   docker exec datanika-postgres psql -U datanika -d datanika \
     -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
   ```

5. Verify pg_stat_statements is populated:
   ```bash
   docker exec datanika-postgres psql -U datanika -d datanika \
     -c "SELECT count(*) FROM pg_stat_statements;"
   ```
   Should return > 0 within a few seconds of app activity.

6. Verify slow query logging is active:
   ```bash
   docker exec datanika-postgres psql -U datanika -d datanika \
     -c "SHOW log_min_duration_statement;"
   ```
   Should return `500`.

7. Verify postgres-exporter is scraped by Prometheus:
   ```bash
   curl -sf 'http://localhost:9090/api/v1/query?query=pg_up' | \
     python3 -c 'import sys,json; d=json.load(sys.stdin); \
     r=d["data"]["result"]; print("pg_up:", r[0]["value"][1] if r else "NOT FOUND")'
   ```
   Should return `pg_up: 1`.

8. Restart Grafana to pick up new alert rules:
   ```bash
   cd /opt/datanika/datanika && set -a && source .env.docker && set +a
   docker compose restart grafana prometheus
   ```

9. Verify Grafana alerts are evaluating:
   ```bash
   source /opt/datanika/datanika/.env.docker
   curl -s -u admin:${GRAFANA_ADMIN_PASSWORD} \
     'http://localhost:3001/api/prometheus/grafana/api/v1/alerts' | \
     python3 -c 'import sys,json; d=json.load(sys.stdin); \
     alerts=d["data"]["alerts"]; \
     [print(a["labels"]["alertname"], a["state"]) for a in alerts if "pg-" in a["labels"].get("uid","")]'
   ```
   Should show `PostgreSQL Slow Query Spike` in `Normal` state.

10. Check Plausible that the site is still serving traffic:
    ```
    https://plausible.datanika.io/datanika.io
    ```
    Current visitors > 0 confirms the app is live.

## Post-deploy: sync dev

11. After promoting, sync `dev` back to `master` so they don't diverge:
    ```bash
    # From local machine
    cd datanika
    git fetch origin
    git push origin origin/master:refs/heads/dev        # fast-forward, NO --force
    ```

    ### ⚠️ Never `--force` this push, and never disarm protection to make it work (changed 2026-07-22)

    This step used to read `--force`, plus *"(Requires temporarily removing dev branch
    protection if set.)"* Both are now wrong, and the second was the dangerous half.

    `--force` was a leftover from the rebase-promotion era, when `master` was *not* a
    descendant of `dev` and the push genuinely wasn't a fast-forward. Since promotions
    adopted merge-commits (2026-04-13) `master` **is** a descendant, so a plain push
    fast-forwards. `--force` has been unnecessary since then.

    It became **dangerous** on 2026-07-22, when merging into `dev` stopped being
    Infra-only (WORKFLOW_RULES §2). Any department can now merge into `dev` at any
    moment — including in the minutes between your `git fetch` and your `git push`. A
    forced push silently discards whatever landed in that window: their commit
    disappears, `dev` still builds green, and their issue stays closed. Nothing
    anywhere reports it. Under the old model Infra was the only writer to `dev`, so
    this was near-impossible; decentralising the merges is what created it.

    GitHub already refuses it — `dev` has `allow_force_pushes: false` in both public
    repos (verified via the API, 2026-07-22). **That is exactly why the old
    "temporarily remove dev branch protection" instruction is the real hazard**: it
    tells you to disarm the one control that stops this, at the precise moment the
    race is open. Don't. There is nothing to remove anyway — `dev` carries no
    pull-request requirement (verified: `required_pull_request_reviews` absent), so a
    plain fast-forward push is permitted as-is.

    **A rejected push is the feature, not a failure.** `! [rejected] non-fast-forward`
    means someone merged during your promotion window. That is information. Fetch,
    confirm what landed, and promote it too or let the next promotion carry it —
    never overwrite it.

## Rollback

> ⚠️ **This section was wrong twice over until 2026-07-22.** It pointed at `46.225.214.120`
> — the Hetzner box **terminated 2026-07-14** — and it rolled back with `git checkout` inside
> `/opt/datanika/datanika`, which on the current host has **no `.git` at all** (the source is
> tar-transferred, see CLAUDE.md). Both commands fail; the second fails *after* you have
> already SSH'd somewhere expecting to fix an outage.

If the deploy breaks the app, roll back by **swapping the serving colour back** — that is what
blue/green is for, and it does not need a rebuild:

```bash
ssh -i ~/.ssh/id_ed25519 root@185.25.22.188
# Which colour is live? blue = 8000/3000, green = 8010/3010
cat /etc/apache2/conf-enabled/datanika-prod-active.conf
docker ps --format '{{.Names}}\t{{.Status}}' | grep datanika-app
```

1. **If the previous colour's container is still running** (normal — `deploy-bluegreen.sh`
   stops it only after the swap verifies), point the include back at it and reload:
   ```bash
   printf 'Define DATANIKA_BE 8000\nDefine DATANIKA_FE 3000\n' \
     > /etc/apache2/conf-enabled/datanika-prod-active.conf   # or 8010/3010 for green
   apachectl configtest && apachectl graceful
   ```
   `configtest` before `graceful` is not optional — a bad include takes Apache down for the
   co-tenants on this shared box, not just for us.
2. **If it was already stopped**, start it first:
   `cd /opt/datanika/datanika && docker compose --profile bluegreen up -d app_b` (or `app`).
3. **Only if both colours are bad** does this become a code rollback. There is no git on the
   box: either re-transfer the previous source (the tar command in CLAUDE.md) or run a pinned
   release image, `ghcr.io/datanika-io/datanika-core:v0.x.y` — which is the argument for
   cutting a `v*` tag on every promotion that matters.

Then investigate on `dev` and fix forward.

---

## Worktree Cleanup Procedure

> **Why**: With auto-delete on merged branches and rebase-only merges, worktrees should never sprawl. But if they do (e.g., legacy state, abandoned tasks), use this procedure to nuke them safely without losing work.

**Canonical paths** (per `plans/WORKFLOW_RULES.md` §1):
- Core: `worktrees/datanika-core-{engineering,product,infra}/`
- Landing: `worktrees/datanika-landing-{engineering,growth,product,infra}/`
- Cloud: `worktrees/datanika-cloud-{engineering,infra}/`

Anything outside these paths is stale.

### Step 1: Audit before deleting

For each repo, identify worktrees that are:
1. **Dirty** (uncommitted work)
2. **Unmerged** (commits ahead of `origin/dev` that don't exist as a remote branch)
3. **Active** (being modified by another session)

```bash
cd /d/Projects/Datanika/<repo>
git fetch origin --prune
for wt in $(git worktree list --porcelain | awk '/^worktree/ {print $2}' | grep -v "^D:/Projects/Datanika/<repo>$"); do
  branch=$(cd "$wt" && git rev-parse --abbrev-ref HEAD)
  ahead=$(git log "$branch" --not origin/dev --oneline 2>/dev/null | wc -l)
  dirty=$(cd "$wt" && git status --porcelain | wc -l)
  echo "${wt##*/} [$branch] — $ahead ahead, $dirty dirty"
done
```

**Run this twice with a 30s gap** to catch active sessions — if any worktree's SHA or branch changes between passes, it's in flight. Skip those.

### Step 2: Preserve unmerged work

For any worktree that has commits not on `origin/dev` and no matching remote branch, push the branch first so the work isn't lost:

```bash
cd /d/Projects/Datanika/worktrees/<stale-worktree>
git push origin <local-branch>          # creates remote branch
# Or, if you only want one specific commit on a fresh branch:
git push origin <local-branch>:<new-branch-name>
```

For dirty worktrees, either commit the work or stash it to a branch (`git stash branch <name>`) before deleting. **Never `--force` delete a dirty worktree without first asking the user** — the user may not know about the in-flight work.

### Step 3: Delete non-canonical worktrees

```bash
cd /d/Projects/Datanika/<repo>
git worktree list --porcelain | awk '/^worktree/ {print $2}' \
  | grep -Fvx "D:/Projects/Datanika/<repo>" \
  | grep -Fvx "D:/Projects/Datanika/worktrees/<repo>-engineering" \
  | grep -Fvx "D:/Projects/Datanika/worktrees/<repo>-product" \
  | grep -Fvx "D:/Projects/Datanika/worktrees/<repo>-infra" \
  | grep -Fvx "D:/Projects/Datanika/worktrees/<repo>-growth" \
  > /tmp/wt_delete.txt

while IFS= read -r wt; do
  echo ">>> removing $wt"
  git worktree remove --force "$wt"
done < /tmp/wt_delete.txt

git worktree prune
```

After deletion, also `rmdir` any empty leftover directories under `D:/Projects/Datanika/worktrees/` that git may have orphaned.

### Step 4: Create canonical worktrees if missing

```bash
cd /d/Projects/Datanika/<repo>
git worktree add -b <issue>-<slug> ../worktrees/<repo>-<agent> origin/dev
```

**Important gotcha**: An agent worktree must NEVER track the local `dev` branch — the main checkout already has it, and git only allows one worktree per branch. Always use `origin/dev` as the start point and create a feature branch in the worktree.

### Step 5: Reset worktree to next task (after merging a PR)

After a worktree's PR merges, prep it for the next task:

```bash
cd /d/Projects/Datanika/worktrees/<repo>-<agent>
git fetch origin --prune
git checkout -b <next-issue>-<slug> origin/dev
git branch -D <previous-branch>  # safe now that the new branch is checked out
```

This avoids the "branch already used by worktree" error from trying to check out local `dev`.

### Last cleanup: 2026-04-12

- **Landing**: 34 stale worktrees deleted, 3 canonical kept (engineering, growth, product). 2 unmerged branches preserved as remote branches: `ai-agent-native-content` (5 connector guides + AI-agent positioning), `og-image-guardrail-test` (cherry-picked from a post-#93 commit).
- **Core**: skipped — multiple active sessions detected mid-audit (worktrees changed state between passes). Re-audit needed before cleanup.
- **Cloud**: no stale worktrees, only main checkout.

---

## Technical decision: rebase-merge divergence between `dev` and `master`

> **Context**: Infra hit this during the 2026-04-12 promotion. After rebase-merging a promotion PR `dev → master`, the two branches diverged by SHA (identical *content*, different *commit hashes*) because rebase rewrites history. A subsequent hotfix PR branched from the new `master` failed to rebase cleanly back onto `dev`, forcing the workaround on step 11 of this runbook (temporarily remove `dev` branch protection + force-push `dev ← master`). The question is whether this is worth fixing structurally.

### Why it happens

With **rebase-only** as the default merge strategy (per `plans/WORKFLOW_RULES.md` §2), every merged PR gets its commits *copied* onto the target branch with new SHAs. That's great for linear history on feature branches — no merge commits, easy to read `git log` — but it creates a predictable problem at branch boundaries:

1. PR #A opens `dev` → `master` (promotion). Rebase-merge copies the 5 `dev`-only commits onto `master` as 5 new commits (new SHAs). `master` now has `dev`'s *content*. `dev` still has the *original* commits. **Identical content, different SHAs.** ~~Merge base is the last pre-promotion commit on both branches.~~
2. PR #B opens `hotfix-branch` (branched from new `master`) → `dev`. Rebase-merge tries to replay the hotfix commit onto `dev`. Git walks back to find the merge base between `hotfix-branch` and `dev`. The merge base is that last pre-promotion commit — so git thinks the hotfix branch has to replay the 5 promoted commits *again* onto `dev`. Those commits already exist on `dev` (same content, different SHA), so git hits conflicts on every line the promotion touched.
3. Workaround: force-push `dev` to match `master` (erasing the old SHAs on `dev`), then retry the rebase. This is what step 11 of this runbook *used to* do — it is no longer what step 11 does, and the force-push is now forbidden (see step 11). Adopting option (b) below removed the need for it in 2026-04-13; decentralised `dev` merges made it unsafe on 2026-07-22.

This is not a Datanika bug — it's an inherent limitation of rebase-only merge strategies on stacked protected branches. Every team that runs `dev → staging → prod` with rebase hits it eventually. GitHub's own "Allow rebase merging" docs warn about it obliquely.

### Options

**(a) Accept the workaround per promotion** (status quo)
- Every promotion PR is followed by step 11: remove dev protection, `git push origin master:dev --force`, restore protection.
- Cost: 30 seconds of manual ceremony per promotion. The window where `dev` is unprotected is small and scoped.
- Risk: if Infra forgets step 11, the next hotfix PR silently conflicts until someone notices. Easy to forget because the previous promotion *appears* successful.
- Fails open: worst case is the next PR can't rebase, which is immediately visible in GitHub's PR UI.

**(b) Switch promotion PRs to merge-commit** (keep feature PRs on rebase)
- Configure GitHub branch protection to allow merge commits on promotion PRs only. Or, since GitHub doesn't support per-PR merge strategy in branch protection, just use `gh pr merge --merge` (not `--rebase`) for promotion PRs and keep `--rebase` for feature PRs.
- Effect: `master` gets a real merge commit pointing at `dev`'s HEAD. Now `dev` and `master` share the same SHA for every pre-promotion commit. The next hotfix PR from `master` → `dev` rebases cleanly because the merge base is the promotion merge commit, and no commits are duplicated.
- Cost: one merge commit on `master` per promotion (reads as "Merge pull request #N from dev"). History is no longer strictly linear on `master`, but `master` is a release branch — linearity there is cosmetic, not structural.
- Risk: mixing merge strategies is a mental-model tax. Engineers have to remember "feature PRs rebase, promotion PRs merge." Easy to get wrong if you use `gh pr merge` without the explicit flag.

**(c) Squash-merge promotion PRs** (keep feature PRs on rebase)
- Instead of copying N commits, promotion PR becomes one squashed "Promote dev → master: <features>" commit on `master`.
- Effect: `master` has a single commit per promotion. `dev` still has the individual feature commits. The merge base logic breaks in a different direction — `master` no longer contains any of `dev`'s individual feature commits, so a hotfix from `master` → `dev` actually rebases cleanly (the hotfix commit doesn't know about the features, and git doesn't try to duplicate them).
- Cost: `master`'s git log stops being useful for forensics ("which feature broke this?" requires cross-referencing PR descriptions). Bisecting on `master` is also useless because every commit is a batch.
- Risk: loses per-feature blame on `master`. If a production bug correlates to a specific feature in a promotion batch, you have to read the squash commit's PR description instead of running `git blame`.

### Recommendation

**Adopt option (b).** The mental-model tax is real but small — it's one line in this runbook ("promotion PRs use `--merge`, feature PRs use `--rebase`") — and it permanently eliminates the divergence-then-force-push dance. The single merge commit on `master` per promotion is a fair trade for not having to remember step 11 every time. It also fails *safely*: if Engineering forgets and uses `--rebase` by habit, the promotion still works, Infra just hits the same workaround as today. No new failure modes.

Option (a) is fine forever if we're disciplined. We haven't been, which is why this runbook exists.

Option (c) is tempting for the log hygiene but costs too much at debug time. Veto.

### Action items (for the next promotion)

1. **Verify branch protection on `master` / `main` allows merge commits**. Step 1 of this runbook already passes `--merge` to `gh pr merge`, but if the branch protection rule is set to *"Require linear history"* or *"Allow rebase merging" only*, GitHub will reject the merge-commit and the PR will either fail or silently fall back to rebase-merge (which is what produces the divergence). Check with:
    ```bash
    gh api repos/datanika-io/datanika-core/branches/master/protection --jq '.required_linear_history, .allow_merge_commit'
    gh api repos/datanika-io/datanika-landing/branches/main/protection --jq '.required_linear_history, .allow_merge_commit'
    ```
    If `required_linear_history: true`, that's the root cause — disable it on the prod branch (not on `dev`). Rebase-merge of feature PRs into `dev` still keeps `dev` linear; only the single promotion merge commit on `master`/`main` breaks linearity, and that's the point.

2. Once (1) is fixed, add a note to step 1 of this runbook: `# MUST be --merge, NOT --rebase — prevents dev/master SHA divergence. See "Technical decision" section below.`

3. Update `plans/WORKFLOW_RULES.md` §2 to add: *"Exception: promotion PRs (`dev → master`, `dev → main`) use merge-commit (`gh pr merge --merge`), not rebase. See `docs/runbooks/RUNBOOK_DEV_TO_MASTER.md` in datanika-core for rationale."*

4. On the next promotion after (1) lands, verify `dev` and `master` no longer require the force-push in step 11 — step 11 should become a no-op and can be removed from the runbook once confirmed on two successive promotions.

5. **Do nothing on the cloud plugin repo** (`datanika-cloud`). It's private without GitHub Pro, so it has no branch protection — rebase-only is effectively unenforceable there anyway, and promotion is different (rebuilt inside the core Docker image, no separate promotion PR).

*— Engineering recommendation, 2026-04-12. Infra owns the implementation decision; this is just a technical opinion. If you disagree, option (a) is genuinely fine and the cost is low, so don't change anything under duress.*

### Adopted 2026-04-13 (by Infra)

Option (b) is now the default promotion strategy. The 2026-04-13 promotion (landing PR #120 + core PR #95) used `gh pr merge --merge --admin` for both. Post-merge verification:

- Landing dev/main diverged by exactly 1 commit (the merge commit). Fast-forward push of dev ← main succeeded on the first try with a normal admin push — **no temporary branch protection removal needed**.
- Core dev/master same result.
- No content divergence, no SHA mismatch on feature commits.

Repo settings updated via API:
```bash
gh api -X PATCH repos/datanika-io/datanika-core  -F allow_merge_commit=true -F allow_rebase_merge=true -F allow_squash_merge=false
gh api -X PATCH repos/datanika-io/datanika-landing -F allow_merge_commit=true -F allow_rebase_merge=true -F allow_squash_merge=false
gh api -X PATCH repos/datanika-io/datanika-cloud -F allow_merge_commit=true -F allow_rebase_merge=true -F allow_squash_merge=false
```

**Feature PRs still rebase.** Only promotion PRs (`dev → master`, `dev → main`) use `--merge`. The rebase-only setting on the repo is relaxed to *allow* merge commits, but the team norm is still "rebase feature PRs, merge promotion PRs". Document in every promotion PR's title.

**Step 11 of the runbook is now effectively a no-op** for promotions that use `--merge`. Keep it in place for the edge case where someone accidentally uses `--rebase` on a promotion — the force-push workaround is still correct, just rarely needed.

---

## Known deploy pitfalls

A running list of non-obvious ways a promotion can silently break.

### Pitfall 1: GHA production environment secrets don't reach the Aweb build

**Discovered**: 2026-04-13 during landing PR #115 hotfix.

**Symptom**: deploy workflow green, CD reports success, post-deploy grep for an expected env-injected string on the live site returns zero hits. Users see a plain `<a>` where a conversion-tracked CTA should be, or a dormant analytics script where tracking should fire.

**Root cause**: the landing `Deploy Landing` workflow (`.github/workflows/deploy.yml`) does two builds:
1. A no-op `Build` step in the GHA runner (only runs to fail-fast on build errors; the output is discarded).
2. A real `Deploy to Aweb` step that SSHes to Aweb and runs `npm run build` there, then `cp -r dist/*` to `/var/www/datanika.io/`. **This second build is what ships.**

GitHub Actions secrets configured on the `production` environment are available inside the GHA runner, but `appleboy/ssh-action` does NOT forward them across SSH by default. So the remote `npm run build` on Aweb runs with an empty shell env and Astro's `import.meta.env.PUBLIC_*` substitutions fall back to `undefined`. The failure is completely silent — the build succeeds, the site ships, and the feature just doesn't work.

**Fix** (already in deploy.yml as of PR #115):
```yaml
- name: Deploy to Aweb
  uses: appleboy/ssh-action@v1
  env:
    PUBLIC_GOOGLE_ADS_CONVERSION_LABEL: ${{ secrets.PUBLIC_GOOGLE_ADS_CONVERSION_LABEL }}
  with:
    envs: PUBLIC_GOOGLE_ADS_CONVERSION_LABEL
    script: |
      ...
      export PUBLIC_GOOGLE_ADS_CONVERSION_LABEL
      npm run build
```

Three things must all be in place: (1) the `env:` block on the step so GHA exposes the secret to the action, (2) the `envs:` with parameter naming the vars to forward, (3) the `export` inside the remote script so `npm` sees it. Missing any one silently breaks.

**When adding a new build-time secret to landing**:
1. Add to the `env:` block of the Deploy to Aweb step
2. Add to the `envs:` list (space-separated if multiple)
3. Add an `export <VAR>` in the remote script before `npm run build`
4. Post-deploy: grep the live site for a substring that proves the substitution happened (not just a 200 OK)

### Pitfall 2: Pydantic Settings env var naming

**Discovered**: 2026-04-13 during core PR #95 post-deploy Plausible env flip.

**Symptom**: set `DATANIKA_ANALYTICS_DOMAIN=...` in `.env.docker` per a PR description, rebuild app, check `/` — no Plausible script tag in the compiled `_document.js`, no behavior change.

**Root cause**: `datanika/config.py` uses `pydantic_settings.BaseSettings` with no `env_prefix` set. The field `analytics_domain: str = ""` maps directly to env var `ANALYTICS_DOMAIN`, NOT `DATANIKA_ANALYTICS_DOMAIN`. PR descriptions that use the "`DATANIKA_` prefix" are a convention from the landing/Astro side (where `PUBLIC_` is required) and do not apply to core.

**Fix**: use the exact field name as the env var, uppercased. For `analytics_domain` the env var is `ANALYTICS_DOMAIN`. Verify with:
```bash
docker exec datanika-app /app/.venv/bin/python -c \
  'from datanika.config import settings; print(repr(settings.analytics_domain))'
```
If it returns `''`, the env var name is wrong. If it returns the expected value, the settings layer is fine and the bug is downstream (component wiring or build cache).

### Pitfall 3: Reflex head_components render via React Helmet client-side

**Discovered**: 2026-04-13 while verifying the Plausible script tag post core PR #95.

**Symptom**: `curl http://localhost:3000/ | grep plausible` returns nothing even though `plausible_head_component()` returns a valid `rx.Component`, the env vars are set correctly, and the component is appended to `rx.App(head_components=[...])`.

**Root cause**: Reflex wraps head_components in `react-helmet-async` which injects into `<head>` at client mount time. The server-rendered initial HTML returned by `curl` does NOT contain the injected tags. Verification requires either a headless browser, or inspecting the compiled frontend directly:
```bash
docker exec datanika-app grep -c 'plausible.datanika.io' /app/.web/app/_document.js
```
A count of 1+ means the script WILL be injected in the browser. Zero means the component isn't reaching the bundle (config issue or wiring issue).

**Takeaway**: post-deploy curl checks are fine for content pages but insufficient for head injections. For Reflex head components, grep the compiled `.web/app/_document.js` instead of the live HTML.

### Pitfall 4: Phoenix LiveView + nginx WebSocket proxy headers

**Discovered**: 2026-04-13 while activating Plausible on Aweb (`plausible.datanika.io`).

**Symptom**: Plausible's `/register`, `/login`, `/sites`, and every other LiveView page re-renders approximately once per second in the browser. Form submit buttons are no-ops (click → nothing → page refresh). No error visible to the user. Server-side HTTP requests to the page return 200 OK and serve valid HTML.

**Root cause**: the `/etc/nginx/sites-enabled/plausible` vhost was missing the WebSocket upgrade directives:
- `proxy_http_version 1.1;`
- `proxy_set_header Upgrade $http_upgrade;`
- `proxy_set_header Connection "upgrade";`

Without them, nginx strips the `Upgrade: websocket` header on its way to `127.0.0.1:8000`, the Phoenix Cowboy server never sees the upgrade request, the LiveView socket never connects, and the client-side JS enters an infinite reconnect loop. Since LiveView forms use `phx-submit` instead of a native HTML action, form submission is also broken — the click event goes to the JS handler which queues it on the (non-existent) socket.

This was caught because the test `curl -H "Upgrade: websocket" ...` **from the nginx host itself** returned `HTTP/1.1 101 Switching Protocols` (because Plausible on `127.0.0.1:8000` accepts the upgrade directly), while the same curl **through nginx** returned the plain 200 page (because nginx was stripping the upgrade header).

**Fix** (already applied in `/etc/nginx/sites-enabled/plausible` on Aweb):
```nginx
server {
    listen 443 ssl;
    server_name plausible.datanika.io;
    ssl_certificate /etc/nginx/ssl/datanika.pem;
    ssl_certificate_key /etc/nginx/ssl/datanika.key;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_read_timeout 86400;
    }
}
```

**Important**: when you `cp` an nginx vhost to a `.bak-<timestamp>` file inside `sites-enabled/`, nginx will try to load both on reload because `include /etc/nginx/sites-enabled/*;` picks up everything. You'll get "conflicting server name ... ignored" warnings. Move backups to `/root/nginx-backups/` or outside `sites-enabled/` entirely.

**Followup**: after fixing nginx, `curl -H "Upgrade: websocket" ... https://plausible.datanika.io/live/websocket` through Cloudflare returned 101 Switching Protocols (end-to-end handshake works), BUT the Plausible browser UI STILL had the LiveView reconnect loop. The client-side failure mode is different from the server-side fix and remains unresolved as of 2026-04-13. Server-side WS is a prerequisite for any LiveView to work; client-side mount is a separate unresolved issue. Plausible event ingest does NOT use LiveView and works correctly regardless. Dashboard UI debugging is deferred until a user with browser DevTools access can inspect the `/live/websocket` handshake from Chrome/Firefox Network panel — current suspicion: empty `<meta name="websocket-url" content="">` in the page source confuses Plausible's bundled LiveView client.

### Pitfall 5: Plausible self-hosted signup requires working SMTP

**Discovered**: 2026-04-13 during Plausible initial user creation.

**Symptom**: Signup form at `https://plausible.datanika.io/register` (LiveView-based) "does nothing" on submit even after registration is set to `DISABLE_REGISTRATION=false`. No user row created in DB.

**Root cause**: Plausible CE's signup flow sends a verification email via `MAILER_EMAIL=plausible@datanika.io` to SMTP at `SMTP_HOST_ADDR=localhost:25`. No SMTP listener exists on Aweb port 25. Plausible's signup changeset tries to enqueue the verification email and silently aborts (no log line, no user feedback).

**Fix**: add `ENABLE_EMAIL_VERIFICATION=false` to `/opt/plausible/plausible-conf.env` and restart the `plausible` container. This bypasses the verification email send and lets registration complete in one step. The user will land in the dashboard without any email confirmation loop.

Note: this is stacked on Pitfall 4 — even with email verification disabled, the LiveView form still needs a working WebSocket to submit. If Pitfall 4 is unresolved, the workaround is to create the user directly via Plausible's Elixir RPC:

```bash
docker exec plausible /app/bin/plausible rpc '
case Plausible.Auth.create_user("<Name>", "<email>", "<password>") do
  {:ok, u} ->
    u2 = Plausible.Repo.update!(Ecto.Changeset.change(u, email_verified: true))
    IO.inspect({:ok, u2.id, u2.email, u2.email_verified})
  {:error, cs} ->
    IO.inspect({:error, cs.errors})
end'
```

Sites:
```bash
docker exec plausible /app/bin/plausible rpc '
user = Plausible.Repo.get!(Plausible.Auth.User, 1)
for domain <- ["datanika.io", "app.datanika.io"] do
  case Plausible.Sites.create(user, %{"domain" => domain, "timezone" => "Europe/Athens"}) do
    {:ok, %{site: s}} -> IO.inspect({:ok, s.id, s.domain})
    err -> IO.inspect(err)
  end
end'
```

Goals (SQL is easier than fighting `Plausible.Goals.find_or_create/2`):
```sql
-- In docker exec plausible-db psql -U postgres -d plausible_db
INSERT INTO goals (site_id, event_name, display_name, inserted_at, updated_at)
VALUES (1, 'Pricing: Start free', 'Pricing: Start free', now(), now()),
       (1, 'Pricing: Contact sales', 'Pricing: Contact sales', now(), now())
RETURNING id, event_name;
```

**Verifying event ingest works** (independent of dashboard UI):
```bash
curl -X POST https://plausible.datanika.io/api/event \
  -H 'Content-Type: application/json' \
  -H 'User-Agent: Mozilla/5.0 Chrome/120' \
  -H 'X-Forwarded-For: 8.8.8.8' \
  -d '{"name":"pageview","url":"https://datanika.io/test","domain":"datanika.io"}'
# → HTTP/1.1 202 Accepted
```

Check ClickHouse:
```bash
docker exec plausible-events clickhouse-client -q "
  SELECT site_id, name, hostname, pathname, timestamp
  FROM plausible_events_db.events_v2
  WHERE timestamp > now() - INTERVAL 5 MINUTE ORDER BY timestamp DESC LIMIT 10"
```

### Pitfall 6: Cloudflare cache rules on multi-subdomain zones must be host-scoped

**Discovered**: 2026-04-13, ~2 hours after setting up Plausible on Aweb. Caused the Phoenix LiveView reconnect loop from Pitfall 4 to look like a WebSocket / CSP / LiveView bug when it was really a CSRF token staleness bug caused by edge caching.

**Symptom**: after adding Cloudflare cache rules for a specific host (e.g. `datanika.io` the apex landing site), a *different* subdomain on the same zone (`plausible.datanika.io`) starts misbehaving — dynamic LiveView/SPA pages return stale HTML, session tokens don't match, authenticated users get logged out randomly, or form submissions no-op. Direct origin (`curl 127.0.0.1:<port>` on the host) works perfectly.

**Diagnostic**:
```bash
curl -sI -H "User-Agent: Mozilla/5.0" https://<misbehaving-subdomain>/ | grep -iE "cf-cache|age"
```
If you see `cf-cache-status: HIT` with a non-zero `Age:` value on a page that should be dynamic, CF is caching it. That's the bug.

**Root cause**: The "Landing cache rules" ruleset I set up 2026-04-12 had 4 rules with expressions like `starts_with(http.request.uri.path, "/og/")` — **path only, no hostname filter**. Cloudflare cache rules apply to *every host on the zone*, not just the one you were thinking about. When `plausible.datanika.io` was added to the same Cloudflare zone, it inherited the "HTML 5min edge cache" rule automatically.

For Plausible specifically: Phoenix LiveView embeds a session-unique `phx-session` token inside every server-rendered page. Cloudflare cached those pages for 5 minutes. Every subsequent user got the same embedded CSRF blob. When the browser tried to open `wss://plausible.datanika.io/live/websocket?_csrf_token=<stale>`, Phoenix rejected the connection (CSRF mismatch). LiveView client reconnected, got another cached page with the same stale token, infinite loop.

**Fix** (applied 2026-04-13 ~15:30 UTC):
```bash
ZONE_ID="31614512980ce302f9bde5cd263ce649"
RULESET_ID="e3445edfb05f4e279e5c23837ad18653"   # "Landing cache rules"
FIRST_RULE_ID="<id of current first rule>"       # look up via GET /rulesets/$RULESET_ID

# 1. Add bypass rule at position 0
curl -sS -X POST "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/rulesets/$RULESET_ID/rules" \
  -H "X-Auth-Email: $CF_EMAIL" -H "X-Auth-Key: $CF_TOKEN" -H "Content-Type: application/json" \
  -d '{
    "action": "set_cache_settings",
    "action_parameters": {"cache": false},
    "description": "Bypass cache for non-landing subdomains",
    "enabled": true,
    "expression": "(http.host in {\"plausible.datanika.io\" \"app.datanika.io\"})",
    "position": {"before": "'"$FIRST_RULE_ID"'"}
  }'

# 2. Purge stale cache for the affected hosts
curl -sS -X POST "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/purge_cache" \
  -H "X-Auth-Email: $CF_EMAIL" -H "X-Auth-Key: $CF_TOKEN" -H "Content-Type: application/json" \
  -d '{"hosts":["plausible.datanika.io","app.datanika.io"]}'

# 3. Verify
curl -sI https://plausible.datanika.io/sites | grep -i cf-cache
# → cf-cache-status: BYPASS
```

**Alternative fix (not taken, but cleaner long-term)**: edit every existing path-based rule's expression to add `and http.host eq "datanika.io"` so the ruleset becomes scoped to the apex landing host and only that host. Reasons I picked the bypass-at-top approach instead: single change, explicitly documents intent, can't accidentally break the existing landing caching, any future new subdomain on the zone is implicitly not cached until we update the bypass rule (which is safer than implicitly cached by default).

**Prevention rule**: whenever you add cache rules to a Cloudflare zone that has multiple hostnames, **scope every rule by `http.host` explicitly**. A position-0 bypass rule alone is NOT sufficient — see the critical correction below.

**Related hosts to watch on datanika.io zone**: `datanika.io` (cache OK), `www.datanika.io` (cache OK, also 301s to apex), `app.datanika.io` (BYPASS — Reflex dynamic), `plausible.datanika.io` (BYPASS — Plausible LiveView). Any new subdomain added to the zone needs explicit inclusion in the bypass rule AND exclusion from every path-based rule.

### Critical correction — 2026-04-13, ~2 hours after the original fix

**What I thought worked**: added a position-0 bypass rule with `(http.host in {"plausible.datanika.io" "app.datanika.io"})` → `cache: false`, verified one curl showed `cf-cache-status: BYPASS`, marked the pitfall resolved.

**What actually happened**: the user came back 2 hours later and Plausible's login was still 403'ing on form submit. Investigation showed `cf-cache-status: HIT` with `Age: 85` on `/login` — still cached. The bypass rule was NOT effective.

**Root cause of my mistake**: Cloudflare's `set_cache_settings` action does **NOT stop subsequent rule evaluation the way firewall rules do**. Cache rules are *merge-style* — every matching rule contributes its settings, and later rules override earlier ones. So:

1. `/login` request to `plausible.datanika.io` arrives at CF edge
2. Rule 0 (my bypass, `http.host in {"plausible.datanika.io"}`) matches → sets `cache: false`
3. CF **keeps evaluating** rules (does not stop at first match for cache phase)
4. Rule 4 (HTML catch-all, `extension in {"", "html"}`) matches by path → sets `cache: true, edge_ttl: 300, mode: override_origin`
5. Rule 4's settings override rule 0's → CF caches the page for 5 minutes, overriding origin's `cache-control: max-age=0, private, must-revalidate`
6. Next user's request gets the cached HTML with stale CSRF token → Plausible 403 on form submit

**Why my single curl verification fell for it**: curl from my workstation hit a CF edge node that happened not to have a cached entry yet, so the first request passed through and returned `BYPASS` (because there was nothing to HIT). The bypass was correctly applied to that first uncached request — CF just wasn't honoring it for subsequent requests. Repeated serial requests from a single browser session, on the other hand, consistently hit the SAME edge node which DID have a cached entry.

**The real fix**: scope every path-based cache rule by `http.host eq "datanika.io"` so they can't match subdomain requests at all. Don't rely on an earlier bypass rule to block them.

```bash
# Example: patch the HTML catch-all rule to add the host filter
curl -sS -X PATCH "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/rulesets/$RULESET_ID/rules/$HTML_RULE_ID" \
  -H "X-Auth-Email: $CF_EMAIL" -H "X-Auth-Key: $CF_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "action": "set_cache_settings",
    "action_parameters": {
      "browser_ttl": {"mode": "respect_origin"},
      "cache": true,
      "edge_ttl": {"default": 300, "mode": "override_origin"}
    },
    "description": "HTML — 5 min edge cache (scoped to apex datanika.io only)",
    "enabled": true,
    "expression": "(http.host eq \"datanika.io\" and not starts_with(http.request.uri.path, \"/og/\") and not starts_with(http.request.uri.path, \"/_astro/\") and http.request.uri.path.extension in {\"\" \"html\"})"
  }'

# Purge cache for the affected hosts
curl -sS -X POST "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/purge_cache" \
  -H "X-Auth-Email: $CF_EMAIL" -H "X-Auth-Key: $CF_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"hosts":["plausible.datanika.io","app.datanika.io"]}'
```

**How to verify it's actually fixed, not just "passed one curl"**: hit the URL **three or more times in a row** from the same client and check:
1. No `cf-cache-status: HIT` on any of them (ideally no cf-cache-status header at all — CF omits it when the origin explicitly opts out of caching)
2. Every response has a DIFFERENT `set-cookie` session token — if the token is identical across requests, you're still reading from cache
3. `Age:` header absent or 0 on every response

Keep the position-0 bypass rule as defense-in-depth — if someone adds a new path-based rule later without scoping by host, the bypass will catch it. But the bypass alone is NOT a sufficient primary defense. **Host-scope every rule that could match a subdomain.**

### Pitfall 7: Cloudflare Bot Fight Mode + fresh incognito sessions

**Discovered**: 2026-04-13 during Plausible dashboard login testing.

**Symptom**: user opens a fresh incognito tab, visits `https://plausible.datanika.io/`, receives a plain 403 Forbidden with no clear error page. No Plausible branding, no CF-branded challenge page, just "403 Forbidden". Repeated visits in the same session sometimes work, sometimes don't. Non-incognito sessions work fine.

**Root cause**: Cloudflare **Bot Fight Mode** (`fight_mode: true` under `/zones/:id/bot_management`) was enabled on the zone. BFM is a free-plan anti-bot feature that uses heuristics (no cookies, no history, first-visit patterns, browser fingerprint anomalies) to identify bots. It's notoriously false-positive prone on fresh incognito sessions, particularly for authenticated apps where users start out cookieless.

Related settings that can compound: `browser_check: on` (Browser Integrity Check, checks for malformed UA / missing headers), `security_level: medium` (more aggressive IP reputation filtering). BIC and security_level can be overridden per host via a Configuration Rule (`http_config_settings` phase); **BFM cannot** on free plan — it's zone-wide.

**Diagnostic**: the 403 page, when inspected carefully, has no Plausible/nginx/Phoenix branding. It's a plain Cloudflare edge response (sometimes with CF logo and Ray ID, sometimes blank depending on browser). Compare against the app's own 403 which is branded.

**Fix options**:
1. **Disable Bot Fight Mode zone-wide** (what I did on 2026-04-13). Loses some automated protection on the landing site but none of the real defenses (landing has CSRF + rate limiting on forms + reCAPTCHA on signup anyway). Keep AI bot protection (`ai_bots_protection: "block"`) so GPTBot/ClaudeBot/etc. are still blocked.
    ```bash
    curl -sS -X PUT "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/bot_management" \
      -H "X-Auth-Email: $CF_EMAIL" -H "X-Auth-Key: $CF_TOKEN" \
      -H "Content-Type: application/json" \
      -d '{"fight_mode": false, "enable_js": true, "ai_bots_protection": "block"}'
    ```
2. **Add a Configuration Rule to relax BIC + security_level for authenticated subdomains** (also done, lives at `http_config_settings` phase). Doesn't affect BFM directly but lowers the compound false-positive rate. Expression: `(http.host in {"plausible.datanika.io" "app.datanika.io"})` → `{"bic": false, "security_level": "essentially_off"}`.
3. **Upgrade to CF Pro** if you want host-scoped Bot Management overrides. Not worth $20/mo for this single use case.

**Prevention rule**: if you ever re-enable BFM zone-wide, verify manually from an incognito session that login still works on every subdomain — the 403 is silent, CF doesn't log it visibly on free plan, and you only find out when a user complains.

### Pitfall 8: Stale local venv after `dev` adds a new dep

**Discovered**: 2026-04-16 during V2 P1 shipment when the main checkout's venv hit `ModuleNotFoundError: mcp` running `precheck.sh` against an infra worktree whose dev-tracking branch had already picked up `mcp>=1.0.0` (core#153, MCP server).

**Symptom**: `precheck.sh` ruff pass is green, pytest collection fails at import time with `ModuleNotFoundError` on a package that exists in `pyproject.toml` on `dev` but not in the installed venv. Manually importing from the same venv (`python -c 'import mcp'`) reproduces.

**Root cause**: the main checkout's venv at `D:/Projects/Datanika/datanika/.venv` is installed once and then drifts. When a feature PR on `dev` adds a new runtime dep, the main venv doesn't auto-install it, and any worktree using that venv as its fallback Python interpreter gets `ImportError` at collection time. `precheck.sh` now auto-prefers `<worktree>/.venv` when present (hardened 2026-04-16), but the fallback path still needs an occasional resync.

**Fix** — one-liner to resync the main venv against whatever `datanika/` is currently checked out to:
```bash
cd /d/Projects/Datanika/datanika
.venv/Scripts/python.exe -m uv pip install -e '.[dev]'
```
If the main checkout is parked on `master` but you need `dev` deps, either fetch + switch first or create a worktree-local venv inside the worktree you're working in:
```bash
cd /d/Projects/Datanika/worktrees/datanika-core-<agent>
uv venv && .venv/Scripts/python.exe -m uv pip install -e '.[dev]'
```
After this, `precheck.sh` will auto-pick the worktree-local venv on the next run — no further resync needed for that worktree.

**Prevention**:
1. When adding a runtime dep on `dev` (e.g. `uv add mcp`), note in the PR description that a venv resync is required for anyone running local tests.
2. Prefer creating a worktree-local venv per canonical worktree so each agent's environment is pinned to the branches they actually work on — this is exactly why `precheck.sh`'s venv detection was hardened to check `<worktree>/.venv` first.
3. When onboarding a new worktree, follow with `uv venv && uv pip install -e '.[dev]'` inside it before the first `precheck.sh` run.

### Pitfall 9: Stacked PRs auto-close when the parent branch is deleted on merge

**Discovered**: 2026-04-16 during the atomic sweep Phase A. Merging core#164 with `--delete-branch` auto-deleted its base branch `162-bytes-metering-migration`, which GitHub used as the base ref for stacked child PR #171. When the base branch disappeared, GitHub auto-closed #171 as "not mergeable."

**Symptom**: a PR that was OPEN and CI-green goes to CLOSED state immediately after a different PR merges. The closed PR's branch still exists on the remote and its commits are intact — only the PR object is closed.

**Root cause**: GitHub tracks each PR's base ref. When the base branch is deleted (either manually or via `delete_branch_on_merge`), any PR whose base pointed at that branch is auto-closed. This is by design — GitHub assumes the PR's diff is no longer meaningful since its base disappeared.

**This bites us because** our repos have `delete_branch_on_merge=true` (set 2026-04-12) and stacked PRs are occasionally used for dependent work (e.g., migration PR as base, scaffolding PR stacked on top).

**Recovery** (what worked for #171 → #174):
```bash
# 1. Check out the stacked branch
git checkout origin/<stacked-branch>

# 2. Rebase onto the new target (usually origin/dev, which now contains the parent's commits)
git rebase origin/dev
# Git detects the parent's commits as content-duplicates and skips them automatically

# 3. Force-push the rebased branch
git push --force-with-lease origin <stacked-branch>

# 4. Open a new PR from the rebased branch (the old PR is closed and can't be reopened cleanly)
gh pr create --base dev --head <stacked-branch> --title "<original title>" --body "<note: replacement for auto-closed #NNN>"
```

**Prevention** — do one of these before merging the parent PR:
1. **Retarget the child PR to `dev`** before merging the parent: `gh pr edit <child> --base dev`. This decouples the child from the parent's branch lifecycle. The child's diff will temporarily show the parent's changes too, but after the parent merges to dev, the child's diff auto-narrows to just its own commits. This is the simplest option.
2. **Merge the parent without `--delete-branch`** and manually delete the branch after the child PR is also merged or retargeted. Awkward because our repos auto-delete — you'd need to temporarily disable the setting or use the API to recreate the branch post-merge.
3. **Don't stack PRs at all** — use a single branch for the combined work, or split into independent branches both based on `dev`. This is cleanest but sometimes impractical when PR B truly depends on PR A's code.

**Recommendation**: option 1 (retarget child to `dev`) is the default. Do it as part of the merge checklist whenever you're about to merge a PR that has children stacked on it. A quick check: `gh pr list --base <branch-being-merged> --state open` — if non-empty, retarget each one first.

---

## Post-promotion smoke gate (core#107 Phase 1)

Every push to `master` runs two post-deploy smoke jobs:

1. **`smoke`** — shallow bash gate against curated URLs (`.github/smoke-check.sh` + `smoke-urls-core.txt`). Catches 404s and empty bodies.
2. **`smoke-prod`** — pytest suite in `scripts/smoke/` against `https://app.datanika.io` + `https://datanika.io`. Catches shape regressions (tier_count changed, agent-guide.md shrunk, openapi.json shape broke) that the URL gate can't distinguish.

Both run after `deploy` succeeds. `smoke-prod` needs `smoke` green first (to avoid dogpiling Telegram on a deep-404 outage). Failure in either triggers a Telegram alert.

### If smoke fails in prod

The deploy already happened — smoke runs **post-deploy**. You have two options:

**Option A — fix-forward (preferred)**: identify the regression, push a fix to `dev`, promote again. Fastest if the fix is small and obvious.

**Option B — revert (when fix-forward is risky or unclear)**:

```bash
# 1. SSH to prod (pointer.gr — the Hetzner box was terminated 2026-07-14)
ssh -i ~/.ssh/id_ed25519 root@185.25.22.188

# 2. Check what is actually serving. NOT `git log` — /opt/datanika has no .git on this
#    host (tar-transferred source). The serving colour is the fact you need:
cat /etc/apache2/conf-enabled/datanika-prod-active.conf
docker ps --format '{{.Names}}\t{{.Image}}\t{{.Status}}' | grep datanika

# 2a. If the previous colour is still up, swap back FIRST (seconds, no rebuild) and only
#     then do the GitHub revert below at your own pace. See the Rollback section above.

# 3. Revert the promotion merge on GitHub
gh pr view <promotion-pr-number> --repo datanika-io/datanika-core
# Create a revert PR — GitHub's revert button on the merged PR is the cleanest path.

# 4. Merge the revert, which triggers a fresh deploy with the prior code
gh pr merge <revert-pr-number> --repo datanika-io/datanika-core --merge --admin

# 5. Wait for the fresh deploy + smoke to go green
gh run watch <run-id> --repo datanika-io/datanika-core --exit-status
```

**Rollback constraints**:
- Revert PR must be from `master` back to a prior `master` SHA. Don't try to revert by force-pushing `master` — CD watches push events, not content.
- Alembic migrations are NOT automatically rolled back. If the broken deploy ran a migration, the revert only reverts the code — you may need `alembic downgrade -1` manually.
- If the revert itself would undo a schema change that downstream code depends on, coordinate with Engineering before merging the revert.

**Prevention**: the `smoke-prod` job's shape assertions are intentionally brittle — `tier_count == 5`, `capability_count == 8`, etc. When Product legitimately changes a SoT, update the smoke assertion **in the same PR** as the SoT change. If you see the smoke job failing on your PR in the master CI, that's the signal to coordinate the update.
