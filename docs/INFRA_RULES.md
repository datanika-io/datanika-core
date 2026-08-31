# Rules earned from incidents — Infrastructure

Each rule below cost a real outage, a silent-failure window, or a wrong conclusion drawn
confidently. **None of these is a work item.** Do not convert one into an issue; issues are for
things to *do*, and these are things to *know* before doing.

**Why this file is in git.** These rules were carried in `plans/infra/PLAN_INFRASTRUCTURE.md`
(272 KB, deleted 2026-08-31), in `plans/infra/current_state.md`, and in the monorepo root
`CLAUDE.md`. The first is gone. The second is required by `WORKFLOW_RULES` §11 to be **rewritten
from scratch every session**, so a rule kept only there is one careless rewrite from gone. And the
third is the trap worth naming: **`CLAUDE.md` is also outside every git repo** — no reflog, no
remote, no recovery, exactly like `plans/`. "It's safe, it's in CLAUDE.md" was never true.
`plans/growth/SEO_KEYWORDS.md` (~36 KB) was destroyed by a truncating write on 2026-08-30 and could
not be restored; that is the failure mode this file exists to avoid.

---

## 1. Evidence — what your signal actually records

**Ask what a green signal is physically capable of recording.** This is the highest-yield rule here,
and every subsection below is an instance of it. A check that cannot observe the failure mode
reports success in exactly the same bytes as a check that observed health.

**A dead service still answers ping.** The provider resolver `62.217.126.164` answered ICMP perfectly
while timing out 10/10 DNS queries. `systemd-resolved` therefore kept it in rotation and paid a full
timeout whenever it selected it — 7.9–9.5 s per lookup, and a cold-cache `api.paddle.com` measured
**20.1 s**. Reachability and function are different questions; probe the function.

**A systemd unit reading `enabled` + `active` is not evidence that the thing it configures is on.**
Measured 2026-08-31 on the Aweb box: `systemctl is-enabled unattended-upgrades` returned `enabled`,
`is-active` returned `active`, and the `apt-daily-upgrade` timer had fired 19 h earlier — while
`apt-config dump` (APT's own view, the authoritative one) read
`APT::Periodic::Unattended-Upgrade "0"`. The timer ran on schedule, read the policy, and correctly
did nothing. `unattended-upgrades.log` had been **0 bytes since January**. The unit was healthy; the
work had never happened. **Verify by effect** — the log the job writes, the packages actually
installed — never by the status of the thing meant to do it.

**CD verified Grafana rule *text* matched the repo for weeks while every rule failed to evaluate.**
The deploy asserted that the right bytes arrived, which is not the same claim as "the rule can run."
Deploy-time checks must assert **evaluability**, not delivery. Two reusable tells: a FIRING→RESOLVED
gap exactly equal to the query window means the rule is firing on *window contents* rather than on a
condition; and that failure surfaced **only in the Grafana rules API**, never in `docker logs` — so
"the log grep found nothing" is not a refutation.

**A `curl` without `-L` on a page that 301s to its trailing slash returns ~178 bytes of redirect,** so
every content grep comes back empty — which reads exactly like *"the fix did not ship."* Check
`size_download` before believing a zero match.

**An absent scheduled run before its slot is not evidence of a missed run.** The dev box's clock is
UTC+3 and rolls to the next date three hours early; it read `2026-08-31 01:18` while UTC was still
`2026-08-30 22:18`. Compare against `date -u`, never the local clock.

**Print the denominator.** A bare zero reads as a broken instrument or a dead system, and is usually
neither.

---

## 2. Alerting

**`for:` cannot debounce a filtering expression, and raising it is the wrong fix.** Every rule in
this repo is a *filtering* expression — healthy is the empty set, which is why they all carry
`noDataState: OK`. On such a rule `reduce: last` never sees a recovery, so **one 15-second failed
sample pages critical**. Observed, not theoretical: `app-unhealthy` did exactly that at
`2026-08-29T16:31:40Z` on a single failed scrape, while the app had been up five weeks.

> **The duration belongs in the query, not in `for:`.** Use
> `count_over_time((<filtering expr>)[2m:15s])` with `gt [3]` and `for: 0s`.

Applied to all five availability rules that needed it. Keep the subquery step at the scrape interval
— a `1m` step against a `15s` scrape silently under-counts, and the inner range bounds how long a
series is *counted*, not how many samples must agree.

**An audit finds the class of defect it was written to find, and is blind to every other class.**
The 2026-08-29 audit classified all 30 rules by **debounce shape** and passed them.
`container-high-memory` was defective in *which series it selects* — `name=~"datanika-(app|celery)"`
does not match `datanika-app-b` — so it watched celery alone for 719 of the last 720 hours. The audit
could not have caught it, and the rule had been green for five weeks. **When an audit reports clean,
state the axis it examined**; a clean result on one axis is not a clean result.

**A rule can be structurally unable to fire and look identical to a rule with nothing to report.**
`container-restart-loop` and `pg-slow-queries` were both green for weeks and both could never fire —
one because `increase()` returned 0 series, one because `--collector.stat_statements` was never
enabled. Silence is not a measurement.

**Two corrections worth carrying, because each issue's own proposed fix was wrong:**

- The restart-loop fix as proposed kept `name=~"datanika-.*"`, and **PromQL anchors regexes**, so it
  matches `datanika-staging-*` — redeployed on every push to `dev`. Backtested at 82 five-minute
  buckets of false `critical` pages in 30 days.
- The slow-query issue's premise was wrong: the metric name in the rule
  (`pg_stat_statements_seconds_total`) had been correct all along; only the collector flag was
  missing. **Re-derive the premise before implementing the fix it implies.**

**An absence alert inverts this repo's convention.** Everything here is a filtering expression with
`noDataState: OK`. To alert on something *not arriving*, use `absent(...)` — the idiom the meta-rules
at the end of `alerts.yml` already use — and keep the in-query debounce.

**Nothing watches disk between deploys.** The deploy fails under 5 GiB free and warns under 20 GiB,
which covers deploy time only. `docker ps` reads healthy and `df -h` reads a comfortable percentage
while image accumulation goes unobserved.

---

## 3. Blue/green — the colour is never guessable

**Read the serving colour; do not infer it.** The colours alternate on *every* deploy:

```bash
cat /etc/apache2/conf-enabled/datanika-prod-active.conf
#  8000 / 3000  = BLUE   (datanika-app)
#  8010 / 3010  = GREEN  (datanika-app-b)
```

Exactly one colour should be running an hour after a deploy; two is a normal *transient* mid-swap.

**Never hardcode a colour anywhere.** A container name, a Prometheus job, or a `name=~` regex that
omits `(-b)?` silently watches nothing for as long as the other colour serves. That is precisely how
`container-high-memory` hid for five weeks. Every selector naming an app container must match both.

**Recovery is a single `EXIT` trap — do not add an `ERR` trap.** `ERR` fires on a strict subset of
the failure paths, and its presence is what once made an unreachable rollback look like a working
one. A failed pre-repoint assertion must leave production on the **old** colour, untouched.

**Assert against the target's own backend port before repointing Apache**, not after: `/healthz`,
`/mcp` (expect 401) and both OAuth `.well-known` documents.

**Celery is deliberately not blue/green** — it serves no HTTP, and two workers on one broker would
double-consume. It gets recreated instead. Workers restart separately from the web swap, so old task
code meets a new schema for **longer** than the web tier does; that window is the reason the
expand/contract policy exists.

---

## 4. Docker, disk, and pruning

**Do not follow docker's own deprecation message on the prune flag.** The run logs
`Flag --keep-storage has been deprecated, keep-storage flag has been changed to max-storage`.
**`--max-storage` is not accepted by `docker builder prune`.** Checked against the binary
(docker 28.1.1), whose help offers **`--max-used-space`**, which is what the script already uses.
Editing it to match the warning text degrades the prune to a non-fatal warning and silently stops
bounding the disk.

**`docker image prune` reclaims 0 bytes here, and that is not a typo.** Measured twice, including in
the first production run, which printed `Total reclaimed space: 0B` beside a build cache going
`59.08 GB → 27.52 GB` and `df` free going `71.4 → 102.6 GiB`. The dangling images and the BuildKit
cache are **the same bytes counted twice**; only a capped `docker builder prune` moves the disk.

**The prune order is load-bearing, because the rollback path is made of things that look like
garbage.** The retired colour is an `Exited (137)` container referencing a **dangling `<none>`
image**. That container plus that image *are* the rollback.

| command | verdict |
|---|---|
| `docker image prune` (no `-a`) | safe — the exited container holds a reference |
| `docker container prune` **then** an image prune | **destroys the rollback** |
| `docker system prune -a` | **destroys the rollback**, and takes the staging image |
| `docker builder prune -af` | empties the cache → next deploy compiles lxml/xmlsec from source |

Prune at the **start of the next deploy**: by then the previous deploy is proven good and the colour
being retired is two generations back. Derive the protected set from `docker ps -aq`, which also
covers the co-tenants. **Never `container prune` on this box** — it is shared, and the exited colour
is deliberate state.

**Clean up by what the platform still knows about the thing, not by the name you gave it.** Once
docker renames a container, `docker compose rm -sf <service>` walks straight past it. Filter on
`label=com.docker.compose.project`.

---

## 5. Config that is not deployed is config that does not apply

**A correct file on the box beside a container that never re-read it is worse than a wrong file**,
because the box now *looks* right. `postgres-exporter`, `cadvisor` and `node-exporter` appeared in
**no** deploy step — not the build, not the swap, not the monitoring recreate — and so ran six weeks
with no mechanism to pick up a config change, while an updated `docker-compose.yml` sat on disk next
to them. `tests/test_deploy/test_deploy_service_coverage.py` now fails when a compose service is
named by no deploy step.

The discriminating proof that a fix reached the box: after that deploy `postgres-exporter` read
`Up 2 minutes` while `cadvisor` and `node-exporter` correctly stayed `Up 6 weeks`, having had no
config change. **Plain `up -d` is a config-hash recreate** and is a no-op unless that service's own
definition changed — deliberately not `--force-recreate`, which would gap the container metrics the
alert rules read.

**Single-*file* bind mounts pin the inode**, so a `tar xzf` over them leaves the container reading the
old file forever. Prometheus, Grafana and blackbox therefore *do* need `--force-recreate`.

**A new backend Starlette route outside `/api/` needs its own Apache vhost entry, or it silently falls
through to the Reflex SPA catch-all** and serves HTML where a client expects JSON. `/mcp` and the
whole OAuth AS were both broken this way. `/oauth/consent` is the exception — it is a Reflex
*frontend* page and must keep resolving to `:3000`.

**Framework config prefixes are not interchangeable with the name you happened to set.** `REDIS_URL`
was set on prod for months; Reflex reads `REFLEX_REDIS_URL`, so with 4 Granian workers it silently
used per-process `StateManagerDisk`, and a stale state read *was a logout* — 48% of prod reconnects.
**Checking that the env var exists is not the check.** Ask the framework what it resolved:

```bash
docker exec datanika-app /app/.venv/bin/python -c \
  "import reflex.config; print(reflex.config.get_config().redis_url)"
```

**Graft installs in the `Dockerfile` are outside `uv.lock` — check the image, not the manifest.**
`uv pip install /cloud` and `uv pip install ./datanika-mcp` run *after* `uv sync --frozen` and never
consult the lock, so each may move any package whose dependency tree overlaps core's. The obvious
mechanism is wrong and should not be re-asserted: core's `mcp` pin lives in the `dev` optional group
and the image builds `--no-dev`, so nothing was clobbered — the sub-package's own unbounded pin was
the sole constraint. **A floor matters as much as a ceiling**: a floor below the version you build and
test is not a constraint but a declaration that a version you have never run is acceptable, and for a
self-hoster installing from PyPI it is the *only* constraint that exists.

---

## 6. Concurrency and mutations

**Anything shared, slow and stateful downstream of a merge needs a concurrency group at the layer
that owns the mutation — and the polarity is a deliberate choice.**

- `cancel-in-progress: true` is right for **verdicts you can recompute** (PR CI).
- It is **wrong for mutations**. Cancelling a deploy mid-flight is what wedged staging, and
  cancelling a landing publish mid-write is what made the web root non-atomic. **Mutations queue.**

**Publishing must be atomic, and a `cp -r` into a live root never is.** Stage into a fresh release
directory, assert it is serveable *before* touching anything live, then `rename(2)` a symlink over the
live path. A request then gets the whole old release or the whole new one, and a bad build leaves the
previous release serving. Deletions propagate, which `cp -r` never did.

**The release-directory allocation is a bare `mkdir` retry loop on purpose — the create *is* the
collision test.** Do not "simplify" it to `rm -rf "$REL"; mkdir -p "$REL"`: on a collision that
deletes the tree the live symlink points at, in place, and on the bad-build path it empties the live
release *before* verification fails.

---

## 7. Promotions

**`--merge --admin`, never `--rebase`.** A merge commit keeps prod a *descendant* of `dev`, so the
post-promotion resync `git push origin origin/master:refs/heads/dev` is a clean fast-forward. A
`--rebase` promotion rewrites history → identical trees, different SHAs → permanent divergence and
phantom commits on every later promotion. Real incident, 2026-07-20.

**Never `git push --force` during the resync.** A department can merge between your `fetch` and your
`push`, and the force erases their commit silently — `dev` still green, issue still closed. Assert
`compare` reads `behind`, then PATCH the ref with `force=false` so GitHub itself refuses a non-FF.

**Read CI's verdict on the exact head you are shipping** — `repos/<r>/commits/<HEAD_SHA>/check-runs`,
not the PR-level rollup. A cancelled run is neither green nor red.

**A promotion CD run carries no staging or E2E verdict at all.** `deploy-staging`, `smoke-staging` and
`e2e-staging` are gated on `github.event_name == 'push' && github.ref == 'refs/heads/dev'`, so they do
not run on a `master` push. The E2E reading for a promotion lands on the **post-promotion `dev` resync
push**, not on the deploy. Looking for it on the CD run finds nothing, and **nothing reads exactly
like clean.** (`smoke` and `smoke-prod` are different jobs and *do* gate production.)

**Landing and cloud CI are `pull_request`-only**, so the absence of a run on a `dev` head is the
workflow's shape, not a gap. Core is not like this.

**`strict = true` on `dev` makes your PR `BEHIND` if a department merges while you push.** Rebase
locally — `allow_update_branch` is false and the button adds a merge commit — then re-watch.

**Paraphrase commit subjects in a promotion body.** A quoted `closes #N` fires anyway. The
`promotion-pr-refs.yml` workflow generates the closing block from the commits actually being promoted;
do not hand-enumerate it, and give it ~1 minute before concluding it is broken. It correctly does
nothing when commits say `refs`, logging `no closing references found`.

**Promotions are not one task.** Cloud → core → landing have their own orderings and gates. Treat each
independently unless a runbook says otherwise.

**Before promoting, confirm no active user sessions would be disrupted** and check a real traffic
window in Plausible/Grafana. Under blue/green the *previously deployed* code runs against the *new*
schema while the new container migrates, and CI cannot catch a break there — it only ever runs one
version against one schema. That window is what
[`SPEC_EXPAND_CONTRACT_MIGRATIONS.md`](specs/SPEC_EXPAND_CONTRACT_MIGRATIONS.md) governs, and
enforcing it at promotion review is Infra's job. Full procedure:
[`RUNBOOK_DEV_TO_MASTER.md`](runbooks/RUNBOOK_DEV_TO_MASTER.md).

---

## 8. Releases

`0.x` SemVer, tag pattern `v*`, cut on accumulated change and never on a calendar. **A release is a
label on a `master` SHA that already deployed green** — not an approval gate, a freeze, or a branch.
Policy: [`SPEC_RELEASE_VERSIONING.md`](specs/SPEC_RELEASE_VERSIONING.md).

**Never tag the platform `mcp-v*`** — `release-mcp.yml` triggers on that and publishes the
`datanika-mcp` sub-package to PyPI. `v*` and `mcp-v*` are disjoint because globs anchor at the start;
verified in both directions, four times in practice.

**A tag build runs the workflow file as of the tagged commit**, so a fix to `release.yml` can only ever
benefit a *future* tag. Same for the private-repo checkout: a `v*` tag does not exist in
`datanika-cloud`, so tag builds must pin cloud to `master`.

---

## 9. Server changes and the pages legally bound to them

**Changing a host, a country, an email provider or a backup target is also a landing-page change.**
`datanika.io/privacy` and `/trust` state the hosting provider, country, OS, reverse proxy,
sub-processor list and backup retention as **legal representations**. Prod moved Germany → Greece on
2026-07-17 and both pages still named the old host **six weeks later**, every build green, because
nothing connected the two. Correcting it exposed a worse error the first had hidden: both pages
claimed *"no data is transferred outside the EU"* while every password-reset email went through a US
provider.

The landing test suite **cannot see production**, so it will never tell you the host changed. Update
the pages in the same batch, and add a dated row to the `/trust` change log.

**All changes go through git — never edit a server file to fix something.** The two deliberate
exceptions are `.env` secrets, and network config: a pushed network change that fails leaves the box
unreachable, so the DNS drop-in is intentionally not CD-synced.

**A canonical copy of a box file drifts, and drifts silently.** On 2026-08-31 the checked-in copies of
both Apache vhosts were **21 and 23 lines behind the box**, missing the entire `proxy-nokeepalive` fix
and its rationale. **The box is the source of truth for box config; refresh the copy from the box
rather than trusting the copy.** Canonical copies live in [`../deploy/server/`](../deploy/server/).

---

## 10. Diagnosis

**For resource-exhaustion symptoms, the first move is an empirical probe on staging** — direct-invoke
the suspect call site N times inside the container and sample the metric every 10 iterations.
Monotonic growth localizes it; flat clears it. *"The most recent change is the root cause"* is a ~40%
heuristic; the probe is ~95%.

**On `app.datanika.io`: 403 = Cloudflare edge, 502 = origin.** That distinction cost real time to
establish.

**When a CI signal costs a full cycle per question, go to the system it is describing.** One defect
survived three CI rounds and two departments because the spec asserted a run's *status* and never its
*reason*, and nobody read the database. One `psql` against staging answered it in a minute.

**Grow the smoke URL list on demand; never point a crawler at it.** `/healthz` is deliberately
excluded — the Reflex SPA catch-all makes it a permanent false positive.

🚨 **`git` exports `GIT_DIR` to its hooks — anything shelling out to git from inside a hook acts on
the REAL repository, whatever `cwd=` says.** git does **not** set `GIT_WORK_TREE` alongside it, so
git treats the *current directory* as the work tree while the metadata goes to the real `.git`.
That combination is why passing `cwd=` looks like it should isolate you and does not.

It fails in exactly one place: inside a hook. Standalone runs and CI have no `GIT_DIR`, so a test
that shells out to git passes everywhere except the context it exists to guard. Measured on
`tests/test_hooks/` — 19 of 20 error with `GIT_DIR` set, 20 pass without. When it bit, a `tmp_path`
fixture committed a stray commit onto the live branch and overwrote `user.email` / `user.name` in
the repo-local config, so a later `--amend` folded real work into a mis-authored commit.

🚨 **The effect that outlives the obvious ones: `git init` under a stray `GIT_DIR` sets
`core.bare = true` on the SHARED repo config.** Reproduced from clean: create a repo with a
worktree, run `git init` from an unrelated directory with `GIT_DIR=<the worktree gitdir>`, and
`core.bare` in the *common* config goes `false` → `true`. The main checkout then refuses every
work-tree operation while **every linked worktree keeps working** — `git rev-parse
--is-bare-repository` returns `false` inside them regardless — so nothing fails, no agent notices,
and it can sit indefinitely. `git worktree list` showing the main checkout as `(bare)` beside a
directory full of source files is the tell.

Two independent signatures date it without needing file mtimes, which are useless here because
`.git/config` is rewritten constantly: **`logallrefupdates = true` next to `bare = true` is
self-contradictory** (git writes the former only for non-bare repos, and `git init --bare` never
produces the pair), and a bare repo does not have a 40-entry working tree beside it. Restore with
`git config --file <repo>/.git/config core.bare false` — the key belongs at `false`, not deleted.

Fix for the class: strip `GIT_*` from the child environment. Reproduce any suspected case with
`GIT_DIR=/path/to/.git pytest <target>`. Applies to any hook, CI helper, or `<dept>_exec.sh` that
runs git in a temporary directory. **After any such incident, check `core.bare` explicitly** — the
commit and the identity announce themselves; this one does not.

**A gate that has never been watched refuse is not known to refuse.** The `pre-push` helm-lint
filter read `git diff @{upstream}..HEAD` with `2>/dev/null || true`; on a branch that had never been
pushed `@{upstream}` did not resolve, the error was swallowed, the file list came back empty, and
the lint was skipped — on precisely the push where a chart change was least reviewed. It read as
"no chart changes" for as long as it existed. Range resolution belongs against `origin/dev`, every
error path means *do the work*, and the refusal itself needs a test.

---

## 11. Working practices

**Everything multi-step goes through the department's allowlisted `<dept>_exec.sh`** — see
`WORKFLOW_RULES` §13 for the full set of traps. The one worth repeating here, because it disguises
itself as an infrastructure failure:

**Bash resumes a running script from a byte offset.** Overwriting `<dept>_exec.sh` while a background
invocation is still executing it makes the running job read the *new* file from that offset and
execute garbage — typically dying on a syntax error *after* it has already produced its real result,
so the failure reads like a probe failure rather than a self-inflicted one. Checking "is it still
running" does not help: the write succeeds silently and the damage appears in the other process. **If
a background job is running the script, do not touch it — run the next probe in the foreground, or
wait.** When two agents from the same department may be live, use a distinct file.

**`docker exec -i` eats the rest of an `ssh 'bash -s' <<EOF` script.** The heredoc feeds the remote
bash on **stdin**, and `-i` attaches that same stdin to the container, so it swallows every remaining
line and the run ends early **with no error** — reading exactly like a dropped connection. Use `-i`
only when deliberately piping in; otherwise `< /dev/null`.

**A test that forbids a string will match the comment explaining why the string is forbidden.** Strip
comments before searching a manifest.

**Windows specifics:** `PYTHONIOENCODING=utf-8` before any Python that prints repo text (the console
is cp1251, and a raise mid-loop makes everything after it simply absent, which reads like a clean
empty result); `UV_NO_SYNC=1` for any hand-run suite; SSH must be the Windows binary
`/c/Windows/System32/OpenSSH/ssh.exe`, because Git Bash's own `ssh` fails pubkey auth.

**A Cloudflare DNS record comment is capped at 100 characters** (API `code 9313`), and `gh api` takes
`--jq` only — `--jq -r` is invalid, and API paths take no leading slash.

---

## 12. Standing maintenance cadence

Recurring hygiene, deliberately **not** issues — an issue that reopens every month is noise. Verify by
effect, per §1.

| Cadence | Check | What proves it |
|---|---|---|
| Monthly | Backup restore drill | [`../deploy/server/restore-drill.sh`](../deploy/server/restore-drill.sh) writes a dated report |
| Monthly | SSH access log review, both boxes | read the log, not the fail2ban status |
| Quarterly | Rotate the Redis password | the app reconnects, and `.env.docker` agrees with the secrets inventory |
| Quarterly | Host patch level | `apt-config dump \| grep Periodic` **and** a non-empty `unattended-upgrades.log` |
| **By 2027-04-16** | Rotate the `datanika-staging-ci` Cloudflare Access service token | CI's `e2e-staging` still authenticates |

The `*.datanika.io` Cloudflare Origin certificate expires **2041-03-06**, so certificate-expiry
monitoring is not worth building; the dated token rotation above is the one that actually comes due.
