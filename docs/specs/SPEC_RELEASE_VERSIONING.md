# SPEC — Release & versioning policy (`datanika-core`)

> **Status: DECIDED (CEO, 2026-07-21).** Owner: Infra (mechanics) · affects Eng (version bumps) + Growth (release notes → distribution).
> Companion to [`RUNBOOK_DEV_TO_MASTER.md`](../runbooks/RUNBOOK_DEV_TO_MASTER.md) — that runbook governs *deploying*; this spec governs *labelling what was deployed*.

---

## The decision, in one line

**Adopt tagged releases. Do NOT adopt release cycles.** Two different things got bundled under "release cycles" — we want the artifact, not the cadence-process.

- ✅ **Tagged, versioned releases** (`v0.1.0` + notes) — an artifact for the open-source / self-host audience.
- ❌ **Release *cycles*** (freeze windows, RC branches, release trains, a release manager) — pure drag at 0 paying users, and it fights our standing "ship immediately pre-traffic" posture.

## Two tracks, deliberately decoupled

| Track | Audience | Mechanism | Cadence |
|---|---|---|---|
| **Continuous deployment** | our SaaS (`app.datanika.io`) | `dev → master` → `deploy-pointer.yml` | every promotion (unchanged) |
| **Tagged releases** | OSS / self-hosters / directories | `v*` tag on a `master` SHA | when meaningful change accumulates |

**A release is a *label on a `master` SHA that already deployed green*** — not a new approval gate, not a freeze, not a separate branch. This is the whole point: it must cost ~nothing.

## Why (what it unlocks — all already-felt pain)

1. **Distribution gates.** `awesome-selfhosted-data` starts its 4-month eligibility clock at *first tagged release* — so ours has **never started**. Directories broadly treat "has releases" as the *is this real?* signal.
2. **Actionable security advisories.** The SAML GHSA had to say "update to `master` @ `6e28d6b` or later" — a self-hoster can't tell if they're affected. `Affected: < v0.1.0 · Patched: v0.1.0` is instantly legible.
3. **Self-hosters get something pinnable — and it is the SOURCE TAG.** ✅ *Delivered.* Today the honest answer to "what version are you running?" would otherwise be a git SHA — painful for support and upgrade decisions the moment we have adopters. `git checkout v0.1.0` answers it, and that is the whole of what a release owes a self-hoster. The image is **deliberately not** part of this; see "The self-host path is clone-and-build" below.
4. **A recurring marketing artifact.** Release notes → blog/social → the "project is alive" signal that drives OSS adoption. Feeds the existing blog-announcement rule with a natural rhythm.

## Rules

- **Scheme: `0.x` SemVer.** Matches `datanika-mcp`, which is already versioned (0.1.0 → 0.2.0). We are *already* doing this for the sub-package; this extends it to the platform.
- **No `1.0` yet.** 1.0 implies API-stability guarantees we don't want to owe pre-launch. Revisit when we commit to a stable public API contract.
- **Tag pattern: `v*`** (e.g. `v0.1.0`). ⚠️ **Never `mcp-v*`** — `release-mcp.yml` triggers on that and would try to publish the MCP sub-package.
- **Cadence: on meaningful accumulation, not the calendar.** A forced monthly tag with nothing in it is worse than no tag.
- **NOT one tag per `dev→master` promotion.** Raised and rejected 2026-07-21: promotions are far more frequent than releases — **six** happened in a single evening — so tagging each would emit `v0.1.1`…`v0.1.6` in one night, and every `v*` tag fires *two* workflows (GitHub Release + a pinnable GHCR image). That is calendar-cadence by another name: it decouples the tag from "meaningful accumulation" and re-couples it to deploy frequency. A promotion is a deploy; a release is a label you choose to put on one.
- **Release notes: generated from merged PR titles.** Our `[Dept] … (closes #N)` convention makes this nearly free.
- **Docker images tagged `:v0.x.y` alongside `:latest`.** These exist and are **private, by
  decision** — they are our own deploy and rollback artifacts, not a distribution channel.
  A `v*` tag still builds one; nothing outside the org can pull it, and nothing outside the
  org is meant to. See the next section.

## The self-host path is clone-and-build — a tag pins a SOURCE revision

> **DECIDED (founder, 2026-09-04), closing [#1014].** This section replaces the "self-hosters
> pin a tag/image" promise this spec carried from 2026-07-21.

**What a self-hoster does:**

```bash
git clone https://github.com/datanika-io/datanika-core.git datanika
cd .. && docker compose -f datanika/docker-compose.yml up -d --build
```

They clone and they build. That is what our own README and quickstart tell them to do, it is
the path CI exercises, and it works today.

**So a tag's job is to pin a source revision**, and it does that completely:

| a release gives you | still true? |
|---|---|
| `git checkout v0.1.0` — a named, reproducible source revision to build from | ✅ |
| `Affected: < v0.1.0 · Patched: v0.1.0` in a security advisory, instead of a commit SHA | ✅ |
| a changelog generated from merged PR titles | ✅ |
| the `awesome-selfhosted-data` 4-month eligibility clock, started at the first tag | ✅ |
| `docker pull ghcr.io/datanika-io/datanika-core:v0.1.0` | ❌ **and not planned** |

**Why the image is not published**, recorded so nobody re-opens it as an oversight:

1. **Nothing published ever promised it.** Growth measured the landing source: **zero**
   `ghcr.io` references and **zero** `docker pull` commands. The core README's `docker pull`
   line was corrected on 2026-09-03. The only artifact that ever claimed a pullable image was
   *this spec* — so the promise was the defect, not the packaging.
2. **Publishing costs an irreversible decision for zero measured demand.** GHCR visibility is
   per **package**, not per tag, so "publish the core-only tag, keep the rest private" is not
   a configuration that exists — it needs a **second package**, and making a package public is
   a one-way door. Against that: 0 stars, 11 unique human viewers, and no external issue in the
   repository's life.
3. **The private visibility is load-bearing on the image we DO build.** The default edition
   grafts the closed-source `datanika-cloud` tree at `/cloud/`. Private is what stands between
   a routine workflow and publishing a private repository.

### ✅ The core-only image variant STAYS — this decision is "do not publish", not "undo the work"

`DATANIKA_IMAGE_EDITION=core` (2026-09-04, [#1014]) is merged, costs nothing, and is asserted on
every PR by `core-only-image` in `ci.yml` — from a context with no `datanika-cloud/` in it,
measured **on the built artifact** rather than on the Dockerfile: `/cloud` absent,
`datanika_cloud` not importable, neither repository's `.git` present, the worker entrypoint
imports. 2.37 GB against the cloud image's 2.42 GB. It earns its keep three ways with nothing
published:

- It is **the only assertion on a real image a fork PR gets** — it needs no `CLOUD_REPO_TOKEN`,
  where `image-probe` and `image-cve` skip every step and report green on a fork.
- It proves the AGPL core builds and runs **without the private tree present**, which is exactly
  the context a self-hoster builds in. Nothing else tests that.
- If publishing is ever revisited, the correct artifact already exists and is verified. The
  decision stays a decision rather than becoming a project.

⚠️ **The `Dockerfile.dockerignore` fix stands entirely on its own merits and is unaffected by
this decision.** An image carrying the private repository's full git history is wrong whether or
not anyone can pull it: it is wrong in the registry, wrong in a rollback artifact on the box, and
wrong the moment a visibility setting is ever changed by someone who does not know it is
load-bearing. It is asserted on the pushed image (`test ! -e /cloud/.git`), not on the ignore
file's text.

**If this is ever revisited**, the question to answer first is *who asked* — a named self-hoster
who tried and could not, not a directory checklist. Until then, the honest artifact is the tag.

## Explicitly out of scope

Release trains · freeze windows · RC/`release-*` branches · a release manager · calendar-driven cadence · **auto-tagging every promotion** · `1.0.0` · **a publicly pullable container image** (founder, 2026-09-04 — see the section above).

## ⚠️ If anyone ever automates tagging, read this first

Tagging is currently **manual by decision**, not by omission. Should that be revisited, there is a trap that makes a naive implementation look like it works while doing nothing:

**GitHub does not fire workflows for events created with `GITHUB_TOKEN`** (deliberate, to prevent recursion). Both `release.yml` and `build-push-image.yml` trigger on `push: tags: ["v*"]` — so a tag pushed by a workflow using the default token triggers **neither**. The result is a tag with no GitHub Release and no pinnable `:v0.x.y` image: a silent no-op that reports success.

Workable approaches: have the tagging job create the Release itself (`gh release create --generate-notes --verify-tag`) and dispatch `build-push-image.yml` (`--ref <tag>`, it has `workflow_dispatch`); or push the tag with a PAT rather than `GITHUB_TOKEN`. Either way, **verify the Release and the image actually exist afterwards** — the tag's existence proves nothing.

Also: any automated path must be idempotent (skip a SHA that is already tagged) and must refuse to emit anything matching `mcp-v*`, which publishes to PyPI irreversibly.

## Implementation checklist (Infra)

> **Implementation landed 2026-07-21** via core [#396] → promoted [#397] (master `f5cf829`). Mechanics below are live; only the first tag remains.

- [x] **DONE 2026-07-21 — cut `v0.1.0`** (annotated tag on `master` `f5cf829`, only after that SHA's CD went green: deploy + smoke + smoke-prod all success — honouring "a release labels a SHA that already deployed green"). Fired exactly the two intended workflows: `release.yml` published the [GitHub Release](https://github.com/datanika-io/datanika-core/releases/tag/v0.1.0) and `build-push-image.yml` pushed `ghcr.io/datanika-io/datanika-core:v0.1.0` (+ `:f5cf829`). **`release-mcp.yml` did NOT fire** — the `v*`/`mcp-v*` disjointness confirmed in practice, not just in theory. Notes: being the *first* tag, auto-generation spanned the entire history (233 PRs / 31 KB), so a short factual header was prepended and the changelog collapsed into a `<details>` block. **Starts the awesome-selfhosted eligibility clock → eligible ~2026-11-21.**
- [x] **DONE 2026-07-21 — `v*`-triggered release workflow** (`.github/workflows/release.yml`): publishes a GitHub Release with notes auto-generated from merged PR titles. **No collision with `release-mcp.yml`** — globs anchor at the start, so `v*` never matches `mcp-v0.1.0`; verified both directions. Both workflow YAMLs validated with `yaml.safe_load` before commit (this repo has been bitten by a silently-uncompilable workflow: #77/#78).
- [x] **DONE 2026-07-21 — Docker `:v0.x.y` alongside `:latest`** (`build-push-image.yml` now also builds on `v*` tags). ⚠️ **Bug caught while implementing:** tag builds would have failed the private cloud checkout — it used `ref: ${{ github.ref_name }}`, which for a tag resolves to `v0.1.0`, a ref that doesn't exist in `datanika-cloud`. Tag builds now pin cloud to `master`.
- [x] **DONE 2026-07-21 — core `README.md` "Releases & versioning"** section: pin a release rather than tracking `master`; advisories cite the first patched release. ⚠️ **Reworded 2026-09-03**: it said *"pin a tag/image"* and the image half was unreachable. It now says `git checkout v0.1.0` + `docker compose up -d --build`, which is the decided path above.
- [x] **DONE 2026-07-21 — policy recorded in root `CLAUDE.md`** (Branching Strategy → "Releases & versioning"). While there, also corrected the stale **"merge strategy is rebase-only"** line — merge commits *are* enabled and promotions MUST use `--merge` (that stale wording caused the 2026-07-20 dev/master divergence).

## Cross-dept implications

- **Eng** — bump the version when cutting a release; keep `0.x` semantics (breaking changes bump the minor while pre-1.0, as `datanika-mcp` 0.1.0→0.2.0 already did).
- **Growth** — each release is a publishable artifact: release notes → blog/social, and a version to cite in directory/registry submissions (`awesome-selfhosted-data` §4 draft is pre-written).
- **QA** — nothing changes; the tag rides a SHA that already passed CI + prod smoke.

## Follow-on

Once `v0.1.0` exists, the **SAML GHSA can cite `Patched: v0.1.0`** instead of commit `6e28d6b` — worth updating the drafted advisory before it's published (`plans/security/GHSA_DRAFT_saml_auth_bypass.md`).

[#1014]: https://github.com/datanika-io/datanika-core/issues/1014
