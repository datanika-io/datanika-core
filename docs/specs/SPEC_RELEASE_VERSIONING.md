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
3. **Self-hosters get something pinnable.** ⚠️ *Partly delivered — the **tag** is pinnable, the **image** is not; see the Docker bullet below and [#1014].* Today the honest answer to "what version are you running?" is a git SHA — painful for support and upgrade decisions the moment we have adopters.
4. **A recurring marketing artifact.** Release notes → blog/social → the "project is alive" signal that drives OSS adoption. Feeds the existing blog-announcement rule with a natural rhythm.

## Rules

- **Scheme: `0.x` SemVer.** Matches `datanika-mcp`, which is already versioned (0.1.0 → 0.2.0). We are *already* doing this for the sub-package; this extends it to the platform.
- **No `1.0` yet.** 1.0 implies API-stability guarantees we don't want to owe pre-launch. Revisit when we commit to a stable public API contract.
- **Tag pattern: `v*`** (e.g. `v0.1.0`). ⚠️ **Never `mcp-v*`** — `release-mcp.yml` triggers on that and would try to publish the MCP sub-package.
- **Cadence: on meaningful accumulation, not the calendar.** A forced monthly tag with nothing in it is worse than no tag.
- **NOT one tag per `dev→master` promotion.** Raised and rejected 2026-07-21: promotions are far more frequent than releases — **six** happened in a single evening — so tagging each would emit `v0.1.1`…`v0.1.6` in one night, and every `v*` tag fires *two* workflows (GitHub Release + a pinnable GHCR image). That is calendar-cadence by another name: it decouples the tag from "meaningful accumulation" and re-couples it to deploy frequency. A promotion is a deploy; a release is a label you choose to put on one.
- **Release notes: generated from merged PR titles.** Our `[Dept] … (closes #N)` convention makes this nearly free.
- **Docker images tagged `:v0.x.y` alongside `:latest`.** 🔴 **This does NOT yet give
  self-hosters something to pin, and this bullet claimed it did from 2026-07-21 to
  2026-09-03.** The GHCR package is **private** — measured, with a positive control on a
  known-public package — so every tag answers `denied` to an anonymous pull. It cannot
  simply be flipped public: the image grafts the closed-source `datanika-cloud` tree at
  `/cloud/`. Until a **core-only** image exists ([#1014]), the pinnable artifact is the
  **source tag**, and the README says so.

## Explicitly out of scope

Release trains · freeze windows · RC/`release-*` branches · a release manager · calendar-driven cadence · **auto-tagging every promotion** · `1.0.0`.

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
- [x] **DONE 2026-07-21 — core `README.md` "Releases & versioning"** section: pin a tag/image rather than tracking `master`; advisories cite the first patched release.
- [x] **DONE 2026-07-21 — policy recorded in root `CLAUDE.md`** (Branching Strategy → "Releases & versioning"). While there, also corrected the stale **"merge strategy is rebase-only"** line — merge commits *are* enabled and promotions MUST use `--merge` (that stale wording caused the 2026-07-20 dev/master divergence).

## Cross-dept implications

- **Eng** — bump the version when cutting a release; keep `0.x` semantics (breaking changes bump the minor while pre-1.0, as `datanika-mcp` 0.1.0→0.2.0 already did).
- **Growth** — each release is a publishable artifact: release notes → blog/social, and a version to cite in directory/registry submissions (`awesome-selfhosted-data` §4 draft is pre-written).
- **QA** — nothing changes; the tag rides a SHA that already passed CI + prod smoke.

## Follow-on

Once `v0.1.0` exists, the **SAML GHSA can cite `Patched: v0.1.0`** instead of commit `6e28d6b` — worth updating the drafted advisory before it's published (`plans/security/GHSA_DRAFT_saml_auth_bypass.md`).

[#1014]: https://github.com/datanika-io/datanika-core/issues/1014
