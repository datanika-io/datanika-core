"""A merge queue is only as good as the checks that report on its entries (core#904).

What this defends
-----------------
``dev`` is ``strict = true`` with five required checks. With five departments merging,
base-branch moves ran ~10-15 minutes apart on 2026-09-01 against a required-check cycle
of about the same length — so a rebased PR can go ``BEHIND`` again *before its own checks
finish* and never converges. A merge queue is the mechanism built for that.

But GitHub does not start a run for a merge-queue entry unless the workflow lists the
``merge_group`` event:

    "you need to update the workflows to include the merge_group event as an additional
     trigger. Otherwise, status checks will not be triggered when you add a pull request
     to a merge queue. The merge will fail as the required status check will not be
     reported."

**0 of 11 core workflows referenced it** when core#904 was filed. Enabling a queue in
that state does not fail loudly: nothing goes red, the entry sits until
``check_response_timeout_minutes`` expires, and it is ejected. A PR that stops
progressing for no visible reason is *strictly worse* than the livelock the queue exists
to remove, because a human clears that one in seconds.

Measured on datanika-landing before this landed, which is why the assertions below are
shaped the way they are:

* GitHub reads workflow triggers from the **merge-group ref's own tree** (base + PR), not
  from the base branch. The landing PR that added the trigger bootstrapped itself through
  the queue: run ``33523211167``, event ``merge_group``, branch
  ``gh-readonly-queue/dev/pr-440-8d93a238``, and ``build`` reported on the merge-group
  commit.
* The merge-group ref is suffixed with the **base** SHA, not the PR head. Two entries for
  the same PR against the same base therefore share a ``github.ref`` — which is why the
  ``cancel-in-progress`` assertion below is load-bearing and not theoretical.

The invariants
--------------
1. **Any workflow that can report a check on a pull request into ``dev``, and has no
   ``paths:`` filter, must trigger on ``merge_group``.**

   Wider than "the required checks" on purpose. A check has to be reportable on a PR to
   be *selectable* as required at all, so covering every PR-to-``dev`` workflow covers
   every check that could ever become one — with no manifest of check names to rot the
   first time branch protection changes.

   The ``paths:`` exemption is derived, not a list of blessed filenames. ``CLAUDE.md``
   already records that ``dev``'s required checks carry "no matrix suffixes and no
   ``paths:`` filters, so each one always reports on a PR" — a paths-filtered workflow
   therefore cannot supply a required check, and forcing it onto every queue entry would
   just burn runner minutes. ``oracle-connector-smoke.yml`` is the live instance.

2. **No such workflow may cancel a merge-group run.** A cancelled run is an *absent*
   verdict. On a pull request that is free: the answer is recomputed on the new head. On
   a queue entry nothing recomputes it — the queue waits out its timeout and ejects.
   ``ci.yml``'s own comment already records what absent verdicts cost here (core#514).

3. **A required check's job must not skip on a merge group.** Invariant 1 gets the
   workflow to *start*; a job-level ``if:`` can still make the check never report. Core
   has legitimately event-filtered jobs, so this is asserted only of the required set.

4. **No job that touches staging may run on a merge group.** Every queue entry would
   otherwise redeploy staging, and staging redeploys are already a documented collision
   source (core#572). Derived from the job bodies, not from a list of job names.

Re-derive the required-check list rather than trusting the constant below::

    gh api repos/datanika-io/datanika-core/branches/dev/protection \\
      -q '[.required_status_checks.checks[].context]'

If it drifts, invariant 3 weakens gracefully — the new check is still covered by
invariant 1 as long as it lives in a non-paths-filtered PR workflow, which our own rule
for required checks already demands.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"

# Re-derived from the API on 2026-09-01; see the module docstring for the command.
REQUIRED_CHECKS_ON_DEV = ("lint", "test", "helm-lint", "migration-roundtrip", "image-probe")

# Markers that mean a job manipulates the staging deployment. Matched against the job's
# own serialised body, so a renamed job cannot escape them.
STAGING_MARKERS = ("--env staging", "datanika-staging", "staging-app", "STAGING_")


def _load() -> dict[str, tuple[str, dict]]:
    """Every workflow, as ``{filename: (raw_text, parsed_doc)}``."""
    out: dict[str, tuple[str, dict]] = {}
    for path in sorted(WORKFLOWS.glob("*.y*ml")):
        text = path.read_text(encoding="utf-8")
        out[path.name] = (text, yaml.safe_load(text) or {})
    return out


def _on(doc: dict) -> dict:
    # PyYAML resolves the bare key `on` to the boolean True (YAML 1.1). GitHub does not.
    raw = doc.get("on", doc.get(True))
    return raw if isinstance(raw, dict) else {}


def _pull_request_targets_dev(doc: dict) -> bool:
    pr = _on(doc).get("pull_request")
    if pr is None and "pull_request" not in _on(doc):
        return False
    pr = pr or {}
    branches = pr.get("branches")
    # No branch filter means every base branch, `dev` included.
    return branches is None or "dev" in branches


def _has_paths_filter(doc: dict) -> bool:
    pr = _on(doc).get("pull_request") or {}
    return bool(pr.get("paths") or pr.get("paths-ignore"))


def _declares_merge_group(doc: dict) -> bool:
    return "merge_group" in _on(doc)


def _cancels_merge_group_runs(doc: dict) -> bool:
    """True when a later run can cancel this workflow's merge-group run.

    Absent ``cancel-in-progress`` defaults to false. A bare ``true`` cancels everything,
    merge groups included. An expression has to carry the guarding conjunct — and it must
    be ``!=``: ``== 'merge_group'`` reads plausibly and cancels exactly the runs that must
    never be cancelled.
    """
    conc = doc.get("concurrency")
    if not isinstance(conc, dict) or "cancel-in-progress" not in conc:
        return False
    value = conc["cancel-in-progress"]
    if isinstance(value, bool):
        return value
    return not re.search(r"github\.event_name\s*!=\s*['\"]merge_group['\"]", str(value))


def _skips_merge_group(job: dict) -> bool:
    """A job whose ``if:`` gates on the event name without naming ``merge_group``."""
    cond = str(job.get("if") or "")
    return "github.event_name" in cond and "merge_group" not in cond


def _touches_staging(job: dict) -> bool:
    body = yaml.safe_dump(job)
    return any(marker in body for marker in STAGING_MARKERS)


WORKFLOWS_BY_NAME = _load()
QUEUED = {
    name: (text, doc)
    for name, (text, doc) in WORKFLOWS_BY_NAME.items()
    if _pull_request_targets_dev(doc) and not _has_paths_filter(doc)
}


def test_the_scan_found_something_to_check():
    """An under-populated run has to fail rather than report green.

    A rename of `.github/workflows`, a glob typo, or a parser change would otherwise
    leave every assertion below iterating an empty mapping.
    """
    assert WORKFLOWS_BY_NAME, f"no workflows found under {WORKFLOWS}"
    assert len(WORKFLOWS_BY_NAME) >= 10, WORKFLOWS_BY_NAME.keys()
    assert "ci.yml" in QUEUED, sorted(QUEUED)


def test_every_pr_to_dev_workflow_listens_for_merge_group():
    missing = sorted(name for name, (_, doc) in QUEUED.items() if not _declares_merge_group(doc))
    assert missing == [], (
        f"{missing} report checks on pull requests into `dev` but do not trigger on "
        "`merge_group`, so their checks would never be reported on a queue entry"
    )


def test_no_pr_to_dev_workflow_cancels_a_merge_group_run():
    cancels = sorted(name for name, (_, doc) in QUEUED.items() if _cancels_merge_group_runs(doc))
    assert cancels == [], f"{cancels} would cancel a merge-group run, which is an absent verdict"


def test_superseded_pull_request_runs_are_still_cancelled():
    """The opposite error: answering this by disabling cancellation altogether.

    ``ci.yml`` is the expensive one — a full pytest suite plus an image build — so leaving
    superseded PR runs alive would queue runners behind work nobody is waiting for.
    """
    _, doc = WORKFLOWS_BY_NAME["ci.yml"]
    value = str(doc["concurrency"]["cancel-in-progress"])
    assert value != "False", "ci.yml no longer cancels superseded pull_request runs"
    assert "github.event_name" in value


def test_the_paths_filter_exemption_has_a_live_subject():
    """Guard the exemption itself.

    If nothing is paths-filtered any more, the exemption is dead code that will quietly
    excuse the next workflow someone adds a filter to. It exists for a measured case.
    """
    exempt = sorted(
        name
        for name, (_, doc) in WORKFLOWS_BY_NAME.items()
        if _pull_request_targets_dev(doc) and _has_paths_filter(doc)
    )
    assert exempt, (
        "no paths-filtered PR workflow remains; delete the exemption in _load/QUEUED "
        "rather than leaving an unexercised branch"
    )


def test_required_check_jobs_do_not_skip_on_a_merge_group():
    """Invariant 1 makes the workflow start; this makes the *job* report.

    ``if: github.event_name == 'pull_request'`` on a required job is the shape that gets
    the workflow started and still leaves the check unreported.
    """
    jobs: dict[str, dict] = {}
    for _, doc in QUEUED.values():
        if not _declares_merge_group(doc):
            continue
        for job_id, spec in (doc.get("jobs") or {}).items():
            jobs.setdefault(job_id, spec or {})

    missing = [c for c in REQUIRED_CHECKS_ON_DEV if c not in jobs]
    assert missing == [], (
        f"required checks {missing} are not jobs in any merge_group-triggering workflow; "
        "either they were renamed or they moved — re-derive the list from the API"
    )

    skipping = [c for c in REQUIRED_CHECKS_ON_DEV if _skips_merge_group(jobs[c])]
    assert skipping == [], f"required checks {skipping} carry an `if:` that excludes merge_group"


def test_no_job_that_touches_staging_runs_on_a_merge_group():
    """Every queue entry would otherwise redeploy staging.

    Derived from each job's own body rather than from a list of job names, so renaming
    ``deploy-staging`` does not silently retire the assertion.
    """
    offenders = []
    for name, (_, doc) in QUEUED.items():
        if not _declares_merge_group(doc):
            continue
        for job_id, spec in (doc.get("jobs") or {}).items():
            spec = spec or {}
            if _touches_staging(spec) and not _skips_merge_group(spec):
                offenders.append(f"{name}:{job_id}")
    assert offenders == [], f"{offenders} touch staging and would run on every queue entry"


def test_the_staging_jobs_are_the_live_subject_of_that_rule():
    """…and that the rule has something to be right about.

    ``test_no_job_that_touches_staging_runs_on_a_merge_group`` passes vacuously if no job
    touches staging at all, which is exactly how a guard stops guarding.
    """
    _, doc = WORKFLOWS_BY_NAME["ci.yml"]
    staging_jobs = sorted(
        job_id for job_id, spec in doc["jobs"].items() if _touches_staging(spec or {})
    )
    assert staging_jobs, "no job in ci.yml touches staging any more"
    for job_id in staging_jobs:
        assert _skips_merge_group(doc["jobs"][job_id]), job_id


# --------------------------------------------------------------------------------------
# The guard against itself. Every mutation below is applied to the REAL `ci.yml`, not to a
# fixture written from the same mental model as the checker.
# --------------------------------------------------------------------------------------

CI_TEXT, CI_DOC = WORKFLOWS_BY_NAME["ci.yml"]


def _mutate(text: str, pattern: str, replacement: str) -> dict:
    mutated, count = re.subn(pattern, replacement, text, count=1, flags=re.M)
    assert count == 1, f"anchor {pattern!r} did not match — the mutation would be a no-op"
    return yaml.safe_load(mutated)


def test_goes_red_when_the_merge_group_trigger_is_deleted():
    doc = _mutate(CI_TEXT, r"^  merge_group:\n(?:    .*\n)*", "")
    assert _pull_request_targets_dev(doc)
    assert not _declares_merge_group(doc)


def test_is_not_satisfied_by_comments_that_merely_mention_merge_group():
    """The sharp control.

    ``ci.yml`` explains the merge-queue reasoning in comments, so the literal string
    ``merge_group`` survives the deletion above. A text-matching guard would stay green
    on a workflow that had lost the actual trigger.
    """
    mutated = re.sub(r"^  merge_group:\n(?:    .*\n)*", "", CI_TEXT, count=1, flags=re.M)
    assert "merge_group" in mutated
    assert not _declares_merge_group(yaml.safe_load(mutated))


def test_goes_red_on_a_bare_cancel_in_progress_true():
    doc = _mutate(CI_TEXT, r"^  cancel-in-progress: .*$", "  cancel-in-progress: true")
    assert _cancels_merge_group_runs(doc)


def test_goes_red_on_an_inverted_expression():
    doc = _mutate(
        CI_TEXT,
        r"^  cancel-in-progress: .*$",
        "  cancel-in-progress: ${{ github.event_name == 'merge_group' }}",
    )
    assert _cancels_merge_group_runs(doc)


def test_goes_red_when_a_required_job_is_gated_on_the_pull_request_event():
    doc = _mutate(CI_TEXT, r"^  test:$", "  test:\n    if: github.event_name == 'pull_request'")
    assert _skips_merge_group(doc["jobs"]["test"])


def test_goes_red_when_a_staging_job_loses_its_event_gate():
    doc = _mutate(
        CI_TEXT,
        r"^    if: github\.event_name == 'push' && github\.ref == 'refs/heads/dev'$",
        "    if: github.ref == 'refs/heads/dev'",
    )
    loosened = [
        job_id
        for job_id, spec in doc["jobs"].items()
        if _touches_staging(spec or {}) and not _skips_merge_group(spec or {})
    ]
    assert loosened, "loosening a staging job's event gate must be detectable"


def test_a_paths_filtered_workflow_is_exempted_for_the_stated_reason():
    """Not by name. The live subject is the oracle connector smoke gate."""
    text, doc = WORKFLOWS_BY_NAME["oracle-connector-smoke.yml"]
    assert _pull_request_targets_dev(doc)
    assert _has_paths_filter(doc)
    assert "oracle-connector-smoke.yml" not in QUEUED
    # …and removing the filter pulls it straight back into scope. The anchor consumes
    # every line indented deeper than `paths:` itself, comments included — a narrower
    # one matched the header alone and orphaned the list, which YAML then refused to
    # parse. A mutation that produces an unparseable file is not a control.
    without = _mutate(text, r"^( +)paths:\n(?:\1[ \t]+.*\n)*", "")
    assert _pull_request_targets_dev(without)
    assert not _has_paths_filter(without)
