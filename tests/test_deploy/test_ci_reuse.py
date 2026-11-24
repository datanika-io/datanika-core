"""merge_group and push reuse a completed success; pull_request does not (core#1713).

The queue rewrites the commit id and keeps the tree. A second run of the same job is
evidence only when a *completed success* of that same job exists for the proof key.
``test`` and ``migration-roundtrip`` also fetch ``origin/master``. ``image-probe``,
``cloud-suite`` and ``image-cve`` check out datanika-cloud, so their key includes that
commit too. ``lint``, ``helm-lint`` and ``core-only-image`` read only the core tree.

An empty lookup, a red or unfinished donor, a different key, and a failed lookup are
not successes. The fast path is a step-level skip inside a job that still reports
success — a job-level ``if`` would skip ``staging``, which needs ``lint``, ``test``
and ``helm-lint`` to finish success. ``pull_request`` always runs the real steps,
because the queue can only enter after those checks exist.

``lint``'s two mypy steps carry the same hit guard as ruff. The donor already ran
mypy against the locked dependencies. Running ``mypy==2.3.1`` alone, after that
install was skipped, is a false red. A different ``if``, ``continue-on-error``,
or an early return inside ``run`` is still a mask.
"""

from __future__ import annotations

import importlib.util
import re
import urllib.error
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CI = ROOT / ".github" / "workflows" / "ci.yml"
SCRIPT = ROOT / ".github" / "scripts" / "ci_reuse.py"

EIGHT = (
    "lint",
    "test",
    "migration-roundtrip",
    "helm-lint",
    "image-probe",
    "core-only-image",
    "cloud-suite",
    "image-cve",
)
TREE_ONLY = ("lint", "helm-lint", "core-only-image")
NEEDS_MASTER = ("test", "migration-roundtrip")
NEEDS_CLOUD = ("image-probe", "cloud-suite", "image-cve")

MIGRATION_FILES = (
    "tests/test_migrations/test_roundtrip.py",
    "tests/test_migrations/test_data_preservation_roundtrip.py",
    "tests/test_migrations/test_n0_audit_jsonb_blue_green.py",
    "tests/test_migrations/test_expand_contract.py",
)

HIT_GUARD = "steps.reuse.outputs.hit != 'true'"
RUFF_HIT_IF = f"success() && {HIT_GUARD}"

# core#1288. These two steps used to re-run on a reuse hit. That installed
# mypy==2.3.1 with the locked dependencies skipped, and mypy exited 1.
MYPY_STEPS = (
    "Install mypy (pinned)",
    "Type check (scripts/ — shape only, never behaviour)",
)
RECORD_STEP = "Record the CI reuse key"

TREE_A = "a" * 40
TREE_B = "b" * 40
EXTRA_A = "c" * 40
EXTRA_B = "d" * 40
HEAD_SHA = "1" * 40


def _load_ci() -> tuple[str, dict[str, Any]]:
    text = CI.read_text(encoding="utf-8")
    doc = yaml.safe_load(text) or {}
    assert doc.get("jobs"), "ci.yml parsed with no jobs; the reuse guards would be vacuous"
    return text, doc


def _on(doc: dict[str, Any]) -> dict[str, Any]:
    raw = doc.get("on", doc.get(True))
    return raw if isinstance(raw, dict) else {}


def _reuse_module():
    if not SCRIPT.is_file():
        pytest.fail(f"{SCRIPT} is missing — the reuse decision has nothing to run")
    spec = importlib.util.spec_from_file_location("ci_reuse", SCRIPT)
    if spec is None or spec.loader is None:
        pytest.fail(f"could not load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def reuse():
    return _reuse_module()


def _donor(
    job: str,
    key: str,
    *,
    status: str = "completed",
    conclusion: str = "success",
) -> dict[str, str]:
    return {"job": job, "key": key, "status": status, "conclusion": conclusion}


def _run_returns_early(run: str) -> bool:
    """A shell line that can succeed without executing the step's command."""
    for raw in run.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if re.search(r"(?:^|[;&|])\s*(?:exit|return)\b", line):
            return True
        if re.search(r"(?:\|\||&&)\s*true\b", line):
            return True
    return False


def _must_guard(job_name: str, step: dict[str, Any]) -> bool:
    """True when this step is expensive and has to carry the hit guard."""
    if step.get("name") == RECORD_STEP:
        return False
    if job_name == "lint":
        uses = str(step.get("uses") or "")
        if "setup-uv" in uses or "setup-python" in uses:
            return False
    return True


def audit_job(name: str, job: dict[str, Any]) -> list[str]:
    """What would make this job skip its real work, or skip the job entirely."""
    errors: list[str] = []
    if job.get("if"):
        errors.append(f"{name}: job-level if ({job.get('if')!r}) skips the check")
    steps = list(job.get("steps") or [])
    reuse_steps = [step for step in steps if step.get("id") == "reuse"]
    if len(reuse_steps) != 1:
        errors.append(f"{name}: expected one step id 'reuse', found {len(reuse_steps)}")
        return errors
    step = reuse_steps[0]
    if step.get("continue-on-error"):
        errors.append(f"{name}: the reuse step has continue-on-error")
    if "ci_reuse.py" not in str(step.get("run") or ""):
        errors.append(f"{name}: the reuse step does not run ci_reuse.py")
    env = step.get("env") or {}
    if str(env.get("REUSE_JOB")) != name:
        errors.append(f"{name}: REUSE_JOB is {env.get('REUSE_JOB')!r}")
    needs = str(env.get("REUSE_NEEDS_EXTRA"))
    extra = str(env.get("REUSE_EXTRA") or "")
    if name in TREE_ONLY:
        if needs != "0":
            errors.append(f"{name}: tree-only job must set REUSE_NEEDS_EXTRA=0, got {needs!r}")
        if extra != "-":
            errors.append(f"{name}: tree-only extra must be '-', got {extra!r}")
    elif name in NEEDS_MASTER:
        if needs != "1":
            errors.append(f"{name}: master is an input and is not in the key ({needs!r})")
        if "steps.deployed.outputs.sha" not in extra:
            errors.append(f"{name}: extra is not the fetched origin/master sha ({extra!r})")
    elif name in NEEDS_CLOUD:
        if needs != "1":
            errors.append(f"{name}: the cloud checkout is an input and is not in the key")
        if "steps.cloudsha.outputs.sha" not in extra:
            errors.append(f"{name}: extra is not the cloud commit ({extra!r})")
    else:
        errors.append(f"{name}: not one of the eight unfiltered jobs")
    after = steps[steps.index(step) + 1 :]
    guarded = [candidate for candidate in after if _must_guard(name, candidate)]
    if not guarded:
        errors.append(f"{name}: no step after the decision is gated, so nothing is reused")
    for candidate in guarded:
        cond = str(candidate.get("if") or "")
        label = candidate.get("name") or candidate.get("uses") or candidate.get("id")
        if HIT_GUARD not in cond:
            errors.append(f"{name}: {label!r} runs on the fast path")
        if candidate.get("name") in MYPY_STEPS:
            if cond != RUFF_HIT_IF:
                errors.append(f"{name}: {label!r} if is {cond!r}, not the ruff hit guard")
            if _run_returns_early(str(candidate.get("run") or "")):
                errors.append(f"{name}: {label!r} returns early inside run")
        if candidate.get("continue-on-error") and candidate.get("id") != "canary":
            errors.append(f"{name}: {label!r} has continue-on-error on the fast-path gate")
    notices = [candidate for candidate in steps if candidate.get("name") == RECORD_STEP]
    if len(notices) != 1:
        errors.append(f"{name}: expected one {RECORD_STEP!r} step, found {len(notices)}")
    else:
        notice = notices[0]
        run = str(notice.get("run") or "")
        if "ci-reuse-key" not in run:
            errors.append(f"{name}: the record step does not emit the reuse-key notice")
        if notice.get("continue-on-error"):
            errors.append(f"{name}: the record step has continue-on-error")
        if HIT_GUARD in str(notice.get("if") or ""):
            errors.append(
                f"{name}: the record step is skipped when the fast path hits, "
                "so a later run cannot see this proof"
            )
    return errors


# ── the decision ────────────────────────────────────────────────────────────────────────


def test_an_exact_completed_success_takes_the_fast_path(reuse) -> None:
    key = reuse.proof_key(TREE_A, "-")
    donors = [_donor("lint", key)]
    assert reuse.decide("merge_group", "lint", key, donors) is True
    assert reuse.decide("push", "lint", key, donors) is True


def test_pull_request_keeps_the_real_steps(reuse) -> None:
    key = reuse.proof_key(TREE_A, "-")
    assert reuse.decide("pull_request", "lint", key, [_donor("lint", key)]) is False
    assert reuse.decide("workflow_dispatch", "lint", key, [_donor("lint", key)]) is False


def test_a_different_tree_is_refused(reuse) -> None:
    current = reuse.proof_key(TREE_A, "-")
    other = reuse.proof_key(TREE_B, "-")
    assert reuse.decide("push", "helm-lint", current, [_donor("helm-lint", other)]) is False


def test_a_red_donor_is_refused(reuse) -> None:
    key = reuse.proof_key(TREE_A, EXTRA_A)
    red = _donor("test", key, conclusion="failure")
    assert reuse.decide("merge_group", "test", key, [red]) is False


def test_an_empty_lookup_is_not_a_success(reuse) -> None:
    key = reuse.proof_key(TREE_A, "-")
    assert reuse.decide("push", "core-only-image", key, []) is False
    assert reuse.decide("push", "core-only-image", key, None) is False


def test_an_unfinished_donor_is_refused(reuse) -> None:
    key = reuse.proof_key(TREE_A, "-")
    pending = _donor("lint", key, status="in_progress", conclusion="")
    assert reuse.decide("merge_group", "lint", key, [pending]) is False


def test_a_different_job_does_not_count(reuse) -> None:
    key = reuse.proof_key(TREE_A, "-")
    assert reuse.decide("push", "test", key, [_donor("lint", key)]) is False


def test_the_extra_input_is_part_of_the_key(reuse) -> None:
    """Same core tree, different origin/master (or cloud) commit, is a different proof."""
    current = reuse.proof_key(TREE_A, EXTRA_A)
    moved = reuse.proof_key(TREE_A, EXTRA_B)
    refused = reuse.decide(
        "push", "migration-roundtrip", current, [_donor("migration-roundtrip", moved)]
    )
    assert refused is False
    accepted = reuse.decide(
        "push", "migration-roundtrip", current, [_donor("migration-roundtrip", current)]
    )
    assert accepted is True


def test_one_completed_success_is_enough_when_another_run_failed(reuse) -> None:
    """A red run does not erase a separate completed success of the same key.

    The fast path needs one completed success. Treating any historical red as a veto
    would re-run the suite forever after a single flake on an unchanged tree.
    """
    key = reuse.proof_key(TREE_A, "-")
    donors = [
        _donor("image-probe", key, conclusion="failure"),
        _donor("image-probe", key, conclusion="success"),
    ]
    assert reuse.decide("merge_group", "image-probe", key, donors) is True


def test_a_failed_lookup_is_not_a_success(reuse) -> None:
    key = reuse.proof_key(TREE_A, "-")

    def boom() -> list[dict[str, str]]:
        raise urllib.error.URLError("actions API 403")

    assert reuse.lookup_hit("push", "lint", key, boom) is False


def test_proof_key_rejects_a_tree_that_is_not_a_sha(reuse) -> None:
    with pytest.raises(ValueError):
        reuse.proof_key("not-a-tree", "-")
    with pytest.raises(ValueError):
        reuse.proof_key(TREE_A, "master")


class _Transport:
    """Records the URLs a lookup touches and answers from a table."""

    def __init__(self, table: dict[str, Any]) -> None:
        self.table = table
        self.urls: list[str] = []

    def __call__(self, url: str, body: dict[str, Any] | None = None) -> Any:
        self.urls.append(url if body is None else "POST " + url)
        if body is not None:
            return self.table["graphql"]
        for prefix, payload in self.table.items():
            if prefix != "graphql" and prefix in url:
                return payload
        raise AssertionError(f"unexpected lookup URL {url}")


def _runs(run_id: int = 5, sha: str = HEAD_SHA) -> dict[str, Any]:
    return {
        "workflow_runs": [
            {"id": run_id, "head_sha": sha, "status": "completed", "conclusion": "success"}
        ]
    }


def _jobs(name: str, *, conclusion: str = "success", check_run: str = "9") -> dict[str, Any]:
    return {
        "jobs": [
            {
                "name": name,
                "status": "completed",
                "conclusion": conclusion,
                "check_run_url": f"https://api.github.com/repos/o/r/check-runs/{check_run}",
            }
        ]
    }


def _graphql(tree: str) -> dict[str, Any]:
    return {"data": {"repository": {"s0": {"oid": tree}}}}


def test_a_tree_only_success_needs_no_annotation(reuse) -> None:
    key = reuse.proof_key(TREE_A, "-")
    transport = _Transport(
        {
            "/actions/workflows/ci.yml/runs?": _runs(),
            "graphql": _graphql(TREE_A),
            "/actions/runs/5/jobs": _jobs("lint"),
        }
    )
    donors = reuse.collect_donors(
        transport, "datanika-io/datanika-core", "lint", TREE_A, "999", False
    )
    assert donors == [_donor("lint", key)]
    assert reuse.decide("push", "lint", key, donors) is True
    assert not any("annotations" in url for url in transport.urls), transport.urls


def test_a_cloud_success_without_a_recorded_key_is_not_reused(reuse) -> None:
    """A run from before this proof existed did not record which cloud commit it built."""
    transport = _Transport(
        {
            "/actions/workflows/ci.yml/runs?": _runs(),
            "graphql": _graphql(TREE_A),
            "/actions/runs/5/jobs": _jobs("cloud-suite"),
            "/check-runs/9/annotations": [],
        }
    )
    donors = reuse.collect_donors(
        transport, "datanika-io/datanika-core", "cloud-suite", TREE_A, "999", True
    )
    assert donors == []
    key = reuse.proof_key(TREE_A, EXTRA_A)
    assert reuse.decide("merge_group", "cloud-suite", key, donors) is False


def test_a_recorded_cloud_key_has_to_match_the_tree_and_the_extra(reuse) -> None:
    key = reuse.proof_key(TREE_A, EXTRA_A)
    transport = _Transport(
        {
            "/actions/workflows/ci.yml/runs?": _runs(),
            "graphql": _graphql(TREE_A),
            "/actions/runs/5/jobs": _jobs("image-cve"),
            "/check-runs/9/annotations": [
                {"title": "ci-reuse-key", "message": key},
            ],
        }
    )
    donors = reuse.collect_donors(
        transport, "datanika-io/datanika-core", "image-cve", TREE_A, "999", True
    )
    assert donors == [_donor("image-cve", key)]
    assert reuse.decide("push", "image-cve", key, donors) is True
    assert reuse.decide("push", "image-cve", reuse.proof_key(TREE_A, EXTRA_B), donors) is False


def test_a_different_tree_does_not_even_read_the_jobs(reuse) -> None:
    transport = _Transport(
        {
            "/actions/workflows/ci.yml/runs?": _runs(),
            "graphql": _graphql(TREE_B),
        }
    )
    donors = reuse.collect_donors(
        transport, "datanika-io/datanika-core", "test", TREE_A, "999", True
    )
    assert donors == []
    assert not any("/jobs" in url for url in transport.urls), transport.urls


def test_the_current_run_is_not_its_own_donor(reuse) -> None:
    transport = _Transport(
        {
            "/actions/workflows/ci.yml/runs?": _runs(run_id=5),
            "graphql": _graphql(TREE_A),
            "/actions/runs/5/jobs": _jobs("helm-lint"),
        }
    )
    donors = reuse.collect_donors(
        transport, "datanika-io/datanika-core", "helm-lint", TREE_A, "5", False
    )
    assert donors == []


def test_a_red_job_on_the_same_tree_is_not_recorded_as_a_donor(reuse) -> None:
    transport = _Transport(
        {
            "/actions/workflows/ci.yml/runs?": _runs(),
            "graphql": _graphql(TREE_A),
            "/actions/runs/5/jobs": _jobs("lint", conclusion="failure"),
        }
    )
    donors = reuse.collect_donors(
        transport, "datanika-io/datanika-core", "lint", TREE_A, "999", False
    )
    assert donors == []


# ── the workflow still starts every required job ───────────────────────────────────────


def test_the_eight_jobs_skip_steps_only_after_a_reuse_decision() -> None:
    _text, doc = _load_ci()
    errors: list[str] = []
    for name in EIGHT:
        assert name in doc["jobs"], name
        errors.extend(audit_job(name, doc["jobs"][name]))
    assert errors == [], "\n".join(errors)


def test_the_auditor_rejects_a_step_that_runs_on_the_fast_path() -> None:
    """A guard that cannot fail is not a guard. This is the shape the live test refuses."""
    job = {
        "steps": [
            {
                "id": "reuse",
                "run": "python3 .github/scripts/ci_reuse.py",
                "env": {
                    "REUSE_JOB": "helm-lint",
                    "REUSE_NEEDS_EXTRA": "0",
                    "REUSE_EXTRA": "-",
                },
            },
            {"name": "Helm lint", "run": "helm lint deploy/helm/datanika"},
            {
                "name": RECORD_STEP,
                "if": "success() && steps.reuse.outputs.key != ''",
                "run": 'echo "::notice title=ci-reuse-key::${KEY}"',
            },
        ]
    }
    errors = audit_job("helm-lint", job)
    assert any("Helm lint" in error for error in errors), errors


def test_the_auditor_accepts_the_guarded_shape() -> None:
    job = {
        "steps": [
            {
                "id": "reuse",
                "run": "python3 .github/scripts/ci_reuse.py",
                "env": {
                    "REUSE_JOB": "helm-lint",
                    "REUSE_NEEDS_EXTRA": "0",
                    "REUSE_EXTRA": "-",
                },
            },
            {
                "name": "Helm lint",
                "if": f"success() && {HIT_GUARD}",
                "run": "helm lint deploy/helm/datanika",
            },
            {
                "name": RECORD_STEP,
                "if": "success() && steps.reuse.outputs.key != ''",
                "run": 'echo "::notice title=ci-reuse-key::${KEY}"',
            },
        ]
    }
    assert audit_job("helm-lint", job) == []


def _lint_reuse_job(step: dict[str, Any]) -> dict[str, Any]:
    return {
        "steps": [
            {
                "id": "reuse",
                "run": "python3 .github/scripts/ci_reuse.py",
                "env": {
                    "REUSE_JOB": "lint",
                    "REUSE_NEEDS_EXTRA": "0",
                    "REUSE_EXTRA": "-",
                },
            },
            step,
            {
                "name": RECORD_STEP,
                "if": "success() && steps.reuse.outputs.key != ''",
                "run": 'echo "::notice title=ci-reuse-key::${KEY}"',
            },
        ]
    }


def test_the_auditor_refuses_another_if_or_an_early_return_on_mypy() -> None:
    """The hit guard is one exact condition. A shorter if, or exit before mypy, is a miss."""
    install = 'uv pip install --system "mypy==2.3.1"'
    wrong = audit_job(
        "lint",
        _lint_reuse_job({"name": "Install mypy (pinned)", "if": "success()", "run": install}),
    )
    assert any("not the ruff hit guard" in error for error in wrong), wrong
    early = audit_job(
        "lint",
        _lint_reuse_job(
            {
                "name": "Type check (scripts/ — shape only, never behaviour)",
                "if": RUFF_HIT_IF,
                "run": "exit 0\nmypy",
            }
        ),
    )
    assert any("returns early" in error for error in early), early
    quarantined = audit_job(
        "lint",
        _lint_reuse_job(
            {
                "name": "Install mypy (pinned)",
                "if": RUFF_HIT_IF,
                "continue-on-error": True,
                "run": install,
            }
        ),
    )
    assert any("continue-on-error" in error for error in quarantined), quarantined
    clean = audit_job(
        "lint",
        _lint_reuse_job(
            {
                "name": "Type check (scripts/ — shape only, never behaviour)",
                "if": RUFF_HIT_IF,
                "run": "mypy",
            }
        ),
    )
    assert clean == [], clean


def test_the_auditor_rejects_a_job_level_if() -> None:
    job = {
        "if": "github.event_name != 'pull_request'",
        "steps": [
            {
                "id": "reuse",
                "run": "python3 .github/scripts/ci_reuse.py",
                "env": {
                    "REUSE_JOB": "test",
                    "REUSE_NEEDS_EXTRA": "1",
                    "REUSE_EXTRA": "${{ steps.deployed.outputs.sha }}",
                },
            },
            {
                "name": "Run tests",
                "if": f"success() && {HIT_GUARD}",
                "run": "pytest tests/ --tb=short -q",
            },
            {
                "name": RECORD_STEP,
                "if": "success() && steps.reuse.outputs.key != ''",
                "run": 'echo "::notice title=ci-reuse-key::${KEY}"',
            },
        ],
    }
    errors = audit_job("test", job)
    assert any("job-level if" in error for error in errors), errors


def test_merge_group_stays_and_concurrency_is_unchanged() -> None:
    text, doc = _load_ci()
    assert "merge_group" in _on(doc)
    assert "group: ci-${{ github.event_name == 'push' && github.sha || github.ref }}" in text
    assert (
        "cancel-in-progress: ${{ github.event_name != 'push' && "
        "github.event_name != 'merge_group' }}"
    ) in text


def test_the_four_migration_files_stay_on_the_real_commands() -> None:
    _text, doc = _load_ci()
    roundtrip = "\n".join(
        str(step.get("run") or "") for step in doc["jobs"]["migration-roundtrip"]["steps"]
    )
    for path in MIGRATION_FILES:
        assert path in roundtrip, path
    test_run = "\n".join(str(step.get("run") or "") for step in doc["jobs"]["test"]["steps"])
    assert "pytest tests/" in test_run
    assert "--ignore=tests/test_migrations" not in test_run


def test_supersession_staging_and_e2e_sso_stay_push_to_dev() -> None:
    _text, doc = _load_ci()
    jobs = doc["jobs"]
    supersession_if = "github.event_name == 'push' && github.ref == 'refs/heads/dev'"
    assert jobs["supersession"]["if"] == supersession_if
    for name in ("staging", "e2e-sso"):
        cond = str(jobs[name]["if"])
        assert "github.event_name == 'push'" in cond, name
        assert "refs/heads/dev" in cond, name
    for name in EIGHT:
        assert "if" not in jobs[name], name


def test_the_image_cve_canary_still_continues_on_error() -> None:
    """The canary's continue-on-error is the real path. The reuse step does not get one."""
    _text, doc = _load_ci()
    steps = doc["jobs"]["image-cve"]["steps"]
    canary = next(step for step in steps if step.get("id") == "canary")
    assert canary.get("continue-on-error") is True
    for name in EIGHT:
        for step in doc["jobs"][name]["steps"]:
            if step.get("id") == "reuse":
                assert "continue-on-error" not in step
