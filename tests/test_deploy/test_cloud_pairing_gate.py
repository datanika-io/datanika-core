"""core#1123 — the core deploy must refuse when cloud ``master`` is not identical to cloud ``dev``.

## What is being guarded

Cloud reaches production only inside core's image, checked out at a pinned ``ref: master``.
So a core promotion whose cloud half is merged to cloud ``dev`` but not promoted to cloud
``master`` ships a pairing that has never run anywhere — and it does so with both branches
reading as promoted, cloud CI green, and every container healthy.

That is not hypothetical. On 2026-09-06 the two halves were **eight minutes** apart, and the
half that would have shipped turned every quota refusal in the product into
*"An error occurred."*

## Why these particular assertions

The gate is one API call and a comparison, so the interesting failure modes are not in the
arithmetic. They are:

1. **The gate exists but runs too late.** A refusal after the tarball is built, after the box
   has been SSH'd, or after the cloud tree is already checked out is a refusal that has
   already done the thing it was meant to prevent. So the ordering assertion is the load
   bearing one, and it is written against *every* mutating step rather than against a
   remembered list of them — with an anti-vacuity check, because a step-classifier that
   matches nothing would let the ordering assertion pass by describing an empty set.
2. **The gate exists and cannot fail.** ``continue-on-error`` on the step or the job turns a
   refusal into an annotation. Pinned in both places.
3. **The gate exists and is quietly pointed somewhere harmless.** The script takes
   ``PAIRING_REPO``/``PAIRING_BASE``/``PAIRING_HEAD`` so it can be armed against a real
   non-identical comparison; the deploy must set none of them. Asserted positively — the
   step's ``env`` keys are enumerated and compared — rather than by grepping for the string,
   so a comment explaining the overrides could never satisfy or trip it.
4. **The verdict is permissive in a direction nobody thought about.** ``verdict`` is
   exhaustively exercised over every status GitHub documents plus the unreadable cases.

## Arming

``verdict`` and ``main`` are shown red by :func:`test_the_gate_refuses_a_real_ahead_comparison`
and its partner, which drive the real ``main()`` with a stubbed transport and assert the exit
code — not the log text. The workflow assertions were shown red by mutating
``deploy-pointer.yml`` itself: moving the gate step below ``Build source tarball`` fails
:func:`test_the_gate_runs_before_anything_mutating`, and deleting the step fails
:func:`test_the_deploy_job_runs_the_gate`.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MODULE_PATH = _REPO_ROOT / ".github" / "scripts" / "cloud_pairing_gate.py"
_WORKFLOW_PATH = _REPO_ROOT / ".github" / "workflows" / "deploy-pointer.yml"


def _load():
    name = "cloud_pairing_gate"
    spec = importlib.util.spec_from_file_location(name, _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


gate = _load()


def _deploy_steps() -> list[dict]:
    doc = yaml.safe_load(_WORKFLOW_PATH.read_text(encoding="utf-8"))
    steps = doc["jobs"]["deploy"]["steps"]
    assert isinstance(steps, list) and steps, "the deploy job has no steps to reason about"
    return steps


def _gate_index(steps: list[dict]) -> int:
    hits = [i for i, s in enumerate(steps) if _MODULE_PATH.name in str(s.get("run", ""))]
    assert len(hits) == 1, (
        f"expected exactly one step invoking {_MODULE_PATH.name}, found {len(hits)}. Zero "
        f"means the deploy has no cloud-pairing gate at all (core#1123); more than one means "
        f"an ordering assertion cannot say which one it measured."
    )
    return hits[0]


def _is_mutating(step: dict) -> bool:
    """Does this step check out cloud, package a tree, or reach the production box?

    Classified by what the step *does*, never by its name — a renamed step must stay
    classified, and the whole point of core#1123 is that the deploy's own labels were not
    what went wrong.
    """
    run = str(step.get("run", ""))
    uses = str(step.get("uses", ""))
    with_block = step.get("with") or {}
    checks_out_cloud = "checkout" in uses and "datanika-cloud" in str(
        with_block.get("repository", "")
    )
    return bool(
        checks_out_cloud
        or "tar czf" in run
        or "ssh " in run
        or "ssh-keyscan" in run
        or "docker compose" in run
    )


# --------------------------------------------------------------------------------------
# The verdict itself
# --------------------------------------------------------------------------------------


def test_only_identical_is_allowed():
    allowed, reason = gate.verdict("identical")
    assert allowed is True, reason


@pytest.mark.parametrize("status", ["ahead", "behind", "diverged"])
def test_every_other_documented_status_refuses(status):
    """``ahead`` is the 2026-09-06 case. ``behind``/``diverged`` refuse on purpose too.

    Cloud ``master`` carrying commits ``dev`` does not have means somebody pushed straight to
    production without resyncing — its own thing to stop on, and the blunt rule covers it for
    free.
    """
    allowed, reason = gate.verdict(status)
    assert allowed is False, f"{status!r} was allowed through: {reason}"
    assert status in reason, "the refusal must name what it read, or it cannot be diagnosed"


@pytest.mark.parametrize("status", [None, "", "IDENTICAL", "unknown", 0, True, {}, ["identical"]])
def test_an_unreadable_status_refuses(status):
    """Fail closed. An instrument that cannot read the pairing has not established it.

    ``"IDENTICAL"`` and ``["identical"]`` are here deliberately: a case-insensitive or
    membership-based comparison would pass one of them, and both are the shape of a gate that
    looks like it works.
    """
    allowed, _reason = gate.verdict(status)
    assert allowed is False, f"{status!r} was allowed through"


def test_the_documented_status_set_is_the_one_being_tested():
    """Anti-vacuity: if GitHub's status vocabulary grows, this file must be revisited.

    Without it, ``test_every_other_documented_status_refuses`` could be parametrised over a
    set that no longer matches the module's own and still pass.
    """
    assert set(gate.KNOWN_STATUSES) == {"identical", "ahead", "behind", "diverged"}
    assert gate.PASSING_STATUS in gate.KNOWN_STATUSES


# --------------------------------------------------------------------------------------
# End to end, with the transport stubbed — the arms that prove it can fail
# --------------------------------------------------------------------------------------


def test_the_gate_refuses_a_real_ahead_comparison(monkeypatch, capsys):
    monkeypatch.setattr(gate, "read_status", lambda *a, **k: ("ahead", "deadbeefcafe1234"))
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    assert gate.main() == 1
    assert "REFUSED" in capsys.readouterr().err


def test_the_gate_passes_an_identical_comparison(monkeypatch, capsys):
    monkeypatch.setattr(gate, "read_status", lambda *a, **k: ("identical", "deadbeefcafe1234"))
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    assert gate.main() == 0
    assert "PASS" in capsys.readouterr().out


def test_an_api_failure_refuses_rather_than_passing(monkeypatch):
    """The failure that matters: no reading at all must not read as a clean one."""
    monkeypatch.setattr(gate, "read_status", lambda *a, **k: (None, None))
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    assert gate.main() == 1


def test_a_refusal_says_how_to_repair_it_in_the_step_summary(monkeypatch, tmp_path):
    """The remedy is a cloud promotion. It must be in the artifact a promoter actually reads."""
    summary = tmp_path / "summary.md"
    monkeypatch.setattr(gate, "read_status", lambda *a, **k: ("ahead", "deadbeefcafe1234"))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert gate.main() == 1
    written = summary.read_text(encoding="utf-8")
    assert "REFUSED" in written
    assert "Promote `datanika-cloud`" in written
    assert "no override" in written.lower()


def test_read_status_folds_a_transport_error_into_no_reading(monkeypatch):
    """``read_status`` must never raise past ``main`` — an exception is an unhandled deploy."""

    def boom(*_a, **_k):
        raise OSError("connection reset")

    monkeypatch.setattr(gate.urllib.request, "urlopen", boom)
    assert gate.read_status("owner/repo", "master", "dev", None) == (None, None)


# --------------------------------------------------------------------------------------
# The wiring — a correct gate in the wrong place is not a gate
# --------------------------------------------------------------------------------------


def test_the_deploy_job_runs_the_gate():
    assert _gate_index(_deploy_steps()) >= 0


def test_the_gate_runs_before_anything_mutating():
    steps = _deploy_steps()
    index = _gate_index(steps)
    mutating = [(i, s.get("name", f"step {i}")) for i, s in enumerate(steps) if _is_mutating(s)]
    assert mutating, (
        "no step in the deploy job was classified as mutating, so this ordering assertion "
        "would pass against an empty set. The classifier has drifted from the workflow."
    )
    too_early = [(i, name) for i, name in mutating if i < index]
    assert not too_early, (
        f"the cloud pairing gate is at step {index} but these mutate first: {too_early}. A "
        f"refusal after the tarball, the SSH or the cloud checkout has already done what it "
        f"exists to prevent."
    )


def test_the_gate_step_cannot_be_downgraded_to_an_annotation():
    doc = yaml.safe_load(_WORKFLOW_PATH.read_text(encoding="utf-8"))
    job = doc["jobs"]["deploy"]
    steps = job["steps"]
    step = steps[_gate_index(steps)]
    assert "continue-on-error" not in step, "a gate with continue-on-error is a log line"
    assert "continue-on-error" not in job, "continue-on-error on the job swallows the gate too"
    assert step.get("if") is None, (
        "the gate must run on every deploy. A conditional here is how the 2026-09-06 pairing "
        "would have shipped anyway — workflow_dispatch builds the master/master pair too."
    )


def test_the_deploy_does_not_repoint_the_gate():
    """Positive form: enumerate the step's env keys rather than grepping for the overrides.

    ``PAIRING_REPO``/``_BASE``/``_HEAD`` exist so the gate can be armed against a genuinely
    non-identical comparison. Setting one here would aim the production gate at something
    that always answers ``identical``.
    """
    steps = _deploy_steps()
    step = steps[_gate_index(steps)]
    env = step.get("env") or {}
    assert set(env) == {"CLOUD_REPO_TOKEN"}, (
        f"the gate step's env is {sorted(env)}; it must carry the cloud token and nothing "
        f"else, so the production pair is the module's own default."
    )


def test_the_production_pair_is_the_default():
    assert gate.DEFAULT_REPO == "datanika-io/datanika-cloud"
    assert gate.DEFAULT_BASE == "master"
    assert gate.DEFAULT_HEAD == "dev"


def test_the_gate_has_no_override_input():
    """AC4: a refusal means *promote cloud first*, not *override it*.

    Asserted against the workflow's own inputs rather than the script's text: an override
    would have to be reachable from the deploy to be usable, and ``workflow_dispatch`` is the
    only place one could be declared.
    """
    doc = yaml.safe_load(_WORKFLOW_PATH.read_text(encoding="utf-8"))
    triggers = doc.get(True) or doc.get("on")  # PyYAML reads a bare `on:` key as True
    dispatch = (triggers or {}).get("workflow_dispatch") or {}
    assert not (dispatch or {}).get("inputs"), (
        "deploy-pointer.yml declares workflow_dispatch inputs. core#1123 is explicit that the "
        "gate takes no override — the repair is a cloud promotion."
    )
