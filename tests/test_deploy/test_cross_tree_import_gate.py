"""core#1140 — a build must refuse when the cloud tree imports a core symbol that is not there.

## What is being guarded, and how it differs from core#1123

``cloud_pairing_gate.py`` grades ``datanika-cloud``'s own ``master``↔``dev`` relationship and
never reads core. So the moment cloud is promoted alone, ``master...dev`` reads ``identical``,
that gate passes, and a core tree lacking the symbols cloud now imports is still a legal build
input. Direction B is structurally outside it.

The two paths that can produce such a pair, and the reason both are wired:

* **``deploy-pointer.yml``** — a ``workflow_dispatch``, or a ``gh run rerun`` of an older run,
  pairs an old core tree with cloud ``master``. On our box this fails *safe*
  (``bootstrap_cloud()`` has no ``try``/``except``, so the new colour dies at import and
  blue/green refuses the repoint) — the gate turns an eight-minute wedged deploy into a named
  refusal before the tarball exists.
* **``build-push-image.yml``** — the one that is **not** prod-safe. ``ref:`` pairs a ``v*``
  tag's core tree with cloud's *current* ``master``, so a release cut on a core ``master``
  predating a cloud-only promotion pushes ``:v0.x.y`` **and** ``:latest``, green, as an image
  that dies at start.

## Why these assertions and not others

The resolver is AST and its arithmetic is dull. The failure modes worth pinning are the ones
that leave a gate present and inert:

1. **Wired into one workflow and not the other.** The tag path is the one that reaches
   strangers, and it is also the one nobody exercises during a promotion — so it is exactly
   the half that would be dropped. Asserted per workflow, by name.
2. **Present but ordered after the thing it protects.** A refusal after the tarball, the SSH,
   or the ``docker/build-push-action`` has already done what it existed to prevent. Written
   against a classified set of mutating steps with an anti-vacuity check, so a classifier that
   matches nothing cannot satisfy the ordering by describing an empty set.
3. **Present but unable to fail** — ``continue-on-error`` on the step or the job.
4. **Present but pointed at the wrong trees.** A gate reading a directory with no
   ``datanika_cloud/`` grades nothing and prints zero failures, which is what a pass looks
   like. Pinned by the ``--min-refs`` floor *and* by asserting the paths each workflow passes.

## Arming

Every resolver assertion below is driven against a **real broken pairing** built from the
repo's own history — ``datanika/errors.py`` as it stood at ``faa1927^``, before core#1113's
core half declared ``ConfigurationError`` and ``InternalInvariantError`` — not against a
hand-written stub. That artifact is the actual instance core#1140 was filed about, and the
gate names all three importing call sites in it.

⚠️ The historical file is reconstructed **only if git can produce it**; otherwise the fixture
synthesises the same shape and says so. A control that silently degrades to "nothing to test"
is the failure this module is about, so the synthetic path still asserts the same red.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO_ROOT / ".github" / "scripts" / "cross_tree_import_gate.py"
_WORKFLOWS = _REPO_ROOT / ".github" / "workflows"

# The commit whose parent is the last core tree without core#1113's two classes.
_PRE_PAIRING_REF = "faa1927^:datanika/errors.py"

# Steps whose failure would mean the gate ran too late. Substrings, matched case-insensitively
# against the step name, because a step's `run:` body is not a stable identifier.
_MUTATING_DEPLOY_STEPS = ("tarball", "ssh", "transfer", "install the box-side", "prune", "build")
_MUTATING_BUILD_STEPS = ("buildx", "log in to ghcr", "build & push")


def _load_gate():
    spec = importlib.util.spec_from_file_location("cross_tree_import_gate", _SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["cross_tree_import_gate"] = mod  # before exec_module; dataclasses need it
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def gate():
    assert _SCRIPT.is_file(), f"{_SCRIPT} is missing — the gate this module guards does not exist"
    return _load_gate()


@pytest.fixture(scope="module")
def cloud_tree(tmp_path_factory) -> Path:
    """A minimal cloud tree importing exactly what the real one imports from core#1113."""
    root = tmp_path_factory.mktemp("cloud")
    pkg = root / "datanika_cloud" / "billing"
    pkg.mkdir(parents=True)
    (root / "datanika_cloud" / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "paddle.py").write_text(
        "from datanika.errors import ConfigurationError\n"
        "from datanika.models.user import Organization\n",
        encoding="utf-8",
    )
    (pkg / "e2e_admin.py").write_text(
        "from datanika.errors import InternalInvariantError\nimport datanika.models\n",
        encoding="utf-8",
    )
    return root


def _write_core(root: Path, errors_src: str) -> Path:
    """A minimal core tree whose `datanika/errors.py` is whatever the caller supplies."""
    pkg = root / "datanika"
    (pkg / "models").mkdir(parents=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "models" / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "models" / "user.py").write_text("class Organization:\n    pass\n", encoding="utf-8")
    (pkg / "errors.py").write_text(errors_src, encoding="utf-8")
    return root


@pytest.fixture(scope="module")
def historical_errors_py() -> tuple[str, str]:
    """`(source, provenance)` for core's `errors.py` before core#1113's core half.

    Prefers the repo's own history. Falls back to a synthetic file of the same shape rather
    than skipping — a control that vanishes when git is unavailable is a control that reports
    green on exactly the machines where nobody looks.
    """
    try:
        out = subprocess.run(
            ["git", "cat-file", "-p", _PRE_PAIRING_REF],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if out.returncode == 0 and len(out.stdout) > 200 and "class UserFacingError" in out.stdout:
            return out.stdout, f"git {_PRE_PAIRING_REF}"
    except (OSError, subprocess.SubprocessError):
        pass
    return (
        'class UserFacingError(ValueError):\n    """Marker."""\n',
        "synthetic (git history unavailable)",
    )


# --------------------------------------------------------------------------------------
# The resolver, armed against the real historical break
# --------------------------------------------------------------------------------------


def test_it_refuses_the_real_pairing_that_core_1140_was_filed_about(
    gate, cloud_tree, historical_errors_py, tmp_path, capsys
):
    source, provenance = historical_errors_py
    assert "ConfigurationError" not in source, (
        f"the control artifact ({provenance}) already declares ConfigurationError, so it "
        "cannot demonstrate the break — the ref this fixture reads has moved"
    )
    core = _write_core(tmp_path / "core-old", source)

    rc = gate.main.__wrapped__ if hasattr(gate.main, "__wrapped__") else None
    assert rc is None  # main takes no injection; drive it through argv

    sys.argv = ["gate", "--core", str(core), "--cloud", str(cloud_tree), "--min-refs", "2"]
    assert gate.main() == 1, f"the gate passed a pairing built from {provenance} that is broken"

    out = capsys.readouterr().out
    assert "ConfigurationError" in out and "InternalInvariantError" in out, (
        "the refusal must name the symbols that are missing — a bare 'REFUSED' sends the "
        f"reader to diff two whole trees. Got:\n{out}"
    )
    assert "paddle.py" in out and "e2e_admin.py" in out, (
        f"the refusal must name the importing file:line, not just the symbol. Got:\n{out}"
    )


def test_it_passes_the_repaired_pairing(gate, cloud_tree, tmp_path):
    """The positive control: the same trees, with the two classes present, must pass.

    Without this the test above is satisfied by a gate that refuses everything.
    """
    core = _write_core(
        tmp_path / "core-new",
        "class UserFacingError(ValueError):\n    pass\n\n\n"
        "class ConfigurationError(Exception):\n    pass\n\n\n"
        "class InternalInvariantError(Exception):\n    pass\n",
    )
    sys.argv = ["gate", "--core", str(core), "--cloud", str(cloud_tree), "--min-refs", "2"]
    assert gate.main() == 0


def _find_cloud_tree() -> Path | None:
    """Locate a cloud checkout beside this one, across the layouts that actually exist.

    ⚠️ Written as a search rather than one path on purpose. The first version looked only at
    `_REPO_ROOT.parent / "datanika-cloud"`, which is the CI layout — and on the dev machine,
    where every agent works in `worktrees/datanika-core-<dept>/`, it resolved to
    `worktrees/datanika-cloud` and skipped **every time**. A test that always skips is not a
    control, and its skip is the quietest possible output.
    """
    for base in (_REPO_ROOT.parent, _REPO_ROOT.parent.parent):
        if not base.is_dir():
            continue
        for cand in sorted(base.glob("datanika-cloud*")):
            if (cand / "datanika_cloud").is_dir():
                return cand
    return None


def test_the_live_trees_in_this_repo_have_a_resolvable_import_surface(gate):
    """The gate must not be red on the tree it ships beside — otherwise it gets disabled.

    🚨 **This one SKIPS in CI and that is expected, not a defect to "fix" by weakening it.**
    `ci.yml`'s `test` job does not check the private cloud repo out (fork PRs have no
    `CLOUD_REPO_TOKEN`), so there is genuinely nothing here to grade. Where this property is
    *enforced* is `deploy-pointer.yml` and `build-push-image.yml`, both of which do have both
    trees and both of which are pinned by the wiring tests below.

    What it buys is the dev machine: a broken pairing is caught before the push rather than
    at 3 a.m. by a wedged deploy.
    """
    cloud = _find_cloud_tree()
    if cloud is None:
        pytest.skip(
            "no cloud tree beside this checkout — expected in CI, where the private repo is "
            "not checked out for `test`. The gate is enforced in deploy-pointer.yml and "
            "build-push-image.yml, which do have both trees."
        )
    sys.argv = ["gate", "--core", str(_REPO_ROOT), "--cloud", str(cloud)]
    assert gate.main() == 0, f"the live core tree does not satisfy {cloud}'s imports"


def test_a_run_that_grades_nothing_is_a_refusal_not_a_pass(gate, tmp_path):
    """Zero references found and zero references failing are the same output.

    This is the shape that makes a misconfigured gate invisible: point it one directory too
    high and it reports no failures forever.
    """
    empty = tmp_path / "empty-cloud"
    (empty / "datanika_cloud").mkdir(parents=True)
    core = _write_core(tmp_path / "core-floor", "class UserFacingError(ValueError):\n    pass\n")
    sys.argv = ["gate", "--core", str(core), "--cloud", str(empty)]
    assert gate.main() == 1


def test_a_conditional_or_try_guarded_binding_in_core_still_counts(gate, tmp_path):
    """A false RED is what gets a gate deleted, so the resolver must see defensive imports."""
    core = tmp_path / "core-cond"
    (core / "datanika").mkdir(parents=True)
    (core / "datanika" / "__init__.py").write_text("", encoding="utf-8")
    (core / "datanika" / "errors.py").write_text(
        "try:\n"
        "    from ._impl import ConfigurationError\n"
        "except ImportError:  # pragma: no cover\n"
        "    class ConfigurationError(Exception):\n"
        "        pass\n"
        "\n"
        "if True:\n"
        "    class InternalInvariantError(Exception):\n"
        "        pass\n",
        encoding="utf-8",
    )
    cloud = tmp_path / "cloud-cond"
    (cloud / "datanika_cloud").mkdir(parents=True)
    (cloud / "datanika_cloud" / "x.py").write_text(
        "from datanika.errors import ConfigurationError, InternalInvariantError\n",
        encoding="utf-8",
    )
    sys.argv = ["gate", "--core", str(core), "--cloud", str(cloud), "--min-refs", "2"]
    assert gate.main() == 0, (
        "a try/except or if-guarded class in core is a real binding for an importer; "
        "refusing it would make this gate red on correct code"
    )


def test_a_submodule_import_from_a_package_resolves(gate, tmp_path):
    """`from datanika.services import concurrency_service` names a file, not a binding."""
    core = tmp_path / "core-sub"
    (core / "datanika" / "services").mkdir(parents=True)
    (core / "datanika" / "__init__.py").write_text("", encoding="utf-8")
    (core / "datanika" / "services" / "__init__.py").write_text("", encoding="utf-8")
    (core / "datanika" / "services" / "concurrency_service.py").write_text("", encoding="utf-8")
    cloud = tmp_path / "cloud-sub"
    (cloud / "datanika_cloud").mkdir(parents=True)
    (cloud / "datanika_cloud" / "y.py").write_text(
        "from datanika.services import concurrency_service\n"
        "from datanika.services import not_a_thing\n",
        encoding="utf-8",
    )
    sys.argv = ["gate", "--core", str(core), "--cloud", str(cloud), "--min-refs", "2"]
    assert gate.main() == 1, "the bogus half must still refuse — otherwise this proves nothing"


# --------------------------------------------------------------------------------------
# Wiring: the gate must run, in both workflows, before anything it protects
# --------------------------------------------------------------------------------------


def _steps(workflow: str, job: str) -> list[dict]:
    doc = yaml.safe_load((_WORKFLOWS / workflow).read_text(encoding="utf-8"))
    return doc["jobs"][job]["steps"]


def _gate_index(steps: list[dict]) -> int:
    for i, s in enumerate(steps):
        if "cross_tree_import_gate.py" in str(s.get("run", "")):
            return i
    raise AssertionError(
        "no step invokes cross_tree_import_gate.py. A gate that no workflow runs is "
        "core#1140 one level up, and it looks identical to a fix."
    )


@pytest.mark.parametrize(
    ("workflow", "job", "mutating"),
    [
        ("deploy-pointer.yml", "deploy", _MUTATING_DEPLOY_STEPS),
        ("build-push-image.yml", "build-push", _MUTATING_BUILD_STEPS),
    ],
)
def test_the_gate_runs_before_anything_it_protects(workflow, job, mutating):
    steps = _steps(workflow, job)
    idx = _gate_index(steps)

    later = [
        (i, s.get("name", s.get("uses", "?")))
        for i, s in enumerate(steps)
        if any(
            m in str(s.get("name", "")).lower() or m in str(s.get("uses", "")).lower()
            for m in mutating
        )
    ]
    assert later, (
        f"the mutating-step classifier matched NOTHING in {workflow}:{job}. The ordering "
        "assertion below would then pass by describing an empty set, which is exactly the "
        "vacuous-guard shape this file exists to avoid."
    )
    offenders = [(i, n) for i, n in later if i < idx]
    assert not offenders, (
        f"{workflow}:{job} runs the gate at step {idx}, after {offenders}. A refusal that "
        "lands after the tarball, the SSH or the push has already done the thing it exists "
        "to prevent."
    )


@pytest.mark.parametrize(
    ("workflow", "job"),
    [("deploy-pointer.yml", "deploy"), ("build-push-image.yml", "build-push")],
)
def test_the_gate_can_actually_fail(workflow, job):
    doc = yaml.safe_load((_WORKFLOWS / workflow).read_text(encoding="utf-8"))
    jobdef = doc["jobs"][job]
    assert not jobdef.get("continue-on-error"), (
        f"{workflow}:{job} is continue-on-error, so every gate in it is an annotation"
    )
    step = _steps(workflow, job)[_gate_index(_steps(workflow, job))]
    assert not step.get("continue-on-error"), (
        f"the {workflow} gate step is continue-on-error — it cannot refuse anything"
    )
    assert "if" not in step, (
        f"the {workflow} gate step carries an `if:`. A conditional gate is a gate whose "
        "condition is the thing nobody re-reads; if one is genuinely needed, assert the "
        "condition here too."
    )


@pytest.mark.parametrize(
    ("workflow", "job", "core_path", "cloud_path"),
    [
        ("deploy-pointer.yml", "deploy", "--core datanika", "--cloud datanika-cloud"),
        (
            "build-push-image.yml",
            "build-push",
            "--core build-context/datanika",
            "--cloud build-context/datanika-cloud",
        ),
    ],
)
def test_the_gate_is_pointed_at_the_trees_that_are_actually_built(
    workflow, job, core_path, cloud_path
):
    """A gate aimed at a directory with no `datanika_cloud/` grades nothing.

    The `--min-refs` floor turns that into a refusal rather than a pass, but the floor is a
    backstop. These are the paths each workflow's own checkout/reshape steps produce, so if
    one of those moves this fails here rather than silently at 3 a.m.
    """
    step = _steps(workflow, job)[_gate_index(_steps(workflow, job))]
    run = " ".join(str(step["run"]).split())
    assert core_path in run, f"{workflow} gate does not pass `{core_path}`; got: {run}"
    assert cloud_path in run, f"{workflow} gate does not pass `{cloud_path}`; got: {run}"


def test_the_deploy_gate_runs_after_the_cloud_checkout_it_grades():
    """Ordering has a floor as well as a ceiling here, and only one of them is obvious.

    core#1123's gate is deliberately *before* the cloud checkout — it grades an API answer.
    This one needs both trees on disk, so being early enough is not the whole requirement.
    """
    steps = _steps("deploy-pointer.yml", "deploy")
    idx = _gate_index(steps)
    checkout = [
        i
        for i, s in enumerate(steps)
        if "checkout" in str(s.get("uses", "")).lower()
        and str(s.get("with", {}).get("path", "")) == "datanika-cloud"
    ]
    assert checkout, "no step checks the cloud repo out to datanika-cloud/ — has the path moved?"
    assert idx > checkout[0], (
        f"the import gate runs at step {idx}, before the cloud checkout at {checkout[0]}. "
        "It would grade an empty directory and pass on the --min-refs floor's refusal only "
        "by accident."
    )
