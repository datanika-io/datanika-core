"""``scripts/build-from-worktree.sh`` must produce a context the Dockerfile can read (core#1197).

Why this tests the CONTEXT and not the build
--------------------------------------------
The defect in core#1197 is entirely in *context construction*: `docker-compose.yml` uses
`context: ..`, the Dockerfile does ``COPY datanika/`` + ``COPY datanika-cloud/``, and
``worktrees/`` contains neither name. A build proves that too, but it costs minutes and a
docker daemon, and CI has no reason to pay either to assert a `tar --transform`.

So these drive the script's ``--list-context`` mode, which emits exactly the stream the
build would receive. Every assertion below is about member *names*, which is the only
thing the rename changes and the only thing that was ever wrong.

Anti-vacuity
------------
Three of these tests exist to make a green here mean something, because a listing-based
check has two silent-pass modes and both have shipped in this repo before:

* an **empty** stream satisfies "no unexpected members" trivially, so
  ``test_context_is_not_empty`` puts a floor under it;
* an **inert** ``--transform`` leaves the original directory names in place and every
  "contains datanika/..." assertion still fails loudly — but a check written the other way
  round would pass, so ``test_original_worktree_names_are_absent`` asserts the rename
  actually happened rather than merely that the target names exist;
* a script that **cannot fail** is not a guard, so the three negative controls assert a
  non-zero exit on a malformed tree. Each was a real way to get a wrong image quietly.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "build-from-worktree.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None, reason="needs bash to drive the shell script"
)


def _gnu_tar() -> bool:
    """Ask the tar **bash** sees, not the one on Python's PATH.

    These differ on Windows and the difference is not cosmetic: Git Bash ships GNU tar
    1.35 while ``tar.exe`` in System32 is bsdtar, which has no ``--transform`` at all.
    Probing the wrong one skipped every context test while reporting nothing wrong.
    """
    try:
        out = subprocess.run(
            ["bash", "-c", "tar --version"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):  # pragma: no cover - platform dependent
        return False
    return "GNU tar" in out.stdout


requires_gnu_tar = pytest.mark.skipif(not _gnu_tar(), reason="--transform is a GNU tar feature")


def _make_tree(root: Path, *, agent: str = "fake", cloud: bool = True) -> Path:
    """A minimal stand-in for `worktrees/`, with the script installed in it."""
    core = root / f"datanika-core-{agent}"
    (core / "scripts").mkdir(parents=True)
    (core / "datanika").mkdir()
    shutil.copy2(SCRIPT, core / "scripts" / SCRIPT.name)
    (core / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
    (core / "pyproject.toml").write_text("[project]\nname='core'\n", encoding="utf-8")
    (core / "datanika" / "config.py").write_text("x = 1\n", encoding="utf-8")
    if cloud:
        cl = root / f"datanika-cloud-{agent}"
        cl.mkdir(parents=True)
        (cl / "pyproject.toml").write_text("[project]\nname='cloud'\n", encoding="utf-8")
    return core


def _run(core: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Invoke via a RELATIVE path from ``cwd``.

    Passing the absolute Windows path is what made every negative control pass for the
    wrong reason: bash received ``D:Temppytest-of-User...`` with the separators eaten and
    exited non-zero with ``No such file or directory``. The exit code looked like the
    guard firing. Only the assertions on stderr text distinguished the two — which is why
    every negative control here checks the MESSAGE, not just the code.
    """
    return subprocess.run(
        ["bash", f"scripts/{SCRIPT.name}", *args],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        cwd=str(core),
        env={k: v for k, v in os.environ.items() if k != "MSYS_NO_PATHCONV"},
    )


# --------------------------------------------------------------------------------------
# The script must exist and be wired in. A guard nothing ships is this bug one level up.
# --------------------------------------------------------------------------------------


def test_script_exists() -> None:
    assert SCRIPT.is_file(), f"{SCRIPT} is missing"


def test_script_does_not_write_into_the_shared_worktrees_directory() -> None:
    """core#1197 AC4. The whole point of streaming is that five departments share that dir."""
    body = SCRIPT.read_text(encoding="utf-8")
    code = "\n".join(ln for ln in body.splitlines() if not ln.lstrip().startswith("#"))
    for forbidden in ("mkdir ", "mktemp", "cp -r", "rsync", "mklink"):
        assert forbidden not in code, (
            f"{forbidden!r} appears in the script body. The context must be streamed, "
            "not staged on disk — see core#1197 AC4."
        )


# --------------------------------------------------------------------------------------
# The positive case.
# --------------------------------------------------------------------------------------


@requires_gnu_tar
def test_context_carries_the_names_the_dockerfile_expects(tmp_path: Path) -> None:
    core = _make_tree(tmp_path)
    res = _run(core, "--list-context")
    assert res.returncode == 0, res.stderr
    members = set(res.stdout.split())
    assert "datanika/pyproject.toml" in members
    assert "datanika-cloud/pyproject.toml" in members
    assert "datanika/datanika/config.py" in members


@requires_gnu_tar
def test_context_is_not_empty(tmp_path: Path) -> None:
    """A zero-member stream would satisfy every 'no unexpected member' assertion."""
    core = _make_tree(tmp_path)
    res = _run(core, "--list-context")
    assert res.returncode == 0, res.stderr
    assert len([ln for ln in res.stdout.splitlines() if ln.strip()]) >= 4


@requires_gnu_tar
def test_original_worktree_names_are_absent(tmp_path: Path) -> None:
    """The rename must HAPPEN, not merely leave the expected names reachable.

    If `--transform` were inert the stream would carry `datanika-core-fake/...`, and
    `COPY datanika/` would fail — which is the bug this script exists to fix, reappearing
    in a form that still lists plausible-looking members.
    """
    core = _make_tree(tmp_path)
    res = _run(core, "--list-context")
    assert res.returncode == 0, res.stderr
    for name in ("datanika-core-fake", "datanika-cloud-fake"):
        assert name not in res.stdout, f"{name!r} survived the rename"


@requires_gnu_tar
def test_core_edition_omits_the_cloud_tree(tmp_path: Path) -> None:
    core = _make_tree(tmp_path)
    res = _run(core, "--edition", "core", "--list-context")
    assert res.returncode == 0, res.stderr
    assert "datanika/pyproject.toml" in res.stdout
    assert "datanika-cloud/" not in res.stdout


# --------------------------------------------------------------------------------------
# Negative controls. Each is a way to get a wrong image quietly; all must be LOUD.
# --------------------------------------------------------------------------------------


def test_fails_when_the_directory_is_not_a_canonical_worktree(tmp_path: Path) -> None:
    core = tmp_path / "some-other-checkout"
    (core / "scripts").mkdir(parents=True)
    shutil.copy2(SCRIPT, core / "scripts" / SCRIPT.name)
    (core / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
    (core / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    res = _run(core, "--list-context")
    assert res.returncode != 0, "a non-canonical directory name must not be guessed at"
    assert "datanika-core-" in res.stderr


def test_cloud_edition_fails_when_the_cloud_tree_is_missing(tmp_path: Path) -> None:
    """Otherwise `COPY datanika-cloud/` fails minutes into the build instead of now."""
    core = _make_tree(tmp_path, cloud=False)
    res = _run(core, "--list-context")
    assert res.returncode != 0
    assert "--edition core" in res.stderr


def test_fails_when_the_dockerfile_is_missing(tmp_path: Path) -> None:
    core = _make_tree(tmp_path)
    (core / "Dockerfile").unlink()
    res = _run(core, "--list-context")
    assert res.returncode != 0
    assert "Dockerfile" in res.stderr


def test_rejects_an_unknown_edition(tmp_path: Path) -> None:
    core = _make_tree(tmp_path)
    res = _run(core, "--edition", "enterprise", "--list-context")
    assert res.returncode != 0
    assert "core" in res.stderr and "cloud" in res.stderr
