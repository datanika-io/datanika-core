"""The pre-push test gate must fail CLOSED.

`scripts/hooks/should-run-tests.sh` decides whether a push runs the full pytest suite.
Exit 0 = run, exit 1 = skip.

The reason this file exists is narrow and worth stating plainly: a path filter that nobody has
watched *refuse* to skip is indistinguishable from a path filter that always skips. The helm
gate in the same hook has been failing open since it was written -- on a branch with no
upstream, `git diff @{upstream}..HEAD` errors, the error is swallowed, the file list comes back
empty, and "no files changed" is read as "nothing to check". Applied to pytest, that shape
would silently skip ~4,000 tests on every first push of a branch.

So the tests below are mostly *negative* ones: break the range, remove the base ref, detach
HEAD, and assert the answer is still RUN.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[2] / "scripts" / "hooks" / "should-run-tests.sh"

RUN = 0
SKIP = 1

# CI is Linux and always has bash; this only bites a Windows dev without Git Bash on PATH.
bash = shutil.which("bash")
pytestmark = pytest.mark.skipif(bash is None, reason="needs bash to execute the hook helper")


def _clean_env() -> dict:
    """An environment with every ``GIT_*`` variable stripped.

    This is not defensive tidiness; without it these tests are wrong in exactly the place they
    are supposed to work. **git exports ``GIT_DIR`` (and ``GIT_INDEX_FILE``, ``GIT_PREFIX``, …)
    to its hooks.** So when this suite runs from inside `pre-push` -- the only context that
    actually matters here -- every ``git`` subprocess below inherits ``GIT_DIR`` and operates on
    the REAL repository instead of the ``tmp_path`` one, no matter what ``cwd=`` says.

    It fails nowhere else: standalone and in a plain CI run there is no ``GIT_DIR``, so the
    tests pass and look fine. Measured: with ``GIT_DIR`` set, 19 of 20 error; without it, 20
    pass. Reproduce with
    ``GIT_DIR=/path/to/.git pytest tests/test_hooks/test_pre_push_gating.py``.
    """
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
        env=_clean_env(),
    ).stdout.strip()


def decide(repo: Path, *args: str) -> subprocess.CompletedProcess:
    """Invoke the helper inside `repo`. Returns the completed process (do not check=True)."""
    return subprocess.run(
        [bash, str(HOOK), *args],
        cwd=repo,
        capture_output=True,
        text=True,
        env=_clean_env(),
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A git repo with a `origin/dev` remote-tracking ref and one commit on a branch."""
    r = tmp_path / "r"
    r.mkdir()
    git(r, "init", "-q", "-b", "dev")
    git(r, "config", "user.email", "t@example.com")
    git(r, "config", "user.name", "t")
    (r / "seed.py").write_text("x = 1\n")
    git(r, "add", "seed.py")
    git(r, "commit", "-qm", "seed")
    # Fabricate origin/dev pointing at this commit, as a real clone would have.
    git(r, "update-ref", "refs/remotes/origin/dev", "HEAD")
    git(r, "checkout", "-q", "-b", "feature")
    return r


def commit(repo: Path, path: str, content: str = "changed\n") -> None:
    p = repo / path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)
    git(repo, "add", path)
    git(repo, "commit", "-qm", f"touch {path}")


# ---------------------------------------------------------------- fails closed


def test_missing_base_ref_runs_everything(repo: Path):
    """The helm gate's exact bug: no upstream to diff against must not mean 'nothing changed'."""
    commit(repo, "README.md")  # a change that WOULD otherwise skip
    git(repo, "update-ref", "-d", "refs/remotes/origin/dev")
    r = decide(repo)
    assert r.returncode == RUN, f"unresolvable base ref must RUN, got: {r.stdout}"
    assert "cannot resolve base ref" in r.stdout


def test_broken_explicit_range_runs_everything(repo: Path):
    commit(repo, "README.md")
    r = decide(repo, "not-a-real-ref..also-not-real")
    assert r.returncode == RUN, f"a bad range must RUN, got: {r.stdout}"


def test_unrelated_history_runs_everything(repo: Path, tmp_path: Path):
    """No merge-base is an error path, not a licence to skip."""
    commit(repo, "README.md")
    git(repo, "checkout", "-q", "--orphan", "orphan")
    git(repo, "commit", "-qm", "orphan root", "--allow-empty")
    r = decide(repo)
    assert r.returncode == RUN, f"unrelated histories must RUN, got: {r.stdout}"


def test_range_with_no_files_runs_everything(repo: Path):
    """Empty file list is consistent with a silently-wrong range, so it must not skip."""
    r = decide(repo)  # feature == origin/dev, nothing changed
    assert r.returncode == RUN, f"empty diff must RUN, got: {r.stdout}"
    assert "no changed files" in r.stdout


def test_detached_head_runs_everything(repo: Path):
    commit(repo, "README.md")
    sha = git(repo, "rev-parse", "HEAD")
    git(repo, "checkout", "-q", sha)
    git(repo, "update-ref", "-d", "refs/remotes/origin/dev")
    r = decide(repo)
    assert r.returncode == RUN, f"detached HEAD without a base must RUN, got: {r.stdout}"


# ---------------------------------------------------------------- earns a skip


def test_doc_only_change_skips(repo: Path):
    commit(repo, "README.md")
    commit(repo, "docs/guide.md")
    r = decide(repo)
    assert r.returncode == SKIP, f"doc-only push should skip, got: {r.stdout}"


def test_mode_only_change_skips(repo: Path):
    """The file-mode-only rebase that paid for a full suite run."""
    git(repo, "update-index", "--chmod=+x", "seed.py")
    git(repo, "commit", "-qm", "chmod +x")
    r = decide(repo)
    assert r.returncode == SKIP, f"mode-only push should skip, got: {r.stdout}"
    assert "mode-only" in r.stdout


# ---------------------------------------------------------------- must not skip


@pytest.mark.parametrize(
    "path",
    [
        "datanika/services/thing.py",
        "pyproject.toml",
        "uv.lock",
        "i18n/en.json",  # test_i18n asserts locale key parity
        "docker-compose.yml",  # test_deploy_service_coverage reads the service list
        "scripts/deploy-bluegreen.sh",  # ditto, the deploy steps
        "deploy/helm/datanika/values.yaml",
        "Dockerfile",
        "docs/conf.py",  # a .py under docs/ is code, not a doc
        "tests/test_x.py",
        "some/unrecognised/thing.bin",  # unknown extension defaults to RUN
    ],
)
def test_relevant_paths_run(repo: Path, path: str):
    commit(repo, path)
    r = decide(repo)
    assert r.returncode == RUN, f"{path} must RUN, got: {r.stdout}"


def test_one_relevant_file_among_docs_still_runs(repo: Path):
    commit(repo, "README.md")
    commit(repo, "datanika/services/thing.py")
    commit(repo, "docs/guide.md")
    r = decide(repo)
    assert r.returncode == RUN, f"a mixed push must RUN, got: {r.stdout}"
    assert "thing.py" in r.stdout


def test_helper_is_executable_and_syntactically_valid():
    assert HOOK.exists(), f"{HOOK} is missing"
    r = subprocess.run([bash, "-n", str(HOOK)], capture_output=True, text=True)
    assert r.returncode == 0, f"syntax error in {HOOK}: {r.stderr}"


def test_suite_is_hermetic_when_git_exports_git_dir(repo: Path, monkeypatch):
    """Regression for the bug that made this file pass everywhere except inside the hook.

    git sets GIT_DIR for its hooks. A test that shells out to git then silently retargets the
    real repository -- so these tests errored only when run from `pre-push`, which is the one
    place they exist to run. Setting GIT_DIR to a bogus path must change nothing.
    """
    monkeypatch.setenv("GIT_DIR", str(repo / "nonexistent-on-purpose.git"))
    commit(repo, "README.md")
    r = decide(repo)
    assert r.returncode == SKIP, f"GIT_DIR must not leak into the helper, got: {r.stdout}"
