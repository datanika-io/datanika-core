"""`scripts/mutation_probe.py` mutates tracked source in place, so its recovery is
the only thing standing between an audit run and a corrupted worktree.

**This file exists because prose was not enough.** On 2026-08-30 a mutation harness
hit a two-minute command timeout mid-run; `finally:` never executed and a mutated
constant was left sitting in `datanika/`. The lesson is not "add a try/finally" —
it already had one. It is that **cleanup which only runs on a clean exit is not
cleanup**, so the recovery information has to be on disk before the mutation, and
the recovery itself has to be runnable by something that is not the dying process.

The load-bearing test here is `TestAHardKillIsRecoverable`. It starts a real probe
against a sandbox repo, waits until it has **observed** a mutant in the working
tree, and kills the process with `Popen.kill()` — `TerminateProcess` on Windows,
`SIGKILL` elsewhere — which runs no `finally:`, no `atexit`, and no signal handler.
Recovery then has to come from the sentinel alone.

Note the `assert saw_a_mutation` in that test: without it, a kill that happened to
land between mutants would leave a clean tree and the test would pass having
demonstrated nothing. That is the exact failure this whole audit is about.
"""

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

PROBE = Path(__file__).resolve().parents[2] / "scripts" / "mutation_probe.py"

sys.path.insert(0, str(PROBE.parent))
import mutation_probe as mp  # noqa: E402

_ORIGINAL = (
    'FLAG = True\nKEY = "alpha"\nLIMIT = 10\n\n\n'
    'def pick(n):\n    """Doc."""\n    return KEY if n >= LIMIT else "other"\n'
)


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-c", "user.email=t@example.com", "-c", "user.name=T", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )


@pytest.fixture
def sandbox(tmp_path: Path) -> Path:
    """A real git repo with a real (slow) test, so a kill can land mid-mutation."""
    repo = tmp_path / "repo"
    (repo / "pkg").mkdir(parents=True)
    (repo / "pkg" / "__init__.py").write_bytes(b"")
    (repo / "pkg" / "m.py").write_text(_ORIGINAL, encoding="utf-8", newline="")
    (repo / "test_m.py").write_text(
        textwrap.dedent(
            """
            import time

            import pkg.m


            def test_flag():
                time.sleep(1.5)          # wide enough to be killed inside
                assert pkg.m.FLAG is True
                assert pkg.m.pick(20) == "alpha"
            """
        ),
        encoding="utf-8",
    )
    # The state dir MUST be ignored, or the probe's own bookkeeping makes the
    # tree dirty and every later run refuses to start.
    (repo / ".gitignore").write_text(f"{mp.DEFAULT_STATE_DIRNAME}/\n", encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "seed")
    return repo


def _run_probe(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(PROBE), "--repo", str(repo), *args],
        capture_output=True,
        text=True,
        timeout=300,
        env=dict(os.environ, UV_NO_SYNC="1"),
    )


class TestAHardKillIsRecoverable:
    def test_a_kill_mid_mutation_leaves_a_sentinel_that_restores(self, sandbox: Path):
        target = sandbox / "pkg" / "m.py"
        original = target.read_bytes()

        proc = subprocess.Popen(
            [
                sys.executable,
                str(PROBE),
                "--repo",
                str(sandbox),
                "--module",
                "pkg/m.py",
                "--tests",
                "test_m.py",
                "--limit",
                "8",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=dict(os.environ, UV_NO_SYNC="1"),
        )
        saw_a_mutation = False
        deadline = time.monotonic() + 120
        try:
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    break
                if target.read_bytes() != original:
                    saw_a_mutation = True
                    proc.kill()
                    break
                time.sleep(0.02)
        finally:
            proc.kill()
            proc.wait(timeout=60)

        # Without this the test could "pass" having killed an idle process and
        # restored a tree that was never dirty — proving nothing at all.
        assert saw_a_mutation, "never observed a mutant in the tree; the kill proves nothing"

        # A hard kill runs no finally/atexit/handler, so the mutant is still there.
        assert target.read_bytes() != original, (
            "the process was killed while a mutant was applied, yet the file is "
            "already pristine — the kill did not land where this test needs it"
        )
        sentinel = sandbox / mp.DEFAULT_STATE_DIRNAME / "SENTINEL"
        assert sentinel.is_file(), "no sentinel on disk after a kill — recovery is impossible"

        # Recovery from the sentinel alone, by a process that is not the dead one.
        r = _run_probe(sandbox, "--restore")
        assert r.returncode == 0, r.stdout + r.stderr
        assert target.read_bytes() == original
        assert _git(sandbox, "status", "--porcelain").stdout.strip() == ""
        assert not sentinel.exists(), "a cleared sentinel must not linger"

    def test_restore_is_idempotent(self, sandbox: Path):
        for _ in range(3):
            r = _run_probe(sandbox, "--restore")
            assert r.returncode == 0, r.stdout


class TestPreflightFailsClosed:
    def test_a_dirty_tree_refuses_rather_than_risking_a_restore_over_real_work(self, sandbox: Path):
        (sandbox / "pkg" / "m.py").write_text("FLAG = True  # unsaved work\n", encoding="utf-8")
        r = _run_probe(sandbox, "--module", "pkg/m.py", "--tests", "test_m.py")
        assert r.returncode == 2, r.stdout
        assert "REFUSING" in r.stdout
        # and it must not have touched anything
        assert "unsaved work" in (sandbox / "pkg" / "m.py").read_text(encoding="utf-8")

    def test_a_red_baseline_aborts_without_mutating(self, sandbox: Path):
        (sandbox / "test_m.py").write_text("def test_x():\n    assert False\n", encoding="utf-8")
        _git(sandbox, "add", "-A")
        _git(sandbox, "commit", "-qm", "red")
        original = (sandbox / "pkg" / "m.py").read_bytes()
        r = _run_probe(sandbox, "--module", "pkg/m.py", "--tests", "test_m.py")
        assert r.returncode == 6, r.stdout
        assert "ALREADY RED" in r.stdout
        assert (sandbox / "pkg" / "m.py").read_bytes() == original

    def test_verify_reports_a_dirty_tree(self, sandbox: Path):
        """The verifier must be able to say no, or its 'clean' means nothing."""
        (sandbox / "junk.txt").write_text("x", encoding="utf-8")
        r = _run_probe(sandbox, "--check")
        assert r.returncode == 4, r.stdout
        assert "IS DIRTY" in r.stdout


class TestMutantEnumeration:
    def test_docstrings_are_never_mutated(self):
        src = 'def f():\n    """Not a mutant."""\n    return "yes"\n'
        values = [m.before for m in mp.enumerate_mutants(src) if m.kind == "str"]
        assert "yes" in values
        assert "Not a mutant." not in values

    def test_module_docstrings_are_never_mutated(self):
        src = '"""Module doc."""\nX = "v"\n'
        assert [m.before for m in mp.enumerate_mutants(src) if m.kind == "str"] == ["v"]

    def test_it_finds_the_shapes_we_have_actually_shipped_as_bugs(self):
        src = (
            'A = True\nK = "auth_source"\n\n\n'
            'def g(m, n):\n    return m == "POST" and n in ("a",)\n'
        )
        kinds = {m.kind for m in mp.enumerate_mutants(src)}
        assert kinds == {"bool", "str", "cmp", "boolop"}

    def test_enumeration_is_deterministic(self):
        a = [str(m) for m in mp.enumerate_mutants(_ORIGINAL)]
        b = [str(m) for m in mp.enumerate_mutants(_ORIGINAL)]
        assert a == b and a == sorted(a, key=lambda s: int(s.split()[0][1:]))


class TestApplyingAMutant:
    def test_a_string_mutant_changes_exactly_one_literal(self):
        src = 'K = "alpha"\nJ = "beta"\n'
        m = next(x for x in mp.enumerate_mutants(src) if x.before == "alpha")
        out = mp.apply_mut(src, m)
        assert out == 'K = "MUTANT_alpha"\nJ = "beta"\n'

    def test_it_declines_rather_than_guessing_when_the_anchor_is_gone(self):
        """A mis-applied mutant would be reported as a survivor and read as a
        coverage gap, so an ambiguous application must return None."""
        src = 'K = "alpha"\n'
        ghost = mp.Mut(1, 0, "str", "not-in-this-line", "MUTANT_x")
        assert mp.apply_mut(src, ghost) is None

    def test_a_comparison_flip_is_applied_at_the_operator(self):
        src = "def f(n):\n    return n >= 10\n"
        m = next(x for x in mp.enumerate_mutants(src) if x.kind == "cmp")
        assert mp.apply_mut(src, m) == "def f(n):\n    return n < 10\n"
