"""The promotion gate must tell "no verdict" apart from "verdict says no".

core#1287. `verify_e2e_attribution.py` has crashed twice on a change to
`e2e_tier_streak.py`'s contract (core#1205, core#1285), both times found by a human
mid-promotion. An unhandled Python exception **also exits 1**, so to anything
reading the exit code — including a person under time pressure — "the gate refused"
and "the gate broke" are the same signal. Measured with a crash injected on
purpose:

    refusal : exit 1, verdict file written saying REFUSED
    crash   : exit 1, NO verdict file
    network : exit 1, NO verdict file

So the gate consumes a **positive artifact**. These tests drive the real script
with stub gates in a temp directory: no network, and every branch of the decision
is exercised against the shape it must reject.

The stubs are the point. A test that could only run against live GitHub would be
skipped exactly when the suite is being trusted.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

_REAL = Path(__file__).resolve().parents[2] / "scripts" / "promotion_gate.sh"

_BLAST_OK = "import sys; print('blast ok'); sys.exit(0)\n"
_BLAST_REFUSE = "import sys; print('blast REFUSED', file=sys.stderr); sys.exit(2)\n"

_ATTR_TEMPLATE = """\
import sys
args = sys.argv[1:]
sha = args[args.index("--sha") + 1]
path = args[args.index("--verdict-file") + 1] if "--verdict-file" in args else None
{body}
"""


def _attr(body: str) -> str:
    return _ATTR_TEMPLATE.format(body=body)


WRITES_OK = _attr(
    'open(path, "w").write(f"PROMOTION-GATE OK sha={sha} at=X checks=all\\n")\nsys.exit(0)\n'
)
WRITES_REFUSED = _attr(
    'open(path, "w").write(f"PROMOTION-GATE REFUSED sha={sha} at=X see stderr\\n")\nsys.exit(1)\n'
)
WRITES_NOTHING_AND_CRASHES = _attr('raise RuntimeError("simulated contract break")\n')
_OTHER_SHA = "0" * 40
WRITES_WRONG_SHA = _attr(
    f'open(path, "w").write("PROMOTION-GATE OK sha={_OTHER_SHA} at=X c=all\\n")\nsys.exit(0)\n'
)


def _clean_env() -> dict:
    """An environment with every ``GIT_*`` variable stripped.

    core#1307, and it is not defensive tidiness -- without it this fixture DESTROYS the
    repository you are pushing from. **git exports ``GIT_DIR`` to its hooks**, and the pre-push
    hook runs ``tests/test_deploy``. Under it, ``cwd=tmp_path`` isolates the directory and
    nothing isolates the repository: ``git init`` re-initialises the REAL repo, ``cwd`` becomes
    its work tree, and the ``git add -A`` below records every tracked file as deleted because
    none of them exist under ``tmp_path``.

    Measured 2026-09-12: a push left the worktree at ``ccbb08b``, author ``t <t@example.com>``,
    subject ``seed``, with a two-file tree and 889 files deleted. git even says so --
    ``warning: re-init: ignored --initial-branch=main`` -- and ``capture_output=True`` swallows it.

    ⚠️ It fails NOWHERE else. Standalone and in a plain CI run there is no ``GIT_DIR``, so this
    file passes and looks fine, which is why it survived review and a required check.

    Copied in spirit from ``tests/test_hooks/test_pre_push_gating.py``, which has carried this
    fix -- and this explanation -- since before the incident. That it was not inherited here is
    what ``test_git_env_isolation.py`` now prevents.
    """
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


@pytest.fixture
def gate(tmp_path: Path):
    """The REAL script, beside stub gates, in a throwaway git repo.

    ⚠️ `bash` is resolved with `shutil.which` and the RESOLVED PATH is used. A bare
    `["bash", ...]` is resolved by CreateProcess, which finds
    `C:\\Windows\\System32\\bash.exe` — **WSL** — before Git Bash, and the script
    then dies with exit 127 and a message about localhost proxies that has nothing
    to do with the thing under test. Same class as the `MSYS_NO_PATHCONV` case: the
    harness runs a different program than the author meant and blames the subject.
    """
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("bash unavailable")
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copy(_REAL, scripts / "promotion_gate.sh")
    assert (scripts / "promotion_gate.sh").stat().st_size > 500, "copied an empty script"

    for cmd in (
        ["git", "init", "-q", "-b", "main"],
        ["git", "config", "user.email", "t@example.com"],
        ["git", "config", "user.name", "t"],
    ):
        subprocess.run(cmd, cwd=tmp_path, check=True, capture_output=True, env=_clean_env())
    # ASSERT THE ISOLATION IS IN EFFECT, before the destructive command runs. If GIT_DIR
    # redirected `git init` at another repository, `.git` is not here -- and the `git add -A`
    # on the next line would stage this fixture's two files into that repo as a commit
    # deleting everything else. Failing here costs a red test; not failing here cost a branch.
    assert (tmp_path / ".git").is_dir(), (
        "`git init` created no repository in tmp_path, so an ambient GIT_DIR redirected it at "
        "another one (core#1307). Refusing to run `git add -A` against it."
    )
    (tmp_path / "seed.txt").write_text("x", encoding="utf-8")
    subprocess.run(
        ["git", "add", "-A"], cwd=tmp_path, check=True, capture_output=True, env=_clean_env()
    )
    subprocess.run(
        ["git", "commit", "-qm", "seed"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        env=_clean_env(),
    )

    def run(blast: str, attr: str) -> subprocess.CompletedProcess[str]:
        (scripts / "blast_radius.py").write_text(blast, encoding="utf-8")
        (scripts / "verify_e2e_attribution.py").write_text(attr, encoding="utf-8")
        return subprocess.run(
            [bash, str(scripts / "promotion_gate.sh"), "HEAD", "HEAD"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=_clean_env(),
        )

    return run


class TestTheThreeOutcomesAreDistinct:
    def test_ok_verdict_passes(self, gate) -> None:
        r = gate(_BLAST_OK, WRITES_OK)
        assert r.returncode == 0, r.stdout + r.stderr
        assert "PASSED" in r.stdout

    def test_a_crash_is_exit_3_no_verdict(self, gate) -> None:
        """The whole reason this script exists."""
        r = gate(_BLAST_OK, WRITES_NOTHING_AND_CRASHES)
        assert r.returncode == 3, f"got {r.returncode}: {r.stdout + r.stderr}"
        assert "NO VERDICT" in r.stderr
        assert "tooling noise" in r.stderr, (
            "the message must name the predicted human failure mode, not just the state"
        )

    def test_a_refusal_is_exit_4_not_exit_3(self, gate) -> None:
        """A refusal has a reading behind it; a crash does not. Different exits."""
        r = gate(_BLAST_OK, WRITES_REFUSED)
        assert r.returncode == 4, f"got {r.returncode}: {r.stdout + r.stderr}"
        assert "NOT ok" in r.stderr

    def test_a_stale_verdict_is_exit_5(self, gate) -> None:
        """core#876 applied to the gate's own artifact."""
        r = gate(_BLAST_OK, WRITES_WRONG_SHA)
        assert r.returncode == 5, f"got {r.returncode}: {r.stdout + r.stderr}"
        assert "not evidence about this one" in r.stderr

    def test_a_blast_radius_refusal_stops_everything(self, gate) -> None:
        r = gate(_BLAST_REFUSE, WRITES_OK)
        assert r.returncode == 3
        assert "No promotion" in r.stderr


class TestTheExitCodesActuallyDiscriminate:
    def test_crash_and_refusal_do_not_share_an_exit_code(self, gate) -> None:
        """Anti-vacuity for the entire premise.

        If these ever collapse to one value, this script has stopped doing the only
        thing it was built for, while every individual test above could still pass.
        """
        crash = gate(_BLAST_OK, WRITES_NOTHING_AND_CRASHES).returncode
        refusal = gate(_BLAST_OK, WRITES_REFUSED).returncode
        ok = gate(_BLAST_OK, WRITES_OK).returncode
        stale = gate(_BLAST_OK, WRITES_WRONG_SHA).returncode
        assert len({crash, refusal, ok, stale}) == 4, (
            f"outcomes collapsed: crash={crash} refusal={refusal} ok={ok} stale={stale}"
        )

    def test_the_underlying_exit_code_really_is_ambiguous(self, gate) -> None:
        """Pin the premise itself: the thing this script works around is real.

        Both stubs exit 1. If a future Python stopped doing that, the justification
        in the script's header would be stale and someone should re-read it.
        """
        import sys

        crash_rc = subprocess.run(
            [sys.executable, "-c", "raise RuntimeError('x')"], capture_output=True
        ).returncode
        assert crash_rc == 1, (
            f"an unhandled exception now exits {crash_rc}, not 1 -- the ambiguity this "
            "script exists to resolve may no longer exist; re-read its header."
        )
