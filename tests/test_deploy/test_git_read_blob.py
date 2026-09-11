"""Reading a blob by `<rev>:<path>` must survive argument conversion AND say "missing".

core#1284. `git show origin/dev:.github/workflows/ci.yml` fails on Git Bash with
`fatal: ambiguous argument 'origin\\dev;.github\\workflows\\ci.yml'` -- MSYS
rewrites the spec because the path after the colon is dot-prefixed. The standard
remedy, `MSYS_NO_PATHCONV=1`, breaks the other arm: every POSIX path handed to a
Windows binary stops being converted. One session hit that six times against
twenty existing documentation entries, so this replaces the need for the variable
rather than documenting it again.

The helper asks git for the object id with the spec on **stdin**, then reads the
object by its plain-hex id, which no conversion can touch.

Two properties are asserted, and each is paired with the thing it must reject:

* the read works **with the variable set and unset** -- immunity, not a coin flip;
* a spec that resolves to nothing exits non-zero and says so, rather than printing
  nothing and exiting 0, which is indistinguishable from an empty file.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / "scripts" / "git-read-blob.sh"

#: Measured trigger, and BOTH halves are required -- neither alone reproduces it:
#:
#:   HEAD:.github/workflows/ci.yml        untouched   (no slash in the rev)
#:   origin/dev:docker-compose.yml        untouched   (no leading dot in the path)
#:   origin/dev:.github/workflows/ci.yml  REWRITTEN   ->  origin\dev;.github\workflows\ci.yml
#:
#: The first draft of this module used `HEAD:` and the negative control below
#: SKIPPED rather than passing -- correctly refusing to certify a helper against
#: a spec that never had the problem. That skip is why this comment exists.
_SLASH_REV = "refs/heads/"  # any ref path works; a full symbolic name always has slashes


def _slash_rev() -> str:
    """A revision containing a slash, resolved rather than assumed.

    `refs/heads/<branch>` is used because it always contains slashes AND always
    resolves. Under a detached HEAD (CI often checks out a SHA) there is no such
    name, and a bare SHA has no slash -- so the hazard cannot be reproduced and
    the tests say so instead of quietly testing nothing.
    """
    r = subprocess.run(
        ["git", "rev-parse", "--symbolic-full-name", "HEAD"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=_REPO,
    )
    name = r.stdout.strip()
    if r.returncode != 0 or not name.startswith(_SLASH_REV):
        pytest.skip(f"HEAD has no slash-containing ref name ({name!r}); hazard not reproducible")
    return name


def _bash() -> str:
    b = shutil.which("bash")
    if not b:
        pytest.skip("bash unavailable")
    return b


def _run(spec: str, *, no_pathconv: bool) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    if no_pathconv:
        env["MSYS_NO_PATHCONV"] = "1"
    else:
        env.pop("MSYS_NO_PATHCONV", None)
    return subprocess.run(
        [_bash(), str(_SCRIPT), spec],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=_REPO,
        env=env,
    )


class TestItSurvivesArgumentConversionBothWays:
    @pytest.mark.parametrize("no_pathconv", [False, True], ids=["default", "MSYS_NO_PATHCONV=1"])
    def test_a_dot_prefixed_path_reads(self, no_pathconv: bool) -> None:
        r = _run(f"{_slash_rev()}:.github/workflows/ci.yml", no_pathconv=no_pathconv)
        assert r.returncode == 0, f"exit={r.returncode} stderr={r.stderr}"
        assert len(r.stdout) > 200, "read succeeded but returned almost nothing"
        assert "name:" in r.stdout, "does not look like the workflow file"

    @pytest.mark.parametrize("no_pathconv", [False, True], ids=["default", "MSYS_NO_PATHCONV=1"])
    def test_a_plain_path_reads(self, no_pathconv: bool) -> None:
        r = _run(f"{_slash_rev()}:docker-compose.yml", no_pathconv=no_pathconv)
        assert r.returncode == 0, f"exit={r.returncode} stderr={r.stderr}"
        assert "services:" in r.stdout

    def test_the_two_settings_return_identical_bytes(self) -> None:
        """Immunity means the same answer, not merely two successful answers."""
        spec = f"{_slash_rev()}:.github/workflows/ci.yml"
        a = _run(spec, no_pathconv=False).stdout
        b = _run(spec, no_pathconv=True).stdout
        assert a == b
        assert a, "both empty would also be 'identical' -- floor required"


class TestTheNegativeControlStillControls:
    """Pin that the naive form really does fail, so these tests target a live problem.

    If Git for Windows ever stops rewriting this shape, these assertions fail and
    someone re-reads the rationale -- rather than the suite quietly continuing to
    guard a hazard that no longer exists.
    """

    def test_git_show_on_a_dot_prefixed_path_still_breaks_by_default(self) -> None:
        """Invoked THROUGH BASH, which is the only way the hazard exists at all.

        The rewrite is done by the MSYS runtime when an MSYS process spawns a
        native Windows binary. Python is itself a native Windows process, so
        `subprocess.run(["git", "show", spec])` passes argv straight through and
        the spec arrives intact -- this control's first draft did exactly that and
        SKIPPED, reporting that the hazard had gone. It had not; the probe had
        simply stepped outside the only environment where it occurs.

        That is the second time this control refused to certify rather than
        passing, and both refusals were correct.
        """
        env = dict(os.environ)
        env.pop("MSYS_NO_PATHCONV", None)
        r = subprocess.run(
            [_bash(), "-c", 'git show "$1"', "_", f"{_slash_rev()}:.github/workflows/ci.yml"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=_REPO,
            env=env,
        )
        if r.returncode == 0:
            pytest.skip(
                "`git show <rev>:.dotted/path` now works unaided from bash -- the "
                "hazard this helper exists for may be gone; re-read the script."
            )
        assert "ambiguous argument" in r.stderr or "unknown revision" in r.stderr
        assert "\\" in r.stderr, (
            "it failed, but not by the backslash rewrite this helper exists for -- "
            "a different failure would make this control pass for the wrong reason"
        )


class TestMissingIsReportedNotImpliedByEmptiness:
    def test_a_nonexistent_path_exits_non_zero(self) -> None:
        r = _run(f"{_slash_rev()}:no/such/file/anywhere.yml", no_pathconv=False)
        assert r.returncode != 0, (
            "a spec resolving to nothing exited 0 -- that is the silent-empty bug "
            "(core#1276) in a new place"
        )
        assert "resolves to no object" in r.stderr

    def test_a_nonexistent_revision_exits_non_zero(self) -> None:
        r = _run("deadbeefdeadbeef:docker-compose.yml", no_pathconv=False)
        assert r.returncode != 0

    def test_a_directory_is_refused_rather_than_dumped(self) -> None:
        r = _run(f"{_slash_rev()}:scripts", no_pathconv=False)
        assert r.returncode == 4
        assert "not a blob" in r.stderr

    def test_no_argument_is_a_usage_error(self) -> None:
        r = subprocess.run(
            [_bash(), str(_SCRIPT)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            cwd=_REPO,
        )
        assert r.returncode == 2
