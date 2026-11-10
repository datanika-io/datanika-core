"""The release publisher takes optional notes and does not fail a second time (core#680).

``release.yml`` used to call ``gh release create --generate-notes --verify-tag`` and
nothing else. A tag that needed to say something had no file to say it in, and a
second run of the same tag failed because the release already existed.

Two properties, both driven by executing the step's own ``run:`` block:

* ``docs/release-notes/<tag>.md`` in the tagged tree is passed as ``--notes-file``.
  ``--generate-notes`` stays, so the pull-request list still appends. No file means
  the create command is the one this workflow already had.
* ``gh release view`` succeeding makes the step a no-op. The create is not run, and
  the step exits 0.

The ``gh`` on ``PATH`` is a recorder. Nothing here talks to GitHub, and nothing
creates a release. The header comment is separate: a tag runs the workflow as of
the tagged commit, so the step only affects tags cut after the change reaches
``master``.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"

_TAGGED_COMMIT = (
    "tag runs the workflow as of the tagged commit, so this only affects tags "
    "cut after the change reaches master"
)


def _bash() -> str:
    git_bash = Path(r"C:\Program Files\Git\bin\bash.exe")
    if git_bash.is_file():
        return str(git_bash)
    found = shutil.which("bash")
    if not found:
        pytest.skip("bash unavailable")
    return found


def _publish_script() -> str:
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    scripts = [
        step.get("run", "")
        for step in doc["jobs"]["release"]["steps"]
        if "gh release create" in step.get("run", "")
    ]
    assert len(scripts) == 1
    return scripts[0]


def _calls(log: Path) -> list[list[str]]:
    if not log.exists():
        return []
    calls: list[list[str]] = []
    for line in log.read_text(encoding="utf-8").splitlines():
        calls.append([part for part in line.split("\0") if part])
    return calls


def _creates(calls: list[list[str]]) -> list[list[str]]:
    return [args for args in calls if args[:2] == ["release", "create"]]


def _run(tmp: Path, *, tag: str, view_rc: int, with_notes: bool) -> tuple[int, list[list[str]]]:
    bin_dir = tmp / "bin"
    tree = tmp / "tree"
    bin_dir.mkdir()
    tree.mkdir()
    log = tmp / "gh.log"
    fake = bin_dir / "gh"
    fake.write_bytes(
        b"#!/bin/bash\n"
        b"set -euo pipefail\n"
        b'{ printf "%s\\0" "$@"; printf "\\n"; } >> "$GH_LOG"\n'
        b'if [ "$1" = "release" ] && [ "$2" = "view" ]; then exit "$GH_VIEW_RC"; fi\n'
        b'if [ "$1" = "release" ] && [ "$2" = "create" ]; then exit 0; fi\n'
        b'echo "unexpected gh: $*" >&2\n'
        b"exit 2\n"
    )
    # ubuntu-latest has a real gh later on PATH. A recorder without the
    # executable bit is skipped, that gh exits 1, and GH_LOG stays empty.
    fake.chmod(0o755)
    if with_notes:
        notes = tree / "docs" / "release-notes"
        notes.mkdir(parents=True)
        (notes / f"{tag}.md").write_text("notes\n", encoding="utf-8", newline="\n")
    env = os.environ.copy()
    env["PATH"] = str(bin_dir) + os.pathsep + env.get("PATH", "")
    env["GH_LOG"] = str(log)
    env["GH_VIEW_RC"] = str(view_rc)
    env["TAG"] = tag
    env["GH_TOKEN"] = "not-a-token"
    env["GITHUB_REPOSITORY"] = "datanika-io/datanika-core"
    env["GITHUB_SERVER_URL"] = "https://github.com"
    proc = subprocess.run(
        [_bash(), "-s"],
        input=_publish_script(),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=tree,
        env=env,
        check=False,
    )
    return proc.returncode, _calls(log)


def test_header_states_the_tagged_commit_limit() -> None:
    header = WORKFLOW.read_text(encoding="utf-8").split("\non:", 1)[0]
    collapsed = " ".join(line.lstrip("#").strip() for line in header.splitlines())
    assert _TAGGED_COMMIT in collapsed


def test_an_existing_release_is_a_noop(tmp_path: Path) -> None:
    rc, calls = _run(tmp_path, tag="v9.9.9", view_rc=0, with_notes=True)
    assert rc == 0, calls
    assert _creates(calls) == []


def test_an_existing_release_without_notes_is_a_noop(tmp_path: Path) -> None:
    rc, calls = _run(tmp_path, tag="v9.9.8", view_rc=0, with_notes=False)
    assert rc == 0, calls
    assert _creates(calls) == []


def test_notes_file_is_passed_and_generate_notes_stays(tmp_path: Path) -> None:
    rc, calls = _run(tmp_path, tag="v9.9.9", view_rc=1, with_notes=True)
    assert rc == 0, calls
    create = _creates(calls)
    assert len(create) == 1
    args = create[0]
    assert "--generate-notes" in args
    assert "--verify-tag" in args
    notes_at = args.index("--notes-file")
    assert args[notes_at + 1].endswith("docs/release-notes/v9.9.9.md")


def test_absent_notes_file_keeps_todays_create(tmp_path: Path) -> None:
    rc, calls = _run(tmp_path, tag="v9.9.8", view_rc=1, with_notes=False)
    assert rc == 0, calls
    create = _creates(calls)
    assert len(create) == 1
    args = create[0]
    assert "--notes-file" not in args
    assert "--generate-notes" in args
    assert "--verify-tag" in args
