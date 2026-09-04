"""The live volume copy must not discard tar's own tear signal (core#1017).

`backup-offsite.sh` tars `uploaded_files` and `dbt_projects` **live, with no
quiesce**. A DuckDB file being written at 03:00 is captured mid-write. The
archive is a valid gzip stream, `tar tzf` lists every member, the gpg round-trip
succeeds, and the file inside is torn: the bytes are all there, some of them are
from two different moments.

The line used to be:

    tar czf "${VTAR}" -C "${MP}" . 2>/dev/null || true

with a comment calling *"file changed as we read it"* expected and not
corruption. The first half was right; the second was backwards. **That message
is the tear condition**, and the line threw away both halves of the only
evidence that exists — the exit code (`|| true`) and the per-file diagnostic
(`2>/dev/null`). Every downstream gate is blind to it: the member count is
satisfied, `gzip -t` passes, the ciphertext round-trips.

MEASURED ON THE PRODUCTION BOX (in /tmp, against a copy of the real
`_docs_samples/warehouse.duckdb` with DuckDB writing it continuously):

    tar under write load          20 of 40 archives flagged, exit 1
    tar with the writer stopped    0 of 1, exit 0, empty stderr    <- control
    retry-on-change               10 of 10 rounds converged in <=5 attempts
    the same, writer stopped       converged on attempt 1          <- control

Why retry rather than a snapshot or a pause: `/` is **ext4, no LVM, no reflink**
(measured — `cp --reflink=always` answers "Operation not supported"), so there is
no snapshot to take; and both writers are `datanika-app-b` and `datanika-celery`,
so pausing means freezing the serving container nightly. The archives take **23 ms
and 50 ms**, which makes simply taking the copy again cheaper than either.

🚨 THE CORRECTION THIS GUARD REALLY EXISTS FOR. A DuckDB integrity check on the
restored file — the issue's option 2 — is a **weak** detector: of the archives tar
flagged, essentially all still opened and completed a full `EXPORT DATABASE`. A
torn copy usually looks fine. So the only moment the tear is observable is at the
copy, and a later edit that "simplifies" the retry away cannot be caught by
anything downstream.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "deploy" / "server" / "backup-offsite.sh"


def _code() -> list[str]:
    """Executable lines only.

    Comments are stripped first, deliberately: this module's own subject is a line
    that is now QUOTED in a comment explaining why it was removed, so a guard
    reading raw text would be satisfied by the explanation of the defect.
    """
    return [
        line
        for line in SCRIPT.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def _tar_lines() -> list[str]:
    return [ln for ln in _code() if re.search(r"\btar\s+czf\b", ln)]


def _volume_loop_body() -> list[str]:
    """Executable lines between `for VOL in ...` and the loop's own `done`.

    ⚠️ `ln.strip() == "done"` finds the RETRY loop's `done` first and silently
    returns a 15-line body. The volume loop's `done` is the one at column 0.
    """
    code = _code()
    start = next(i for i, ln in enumerate(code) if re.search(r"^for VOL in", ln))
    end = next(i for i, ln in enumerate(code[start + 1 :], start + 1) if ln == "done")
    assert end - start > 40, (
        f"the volume-loop body is only {end - start} lines — that is the inner retry "
        "loop, so every assertion over it would be measuring the wrong block"
    )
    return code[start:end]


def _vol_metrics_assignment() -> str:
    """Just the `VOL_METRICS=...` assignment, which is what actually emits a series.

    🚨 Why this exists rather than a substring search over the whole script.
    Asserting `"datanika_backup_files_torn" in body` was measured GREEN against a
    mutation that deleted the metric line entirely — because the script also names
    the metric in a *warning message* telling the operator what to watch. The guard
    was satisfied by a mention of the fix rather than by the fix. Only mutating the
    real artifact found it; the guard's own suite agreed with the guard throughout.
    """
    body = _volume_loop_body()
    start = next(i for i, ln in enumerate(body) if ln.strip().startswith("VOL_METRICS="))
    out = [body[start]]
    for line in body[start + 1 :]:
        out.append(line)
        if line.strip().endswith('"') and not line.strip().startswith("VOL_METRICS="):
            break
    return "\n".join(out)


def test_the_volume_tar_does_not_discard_its_own_verdict():
    lines = _tar_lines()
    assert lines, "no `tar czf` line found — the parser is broken, not the script"
    for line in lines:
        assert "2>/dev/null" not in line, (
            f"the volume tar sends stderr to /dev/null: {line.strip()!r}. That is where "
            "`file changed as we read it` goes, and it is the only signal that the copy "
            "is not a point-in-time one."
        )
        assert "|| true" not in line, (
            f"the volume tar swallows its exit code: {line.strip()!r}. GNU tar exits 1 "
            "for exactly the condition this backup needs to know about."
        )


def test_tar_stderr_is_captured_and_inspected_for_the_tear_message():
    body = "\n".join(_code())
    assert re.search(r"tar\s+czf[^\n]*2>\"?\$\{?TAR_ERR", body), (
        "tar's stderr must be captured to a file, not dropped"
    )
    assert "changed as we read it" in body, (
        "nothing greps for GNU tar's tear message, so capturing stderr achieves nothing"
    )


def test_a_changed_file_causes_the_copy_to_be_retaken():
    body = "\n".join(_code())
    assert re.search(r"for\s+TRY\s+in\s+1\s+2\s+3\s+4\s+5", body), (
        "there is no retry loop. Detecting the tear and shipping it anyway every time "
        "grades the defect instead of removing it — measured to converge 10/10 within "
        "5 attempts under continuous write load."
    )


def test_an_unconverged_volume_still_ships():
    """The direction that matters more than the fix.

    Turning the warning into `exit 1` looks like rigour and is strictly worse: it
    means NO off-site copy at all on any night a file happened to move, and an
    empty off-site directory is the failure this whole script exists to prevent.
    """
    # Scoped to the torn branch, and both halves of that scoping were measured.
    # A fixed 25-line window from the first `VOL_TORN` was BLIND — the `exit 1` a
    # well-meaning edit adds sits ~30 lines lower. Widening to the whole volume
    # loop then went red on the loop's LEGITIMATE aborts (missing mountpoint,
    # member-count shortfall, ciphertext mismatch), which must keep aborting.
    # The branch is the right unit.
    body = _volume_loop_body()
    start = next(i for i, ln in enumerate(body) if 'VOL_TORN}" -eq 0' in ln)
    end = next(i for i, ln in enumerate(body[start + 1 :], start + 1) if ln.strip() == "fi")
    branch = body[start : end + 1]
    assert any(ln.strip() == "else" for ln in branch), (
        "no else-branch found — the torn case is not handled at all"
    )
    # ⚠️ NOT anchored with `^\s*`. That version was measured blind against
    # `echo "..."; exit 1`, which is exactly how such an edit gets written — the
    # abort rides along on an existing line rather than getting one of its own.
    offenders = [ln for ln in branch if re.search(r"\bexit\s+[1-9]", ln)]
    assert not offenders, (
        f"the torn branch aborts the backup: {[o.strip() for o in offenders]}. A "
        "possibly-torn archive beats no archive — an empty off-site directory is "
        "the failure this whole script exists to prevent. Report it, do not refuse."
    )


def test_the_tear_is_recorded_in_a_metric_per_volume():
    """Size and count cannot carry this: a torn archive satisfies both.

    Nor can anything downstream recover it — a restored torn DuckDB file usually
    opens and exports cleanly. If it is not written here it is not knowable at all.
    """
    assign = _vol_metrics_assignment()
    assert 'datanika_backup_files_torn{volume=\\"${VOL}\\"}' in assign, (
        "the torn flag is not in the VOL_METRICS assignment, so no series is "
        "emitted for it. Naming the metric in a log line does not create it — that "
        "exact confusion is what this helper's docstring records."
    )
    assert "datanika_backup_files_tar_attempts" in assign


def test_the_new_metrics_are_declared_with_help_and_type():
    """An undeclared series still scrapes, so this is a legibility gate, not a
    correctness one — but the sibling metrics all declare and a silent exception
    is how the next person concludes the metric is not real."""
    text = SCRIPT.read_text(encoding="utf-8")
    for metric in ("datanika_backup_files_torn", "datanika_backup_files_tar_attempts"):
        assert f"# HELP {metric}" in text, f"{metric} has no HELP line"
        assert f"# TYPE {metric}" in text, f"{metric} has no TYPE line"


def test_the_metric_is_emitted_inside_the_per_volume_loop():
    """Emitted once, outside the loop, it would describe whichever volume ran last."""
    assert "datanika_backup_files_torn" in _vol_metrics_assignment(), (
        "the torn metric is assembled outside the per-volume loop"
    )
