"""Bound ``runs.logs`` at write (core#1196).

The issue names the marker and that the tail survives. It does not name the cap.
256 KiB of UTF-8 is the cap: a normal dbt summary or dlt load line is stored whole,
and a retry loop cannot grow the row without a bound. The marker is
``… [truncated, N further bytes]`` with N the omitted UTF-8 bytes. The cut is the
middle.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus
from datanika.models.user import Organization
from datanika.services.execution_service import RUN_LOG_MAX_BYTES, ExecutionService
from datanika.services.maintenance_service import reap_stuck_cancelling_runs

_MARKER = re.compile(r"… \[truncated, (\d+) further bytes\]")
_HEAD = "HEAD-START\n"
_TAIL = "\nTAIL-ERROR: relation does not exist"


def _org(db_session) -> Organization:
    org = Organization(name="Acme", slug="acme-1196-log-cap")
    db_session.add(org)
    db_session.flush()
    return org


def _complete(db_session, org_id: int, logs: str):
    svc = ExecutionService()
    run = svc.create_run(db_session, org_id, NodeType.UPLOAD, 1)
    svc.start_run(db_session, org_id, run.id)
    return svc.complete_run(db_session, org_id, run.id, rows_loaded=1, logs=logs)


def _over_cap() -> str:
    """A tool log whose middle is multibyte, so a character count cannot pass as bytes."""
    return _HEAD + ("é" * RUN_LOG_MAX_BYTES) + _TAIL


def _split(stored: str) -> tuple[str, int, str]:
    matches = list(_MARKER.finditer(stored))
    assert len(matches) == 1, stored[:120]
    match = matches[0]
    return stored[: match.start()], int(match.group(1)), stored[match.end() :]


class TestAShortLogIsUnchanged:
    """Acceptance 2. A log at the limit does not stand in for a normal run."""

    def test_the_cap_is_256_kib(self):
        assert RUN_LOG_MAX_BYTES == 256 * 1024

    def test_a_short_run_log_is_stored_byte_identical(self, db_session):
        org = _org(db_session)
        normal = "Model: café | Status: success | CREATE VIEW | Time: 0.12s\n"
        assert len(normal.encode("utf-8")) < RUN_LOG_MAX_BYTES // 2

        completed = _complete(db_session, org.id, normal)

        assert completed is not None
        assert completed.logs == normal
        assert completed.logs.encode("utf-8") == normal.encode("utf-8")
        assert _MARKER.search(completed.logs) is None

    def test_a_log_exactly_at_the_cap_is_stored_byte_identical(self, db_session):
        org = _org(db_session)
        body = "Model: orders | Status: success\n"
        exact = body + ("." * (RUN_LOG_MAX_BYTES - len(body.encode("utf-8"))))
        assert len(exact.encode("utf-8")) == RUN_LOG_MAX_BYTES

        completed = _complete(db_session, org.id, exact)

        assert completed is not None
        assert completed.logs == exact
        assert _MARKER.search(completed.logs) is None


class TestALogOverTheCapKeepsItsTail:
    """Acceptance 1. The tail of a failing run is the error."""

    def test_complete_run_cuts_the_middle_and_counts_omitted_bytes(self, db_session):
        org = _org(db_session)
        original = _over_cap()
        assert len(original.encode("utf-8")) > RUN_LOG_MAX_BYTES

        completed = _complete(db_session, org.id, original)

        assert completed is not None
        stored = completed.logs
        assert stored is not None
        assert len(stored.encode("utf-8")) <= RUN_LOG_MAX_BYTES
        head, omitted, tail = _split(stored)
        assert original.startswith(head)
        assert original.endswith(tail)
        assert head.startswith(_HEAD)
        assert tail.endswith(_TAIL)
        byte_gap = len(original.encode("utf-8")) - len(head.encode("utf-8")) - len(tail.encode())
        assert omitted == byte_gap
        assert byte_gap != len(original) - len(head) - len(tail)
        assert "\ufffd" not in stored

    def test_fail_run_bounds_logs_and_leaves_the_error_message_whole(self, db_session):
        org = _org(db_session)
        svc = ExecutionService()
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        svc.start_run(db_session, org.id, run.id)
        message = "E" * (RUN_LOG_MAX_BYTES + 50) + "ERR"
        logs = "L" * (RUN_LOG_MAX_BYTES + 50) + "LOGTAIL"

        failed = svc.fail_run(db_session, org.id, run.id, error_message=message, logs=logs)

        assert failed is not None
        assert failed.error_message == message
        assert failed.logs is not None
        assert failed.logs.endswith("LOGTAIL")
        assert len(failed.logs.encode("utf-8")) <= RUN_LOG_MAX_BYTES
        _head, omitted, _tail = _split(failed.logs)
        assert omitted > 0

    def test_append_logs_bounds_the_stored_value(self, db_session):
        """A cap on each chunk still lets the column grow. The stored text is the bound."""
        org = _org(db_session)
        svc = ExecutionService()
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        svc.start_run(db_session, org.id, run.id)
        svc.complete_run(db_session, org.id, run.id, rows_loaded=1, logs="short start\n")

        svc.append_logs(db_session, org.id, run.id, "A" * RUN_LOG_MAX_BYTES + "APPEND-TAIL")

        assert run.logs is not None
        assert run.logs.startswith("short start\n")
        assert run.logs.endswith("APPEND-TAIL")
        assert len(run.logs.encode("utf-8")) <= RUN_LOG_MAX_BYTES
        _split(run.logs)

    def test_the_reaper_bounds_the_log_it_writes(self, db_session):
        org = _org(db_session)
        run = Run(
            org_id=org.id,
            target_type=NodeType.UPLOAD,
            target_id=1,
            status=RunStatus.CANCELLING,
            logs="Q" * (RUN_LOG_MAX_BYTES + 100) + "OLD-TAIL",
        )
        db_session.add(run)
        db_session.flush()
        run.updated_at = datetime.now(UTC) - timedelta(minutes=30)
        db_session.flush()

        assert reap_stuck_cancelling_runs(db_session) == 1

        db_session.refresh(run)
        assert run.logs is not None
        assert "did not acknowledge" in run.logs
        assert run.logs.endswith("destination are still there.")
        assert len(run.logs.encode("utf-8")) <= RUN_LOG_MAX_BYTES
        _split(run.logs)


class TestNoTimeBasedPurge:
    """Acceptance 3. The comment is the decision; a purge must not return beside it."""

    def test_the_retention_comment_still_holds(self):
        src = (
            Path(__file__).resolve().parents[2] / "datanika" / "services" / "maintenance_service.py"
        ).read_text(encoding="utf-8")
        assert "There is deliberately no run-retention sweep here (core#1000)." in src
        assert "def purge_old_runs" not in src
