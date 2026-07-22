"""A failed run must notify (core#465).

Both run-completion handlers already branch on `status == "failed"` — building
`Run #N failed` / `NotificationType.RUN_FAILED` and choosing the `run_failure`
notification event. **Nothing ever sent that status.** All three announce sites
were on success paths and the `fail_run` paths announced nothing at all, so the
branch was unreachable and a failed run had never produced a notification:
Slack, email or in-app.

Two decisions worth keeping visible, because neither is what the issue proposed:

**Announced from `ExecutionService.fail_run`, not from each task.** The issue
scoped this as "three `except` paths"; there are **five** `fail_run` call sites,
and two are not `except` blocks — a dbt command failing
(`pipeline_tasks`) and upstream dependencies never being satisfied
(`dependency_helpers`). Announcing from the one place they all pass through
covers those, leaves the error-handling paths untouched (which is what the
issue's own warning asked for), and cannot be forgotten by a sixth caller.

**A new `run.failed` event, not `run.*_completed` with `status="failed"`.**
datanika-cloud subscribes four metering handlers to those three events —
`handle_model_runs`, `handle_upload_runs`, `handle_transformation_run`,
`handle_bytes_processed` — and every one calls `record_usage` **unconditionally**;
not one checks `status`. Reusing those events would have billed users for failed
runs, turning a missing notification into a billing defect. That is asserted
below, because it is a cross-repo invariant no core test would otherwise hold.
"""

import pytest

from datanika import hooks
from datanika.models.dependency import NodeType
from datanika.models.notification import NotificationType
from datanika.models.run import Run, RunStatus
from datanika.models.user import Organization
from datanika.services.execution_service import ExecutionService
from datanika.services.in_app_notification_service import InAppNotificationService


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-run-failed")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def run(db_session, org):
    run = Run(
        org_id=org.id,
        target_type=NodeType.UPLOAD,
        target_id=7,
        status=RunStatus.RUNNING,
    )
    db_session.add(run)
    db_session.flush()
    return run


@pytest.fixture
def clean_hooks():
    """Isolate the global bus — it is process-wide state."""
    saved = {event: list(handlers) for event, handlers in hooks._handlers.items()}
    hooks._handlers.clear()
    yield hooks
    hooks._handlers.clear()
    hooks._handlers.update(saved)


class TestFailedRunsAnnounce:
    def test_fail_run_announces_run_failed(self, db_session, run, clean_hooks):
        seen = []
        clean_hooks.on("run.failed", lambda **kw: seen.append(kw))

        ExecutionService().fail_run(db_session, run.id, error_message="boom", logs="trace")

        assert len(seen) == 1, "a failed run announced nothing"
        payload = seen[0]
        assert payload["status"] == "failed"
        assert payload["error_message"] == "boom"
        assert payload["run_id"] == run.id
        assert payload["org_id"] == run.org_id

    def test_the_payload_identifies_which_run_failed(self, db_session, run, clean_hooks):
        """`Run #N failed` is the notification title — without these it says nothing."""
        seen = []
        clean_hooks.on("run.failed", lambda **kw: seen.append(kw))

        ExecutionService().fail_run(db_session, run.id, error_message="boom", logs="t")

        payload = seen[0]
        assert payload["target_type"] == NodeType.UPLOAD.value
        assert payload["target_id"] == 7
        assert isinstance(payload["target_type"], str), "enum would not serialise for Celery"

    def test_a_missing_run_announces_nothing(self, db_session, clean_hooks):
        seen = []
        clean_hooks.on("run.failed", lambda **kw: seen.append(kw))

        assert ExecutionService().fail_run(db_session, 999999, error_message="x", logs="y") is None
        assert seen == []

    def test_a_broken_notifier_cannot_mask_the_failure(self, db_session, run, clean_hooks):
        """`announce`, not `emit` — the run already failed (core#456)."""

        def explode(**_kw):
            raise RuntimeError("notifier down")

        clean_hooks.on("run.failed", explode)

        result = ExecutionService().fail_run(db_session, run.id, error_message="boom", logs="t")

        assert result is not None
        assert result.status == RunStatus.FAILED
        assert result.error_message == "boom"


class TestTheNotificationActuallyArrives:
    def test_a_failed_run_creates_an_in_app_notification(self, db_session, run, clean_hooks):
        """The assertion that would have caught this.

        Everything above proves the event fires; this proves a user sees
        something, through the real handler and the real service.
        """
        from datanika.services.in_app_notification_hooks import register_in_app_notification_hooks

        register_in_app_notification_hooks()

        ExecutionService().fail_run(
            db_session, run.id, error_message="dbt command failed", logs="t"
        )

        # The hook creates an org-wide notification (no user_id), which
        # `list_for_user` returns to every member of the org.
        notifications = InAppNotificationService.list_for_user(db_session, run.org_id, user_id=1)
        assert len(notifications) == 1, "a failed run produced no notification"
        assert notifications[0].type == NotificationType.RUN_FAILED
        assert f"Run #{run.id}" in notifications[0].title
        assert notifications[0].message == "dbt command failed"


class TestFailuresAreNotMetered:
    def test_run_failed_is_not_one_of_the_metered_events(self):
        """Cross-repo invariant, asserted here because nothing else holds it.

        datanika-cloud subscribes `handle_model_runs`, `handle_upload_runs`,
        `handle_transformation_run` and `handle_bytes_processed` to the three
        `run.*_completed` events, and every one records usage **without
        checking status**. If a future change routes failures onto those
        events, users get billed for failed runs.
        """
        metered = {
            "run.upload_completed",
            "run.models_completed",
            "run.transformation_completed",
        }
        assert "run.failed" not in metered

    def test_fail_run_does_not_announce_a_metered_event(self, db_session, run, clean_hooks):
        """The behavioural half — a rename could quietly undo the one above."""
        metered_hits = []
        for event in (
            "run.upload_completed",
            "run.models_completed",
            "run.transformation_completed",
        ):
            clean_hooks.on(event, lambda _event=event, **kw: metered_hits.append(_event))

        ExecutionService().fail_run(db_session, run.id, error_message="boom", logs="t")

        assert metered_hits == [], (
            f"a failed run fired metered events {metered_hits} — cloud's handlers "
            "record usage unconditionally, so this bills users for failures"
        )
