"""TDD tests for dependency_helpers — Celery retry wrapper."""

from unittest.mock import MagicMock, patch

import pytest
from celery.exceptions import MaxRetriesExceededError, Retry

from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus
from datanika.models.user import Organization
from datanika.services.dependency_check import DependencyCheckResult
from datanika.tasks.dependency_helpers import (
    DEPENDENCY_MAX_RETRIES,
    DEPENDENCY_RETRY_COUNTDOWN,
    check_deps_or_retry,
)


@pytest.fixture
def org(db_session):
    org = Organization(name="HelperOrg", slug="helper-org")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def run(db_session, org):
    r = Run(
        org_id=org.id,
        target_type=NodeType.UPLOAD,
        target_id=42,
        status=RunStatus.PENDING,
    )
    db_session.add(r)
    db_session.flush()
    return r


class TestCheckDepsOrRetry:
    @patch("datanika.tasks.dependency_helpers.check_upstream_dependencies")
    def test_satisfied_returns_normally(self, mock_check, db_session, org, run):
        """When deps are satisfied, check_deps_or_retry returns without retry."""
        mock_check.return_value = DependencyCheckResult(satisfied=True)
        mock_task = MagicMock()

        check_deps_or_retry(mock_task, run.id, org.id, NodeType.UPLOAD, session=db_session)

        mock_task.retry.assert_not_called()

    @patch("datanika.tasks.dependency_helpers.check_upstream_dependencies")
    def test_unsatisfied_triggers_retry(self, mock_check, db_session, org, run):
        """When deps are unsatisfied, task.retry is called which raises Retry."""
        mock_check.return_value = DependencyCheckResult(
            satisfied=False, unsatisfied_nodes=["upload:99"]
        )
        mock_task = MagicMock()
        mock_task.retry.side_effect = Retry()

        with pytest.raises(Retry):
            check_deps_or_retry(
                mock_task, run.id, org.id, NodeType.UPLOAD, session=db_session
            )

        mock_task.retry.assert_called_once_with(
            countdown=DEPENDENCY_RETRY_COUNTDOWN,
            max_retries=DEPENDENCY_MAX_RETRIES,
        )

    @patch("datanika.tasks.dependency_helpers.check_upstream_dependencies")
    def test_max_retries_exceeded_fails_run(self, mock_check, db_session, org, run):
        """On MaxRetriesExceededError, the run is marked FAILED and error re-raised."""
        mock_check.return_value = DependencyCheckResult(
            satisfied=False, unsatisfied_nodes=["upload:99"]
        )
        mock_task = MagicMock()
        mock_task.retry.side_effect = MaxRetriesExceededError()

        with pytest.raises(MaxRetriesExceededError):
            check_deps_or_retry(
                mock_task, run.id, org.id, NodeType.UPLOAD, session=db_session
            )

        db_session.refresh(run)
        assert run.status == RunStatus.FAILED
        assert "not satisfied" in run.error_message

    @patch("datanika.tasks.dependency_helpers.check_upstream_dependencies")
    def test_nonexistent_run_returns(self, mock_check, db_session, org):
        """If run_id doesn't exist, returns without crashing."""
        mock_task = MagicMock()
        check_deps_or_retry(mock_task, 99999, org.id, NodeType.UPLOAD, session=db_session)
        mock_check.assert_not_called()
