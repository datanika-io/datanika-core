"""A failed audit write must be swallowed *loudly* (core#723).

Enabling ruff's flake8-bandit rules surfaced seven `try`/`except`/`pass` blocks
in application code, four of them on the session path
(`auth_state.py` login, invitation acceptance and logout; `base_state.py`
`_audit`). Every one of them is a **deliberate** swallow with a correct reason —
audit logging must not break a login — and every one of them was also silent.

The silence is the defect, and it is a security defect rather than an untidiness:

* `AuditAction` is a `StrEnum` column and `_audit` takes `action: str`, so
  `AuditAction(action)` raises `ValueError` on any typo or renamed action. That
  raise is indistinguishable, from outside, from an audit entry that was
  written.
* The write shares the caller's `Session`. A failure here means the audit row is
  absent while the operation it describes succeeded and committed.
* Nothing else observes it. There is no counter, no metric and no alert on audit
  volume, so an audit trail that stops recording keeps reading as an audit trail
  with nothing to record — and the two are only distinguishable at the moment
  someone needs it, which is after an incident.

This is the same family as every other finding in this project's history where a
signal could not have looked different had the thing failed. The fix is not to
stop swallowing; it is to leave evidence.
"""

import logging

import pytest

from datanika.ui.state.base_state import BaseState

_AUDIT_LOGGER = "datanika.ui.state.base_state"


@pytest.fixture
def exploding_audit(monkeypatch):
    """Make the audit write fail the way a renamed action actually would."""
    from datanika.services.audit_service import AuditService

    def boom(*_args, **_kwargs):
        raise ValueError("'not_a_real_action' is not a valid AuditAction")

    monkeypatch.setattr(AuditService, "log_action", boom)


class TestAFailedAuditWriteLeavesEvidence:
    def test_the_failure_is_logged(self, exploding_audit, caplog):
        with caplog.at_level(logging.WARNING, logger=_AUDIT_LOGGER):
            BaseState._audit(
                session=object(),
                org_id=1,
                user_id=2,
                action="create",
                resource_type="connection",
                resource_id=3,
            )

        records = [r for r in caplog.records if r.name == _AUDIT_LOGGER]
        assert records, (
            "A failed audit write produced no log record at all (core#723).\n"
            "Swallowing the exception is correct — audit logging must never break "
            "the operation it describes. Swallowing it SILENTLY is not: the audit "
            "trail then stops recording with no signal anywhere, and 'no audit "
            "rows' is indistinguishable from 'nothing happened'."
        )

    def test_the_log_carries_the_original_exception(self, exploding_audit, caplog):
        """A message with no traceback cannot be acted on."""
        with caplog.at_level(logging.WARNING, logger=_AUDIT_LOGGER):
            BaseState._audit(
                session=object(),
                org_id=1,
                user_id=2,
                action="create",
                resource_type="connection",
            )

        records = [r for r in caplog.records if r.name == _AUDIT_LOGGER]
        assert records, "no log record (see the previous test)"
        assert any(r.exc_info for r in records), (
            "The audit failure was logged without exception info. Use "
            "`logger.exception(...)` (or `exc_info=True`) — the whole point is "
            "to be able to tell a renamed AuditAction from a dead database "
            "session, and the message alone cannot."
        )

    def test_the_caller_is_not_broken_by_the_failure(self, exploding_audit, caplog):
        """The swallow itself is load-bearing — do not 'fix' it by re-raising.

        Written as its own assertion so that a future change which turns the
        swallow into a propagation fails here rather than in production, where
        it would present as logins breaking whenever the audit table is
        unreachable.
        """
        with caplog.at_level(logging.WARNING, logger=_AUDIT_LOGGER):
            BaseState._audit(
                session=object(),
                org_id=1,
                user_id=2,
                action="login",
                resource_type="session",
            )
        # Reaching this line at all is the assertion: _audit returned normally.
