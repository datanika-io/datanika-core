"""Transferring an org must leave a record of who handed it to whom (core#1127).

``SettingsState.transfer_ownership`` is the highest-privilege action in the product —
handing another account the ability to remove you — and it had **never written an audit
row**. Not because the call was missing: the call was there, in the right session, with
the right payload. It passed ``"transfer_ownership"`` as the action, which is not an
``AuditAction`` member, so ``AuditAction(action)`` raised inside ``BaseState._audit`` and
the deliberate swallow dropped the row (SPEC_AUDIT_TRAIL §2.2, §6.1).

⚠️ **This file drives the handler with the REAL ``_audit``.** Every other stand-in in this
suite stubs it (``test_leave_org_session.py``'s ``_State._audit`` returns ``None``), which
is correct for what those files test and is exactly why none of them could see this. A
stub of the chokepoint cannot fail the way the chokepoint failed.

⚠️ **And the stand-in is deliberately not a bare ``MagicMock``.** A MagicMock's ``_audit``
succeeds silently, so the assertion below would pass against a handler that writes
nothing at all.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import select

import datanika.ui.state.settings_state as settings_module
from datanika.models.audit_log import AuditAction, AuditLog
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService
from datanika.ui.state.base_state import BaseState


class _TestSession:
    """``db_session`` proxy whose ``commit()`` is a ``flush()``.

    The shared fixture isolates each test in one outer transaction it rolls back at
    teardown; a real ``commit()`` releases that savepoint and leaks rows into the next
    test. Flushing keeps every behaviour the handler depends on — writes visible to later
    queries in the same transaction — while leaving the rollback able to do its job.
    Same device as ``test_account_state.py``.
    """

    def __init__(self, session):
        self._session = session

    def commit(self):
        self._session.flush()

    def __getattr__(self, name):
        return getattr(self._session, name)


class _SessionFactory:
    """Stands in for ``get_sync_session``, and **counts**.

    ⚠️ The counting is the point. A ``MagicMock`` context manager that returns the same
    session on every ``__enter__`` cannot tell one ``with get_sync_session()`` block from
    two — so the "audit in a session of its own" implementation would be invisible to it,
    which is the one structural mistake SPEC_AUDIT_TRAIL §2.1 exists to rule out.
    """

    def __init__(self, db_session):
        self._db_session = db_session
        self.handed_out: list[_TestSession] = []

    def __call__(self):
        return self

    def __enter__(self):
        session = _TestSession(self._db_session)
        self.handed_out.append(session)
        return session

    def __exit__(self, *_exc):
        return False


class _Auth:
    """Stand-in for AuthState — only what ``transfer_ownership`` may touch."""

    def __init__(self, org_id, user_id):
        self.current_org = SimpleNamespace(id=org_id)
        self.current_user = SimpleNamespace(id=user_id)
        self.role_reloaded_for = None

    def _load_current_role(self, user_id, org_id):
        self.role_reloaded_for = (user_id, org_id)


class _State:
    """Stand-in for SettingsState. ``_audit`` is the real one, on purpose."""

    #: The chokepoint under test. ⚠️ ``staticmethod(...)`` is load-bearing, not ceremony:
    #: ``BaseState._audit`` reads off the class as a **plain function**, so assigning it
    #: bare here would rebind it as an instance method and ``self`` would arrive as
    #: ``session``. That produced a real red — ``got multiple values for argument
    #: 'resource_id'`` — which looked like the defect under test and was not.
    _audit = staticmethod(BaseState._audit)

    def __init__(self, auth, service, members, email):
        self._auth = auth
        self._service = service
        self.members = members
        self.transfer_to_email = email
        self.error_message = "stale"
        self.toasted = None

    async def _check_role(self, _role):
        return True

    async def get_state(self, _cls):
        return self._auth

    def _get_user_service(self):
        return self._service

    async def load_settings(self):
        return None

    async def _saved_toast(self, key, fallback):
        self.toasted = (key, fallback)
        return self.toasted

    def _safe_error(self, exc, fallback):
        return f"{fallback}: {exc}"


@pytest.fixture
def svc():
    return UserService(AuthService("test-secret"))


@pytest.fixture
def org_with_two_members(db_session, svc):
    """An owner and an editor in one org — the minimum a transfer needs."""
    owner = svc.register_user(db_session, "owner@example.com", "correct horse", "Owner")
    successor = svc.register_user(db_session, "heir@example.com", "correct horse", "Heir")
    org = Organization(name="Transfer Org", slug=f"transfer-{owner.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=owner.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.add(Membership(user_id=successor.id, org_id=org.id, role=MemberRole.EDITOR))
    db_session.flush()
    return SimpleNamespace(org=org, owner=owner, successor=successor)


async def _transfer(db_session, fixture, svc):
    auth = _Auth(fixture.org.id, fixture.owner.id)
    members = [
        SimpleNamespace(email="owner@example.com", is_self=True, user_id=fixture.owner.id),
        SimpleNamespace(email="heir@example.com", is_self=False, user_id=fixture.successor.id),
    ]
    st = _State(auth, svc, members, "heir@example.com")
    factory = _SessionFactory(db_session)
    with patch.object(settings_module, "get_sync_session", factory):
        async for _ in settings_module.SettingsState.transfer_ownership.fn(st):
            pass
    st.sessions = factory.handed_out
    return st


def _audit_rows(db_session, org_id):
    return list(db_session.execute(select(AuditLog).where(AuditLog.org_id == org_id)).scalars())


class TestTransferOwnershipIsRecorded:
    @pytest.mark.asyncio
    async def test_a_successful_transfer_writes_exactly_one_audit_row(
        self, db_session, org_with_two_members, svc
    ):
        """core#1127. Red against the unfixed handler — 0 rows, because the action string
        was not an ``AuditAction`` member and the swallow dropped it.

        ⚠️ The row-count assertion is what makes the red attributable. *"At least one
        row"* would also be satisfied by a handler that audits twice, and *"the transfer
        happened"* passes against the defect, since the transfer always worked — it was
        only the record that was missing.
        """
        st = await _transfer(db_session, org_with_two_members, svc)
        assert st.error_message == "", f"the transfer itself failed: {st.error_message}"

        rows = _audit_rows(db_session, org_with_two_members.org.id)
        assert len(rows) == 1, (
            "transfer_ownership must leave exactly one audit row; found "
            f"{len(rows)}. Zero means BaseState._audit swallowed a ValueError from "
            "AuditAction(action) — check the action string is a member (core#1127)."
        )

    @pytest.mark.asyncio
    async def test_the_row_names_both_ends_of_the_handover(
        self, db_session, org_with_two_members, svc
    ):
        """SPEC_AUDIT_TRAIL §2.5 — *who handed this org to that account*.

        The payload was already correct before the fix and must stay correct: this is the
        half that says the one-string change did not "improve" anything on its way past.
        """
        await _transfer(db_session, org_with_two_members, svc)
        (row,) = _audit_rows(db_session, org_with_two_members.org.id)

        assert row.action is AuditAction.UPDATE
        assert row.resource_type == "member"
        assert row.resource_id == org_with_two_members.successor.id
        assert row.old_values == {"owner_user_id": org_with_two_members.owner.id}
        assert row.new_values == {"owner_user_id": org_with_two_members.successor.id}
        assert row.user_id == org_with_two_members.owner.id, (
            "the actor is the outgoing owner — the row answers 'who handed it over'"
        )

    @pytest.mark.asyncio
    async def test_the_row_and_the_role_change_ride_one_session(
        self, db_session, org_with_two_members, svc
    ):
        """SPEC_AUDIT_TRAIL §2.1, asserted structurally rather than by outcome.

        🔑 *A row-exists assertion is satisfied by the bug it is meant to name.* An
        implementation that opens its own ``get_sync_session()`` to "make sure the audit
        lands" writes a row too, and both tests above stay green against it — while
        having built exactly the defect the log exists to rule out: **an audit write
        outside the mutation's transaction is a log of things that did not happen.** The
        property is that both changes ride one ``Session``.

        ⚠️ **Not asserted the way SPEC_AUDIT_TRAIL §4.2 phrases it, and the difference is
        real.** §4.2 says to catch the pending ``AuditLog`` in ``session.new`` at commit
        time. It cannot be there: ``AuditService.log_action`` ends in ``session.add(log)``
        followed by ``session.flush()`` (``audit_service.py:178-179``), so the row is
        already persistent before the handler's ``commit()`` runs, and ``session.new`` is
        empty by then. A test written to §4.2's letter fails against **correct** code.
        Reported for core#934, which inherits that phrasing.

        What discriminates instead is session **identity**: the handler entered
        ``get_sync_session`` exactly once, and the audit row was flushed on that session.
        """
        from sqlalchemy import event

        audited_on: list[int] = []

        def sample(session, _flush_context, _instances):
            if any(isinstance(o, AuditLog) for o in session.new):
                audited_on.append(id(session))

        event.listen(db_session, "before_flush", sample)
        try:
            st = await _transfer(db_session, org_with_two_members, svc)
        finally:
            event.remove(db_session, "before_flush", sample)

        assert len(st.sessions) == 1, (
            "transfer_ownership opened "
            f"{len(st.sessions)} sessions. The audit row must ride the mutation's own "
            "transaction (SPEC_AUDIT_TRAIL §2.1); a second session makes it durable even "
            "when the transfer rolls back."
        )
        assert audited_on, (
            "no flush carried a pending AuditLog — the listener never saw one, so this "
            "assertion would pass vacuously if the audit call were deleted"
        )
        assert audited_on == [id(db_session)], (
            "the AuditLog was flushed on a session other than the one the mutation used"
        )
