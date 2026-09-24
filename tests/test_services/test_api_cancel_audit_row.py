"""`POST /api/v1/runs/{id}/cancel` owes the record what its UI twin already writes.

``SPEC_AUDIT_TRAIL`` §8, ruled 2026-09-23. [core#1533].

**This is a consistency defect, not a collection one.** Both doors call the same
``ExecutionService.cancel_run``; the UI handler audits and the route did not. So the same action,
on the same run, in the same org, was recorded or not **according to which door it came through** —
and §1's formulation is why that is worse than recording neither: *an absent log is not consulted,
a lying one is believed.* An empty ``audit_logs`` prompts *"do we even log this?"*; a table holding
**some** cancels does not, so an admin asking *"who stopped run 42?"* about an API cancel gets a
well-formed, confident silence and reads it as *"nobody did."*

🚨 **Both doors drive the same ``db_session`` here, deliberately.** AC4 asks whether the two rows
are indistinguishable, and the only honest way to answer that is to write both and compare them —
not to write one and reconstruct what the other *would* have said, which asserts my reading of the
UI handler rather than its behaviour.

⚠️ **A test asserting only that a row exists is satisfied by the §8.5 bug**, so every assertion
below is on contents.
"""

from __future__ import annotations

import contextlib
import uuid
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select
from starlette.applications import Starlette
from starlette.testclient import TestClient

import datanika.models.invitation  # noqa: F401  — Base.metadata must know every table
import datanika.models.notification_channel  # noqa: F401
import datanika.models.sso_config  # noqa: F401
import datanika.ui.state.auth_state as auth_state_module
import datanika.ui.state.run_state as run_state_module
from datanika.models.audit_log import AuditAction, AuditLog
from datanika.models.run import Run, RunStatus
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.rate_limit_service import RateLimitResult
from datanika.ui.state.base_state import BaseState
from datanika.ui.state.run_state import RunState
from tests.factories import make_user
from tests.test_ui.test_models_empty_state_after_a_load import _session_patch
from tests.test_ui.test_runs_cancel_control import (
    SECRET,
    _auth_stand_in,
    _press_stop,
    _run,
)

#: Deliberately shaped like personal data. `redact_pii_payload` is **nominal** — it matches key
#: *names*, so anything arriving under `api_key_name` is invisible to it (§2.4). If the row ever
#: carried the key's name, this value is what would land in the table unredacted.
KEY_NAME = "ci-bot alice@example.com"


@pytest.fixture
def db_session():
    """One session both doors write to — and it must survive being used from another thread.

    ⚠️ **Not `conftest`'s `db_session`.** Starlette's `TestClient` runs the app in a *different
    thread*, and conftest's SQLite connection is thread-bound, so the route raised
    ``SQLite objects created in a thread can only be used in that same thread`` before it reached
    any audit code. `StaticPool` + ``check_same_thread=False`` is the same combination
    `test_api_v1_routes.py` uses, kept here so **both** doors share one table — reconstructing
    the UI row instead would assert my reading of that handler rather than its behaviour.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession
    from sqlalchemy.pool import StaticPool

    from datanika.models.base import Base

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session = SASession(engine)
    yield session
    session.close()
    engine.dispose()


@pytest.fixture
def org_and_actor(db_session):
    org = Organization(name="Acme", slug=f"acme-api-cancel-{uuid.uuid4().hex[:8]}")
    db_session.add(org)
    db_session.flush()
    user = make_user(db_session, email=f"api-cancel-{org.id}@test.io", password_hash="x")
    db_session.add(Membership(user_id=user.id, org_id=org.id, role=MemberRole.EDITOR))
    db_session.flush()
    return org.id, user.id


@pytest.fixture
def api_key(org_and_actor):
    org_id, user_id = org_and_actor
    key = MagicMock()
    key.id = 4242
    key.org_id = org_id
    key.user_id = user_id
    key.name = KEY_NAME
    key.scopes = None
    return key


@pytest.fixture
def api_cancel(db_session, api_key):
    """Drive the real route, on the test's own session, through the real middleware."""
    client = TestClient(Starlette(routes=api_v1_routes))
    ok = RateLimitResult(
        allowed=True, current_count=1, limit=60, remaining=59, retry_after=0, reset_at=9999999999
    )

    @contextlib.contextmanager
    def _patched():
        @contextlib.contextmanager
        def fake_session():
            yield db_session

        with (
            patch("datanika.services.api_middleware._api_key_svc") as svc,
            patch("datanika.services.api_middleware._rate_limit_svc") as rl,
            patch("datanika.services.api_middleware._get_session", fake_session),
        ):
            svc.authenticate_api_key.return_value = api_key
            rl.get_limit_for_org.return_value = 60
            rl.check_rate_limit.return_value = ok
            yield

    def cancel(run_id: int):
        with _patched():
            return client.post(
                f"/api/v1/runs/{run_id}/cancel", headers={"Authorization": "Bearer etf_test"}
            )

    return cancel


@pytest.fixture
def ui_cancel(db_session, monkeypatch, org_and_actor):
    """The UI door, built the way `test_runs_cancel_control` builds it — one source of truth."""
    org_id, user_id = org_and_actor

    async def _get_state(self, state_cls):
        return auth

    auth = _auth_stand_in("editor", org_id, user_id)
    monkeypatch.setattr(RunState, "get_state", _get_state)
    monkeypatch.setattr(auth_state_module.settings, "secret_key", SECRET)
    monkeypatch.setattr(run_state_module, "get_sync_session", lambda: _session_patch(db_session))
    monkeypatch.setattr(run_state_module, "EncryptionService", MagicMock())
    state = RunState(parent_state=BaseState(init_substates=False), init_substates=False)

    async def press(run_id: int):
        return await _press_stop(state, run_id)

    return press


def _rows(session, org_id: int) -> list[AuditLog]:
    return list(
        session.execute(
            select(AuditLog).where(AuditLog.org_id == org_id, AuditLog.resource_type == "run")
        )
        .scalars()
        .all()
    )


# ---------------------------------------------------------------------------------------------


class TestTheHarnessDrivesTheRealThing:
    """If the door does not actually cancel, every audit assertion below is vacuous."""

    def test_the_api_door_cancels(self, db_session, org_and_actor, api_cancel):
        org_id, _ = org_and_actor
        run = _run(db_session, org_id, RunStatus.RUNNING)
        assert api_cancel(run.id).status_code == 200
        db_session.expire(run)
        assert db_session.get(Run, run.id).status == RunStatus.CANCELLING

    @pytest.mark.asyncio
    async def test_the_ui_door_cancels(self, db_session, org_and_actor, ui_cancel):
        org_id, _ = org_and_actor
        run = _run(db_session, org_id, RunStatus.RUNNING)
        await ui_cancel(run.id)
        db_session.expire(run)
        assert db_session.get(Run, run.id).status == RunStatus.CANCELLING


class TestAnApiCancelIsRecorded:
    """AC1 + §8.4."""

    def test_it_writes_exactly_one_row_in_the_specified_shape(
        self, db_session, org_and_actor, api_cancel, api_key
    ):
        org_id, user_id = org_and_actor
        run = _run(db_session, org_id, RunStatus.RUNNING)

        assert api_cancel(run.id).status_code == 200

        rows = _rows(db_session, org_id)
        assert len(rows) == 1, f"expected exactly one row, got {len(rows)}"
        row = rows[0]
        assert row.action is AuditAction.UPDATE, "§8.4: `update`, never a new `cancel` member"
        assert row.resource_type == "run"
        assert row.resource_id == run.id
        assert row.user_id == user_id, "the key acts on behalf of its owner"

    def test_the_row_records_the_transition_that_actually_happened(
        self, db_session, org_and_actor, api_cancel
    ):
        """🚨 §8.5, and the assertion that discriminates.

        The route holds `run` from its own pre-flight `get_run`. Under one session and one
        identity map that is very likely the **same object** the service mutates, so reading
        `run.status` *after* the call yields the NEW status and the row records
        ``old == new`` — a transition that never happened, filed under the name of somebody
        who did something else. **A row-exists assertion passes against that bug.**
        """
        org_id, _ = org_and_actor
        run = _run(db_session, org_id, RunStatus.RUNNING)

        api_cancel(run.id)

        row = _rows(db_session, org_id)[0]
        assert row.old_values["status"] != row.new_values["status"], (
            "old == new: the before-status was read AFTER the service call (§8.5)"
        )
        assert row.old_values == {"status": "running"}
        assert row.new_values["status"] == "cancelling"

    def test_a_pending_run_records_its_own_transition_not_a_hardcoded_pair(
        self, db_session, org_and_actor, api_cancel
    ):
        """A second status pair, so the test above cannot be satisfied by two literals.

        A `pending` run is cancellable and goes straight to `cancelled`, not `cancelling`.
        """
        org_id, _ = org_and_actor
        run = _run(db_session, org_id, RunStatus.PENDING)

        api_cancel(run.id)

        row = _rows(db_session, org_id)[0]
        assert row.old_values == {"status": "pending"}
        assert row.new_values["status"] == "cancelled"


class TestWhichKeyActed:
    """AC3 + §8.6. `user_id` alone answers 'Alice' when the useful answer is 'Alice's CI bot'."""

    def test_the_row_carries_the_key_id(self, db_session, org_and_actor, api_cancel, api_key):
        org_id, _ = org_and_actor
        run = _run(db_session, org_id, RunStatus.RUNNING)

        api_cancel(run.id)

        assert _rows(db_session, org_id)[0].new_values["api_key_id"] == api_key.id

    def test_the_row_does_not_carry_the_key_name(self, db_session, org_and_actor, api_cancel):
        """⛔ §8.6. A key name is user-chosen free text and can contain anything, including an
        email address — and `redact_pii_payload` is **nominal**, so personal data arriving under
        `api_key_name` is invisible to it. That would be the *"never put personal data under a
        non-PII key name"* failure made by the spec rather than by a careless call site."""
        org_id, _ = org_and_actor
        run = _run(db_session, org_id, RunStatus.RUNNING)

        api_cancel(run.id)

        row = _rows(db_session, org_id)[0]
        blob = f"{row.old_values}{row.new_values}"
        assert KEY_NAME not in blob
        assert "alice@example.com" not in blob, "an email reached the payload unredacted"


class TestTheTwoDoorsTellOneStory:
    """AC4. The whole point of §8.2's invariant."""

    @pytest.mark.asyncio
    async def test_a_ui_cancel_and_an_api_cancel_differ_only_by_the_key_id(
        self, db_session, org_and_actor, api_cancel, ui_cancel, api_key
    ):
        org_id, user_id = org_and_actor
        via_ui = _run(db_session, org_id, RunStatus.RUNNING)
        via_api = _run(db_session, org_id, RunStatus.RUNNING)

        await ui_cancel(via_ui.id)
        api_cancel(via_api.id)

        rows = {r.resource_id: r for r in _rows(db_session, org_id)}
        assert set(rows) == {via_ui.id, via_api.id}, "one row per door, no more and no fewer"
        ui_row, api_row = rows[via_ui.id], rows[via_api.id]

        assert (ui_row.action, ui_row.resource_type, ui_row.user_id) == (
            api_row.action,
            api_row.resource_type,
            api_row.user_id,
        )
        assert ui_row.old_values == api_row.old_values

        # The ONLY permitted difference, and its presence is itself the signal for which door
        # was used (§8.6). The UI row carries no `api_key_id` deliberately — writing
        # `api_key_id: null` there would assert a key was involved and was empty.
        assert "api_key_id" not in ui_row.new_values
        assert api_row.new_values.pop("api_key_id") == api_key.id
        assert ui_row.new_values == api_row.new_values, (
            "the two doors disagree on something other than the acting key"
        )


class TestNothingIsRecordedThatDidNotHappen:
    """AC2 and the refusal controls. §8.4's condition, and §1's worse failure mode."""

    def test_a_second_cancel_on_a_stopping_run_writes_no_second_row(
        self, db_session, org_and_actor, api_cancel
    ):
        """`CANCELLING` is itself in `CANCELLABLE_RUN_STATUSES`, so the route's own guard lets a
        second stop through and the service returns the run unchanged. An unconditional write
        files a `cancelling -> cancelling` row asserting something that never happened."""
        org_id, _ = org_and_actor
        run = _run(db_session, org_id, RunStatus.RUNNING)

        assert api_cancel(run.id).status_code == 200
        second = api_cancel(run.id)

        assert second.status_code == 200, "the second stop is idempotent, not an error"
        assert len(_rows(db_session, org_id)) == 1

    def test_a_terminal_run_is_refused_and_writes_nothing(
        self, db_session, org_and_actor, api_cancel
    ):
        org_id, _ = org_and_actor
        run = _run(db_session, org_id, RunStatus.SUCCESS)

        assert api_cancel(run.id).status_code == 409

        assert not _rows(db_session, org_id)

    def test_a_missing_run_writes_nothing(self, db_session, org_and_actor, api_cancel):
        org_id, _ = org_and_actor

        assert api_cancel(999_999).status_code == 404

        assert not _rows(db_session, org_id)

    def test_another_orgs_run_is_neither_cancelled_nor_audited(
        self, db_session, org_and_actor, api_cancel
    ):
        org_id, _ = org_and_actor
        other = Organization(name="Other", slug=f"other-api-cancel-{uuid.uuid4().hex[:8]}")
        db_session.add(other)
        db_session.flush()
        foreign = _run(db_session, other.id, RunStatus.RUNNING)

        assert api_cancel(foreign.id).status_code == 404

        db_session.expire(foreign)
        assert db_session.get(Run, foreign.id).status == RunStatus.RUNNING
        assert not _rows(db_session, org_id)
        assert not _rows(db_session, other.id)
