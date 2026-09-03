"""Tenant-owned models other than `Connection`, resolved by primary key (#732).

`Connection` was consolidated onto one org-scoped accessor in #733 and is held
there by `test_tenant_fk_boundary.py`. The same shape remained everywhere else:
**13** bare `session.get(Run, run_id)`, plus `UploadedFile`, `OAuthGrant` and
`ApiKey`. Org scoping was a property of the *call site* rather than of the
accessor, so correctness had to be re-established by reading every caller.

**This is defence-in-depth, and the distinction was checked rather than
assumed.** The `/api/v1` run routes resolve through
`ExecutionService.get_run(session, org_id, run_id)`, which *is* scoped; the bare
lookups sat in internal mutators that tasks call with an id they had already
resolved. No user-supplied id reached one of them unscoped. What made it worth
closing is that `start_run`/`complete_run`/`fail_run`/`append_logs`/`cancel_run`
**took a `run_id` with no `org_id` at all** — so a future route calling one
directly would be unscoped *by default* rather than by mistake.

⚠️ **Against the unfixed code most of this file fails with `TypeError`, not
`AssertionError`, and that is the point rather than a defect in the test.** The
missing parameter *is* the finding: a method that cannot be told which org is
asking cannot refuse the wrong one. `test_mcp_oauth_*` and
`test_get_org_uploaded_file_*` below are the arms that go red behaviourally.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.uploaded_file import UploadedFile
from datanika.models.user import Organization
from datanika.services.execution_service import ExecutionService, get_org_run


@pytest.fixture
def two_orgs(db_session):
    a = Organization(name="Run-A", slug="run-boundary-a")
    b = Organization(name="Run-B", slug="run-boundary-b")
    db_session.add_all([a, b])
    db_session.flush()
    return a, b


@pytest.fixture
def a_run(db_session, two_orgs):
    org_a, _org_b = two_orgs
    return ExecutionService().create_run(db_session, org_a.id, NodeType.PIPELINE, 1)


class TestRunMutatorsRefuseAnotherOrgsRun:
    """Every state transition must be reachable only by the run's own org.

    Each mutator gets both arms. Without the "own org still works" half, a
    `return None` in every branch satisfies the whole class and ships a total
    outage of run execution — the same negative-control reasoning as
    `test_tenant_fk_boundary.py`.
    """

    def test_start_run_refuses(self, db_session, two_orgs, a_run):
        _org_a, org_b = two_orgs
        assert ExecutionService().start_run(db_session, org_b.id, a_run.id) is None
        db_session.refresh(a_run)
        assert a_run.status == RunStatus.PENDING, "another org moved the run to RUNNING"

    def test_start_run_still_works_for_its_own_org(self, db_session, two_orgs, a_run):
        org_a, _org_b = two_orgs
        assert ExecutionService().start_run(db_session, org_a.id, a_run.id) is not None
        db_session.refresh(a_run)
        assert a_run.status == RunStatus.RUNNING

    def test_complete_run_refuses(self, db_session, two_orgs, a_run):
        _org_a, org_b = two_orgs
        svc = ExecutionService()
        assert svc.complete_run(db_session, org_b.id, a_run.id, rows_loaded=9, logs="x") is None
        db_session.refresh(a_run)
        assert a_run.status == RunStatus.PENDING
        assert a_run.rows_loaded != 9

    def test_complete_run_still_works_for_its_own_org(self, db_session, two_orgs, a_run):
        org_a, _org_b = two_orgs
        svc = ExecutionService()
        assert svc.complete_run(db_session, org_a.id, a_run.id, rows_loaded=9, logs="x") is not None
        db_session.refresh(a_run)
        assert a_run.status == RunStatus.SUCCESS

    def test_fail_run_refuses(self, db_session, two_orgs, a_run):
        """A cross-org `fail_run` is a denial of service, not just a read.

        It also announces `run.failed`, so an unscoped one would fire another
        tenant's notification channels with our error message in it.
        """
        _org_a, org_b = two_orgs
        svc = ExecutionService()
        assert svc.fail_run(db_session, org_b.id, a_run.id, error_message="boom", logs="t") is None
        db_session.refresh(a_run)
        assert a_run.status == RunStatus.PENDING
        assert a_run.error_message is None

    def test_fail_run_still_works_for_its_own_org(self, db_session, two_orgs, a_run):
        org_a, _org_b = two_orgs
        svc = ExecutionService()
        assert (
            svc.fail_run(db_session, org_a.id, a_run.id, error_message="boom", logs="t") is not None
        )
        db_session.refresh(a_run)
        assert a_run.status == RunStatus.FAILED

    def test_append_logs_refuses(self, db_session, two_orgs, a_run):
        _org_a, org_b = two_orgs
        assert ExecutionService().append_logs(db_session, org_b.id, a_run.id, "leak") is None
        db_session.refresh(a_run)
        assert "leak" not in (a_run.logs or "")

    def test_append_logs_still_works_for_its_own_org(self, db_session, two_orgs, a_run):
        org_a, _org_b = two_orgs
        assert ExecutionService().append_logs(db_session, org_a.id, a_run.id, "note") is not None
        db_session.refresh(a_run)
        assert "note" in a_run.logs

    def test_cancel_run_refuses(self, db_session, two_orgs, a_run):
        _org_a, org_b = two_orgs
        assert ExecutionService().cancel_run(db_session, org_b.id, a_run.id) is None
        db_session.refresh(a_run)
        assert a_run.status == RunStatus.PENDING

    def test_cancel_run_still_works_for_its_own_org(self, db_session, two_orgs, a_run):
        org_a, _org_b = two_orgs
        assert ExecutionService().cancel_run(db_session, org_a.id, a_run.id) is not None
        db_session.refresh(a_run)
        assert a_run.status == RunStatus.CANCELLED


class TestGetOrgRunIsTheSingleDefinition:
    def test_resolves_the_orgs_own_run(self, db_session, two_orgs, a_run):
        org_a, _org_b = two_orgs
        assert get_org_run(db_session, org_a.id, a_run.id) is a_run

    def test_refuses_another_orgs_run(self, db_session, two_orgs, a_run):
        _org_a, org_b = two_orgs
        assert get_org_run(db_session, org_b.id, a_run.id) is None

    def test_refuses_an_id_that_does_not_exist(self, db_session, two_orgs):
        org_a, _org_b = two_orgs
        assert get_org_run(db_session, org_a.id, 10_000_000) is None


class TestRunWaiter:
    """CONTROL — `wait_for_run` already refused, by a post-hoc comparison.

    It resolved the row and *then* compared `run.org_id != org_id`, so the
    behaviour was correct while the accessor was not. Kept green across the
    change: the point of #732 is to move the property from the call site into
    the accessor without altering what callers observe.
    """

    async def test_another_orgs_run_is_not_returned(self, db_session, two_orgs, a_run, monkeypatch):
        from datanika.services import run_waiter

        monkeypatch.setattr(run_waiter, "POLL_INTERVAL", 0.01)
        monkeypatch.setattr(run_waiter, "get_sync_session", lambda: _NoCloseSession(db_session))
        assert await run_waiter.wait_for_run(a_run.id, two_orgs[1].id, timeout=1) is None

    async def test_another_orgs_terminal_run_is_not_returned_from_the_poll_loop(
        self, db_session, two_orgs, a_run, monkeypatch
    ):
        """The arm that actually pins the *loop* lookup.

        ⚠️ Written because a mutation probe caught the test above passing for
        the wrong reason. `wait_for_run` reads the run twice — once per poll and
        once after the timeout — and reverting only the poll lookup to
        `session.get(Run, run_id)` left that test **green**: a non-terminal run
        never returns from the loop, so the still-scoped timeout branch answered
        None and the mutation was invisible. A run in a terminal state returns
        from inside the loop, so this is the only arm the poll lookup can fail.
        """
        from datanika.services import run_waiter

        a_run.status = RunStatus.SUCCESS
        db_session.flush()
        monkeypatch.setattr(run_waiter, "POLL_INTERVAL", 0.01)
        monkeypatch.setattr(run_waiter, "get_sync_session", lambda: _NoCloseSession(db_session))
        assert await run_waiter.wait_for_run(a_run.id, two_orgs[1].id, timeout=1) is None

    async def test_its_own_terminal_run_is_returned(self, db_session, two_orgs, a_run, monkeypatch):
        from datanika.services import run_waiter

        a_run.status = RunStatus.SUCCESS
        db_session.flush()
        monkeypatch.setattr(run_waiter, "POLL_INTERVAL", 0.01)
        monkeypatch.setattr(run_waiter, "get_sync_session", lambda: _NoCloseSession(db_session))
        result = await run_waiter.wait_for_run(a_run.id, two_orgs[0].id, timeout=1)
        assert result is not None and result.id == a_run.id


class _NoCloseSession:
    """Hand the waiter the test's session without letting it close or expunge it."""

    def __init__(self, session):
        self._session = session

    def __enter__(self):
        return self._session

    def __exit__(self, *_exc):
        return False


def _uploaded_file(db_session, org_id: int, *, name: str, deleted: bool = False) -> UploadedFile:
    record = UploadedFile(
        org_id=org_id,
        original_name=name,
        content_type="text/csv",
        file_size=1,
        file_hash=f"hash-{name}",
        archive_path=f"/tmp/{name}.zip",
        deleted_at=datetime.now(UTC) if deleted else None,
    )
    db_session.add(record)
    db_session.flush()
    return record


class TestUploadedFileIsOrgScoped:
    def test_refuses_another_orgs_file(self, db_session, two_orgs):
        from datanika.services.file_upload_service import get_org_uploaded_file

        org_a, org_b = two_orgs
        record = _uploaded_file(db_session, org_a.id, name="a")

        assert get_org_uploaded_file(db_session, org_a.id, record.id) is record
        assert get_org_uploaded_file(db_session, org_b.id, record.id) is None

    def test_refuses_a_soft_deleted_file(self, db_session, two_orgs):
        """`run_upload` extracts the archive from disk and names a table after it.

        A soft-deleted record's archive may already have been removed by
        `cleanup_orphaned_archives`, so resolving one is a crash at best. The
        bare `session.get` this replaces did not filter `deleted_at` either.
        """
        from datanika.services.file_upload_service import get_org_uploaded_file

        org_a, _org_b = two_orgs
        record = _uploaded_file(db_session, org_a.id, name="gone", deleted=True)

        assert get_org_uploaded_file(db_session, org_a.id, record.id) is None


class TestMcpOAuthRefusesStoredCrossOrgReferences:
    """The consumer half, for the credential chain.

    The lookups keyed on a *secret* are legitimately cross-org — the token is
    what establishes which org is calling, so there is no org to scope by, and
    they are allowlisted in `test_tenant_fk_boundary.py` for exactly that
    reason. But everything the chain resolves **after** that point has an org in
    hand: `token.org_id`, then `grant.org_id`. Following those foreign keys by
    bare primary key means a stored cross-org reference hands out another
    tenant's API key — the same shape as the `Connection` defect, one layer up.

    These arms go red **behaviourally** against the unfixed code, not with a
    `TypeError`: the signatures were already adequate, only the queries were not.
    """

    @staticmethod
    def _mint(db_session, *, token_org, grant_org, key_org, access_token):
        import base64
        import hashlib

        from datanika.models.api_key import ApiKey
        from datanika.models.mcp_oauth import OAuthGrant, OAuthToken
        from datanika.services.encryption import EncryptionService
        from tests.factories import make_user

        fernet_key = base64.urlsafe_b64encode(b"0" * 32).decode()
        user = make_user(
            db_session,
            email=f"mcp-{access_token[-6:]}@example.com",
            full_name="MCP",
            password_hash="h",
        )

        key = ApiKey(org_id=key_org, user_id=user.id, name="k", key_hash=f"h{access_token[-8:]}")
        db_session.add(key)
        db_session.flush()

        grant = OAuthGrant(
            org_id=grant_org,
            client_id="mcp_testclient",
            user_id=user.id,
            api_key_id=key.id,
            encrypted_api_key=EncryptionService(fernet_key).encrypt({"key": "etf_minted"}),
            code_hash=None,
            code_challenge="c" * 43,
            redirect_uri="https://claude.ai/api/mcp/auth_callback",
            scope="mcp:read",
            resource="https://app.datanika.io/mcp",
            code_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        db_session.add(grant)
        db_session.flush()

        token = OAuthToken(
            org_id=token_org,
            grant_id=grant.id,
            access_token_hash=hashlib.sha256(access_token.encode()).hexdigest(),
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
        db_session.add(token)
        db_session.flush()
        return fernet_key

    def _svc(self, fernet_key):
        from datanika.services.mcp_oauth import McpOAuthService

        return McpOAuthService(
            public_base_url="https://app.datanika.io",
            encryption_key_provider=lambda: fernet_key,
        )

    def test_a_token_pointing_at_another_orgs_grant_resolves_to_nothing(self, db_session, two_orgs):
        org_a, org_b = two_orgs
        fernet_key = self._mint(
            db_session,
            token_org=org_a.id,
            grant_org=org_b.id,  # ← the corrupt reference
            key_org=org_b.id,
            access_token="dtk_at_crossgrant",
        )
        assert self._svc(fernet_key).resolve_access_token(db_session, "dtk_at_crossgrant") is None

    def test_a_grant_pointing_at_another_orgs_api_key_resolves_to_nothing(
        self, db_session, two_orgs
    ):
        org_a, org_b = two_orgs
        fernet_key = self._mint(
            db_session,
            token_org=org_a.id,
            grant_org=org_a.id,
            key_org=org_b.id,  # ← the corrupt reference
            access_token="dtk_at_crosskey",
        )
        assert self._svc(fernet_key).resolve_access_token(db_session, "dtk_at_crosskey") is None

    def test_a_consistent_chain_still_resolves(self, db_session, two_orgs):
        """NEGATIVE CONTROL — without this, refusing everything passes above."""
        org_a, _org_b = two_orgs
        fernet_key = self._mint(
            db_session,
            token_org=org_a.id,
            grant_org=org_a.id,
            key_org=org_a.id,
            access_token="dtk_at_consistent",
        )
        resolved = self._svc(fernet_key).resolve_access_token(db_session, "dtk_at_consistent")
        assert resolved is not None
        assert resolved.api_key == "etf_minted"


class TestExecutionServiceGetRunUnchanged:
    """CONTROL — the already-scoped reader keeps behaving as it did."""

    def test_get_run_still_resolves_own(self, db_session, two_orgs, a_run):
        org_a, _org_b = two_orgs
        assert ExecutionService().get_run(db_session, org_a.id, a_run.id) is a_run

    def test_get_run_still_refuses_other(self, db_session, two_orgs, a_run):
        _org_a, org_b = two_orgs
        assert ExecutionService().get_run(db_session, org_b.id, a_run.id) is None
