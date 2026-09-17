"""Transformations and member-requested runs enforce their role in the SERVICE (core#681).

`SPEC_SERVICE_AUTHORIZATION` §1 records the thresholds the Reflex handlers already declare:
`save_transformation` at `editor`, `delete_transformation` at `admin`, `run_upload` and
`run_pipeline` at `editor`. §2's one rule covers the two operations that have no Reflex handler
at all -- running a transformation and cancelling a run are the ordinary lifecycle, so `editor`.

§4 puts the check in the service so every surface inherits it: the Reflex handler, the REST route
and anything added later. The scheduler is not a member and does not ask: it creates runs through
`ExecutionService.create_run`, which stays actor-free on purpose.

Every refusal below has its control beside it -- the same call by an actor who holds the role
succeeds -- because a refusal test with no success case cannot tell "the guard works" from
"nothing works" (§5 AC8).
"""

from __future__ import annotations

import pytest

from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.transformation import Materialization
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.authorization import InsufficientRoleError
from datanika.services.execution_service import ExecutionService
from datanika.services.transformation_service import TransformationService
from tests.factories import make_user


@pytest.fixture
def org(db_session):
    o = Organization(name="Acme", slug="acme-transform-run-authz")
    db_session.add(o)
    db_session.flush()
    return o


def _member(db_session, org, role: MemberRole) -> int:
    user = make_user(db_session, email=f"{role.value}-trauthz@x.io", password_hash="x")
    db_session.add(Membership(user_id=user.id, org_id=org.id, role=role))
    db_session.flush()
    return user.id


@pytest.fixture
def admin(db_session, org):
    return _member(db_session, org, MemberRole.ADMIN)


@pytest.fixture
def editor(db_session, org):
    return _member(db_session, org, MemberRole.EDITOR)


@pytest.fixture
def viewer(db_session, org):
    return _member(db_session, org, MemberRole.VIEWER)


@pytest.fixture
def transformation(db_session, org, admin):
    return TransformationService().create_transformation(
        db_session, org.id, "existing", "select 1", Materialization.VIEW, actor_user_id=admin
    )


class TestTransformations:
    def test_a_viewer_cannot_create(self, db_session, org, viewer):
        with pytest.raises(InsufficientRoleError) as exc:
            TransformationService().create_transformation(
                db_session, org.id, "m", "select 1", Materialization.VIEW, actor_user_id=viewer
            )
        assert exc.value.required_role == "editor"

    def test_an_editor_can_create(self, db_session, org, editor):
        t = TransformationService().create_transformation(
            db_session, org.id, "m", "select 1", Materialization.VIEW, actor_user_id=editor
        )
        assert t.id is not None

    def test_a_viewer_cannot_update_and_nothing_changes(
        self, db_session, org, viewer, transformation
    ):
        with pytest.raises(InsufficientRoleError) as exc:
            TransformationService().update_transformation(
                db_session, org.id, transformation.id, name="renamed", actor_user_id=viewer
            )
        assert exc.value.required_role == "editor"
        db_session.refresh(transformation)
        assert transformation.name == "existing"

    def test_an_editor_can_update(self, db_session, org, editor, transformation):
        t = TransformationService().update_transformation(
            db_session, org.id, transformation.id, name="renamed", actor_user_id=editor
        )
        assert t.name == "renamed"

    def test_an_editor_cannot_delete(self, db_session, org, editor, transformation):
        with pytest.raises(InsufficientRoleError) as exc:
            TransformationService().delete_transformation(
                db_session, org.id, transformation.id, actor_user_id=editor
            )
        assert exc.value.required_role == "admin"
        db_session.refresh(transformation)
        assert transformation.deleted_at is None

    def test_an_admin_can_delete(self, db_session, org, admin, transformation):
        assert TransformationService().delete_transformation(
            db_session, org.id, transformation.id, actor_user_id=admin
        )

    def test_another_orgs_transformation_is_not_found_before_the_role_is_checked(
        self, db_session, org, viewer, transformation
    ):
        """§7.3: after the org-scoped lookup. An id this org does not own answers `None`, never a
        refusal that confirms the row exists somewhere."""
        assert (
            TransformationService().update_transformation(
                db_session, org.id, 999_999, name="x", actor_user_id=viewer
            )
            is None
        )


class TestMemberRequestedRuns:
    @pytest.mark.parametrize(
        "target", [NodeType.UPLOAD, NodeType.PIPELINE, NodeType.TRANSFORMATION]
    )
    def test_a_viewer_cannot_request_a_run(self, db_session, org, viewer, target):
        with pytest.raises(InsufficientRoleError) as exc:
            ExecutionService().create_requested_run(
                db_session, org.id, target, 1, actor_user_id=viewer
            )
        assert exc.value.required_role == "editor"

    @pytest.mark.parametrize(
        "target", [NodeType.UPLOAD, NodeType.PIPELINE, NodeType.TRANSFORMATION]
    )
    def test_an_editor_can_request_a_run(self, db_session, org, editor, target):
        run = ExecutionService().create_requested_run(
            db_session, org.id, target, 1, actor_user_id=editor
        )
        assert run.status == RunStatus.PENDING

    def test_the_scheduler_path_stays_actor_free(self, db_session, org):
        """The scheduler is not a member. `create_run` must not start demanding one."""
        run = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, 1)
        assert run.status == RunStatus.PENDING


class TestCancellingARun:
    def test_a_viewer_cannot_cancel_and_the_run_is_untouched(self, db_session, org, viewer):
        svc = ExecutionService()
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        with pytest.raises(InsufficientRoleError) as exc:
            svc.cancel_run(db_session, org.id, run.id, actor_user_id=viewer)
        assert exc.value.required_role == "editor"
        db_session.refresh(run)
        assert run.status == RunStatus.PENDING

    def test_an_editor_can_cancel(self, db_session, org, editor):
        svc = ExecutionService()
        run = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        cancelled = svc.cancel_run(db_session, org.id, run.id, actor_user_id=editor)
        assert cancelled is not None and cancelled.status == RunStatus.CANCELLED

    def test_an_unknown_run_is_none_before_the_role_is_checked(self, db_session, org, viewer):
        assert (
            ExecutionService().cancel_run(db_session, org.id, 999_999, actor_user_id=viewer) is None
        )
