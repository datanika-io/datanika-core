"""The org permission model of record — `plans/product/SPEC_ORG_ROLES.md`.

`test_privilege_escalation.py` (QA) proves the **defect** core#658 reports and
the two paths that reach it. This file covers what that harness does not, and
each section exists because a fix could pass QA's file and still be wrong:

* **Section 1 — the ceiling and the reach as rules**, not as the two cases that
  happened to be reported. R2 (you may not grant at or above your own role) and
  R3 (you may only act on members strictly below you), including the R4
  owner-on-owner exception that keeps two co-founders able to separate.
* **Section 2 — transfer ownership** (§3). R1 makes `owner` ungrantable from
  every role-assignment control, so without a dedicated operation the fix would
  have *removed* a legitimate capability. Owner count is asserted **inside** the
  transaction, not only after it.
* **Section 3 — leave** (R6, audit P8). No `leave_*` handler existed anywhere,
  so a viewer was in an org until somebody else removed them.
* **Section 4 — the owner-count invariant** (AC9), asserted through one shared
  helper called from every mutating test rather than nine hand-written copies.
* **Section 5 — `owner` is unreachable outside the two allowlisted paths**
  (AC13), derived from the AST rather than from a hardcoded count.

⚠️ **Why these are not shown red against unfixed `dev`.** Sections 2 and 3 test
operations that did not exist, so their "red" would be `AttributeError` — a
broken-harness red, which the audit doc rules out as evidence. Sections 1, 4
and 5 are green against a *plausible wrong fix* as often as against the right
one. So the artifact for this file is the **mutation run** in the PR body:
every claim below was verified by re-breaking the shipped rule and watching a
named test go red. A test that has never failed has never been shown to be able
to.
"""

import ast
import pathlib

import pytest

from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.services.auth import (
    AuthService,
    assignable_roles,
    may_grant_role,
    may_manage_member,
)
from datanika.services.invitation_service import InvitationService
from datanika.services.user_service import UserService, UserServiceError


@pytest.fixture
def auth() -> AuthService:
    return AuthService(secret_key="test-secret-key-for-role-authority")


@pytest.fixture
def svc(auth) -> UserService:
    return UserService(auth)


@pytest.fixture
def org(db_session) -> Organization:
    o = Organization(name="Roles Co", slug="roles-co-658")
    db_session.add(o)
    db_session.flush()
    return o


def _member(db_session, org, email, role) -> Membership:
    u = User(email=email, password_hash="h", full_name=email.split("@")[0])
    db_session.add(u)
    db_session.flush()
    m = Membership(user_id=u.id, org_id=org.id, role=role)
    db_session.add(m)
    db_session.flush()
    return m


@pytest.fixture
def owner(db_session, org) -> Membership:
    return _member(db_session, org, "owner@roles.test", MemberRole.OWNER)


@pytest.fixture
def admin(db_session, org) -> Membership:
    return _member(db_session, org, "admin@roles.test", MemberRole.ADMIN)


@pytest.fixture
def editor(db_session, org) -> Membership:
    return _member(db_session, org, "editor@roles.test", MemberRole.EDITOR)


@pytest.fixture
def viewer(db_session, org) -> Membership:
    return _member(db_session, org, "viewer@roles.test", MemberRole.VIEWER)


def _owner_count(db_session, org_id: int) -> int:
    """AC9's shared helper. Called after every mutating test in this file.

    Hand-copying this assertion is how [core#651]'s two lists diverged; there
    is one of it, and every test that changes a membership calls it.
    """
    return (
        db_session.query(Membership)
        .filter(
            Membership.org_id == org_id,
            Membership.role == MemberRole.OWNER,
            Membership.deleted_at.is_(None),
        )
        .count()
    )


def _live(db_session, membership_id: int) -> Membership | None:
    return (
        db_session.query(Membership)
        .filter(Membership.id == membership_id, Membership.deleted_at.is_(None))
        .one_or_none()
    )


# ---------------------------------------------------------------------------
# Section 1 — the ceiling (R2) and the reach (R3/R4) as rules
# ---------------------------------------------------------------------------


class TestTheCeilingAndTheReach:
    def test_owner_is_grantable_by_nobody(self):
        """R1, stated at the predicate rather than at one call site.

        Including by an owner: ownership moves through `transfer_ownership`
        alone. That is what makes R1 safe to assert absolutely — a later bug in
        the role-change path cannot reach ownership, because ownership is not
        on that control any more.
        """
        for role in ("owner", "admin", "editor", "viewer"):
            assert may_grant_role(role, "owner") is False

    def test_an_admin_may_not_grant_admin(self):
        """R2. `admin` carries `delete` on every resource in the org, so who
        may destroy a customer's pipelines is not a self-replicating grant."""
        assert may_grant_role("admin", "admin") is False
        assert may_grant_role("admin", "editor") is True
        assert may_grant_role("admin", "viewer") is True

    def test_an_owner_may_grant_admin(self):
        assert may_grant_role("owner", "admin") is True

    def test_editors_and_viewers_grant_nothing(self):
        assert assignable_roles("editor") == []
        assert assignable_roles("viewer") == []

    def test_the_selects_offer_exactly_what_may_be_granted(self):
        """The settings page derives its options from this, so a control the
        server would refuse cannot be rendered (core#658 AC4)."""
        assert assignable_roles("owner") == ["viewer", "editor", "admin"]
        assert assignable_roles("admin") == ["viewer", "editor"]

    def test_an_admin_reaches_below_but_not_across(self):
        """R3. Not `<=` — an admin cannot touch a peer admin."""
        assert may_manage_member("admin", "viewer") is True
        assert may_manage_member("admin", "editor") is True
        assert may_manage_member("admin", "admin") is False
        assert may_manage_member("admin", "owner") is False

    def test_an_owner_reaches_another_owner(self):
        """R4, the one peer exception. Two co-founders separating must not
        need us. Safe because R1+R2 mean a second owner exists only where an
        owner deliberately created one."""
        assert may_manage_member("owner", "owner") is True

    def test_an_editor_manages_nobody(self):
        assert may_manage_member("editor", "viewer") is False


class TestTheServiceRefusesWithoutAnActor:
    """The service was unauthenticated, not merely admin-permissive.

    Its safety rested entirely on one `_check_role("admin")` in the Reflex
    state layer. A default that *allowed* a missing actor would leave the same
    hole open for the next caller while looking fixed.
    """

    @pytest.mark.parametrize("role", [MemberRole.VIEWER, MemberRole.ADMIN])
    def test_add_member_without_an_actor_is_refused(self, svc, db_session, org, owner, role):
        stranger = User(email="s@roles.test", password_hash="h", full_name="S")
        db_session.add(stranger)
        db_session.flush()
        with pytest.raises(UserServiceError, match="acting user"):
            svc.add_member(db_session, org.id, stranger.id, role)

    def test_change_role_without_an_actor_is_refused(self, svc, db_session, org, owner, viewer):
        with pytest.raises(UserServiceError, match="acting user"):
            svc.change_role(db_session, org.id, viewer.id, MemberRole.EDITOR)
        assert _live(db_session, viewer.id).role is MemberRole.VIEWER

    def test_remove_member_without_an_actor_is_refused(self, svc, db_session, org, owner, viewer):
        with pytest.raises(UserServiceError, match="acting user"):
            svc.remove_member(db_session, org.id, viewer.id)
        assert _live(db_session, viewer.id) is not None

    def test_a_non_member_cannot_manage_members(self, svc, db_session, org, owner, viewer):
        outsider = User(email="outsider@roles.test", password_hash="h", full_name="O")
        db_session.add(outsider)
        db_session.flush()
        with pytest.raises(UserServiceError, match="not a member"):
            svc.remove_member(db_session, org.id, viewer.id, actor_user_id=outsider.id)
        assert _live(db_session, viewer.id) is not None


class TestTheNegativeControl:
    """AC5. A fix that denies everything passes every escalation test.

    These are the operations that must keep working, and they are the reason
    the ceiling is `>=` on the granted role rather than a blanket refusal.
    """

    def test_an_admin_can_still_promote_a_viewer_to_editor(
        self, svc, db_session, org, owner, admin, viewer
    ):
        svc.change_role(
            db_session, org.id, viewer.id, MemberRole.EDITOR, actor_user_id=admin.user_id
        )
        assert _live(db_session, viewer.id).role is MemberRole.EDITOR
        assert _owner_count(db_session, org.id) == 1

    def test_an_admin_can_still_remove_an_editor(self, svc, db_session, org, owner, admin, editor):
        assert svc.remove_member(db_session, org.id, editor.id, actor_user_id=admin.user_id) is True
        assert _owner_count(db_session, org.id) == 1

    def test_an_admin_can_still_invite_an_editor(self, svc, db_session, org, owner, admin):
        newcomer = User(email="new@roles.test", password_hash="h", full_name="N")
        db_session.add(newcomer)
        db_session.flush()
        m = svc.add_member(
            db_session, org.id, newcomer.id, MemberRole.EDITOR, actor_user_id=admin.user_id
        )
        assert m.role is MemberRole.EDITOR
        assert _owner_count(db_session, org.id) == 1

    def test_an_owner_can_still_promote_someone_to_admin(self, svc, db_session, org, owner, editor):
        svc.change_role(
            db_session, org.id, editor.id, MemberRole.ADMIN, actor_user_id=owner.user_id
        )
        assert _live(db_session, editor.id).role is MemberRole.ADMIN
        assert _owner_count(db_session, org.id) == 1


# ---------------------------------------------------------------------------
# Section 2 — transfer ownership (SPEC_ORG_ROLES §3, AC10-12)
# ---------------------------------------------------------------------------


class TestTransferOwnership:
    def test_the_successor_becomes_owner_and_the_actor_becomes_admin(
        self, svc, db_session, org, owner, editor
    ):
        svc.transfer_ownership(db_session, org.id, editor.user_id, actor_user_id=owner.user_id)
        assert _live(db_session, editor.id).role is MemberRole.OWNER
        assert _live(db_session, owner.id).role is MemberRole.ADMIN
        assert _owner_count(db_session, org.id) == 1

    def test_the_owner_count_never_dips(self, svc, db_session, org, owner, editor):
        """AC10 — asserted *inside* the transaction, not only after it.

        ⚠️ **The obvious version of this test cannot fail.** Reading the count
        twice *after* `transfer_ownership` returns sees the finished state both
        times, so a demote-then-promote implementation — which transiently
        leaves the org with zero owners, the exact state this spec exists to
        eliminate — passes it. Not hypothetical: that mutation survived the
        first draft of this test untouched, and only the mutation run said so.

        So the count is sampled on an **independent channel**: an `after_flush`
        listener, which fires once per flush inside the call. A single-flush
        implementation produces samples that are never zero; anything that
        flushes between the two writes produces a zero and is caught. The
        `assert samples` line matters too — a harness that never fired would
        otherwise pass by vacuity.
        """
        from sqlalchemy import event, func, select

        samples: list[int] = []

        def sample(session, _flush_context):
            samples.append(
                session.connection()
                .execute(
                    select(func.count())
                    .select_from(Membership)
                    .where(
                        Membership.org_id == org.id,
                        Membership.role == MemberRole.OWNER,
                        Membership.deleted_at.is_(None),
                    )
                )
                .scalar_one()
            )

        event.listen(db_session, "after_flush", sample)
        try:
            svc.transfer_ownership(db_session, org.id, editor.user_id, actor_user_id=owner.user_id)
        finally:
            event.remove(db_session, "after_flush", sample)

        assert samples, "the transfer never flushed - nothing was sampled"
        assert 0 not in samples, f"the org was transiently ownerless: {samples}"
        assert _owner_count(db_session, org.id) == 1

    def test_an_admin_cannot_transfer_ownership(self, svc, db_session, org, owner, admin, editor):
        """AC11. Otherwise transfer becomes the escalation R1 just closed."""
        with pytest.raises(UserServiceError, match="[Oo]wner"):
            svc.transfer_ownership(db_session, org.id, editor.user_id, actor_user_id=admin.user_id)
        assert _live(db_session, editor.id).role is MemberRole.EDITOR
        assert _owner_count(db_session, org.id) == 1

    def test_the_successor_must_already_be_a_member(self, svc, db_session, org, owner):
        """AC12. An email field here would rebuild the invite-as-owner path."""
        stranger = User(email="stranger@roles.test", password_hash="h", full_name="S")
        db_session.add(stranger)
        db_session.flush()
        with pytest.raises(UserServiceError, match="member"):
            svc.transfer_ownership(db_session, org.id, stranger.id, actor_user_id=owner.user_id)
        assert _owner_count(db_session, org.id) == 1

    def test_a_removed_member_is_not_a_successor(self, svc, db_session, org, owner, editor):
        svc.remove_member(db_session, org.id, editor.id, actor_user_id=owner.user_id)
        with pytest.raises(UserServiceError, match="member"):
            svc.transfer_ownership(db_session, org.id, editor.user_id, actor_user_id=owner.user_id)
        assert _owner_count(db_session, org.id) == 1

    def test_an_owner_can_add_a_second_owner(self, svc, db_session, org, owner, editor):
        """§3: multi-owner stays possible — bus-factor is a real need. The
        defect was never that two owners can exist, it was that an *admin*
        could become the second one."""
        svc.add_owner(db_session, org.id, editor.user_id, actor_user_id=owner.user_id)
        assert _owner_count(db_session, org.id) == 2

    def test_an_admin_cannot_add_an_owner(self, svc, db_session, org, owner, admin, editor):
        with pytest.raises(UserServiceError, match="[Oo]wner"):
            svc.add_owner(db_session, org.id, editor.user_id, actor_user_id=admin.user_id)
        assert _owner_count(db_session, org.id) == 1


# ---------------------------------------------------------------------------
# Section 3 — leave (R6, audit P8)
# ---------------------------------------------------------------------------


class TestLeaveOrg:
    def test_a_viewer_can_leave(self, svc, db_session, org, owner, viewer):
        assert svc.leave_org(db_session, org.id, actor_user_id=viewer.user_id) is True
        assert _live(db_session, viewer.id) is None
        assert _owner_count(db_session, org.id) == 1

    def test_an_admin_can_leave(self, svc, db_session, org, owner, admin):
        assert svc.leave_org(db_session, org.id, actor_user_id=admin.user_id) is True
        assert _owner_count(db_session, org.id) == 1

    def test_the_sole_owner_cannot_leave(self, svc, db_session, org, owner):
        """AC6. The constraint is normal — GitHub, Stripe and Paddle behave the
        same way — but only because both exits now exist: transfer then leave,
        or delete the org."""
        with pytest.raises(UserServiceError, match="[Ll]ast owner"):
            svc.leave_org(db_session, org.id, actor_user_id=owner.user_id)
        assert _owner_count(db_session, org.id) == 1

    def test_one_of_two_owners_can_leave(self, svc, db_session, org, owner, editor):
        """AC8."""
        svc.add_owner(db_session, org.id, editor.user_id, actor_user_id=owner.user_id)
        assert svc.leave_org(db_session, org.id, actor_user_id=owner.user_id) is True
        assert _owner_count(db_session, org.id) == 1

    def test_transfer_then_leave_is_the_sole_owners_exit(self, svc, db_session, org, owner, editor):
        """The capability R1 must not have removed, end to end."""
        svc.transfer_ownership(db_session, org.id, editor.user_id, actor_user_id=owner.user_id)
        assert svc.leave_org(db_session, org.id, actor_user_id=owner.user_id) is True
        assert _live(db_session, owner.id) is None
        assert _owner_count(db_session, org.id) == 1

    def test_a_non_member_cannot_leave(self, svc, db_session, org, owner):
        stranger = User(email="nobody@roles.test", password_hash="h", full_name="N")
        db_session.add(stranger)
        db_session.flush()
        with pytest.raises(UserServiceError, match="not a member"):
            svc.leave_org(db_session, org.id, actor_user_id=stranger.id)


# ---------------------------------------------------------------------------
# Section 4 — the invitation path (AC3, SPEC §1.1)
# ---------------------------------------------------------------------------


class TestInvitationsCannotGrantOwnership:
    @pytest.fixture
    def inv(self, auth) -> InvitationService:
        return InvitationService(auth)

    def test_an_admin_cannot_create_an_owner_invitation(self, inv, db_session, org, owner, admin):
        with pytest.raises(UserServiceError, match="[Oo]wnership"):
            inv.create_invitation(
                db_session, org.id, "target@roles.test", MemberRole.OWNER, admin.user_id
            )

    def test_an_owner_cannot_create_an_owner_invitation_either(self, inv, db_session, org, owner):
        """R1 has no caller exception — an owner uses Transfer ownership."""
        with pytest.raises(UserServiceError, match="[Oo]wnership"):
            inv.create_invitation(
                db_session, org.id, "target2@roles.test", MemberRole.OWNER, owner.user_id
            )

    def test_an_admin_cannot_create_an_admin_invitation(self, inv, db_session, org, owner, admin):
        with pytest.raises(UserServiceError, match="cannot grant"):
            inv.create_invitation(
                db_session, org.id, "target3@roles.test", MemberRole.ADMIN, admin.user_id
            )

    def test_an_admin_can_still_invite_an_editor(self, inv, db_session, org, owner, admin):
        invitation = inv.create_invitation(
            db_session, org.id, "friend@roles.test", MemberRole.EDITOR, admin.user_id
        )
        assert invitation.role is MemberRole.EDITOR

    def test_accepting_a_stored_owner_invitation_creates_no_owner(
        self, inv, db_session, org, owner, auth
    ):
        """AC3, asserted on the `Membership` row rather than on the response.

        An invitation is a role grant that outlives the check that made it, so
        a row written before this landed — or by any future writer of the table
        — must not become an owner membership on accept. A fix that only
        changed the invite-role select leaves this open.
        """
        from datetime import UTC, datetime, timedelta

        from datanika.models.invitation import Invitation, InvitationStatus

        invitee = User(email="invitee@roles.test", password_hash="h", full_name="I")
        db_session.add(invitee)
        db_session.flush()

        # Constructed directly: the service refuses to create one of these now,
        # which is exactly why accept needs its own guard.
        stale = Invitation(
            org_id=org.id,
            email="invitee@roles.test",
            role=MemberRole.OWNER,
            invited_by_user_id=owner.user_id,
            token="stale-owner-invitation-token",
            status=InvitationStatus.PENDING,
            expires_at=datetime.now(UTC) + timedelta(days=7),
        )
        db_session.add(stale)
        db_session.flush()

        assert inv.accept_invitation(db_session, stale.token) is None
        assert _owner_count(db_session, org.id) == 1
        assert (
            db_session.query(Membership)
            .filter(Membership.org_id == org.id, Membership.user_id == invitee.id)
            .count()
            == 0
        )


# ---------------------------------------------------------------------------
# Section 5 — `owner` is unreachable outside the allowlisted paths (AC13)
# ---------------------------------------------------------------------------


# module -> {enclosing function names allowed to assign MemberRole.OWNER}
_OWNER_WRITERS = {
    "datanika/services/user_service.py": {
        "create_org",  # signup default org — the creator is the owner
        "find_or_create_oauth_user",  # OAuth default org, same reason
        "transfer_ownership",
        "add_owner",
    },
    # Deterministic E2E fixture, not a production path — but it does write
    # owner memberships, so it is named rather than excluded by directory.
    "datanika/scripts/e2e_seed.py": {"_build_fixture"},
}


def _owner_assignment_sites(root: pathlib.Path) -> dict[str, set[str]]:
    """Every place production code *writes* `MemberRole.OWNER`.

    Assignments and `Membership(role=...)` keywords only — a comparison
    (`membership.role == MemberRole.OWNER`) is a read and is not a grant.
    Derived from the AST rather than from a hardcoded count, because the count
    is the part that rots.
    """
    found: dict[str, set[str]] = {}

    def is_owner(node) -> bool:
        return (
            isinstance(node, ast.Attribute)
            and node.attr == "OWNER"
            and isinstance(node.value, ast.Name)
            and node.value.id == "MemberRole"
        )

    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root.parent).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for func in ast.walk(tree):
            if not isinstance(func, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for node in ast.walk(func):
                writes = []
                # An assignment (`membership.role = MemberRole.OWNER`) or a
                # `role=` keyword (`Membership(role=MemberRole.OWNER)`).
                # Explicitly parenthesised: ruff's SIM114 merges the two
                # branches, and `A or B and C` is correct but reads as though
                # it might not be.
                if isinstance(node, ast.Assign) or (
                    isinstance(node, ast.keyword) and node.arg == "role"
                ):
                    writes.append(node.value)
                if any(is_owner(w) for w in writes):
                    found.setdefault(rel, set()).add(func.name)
    return found


class TestOwnershipIsWrittenInOnlyFourPlaces:
    def test_no_unexpected_code_path_grants_owner(self):
        root = pathlib.Path(__file__).resolve().parents[2] / "datanika"
        sites = _owner_assignment_sites(root)
        unexpected = {
            module: sorted(fns - _OWNER_WRITERS.get(module, set()))
            for module, fns in sites.items()
            if fns - _OWNER_WRITERS.get(module, set())
        }
        assert not unexpected, (
            "these functions assign MemberRole.OWNER and are not allowlisted: "
            f"{unexpected}. Ownership is reached through transfer_ownership / "
            "add_owner and the two account-creation paths, and nowhere else "
            "(SPEC_ORG_ROLES R1)."
        )

    def test_the_allowlist_still_describes_real_code(self):
        """The allowlist is only evidence while it matches the tree.

        A stale entry makes the test above pass by describing a function that
        no longer exists — the same shape as a green that records nothing.
        """
        root = pathlib.Path(__file__).resolve().parents[2] / "datanika"
        sites = _owner_assignment_sites(root)
        for module, fns in _OWNER_WRITERS.items():
            assert module in sites, f"{module} no longer writes MemberRole.OWNER"
            assert fns <= sites[module], (
                f"allowlisted functions missing from {module}: {sorted(fns - sites[module])}"
            )
