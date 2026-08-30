"""Proof for core#658: an admin can promote itself to owner and lock the owner out.

What the issue claims is a two-step chain. This module proves the **end state**
it reaches, not just the first step -- the founder is removed from their own
organization, by someone they granted admin, with no way back.

Why the assertions are on outcomes rather than on call signatures
----------------------------------------------------------------
``UserService.change_role`` takes ``(session, org_id, membership_id, new_role)``
and **no caller identity at all**, so "an admin may not grant owner" is not
expressible against today's signature -- there is nothing to pass. Its only
guard fires on demotion::

    if membership.role == MemberRole.OWNER and new_role != MemberRole.OWNER:
        self._check_last_owner(session, org_id)

A promotion is not a demotion, and the target is not currently an owner, so
nothing fires. The single gate is in the UI state layer
(``settings_state.change_member_role`` and ``remove_member``, both
``_check_role("admin")``), and it admits the attacker by construction.

So every test here performs the exact calls those two handlers make and then
asserts the **resulting membership rows**, never a signature and never a call
site. An assertion about *where* the check lives goes red on a correct fix that
puts it somewhere else, which is worse than no test.

⚠️ **These assert that the SERVICE refuses, and that is a deliberate position,
not an accident of where the test sits.** core#658 offers two fix directions,
and they are not equivalent here:

* **A caller check in the service** (the issue's direction 1). These tests go
  green -- provided the check **fails closed** when no caller is supplied, which
  is this repo's own documented convention for exactly this situation. See
  ``UserService.find_or_create_oauth_user``: *"``email_verified`` is keyword-only
  and defaults to ``False`` so that a caller which forgets it fails closed
  instead of trusting silently."* The same reasoning applies to an actor
  argument on ``change_role``.
* **Raising the UI gate to ``owner``** (direction 2), and nothing else. These
  tests stay **red**, and that is a true negative rather than a failed fix:
  ``change_role`` would still grant ``owner`` to anyone who calls it. There is
  exactly one caller today (``settings_state``), so direction 2 does close the
  currently reachable path -- but it leaves a privilege-granting method with no
  authorisation of its own, and the next caller (an API route, the MCP surface,
  an invitation-acceptance flow) reopens it silently.
  ``test_an_editor_cannot_grant_itself_owner`` exists to make that visible.

The issue says "prefer both". These tests hold the service half. **If you take
direction 2 only, do not delete these -- say so on the issue**, because a red
here after that fix is information, not noise.

The escalation calls are wrapped in ``_attempt`` so that a fix which *refuses*
them does not error the test. The oracle is the end state, never the exception.

Markers
-------
``xfail(strict=True, raises=AssertionError)``. ``strict`` makes XPASS a failure,
so the marker cannot outlive the fix. ``raises=AssertionError`` keeps an
ImportError or a signature change from being absorbed into a silent xfail.

Bounds, stated so nobody re-escalates on a re-read: the actor must already hold
admin, granted deliberately by an owner, and the reach is that one org --
``_check_last_owner`` and the tenant-isolation suite are not bypassed. What
keeps it on the list is that the lockout is **irreversible from the victim's
side**: there is no ownership recovery, no support tooling, and no account or
org deletion (core#655).
"""

import ast
import inspect

import pytest

from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService

xfail_658 = pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "core#658: change_role takes no caller identity and its only guard is on "
        "demotion, so an admin may grant itself owner and then remove the founder."
    ),
)

xfail_658_ui = pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "core#658 (UI honesty half): member_row renders the role dropdown and the "
        "Remove button for every viewer regardless of role. Server-side checks make "
        "it unexploitable -- it shows controls that always fail."
    ),
)


# Keyword names a fix might use to carry the caller's identity into the service.
_ACTOR_KWARGS = (
    "actor_user_id",
    "acting_user_id",
    "caller_user_id",
    "actor_id",
    "by_user_id",
    "requested_by",
)


def _call_as(fn, actor: Membership, *args):
    """Invoke a ``UserService`` mutator as ``actor``.

    Today the service takes no caller identity, so the actor is dropped and the
    call made is byte-for-byte the one ``settings_state`` makes. If a fix adds a
    keyword for the caller, it is forwarded.

    This seam exists so that closing core#658 does not require rewriting this
    file: a suite you must edit before the fix can pass is not a regression
    guard. ⚠️ If a fix uses a keyword name not listed in ``_ACTOR_KWARGS``, the
    actor is silently dropped -- which makes a fail-closed service refuse, so
    **the escalation tests go green and the Section 3 controls go RED and name
    themselves.** That is the safe polarity: a missed name is loud, never a
    false pass. Add the name here.
    """
    params = inspect.signature(fn).parameters
    for name in _ACTOR_KWARGS:
        if name in params:
            return fn(*args, **{name: actor.user_id})
    return fn(*args)


def _attempt(fn, actor: Membership, *args) -> bool:
    """Run an operation the UI currently permits, as ``actor``. True if it completed.

    Swallows refusals on purpose: after the fix these calls are *supposed* to
    raise, and the test must survive that to go on and assert the end state.
    Nothing is concluded from the return value -- the membership rows are the
    oracle.
    """
    try:
        _call_as(fn, actor, *args)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Fixtures — one org, its founder, and an admin the founder granted.
# ---------------------------------------------------------------------------


@pytest.fixture
def svc() -> UserService:
    return UserService(AuthService(secret_key="test-secret-key-for-escalation"))


@pytest.fixture
def org(db_session) -> Organization:
    o = Organization(name="Founders Co", slug="founders-co-658")
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
def founder(db_session, org) -> Membership:
    """The original owner. The victim."""
    return _member(db_session, org, "founder@example.com", MemberRole.OWNER)


@pytest.fixture
def attacker(db_session, org) -> Membership:
    """An admin the founder deliberately granted. The actor."""
    return _member(db_session, org, "admin@example.com", MemberRole.ADMIN)


def _live(db_session, membership_id: int) -> Membership | None:
    """The membership row if it is still active, else None."""
    return (
        db_session.query(Membership)
        .filter(
            Membership.id == membership_id,
            Membership.deleted_at.is_(None),
        )
        .one_or_none()
    )


def _owner_user_ids(db_session, org_id: int) -> set[int]:
    rows = (
        db_session.query(Membership)
        .filter(
            Membership.org_id == org_id,
            Membership.role == MemberRole.OWNER,
            Membership.deleted_at.is_(None),
        )
        .all()
    )
    return {r.user_id for r in rows}


def _escalate_and_evict(svc, db_session, org, founder, attacker) -> None:
    """The full chain, exactly as the two state handlers invoke it.

    ``settings_state.change_member_role`` -> ``svc.change_role(...)``
    ``settings_state.remove_member``      -> ``svc.remove_member(...)``

    Both are gated only at ``_check_role("admin")``, which the attacker passes.
    """
    _attempt(svc.change_role, attacker, db_session, org.id, attacker.id, MemberRole.OWNER)
    _attempt(svc.remove_member, attacker, db_session, org.id, founder.id)


# ---------------------------------------------------------------------------
# Section 1 — the reachable end state. Red today.
# ---------------------------------------------------------------------------


class TestTheFounderCannotBeLockedOut:
    """Each test runs the whole chain and asserts one facet of the end state."""

    @xfail_658
    def test_the_founder_still_belongs_to_their_organization(
        self, svc, db_session, org, founder, attacker
    ):
        _escalate_and_evict(svc, db_session, org, founder, attacker)
        assert _live(db_session, founder.id) is not None

    @xfail_658
    def test_the_founder_is_still_an_owner(self, svc, db_session, org, founder, attacker):
        _escalate_and_evict(svc, db_session, org, founder, attacker)
        assert founder.user_id in _owner_user_ids(db_session, org.id)

    @xfail_658
    def test_the_attacker_is_not_the_sole_owner(self, svc, db_session, org, founder, attacker):
        _escalate_and_evict(svc, db_session, org, founder, attacker)
        assert _owner_user_ids(db_session, org.id) != {attacker.user_id}

    @xfail_658
    def test_the_org_is_still_listed_for_the_founder(self, svc, db_session, org, founder, attacker):
        """The lockout, from the victim's side.

        ``get_user_orgs`` is what the org switcher reads. Once the membership is
        soft-deleted the org stops being listed, and there is no recovery path
        and no support tooling -- so this is the whole of the user-visible
        damage.
        """
        _escalate_and_evict(svc, db_session, org, founder, attacker)
        assert org.id in {o.id for o in svc.get_user_orgs(db_session, founder.user_id)}


# ---------------------------------------------------------------------------
# Section 2 — the two independent holes, each on its own. Red today.
# ---------------------------------------------------------------------------


class TestEachStepOfTheChain:
    """Both steps are separately broken, so half a fix is not a fix.

    core#658 says it explicitly: ``remove_member`` shares the gate, so closing
    only ``change_role`` still lets an admin remove an owner whenever a second
    owner exists.
    """

    @xfail_658
    def test_an_admin_cannot_grant_itself_owner(self, svc, db_session, org, founder, attacker):
        _attempt(svc.change_role, attacker, db_session, org.id, attacker.id, MemberRole.OWNER)
        assert _live(db_session, attacker.id).role is MemberRole.ADMIN

    @xfail_658
    def test_an_admin_cannot_grant_owner_to_a_third_party(self, svc, db_session, org, attacker):
        """Escalation by proxy -- granting owner to an account the actor controls."""
        _member(db_session, org, "keeper@example.com", MemberRole.OWNER)
        patsy = _member(db_session, org, "patsy@example.com", MemberRole.VIEWER)
        _attempt(svc.change_role, attacker, db_session, org.id, patsy.id, MemberRole.OWNER)
        assert _live(db_session, patsy.id).role is MemberRole.VIEWER

    @xfail_658
    def test_an_admin_cannot_remove_an_owner(self, svc, db_session, org, founder, attacker):
        """Independent of the promotion step.

        A second owner is present, so ``_check_last_owner`` is satisfied and does
        not mask the missing authorisation check. The admin is never promoted
        here -- this is the ``remove_member`` hole on its own.
        """
        _member(db_session, org, "cofounder@example.com", MemberRole.OWNER)
        _attempt(svc.remove_member, attacker, db_session, org.id, founder.id)
        assert _live(db_session, founder.id) is not None

    @xfail_658
    def test_an_editor_cannot_grant_itself_owner(self, svc, db_session, org, founder):
        """The service layer has no caller check at all, so role is irrelevant to it.

        This is red for the same reason as the admin case and green after any
        fix that consults the caller. It matters because it shows the service is
        not merely too permissive about admins -- it is unauthenticated, and its
        safety rests entirely on the single UI gate above it.
        """
        editor = _member(db_session, org, "editor@example.com", MemberRole.EDITOR)
        _attempt(svc.change_role, editor, db_session, org.id, editor.id, MemberRole.OWNER)
        assert _live(db_session, editor.id).role is MemberRole.EDITOR


# ---------------------------------------------------------------------------
# Section 3 — controls. Green today, and must stay green after the fix.
# ---------------------------------------------------------------------------


class TestLegitimateOwnershipFlowsKeepWorking:
    """core#658 acceptance criterion 3, and the guard against overshooting.

    The obvious way to get this wrong is to forbid granting ``owner`` outright,
    which would break the one flow that has to keep working.
    """

    def test_an_owner_can_transfer_ownership_and_step_down(
        self, svc, db_session, org, founder, attacker
    ):
        _call_as(svc.change_role, founder, db_session, org.id, attacker.id, MemberRole.OWNER)
        assert _live(db_session, attacker.id).role is MemberRole.OWNER

        _call_as(svc.change_role, founder, db_session, org.id, founder.id, MemberRole.ADMIN)
        assert _live(db_session, founder.id).role is MemberRole.ADMIN
        assert _owner_user_ids(db_session, org.id) == {attacker.user_id}

    def test_an_owner_can_hand_over_and_leave_entirely(
        self, svc, db_session, org, founder, attacker
    ):
        _call_as(svc.change_role, founder, db_session, org.id, attacker.id, MemberRole.OWNER)
        _call_as(svc.remove_member, founder, db_session, org.id, founder.id)
        assert _live(db_session, founder.id) is None
        assert _owner_user_ids(db_session, org.id) == {attacker.user_id}

    def test_the_last_owner_cannot_be_demoted(self, svc, db_session, org, founder):
        """The one guard that does exist still holds -- even for the owner."""
        assert (
            _attempt(svc.change_role, founder, db_session, org.id, founder.id, MemberRole.ADMIN)
            is False
        )
        assert _live(db_session, founder.id).role is MemberRole.OWNER

    def test_the_last_owner_cannot_be_removed(self, svc, db_session, org, founder):
        assert _attempt(svc.remove_member, founder, db_session, org.id, founder.id) is False
        assert _live(db_session, founder.id) is not None

    def test_an_owner_can_still_manage_non_owner_roles(self, svc, db_session, org, founder):
        editor = _member(db_session, org, "e2@example.com", MemberRole.EDITOR)
        _call_as(svc.change_role, founder, db_session, org.id, editor.id, MemberRole.ADMIN)
        assert _live(db_session, editor.id).role is MemberRole.ADMIN

    def test_an_admin_can_still_manage_non_owner_roles(
        self, svc, db_session, org, founder, attacker
    ):
        """Admins keep the member management they are supposed to have.

        A fix that closed core#658 by removing admin's role-change ability
        entirely would pass every red test above and break the feature. This is
        the floor.
        """
        editor = _member(db_session, org, "e3@example.com", MemberRole.EDITOR)
        _call_as(svc.change_role, attacker, db_session, org.id, editor.id, MemberRole.VIEWER)
        assert _live(db_session, editor.id).role is MemberRole.VIEWER


# ---------------------------------------------------------------------------
# Section 4 — the UI-honesty half. Red today, S3.
# ---------------------------------------------------------------------------


def _member_row_source() -> ast.FunctionDef:
    from datanika.ui.pages import settings as settings_page

    with open(inspect.getfile(settings_page), encoding="utf-8") as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "member_row":
            return node
    raise AssertionError("member_row not found in datanika.ui.pages.settings")


class TestMemberControlsAreRoleGated:
    """core#658 acceptance criterion 4. Not a hole -- the server checks are real.

    A viewer simply sees a role dropdown and a Remove button that always fail.
    Follows the ``test_rbac_ui_visibility`` convention (AST, no Reflex state) so
    the invariant survives markup refactors.
    """

    @xfail_658_ui
    def test_member_row_gates_its_controls_on_a_condition(self):
        row = _member_row_source()
        has_cond = any(
            isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "cond"
            for n in ast.walk(row)
        )
        assert has_cond, (
            "member_row renders the role select and the Remove button "
            "unconditionally -- no rx.cond anywhere in the function."
        )
