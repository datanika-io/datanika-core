"""Service-layer authorization — the control, not the second layer (core#681).

`SPEC_SERVICE_AUTHORIZATION` §4. `SPEC_ORG_ROLES` §4 already decided *where* enforcement lives —
*"in `UserService`, not in the Reflex state … the UI check stays as a second layer for the error
message, but it is **not the control**"* — and it shipped for membership only. This is that
mechanism, generalised so the other eight subsystems can use it.

Why the service and not the handler
-----------------------------------
🔑 A check in `ConnectionService` is inherited by the Reflex path **and** by the 26 mutating REST
endpoints. A check in `ConnectionState` is inherited by **neither** of them. Hardening a handler
widens the gap between the two surfaces rather than closing it — which is why two layers hardened
above an unguarded one reads, from every instrument we have, as three (§6).

The intersection, and that it is a decision
-------------------------------------------
🚨 **Founder-decided 2026-09-10 (core#681): a key's authority IS intersected with its owner's
current org role, at authentication time.** This module is where that happens, and it happens by
construction: :func:`assert_org_role` resolves the actor's membership **now**, from the database,
every call. On the REST path the only available actor is ``api_key.user_id``, so a key minted by an
admin who is later demoted stops working — at a moment nobody associates with the key, with no
deploy, and with nothing in the system having changed except who is using it.

**That silent breakage is the entire cost of the decision, and its only mitigation is that the
refusal says why.** :class:`InsufficientRoleError` therefore carries ``required_role`` as a
**field**: `api_v1_routes` renders it into a `403` a script can branch on, and nothing in the
message may read as *"expired"* or *"revoked"*, which would send the caller to re-mint a key that
was never the problem.

Existing keys are intersected on next use. **No grandfather** — Product: *"an exemption without an
expiry is branch B wearing a date."*

⚠️ Two ways to get this wrong, both named in §4
-----------------------------------------------
1. **Do not put `_check_role` in a service.** It reads Reflex state, and a service reached from a
   REST request has none. That is the UI-only defect one layer down.
2. **Do not pass a role down from the caller.** ``save(..., role="editor")`` is not a check — *a
   caller that supplies its own authority is not being checked* — and it reduces the REST path's
   job to "send the right string".
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from datanika.errors import UserFacingError
from datanika.models.user import MemberRole, Membership
from datanika.services.auth import ROLE_RANK


class InsufficientRoleError(UserFacingError):
    """The actor's **current** membership does not permit this operation.

    Carries ``required_role`` as a structured attribute rather than only in the sentence,
    because the REST surface answers a script (§7.1) and the Reflex surface answers a person
    (§7.2). Those are different messages and sharing one string gives a poor version of both.

    ⚠️ Distinct from "not a member" and from "not your org". Cross-org lookups return **not
    found** and must keep doing so — the difference between *"not yours"* and *"not allowed"* is
    the whole reason a `403` is safe here (§7.3).
    """

    def __init__(
        self,
        *,
        required_role: str,
        operation: str,
        actor_role: str | None = None,
    ) -> None:
        self.required_role = required_role
        self.operation = operation
        self.actor_role = actor_role
        super().__init__(f"This operation requires the {required_role} role.")


def assert_org_role(
    session: Session,
    org_id: int,
    actor_user_id: int | None,
    *,
    required: MemberRole | str,
    operation: str,
) -> Membership:
    """Refuse unless ``actor_user_id``'s **current** membership in ``org_id`` reaches ``required``.

    Returns the membership so a caller that needs the role can use it without a second query.

    ⚠️ **Fails closed on a missing actor**, copying `UserService._actor_membership`'s convention
    and for the reason its docstring gives (core#658): defaulting the actor to ``None`` and
    *allowing* it leaves the same hole open for the next caller — an API route, the MCP surface,
    a restore flow — while looking fixed.

    ⚠️ **Call this AFTER the org-scoped lookup and BEFORE the mutation** (§7.3). Checking the role
    first turns a cross-org probe into a `403`, which confirms the resource exists somewhere. The
    right order is not a general rule; it is decided per surface by what the refusal would
    otherwise disclose — the mirror of `SPEC_SIGNUP_ENUMERATION` D5, where the limit had to come
    *before* the lookup.
    """
    required_value = getattr(required, "value", required)
    if required_value not in ROLE_RANK:
        # Not a UserFacingError: an unknown threshold is a programming error, and §2 says a
        # fourth threshold is a Product question rather than something to invent here.
        raise ValueError(f"Unknown role threshold {required_value!r}")

    if actor_user_id is None:
        raise InsufficientRoleError(required_role=required_value, operation=operation)

    membership = (
        session.query(Membership)
        .filter(
            Membership.org_id == org_id,
            Membership.user_id == actor_user_id,
            Membership.deleted_at.is_(None),
        )
        .one_or_none()
    )
    if membership is None:
        raise InsufficientRoleError(required_role=required_value, operation=operation)

    actor_value = getattr(membership.role, "value", membership.role)
    if ROLE_RANK.get(actor_value, -1) < ROLE_RANK[required_value]:
        raise InsufficientRoleError(
            required_role=required_value,
            operation=operation,
            actor_role=actor_value,
        )
    return membership
