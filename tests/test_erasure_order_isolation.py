"""The erasure re-registration fails when a prior commit survives the fixture.

Measured on ``f4fb79c7``, the tree core#1218 was filed against. The shortest
prefix of ``tests/test_tasks`` then ``tests/test_services`` that made
``test_the_erased_address_can_register_again`` raise ``Slug already exists``
ended at ``test_the_route_sets_the_column`` in
``TestVerifyEmailRouteStillClosesTheLoop``
(``tests/test_services/test_email_verification_wiring.py``).
That route test alone does not flip it. ``verify_email`` commits one user
(``loop@example.com``). The two organizations in front of it come from
``TestRunHistoryIsNeverPurged`` (``retention-<hex>`` slugs). At the erasure
test those were organizations 1 and 2 and user 1. Erasure renamed the new
org to ``org-3``, and the second registration asked for ``org-3``.

On current ``dev`` that same order stays green with the pysqlite listeners
removed, because the counts no longer match. A shuffled suite can pass
while ``Session.commit`` still escapes the rollback, so it is not this guard.
These two tests pin the measured counts, in this order. Both steps in one
test would collide inside the session even when the rollback holds.
"""

from sqlalchemy import select

from datanika.models.user import Organization
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService

_PASSWORD = "sup3rSecret!pw"


def _service() -> UserService:
    return UserService(AuthService("test-secret-key-for-erasure-order"))


def test_a_prior_test_commits_two_orgs_and_one_user(db_session):
    """The leaked shape: two organizations and one user, committed."""
    db_session.add(Organization(name="Retention one", slug="retention-a"))
    db_session.add(Organization(name="Retention two", slug="retention-b"))
    db_session.flush()
    user = _service().register_user(db_session, "loop@example.com", _PASSWORD, "Loop Person")
    db_session.commit()
    slugs = set(
        db_session.scalars(
            select(Organization.slug).where(Organization.slug.in_(("retention-a", "retention-b")))
        ).all()
    )
    assert slugs == {"retention-a", "retention-b"}
    assert user.email == "loop@example.com"


def test_b_the_erased_address_can_register_again(db_session):
    """Re-registration after erasure. Raises if the commit above is still there."""
    svc = _service()
    user = svc.register_user(db_session, "returning@example.com", _PASSWORD, "Returning Person")
    svc.create_org(db_session, "Returning Person's Org", f"org-{user.id}", user.id)
    old_id = user.id
    svc.erase_user(db_session, old_id)
    db_session.flush()
    again = svc.register_user(db_session, "returning@example.com", _PASSWORD, "Returning Person")
    svc.create_org(db_session, "Returning Person's Org", f"org-{again.id}", again.id)
    assert again.id != old_id
