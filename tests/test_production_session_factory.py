"""The shared two-session fixture reproduces what a one-session test cannot (core#1412).

``production_session_factory`` exists for one property: an instance a session already holds is not
refreshed by an ORM ``SELECT`` when ANOTHER session commits a change to its row. core#657 lived in
that gap. A fixture that silently lost the property would turn every test built on it into a
single-session test again, still green, which is the defect it was built to expose. So the property
is asserted here, beside the two things it depends on.
"""

from sqlalchemy import select

from datanika.models.user import Organization


def _seed(factory) -> int:
    with factory() as setup:
        org = Organization(name="Before", slug="acme-1412-fixture")
        setup.add(org)
        setup.commit()
        return org.id


def test_it_is_built_with_productions_session_options(production_session_factory):
    from datanika.db import sync_session_factory

    ours = {k: v for k, v in production_session_factory.kw.items() if k != "bind"}
    theirs = {k: v for k, v in sync_session_factory.kw.items() if k != "bind"}
    assert ours == theirs
    assert production_session_factory.class_.__mro__[1] is sync_session_factory.class_.__mro__[1]


def test_the_sessions_share_one_database(production_session_factory):
    """A ``:memory:`` engine gives each connection its own database and fails this."""
    org_id = _seed(production_session_factory)

    with production_session_factory() as other:
        assert other.get(Organization, org_id) is not None


def test_a_held_instance_stays_stale_after_another_sessions_commit(production_session_factory):
    """The property. Without it, a green on this fixture could mean nothing was ever stale."""
    org_id = _seed(production_session_factory)
    worker = production_session_factory()
    try:
        held = worker.execute(select(Organization).where(Organization.id == org_id)).scalar_one()

        with production_session_factory() as api:
            api.get(Organization, org_id).name = "After"
            api.commit()

        again = worker.execute(select(Organization).where(Organization.id == org_id)).scalar_one()
        assert again is held, "the ORM did not hand back the identity-mapped instance"
        assert again.name == "Before", "the held instance was refreshed, so nothing is stale"
    finally:
        worker.close()

    with production_session_factory() as reader:
        assert reader.get(Organization, org_id).name == "After"
