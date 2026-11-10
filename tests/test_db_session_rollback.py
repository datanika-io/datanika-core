"""A commit in one test must not remain visible in the next.

``db_session`` opens an outer transaction and rolls it back. On pysqlite,
``Session.commit`` releases the nested savepoint and that release commits the
outer transaction, so the fixture's ``rollback()`` no longer undoes the row.
Committed organizations then keep their ids for the rest of the process.

``DashboardState.load_dashboard`` builds a ``RunItem`` for every run
``list_runs`` returns before it emits ``usage.get_summary``. The sweep harness
signs in as org 7 and calls ``_resolve_target_name`` on a ``MagicMock``, so a
committed run for that org fails
``test_the_harness_can_see_a_session_and_an_emit`` before the emit.
``tests/test_tasks/test_transformation_dbt_failure.py`` commits one
organization per real dbt run, and the column-catalog test is one more of
those. refs #1375.
"""

from datanika.models.user import Organization

_COMMITTED_ORG_ID: dict[str, int] = {}


def test_a_commit_is_visible_inside_the_test(db_session):
    org = Organization(name="Isolation", slug="isolation-commit")
    db_session.add(org)
    db_session.commit()
    found = db_session.get(Organization, org.id)
    assert found is not None
    assert found.slug == "isolation-commit"
    _COMMITTED_ORG_ID["id"] = org.id


def test_the_next_test_does_not_see_that_commit(db_session):
    org_id = _COMMITTED_ORG_ID["id"]
    assert db_session.get(Organization, org_id) is None
