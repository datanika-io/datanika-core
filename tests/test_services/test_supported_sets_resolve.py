"""Every ``SUPPORTED_*`` set is a claim. This asks the layer beneath to honour it.

[core#494] (DuckDB), [core#845] (Redshift) and [core#865] (MySQL and SQLite as
load destinations) are the same defect three times: a connector advertised in a
set whose dialect or factory does not exist. Every cheap check passed each time
— the type is in the set, the form renders, a DBAPI package of roughly the right
name is installed — because the only thing that separates a real capability from
an advertised one is **calling the layer below**.

`redshift-connector` made that especially convincing: it is in the lock (pulled
by `dbt-redshift`), so the driver is installed and a manifest read is clean. It
registers no SQLAlchemy dialect. `sqlalchemy-redshift` does, and we did not ship
it, so **no Redshift `sql_database()` source has ever worked** — it raised
`NoSuchModuleError` at `create_engine`, before any network call.

🔑 **The per-entry walk is what makes a failure attributable.** A test asserting
only "redshift is broken" would say nothing about the other seven; the entries
that *do* resolve are what turn "something is odd about this environment" into
"this entry is wrong". Both tests below therefore assert a floor on the number
that resolved **before** they assert the failures are empty — in that order,
because an environment-wide breakage otherwise reads as a per-entry defect.

⚠️ `create_engine(f"{drivername}://u:p@h/db")` is the wrong probe even though it
is the one the issue used: SQLite rejects a URL carrying a host and credentials,
so a correct entry fails for a reason that has nothing to do with the dialect
being registered. `make_url(f"{drivername}://").get_dialect()` loads the plugin
and nothing else.
"""

import ast
import pathlib

import dlt
import sqlalchemy as sa

from datanika.services.dlt_runner import SOURCE_DRIVERNAME_MAP, DltRunnerService

#: Destination types we advertise for which dlt ships no factory.
#:
#: 🚨 **This set may only ever shrink, and both directions are asserted.** It is
#: not a suppression list: withdrawing an advertised capability is a product
#: decision ([core#865], and [core#862] for the `SUPPORTED_ADAPTERS` twin), so
#: Engineering records the gap here rather than deleting the entries on its own
#: authority. An entry that starts resolving must come out of this set in the
#: same change, or the guard goes green over a claim nobody re-checked.
KNOWN_UNRESOLVABLE_DESTINATIONS = {"mysql", "sqlite"}


class TestEverySourceDialectIsRegistered:
    def test_the_walk_resolves_most_entries(self):
        """Negative control, and it has to run first.

        If almost nothing resolves, the failure is in this environment and the
        assertion below cannot attribute anything to a single map entry.
        """
        resolved, _ = _walk_dialects()
        assert len(resolved) >= 6, (
            f"only {len(resolved)} of {len(SOURCE_DRIVERNAME_MAP)} dialects resolved — "
            "that is an environment problem, not a map problem, and nothing below "
            "this line means what it says"
        )

    def test_every_entry_resolves(self):
        resolved, failures = _walk_dialects()
        assert failures == {}, (
            f"advertised source types whose SQLAlchemy dialect is not installed: "
            f"{failures}. {sorted(resolved)} resolve, which is what makes this the "
            f"entry's fault rather than the environment's."
        )


class TestEveryDestinationTypeHasAFactory:
    def test_the_walk_resolves_most_entries(self):
        resolved, _ = _walk_destinations()
        assert len(resolved) >= 8, (
            f"only {len(resolved)} of {len(DltRunnerService.SUPPORTED_DESTINATION_TYPES)} "
            "destination factories resolved — read this as an environment or dlt-version "
            "problem before reading anything below it"
        )

    def test_no_new_destination_is_advertised_without_a_factory(self):
        _, missing = _walk_destinations()
        assert missing <= KNOWN_UNRESOLVABLE_DESTINATIONS, (
            f"newly advertised destination types with no dlt factory: "
            f"{sorted(missing - KNOWN_UNRESOLVABLE_DESTINATIONS)}. "
            "`build_destination` does getattr(dlt.destinations, type), so this is an "
            "AttributeError inside a Celery task, not a refusal at selection time."
        )

    def test_the_known_gap_list_has_not_gone_stale(self):
        """The other direction, and the one that keeps the list honest.

        An entry that starts resolving — a dlt release, a plugin — must leave
        this set in the same change. Without this assertion the allowlist is
        green forever and stops describing anything.
        """
        _, missing = _walk_destinations()
        assert missing >= KNOWN_UNRESOLVABLE_DESTINATIONS, (
            f"these are listed as having no dlt factory and now resolve: "
            f"{sorted(KNOWN_UNRESOLVABLE_DESTINATIONS - missing)}. Remove them from "
            "KNOWN_UNRESOLVABLE_DESTINATIONS."
        )

    def test_an_unresolvable_destination_is_refused_by_name(self):
        """core#865 — the failure must name the cause, not be an AttributeError.

        `getattr(dlt.destinations, "mysql")` raises `AttributeError: module
        'dlt.destinations' has no attribute 'mysql'` from inside a Celery task,
        which reads as a dlt bug rather than as us advertising something we do
        not have.
        """
        import pytest

        from datanika.services.dlt_runner import DltRunnerError

        for connection_type in sorted(KNOWN_UNRESOLVABLE_DESTINATIONS):
            with pytest.raises(DltRunnerError) as exc:
                DltRunnerService().build_destination(connection_type, {"host": "h"})
            assert connection_type in str(exc.value)


def _walk_dialects() -> tuple[dict[str, str], dict[str, str]]:
    resolved: dict[str, str] = {}
    failures: dict[str, str] = {}
    for conn_type, drivername in sorted(SOURCE_DRIVERNAME_MAP.items()):
        try:
            dialect = sa.engine.url.make_url(f"{drivername}://").get_dialect()
        except Exception as exc:
            failures[conn_type] = f"{drivername} -> {type(exc).__name__}: {exc}"
        else:
            resolved[conn_type] = f"{dialect.__module__}.{dialect.__name__}"
    return resolved, failures


def _walk_destinations() -> tuple[set[str], set[str]]:
    advertised = set(DltRunnerService.SUPPORTED_DESTINATION_TYPES)
    missing = {t for t in advertised if not hasattr(dlt.destinations, t)}
    return advertised - missing, missing


TESTS_DIR = pathlib.Path(__file__).parent.parent

#: The name a `@patch("datanika.services.dlt_runner.dlt")` argument conventionally
#: takes, so `<mock>.destinations.<x>` is the shape being looked for.
_MOCK_NAME = "mock_dlt"


class TestNoMockAssertsADestinationThatDoesNotExist:
    """A ``MagicMock`` materialises whatever attribute is asked of it.

    So ``@patch("datanika.services.dlt_runner.dlt")`` makes
    ``dlt.destinations.<anything>`` exist, and a test written that way asserts
    the connector works for **every** name — including the two dlt does not
    ship. Not hypothetical: ``test_mysql_returns_destination`` and
    ``test_sqlite_returns_destination`` did exactly that and **could not have
    failed**. They passed for years, which is why core#865 was found by
    attempting a real load rather than by the suite. The one check that would
    have caught it was the one the mock replaced.

    Derived from the test sources rather than from a list of the names we happen
    to remember, so the next mocked destination cannot reintroduce it.
    """

    def test_the_scan_matches_the_real_idiom(self):
        """Negative control. A regex that matches nothing reports zero offenders.

        ``postgres`` is mocked this way in ``test_dlt_runner.py`` and really
        exists, so its presence proves the scan is reading the files and the
        pattern it claims to.
        """
        assert "postgres" in _mocked_destination_names(), (
            "the scan found no `mock_dlt.destinations.postgres`, so it is not "
            "looking at what it claims to and its silence proves nothing"
        )

    def test_every_mocked_destination_name_really_exists(self):
        offenders = {
            name: sorted(files)
            for name, files in _mocked_destination_names(with_files=True).items()
            if not hasattr(dlt.destinations, name)
        }
        assert offenders == {}, (
            f"these tests mock a dlt destination that does not exist, so they assert "
            f"a capability we do not have and cannot fail: {offenders}"
        )


def _mocked_destination_names(*, with_files: bool = False):
    """Walk the AST, not the text.

    ⚠️ The first version of this was a regex over the file contents, and it
    reported an offender that was **its own docstring** — the paragraph above
    quotes `mock_dlt.destinations.mysql` in order to explain the trap. A guard
    that a correction *describing* the defect can trip is a guard nobody can
    ever get to green, and the natural response is to weaken it. Same shape as
    WORKFLOW_RULES §4's connector guides, which were corrected to deny a phrase
    and still contained it: count the instruction, never the phrase.

    `ast` sees attribute access and does not see docstrings or comments.
    """
    found: dict[str, set[str]] = {}
    for path in sorted(TESTS_DIR.rglob("test_*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:  # pragma: no cover - a broken test file is its own failure
            continue
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Attribute)
                and node.value.attr == "destinations"
                and isinstance(node.value.value, ast.Name)
                and node.value.value.id == _MOCK_NAME
            ):
                found.setdefault(node.attr, set()).add(path.name)
    return found if with_files else set(found)
