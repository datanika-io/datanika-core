"""Advertising a connector is a claim. This asks whether we can execute it.

[core#885]. `s3` was offered in the picker, given a first-class `endpoint_url`
form field, documented at `datanika.io/docs/connectors/s3/` and cross-linked from
six other connector pages, while `s3fs` was absent from `uv.lock` — so every run
died with `ImportError: Install s3fs to access S3`. It stayed that way for four
months and was found by a person reading a docstring, not by a build.

## Why every existing guard was green through it

`tests/test_services/test_supported_sets_resolve.py` already does exactly the
right thing — *ask the layer beneath* — on **two** axes: SQLAlchemy dialects for
SQL sources, and `dlt.destinations` factories for destinations.
`test_connector_type_contracts.py` adds a third, dbt adapters. All three resolve
a *destination* or a *SQL* connector.

**Nothing interrogates fsspec.** File and blob sources reach `filesystem()` with
the user's `bucket_url` passed straight through, and the scheme is parsed by
fsspec at runtime; the `s3 -> s3fs` relation is written down only in prose. So
the one axis with no guard is the axis the defect was on, and that is not a
coincidence — it is where the mapping was never tabulated.

`TestDeferredCapability` in `tests/test_security/test_dependency_advisories.py`
is a good guard pointed the other way: it fires the day the blocker LIFTS, i.e.
when the news is good. It cannot answer *"are we still selling it while we
can't?"* Those two questions fail at opposite moments.

And the four `TestS3FileSourceMovesRows` tests that would have caught it at
runtime are `skipif`-ed on `_s3fs_available()` — **disabled by the same condition
that breaks the capability**. A skip is the same colour as a pass, so the suite
went quiet exactly when it should have got louder (`docs/QA_RULES.md` §11).

## What this file asserts

1. Every advertised source type is dispatched by *some* loader family, where the
   families are read out of `build_source`'s own dispatch chain rather than
   restated. A connector added to `SOURCE_TYPES` with no branch to run it is red.
2. Every advertised **file** source's fsspec protocol resolves in the
   environment the suite is running in — asked by calling fsspec, not by reading
   a manifest.

Withdrawal is the exemption, and it is the one that already exists:
`WITHDRAWN_SOURCE_TYPES` removes a type from `SOURCE_TYPES`, so an exempted
connector is not asserted here. That is deliberate — a second exemption list
would be a second thing to keep true. The exemption is visible in source, names
its issue (`s3` -> core#863), and cannot be satisfied by deleting the connector:
`test_a_withdrawn_type_still_resolves_for_stored_connections` in
`tests/test_connector_type_contracts.py` fails if the `ConnectionType` member
goes away.

## ⚠️ This file's verdict differs between your worktree and CI, on purpose

`s3fs` is absent from `uv.lock` (0 occurrences) but is still installed in every
worktree venv created before core#825 removed it, because we all export
`UV_NO_SYNC=1` and that makes a venv a *superset* of the lock, never a subset
(`plans/WORKFLOW_RULES.md` §13 trap 18b). So `fsspec.get_filesystem_class("s3")`
resolves locally and raises in CI.

CI is the environment that matters, and the failure messages below say so. If you
want a CI-truthful local run:

    uv pip uninstall --python "$(pwd)/.venv/Scripts/python.exe" s3fs

**The discriminating power of this guard is therefore NOT staked on s3.**
`TestTheResolverCanActuallyFail` uses an invented protocol that neither
environment can resolve, so the checker is shown able to fail on every run, in
every venv, forever — rather than once, by me, on a day I remembered to look.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import textwrap

import fsspec
import pytest

from datanika.services import dlt_runner as dlt_runner_module
from datanika.services.connection_service import SOURCE_TYPES, WITHDRAWN_SOURCE_TYPES
from datanika.services.dlt_runner import (
    SOURCE_DRIVERNAME_MAP,
    SUPPORTED_FILE_TYPES,
    DltRunnerService,
)

#: The fsspec protocol each file/blob source type's ``bucket_url`` carries.
#:
#: This is the table whose absence is core#885. ``csv``/``json``/``parquet`` are
#: *formats* read from a local path, so their protocol is fsspec's built-in
#: ``file``; ``s3`` is the only scheme-bearing member today and the only one
#: whose implementation ships as a separate distribution.
#:
#: ⚠️ Keep this keyed by CONNECTOR TYPE, not by protocol. `BLOCKED_BY_S3FS_CONFLICT`
#: in `tests/test_security/test_dependency_advisories.py` is keyed by *package*
#: (`s3fs`), which is why nothing there could ever join up to `ConnectionType.S3`
#: — the connector appears in that file only inside prose.
FILE_SOURCE_PROTOCOLS: dict[str, str] = {
    "csv": "file",
    "json": "file",
    "parquet": "file",
    "s3": "s3",
}

#: A protocol no fsspec installation can resolve. Used only by the permanent
#: negative control below. Deliberately not a plausible-but-absent real protocol
#: (`gs`, `az`): those resolve in some venvs here, which would make the control
#: itself environment-dependent — the exact defect this file is about.
_UNRESOLVABLE_PROTOCOL = "datanika-no-such-protocol"


def unresolvable_protocols(types: set[str], protocols: dict[str, str]) -> dict[str, str]:
    """Return ``{connector_type: reason}`` for every type fsspec cannot serve.

    Asks fsspec, not the lockfile. `fsspec.get_filesystem_class` is the call
    `filesystem()` makes underneath, so a type that fails here fails in the
    product, for the same reason, with the same message.
    """
    failures: dict[str, str] = {}
    for connector_type in sorted(types):
        protocol = protocols.get(connector_type)
        if protocol is None:
            failures[connector_type] = "no protocol declared in FILE_SOURCE_PROTOCOLS"
            continue
        try:
            fsspec.get_filesystem_class(protocol)
        except Exception as exc:  # ImportError in practice; fsspec also raises ValueError
            failures[connector_type] = f"{protocol}:// -> {type(exc).__name__}: {exc}"
    return failures


def dispatch_families() -> dict[str, set[str]]:
    """Read the loader families out of ``build_source``'s own dispatch chain.

    Derived, not restated. ``build_source`` is a chain of
    ``if connection_type in SUPPORTED_<X>_TYPES:`` branches with a fallthrough to
    the SQL path keyed on ``SOURCE_DRIVERNAME_MAP``. Parsing the chain means a
    family added tomorrow joins this guard the day it is written, which a
    hand-listed union does not — and a hand-listed union is precisely how the
    withdrawal contract test came to be SaaS-shaped (core#863).
    """
    # ⚠️ `dedent` is load-bearing. `inspect.getsource` on a METHOD returns it
    # still indented at class level, and `ast.parse` raises IndentationError on
    # that — which, inside a try/except or a strict xfail, is an assertion that
    # never runs while the test reports fine. That exact shape is recorded in
    # `plans/current_state.md`; it is caught here only because
    # `test_the_dispatch_chain_was_actually_parsed` runs the parse for real.
    tree = ast.parse(textwrap.dedent(inspect.getsource(DltRunnerService.build_source)))
    names: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare) or len(node.ops) != 1:
            continue
        if not isinstance(node.ops[0], ast.In):
            continue
        if not (isinstance(node.left, ast.Name) and node.left.id == "connection_type"):
            continue
        target = node.comparators[0]
        if isinstance(target, ast.Name):
            names.append(target.id)
        elif isinstance(target, ast.Attribute):  # self.SUPPORTED_SOURCE_TYPES
            names.append(target.attr)

    families: dict[str, set[str]] = {}
    for name in names:
        value = getattr(dlt_runner_module, name, None)
        if value is None:
            value = getattr(DltRunnerService, name, None)
        if isinstance(value, set):
            families[name] = value

    # The SQL fallthrough is not an `in` test — `build_source` reaches it by
    # exhausting the chain, and the type is resolved via SOURCE_DRIVERNAME_MAP
    # inside the credentials builder. Named explicitly because the AST cannot
    # see it, and named LAST so the assertion below still fails if the scan
    # silently found nothing else.
    families["SOURCE_DRIVERNAME_MAP"] = set(SOURCE_DRIVERNAME_MAP)
    return families


class TestTheResolverCanActuallyFail:
    """The negative control, shipped rather than performed once.

    `docs/QA_RULES.md` §2: any green you have not personally forced red is
    unproven. Making the control a permanent test means the proof is re-run on
    every CI run instead of resting on a sentence in a PR body — and because it
    uses an invented protocol, it holds in every venv regardless of what
    `UV_NO_SYNC=1` has left lying around.
    """

    def test_an_unresolvable_protocol_is_reported(self):
        failures = unresolvable_protocols({"invented"}, {"invented": _UNRESOLVABLE_PROTOCOL})
        assert set(failures) == {"invented"}, (
            "unresolvable_protocols() did not report a protocol fsspec cannot resolve, "
            "so every green below this line is meaningless"
        )

    def test_a_resolvable_protocol_is_not_reported(self):
        assert unresolvable_protocols({"local"}, {"local": "file"}) == {}, (
            "unresolvable_protocols() reported fsspec's built-in local filesystem as "
            "broken, so it fails on everything and attributes nothing"
        )

    def test_a_type_with_no_declared_protocol_is_reported(self):
        failures = unresolvable_protocols({"undeclared"}, {})
        assert set(failures) == {"undeclared"}, (
            "a file source with no entry in FILE_SOURCE_PROTOCOLS was silently treated "
            "as fine — that is the hole core#885 is about, one level up"
        )


class TestEveryAdvertisedFileSourceResolves:
    def test_the_protocol_map_covers_every_file_source_type(self):
        """Adding a file connector without declaring its protocol must be red.

        Both directions: an undeclared type would be skipped by the walk below
        (silent), and a stale entry means the map is describing a connector that
        no longer exists.
        """
        undeclared = SUPPORTED_FILE_TYPES - set(FILE_SOURCE_PROTOCOLS)
        assert not undeclared, (
            f"{sorted(undeclared)} are in SUPPORTED_FILE_TYPES with no entry in "
            "FILE_SOURCE_PROTOCOLS, so nothing checks that fsspec can serve them"
        )
        stale = set(FILE_SOURCE_PROTOCOLS) - SUPPORTED_FILE_TYPES
        assert not stale, (
            f"{sorted(stale)} are declared in FILE_SOURCE_PROTOCOLS but are no longer "
            "in SUPPORTED_FILE_TYPES — drop them rather than leaving the map describing "
            "connectors that do not exist"
        )

    def test_every_advertised_file_source_protocol_resolves(self):
        """The core assertion of core#885.

        `SOURCE_TYPES` already excludes withdrawn types, so `s3` is not asserted
        while it is withdrawn — the exemption is `WITHDRAWN_SOURCE_TYPES` and it
        is visible in `connection_service.py` naming core#863. Restore `s3` to
        `SOURCE_TYPES` without restoring `s3fs` and this goes red.
        """
        advertised = SUPPORTED_FILE_TYPES & SOURCE_TYPES
        assert advertised, (
            "no file source type is advertised at all. That is either a real "
            "withdrawal of every one of them, or SOURCE_TYPES/SUPPORTED_FILE_TYPES "
            "stopped agreeing — and this guard would otherwise pass vacuously, which "
            "is how a guard gets satisfied by the feature being deleted"
        )
        failures = unresolvable_protocols(advertised, FILE_SOURCE_PROTOCOLS)
        assert not failures, (
            "advertised file source types whose fsspec protocol does not resolve here: "
            f"{failures}. We offer these in the picker, render a config form for them "
            "and document them, and the shipped image cannot run them (core#885).\n"
            "⚠️ If this is red locally but green in CI, or vice versa: `s3fs` is absent "
            "from uv.lock but still installed in pre-core#825 worktree venvs, because "
            "UV_NO_SYNC=1 makes a venv a superset of the lock (WORKFLOW_RULES §13 trap "
            "18b). CI is the environment that counts."
        )

    def test_a_withdrawn_file_source_is_not_asserted_but_is_still_declared(self):
        """Withdrawal exempts a type from the walk; it must not erase it.

        The failure this prevents is the cheap fix: dropping `s3` out of
        `FILE_SOURCE_PROTOCOLS` and `SUPPORTED_FILE_TYPES` too, which would make
        the guard green by making the connector unmentionable — and would take
        the four `TestS3FileSourceMovesRows` tests' subject with it.
        """
        withdrawn_file_types = WITHDRAWN_SOURCE_TYPES & SUPPORTED_FILE_TYPES
        for connector_type in withdrawn_file_types:
            assert connector_type not in SOURCE_TYPES, (
                f"{connector_type} is withdrawn but still advertised in SOURCE_TYPES"
            )
            assert connector_type in FILE_SOURCE_PROTOCOLS, (
                f"{connector_type} is withdrawn and its protocol declaration was deleted. "
                "Keep it: the declaration is what makes the restoration checkable, and "
                "core#863 requires the four requires_s3fs tests to pass UNMODIFIED when "
                "the transport comes back."
            )


class TestEveryAdvertisedSourceTypeIsDispatched:
    """A connector nothing can run is the general form of core#885.

    The fsspec walk above covers one family. This covers the join: every
    advertised type must land on some branch of `build_source`. Without it, a
    connector added to `SOURCE_TYPES` in a family that has no guard is invisible
    to all of them — which is the shape that let `s3` sit for four months.
    """

    def test_the_dispatch_chain_was_actually_parsed(self):
        """Floor first, so a broken AST scan cannot read as a clean sweep.

        `test_supported_sets_resolve.py` establishes this ordering: assert the
        walk found something before asserting it found no failures. An AST scan
        that matches nothing would report every advertised type as undispatched
        and send the reader hunting through `SOURCE_TYPES`.
        """
        families = dispatch_families()
        assert len(families) >= 8, (
            f"only {len(families)} dispatch families were parsed out of "
            f"build_source ({sorted(families)}). The dispatch chain has been "
            "restructured and this scan no longer reads it — fix the scan; do not "
            "delete the assertions below, which are now measuring nothing."
        )
        assert sum(len(v) for v in families.values()) >= 25, (
            "the parsed families are nearly empty, so the union below cannot "
            "attribute anything to a single connector"
        )

    def test_every_advertised_source_type_has_a_loader(self):
        families = dispatch_families()
        dispatched: set[str] = set().union(*families.values())
        orphans = SOURCE_TYPES - dispatched
        assert not orphans, (
            f"{sorted(orphans)} are advertised in SOURCE_TYPES but no branch of "
            f"DltRunnerService.build_source dispatches them, so an upload raises "
            f"'Unsupported source type'. Families parsed: {sorted(families)}."
        )

    def test_a_withdrawn_type_keeps_its_dispatch(self):
        """The other half, asserted against the DERIVED union.

        `tests/test_connector_type_contracts.py` asserts this too and used to do
        it against `SUPPORTED_SAAS_TYPES` alone, because the only withdrawal that
        had ever happened was `google_ads`. Withdrawing `s3` failed it for the
        wrong reason. It now names two sets by hand; this one derives them, so a
        third family costs nothing.
        """
        families = dispatch_families()
        dispatched: set[str] = set().union(*families.values())
        lost = WITHDRAWN_SOURCE_TYPES - dispatched
        assert not lost, (
            f"{sorted(lost)} are withdrawn AND undispatched, so a connection someone "
            "already stored fails with a generic 'Unsupported source type' instead of "
            "an error that explains itself (core#555)."
        )


@pytest.mark.parametrize("connector_type", sorted(SUPPORTED_FILE_TYPES))
def test_every_file_source_type_declares_a_protocol_string(connector_type: str):
    """Per-entry, so a failure names the connector rather than the set."""
    protocol = FILE_SOURCE_PROTOCOLS.get(connector_type)
    assert isinstance(protocol, str) and protocol, (
        f"{connector_type} has no fsspec protocol declared; the walk would skip it"
    )


def test_this_guard_is_referenced_where_the_capability_is_withdrawn():
    """The exemption must be findable from the source that grants it.

    core#885's own acceptance says the exemption list is the artifact a human
    reads. `WITHDRAWN_SOURCE_TYPES` is that artifact, so it has to name the
    tracking issue — otherwise "why is s3 not here?" costs a git-blame.
    """
    source = pathlib.Path(
        inspect.getfile(__import__("datanika.services.connection_service", fromlist=["x"]))
    ).read_text(encoding="utf-8")
    block = source.split("WITHDRAWN_SOURCE_TYPES")[0][-4000:]
    for connector_type in WITHDRAWN_SOURCE_TYPES:
        assert connector_type in block, (
            f"{connector_type} is withdrawn but the comment above "
            "WITHDRAWN_SOURCE_TYPES does not mention it"
        )
    if WITHDRAWN_SOURCE_TYPES:
        assert "core#" in block, (
            "the withdrawal comment names no tracking issue, so the exemption cannot "
            "be re-derived by the next reader"
        )
