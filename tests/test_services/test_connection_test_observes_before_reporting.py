"""core#978 + core#979 — a Test Connection that reports success must have
*observed* something it did not create.

Two defects on the zero-credentials onboarding path, both of which the existing
suite is green through:

* **core#979 — `sqlite` creates the database it reports finding.** SQLite's
  semantics are open-or-create, so `sqlite:///{path}` succeeds against a path
  that holds nothing, returns ``ok=True, "Connected successfully"``, **and leaves
  a new empty database behind.** The user's evidence that the path is right is
  the artifact the check just fabricated. Test Connection runs in the *web*
  process while the extract runs in *celery*, so a correct volume plus a mistyped
  filename still yields a green check and a run that finds no tables.
* **core#978 — `duckdb` can never succeed.** `test_connection` passes
  ``connect_timeout=5`` to every non-SQLite, non-Oracle, non-MSSQL dialect;
  `duckdb.connect()` accepts only ``(database, read_only, config)`` and raises
  ``TypeError``. The generic handler then tells the user to *"check your
  credentials and network settings"* — for a local file, with no credentials and
  no network.

## The invariant, and why it is stated as ONE test with two halves

The obvious pair of tests is *"an absent database must not report success"* and
*"an existing one must"*. Split, the first one **passes vacuously against
`duckdb`**, because a connector that can never succeed trivially never succeeds
wrongly. Asserting both halves in one test makes the "existing database
connects" half the anti-vacuity guard for the "absent one does not" half:

    reporting success  =>  something was observed  AND  it was not created here

Neither defect can hide behind the other, and neither param can go green by
getting worse.

## 🚨 core#979's negative control is the hard half — read this before editing

**The natural way to write the sqlite regression test passes before AND after the
fix.** Point sqlite at `<tmp>/no_such_dir/x.db` and the *unfixed* code already
returns ``False`` and creates nothing, because the missing **directory** — not
the missing database — is what stops it. A test written that way never had the
power to detect the defect, and would have sat green through the whole ~5 months
this bug has been shipping.

So :func:`test_the_naive_probe_is_uninformative` states that explicitly and
:class:`TestTheProbeDirectoryIsArmed` proves the directory used by the real
regression test *would* have permitted creation. Without those two, the
assertions below are a coin whose faces are both heads.

## Why these are `xfail(strict=True)` and not simply red

Engineering owns the fixes; this file is the test that would have caught them,
landed **before** them so it cannot be written to match whatever the fix happens
to do. `strict=True` is the coupling: the moment either defect is fixed the
corresponding test XPASSes, which pytest reports as a **failure**, so the fix PR
cannot merge without deleting the marker. That is "ship the test with the fix"
enforced by the tooling rather than by memory.

⚠️ **When you clear one of these markers, mutate the fix back and confirm the
test goes red for the stated reason.** A strict xfail has been satisfied by a
broken harness in this repo before, and the marker hides it.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from datanika.models.connection import ConnectionType
from datanika.services.connection_service import ConnectionService, _build_sa_url

# --------------------------------------------------------------------------- #
# Derivation — which connector types resolve a LOCAL FILESYSTEM path
# --------------------------------------------------------------------------- #

_SENTINEL = "datanika_local_path_sentinel"

#: Enough keys to get every type past its early returns and into `_build_sa_url`.
#: `host` is deliberately non-empty so a networked type cannot look host-less and
#: be mistaken for a local one.
_PROBE_CONFIG = {
    "path": _SENTINEL,
    "database": _SENTINEL,
    "host": "db.example.invalid",
    "port": "1",
    "user": "u",
    "password": "p",
    "project": "proj",
    "dataset": "ds",
    "account": "acct",
    "http_path": "/sql/1.0/w/x",
    "token": "t",
    "catalog": "main",
    "schema": "s",
}


def local_path_types() -> list[ConnectionType]:
    """Types whose SQLAlchemy URL embeds a local filesystem path.

    Derived from `_build_sa_url`, not remembered: a URL with **no host** whose
    database component is the sentinel we handed in is a path on this machine.
    Everything networked keeps a host, so `postgres`, `mysql`, `bigquery`,
    `databricks`, `snowflake` and friends fall out on their own.

    `csv`/`json`/`parquet` also resolve local paths but never reach
    `_build_sa_url` — they go through the dlt filesystem branch, which is
    core#979's remaining scope and a different code path.
    """
    found = []
    for ct in ConnectionType:
        try:
            url_text = _build_sa_url(dict(_PROBE_CONFIG), ct)
        except (ValueError, KeyError):
            continue  # not a SQLAlchemy-URL type at all
        try:
            url = make_url(url_text)
        except ArgumentError:  # pragma: no cover - a malformed URL is a different bug
            continue
        if not url.host and url.database and _SENTINEL in url.database:
            found.append(ct)
    return found


#: The two defects, each pinned to its issue. Keeping them in one place means the
#: xfail reason, the parametrisation and the control below cannot drift apart.
KNOWN_BROKEN: dict[ConnectionType, str] = {
    ConnectionType.SQLITE: (
        "core#979 — reports 'Connected successfully' for a path that holds no "
        "database, and creates the database while doing so"
    ),
    ConnectionType.DUCKDB: (
        "core#978 — connect_timeout is passed to a dialect that rejects it, so no input can succeed"
    ),
}


def _param(ct: ConnectionType):
    reason = KNOWN_BROKEN.get(ct)
    marks = [pytest.mark.xfail(reason=reason, strict=True, raises=AssertionError)] if reason else []
    return pytest.param(ct, marks=marks, id=ct.value)


def _make_database(ct: ConnectionType, path: Path) -> None:
    """Create a real, non-empty database of the given kind at `path`."""
    if ct is ConnectionType.SQLITE:
        conn = sqlite3.connect(path)
        conn.execute("create table observed (x integer)")
        conn.commit()
        conn.close()
    elif ct is ConnectionType.DUCKDB:
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(str(path))
        con.execute("create table observed (x integer)")
        con.close()
    else:  # pragma: no cover - the derivation currently yields only these two
        raise AssertionError(f"no builder for {ct}; add one rather than skipping")
    assert path.is_file() and path.stat().st_size > 0, f"failed to build a real {ct.value} db"


# --------------------------------------------------------------------------- #
# Arming — green today AND after the fix. If these ever fail, nothing below
# means what it says.
# --------------------------------------------------------------------------- #


class TestTheDerivationIsArmed:
    def test_it_finds_exactly_the_local_path_types(self) -> None:
        got = set(local_path_types())
        assert got == {ConnectionType.SQLITE, ConnectionType.DUCKDB}, (
            f"local_path_types() returned {sorted(t.value for t in got)}. Every "
            "parametrised assertion below is derived from this, so a wrong or empty "
            "answer makes them vacuous. If a connector was added or its URL shape "
            "changed, extend KNOWN_BROKEN/_make_database rather than narrowing this."
        )

    def test_a_networked_type_is_not_mistaken_for_a_local_one(self) -> None:
        """Negative control on the derivation's discriminator."""
        assert ConnectionType.POSTGRES not in local_path_types()
        url = make_url(_build_sa_url(dict(_PROBE_CONFIG), ConnectionType.POSTGRES))
        assert url.host, "postgres lost its host — the derivation's discriminator is gone"

    def test_every_known_broken_type_is_actually_a_local_path_type(self) -> None:
        unknown = sorted(t.value for t in KNOWN_BROKEN if t not in local_path_types())
        assert not unknown, (
            f"KNOWN_BROKEN names {unknown}, which the derivation does not classify as "
            "local-path connectors. A pin that matches nothing stops covering anything "
            "and stops being checked."
        )


class TestTheProbeDirectoryIsArmed:
    """🚨 The control that makes core#979's regression test mean anything.

    If the directory were absent or unwritable, the *unfixed* code also returns
    False and creates nothing — so the regression test would pass before and
    after the fix. Prove the directory permits creation before relying on the
    fact that nothing was created in it.
    """

    def test_the_directory_exists_and_is_writable(self, tmp_path: Path) -> None:
        assert tmp_path.is_dir()
        assert os.access(tmp_path, os.W_OK)

    def test_sqlite_really_would_have_created_a_file_here(self, tmp_path: Path) -> None:
        """Not an assumption about permissions — a demonstration."""
        sentinel = tmp_path / "sqlite_can_create_here.db"
        assert not sentinel.exists()
        sqlite3.connect(sentinel).close()
        assert sentinel.exists(), (
            "sqlite could NOT create a database in the probe directory. Every "
            "'the check did not create a file' assertion below is therefore satisfied "
            "by the filesystem rather than by the code under test."
        )
        sentinel.unlink()


def test_the_naive_probe_is_uninformative(tmp_path: Path) -> None:
    """core#979's trap, made executable — and it stays true after the fix.

    A regression test that points sqlite at a path inside a **missing directory**
    passes against the unfixed code, because the absent directory stops the
    open-or-create before the absent database ever matters. It is green before
    the fix and green after it: a test with no power to detect the thing it is
    named for.

    This is the shape that let core#979 ship for months, so it is asserted rather
    than described. It is a *control*, not a second regression test — it must keep
    passing.
    """
    doomed = tmp_path / "no_such_directory" / "x.db"
    assert not doomed.parent.exists()

    ok, _msg = ConnectionService.test_connection({"path": str(doomed)}, ConnectionType.SQLITE)

    assert ok is False, (
        "The unfixed code is expected to fail here, for the WRONG reason — the "
        "missing directory. If this ever returns True the trap has changed shape and "
        "the reasoning in this file needs re-deriving."
    )
    assert not doomed.exists()


# --------------------------------------------------------------------------- #
# The invariant
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("connection_type", [_param(ct) for ct in local_path_types()])
def test_success_means_it_observed_something_it_did_not_create(
    connection_type: ConnectionType, tmp_path: Path
) -> None:
    """Reporting success must mean a database was *found*, not made.

    Both halves are asserted together on purpose. The first rules out the
    degenerate reading of the second: a connector that can never succeed satisfies
    "never succeeds wrongly" for free, which is precisely core#978's state.
    """
    # --- half 1: an existing database connects (this is what core#978 breaks) ---
    real = tmp_path / f"real.{connection_type.value}"
    _make_database(connection_type, real)
    ok_real, msg_real = ConnectionService.test_connection({"path": str(real)}, connection_type)
    assert ok_real is True, (
        f"{connection_type.value}: a real database at {real} did not connect: {msg_real!r}. "
        "Until this passes, the assertion below is satisfied by a connector that cannot "
        "succeed at all rather than by one that checks."
    )

    # --- half 2: an absent one does not, and is not brought into existence ---
    absent = tmp_path / f"absent.{connection_type.value}"
    assert not absent.exists()
    ok_absent, msg_absent = ConnectionService.test_connection(
        {"path": str(absent)}, connection_type
    )
    assert ok_absent is False, (
        f"{connection_type.value}: reported {msg_absent!r} for a path holding no database. "
        "The directory is writable (proved by TestTheProbeDirectoryIsArmed), so this is "
        "open-or-create semantics being read as a successful connection."
    )
    assert not absent.exists(), (
        f"{connection_type.value}: the check CREATED {absent}. The user's evidence that "
        "the path is right is then the artifact the check fabricated — and the extract, "
        "which runs in a different process, will find no tables."
    )


@pytest.mark.xfail(
    reason=KNOWN_BROKEN[ConnectionType.SQLITE] + " (core#979 AC2: the two failure modes)",
    strict=True,
    raises=AssertionError,
)
def test_absent_and_unopenable_are_distinguishable(tmp_path: Path) -> None:
    """core#979 AC2 — they call for different user actions, so they must read differently.

    Measured 2026-09-03 on the unfixed code: *both* return the identical
    ``"Connection failed — check your credentials and network settings:
    (sqlite3.OperationalError) unable to open database file"``. A user told that
    has no way to know whether to create the database, fix the path, or fix a
    permission.

    ⚠️ **The precondition below is load-bearing and was added after this test
    XPASSed.** Written as a bare ``msg_absent != msg_unopenable`` it *passes
    today* — because the absent path does not produce a failure message at all,
    it produces ``"Connected successfully"``. Two strings differing is not two
    failure modes differing, and the test would have been green for a reason
    unrelated to the property it is named for. Assert that both are failures
    first, and the comparison starts meaning something.
    """
    absent = tmp_path / "not_here.db"
    unopenable = tmp_path  # a directory is not a database file

    ok_absent, msg_absent = ConnectionService.test_connection(
        {"path": str(absent)}, ConnectionType.SQLITE
    )
    ok_unopenable, msg_unopenable = ConnectionService.test_connection(
        {"path": str(unopenable)}, ConnectionType.SQLITE
    )

    assert ok_absent is False and ok_unopenable is False, (
        f"Both cases must be failures before their messages can be compared: "
        f"absent -> ok={ok_absent!r} {msg_absent!r}; unopenable -> ok={ok_unopenable!r}. "
        "core#979's primary fix has to land before this criterion is even reachable."
    )
    assert msg_absent != msg_unopenable, (
        f"Both failure modes report {msg_absent!r}. 'There is no database at that path' "
        "and 'that path cannot be opened' are different problems with different fixes."
    )


# --------------------------------------------------------------------------- #
# core#978 — the three config spellings, and the message
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("spelling", ["path", "database", "memory"])
@pytest.mark.xfail(reason=KNOWN_BROKEN[ConnectionType.DUCKDB], strict=True, raises=AssertionError)
def test_duckdb_connects_through_every_supported_spelling(spelling: str, tmp_path: Path) -> None:
    """core#978 AC1 — all three, because the config has two spellings and the
    published guide uses a file.

    `duckdb` is the zero-credentials onboarding path: the connector guide tells a
    new user to pick it when they have no warehouse. The first action they take is
    Test Connection.
    """
    if spelling == "memory":
        config = {"path": ":memory:"}
    else:
        real = tmp_path / "real.duckdb"
        _make_database(ConnectionType.DUCKDB, real)
        config = {spelling: str(real)}

    ok, msg = ConnectionService.test_connection(config, ConnectionType.DUCKDB)
    assert ok is True, f"duckdb via {spelling!r} did not connect: {msg!r}"


def test_the_duckdb_dbapi_really_rejects_connect_timeout() -> None:
    """The mechanism, asserted against the real consumer rather than the source.

    Green today and green after the fix — it is a statement about `duckdb`, not
    about us, and it is what makes core#978's fix a dialect question rather than
    a style preference. ⚠️ `duckdb.connect` is a pybind11 builtin with **no
    introspectable signature**, so `inspect.signature` cannot answer this; the
    only honest check is to call it.
    """
    duckdb = pytest.importorskip("duckdb")
    with pytest.raises(TypeError) as exc:
        duckdb.connect(":memory:", connect_timeout=5)
    assert "incompatible function arguments" in str(exc.value)


@pytest.mark.parametrize("connection_type", local_path_types(), ids=lambda t: t.value)
@pytest.mark.xfail(
    reason=(
        "core#978 AC4 — the generic handler tells the user to 'check your credentials "
        "and network settings' for a file-backed connector that has neither. Note this "
        "is a SEPARATE acceptance criterion from the connect_timeout fix: correcting "
        "the connect args alone leaves this message exactly as it is."
    ),
    strict=True,
    raises=AssertionError,
)
def test_a_file_backed_failure_never_blames_credentials_or_the_network(
    connection_type: ConnectionType, tmp_path: Path
) -> None:
    """core#978 AC4 — assert the message, not only the boolean.

    ``"check your credentials and network settings"`` is advice, and for a local
    file with neither it is advice that sends the user to look at things that do
    not exist. The misdirection is half the cost of core#978: the connector we
    recommend precisely *because* it needs no credentials fails by telling you to
    check your credentials.

    ⚠️ This is asserted on a failure the connector should still have — a path that
    cannot be opened at all — so it stays meaningful after core#979 makes absent
    paths fail too.
    """
    _, msg = ConnectionService.test_connection({"path": str(tmp_path)}, connection_type)
    lowered = msg.lower()
    for banned in ("credential", "network"):
        assert banned not in lowered, (
            f"{connection_type.value} is file-backed and has no {banned}s, but its failure "
            f"message says: {msg!r}"
        )
