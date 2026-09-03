"""Test Connection on a local-file source (core#978, core#979).

Implements `docs/specs/SPEC_LOCAL_FILE_CONNECTIONS.md` §5. Product measured the
contract; this is its executable form.

Three defects, each with a different failure signature and none of which the
other two share:

* **core#978 — DuckDB could never succeed, on any input.** `connect_args`
  carried `{"connect_timeout": 5}` for every dialect except mssql, oracle and
  sqlite. `duckdb.connect()` accepts only `(database, read_only, config)` and
  raises `TypeError: connect(): incompatible function arguments`; the generic
  `except` caught it and told the user to *check your credentials and network
  settings* — for a local file, on the connector our own docs recommend to
  someone who has **no warehouse and no credentials**.
* **core#979 — SQLite created the database it reported finding.** Open-or-create
  means "connected" is evidence the *directory* is writable, not that the
  database exists. The user's evidence that the path was right was the artifact
  the check had just fabricated.
* **The misdirection is the whole local-file class, not one connector.** Product
  measured sqlite's *failure* message as the same
  `check your credentials and network settings` string. That is why AC5 is
  asserted over the message **set**.

🚨 **Every verdict below is a real connect against a real file on disk.** These
two dialects are the only ones in the tree that can be exercised end to end with
no external service, which is exactly why a defect that made one of them
impossible to use survived.
"""

import pathlib

import pytest
from sqlalchemy import create_engine, text

from datanika.models.connection import ConnectionType
from datanika.services.connection_service import (
    _IN_MEMORY_PATHS,
    _LOCAL_FILE_DB_TYPES,
    ConnectionService,
    _build_sa_url,
    _connect_args,
)
from datanika.ui.state.connection_state import _VERDICT_KEYS

CREDENTIAL_WORDS = ("credential", "credentials", "network")

#: The five types whose location is a local filesystem path, derived in
#: SPEC_LOCAL_FILE_CONNECTIONS §4 D2 rather than remembered. `s3` is excluded
#: deliberately: its `bucket_url` is remote, so it means the same thing in the
#: web container and in the worker, and its errors legitimately mention
#: credentials.
LOCAL_PATH_TYPES = [
    ConnectionType.SQLITE,
    ConnectionType.DUCKDB,
    ConnectionType.CSV,
    ConnectionType.JSON,
    ConnectionType.PARQUET,
]


@pytest.fixture
def real_duckdb(tmp_path) -> pathlib.Path:
    p = tmp_path / "real.duckdb"
    engine = create_engine(f"duckdb:///{p}")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (a INTEGER)"))
        conn.commit()
    engine.dispose()
    assert p.exists() and p.stat().st_size > 0
    return p


@pytest.fixture
def real_sqlite(tmp_path) -> pathlib.Path:
    p = tmp_path / "real.sqlite"
    engine = create_engine(f"sqlite:///{p}")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (a INTEGER)"))
        conn.commit()
    engine.dispose()
    assert p.exists() and p.stat().st_size > 0
    return p


# --------------------------------------------------------------------------
# AC1 — duckdb succeeds, on all three spellings
# --------------------------------------------------------------------------


def test_duckdb_connects_to_a_real_file(real_duckdb):
    """core#978. Red before the fix on every input there is."""
    ok, msg = ConnectionService.test_connection({"path": str(real_duckdb)}, ConnectionType.DUCKDB)
    assert ok is True, f"duckdb could not open a real .duckdb file: {msg}"


def test_duckdb_connects_through_the_database_key(real_duckdb):
    """The config has two spellings; the guide uses one and the form the other."""
    ok, msg = ConnectionService.test_connection(
        {"database": str(real_duckdb)}, ConnectionType.DUCKDB
    )
    assert ok is True, msg


def test_duckdb_connects_in_memory():
    """`:memory:` must work, and it is the reason read-only is conditional.

    Measured: `duckdb.connect(":memory:", read_only=True)` raises
    `Cannot launch in-memory database in read-only mode`. An in-memory database
    is created fresh on every connect by definition, so there is nothing for a
    read-only open to protect and nothing that could pre-exist.
    """
    ok, msg = ConnectionService.test_connection({"path": ":memory:"}, ConnectionType.DUCKDB)
    assert ok is True, msg


def test_the_sqlite_control_still_passes(real_sqlite):
    """The control that made core#978 a finding rather than a broken probe.

    The same call, the same branch, one line apart in the old `elif` chain.
    """
    ok, msg = ConnectionService.test_connection({"path": str(real_sqlite)}, ConnectionType.SQLITE)
    assert ok is True, msg


# --------------------------------------------------------------------------
# AC2 — the duckdb fix, shown able to fail
# --------------------------------------------------------------------------


def test_restoring_connect_timeout_breaks_duckdb_again(real_duckdb):
    """AC2: shown red first, by reproducing the defect through the public seam.

    A test written after a fix has not been shown able to fail. This one drives
    the *cause* — `connect_timeout` on duckdb — rather than asserting a string,
    so it stays meaningful if the message changes.
    """
    url = _build_sa_url({"path": str(real_duckdb)}, ConnectionType.DUCKDB)
    engine = create_engine(url, connect_args={"connect_timeout": 5})
    with pytest.raises(Exception) as exc, engine.connect() as conn:  # noqa: PT011
        conn.execute(text("SELECT 1"))
    engine.dispose()
    assert "incompatible function arguments" in str(exc.value) or isinstance(
        exc.value, TypeError
    ), (
        "restoring connect_timeout no longer breaks duckdb, so the fix above is "
        f"guarding nothing. Got: {exc.value!r}"
    )


def test_connect_args_gives_no_network_timeout_to_a_local_file_database():
    """D3 as a property, not as a list of carve-outs.

    `connect_timeout` is a *network* parameter. Asserting its absence for the
    whole `_LOCAL_FILE_DB_TYPES` set means the next file-backed dialect is
    covered on the day it is added, which is the failure D3 exists to prevent.
    """
    for ct in _LOCAL_FILE_DB_TYPES:
        args = _connect_args(ct, {"path": "/tmp/x.db"})
        assert "connect_timeout" not in args, f"{ct.value} was handed a network timeout: {args}"


def test_connect_args_still_bounds_a_network_dialect():
    """The false-positive control: removing the timeout everywhere would satisfy
    the assertion above and leave every real database hanging on a dead host."""
    for ct, expected in [
        (ConnectionType.POSTGRES, "connect_timeout"),
        (ConnectionType.MYSQL, "connect_timeout"),
        (ConnectionType.MSSQL, "login_timeout"),
        (ConnectionType.ORACLE, "tcp_connect_timeout"),
    ]:
        args = _connect_args(ct, {})
        assert expected in args, f"{ct.value} lost its connect bound: {args}"
        assert args[expected] == 5


# --------------------------------------------------------------------------
# AC3 / AC4 — sqlite must not manufacture its own evidence
# --------------------------------------------------------------------------


def test_sqlite_on_a_missing_path_fails_and_creates_nothing(tmp_path):
    """core#979. **Assert the second half** — the boolean alone passes on an
    implementation that still writes."""
    missing = tmp_path / "not-there.sqlite"
    assert not missing.exists()

    ok, msg = ConnectionService.test_connection({"path": str(missing)}, ConnectionType.SQLITE)

    assert ok is False, f"a path with no database at it reported {ok!r}: {msg}"
    assert not missing.exists(), (
        "Test Connection created the database it then reported on. The user's "
        "evidence that the path was right is the artifact the check fabricated."
    )


def test_duckdb_on_a_missing_path_fails_and_creates_nothing(tmp_path):
    """The same property on the other local-file dialect.

    duckdb is open-or-create too — measured: without `read_only=True` it creates
    the file. core#979 was filed against sqlite only.
    """
    missing = tmp_path / "not-there.duckdb"
    assert not missing.exists()

    ok, msg = ConnectionService.test_connection({"path": str(missing)}, ConnectionType.DUCKDB)

    assert ok is False, f"a path with no database at it reported {ok!r}: {msg}"
    assert not missing.exists(), "duckdb's Test Connection created the database"


def test_a_real_sqlite_file_still_passes_and_is_not_modified(real_sqlite):
    """AC4's positive control, plus D1's actual claim.

    Read-only is not only about the missing-file case: a *check* must be
    incapable of writing at all. Comparing mtime and size is what distinguishes
    "it opened read-only" from "it opened and happened not to write".
    """
    before = (real_sqlite.stat().st_mtime_ns, real_sqlite.stat().st_size)
    ok, msg = ConnectionService.test_connection({"path": str(real_sqlite)}, ConnectionType.SQLITE)
    assert ok is True, msg
    after = (real_sqlite.stat().st_mtime_ns, real_sqlite.stat().st_size)
    assert before == after, "the check modified the database it was checking"


def test_missing_and_unopenable_are_distinguishable(tmp_path):
    """AC4. They call for different user actions, so they need different words.

    🚨 **The driver cannot tell them apart.** Measured: SQLite returns the
    identical `unable to open database file` for a path with nothing at it and
    for a path whose directory cannot be read. So the distinction has to come
    from an explicit existence check *before* the open — which is why read-only
    alone does not close core#979: it yields the right boolean with the wrong
    sentence.
    """
    missing = ConnectionService.test_connection_verdict(
        {"path": str(tmp_path / "nothing.sqlite")}, ConnectionType.SQLITE
    )

    # A path that exists but is not a database: openable as a file, not as a DB.
    not_a_db = tmp_path / "junk.sqlite"
    not_a_db.write_bytes(b"this is not a sqlite database" * 64)
    junk = ConnectionService.test_connection_verdict({"path": str(not_a_db)}, ConnectionType.SQLITE)

    assert missing.ok is False and junk.ok is False

    # ⚠️ Asserted on the **reason**, not on the prose. An earlier draft matched
    # the literals "No database" and "Cannot open"; a mutation that merely
    # reworded one message to "Unable to open" turned this red, which is a guard
    # that fires on copy edits and gets deleted the third time it does. The
    # machine-readable half is what the UI branches on, and it is what the user
    # experiences as "a different answer".
    assert missing.reason == "file_missing"
    assert junk.reason == "file_unopenable"
    assert missing.message != junk.message, (
        "'there is no database here' and 'I cannot open this' produce the same "
        f"sentence, so the user cannot tell which to act on: {missing.message!r}"
    )


# --------------------------------------------------------------------------
# AC5 — no file-backed message may say "credentials" or "network"
# --------------------------------------------------------------------------


def _collect_local_file_messages(tmp_path) -> dict[str, str]:
    """Every message the five local-path types can produce, by construction.

    Asserted over the message **set** rather than per call site (AC5), because
    the misdirection was found in two places and filed as one.
    """
    real_sqlite = tmp_path / "ok.sqlite"
    engine = create_engine(f"sqlite:///{real_sqlite}")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (a INTEGER)"))
        conn.commit()
    engine.dispose()

    junk = tmp_path / "junk.duckdb"
    junk.write_bytes(b"not a database" * 64)

    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    cases = {
        "sqlite/missing": ({"path": str(tmp_path / "no.sqlite")}, ConnectionType.SQLITE),
        "sqlite/real": ({"path": str(real_sqlite)}, ConnectionType.SQLITE),
        "sqlite/memory": ({"path": ":memory:"}, ConnectionType.SQLITE),
        "duckdb/missing": ({"path": str(tmp_path / "no.duckdb")}, ConnectionType.DUCKDB),
        "duckdb/junk": ({"path": str(junk)}, ConnectionType.DUCKDB),
        "duckdb/memory": ({"path": ":memory:"}, ConnectionType.DUCKDB),
        "csv/empty-dir": ({"bucket_url": str(empty_dir)}, ConnectionType.CSV),
        "csv/absent-dir": ({"bucket_url": str(tmp_path / "gone")}, ConnectionType.CSV),
        "json/absent-dir": ({"bucket_url": str(tmp_path / "gone")}, ConnectionType.JSON),
        "parquet/absent-dir": ({"bucket_url": str(tmp_path / "gone")}, ConnectionType.PARQUET),
    }
    out = {}
    for label, (config, ct) in cases.items():
        _ok, msg = ConnectionService.test_connection(config, ct)
        out[label] = msg
    return out


def test_the_message_collector_is_armed(tmp_path):
    """Anti-vacuity. An empty set satisfies every "contains no X" assertion."""
    messages = _collect_local_file_messages(tmp_path)
    assert len(messages) == 10, f"expected 10 messages, collected {len(messages)}"
    assert all(m.strip() for m in messages.values()), f"an empty message: {messages}"


def test_no_local_file_message_mentions_credentials_or_a_network(tmp_path):
    """AC5. A file on a local disk has neither, so naming them sends the user to
    check two things that do not exist."""
    offenders = {
        label: msg
        for label, msg in _collect_local_file_messages(tmp_path).items()
        if any(w in msg.lower() for w in CREDENTIAL_WORDS)
    }
    assert not offenders, (
        "these local-file verdicts send the user to check credentials or a network "
        "the connector does not have:\n  "
        + "\n  ".join(f"{k}: {v}" for k, v in sorted(offenders.items()))
    )


def test_s3_keeps_the_credentials_clause():
    """The false-positive control for AC5.

    `s3` is excluded from the local-path class on purpose — its `bucket_url` is
    remote, it really does take credentials, and they really are the likeliest
    cause of a failure. A sweep that stripped the word everywhere would satisfy
    the assertion above and make the *right* advice unavailable where it applies.
    """
    ok, msg = ConnectionService.test_connection(
        {"bucket_url": "s3://datanika-no-such-bucket-98f1/", "aws_access_key_id": "AKIA_BOGUS"},
        ConnectionType.S3,
    )
    assert ok is False
    assert "credentials" in msg.lower(), (
        f"s3 lost its credentials advice, which for s3 is correct: {msg!r}"
    )


# --------------------------------------------------------------------------
# AC8 / D6 — the sentences exist in all nine locales, with their reader
# --------------------------------------------------------------------------


NEW_KEYS = [
    "connections.test_file_found",
    "connections.test_file_missing",
    "connections.test_file_unopenable",
    "connections.test_file_in_memory",
    "connections.test_driver_unavailable",
]


@pytest.mark.parametrize("locale", ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"])
def test_the_verdict_keys_exist_and_keep_their_placeholder(locale):
    """Key parity is enforced elsewhere; these three properties are not.

    The placeholder is substituted **server-side**, so a translation that drops
    `{arg}` silently loses the one piece of information the sentence exists to
    carry, and one that adds a stray `{arg}` to a message with no argument puts
    a literal brace on screen.
    """
    data = _locale("datanika/i18n", locale)
    en = _locale("datanika/i18n", "en")
    for key in NEW_KEYS:
        assert key in data, f"{locale}.json is missing {key}"
        assert data[key].strip(), f"{locale}.json has an empty {key}"
        assert ("{arg}" in data[key]) == ("{arg}" in en[key]), (
            f"{locale}.json's {key} disagrees with en.json about the {{arg}} placeholder"
        )
        if locale != "en":
            assert data[key] != en[key], f"{locale}.json's {key} is the English string verbatim"


def test_every_reason_the_service_produces_has_a_key(tmp_path):
    """The two halves must meet, and each fails in its own direction.

    A **reason with no key** falls back to the service's English, so the fix
    ships nine locales' worth of it — the exact defect D6 names. A **key with no
    reason** is an orphan, and `tests/test_i18n`'s documented remedy for an
    orphan is to delete it, which silently drops nine translations with every
    check green (core#872).
    """
    real = tmp_path / "ok.sqlite"
    engine = create_engine(f"sqlite:///{real}")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (a INTEGER)"))
        conn.commit()
    engine.dispose()
    junk = tmp_path / "junk.sqlite"
    junk.write_bytes(b"nope" * 64)

    produced = {
        ConnectionService.test_connection_verdict(cfg, ct).reason
        for cfg, ct in [
            ({"path": str(tmp_path / "no.sqlite")}, ConnectionType.SQLITE),
            ({"path": str(real)}, ConnectionType.SQLITE),
            ({"path": ":memory:"}, ConnectionType.SQLITE),
            ({"path": str(junk)}, ConnectionType.SQLITE),
        ]
    }
    # `driver_unavailable` cannot be produced with the driver installed, which is
    # the point of it — assert it is mapped rather than pretending to reach it.
    assert produced == {"file_missing", "file_found", "file_in_memory", "file_unopenable"}, (
        f"the local-file branch produces {sorted(produced)}"
    )

    mapped = set(_VERDICT_KEYS)
    assert produced <= mapped, f"unmapped reasons: {sorted(produced - mapped)}"
    assert "driver_unavailable" in mapped

    en = _locale("datanika/i18n", "en")
    for reason, key in _VERDICT_KEYS.items():
        assert key in en, f"reason {reason!r} maps to {key!r}, which en.json does not define"
    assert set(_VERDICT_KEYS.values()) == set(NEW_KEYS)


def test_a_verdict_that_interpolates_carries_its_argument(tmp_path):
    """`arg` is what `{arg}` is replaced with. Empty means a literal brace on
    screen, in every locale."""
    en = _locale("datanika/i18n", "en")
    missing = tmp_path / "no.sqlite"
    v = ConnectionService.test_connection_verdict({"path": str(missing)}, ConnectionType.SQLITE)
    assert "{arg}" in en[_VERDICT_KEYS[v.reason]], "the key does not interpolate; test aimed wrong"
    assert v.arg == str(missing)

    # And the converse: a message with no placeholder must not carry an argument
    # nobody will substitute.
    v2 = ConnectionService.test_connection_verdict({"path": ":memory:"}, ConnectionType.SQLITE)
    assert "{arg}" not in en[_VERDICT_KEYS[v2.reason]]
    assert v2.arg == ""


def _locale(root: str, locale: str) -> dict:
    import json
    from pathlib import Path as LocalePath

    return json.loads((LocalePath(root) / f"{locale}.json").read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# The type sets themselves
# --------------------------------------------------------------------------


def test_the_local_file_set_matches_what_build_sa_url_actually_does():
    """`_LOCAL_FILE_DB_TYPES` is a claim about `_build_sa_url`, so check it.

    The claim: exactly these types produce a URL with no host — `dialect:///`
    followed by a path. If a new file-backed dialect is added and not listed
    here, it gets a network timeout and reproduces core#978 exactly.
    """
    # A sentinel that no branch could produce for any other reason, so the
    # detector answers "did this dialect put the caller's *path* where a host
    # would go?" rather than "does this URL happen to start with a slash".
    # Without the sentinel, `bigquery://{project}/{dataset}` with both empty
    # renders `bigquery:///` and reads as a local file — a false positive that
    # would have had this test demanding a network dialect be treated as a file.
    sentinel = "/dnk-sentinel-path/probe.db"
    hostless = set()
    for ct in ConnectionType:
        try:
            url = _build_sa_url({"path": sentinel, "database": sentinel}, ct)
        except Exception:  # noqa: S112 - a type with no URL branch is simply not a file DB
            continue
        _scheme, _, rest = url.partition("://")
        if rest.split("?")[0].rstrip("/").endswith(sentinel.lstrip("/")) and rest.startswith("/"):
            hostless.add(ct)
    assert hostless >= _LOCAL_FILE_DB_TYPES, (
        f"{_LOCAL_FILE_DB_TYPES - hostless} are listed as local-file types but "
        "_build_sa_url gives them a host"
    )
    missing = hostless - _LOCAL_FILE_DB_TYPES
    assert not missing, (
        f"{[c.value for c in missing]} produce a hostless URL — a local file — but are "
        "not in _LOCAL_FILE_DB_TYPES, so they get a network connect_timeout and will "
        "fail exactly the way duckdb did (core#978)"
    )


def test_in_memory_paths_are_recognised_on_both_dialects():
    """The set that decides whether read-only is even a mode."""
    assert ":memory:" in _IN_MEMORY_PATHS
    for ct in _LOCAL_FILE_DB_TYPES:
        assert _connect_args(ct, {"path": ":memory:"}) == {}, (
            f"{ct.value} was handed a read-only flag for an in-memory database, which "
            "duckdb refuses outright"
        )


def test_read_only_alone_prevents_creation__and_is_verified_on_its_own(tmp_path):
    """The **second** mechanism, exercised without the first (D1).

    🚨 **The two guards mask each other, and only a mutation shows it.** With
    the existence pre-check in place, removing ``read_only`` left every other
    test in this file green — the pre-check catches the missing-file case first,
    so the read-only open is never reached through ``test_connection``. Two
    guards where either alone passes the suite is one guard and a decoration.

    So this drives the URL Test Connection *builds*, past the pre-check, and
    carries its own negative control: the writable URL **does** create the file,
    which is what makes the read-only assertion a statement about the flag
    rather than about SQLite.
    """
    target = tmp_path / "must-not-appear.sqlite"
    config = {"path": str(target)}

    ro_url = _build_sa_url(config, ConnectionType.SQLITE, read_only=True)
    engine = create_engine(ro_url, connect_args=_connect_args(ConnectionType.SQLITE, config))
    with pytest.raises(Exception), engine.connect() as conn:  # noqa: PT011, B017
        conn.execute(text("SELECT 1"))
    engine.dispose()
    assert not target.exists(), "the read-only URL created the database anyway"

    # Negative control: the same path, the writable URL every loader uses.
    rw_url = _build_sa_url(config, ConnectionType.SQLITE)
    engine = create_engine(rw_url)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    engine.dispose()
    assert target.exists(), (
        "the writable URL did not create the file either, so the assertion above "
        "says nothing about read-only -- something else is stopping the write"
    )


def test_duckdb_read_only_is_verified_on_its_own(tmp_path):
    """The same property on the other dialect, where the mode is a connect arg.

    duckdb carries read-only in ``connect_args`` rather than in the URL, so it
    is a genuinely separate mechanism from sqlite's and needs its own control.
    """
    target = tmp_path / "must-not-appear.duckdb"
    url = _build_sa_url({"path": str(target)}, ConnectionType.DUCKDB)

    engine = create_engine(url, connect_args={"read_only": True})
    with pytest.raises(Exception), engine.connect() as conn:  # noqa: PT011, B017
        conn.execute(text("SELECT 1"))
    engine.dispose()
    assert not target.exists()

    engine = create_engine(url)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    engine.dispose()
    assert target.exists(), "duckdb did not create the file without read_only either"


def test_the_test_connection_path_actually_asks_for_read_only(tmp_path):
    """And that the production path is the one carrying the flag.

    The two tests above prove read-only *works*. This proves ``test_connection``
    *uses* it — the gap between a mechanism existing and a mechanism being
    wired is where core#772 and core#646 both lived.
    """
    real = tmp_path / "real.sqlite"
    engine = create_engine(f"sqlite:///{real}")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (a INTEGER)"))
        conn.commit()
    engine.dispose()

    seen = {}
    real_create = create_engine

    def spy(url, **kwargs):
        seen["url"] = str(url)
        seen["connect_args"] = kwargs.get("connect_args")
        return real_create(url, **kwargs)

    import datanika.services.connection_service as cs

    original = cs.create_engine
    cs.create_engine = spy
    try:
        ok, _msg = ConnectionService.test_connection({"path": str(real)}, ConnectionType.SQLITE)
    finally:
        cs.create_engine = original

    assert ok is True
    assert "mode=ro" in seen["url"], (
        f"Test Connection opened sqlite writable: {seen['url']!r}. The check is then "
        "capable of bringing its subject into existence, which is core#979."
    )

    duck = tmp_path / "real.duckdb"
    engine = create_engine(f"duckdb:///{duck}")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE t (a INTEGER)"))
        conn.commit()
    engine.dispose()

    cs.create_engine = spy
    try:
        ok, _msg = ConnectionService.test_connection({"path": str(duck)}, ConnectionType.DUCKDB)
    finally:
        cs.create_engine = original

    assert ok is True
    assert seen["connect_args"] == {"read_only": True}, (
        f"Test Connection opened duckdb writable: {seen['connect_args']!r}"
    )
