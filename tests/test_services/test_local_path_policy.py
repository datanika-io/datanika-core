"""A local filesystem path is a self-hosted feature (core#969, SPEC_LOCAL_FILE_CONNECTIONS D4).

## What was measured, and why it is not tidiness

Infra found production connection **id=14** (org 23, `duckdb`, `direction=both`,
not deleted) pointing at `/app/dbt_projects/_docs_samples/warehouse.duckdb` —
**1.32 MB, 92% of that volume by bytes, and written by no code path in
`datanika/`.** Every `tenant_*` file on that volume is regenerated from database
state before each run; `_docs_samples/` is not, and cannot be.

The general shape is the part worth fixing: **a file-based destination turns
whatever directory it points at into a store of record, and nothing constrained
where that may be.** The same `path` could name a directory inside the image
(destroyed on every rebuild — core#471), `/tmp` (lost on restart), a bind mount
shared with another tenant, or a path that resolves differently in the web tier
and the worker (core#712).

🚨 **None of those fail loudly.** The connection saves, dlt writes, and the data
is gone at the next deploy — with a `succeeded` run and a row count in the UI.

## The decision, and the two things it is not

`DATANIKA_ALLOW_LOCAL_FILE_PATHS`, default **True**.

* **Not gated on `datanika_edition`.** The property is *"is this deployment
  multi-tenant?"* — a deployment fact, not an edition. A self-hoster running the
  cloud plugin is a shape we support and must keep their local paths.
* **Not enforced at test time.** The run reads the **stored config** and Test
  Connection is optional, so a test-time refusal stops nobody. It is a save-time
  refusal, where the user can still see the error.

⚠️ **Writes only.** Existing rows already hold local paths — id=14 does — and
they must keep loading and listing. That is AC7, and it has its own test below,
because a validator bolted onto the wrong layer breaks the connections page for
everyone who has one.
"""

import pytest

from datanika.models.connection import ConnectionType
from datanika.models.user import Organization
from datanika.services.connection_service import (
    _LOCAL_PATH_TYPES,
    ConnectionService,
    LocalPathNotAllowedError,
    is_local_filesystem_location,
)
from datanika.services.encryption import EncryptionService

KEY = "3Zq7Yq5wJvXk9nR2mT8pL4sV6dC0bN1hG5jF7aE3uI0="

#: Locations that are on the local disk, and the ones that are not. Both lists
#: are load-bearing: a guard that only ever sees local paths cannot distinguish
#: "refuses local paths" from "refuses everything".
LOCAL = [
    "/app/dbt_projects/_docs_samples/warehouse.duckdb",  # the production row
    "/tmp/mywarehouse.duckdb",
    "./data",
    "data/incoming",
    "file:///srv/data/customers.csv",
    "D:\\data\\customers.csv",  # a Windows drive letter parses as a 1-char scheme
    "/var/lib/datanika/db.sqlite",
]
REMOTE = [
    "s3://bucket/prefix/",
    "s3a://bucket/prefix/",
    "gs://bucket/prefix/",
    "az://container/prefix/",
    "abfss://fs@acct.dfs.core.windows.net/p",
    "https://example.com/data.csv",
    "memory://",
]


@pytest.fixture
def svc():
    return ConnectionService(EncryptionService(KEY))


@pytest.fixture
def org(db_session):
    o = Organization(name="Local Path Org", slug="local-path-org")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def forbid(monkeypatch):
    """A deployment that does not permit local paths — i.e. production."""
    from datanika.config import settings

    monkeypatch.setattr(settings, "datanika_allow_local_file_paths", False)


@pytest.fixture
def permit(monkeypatch):
    """The default, and what every self-hoster gets."""
    from datanika.config import settings

    monkeypatch.setattr(settings, "datanika_allow_local_file_paths", True)


# --------------------------------------------------------------------------
# The predicate, in both directions
# --------------------------------------------------------------------------


@pytest.mark.parametrize("value", LOCAL)
def test_local_locations_are_recognised(value):
    assert is_local_filesystem_location(value) is True, f"{value!r} read as remote"


@pytest.mark.parametrize("value", REMOTE)
def test_remote_locations_are_not_flagged(value):
    """The control. A predicate that answers True for everything would satisfy
    every refusal test below and block S3, HTTPS and every future bucket."""
    assert is_local_filesystem_location(value) is False, f"{value!r} read as local"


def test_in_memory_is_not_a_filesystem_location():
    """`:memory:` stores nothing, so there is no location to have an opinion
    about. Refusing it would block a harmless configuration and tell the user to
    upload a file that has nothing to do with it."""
    assert is_local_filesystem_location(":memory:") is False
    assert is_local_filesystem_location("") is False


def test_an_unknown_scheme_fails_closed():
    """Aimed deliberately, and the two errors are not symmetric.

    An unknown-but-remote scheme refused is a **visible, reportable** wrong
    answer. An unknown-but-local scheme let through is a **silent** tenancy hole
    on a shared box. So anything not on the remote list is treated as local.
    """
    assert is_local_filesystem_location("weirdfs://host/path") is True


# --------------------------------------------------------------------------
# AC6 — refused at save, both directions
# --------------------------------------------------------------------------


@pytest.mark.parametrize("ct", sorted(_LOCAL_PATH_TYPES, key=lambda c: c.value))
def test_a_local_path_is_refused_at_save(db_session, svc, org, forbid, ct):
    """Every type that carries a filesystem location, not just the two filed."""
    key = "bucket_url" if ct in {ConnectionType.CSV, ConnectionType.JSON} else "path"
    with pytest.raises(LocalPathNotAllowedError) as exc:
        svc.create_connection(db_session, org.id, f"local {ct.value}", ct, {key: "/tmp/thing"})
    assert "Upload the file" in str(exc.value), (
        "the refusal must name a route that works, not merely refuse"
    )
    assert "S3" not in str(exc.value), (
        "s3 is WITHDRAWN (WITHDRAWN_SOURCE_TYPES, core#863) — s3fs is absent from the "
        "image and the type is in neither SOURCE_TYPES nor PICKER_TYPES. Naming it "
        "sends the refused user to a connector they cannot create, which the "
        "constant's own comment calls a worse falsehood than the one being fixed."
    )


@pytest.mark.parametrize("ct", sorted(_LOCAL_PATH_TYPES, key=lambda c: c.value))
def test_the_same_save_succeeds_when_local_paths_are_permitted(db_session, svc, org, permit, ct):
    """The other direction, and without it the guard proves nothing.

    Default-permitted is the whole reason no self-hosted deployment breaks on
    upgrade — the people this feature was built for.
    """
    key = "bucket_url" if ct in {ConnectionType.CSV, ConnectionType.JSON} else "path"
    conn = svc.create_connection(db_session, org.id, f"ok {ct.value}", ct, {key: "/tmp/thing"})
    assert conn.id


def test_a_remote_bucket_is_unaffected(db_session, svc, org, forbid):
    """A bucket URL means the same thing in both containers, so it is not the
    thing being refused."""
    conn = svc.create_connection(
        db_session, org.id, "remote csv", ConnectionType.CSV, {"bucket_url": "s3://b/p/"}
    )
    assert conn.id


def test_a_database_connection_is_unaffected(db_session, svc, org, forbid):
    """The false-positive control at the type level: postgres has a `database`
    key and it is a database *name*, not a path."""
    conn = svc.create_connection(
        db_session,
        org.id,
        "pg",
        ConnectionType.POSTGRES,
        {"host": "db", "port": 5432, "user": "u", "password": "p", "database": "analytics"},
    )
    assert conn.id


def test_update_is_refused_too(db_session, svc, org, permit, forbid):
    """Create is not the only write.

    ⚠️ Order matters here: the row is created while paths are permitted (as an
    existing row would have been) and then *edited* under the ban. A guard on
    `create_connection` alone leaves editing as an open door to the same state.
    """
    from datanika.config import settings

    settings.datanika_allow_local_file_paths = True
    conn = svc.create_connection(
        db_session, org.id, "duck", ConnectionType.DUCKDB, {"path": "/tmp/a.duckdb"}
    )
    settings.datanika_allow_local_file_paths = False
    with pytest.raises(LocalPathNotAllowedError):
        svc.update_connection(db_session, org.id, conn.id, config={"path": "/tmp/b.duckdb"})


def test_update_of_an_unrelated_field_is_not_refused(db_session, svc, org, monkeypatch):
    """Renaming a connection that holds a local path must still work.

    Otherwise an org with an existing local-path connection cannot tidy it up,
    cannot re-point it at a bucket, and cannot do anything but delete it.
    """
    from datanika.config import settings

    monkeypatch.setattr(settings, "datanika_allow_local_file_paths", True)
    conn = svc.create_connection(
        db_session, org.id, "duck", ConnectionType.DUCKDB, {"path": "/tmp/a.duckdb"}
    )
    monkeypatch.setattr(settings, "datanika_allow_local_file_paths", False)
    updated = svc.update_connection(db_session, org.id, conn.id, name="renamed")
    assert updated is not None
    assert updated.name == "renamed"


def test_repointing_a_local_row_at_a_bucket_is_allowed(db_session, svc, org, monkeypatch):
    """The migration path for production connection id=14.

    ⚠️ **This is the assertion that makes the refusal survivable.** Whatever the
    fix is, it has a migration half, and an org holding a local path must have a
    way *out* that is not "delete and recreate".
    """
    from datanika.config import settings

    monkeypatch.setattr(settings, "datanika_allow_local_file_paths", True)
    conn = svc.create_connection(
        db_session, org.id, "csv", ConnectionType.CSV, {"bucket_url": "/srv/incoming"}
    )
    monkeypatch.setattr(settings, "datanika_allow_local_file_paths", False)
    updated = svc.update_connection(
        db_session, org.id, conn.id, config={"bucket_url": "s3://bucket/incoming/"}
    )
    assert updated is not None


# --------------------------------------------------------------------------
# AC7 — reads are untouched
# --------------------------------------------------------------------------


def test_an_existing_local_path_row_still_loads_and_lists(db_session, svc, org, monkeypatch):
    """AC7. **List them, do not hide them.**

    Production connection id=14 exists. A refusal that reached the read path
    would break the connections page for anyone who has one — and would do it on
    the deploy that flips the flag, with no warning and nothing to click.
    """
    from datanika.config import settings

    monkeypatch.setattr(settings, "datanika_allow_local_file_paths", True)
    conn = svc.create_connection(
        db_session,
        org.id,
        "legacy duckdb",
        ConnectionType.DUCKDB,
        {"path": "/app/dbt_projects/_docs_samples/warehouse.duckdb"},
    )
    conn_id = conn.id
    db_session.flush()

    monkeypatch.setattr(settings, "datanika_allow_local_file_paths", False)

    assert svc.get_connection(db_session, org.id, conn_id) is not None
    assert any(c.id == conn_id for c in svc.list_connections(db_session, org.id))
    config = svc.get_connection_config(db_session, org.id, conn_id)
    assert config["path"].endswith("warehouse.duckdb")


def test_deleting_a_local_path_row_still_works(db_session, svc, org, monkeypatch):
    """The other thing an org must be able to do with a row it can no longer save."""
    from datanika.config import settings

    monkeypatch.setattr(settings, "datanika_allow_local_file_paths", True)
    conn = svc.create_connection(
        db_session, org.id, "legacy", ConnectionType.SQLITE, {"path": "/tmp/legacy.sqlite"}
    )
    monkeypatch.setattr(settings, "datanika_allow_local_file_paths", False)
    assert svc.delete_connection(db_session, org.id, conn.id) is True


# --------------------------------------------------------------------------
# The type set, and the default
# --------------------------------------------------------------------------


def test_the_default_permits_local_paths():
    """A default of False would break every existing self-hosted deployment on
    upgrade, and self-hosters are the only people the feature was ever for."""
    from datanika.config import Settings

    assert Settings().datanika_allow_local_file_paths is True


def test_s3_is_not_in_the_local_path_set():
    """Its bucket_url is remote, so it means the same thing in both containers."""
    assert ConnectionType.S3 not in _LOCAL_PATH_TYPES


def test_the_local_path_set_covers_every_type_that_reads_a_path():
    """Derived, not remembered.

    The five types are sqlite + duckdb (a `path`/`database` into a
    `dialect:///` URL) and csv/json/parquet (a `bucket_url`/`path` handed to
    dlt's `filesystem()`). If a sixth is added and not listed, it silently keeps
    the unconstrained behaviour this issue is about.
    """
    assert {c.value for c in _LOCAL_PATH_TYPES} == {
        "sqlite",
        "duckdb",
        "csv",
        "json",
        "parquet",
    }


# --------------------------------------------------------------------------
# D6 — the refusal reaches the user in their own language
# --------------------------------------------------------------------------


def test_the_refusal_is_translated_not_surfaced_verbatim():
    """`_set_error` passes a ValueError's message straight through.

    🚨 That is exactly the defect D6 names — `test_message` is rendered raw from
    the service and reads in English in all nine locales — and routing this
    refusal through the ordinary error path would have reintroduced it inside
    the change that closes it. `save_connection` catches
    `LocalPathNotAllowedError` **before** the generic handler and translates it.

    Asserted structurally: the specific `except` must come first, because Python
    matches in order and a broader one above it would swallow this.
    """
    import ast
    import inspect
    import textwrap

    from datanika.ui.state.connection_state import _ERROR_KEYS, ConnectionState

    fn = ast.parse(
        textwrap.dedent(
            inspect.getsource(
                getattr(ConnectionState.save_connection, "fn", ConnectionState.save_connection)
            )
        )
    ).body[0]

    handlers = [h for n in ast.walk(fn) if isinstance(n, ast.Try) for h in n.handlers]
    names = [ast.unparse(h.type) if h.type else "bare" for h in handlers]
    assert "LocalPathNotAllowedError" in names, (
        "save_connection does not catch LocalPathNotAllowedError, so the refusal reaches "
        "the user through _set_error — verbatim English, in nine locales"
    )
    assert names.index("LocalPathNotAllowedError") < names.index("Exception"), (
        f"the generic handler comes first ({names}), so the specific one never runs"
    )

    body = ast.unparse(handlers[names.index("LocalPathNotAllowedError")])
    assert "_translated" in body, "the refusal is not translated"
    assert "_ERROR_KEYS" in body, "the key does not come from the UI-side mapping"

    assert _ERROR_KEYS[LocalPathNotAllowedError.reason] == "connections.local_path_not_allowed"


@pytest.mark.parametrize("locale", ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"])
def test_the_refusal_exists_in_every_locale(locale):
    import json
    from pathlib import Path

    data = json.loads((Path("datanika/i18n") / f"{locale}.json").read_text(encoding="utf-8"))
    en = json.loads((Path("datanika/i18n") / "en.json").read_text(encoding="utf-8"))
    key = "connections.local_path_not_allowed"
    assert key in data and data[key].strip()
    if locale != "en":
        assert data[key] != en[key], f"{locale}.json carries the English string verbatim"


def test_no_locale_names_a_withdrawn_connector():
    """🚨 The spec's own D4 copy says *"point this connection at **S3** or a
    database"*, and **`s3` is withdrawn** — `WITHDRAWN_SOURCE_TYPES`, core#863.
    s3fs is absent from the image and the type is in neither `SOURCE_TYPES`,
    `CONFIG_SCHEMAS` nor `PICKER_TYPES`.

    A refusal exists to name the routes that work. Naming a dead one defeats it,
    and lands on the person who has just been refused — the worst moment to be
    sent somewhere that does not exist. The constant's own comment says the same
    thing about a different connector: *"'Use GCS instead' would name a connector
    that does not exist, which is a worse falsehood than the one being fixed."*

    ⚠️ When s3 is restored (the trigger is `TestDeferredCapability` in
    `test_dependency_advisories.py` going red), this assertion is the thing to
    revisit — the sentence should get S3 back alongside the connector.
    """
    import json
    from pathlib import Path

    from datanika.services.connection_service import WITHDRAWN_SOURCE_TYPES

    assert "s3" in WITHDRAWN_SOURCE_TYPES, (
        "s3 is no longer withdrawn — restore it to the refusal message, which is "
        "poorer for having only one alternative to offer"
    )
    for locale in ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]:
        text = json.loads((Path("datanika/i18n") / f"{locale}.json").read_text(encoding="utf-8"))[
            "connections.local_path_not_allowed"
        ]
        assert "S3" not in text and "s3://" not in text, (
            f"{locale}.json's refusal names S3, which cannot be created today: {text!r}"
        )
