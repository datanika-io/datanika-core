"""An import naming a resource the name rule refuses answers 400 and writes nothing (core#1440).

The validator checked that each name was present and not duplicated; the rule itself was applied
later, by the service doing the creating, and nothing on that path turned its ``UserFacingError``
into a response. So the caller got a ``500`` with no message for a name the product simply does not
allow — on both import routes, for an admin key and an editor key alike.

Two properties, and the second is the one a status code cannot show: the refusal **names the
resource and the rule**, and the import **writes nothing**. The second is asserted on the rows,
because an import that refuses the payload's last entry after creating its first is a partial
import wearing a 400. The issue records that half as *not measured*; it is measured here.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from datanika.models.connection import Connection
from datanika.models.transformation import Transformation
from datanika.models.upload import Upload
from tests.test_services.test_import_endpoint import (  # noqa: F401  (fixtures)
    _auth_headers,
    _patch_auth,
    app,
    client,
    fake_api_key,
    rate_limit_ok,
)

#: Refused by `validate_name`: the connection rule allows letters, digits and spaces only.
BAD_CONNECTION_NAME = "imp_c"
GOOD_CONNECTION_NAME = "Imported PG"

_CONFIG = {"host": "localhost", "port": 5432, "user": "u", "password": "p", "database": "db"}


def _conn(name: str) -> dict:
    return {"name": name, "connection_type": "postgres", "config": dict(_CONFIG)}


def _rows(session) -> dict[str, list[str]]:
    """Every name the import could have written, read back off the rows."""
    return {
        "connections": [
            r for (r,) in session.execute(select(Connection.name).order_by(Connection.name))
        ],
        "uploads": [r for (r,) in session.execute(select(Upload.name).order_by(Upload.name))],
        "transformations": [
            r for (r,) in session.execute(select(Transformation.name).order_by(Transformation.name))
        ],
    }


def _yaml(name: str) -> str:
    return (
        "version: 2\n"
        "connections:\n"
        f"  - name: {name}\n"
        "    connection_type: postgres\n"
        "    config: {host: localhost, port: 5432, user: u, password: p, database: db}\n"
    )


def _post_json(client, payload):  # noqa: F811
    return client.post("/api/v1/import", json=payload, headers=_auth_headers())


def _post_yaml(client, body: str):  # noqa: F811
    headers = {**_auth_headers(), "Content-Type": "application/x-yaml"}
    return client.post("/api/v1/pipelines/yaml", content=body.encode(), headers=headers)


# ---------------------------------------------------------------------------------------------
# The refusal
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("route", ["json", "yaml"])
def test_a_name_the_rule_refuses_answers_400_and_writes_nothing(
    client,  # noqa: F811
    fake_api_key,  # noqa: F811
    rate_limit_ok,  # noqa: F811
    route,
):
    with _patch_auth(fake_api_key, rate_limit_ok) as session:
        if route == "json":
            resp = _post_json(client, {"version": 2, "connections": [_conn(BAD_CONNECTION_NAME)]})
        else:
            resp = _post_yaml(client, _yaml(BAD_CONNECTION_NAME))

        assert resp.status_code == 400, (resp.status_code, resp.text[:300])
        body = resp.json()
        assert "errors" in body, body
        blob = str(body["errors"])
        assert BAD_CONNECTION_NAME in blob, blob
        assert "alphanumeric" in blob, "the refusal does not state the rule"
        assert _rows(session) == {"connections": [], "uploads": [], "transformations": []}


def test_an_invalid_name_after_a_valid_one_still_writes_nothing(
    client,  # noqa: F811
    fake_api_key,  # noqa: F811
    rate_limit_ok,  # noqa: F811
):
    """The half a status code cannot show: refusing entry two must not keep entry one."""
    with _patch_auth(fake_api_key, rate_limit_ok) as session:
        resp = _post_json(
            client,
            {
                "version": 2,
                "connections": [_conn(GOOD_CONNECTION_NAME), _conn(BAD_CONNECTION_NAME)],
                "transformations": [{"name": "imported_model", "sql_body": "select 1"}],
            },
        )

        assert resp.status_code == 400, (resp.status_code, resp.text[:300])
        assert BAD_CONNECTION_NAME in str(resp.json()["errors"])
        rows = _rows(session)
        assert rows["connections"] == [], f"a connection survived a refused import: {rows}"
        assert rows["transformations"] == [], f"a transformation survived: {rows}"


def test_every_invalid_name_in_the_payload_is_reported_at_once(
    client,  # noqa: F811
    fake_api_key,  # noqa: F811
    rate_limit_ok,  # noqa: F811
):
    """One round trip per payload, not one per bad name."""
    with _patch_auth(fake_api_key, rate_limit_ok):
        resp = _post_json(
            client,
            {"version": 2, "connections": [_conn("bad_one"), _conn("bad~two")]},
        )

        assert resp.status_code == 400
        blob = str(resp.json()["errors"])
        assert "bad_one" in blob and "bad~two" in blob, blob


# ---------------------------------------------------------------------------------------------
# Controls — the refusal must not widen
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("route", ["json", "yaml"])
def test_control_a_valid_name_still_imports(
    client,  # noqa: F811
    fake_api_key,  # noqa: F811
    rate_limit_ok,  # noqa: F811
    route,
):
    with _patch_auth(fake_api_key, rate_limit_ok) as session:
        if route == "json":
            resp = _post_json(client, {"version": 2, "connections": [_conn(GOOD_CONNECTION_NAME)]})
        else:
            resp = _post_yaml(client, _yaml(f"'{GOOD_CONNECTION_NAME}'"))

        assert resp.status_code == 201, (resp.status_code, resp.text[:300])
        assert _rows(session)["connections"] == [GOOD_CONNECTION_NAME]


def test_control_a_transformation_name_follows_its_own_rule(
    client,  # noqa: F811
    fake_api_key,  # noqa: F811
    rate_limit_ok,  # noqa: F811
):
    """Underscores are refused in a connection name and required-ish in a model name.

    The rules differ on purpose, so a fix that applied one rule to everything would land here
    rather than in production. ``imported_model`` is valid; a connection called the same is not.
    """
    with _patch_auth(fake_api_key, rate_limit_ok) as session:
        resp = _post_json(
            client,
            {"version": 2, "transformations": [{"name": "imported_model", "sql_body": "select 1"}]},
        )

        assert resp.status_code == 201, (resp.status_code, resp.text[:300])
        assert _rows(session)["transformations"] == ["imported_model"]


def test_control_an_invalid_transformation_name_is_also_refused_with_400(
    client,  # noqa: F811
    fake_api_key,  # noqa: F811
    rate_limit_ok,  # noqa: F811
):
    """Same class, different rule: a model name the model rule refuses is not a 500 either."""
    with _patch_auth(fake_api_key, rate_limit_ok) as session:
        resp = _post_json(
            client,
            {"version": 2, "transformations": [{"name": "9 bad model", "sql_body": "select 1"}]},
        )

        assert resp.status_code == 400, (resp.status_code, resp.text[:300])
        assert "9 bad model" in str(resp.json()["errors"])
        assert _rows(session)["transformations"] == []
