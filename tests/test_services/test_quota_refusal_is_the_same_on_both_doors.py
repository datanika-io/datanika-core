"""The same refusal must read the same through every door of the v1 API (core#1569).

A plan-cap refusal raised by a ``connection.before_create`` subscriber reached
``POST /api/v1/connections`` as a **400 naming the limit** and
``POST /api/v1/import`` as a **500 "Internal server error"**. On the cloud edition an
integrator who hits their connection cap through bulk import is therefore told the service is
broken: they retry, get 500 again, and open a support conversation about an outage. The message
naming the metric, the limit and the usage exists -- cloud builds it -- and was discarded into a
log line nobody outside the box reads.

Both doors were wrong, in opposite directions, which is why one change fixes both:

* ``bulk_import`` wrapped nothing, so the refusal fell to ``api_middleware``'s ``except
  Exception`` and became a 500. That handler already argues this exact case for
  ``InsufficientRoleError``, verbatim: *"becomes a 500 -- which reads as OUR bug rather than as
  an answer"*. The reasoning transferred unchanged; the class was never generalised.
* the five ``create_*`` handlers caught ``(ValueError, Exception)`` and rendered ``str(exc)``, so
  a ``KeyError`` in a service also became a 400 carrying text we did not author for a user. That
  is precisely what ``UserFacingError`` exists to prevent (``datanika/errors.py``, core#1094).

🔑 **Why this file compares the two doors rather than asserting a literal.** The two layers have
**different error-body shapes** -- ``api_v1_routes._error`` nests
``{"error": {"code", "message"}}`` while ``api_middleware._error`` is flat ``{"error": "..."}``.
So "a 400 carrying the message" is satisfiable while the two doors still disagree, and an
integrator reading ``error.message`` still gets nothing. Comparing the doors to each other
derives the expectation from behaviour this change does not move (the connections door), instead
of from a literal somebody typed twice.

⚠️ **Ordering is load-bearing and is asserted below.** ``InsufficientRoleError`` *is* a
``UserFacingError``, so a ``except UserFacingError`` placed ahead of it collapses the 403 carrying
``required_role`` into a plain 400. That is core#896's shape: a gate that cannot fire because
something upstream already answered.
"""

from unittest.mock import MagicMock

import pytest
from sqlalchemy import func, select
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika import hooks
from datanika.errors import UserFacingError
from datanika.models.connection import Connection
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.rate_limit_service import RateLimitResult

# The real harness -- a real in-memory session, real services, real routes -- imported rather
# than copied, so both files exercise one harness. Only plain helpers are imported: pytest
# fixtures brought in by name would shadow these test functions' own parameters (ruff F811).
from tests.test_services.test_import_endpoint import (
    _MYSQL_CONN,
    _PG_CONN,
    _auth_headers,
    _patch_auth,
)


@pytest.fixture
def fake_api_key():
    key = MagicMock()
    key.id = 1
    key.org_id = 10
    key.user_id = 1
    key.name = "Test Key"
    key.scopes = None
    return key


@pytest.fixture
def rate_limit_ok():
    return RateLimitResult(
        allowed=True,
        current_count=1,
        limit=60,
        remaining=59,
        retry_after=0,
        reset_at=9999999999,
    )


@pytest.fixture
def client():
    return TestClient(Starlette(routes=api_v1_routes))


#: Cloud's ``QuotaExceededError`` subclasses ``UserFacingError`` (read off cloud ``origin/master``).
#: Standing in for it keeps this test in core: ``hooks.emit`` propagates deliberately, so the cloud
#: tree does not have to be installed to reproduce the defect.
REFUSAL = "Plan limit reached: 5 of 5 connections used"


class _PlanCapError(UserFacingError):
    """The shape cloud raises from ``check_connection_quota``."""


@pytest.fixture
def refuse_connections():
    """Subscribe a refuser to ``connection.before_create``; unsubscribe on the way out.

    ``hooks.clear()`` is deliberately not used -- it would drop every other subscription in the
    process, including any a concurrently-running test registered.
    """
    registered: list = []

    def _install(exc: Exception, *, after: int = 0):
        calls = {"n": 0}

        def handler(**_kwargs):
            calls["n"] += 1
            if calls["n"] > after:
                raise exc

        hooks.on("connection.before_create", handler)
        registered.append(handler)
        return calls

    yield _install
    for handler in registered:
        hooks.off("connection.before_create", handler)


def _connection_count(session) -> int:
    return session.execute(select(func.count()).select_from(Connection)).scalar_one()


class TestBothDoorsAnswerTheSameRefusalTheSameWay:
    """AC1 + AC2. The import door must match the connections door, shape included."""

    def test_the_connections_door_names_the_limit(
        self, client, fake_api_key, rate_limit_ok, refuse_connections
    ):
        """The door that was already right. Pinned so the comparison below cannot pass by
        both doors breaking together."""
        with _patch_auth(fake_api_key, rate_limit_ok):
            refuse_connections(_PlanCapError(REFUSAL))
            resp = client.post("/api/v1/connections", json=_PG_CONN, headers=_auth_headers())
        assert resp.status_code == 400
        assert resp.json()["error"]["message"] == REFUSAL

    def test_the_import_door_does_not_call_a_refusal_an_internal_error(
        self, client, fake_api_key, rate_limit_ok, refuse_connections
    ):
        with _patch_auth(fake_api_key, rate_limit_ok):
            refuse_connections(_PlanCapError(REFUSAL))
            resp = client.post(
                "/api/v1/import",
                json={"version": 2, "connections": [_PG_CONN]},
                headers=_auth_headers(),
            )
        assert resp.status_code == 400, (
            f"A quota refusal came back {resp.status_code} {resp.json()}. A refusal we authored is "
            "an answer, not an outage."
        )
        assert resp.json()["error"]["message"] == REFUSAL

    def test_the_yaml_door_too(self, client, fake_api_key, rate_limit_ok, refuse_connections):
        """``/api/v1/pipelines/yaml`` shares ``_execute_validated_import``, so it shares the
        defect. Asserted rather than assumed -- it is a separate route object."""
        yaml_body = (
            "version: 2\nconnections:\n  - name: My PG\n    connection_type: postgres\n"
            "    config:\n      host: localhost\n      port: 5432\n      user: u\n"
            "      password: p\n      database: db\n"
        )
        with _patch_auth(fake_api_key, rate_limit_ok):
            refuse_connections(_PlanCapError(REFUSAL))
            resp = client.post(
                "/api/v1/pipelines/yaml",
                content=yaml_body,
                headers={**_auth_headers(), "Content-Type": "application/x-yaml"},
            )
        assert resp.status_code == 400, (resp.status_code, resp.text)
        assert resp.json()["error"]["message"] == REFUSAL

    def test_the_two_doors_are_byte_identical(
        self, client, fake_api_key, rate_limit_ok, refuse_connections
    ):
        """AC2 in its strongest form: not *"both are 400"* but *"both are the same"*.

        The expectation comes from the connections door's own behaviour, which this change does
        not alter -- not from a literal typed in twice, which would agree with itself even if the
        shape were wrong in both places.
        """
        with _patch_auth(fake_api_key, rate_limit_ok):
            refuse_connections(_PlanCapError(REFUSAL))
            single = client.post("/api/v1/connections", json=_PG_CONN, headers=_auth_headers())
        with _patch_auth(fake_api_key, rate_limit_ok):
            refuse_connections(_PlanCapError(REFUSAL))
            bulk = client.post(
                "/api/v1/import",
                json={"version": 2, "connections": [_PG_CONN]},
                headers=_auth_headers(),
            )
        assert (single.status_code, single.json()) == (bulk.status_code, bulk.json()), (
            "The two doors still disagree. Note the two layers have different error-body shapes: "
            "api_v1_routes._error nests {'error': {'code', 'message'}} and api_middleware._error "
            "is flat {'error': '...'}, so matching the status alone is not matching the door."
        )


class TestOnlyRefusalsWeAuthoredAreRendered:
    """AC3 + AC4. Driven with both populations, requiring **different** answers.

    A test that only exercises the refusal cannot tell a correct handler from one that turns
    every exception into a 400 -- which is the bug AC3 fixes on the other side.
    """

    @pytest.mark.parametrize("path", ["/api/v1/connections", "/api/v1/import"])
    def test_an_exception_we_did_not_author_stays_an_internal_error(
        self, client, fake_api_key, rate_limit_ok, refuse_connections, path
    ):
        body = (
            _PG_CONN if path == "/api/v1/connections" else {"version": 2, "connections": [_PG_CONN]}
        )
        leaky = "sqlalchemy internals: column orgg_id does not exist"
        with _patch_auth(fake_api_key, rate_limit_ok):
            refuse_connections(RuntimeError(leaky))
            resp = client.post(path, json=body, headers=_auth_headers())
        assert resp.status_code == 500, (
            f"{path} rendered a non-user-facing exception as {resp.status_code}. Anything outside "
            "UserFacingError is ours, not the caller's, and its text was not written for them."
        )
        assert leaky not in resp.text, (
            f"{path} echoed internal exception text to the caller: {resp.text}"
        )

    def test_a_permissions_refusal_keeps_its_own_403_and_is_not_flattened_to_400(
        self, client, fake_api_key, rate_limit_ok
    ):
        """The ordering assertion. ``InsufficientRoleError`` is a ``UserFacingError``, so a
        ``except UserFacingError`` ahead of its branch turns 403 + ``required_role`` into a bare
        400. Driven through the endpoint, because reading the ``except`` order is what missed it
        the first time (``ENGINEERING_RULES`` §57).
        """
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            from datanika.models.user import MemberRole, Membership

            membership = session.execute(
                select(Membership).where(Membership.user_id == fake_api_key.user_id)
            ).scalar_one()
            membership.role = MemberRole.VIEWER
            session.flush()
            resp = client.post("/api/v1/connections", json=_PG_CONN, headers=_auth_headers())
        assert resp.status_code == 403, (resp.status_code, resp.text)
        assert "required_role" in resp.text, resp.text


class TestARefusalMidImportPersistsNothing:
    """AC5. The reason no partial-failure UX is being designed, pinned so it cannot change
    silently -- it would, if the session handling ever moved."""

    @pytest.mark.parametrize("after", [0, 1, 2])
    def test_a_refusal_at_the_nth_connection_persists_none_of_them(
        self, client, fake_api_key, rate_limit_ok, refuse_connections, after
    ):
        payload = {
            "version": 2,
            "connections": [
                {**_PG_CONN, "name": "c1"},
                {**_MYSQL_CONN, "name": "c2"},
                {**_PG_CONN, "name": "c3"},
            ],
        }
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            refuse_connections(_PlanCapError(REFUSAL), after=after)
            resp = client.post("/api/v1/import", json=payload, headers=_auth_headers())
            assert resp.status_code == 400, (resp.status_code, resp.text)
            assert _connection_count(session) == 0, (
                f"Refusing at connection {after + 1} of 3 left rows behind, so the import is no "
                "longer atomic and a partial-failure UX now has to be designed."
            )

    def test_the_control_a_clean_import_persists_all_three(
        self, client, fake_api_key, rate_limit_ok
    ):
        """Without this, every count above is satisfied by an import that persists nothing ever."""
        payload = {
            "version": 2,
            "connections": [
                {**_PG_CONN, "name": "c1"},
                {**_MYSQL_CONN, "name": "c2"},
                {**_PG_CONN, "name": "c3"},
            ],
        }
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            resp = client.post("/api/v1/import", json=payload, headers=_auth_headers())
            assert resp.status_code == 201, (resp.status_code, resp.text)
            assert _connection_count(session) == 3
