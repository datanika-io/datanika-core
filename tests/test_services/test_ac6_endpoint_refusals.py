"""AC6 — the criterion that proves the fix reached the SECOND surface (core#681).

`SPEC_SERVICE_AUTHORIZATION` §5 AC6:

> **One REST endpoint per subsystem is driven with a key whose owner lacks the role**, and
> refused. 🚨 This is the criterion that proves the fix reached the second surface, and it is
> the one that cannot be satisfied by hardening handlers.

Why this file is a TABLE
------------------------
Eight subsystems have to satisfy the same criterion. Written as eight bespoke tests it is eight
chances to get the harness subtly wrong; written as a table, adding a subsystem is adding a row
and the harness is asserted once. §57's rule — *drive the endpoint and read the status code* —
becomes the mechanical step rather than an insight someone has to have seven more times.

🔑 Why it must be driven over HTTP rather than asserted on the service
----------------------------------------------------------------------
The service tests already prove the service refuses. They cannot prove the **caller sees the
refusal**, and on this surface that is a separate fact with its own failure mode. Measured on
this branch: `api_v1_routes` wraps these calls in `except (ValueError, Exception)`, and
`InsufficientRoleError` is a `UserFacingError`, which is a `ValueError` — so the route answered
**first** with a `400` and prose, and `api_middleware`'s §7.1 handler could never run. Both
halves were green in isolation and the contract was not met.

That is `ENGINEERING_RULES` §57, and it is why reading `except` blocks is not the audit: there
are 20+ in that file. This is.

⚠️ What a passing row does NOT prove
------------------------------------
That the *service* enforces. A route could return `403` from its own handler and satisfy every
assertion here while the service stayed open to the Reflex path and to any future caller. The
service tests are the other half; **neither file is sufficient alone**, which is the whole shape
of §6 — two layers hardened above an unguarded one reads, from every instrument we have, as
three.
"""

from __future__ import annotations

import contextlib
import pathlib
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SASession
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.models.base import Base
from datanika.models.connection import ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.notification_channel import ChannelType
from datanika.models.pipeline import DbtCommand
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.notification_service import NotificationService
from datanika.services.pipeline_service import PipelineService
from datanika.services.rate_limit_service import RateLimitResult
from datanika.services.schedule_service import ScheduleService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from tests.factories import make_user

ORG_ID = 10
_RATE_OK = RateLimitResult(
    allowed=True, current_count=1, limit=60, remaining=59, retry_after=0, reset_at=9999999999
)

#: (subsystem, method, path template, body, required_role, a role that is BELOW it)
#:
#: Add a row per subsystem as it is wired. The seven still to come — uploads, pipelines,
#: schedules, transformations, api keys, notification channels, backup/export — each need one
#: `editor` row and, where §1's table reserves deletion for admin, one `admin` row.
CASES = [
    ("connections", "POST", "/api/v1/connections", "conn_create", "editor", MemberRole.VIEWER),
    ("connections", "PUT", "/api/v1/connections/{conn}", "rename", "editor", MemberRole.VIEWER),
    ("connections", "DELETE", "/api/v1/connections/{conn}", None, "admin", MemberRole.EDITOR),
    ("uploads", "POST", "/api/v1/uploads", "upload_create", "editor", MemberRole.VIEWER),
    ("uploads", "PUT", "/api/v1/uploads/{upload}", "rename", "editor", MemberRole.VIEWER),
    ("uploads", "DELETE", "/api/v1/uploads/{upload}", None, "admin", MemberRole.EDITOR),
    ("pipelines", "POST", "/api/v1/pipelines", "pipeline_create", "editor", MemberRole.VIEWER),
    ("pipelines", "PUT", "/api/v1/pipelines/{pipeline}", "rename", "editor", MemberRole.VIEWER),
    ("pipelines", "DELETE", "/api/v1/pipelines/{pipeline}", None, "admin", MemberRole.EDITOR),
    ("schedules", "POST", "/api/v1/schedules", "schedule_create", "editor", MemberRole.VIEWER),
    ("schedules", "DELETE", "/api/v1/schedules/{schedule}", None, "admin", MemberRole.EDITOR),
    (
        "channels",
        "POST",
        "/api/v1/notifications/channels",
        "channel_create",
        "admin",
        MemberRole.EDITOR,
    ),
    (
        "channels",
        "PUT",
        "/api/v1/notifications/channels/{channel}",
        "rename",
        "admin",
        MemberRole.EDITOR,
    ),
    (
        "channels",
        "DELETE",
        "/api/v1/notifications/channels/{channel}",
        None,
        "admin",
        MemberRole.EDITOR,
    ),
]

PG = {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"}


def _body(key: str, ids: dict) -> dict | None:
    """Bodies are built per-request because three of them reference fixture ids."""
    return {
        "conn_create": {"name": "New", "connection_type": "postgres", "config": PG},
        "rename": {"name": "renamed"},
        "upload_create": {
            "name": "U2",
            "source_connection_id": ids["conn"],
            "destination_connection_id": ids["conn"],
        },
        "pipeline_create": {
            "name": "P2",
            "destination_connection_id": ids["conn"],
            "command": "run",
        },
        "channel_create": {
            "name": "C2",
            "channel_type": "slack",
            "config": {"webhook_url": "https://hooks.slack.com/services/AC6"},
            "events": ["run_failure"],
        },
        "schedule_create": {
            "target_type": "upload",
            "target_id": ids["upload"],
            "cron_expression": "0 * * * *",
        },
    }.get(key)


@contextlib.contextmanager
def _surface(actor_role: MemberRole):
    """A live REST surface whose API key is owned by a member holding `actor_role`.

    ⚠️ The key's owner is a **real membership row**, never an invented id. On the REST path the
    only actor is `api_key.user_id` (branch A of §8), so an invented id would be refused for
    *not being a member* — which passes an assertion about `403` while testing nothing about
    the role. The `admin` control below is what catches that: it must succeed.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=__import__("sqlalchemy.pool", fromlist=["StaticPool"]).StaticPool,
    )
    Base.metadata.create_all(engine)
    session = SASession(engine)

    session.add(Organization(id=ORG_ID, name="Acme", slug="acme-ac6"))
    session.flush()
    actor = make_user(session, email=f"ac6-{actor_role.value}@test.io", password_hash="x")
    session.add(Membership(user_id=actor.id, org_id=ORG_ID, role=actor_role))
    # A separate admin exists to build fixtures with, so the subject's own role never has to be
    # raised to set the scene.
    setup_admin = make_user(session, email="ac6-setup@test.io", password_hash="x")
    session.add(Membership(user_id=setup_admin.id, org_id=ORG_ID, role=MemberRole.ADMIN))
    session.flush()

    enc = EncryptionService(Fernet.generate_key().decode())
    conn_svc = ConnectionService(enc)
    upload_svc = UploadService(conn_svc)
    pipeline_svc = PipelineService()
    transform_svc = TransformationService()
    schedule_svc = ScheduleService(upload_svc, transform_svc, pipeline_service=pipeline_svc)
    notif_svc = NotificationService()
    conn = conn_svc.create_connection(
        session, ORG_ID, "Existing", ConnectionType.POSTGRES, PG, actor_user_id=setup_admin.id
    )
    session.flush()
    upload = upload_svc.create_upload(
        session,
        ORG_ID,
        "Existing upload",
        None,
        conn.id,
        conn.id,
        {},
        actor_user_id=setup_admin.id,
    )
    pipeline = pipeline_svc.create_pipeline(
        session,
        ORG_ID,
        "Existing pipeline",
        None,
        conn.id,
        DbtCommand.RUN,
        actor_user_id=setup_admin.id,
    )
    session.flush()
    schedule = schedule_svc.create_schedule(
        session,
        ORG_ID,
        NodeType.UPLOAD,
        upload.id,
        "0 * * * *",
        actor_user_id=setup_admin.id,
    )
    session.flush()
    channel = notif_svc.create_channel(
        session,
        ORG_ID,
        actor_user_id=setup_admin.id,
        name="Existing channel",
        channel_type=ChannelType.SLACK,
        config={"webhook_url": "https://hooks.slack.com/services/EXISTING"},
        events=["run_failure"],
    )
    session.flush()
    ids = {
        "channel": channel.id,
        "conn": conn.id,
        "upload": upload.id,
        "pipeline": pipeline.id,
        "schedule": schedule.id,
    }

    key = MagicMock()
    key.id = 1
    key.org_id = ORG_ID
    key.user_id = actor.id
    key.name = "K"
    key.scopes = None

    @contextlib.contextmanager
    def fake_session():
        yield session

    import datanika.services.api_v1_routes as routes_mod

    with (
        patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
        patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
        patch("datanika.services.api_middleware._get_session", fake_session),
        patch.object(routes_mod, "_get_conn_svc", return_value=conn_svc),
        patch.object(routes_mod, "_get_upload_svc", return_value=upload_svc),
        patch.object(routes_mod, "_get_schedule_svc", return_value=schedule_svc),
    ):
        mock_svc.authenticate_api_key.return_value = key
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = _RATE_OK
        yield TestClient(Starlette(routes=api_v1_routes)), ids

    session.close()
    engine.dispose()


def _drive(client, method: str, path: str, body):
    headers = {"Authorization": "Bearer etf_ac6"}
    fn = getattr(client, method.lower())
    return fn(path, headers=headers) if body is None else fn(path, json=body, headers=headers)


@pytest.mark.parametrize(
    "subsystem,method,path,body_key,required_role,below",
    CASES,
    ids=[f"{c[0]}-{c[1]}-{c[4]}" for c in CASES],
)
def test_an_under_privileged_key_is_refused(
    subsystem, method, path, body_key, required_role, below
):
    """AC6. The refusal must be a `403` naming the role — not a `401`, `400`, `404` or `500`."""
    with _surface(below) as (client, ids):
        resp = _drive(client, method, path.format(**ids), _body(body_key, ids))

    assert resp.status_code == 403, (
        f"{subsystem} {method} {path} answered {resp.status_code} for a {below.value} — "
        f"AC6 wants 403. A 400 means a broad `except` upstream swallowed the refusal before "
        f"api_middleware could render §7.1 (ENGINEERING_RULES §57); a 500 means nothing "
        f"handled it at all; a 401 is the shape §7.1 forbids. Body: {resp.text[:300]}"
    )
    err = resp.json()["error"]
    assert err["code"] == "insufficient_role"
    assert err["required_role"] == required_role, (
        f"named {err['required_role']!r}, expected {required_role!r} — the field a script "
        "branches on"
    )
    text = resp.text.lower()
    for forbidden in ("expired", "revoked", "invalid"):
        assert forbidden not in text, (
            f"refusal reads as {forbidden!r}; the caller re-mints a key that was never the "
            f"problem and the new one fails identically. Body: {resp.text[:200]}"
        )


@pytest.mark.parametrize(
    "subsystem,method,path,body_key,required_role,below",
    CASES,
    ids=[f"{c[0]}-{c[1]}-{c[4]}" for c in CASES],
)
def test_a_key_whose_owner_holds_the_role_is_not_refused(
    subsystem, method, path, body_key, required_role, below
):
    """🔑 The control, and without it every assertion above is satisfied by refusing everyone.

    An `admin` clears every threshold in §1's table, so this must succeed on every row. It is
    also what catches an actor who is refused for **not being a member at all** — that would
    produce a `403` and pass the test above while testing nothing about the role.
    """
    with _surface(MemberRole.ADMIN) as (client, ids):
        resp = _drive(client, method, path.format(**ids), _body(body_key, ids))

    assert resp.status_code != 403, (
        f"{subsystem} {method} refused an ADMIN with {resp.text[:200]} — the refusal is not "
        "discriminating on role, so the test above proves nothing"
    )
    assert resp.status_code < 400, f"expected success, got {resp.status_code}: {resp.text[:200]}"


def test_the_table_covers_every_wired_subsystem():
    """The floor, and the thing that will fail as each subsystem is wired.

    §1 names eight. Seven are still UI-only, so this asserts the count it can honestly claim
    today and must be raised with each wiring — a table that silently stayed at one subsystem
    while the others shipped would report AC6 satisfied for work nobody did.
    """
    covered = {c[0] for c in CASES}
    assert covered == {"connections", "uploads", "pipelines", "schedules", "channels"}, (
        f"AC6 table covers {sorted(covered)}. Raise this assertion as each subsystem is wired "
        "— uploads, pipelines, schedules, transformations, api keys, notification channels, "
        "backup/export — so the table cannot silently lag the wiring."
    )


#: §1 names eight subsystems. Two of them have NO REST surface at all.
REFLEX_ONLY = {"api keys", "backup/export"}


def test_the_two_reflex_only_subsystems_still_have_no_rest_surface():
    """🔑 The honest AC9 statement, asserted rather than written down.

    AC6 is *"one REST endpoint per subsystem driven with a key whose owner lacks the role"*.
    **API keys and backup/export expose no REST endpoint**, so for those two AC6 is
    **INAPPLICABLE, not unmet** — and recording it as met would be exactly the defect Product
    fixed in AC6 itself: a criterion satisfied by something that is not the thing.

    ⚠️ Inapplicable is **not** uncovered. Both enforce in the service and are covered by
    service-level tests; what is missing is the *driven-endpoint witness*, and
    *"enforced, witness not obtainable at the endpoint layer"* is a different sentence from
    *"AC6 met"*.

    This is asserted so the claim cannot go stale silently: the day someone adds an API-key or
    backup route, this fails and forces an AC6 row rather than leaving a subsystem with a
    service check and no endpoint evidence.
    """
    routes_src = (
        pathlib.Path(__file__).resolve().parents[2] / "datanika" / "services" / "api_v1_routes.py"
    ).read_text(encoding="utf-8")
    route_lines = [ln for ln in routes_src.splitlines() if ln.strip().startswith("Route(")]
    assert route_lines, "no routes parsed — this assertion would be vacuous"

    offenders = [
        ln.strip()
        for ln in route_lines
        if any(w in ln.lower() for w in ("api-key", "api_key", "apikey", "backup", "export"))
    ]
    assert not offenders, (
        "a REST route now exists for a subsystem recorded as Reflex-only, so AC6 is no longer "
        "inapplicable for it and needs a row in CASES:\n  " + "\n  ".join(offenders)
    )
