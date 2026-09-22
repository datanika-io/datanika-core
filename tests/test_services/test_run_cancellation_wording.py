"""What cancelling a run does, said once and read by every surface (core#657).

``SPEC_RUN_CANCELLATION`` AC10 and AC12 put two sentences on three surfaces — the confirmation
dialog, the API response body and the docs — *worded the same*. "Worded the same" is a property
no single surface can check about itself, so the wording lives in ONE module,
:mod:`datanika.services.run_cancellation`, and each surface in this repository is asserted to
read it: the dialog's English strings equal it, the cancel response carries it, and the published
OpenAPI operation describes it. The docs page is in the landing repository and carries the same
text by hand; that is the one copy this file cannot see.

🔑 **The wording describes 2a, not 2b — and that is the point of pinning it.** D3 as first
written (*"Cancelling stops further loading … `append` will duplicate the partial rows"*) describes
a mid-flight stop, which §7.2 declares unbuildable today: a run already inside its engine call
runs to the end. Shipping D3 verbatim would tell the user the one thing about cancelling that is
not true. The spec records the 2a wording as D3a, with the condition that flips it back.

⚠️ **The billing sentence follows the edition.** The open-source edition bills nobody, so a
self-hoster told *"you are billed for what was processed"* has been told something false. Only
``DATANIKA_EDITION=cloud`` says it — the same setting that gates billing everywhere else.
"""

from __future__ import annotations

import contextlib
import json
import pathlib
import uuid
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SASession
from sqlalchemy.pool import StaticPool
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.models.base import Base
from datanika.models.dependency import NodeType
from datanika.models.run import (
    NON_TERMINAL_RUN_STATUSES,
    TERMINAL_RUN_STATUSES,
    Run,
    RunStatus,
)
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services import run_cancellation
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.openapi import build_openapi_spec
from datanika.services.rate_limit_service import RateLimitResult
from datanika.services.run_cancellation import (
    CANCEL_BILLING,
    CANCEL_EFFECT,
    cancellation_notice,
)
from tests.factories import make_user

I18N = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "i18n"
LOCALES = ("en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr")

_RATE_OK = RateLimitResult(
    allowed=True, current_count=1, limit=60, remaining=59, retry_after=0, reset_at=9999999999
)


def _locale(code: str) -> dict:
    return json.loads((I18N / f"{code}.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------------------------
# The wording itself
# ---------------------------------------------------------------------------------------------


class TestTheWordingDescribesWhatHappensToday:
    """Each assertion is a PRESENCE: the property the sentence must state, not a banned phrase.

    A ban on D3's old wording would be satisfied by deleting the sentence, and by a correction
    that quotes the old wording in order to retract it.
    """

    def test_a_run_that_has_not_started_stops_before_anything_is_read_or_written(self):
        """2a, shipped: the pre-flight checkpoint refuses the engine, and the run records 0 rows."""
        assert "has not started its work yet stops before anything is read or written" in (
            CANCEL_EFFECT
        )

    def test_work_already_in_progress_is_said_to_run_to_the_end(self):
        """🚦 The flip condition. When 2b ships — an engine call that a cancel can interrupt —
        THIS is the assertion that must change, and the wording with it (spec D3a)."""
        assert "cannot be interrupted" in CANCEL_EFFECT
        assert "runs to the end" in CANCEL_EFFECT

    def test_the_data_stays_and_the_append_consequence_is_named(self):
        """D3: `append` is the one disposition where cancelling leaves the user a cleanup."""
        assert "stays there" in CANCEL_EFFECT
        assert "appends" in CANCEL_EFFECT

    def test_the_billing_sentence_is_ac12s(self):
        assert "billed for what was processed before the run stopped" in CANCEL_BILLING


class TestTheNoticeFollowsTheEdition:
    def test_the_hosted_edition_states_the_billing(self, monkeypatch):
        monkeypatch.setattr(run_cancellation.settings, "datanika_edition", "cloud")
        notice = cancellation_notice()
        assert CANCEL_EFFECT in notice
        assert CANCEL_BILLING in notice

    def test_the_open_source_edition_does_not_claim_a_bill(self, monkeypatch):
        """Both halves in one test: the effect is still said, and the bill is not."""
        monkeypatch.setattr(run_cancellation.settings, "datanika_edition", "core")
        notice = cancellation_notice()
        assert CANCEL_EFFECT in notice
        assert CANCEL_BILLING not in notice


# ---------------------------------------------------------------------------------------------
# Surface 1 — the dialog
# ---------------------------------------------------------------------------------------------


class TestTheDialogSaysTheSameThing:
    def test_the_english_body_is_the_wording(self):
        assert _locale("en")["runs.cancel_body"] == CANCEL_EFFECT

    def test_the_english_billing_line_is_the_wording(self):
        assert _locale("en")["runs.cancel_billing"] == CANCEL_BILLING

    @pytest.mark.parametrize("code", [c for c in LOCALES if c != "en"])
    def test_every_other_locale_translates_both(self, code):
        english, local = _locale("en"), _locale(code)
        for key in ("runs.cancel_body", "runs.cancel_billing"):
            assert local.get(key), f"{code}.json has no {key}"
            assert local[key] != english[key], f"{code}.json left {key} in English"


# ---------------------------------------------------------------------------------------------
# Surface 2 — the API response
# ---------------------------------------------------------------------------------------------


@pytest.fixture
def cancel_api():
    """The REST surface with a real run, a real editor and the middleware's session patched.

    Its own engine rather than ``db_session``: the middleware runs a sync handler in a thread
    pool, and a default SQLite connection refuses to be used from a second thread — the same
    reason ``test_ac6_endpoint_refusals._surface`` builds one.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    db_session = SASession(engine)
    org = Organization(name="Acme", slug=f"acme-cancel-notice-{uuid.uuid4().hex[:8]}")
    db_session.add(org)
    db_session.flush()
    actor = make_user(db_session, email=f"notice-{org.id}@test.io", password_hash="x")
    db_session.add(Membership(user_id=actor.id, org_id=org.id, role=MemberRole.EDITOR))
    db_session.flush()

    key = MagicMock()
    key.id = 1
    key.org_id = org.id
    key.user_id = actor.id
    key.name = "K"
    key.scopes = None

    @contextlib.contextmanager
    def fake_session():
        yield db_session

    with (
        patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
        patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
        patch("datanika.services.api_middleware._get_session", fake_session),
    ):
        mock_svc.authenticate_api_key.return_value = key
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = _RATE_OK
        yield TestClient(Starlette(routes=api_v1_routes)), org.id, db_session

    db_session.close()
    engine.dispose()


def _run(session, org_id: int, status: RunStatus) -> Run:
    run = Run(org_id=org_id, target_type=NodeType.UPLOAD, target_id=1, status=status)
    session.add(run)
    session.flush()
    return run


def _cancel(client, run_id: int):
    return client.post(f"/api/v1/runs/{run_id}/cancel", headers={"Authorization": "Bearer etf_n"})


class TestTheCancelResponseCarriesTheNotice:
    @pytest.mark.parametrize("status", [RunStatus.PENDING, RunStatus.RUNNING])
    def test_an_accepted_cancel_says_what_it_did(self, cancel_api, monkeypatch, status):
        client, org_id, session = cancel_api
        monkeypatch.setattr(run_cancellation.settings, "datanika_edition", "cloud")
        run = _run(session, org_id, status)

        resp = _cancel(client, run.id)

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["id"] == run.id, "the body is still the run"
        assert body["notice"] == cancellation_notice()
        assert CANCEL_BILLING in body["notice"]

    def test_the_open_source_edition_says_the_effect_and_no_bill(self, cancel_api, monkeypatch):
        client, org_id, session = cancel_api
        monkeypatch.setattr(run_cancellation.settings, "datanika_edition", "core")
        run = _run(session, org_id, RunStatus.RUNNING)

        body = _cancel(client, run.id).json()

        assert CANCEL_EFFECT in body["notice"]
        assert CANCEL_BILLING not in body["notice"]

    def test_control_a_refused_cancel_carries_no_notice(self, cancel_api):
        """A `409` did nothing, so it must not describe what cancelling does to a run."""
        client, org_id, session = cancel_api
        run = _run(session, org_id, RunStatus.SUCCESS)

        resp = _cancel(client, run.id)

        assert resp.status_code == 409
        assert "notice" not in resp.json()


# ---------------------------------------------------------------------------------------------
# Surface 3 (in this repository) — the published API reference
# ---------------------------------------------------------------------------------------------


class TestTheApiReference:
    """The cancel operation was served and never published: `/api/v1/runs/{id}/cancel` was in
    the routes and absent from the OpenAPI document, so a client generated from the reference
    could not cancel at all, and the one place that would carry D3's sentence did not exist."""

    CANCEL = "/api/v1/runs/{id}/cancel"

    def test_the_cancel_operation_is_published(self):
        op = build_openapi_spec()["paths"][self.CANCEL]["post"]
        assert {"200", "401", "404", "409", "429"} <= set(op["responses"])
        assert op["x-stability"] == "stable"

    def test_it_describes_what_cancelling_does(self):
        op = build_openapi_spec()["paths"][self.CANCEL]["post"]
        assert CANCEL_EFFECT in op["description"]

    def test_the_200_body_documents_the_notice_and_every_status(self):
        schema = build_openapi_spec()["components"]["schemas"]["RunCancellation"]
        assert "notice" in schema["properties"]
        assert schema["properties"]["status"]["enum"] == [s.value for s in RunStatus]

    def test_the_wait_outcomes_name_every_status_they_can_carry(self):
        """§4, in prose. The `408` read *"still pending/running"* and the `422`
        *"(`failed`, `cancelled`)"*: two more hand-written status lists, and the first went
        stale the day `cancelling` was added — a `cancelling` run at timeout is a `408`."""
        responses = build_openapi_spec()["paths"]["/api/v1/uploads/{id}/run"]["post"]["responses"]
        for status in NON_TERMINAL_RUN_STATUSES:
            assert f"`{status.value}`" in responses["408"]["description"], status
        for status in TERMINAL_RUN_STATUSES - {RunStatus.SUCCESS}:
            assert f"`{status.value}`" in responses["422"]["description"], status
