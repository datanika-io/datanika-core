"""The redactor behind stored run text, and the hold that turns it on (core#1460).

``tests/test_tasks/test_run_text_never_carries_connection_secrets.py`` drives two connectors
through the real ``run_upload``. This file pins the parts those arms cannot reach one by one: every
spelling, a secret nested where a REST connection keeps it, the fail-closed rule for a secret too
short to cut out of prose, and the guarantee that every task reading a connection config holds it.
"""

from __future__ import annotations

import ast
import base64
import json
import pathlib
from urllib.parse import quote, quote_plus

import pytest

from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.models.user import Organization
from datanika.services.connection_service import RUN_TEXT_WITHHELD, redact_run_text
from datanika.services.execution_service import (
    ExecutionService,
    hold_run_secrets,
    release_run_secrets,
)

SECRET = "s3cret value/+@=="


class TestRedactRunText:
    @pytest.mark.parametrize(
        "spelling",
        [
            pytest.param(SECRET, id="as stored"),
            pytest.param(quote(SECRET, safe=""), id="percent-encoded"),
            pytest.param(quote_plus(SECRET), id="form-encoded"),
            pytest.param(base64.b64encode(SECRET.encode()).decode(), id="base64"),
        ],
    )
    def test_every_spelling_a_driver_can_quote_is_removed(self, spelling):
        text = f"connection failed for postgresql://u:{spelling}@host/db"
        redacted = redact_run_text(text, [{"password": SECRET}])
        assert spelling not in redacted and "connection failed" in redacted

    def test_a_secret_nested_where_a_rest_connection_keeps_it(self):
        config = {"base_url": "https://api.example.com", "auth": {"api_key": "nested-key-1460"}}
        redacted = redact_run_text("401 for url: /items?key=nested-key-1460", [config])
        assert "nested-key-1460" not in redacted and "401" in redacted

    def test_a_structured_secret_is_removed_as_its_json_text_too(self):
        keyfile = {"type": "service_account", "private_key": "-----BEGIN KEY-----abc"}
        redacted = redact_run_text(
            f"bad key file: {json.dumps(keyfile)}", [{"keyfile_json": keyfile}]
        )
        assert json.dumps(keyfile) not in redacted and "BEGIN KEY" not in redacted

    def test_a_secret_that_contains_another_is_removed_whole(self):
        """Removed shortest-first, the longer key would lose its head and keep its tail."""
        configs = [{"password": "abcd1234"}, {"api_key": "abcd1234efgh"}]
        redacted = redact_run_text("rejected key abcd1234efgh", configs)
        assert "efgh" not in redacted and "rejected key" in redacted

    def test_a_credential_stored_as_json_text_is_removed_leaf_by_leaf(self):
        """The connection form stores a key file as the text that was pasted, indentation and all,
        while a client that parsed it quotes one field of it."""
        pasted = json.dumps(
            {"type": "service_account", "private_key": "-----BEGIN PRIVATE KEY-----MIIEv1460"},
            indent=2,
        )
        redacted = redact_run_text(
            "could not load -----BEGIN PRIVATE KEY-----MIIEv1460", [{"keyfile_json": pasted}]
        )
        assert "MIIEv1460" not in redacted and "could not load" in redacted

    def test_a_short_part_of_a_structured_credential_does_not_withhold_the_text(self):
        """Only a secret that stands alone fails closed; "jwt" inside a key file is not the key."""
        config = {"credentials": {"type": "jwt", "private_key": "long-private-key-1460"}}
        text = "jwt signature rejected"
        assert redact_run_text(text, [config]) == text

    def test_a_value_under_an_ordinary_key_is_left_alone(self):
        config = {"host": "warehouse.example.com", "password": "hunter2-long"}
        text = "could not reach warehouse.example.com"
        assert redact_run_text(text, [config]) == text

    def test_a_short_secret_that_appears_withholds_the_whole_text(self):
        """Removing "ab" from prose would shred it, so the text is withheld.

        Test Connection's messages already fail closed the same way.
        """
        assert redact_run_text("password ab rejected", [{"password": "ab"}]) == RUN_TEXT_WITHHELD

    def test_a_short_secret_that_does_not_appear_leaves_the_text(self):
        assert redact_run_text("timed out", [{"password": "ab"}]) == "timed out"

    def test_empty_text_and_no_configs_pass_through(self):
        assert redact_run_text(None, [{"password": SECRET}]) is None
        assert redact_run_text("", [{"password": SECRET}]) == ""
        assert redact_run_text(f"x {SECRET}", []) == f"x {SECRET}"


class TestTheHold:
    def _run(self, db_session):
        org = Organization(name="Acme", slug="acme-1460-hold")
        db_session.add(org)
        db_session.flush()
        return org.id, ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, 1)

    def test_a_failure_stored_while_held_carries_no_secret(self, db_session):
        org_id, run = self._run(db_session)
        token = hold_run_secrets({"password": SECRET})
        try:
            ExecutionService().fail_run(
                db_session, org_id, run.id, error_message=f"bad: {SECRET}", logs=f"tb {SECRET}"
            )
        finally:
            release_run_secrets(token)
        assert run.status == RunStatus.FAILED
        assert SECRET not in run.error_message and SECRET not in run.logs
        assert run.error_message.startswith("bad: ")

    def test_control_without_a_hold_the_text_is_stored_as_given(self, db_session):
        """The redaction is the hold's doing: nothing held, nothing removed."""
        org_id, run = self._run(db_session)
        ExecutionService().fail_run(
            db_session, org_id, run.id, error_message=f"bad: {SECRET}", logs="tb"
        )
        assert SECRET in run.error_message

    def test_success_logs_and_appended_logs_are_covered(self, db_session):
        org_id, run = self._run(db_session)
        service = ExecutionService()
        token = hold_run_secrets({"api_key": "appended-key-1460"})
        try:
            service.complete_run(
                db_session, org_id, run.id, rows_loaded=1, logs="ok appended-key-1460"
            )
            service.append_logs(db_session, org_id, run.id, "WARNING appended-key-1460")
        finally:
            release_run_secrets(token)
        assert "appended-key-1460" not in run.logs
        assert "WARNING" in run.logs and "ok" in run.logs

    def test_releasing_ends_the_hold(self, db_session):
        org_id, run = self._run(db_session)
        release_run_secrets(hold_run_secrets({"password": SECRET}))
        ExecutionService().fail_run(db_session, org_id, run.id, error_message=SECRET, logs="")
        assert run.error_message == SECRET


TASKS = pathlib.Path(__file__).resolve().parents[2] / "datanika" / "tasks"
READS_A_CONFIG = {"decrypt", "get_connection_config"}


def _called(function: ast.AST) -> set[str]:
    names = set()
    for node in ast.walk(function):
        if isinstance(node, ast.Call):
            target = node.func
            names.add(
                target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", "")
            )
    return names


def _functions_reading_configs() -> dict[str, set[str]]:
    found = {}
    for path in sorted(TASKS.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                calls = _called(node)
                if calls & READS_A_CONFIG:
                    found[f"{path.name}:{node.name}"] = calls
    return found


def test_the_scan_finds_the_tasks_known_to_read_a_config():
    """Anti-vacuity: an empty scan would let every task through."""
    found = set(_functions_reading_configs())
    assert {
        "upload_tasks.py:run_upload",
        "pipeline_tasks.py:run_pipeline",
        "transformation_tasks.py:run_transformation",
    } <= found, found


def test_every_task_that_reads_a_connection_config_holds_it():
    missing = sorted(
        name
        for name, calls in _functions_reading_configs().items()
        if not {"hold_run_secrets", "release_run_secrets"} <= calls
    )
    assert not missing, (
        f"{missing} read a connection config and do not hold it, so text they store can carry its "
        "secrets. Call hold_run_secrets after reading it and release_run_secrets in `finally`."
    )
