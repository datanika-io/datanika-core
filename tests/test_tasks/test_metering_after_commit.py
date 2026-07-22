"""A run is metered only after its commit lands (core#522).

The metered announce used to fire **inside the `try`**, before `flush`/`commit`:

    announce("run.upload_completed", ..., status="success", ...)   # cloud meters
    upload.status = UploadStatus.ACTIVE
    session.flush()          # <-- can raise
    session.commit()         # <-- can raise
    except Exception:
        session.rollback()
        execution_service.fail_run(...)   # run ends FAILED

So a commit failure after a successful load ended the run `FAILED` having
**already been billed**. And the rollback did not undo it: `datanika-cloud`'s
handlers call `_get_session()` — a new engine — and commit the ledger row
independently, so it survives the outer rollback.

Found while confirming the core#465 / cloud#84 interaction before the cloud
promotion. Worth being precise about scope: cloud#84 (handlers skipping
`status == "failed"`) **cannot** close this, because at announce time the status
genuinely *is* `"success"` — the failure happens afterwards.

The fix puts the announce in the `try`'s `else` clause, which runs only when the
whole block completed and still runs before `finally` closes the session the
handlers are handed. That makes the ordering structural: no statement added
inside `try` later can slip in front of it, and anything that raises there skips
it.
"""

from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet

from datanika import hooks
from datanika.models.connection import ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.user import Organization
from datanika.services.encryption import EncryptionService

#: The events datanika-cloud subscribes its metering handlers to.
METERED_EVENTS = (
    "run.upload_completed",
    "run.models_completed",
    "run.transformation_completed",
)


@pytest.fixture
def spy_hooks():
    """Record every event fired, with the global bus isolated."""
    saved = {event: list(handlers) for event, handlers in hooks._handlers.items()}
    hooks._handlers.clear()
    fired = []
    for event in (*METERED_EVENTS, "run.failed"):
        hooks.on(event, lambda _e=event, **kw: fired.append(_e))
    yield fired
    hooks._handlers.clear()
    hooks._handlers.update(saved)


@pytest.fixture
def upload_run(db_session):
    """Org + connections + upload + pending run, built through the services.

    Deliberately not importing `setup_upload` from `test_upload_tasks`: pytest
    registers an imported fixture as a second FixtureDef (core#449), and these
    services set fields — `connections.direction` via `infer_direction` — that
    hand-built rows miss.
    """
    import uuid

    from datanika.services.connection_service import ConnectionService
    from datanika.services.execution_service import ExecutionService
    from datanika.services.upload_service import UploadService

    encryption = EncryptionService(Fernet.generate_key().decode())
    conn_svc = ConnectionService(encryption)
    upload_svc = UploadService(conn_svc)

    org = Organization(name="Acme", slug=f"acme-522-{uuid.uuid4().hex[:8]}")
    db_session.add(org)
    db_session.flush()

    src = conn_svc.create_connection(
        db_session, org.id, "S", ConnectionType.POSTGRES, {"host": "src", "port": 5432}
    )
    dst = conn_svc.create_connection(
        db_session, org.id, "D", ConnectionType.BIGQUERY, {"project": "p", "dataset": "d"}
    )
    upload = upload_svc.create_upload(
        db_session, org.id, "test", "desc", src.id, dst.id, {"write_disposition": "append"}
    )
    run = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, upload.id)
    return org, run, encryption


class TestMeteringFollowsTheCommit:
    def test_a_successful_run_still_meters(self, db_session, upload_run, spy_hooks):
        """The guard must not stop billing working runs."""
        org, run, encryption = upload_run

        with (
            patch("datanika.tasks.upload_tasks.DltRunnerService") as runner,
            patch("datanika.tasks.upload_tasks._sync_catalog_after_upload", return_value=1),
        ):
            runner.return_value.execute.return_value = {
                "rows_loaded": 12,
                "load_info": None,
            }
            from datanika.tasks.upload_tasks import run_upload

            run_upload(run.id, org.id, session=db_session, encryption=encryption)

        assert "run.upload_completed" in spy_hooks, "a successful run was not metered"

    def test_a_failure_after_the_load_does_not_meter(self, db_session, upload_run, spy_hooks):
        """The window: load succeeds, the write that follows it fails.

        `flush` is made to raise at the point the task marks the upload ACTIVE
        — the same position a commit failure occupies. Before core#522 the
        announce had already fired by then.
        """
        from datanika.models.upload import Upload, UploadStatus

        org, run, encryption = upload_run

        real_flush = db_session.flush
        blown = {"once": False}

        def flaky_flush(*args, **kwargs):
            # Fail exactly the write that follows the metering point — marking
            # the upload ACTIVE — rather than counting flushes, which would
            # silently stop testing this the moment the task gained one.
            #
            # One-shot: a transient fault, so the error path that follows can
            # still record the failure. Raising every time would blow up
            # `fail_run` too and the task would never reach its own handling.
            if not blown["once"] and any(
                isinstance(obj, Upload) and obj.status == UploadStatus.ACTIVE
                for obj in db_session.dirty
            ):
                blown["once"] = True
                raise RuntimeError("connection reset by peer")
            return real_flush(*args, **kwargs)

        with (
            patch("datanika.tasks.upload_tasks.DltRunnerService") as runner,
            patch("datanika.tasks.upload_tasks._sync_catalog_after_upload", return_value=1),
            patch.object(db_session, "flush", flaky_flush),
        ):
            runner.return_value.execute.return_value = {
                "rows_loaded": 12,
                "load_info": None,
            }
            from datanika.tasks.upload_tasks import run_upload

            run_upload(run.id, org.id, session=db_session, encryption=encryption)

        metered = [e for e in spy_hooks if e in METERED_EVENTS]
        assert metered == [], (
            f"a run that ended FAILED was metered via {metered} — cloud commits "
            "the ledger row on its own session, so the rollback does not undo it"
        )

    def test_the_spy_would_have_seen_a_metered_event(self, spy_hooks):
        """Anti-vacuity: a spy that never fires proves nothing above."""
        from datanika.hooks import announce

        announce("run.upload_completed", org_id=1, run_id=1, status="success")
        assert spy_hooks == ["run.upload_completed"]


class TestStructuralOrdering:
    @pytest.mark.parametrize(
        "module",
        [
            "datanika.tasks.upload_tasks",
            "datanika.tasks.pipeline_tasks",
            "datanika.tasks.transformation_tasks",
        ],
    )
    def test_the_metered_announce_lives_in_an_else_clause(self, module):
        """Pin the construct, not just today's behaviour.

        The behavioural test above covers uploads. This covers all three task
        modules at once and states *why* the code is shaped this way, so a
        future refactor that moves an announce back inside `try` fails here
        with the reason attached.
        """
        import ast
        import importlib
        import pathlib

        path = pathlib.Path(importlib.import_module(module).__file__)
        source = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

        announced_in_else = set()
        for node in ast.walk(source):
            if not isinstance(node, ast.Try) or not node.orelse:
                continue
            for inner in ast.walk(ast.Module(body=node.orelse, type_ignores=[])):
                if (
                    isinstance(inner, ast.Call)
                    and getattr(inner.func, "id", None) == "announce"
                    and inner.args
                    and isinstance(inner.args[0], ast.Constant)
                ):
                    announced_in_else.add(inner.args[0].value)

        metered_here = announced_in_else & set(METERED_EVENTS)
        assert metered_here, (
            f"{module} announces no metered event from a try/else. Metering must "
            "follow the commit (core#522): announcing inside `try` bills runs that "
            "then fail, and cloud commits the ledger row on its own session so the "
            "rollback does not undo it."
        )
