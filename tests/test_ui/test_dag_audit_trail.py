"""Rewiring the run graph must leave a record (core#934, SPEC_AUDIT_TRAIL §3).

`dag_state.py` contained **0** occurrences of `_audit` while both of its handlers commit.
Every other mutating state class in the product audits — ten of them, 33 call sites.

The dependency graph is the one object here whose corruption is **silent by design**, and
`pages/dag.py`'s own dialog docstring says so: *"nothing breaks, nothing errors, and no row
disappears from any other page. The downstream job simply stops waiting for the upstream one
and starts running against whatever data happens to be there — a silently wrong result rather
than a failure."* So the question arrives days later as *"why is this model wrong?"*, and the
audit log is exactly the instrument you reach for to ask *"did somebody change the graph?"*
It has never had an answer.

⚠️ **The real `_audit` is bound onto the stand-in**, not stubbed. `test_leave_org_session.py`
stubs it — correctly, for what that file tests — and that is precisely why nothing in this
suite could see core#1127, where the chokepoint itself dropped the row. A stub of the
chokepoint cannot fail the way the chokepoint failed.

⚠️ **The stand-ins are deliberately not `MagicMock`s.** A MagicMock answers every attribute
truthily and every call successfully, so a test built on one passes against code that never
touched the object.

What each test can and cannot see
--------------------------------
SPEC_AUDIT_TRAIL §4 asks for this to be stated, because a suite whose members overlap is one
test with three names:

* **row shape (add / remove)** — kills the missing call. **Blind to** an audit written in a
  session of its own (that row exists too) *and* to a call placed after ``commit()``.
* **one-session** — kills exactly that second-session audit, and nothing else does.
* **rows-at-commit** — kills the call placed after ``commit()``, and nothing else does.
* **conditional removal** — kills auditing, or toasting, a removal that removed nothing.
* **failed commit** — kills an audit row surviving a mutation that did not happen.

Every one of those is a **measured** mutant, not a prediction: N1–N7 in the sweep for this
issue, each red on exactly the row above it.

⚠️ **Two of SPEC_AUDIT_TRAIL §4's claims are corrected here, and both were optimistic.**

1. **§4.1 says the row-count test kills "the audit call placed after ``session.commit()``".**
   It does not — not against a harness whose ``commit()`` is a ``flush()``, which every audit
   harness in this repo uses because the fixture owns the real transaction. Measured: really
   moving the call below ``commit()`` left all nine other tests green. The rows-at-commit
   watermark is what catches it.
2. **§4.3 wants the rollback test to kill the second-session implementation "from the other
   side".** It cannot *here*: the fixture is a single SQLite connection, so a second session
   in this harness is the same transaction and rolls back with it. The one-session test is
   what kills that mutant, measured doing so on core#1127 (M3) and again here (N3, N4).

Saying which test carries the weight beats implying two of them do.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import func, select

import datanika.ui.state.dag_state as dag_module
from datanika.models.audit_log import AuditAction, AuditLog
from datanika.models.connection import ConnectionType
from datanika.models.dependency import Dependency, NodeType
from datanika.models.transformation import Materialization
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService
from datanika.services.dependency_service import DependencyService
from datanika.services.encryption import EncryptionService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from datanika.ui.state.base_state import BaseState

ACTOR_ID = 77


# --------------------------------------------------------------------------------------
# Fixtures — real service, real rows. add_dependency validates both nodes exist.
# --------------------------------------------------------------------------------------


@pytest.fixture
def svc():
    encryption = EncryptionService(Fernet.generate_key().decode())
    conn_svc = ConnectionService(encryption)
    return DependencyService(UploadService(conn_svc), TransformationService()), conn_svc


@pytest.fixture
def org(db_session):
    o = Organization(name="Acme", slug="acme-dag-audit")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def graph(db_session, org, svc):
    dep_svc, conn_svc = svc
    src = conn_svc.create_connection(db_session, org.id, "Src", ConnectionType.POSTGRES, {"h": "x"})
    dst = conn_svc.create_connection(
        db_session, org.id, "Dst", ConnectionType.BIGQUERY, {"project": "p", "dataset": "d"}
    )
    upload = UploadService(conn_svc).create_upload(
        db_session, org.id, "nightly orders", "desc", src.id, dst.id, {}
    )
    transformation = TransformationService().create_transformation(
        db_session, org.id, "dds_orders", "SELECT 1", Materialization.VIEW
    )
    return SimpleNamespace(svc=dep_svc, upload=upload, transformation=transformation, org=org)


# --------------------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------------------


class _TestSession:
    """``db_session`` proxy whose ``commit()`` is a ``flush()``.

    The shared fixture isolates each test in one outer transaction it rolls back at teardown;
    a real ``commit()`` releases that savepoint and leaks rows into the next test.
    """

    def __init__(self, session, commit_raises=False):
        self._session = session
        self._commit_raises = commit_raises
        #: How many audit rows were already **in this transaction** each time ``commit()``
        #: was called.
        #:
        #: 🚨 Without this the harness is **blind to an audit call placed after
        #: ``commit()``** — the defect SPEC_AUDIT_TRAIL §4.1 says the row-count test
        #: catches. It does not, not here: ``commit()`` is a ``flush()`` (the fixture owns
        #: the real transaction), so a row added afterwards is still flushed into the same
        #: transaction and a later query finds it. Measured: the mutation that really moves
        #: the call below ``commit()`` left all nine other tests green.
        #:
        #: 🔑 **It counts ROWS, not objects, and the first version counted objects and was
        #: wrong.** ``any(isinstance(o, AuditLog) for o in session.identity_map.values())``
        #: reads **False on correct code**: ``identity_map`` is a *weak* dict, and
        #: ``AuditService.log_action`` flushes the row and returns it while every caller
        #: here discards the return — so the object is collected out of the map although
        #: its row is sitting in the transaction. Isolated probe: ``identity_map`` size 0,
        #: ``session.new`` empty, and ``SELECT count(*)`` = 1. **Presence of the object is
        #: not presence of the row**, and only the query distinguishes them.
        self.audit_rows_at_commit: list[int] = []

    def commit(self):
        self.audit_rows_at_commit.append(
            self._session.execute(select(func.count()).select_from(AuditLog)).scalar_one()
        )
        if self._commit_raises:
            raise RuntimeError("commit failed")
        self._session.flush()

    def __getattr__(self, name):
        return getattr(self._session, name)


class _SessionFactory:
    """Stands in for ``get_sync_session``, and **counts**.

    The counting is the point: a context manager that returns the same session every time
    cannot tell one ``with get_sync_session()`` block from two, so the "audit in a session of
    its own" implementation would be invisible to it.

    ``first_commit_raises`` patches the commit of the **first** session handed out only —
    SPEC_AUDIT_TRAIL §4.3's precision. Patching ``Session.commit`` class-wide breaks a second
    session too, and the mutant then passes.
    """

    def __init__(self, db_session, first_commit_raises=False):
        self._db_session = db_session
        self._first_commit_raises = first_commit_raises
        self.handed_out: list[_TestSession] = []

    def __call__(self):
        return self

    def __enter__(self):
        raises = self._first_commit_raises and not self.handed_out
        s = _TestSession(self._db_session, commit_raises=raises)
        self.handed_out.append(s)
        return s

    def __exit__(self, *_exc):
        return False


class _State:
    """Stand-in for DagState — only what the two handlers may touch."""

    _audit = staticmethod(BaseState._audit)
    #: The real payload builder — it IS part of the code under test, and re-implementing it
    #: here would give the assertions a second implementation to agree with instead of the
    #: one that ships. ``staticmethod`` for the same reason as ``_audit``: read off the
    #: class it is a plain function, and assigning it bare would rebind ``self``.
    _edge_payload = staticmethod(dag_module.DagState._edge_payload)

    def __init__(self, graph, *, dependencies=None, tf_value="", tf_unit="minutes"):
        self._graph = graph
        self.dependencies = dependencies or []
        self.error_message = "stale"
        self.form_upstream_type = "upload"
        self.form_upstream_name = "nightly orders"
        self.form_downstream_type = "transformation"
        self.form_downstream_name = "dds_orders"
        self.form_check_timeframe_value = tf_value
        self.form_check_timeframe_unit = tf_unit
        self._name_to_id = {
            "upload": {"nightly orders": graph.upload.id},
            "transformation": {"dds_orders": graph.transformation.id},
        }
        self.toasts: list[tuple[str, str]] = []
        self.reloaded = 0

    async def _check_role(self, _role):
        return True

    async def _get_org_id(self):
        return self._graph.org.id

    async def _actor_id(self):
        return ACTOR_ID

    def _get_service(self):
        return self._graph.svc

    async def load_dependencies(self):
        self.reloaded += 1

    async def _saved_toast(self, key, fallback):
        self.toasts.append(("saved", key))
        return ("saved", key)

    async def _deleted_toast(self, key, fallback):
        self.toasts.append(("deleted", key))
        return ("deleted", key)

    async def _translated(self, key, fallback):
        return f"<{key}>"

    def _safe_error(self, exc, fallback):
        return f"{fallback}: {exc}"


async def _drive(handler, st, db_session, factory):
    with patch.object(dag_module, "get_sync_session", factory):
        result = handler(st)
        if hasattr(result, "__aiter__"):
            async for _ in result:
                pass
        else:
            await result
    st.sessions = factory.handed_out
    return st


async def _add(db_session, graph, **kw):
    st = _State(graph, **kw)
    factory = _SessionFactory(db_session)
    return await _drive(dag_module.DagState.add_dependency.fn, st, db_session, factory)


async def _remove(db_session, graph, dep_id, *, dependencies=None, first_commit_raises=False):
    st = _State(graph, dependencies=dependencies)
    factory = _SessionFactory(db_session, first_commit_raises=first_commit_raises)

    def handler(state):
        return dag_module.DagState.remove_dependency.fn(state, dep_id)

    return await _drive(handler, st, db_session, factory)


def _rows(db_session, org_id):
    return list(db_session.execute(select(AuditLog).where(AuditLog.org_id == org_id)).scalars())


def _item(dep, upstream_name, downstream_name):
    return SimpleNamespace(id=dep.id, upstream_name=upstream_name, downstream_name=downstream_name)


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------


class TestAddDependencyIsRecorded:
    @pytest.mark.asyncio
    async def test_it_writes_exactly_one_create_row_naming_both_ends(self, db_session, graph):
        """AC1 / §3.2. `#12` identifies an edge to nobody — the same argument the confirmation
        dialog already makes, applied to the record instead of the prompt."""
        st = await _add(db_session, graph)
        assert st.error_message == "", f"the add itself failed: {st.error_message}"

        rows = _rows(db_session, graph.org.id)
        assert len(rows) == 1, f"expected one audit row, found {len(rows)}"
        (row,) = rows
        dep = db_session.execute(select(Dependency)).scalars().one()

        assert row.action is AuditAction.CREATE
        assert row.resource_type == "dependency"
        assert row.resource_id == dep.id
        assert row.user_id == ACTOR_ID
        assert row.old_values is None, "nothing existed before"
        assert row.new_values == {
            "upstream_type": "upload",
            "upstream_id": graph.upload.id,
            "upstream_name": "nightly orders",
            "downstream_type": "transformation",
            "downstream_id": graph.transformation.id,
            "downstream_name": "dds_orders",
        }

    @pytest.mark.asyncio
    async def test_a_supplied_timeframe_reaches_the_payload(self, db_session, graph):
        """§3.2's conditional pair. Absent above, present here — the two rows together are
        what say the ``if`` is real rather than the key being unconditionally written."""
        st = await _add(db_session, graph, tf_value="30", tf_unit="minutes")
        assert st.error_message == ""
        (row,) = _rows(db_session, graph.org.id)
        assert row.new_values["check_timeframe_value"] == 30
        assert row.new_values["check_timeframe_unit"] == "minutes"

    @pytest.mark.asyncio
    async def test_a_rejected_add_writes_nothing(self, db_session, graph):
        """The service raises on a self-reference. No edge, so no row."""
        st = _State(graph)
        st.form_downstream_type = "upload"
        st.form_downstream_name = "nightly orders"
        st._name_to_id["upload"]["nightly orders"] = graph.upload.id
        factory = _SessionFactory(db_session)
        await _drive(dag_module.DagState.add_dependency.fn, st, db_session, factory)

        assert st.error_message, "a refused add must say so"
        assert _rows(db_session, graph.org.id) == []


class TestRemoveDependencyIsRecorded:
    @pytest.fixture
    def edge(self, db_session, graph):
        return graph.svc.add_dependency(
            db_session,
            graph.org.id,
            NodeType.UPLOAD,
            graph.upload.id,
            NodeType.TRANSFORMATION,
            graph.transformation.id,
        )

    @pytest.mark.asyncio
    async def test_it_writes_exactly_one_delete_row_naming_both_ends(self, db_session, graph, edge):
        """AC2 / §3.3."""
        st = await _remove(
            db_session,
            graph,
            edge.id,
            dependencies=[_item(edge, "nightly orders", "dds_orders")],
        )
        rows = _rows(db_session, graph.org.id)
        assert len(rows) == 1, f"expected one audit row, found {len(rows)}"
        (row,) = rows

        assert row.action is AuditAction.DELETE
        assert row.resource_type == "dependency"
        assert row.resource_id == edge.id
        assert row.user_id == ACTOR_ID
        assert row.new_values is None
        assert row.old_values == {
            "upstream_type": "upload",
            "upstream_id": graph.upload.id,
            "upstream_name": "nightly orders",
            "downstream_type": "transformation",
            "downstream_id": graph.transformation.id,
            "downstream_name": "dds_orders",
        }
        assert ("deleted", "dag.deleted_toast") in st.toasts

    @pytest.mark.asyncio
    async def test_an_unresolvable_name_is_written_as_empty_not_omitted(
        self, db_session, graph, edge
    ):
        """§3.4. A key that is *sometimes absent* makes every future reader write a
        ``.get()``; core#694 says there are no readers yet, so this is the cheapest moment
        to fix the shape."""
        st = await _remove(db_session, graph, edge.id, dependencies=[])
        (row,) = _rows(db_session, graph.org.id)
        assert row.old_values["upstream_name"] == ""
        assert row.old_values["downstream_name"] == ""
        assert row.old_values["upstream_id"] == graph.upload.id, (
            "the ids come from the persisted row and must survive a name lookup miss"
        )
        assert st.toasts, "the removal still succeeded"

    @pytest.mark.asyncio
    async def test_a_removal_that_removed_nothing_writes_no_row_and_does_not_claim_success(
        self, db_session, graph
    ):
        """🔑 **AC1's second half, and it is one criterion, not two.**

        ``DependencyService.remove_dependency`` returns ``False`` when the row does not
        exist, is already soft-deleted, or belongs to another org. The handler discarded
        that return and yielded *"Dependency removed"* regardless.

        Auditing conditionally while toasting unconditionally is **worse than either half
        alone**: the user is told the edge is gone and the record says it is not. So both
        assertions live in one test — splitting them would let a half-fix go green.
        """
        st = await _remove(db_session, graph, 999_999)

        assert _rows(db_session, graph.org.id) == [], "nothing was removed, so nothing to record"
        assert ("deleted", "dag.deleted_toast") not in st.toasts, (
            "the success toast fired for a removal that removed nothing"
        )
        assert st.error_message == "<dag.remove_missing>", (
            "the user must be told the dependency was NOT removed, in their own locale"
        )


class TestTheRecordRidesTheMutationsTransaction:
    @pytest.mark.asyncio
    async def test_add_uses_exactly_one_session(self, db_session, graph):
        """SPEC_AUDIT_TRAIL §2.1, asserted structurally.

        🔑 A row-exists assertion is satisfied by the bug it is meant to name — an audit
        written in its own session produces a row too. Measured on core#1127: against that
        mutant both row-shape tests stayed green and only this one went red.
        """
        st = await _add(db_session, graph)
        assert len(st.sessions) == 1, (
            f"add_dependency opened {len(st.sessions)} sessions; the audit row must ride the "
            "mutation's own transaction"
        )

    @pytest.mark.asyncio
    async def test_remove_uses_exactly_one_session(self, db_session, graph):
        edge = graph.svc.add_dependency(
            db_session,
            graph.org.id,
            NodeType.UPLOAD,
            graph.upload.id,
            NodeType.TRANSFORMATION,
            graph.transformation.id,
        )
        st = await _remove(db_session, graph, edge.id)
        assert len(st.sessions) == 1, f"remove_dependency opened {len(st.sessions)} sessions"

    @pytest.mark.asyncio
    async def test_the_audit_row_exists_before_the_handler_commits(self, db_session, graph):
        """The row must be *in* the transaction being committed, not added after it.

        ⚠️ **SPEC_AUDIT_TRAIL §4.1 claims the row-count test kills this. It does not** — not
        against a harness whose ``commit()`` is a ``flush()``, which every audit harness in
        this repo uses because the fixture owns the real transaction. Measured: moving the
        `_audit` call below `session.commit()` left all nine other tests green.

        In production the consequence is total: `log_action` adds and flushes, the `with`
        block exits without another commit, and the row is discarded — a handler that
        audits after committing writes nothing at all, exactly like core#1127, and for a
        completely different reason.
        """
        st = await _add(db_session, graph)
        assert st.error_message == ""
        (session,) = st.sessions
        assert session.audit_rows_at_commit == [1], (
            "the transaction held no audit row at the moment commit() was called, so the "
            "audit is being written after the transaction it describes — in production the "
            "`with` block then exits without another commit and the row is discarded "
            f"(rows at commit={session.audit_rows_at_commit})"
        )

    @pytest.mark.asyncio
    async def test_a_failed_commit_leaves_no_durable_audit_row(self, db_session, graph):
        """SPEC_AUDIT_TRAIL §4.3 — an audit write outside the mutation's transaction is a
        log of things that did not happen.

        The savepoint is what makes this real: ``_TestSession.commit`` is a ``flush``, so a
        row is visible until something rolls it back. Rolling back to a savepoint taken
        before the call is the harness's stand-in for the failed transaction.
        """
        edge = graph.svc.add_dependency(
            db_session,
            graph.org.id,
            NodeType.UPLOAD,
            graph.upload.id,
            NodeType.TRANSFORMATION,
            graph.transformation.id,
        )
        db_session.flush()
        nested = db_session.begin_nested()

        # ⚠️ `remove_dependency` has **no** try/except — unlike `add_dependency`, which does.
        # So a raising commit propagates rather than becoming an `error_message`. That
        # asymmetry is real and pre-existing; it is NOT fixed here, because changing a
        # destructive handler's error semantics is a Product decision about what the user
        # sees, not a consequence of adding an audit row. Recorded on the issue instead.
        #
        # ⚠️ The state is built here rather than through `_remove` so the assertions below
        # read the REAL object the handler wrote to. Substituting a fresh stand-in after the
        # raise would make the toast assertion vacuous — it would be empty by construction
        # and could never fail.
        st = _State(graph, dependencies=[_item(edge, "nightly orders", "dds_orders")])
        factory = _SessionFactory(db_session, first_commit_raises=True)
        with pytest.raises(RuntimeError, match="commit failed"):
            await _drive(
                lambda s: dag_module.DagState.remove_dependency.fn(s, edge.id),
                st,
                db_session,
                factory,
            )

        nested.rollback()
        assert _rows(db_session, graph.org.id) == [], (
            "an audit row survived a mutation whose commit raised"
        )
        live = db_session.execute(
            select(Dependency).where(Dependency.id == edge.id, Dependency.deleted_at.is_(None))
        ).scalar_one_or_none()
        assert live is not None, "the edge must still be live after a failed commit"
        assert ("deleted", "dag.deleted_toast") not in st.toasts, (
            "a failed removal must not report success"
        )
