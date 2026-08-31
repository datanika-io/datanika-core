"""Shared Postgres harness for the migration tests.

**Why this is a conftest and not an import.** ``test_roundtrip.py`` owned
``roundtrip_db_url``, and ``test_expand_contract.py`` reached for it with
``from ...test_roundtrip import roundtrip_db_url``. That looks like sharing and
isn't: pytest registers an imported fixture as a **separate FixtureDef in the
importing module**, so ``scope="session"`` caches once per module rather than
once per session. ``pytest --setup-plan`` showed it plainly::

    SETUP    S roundtrip_db_url      <- for test_expand_contract.py
    SETUP    S roundtrip_db_url      <- for test_roundtrip.py
    TEARDOWN S roundtrip_db_url
    TEARDOWN S roundtrip_db_url

Two setups meant **two Postgres containers per run**, each with its own ryuk and
its own teardown — and on Windows the second teardown timed out against the
Docker named pipe, failing an otherwise green suite in ``TEARDOWN`` (session
``exitstatus=OK, testsfailed=0``) and leaving containers running.

A fixture defined here is discovered by every module in this directory as **one**
FixtureDef: one container, one teardown. Helpers live here too, so neither test
module has to import from the other.
"""

from __future__ import annotations

import ast
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory

PROJECT_ROOT = Path(__file__).parent.parent.parent


def _docker_available() -> bool:
    """Fast check for a reachable Docker daemon."""
    if not shutil.which("docker"):
        return False
    try:
        return (
            subprocess.run(
                ["docker", "info"],
                capture_output=True,
                timeout=5,
                check=False,
            ).returncode
            == 0
        )
    except (subprocess.TimeoutExpired, OSError):
        return False


def _no_postgres(reason: str) -> None:
    """Skip locally when Postgres is unavailable — but FAIL in CI.

    The ``migration-roundtrip`` job exists to run these files, so if they skip,
    the job exits 0 and reports green having verified nothing — while the thing
    it guards (a downgrade that only matters at 2 AM during a prod rollback)
    goes unchecked. CI always has both Docker and ``DATABASE_URL_SYNC_TEST``, so
    reaching here in CI means the workflow broke, and that must be loud.

    Locally, skipping stays correct: devs without Docker shouldn't be blocked.
    Same reasoning as the connector smoke suite's strict mode (core#407).
    """
    if os.environ.get("CI"):
        pytest.fail(
            f"{reason}\n\n"
            "This is a hard failure because CI is expected to provide Postgres "
            "(DATABASE_URL_SYNC_TEST, or Docker for testcontainers). Skipping here "
            "would let the migration-roundtrip job pass without testing a single "
            "migration. Check the job's env block and service container."
        )
    pytest.skip(reason)


def _run_alembic(
    cmd: list[str], db_url: str, config_path: Path | None = None
) -> subprocess.CompletedProcess:
    """Run an alembic command against the test DB.

    Uses `uv run` so the subprocess inherits the project's venv, and
    passes ``DATABASE_URL_SYNC`` in the environment — alembic's env.py
    reads `settings.database_url_sync` which is sourced from that env var.

    ``config_path`` points alembic at a different ``alembic.ini``. Used by the
    data-preservation controls, which run the **real** migration tree with one
    synthetic head appended, so a destructive downgrade can be demonstrated
    without one living in the repo.

    ⚠️ Running this by hand on Windows: ``export UV_NO_SYNC=1`` first. Without
    it ``uv run`` re-resolves and can gut the worktree venv mid-suite; it
    presents as "my dependencies randomly vanished". Deliberately **not** set
    here — CI installs with ``uv pip install --system`` and has no ``.venv``,
    so ``uv run`` must be free to create one there.
    """
    env = {**os.environ, "DATABASE_URL_SYNC": db_url, "DATABASE_URL": db_url}
    prefix = ["-c", str(config_path)] if config_path else []
    return subprocess.run(
        ["uv", "run", "alembic", *prefix, *cmd],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )


# ---------------------------------------------------------------------------
# core#726 item 2 — the `one_way` escape hatch.
#
# 🚨 This marker was DOCUMENTED AND INERT for four months. `test_roundtrip.py`
# stated as fact that a data-destructive migration "must mark ``one_way = True``
# in their module globals — the test will skip them", and its downgrade-failure
# message told the reader to "skip this test via the ONE_WAY_REVISIONS list in
# test_roundtrip.py". Neither existed anywhere in the repo: three references,
# zero implementations. The failure message is the worse half, because of *when*
# it is read — it fires while someone is establishing whether a bad deploy can
# be rolled back, and sends them looking for a list they will not find, having
# been told by the test itself that it is there.
#
# Design notes, because each is load-bearing:
#
# * **Read by AST, not by import.** Importing a migration executes it. It also
#   makes ``one_way = os.environ.get("X")`` silently truthy — and a classifier
#   that returns something falsy for an unrecognised shape produces a SKIP,
#   which is the same colour as a PASS. A non-literal marker raises instead.
# * **Module scope only.** A ``one_way`` bound inside a function is not a
#   declaration about the migration.
# * **The head is resolved by alembic's own ``ScriptDirectory``**, not by
#   grepping filenames. The migration graph's opinion of "head" is the only one
#   that matters, and it is the same object ``alembic heads`` reports.
# ---------------------------------------------------------------------------

ONE_WAY_MARKER = "one_way"
ONE_WAY_REASON_MARKER = "one_way_reason"


def _script_directory(config_path: Path | None = None) -> ScriptDirectory:
    """Alembic's own view of the migration graph.

    ``script_location`` in ``alembic.ini`` is relative, and alembic resolves it
    against the process cwd — which is not necessarily the project root when
    pytest runs. Resolve it against the ini file instead, so this is correct
    from any directory.
    """
    ini = Path(config_path) if config_path else PROJECT_ROOT / "alembic.ini"
    cfg = AlembicConfig(str(ini))
    location = cfg.get_main_option("script_location")
    if location and not Path(location).is_absolute():
        cfg.set_main_option("script_location", str((ini.parent / location).resolve()))
    return ScriptDirectory.from_config(cfg)


def head_revision(config_path: Path | None = None) -> tuple[str, Path]:
    """``(revision id, file path)`` of the single graph head.

    Multiple heads is its own defect — ``upgrade head`` fails on it — so this
    refuses rather than picking one, which would silently test the wrong
    migration.
    """
    script = _script_directory(config_path)
    heads = script.get_heads()
    assert len(heads) == 1, (
        f"the migration graph has {len(heads)} heads ({heads}); "
        "a round-trip against 'head' is undefined until they are merged"
    )
    revision = script.get_revision(heads[0])
    return revision.revision, Path(revision.path)


def read_module_marker(path: Path, name: str) -> object | None:
    """Read a module-scope literal assignment out of a migration, without
    importing it. Returns ``None`` when the name is not assigned at all.

    Raises ``ValueError`` when the name IS assigned but not to a literal — the
    caller must not get to decide that an expression it cannot evaluate means
    "absent". See the header comment.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            targets = [node.target.id]
        else:
            continue
        if name not in targets:
            continue
        if isinstance(node.value, ast.Constant):
            return node.value.value
        raise ValueError(
            f"{path.name} assigns `{name}` to a non-literal expression. "
            "This marker gates whether the round-trip guards run at all, so it "
            "must be a plain literal that can be read without executing the "
            "migration. Anything else would be classified by guesswork, and a "
            "wrong guess here is a silently skipped guard."
        )
    return None


def one_way_skip_reason(config_path: Path | None = None) -> str | None:
    """Why the round-trip guards do not apply to the current head, or ``None``.

    ``one_way = True`` means **this migration cannot be rolled back** — not
    "its data is not preserved". It disables the schema round-trip as well,
    deliberately: the failure message that sent people looking for
    ``ONE_WAY_REVISIONS`` fires on ``alembic downgrade -1`` *failing*, i.e. on a
    migration with no working downgrade at all. One marker, one meaning.

    Because it switches off a gate, it costs one sentence: ``one_way_reason``
    must also be declared, and it is what the skip line says. An escape hatch
    whose use is invisible is indistinguishable from one that never engaged.
    """
    revision, path = head_revision(config_path)
    marker = read_module_marker(path, ONE_WAY_MARKER)
    if marker is not True:
        if marker not in (None, False):
            raise ValueError(
                f"{path.name} sets `{ONE_WAY_MARKER} = {marker!r}`. Only True or "
                "False are meaningful; anything else is a typo that reads as "
                "'not one-way' and silently keeps a broken downgrade gated."
            )
        return None
    reason = read_module_marker(path, ONE_WAY_REASON_MARKER)
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError(
            f"{path.name} declares `{ONE_WAY_MARKER} = True` without a non-empty "
            f'`{ONE_WAY_REASON_MARKER} = "..."`. Marking a migration one-way '
            "switches off both round-trip guards for the whole release, so the "
            "reason is required and is printed in the skip line someone reads "
            "while deciding whether a rollback is possible."
        )
    return (
        f"revision {revision} ({path.name}) declares `{ONE_WAY_MARKER} = True`: "
        f"{reason.strip()} — it cannot be rolled back, so the round-trip guards "
        "do not apply to this head."
    )


@pytest.fixture(scope="session")
def roundtrip_db_url():
    """Postgres URL for the migration tests.

    Priority:
    1. ``DATABASE_URL_SYNC_TEST`` env var (CI, or manual override)
    2. testcontainers[postgres] auto-provisioned container (local with Docker)
    3. skip the test locally — but **fail** in CI (see ``_no_postgres``)

    Must be a generator (yield-based) on every path — pytest treats a
    function with any `yield` as a generator, and hitting `return` on
    one path while `yield`-ing on another raises
    ``ValueError: fixture did not yield a value``.
    """
    env_url = os.environ.get("DATABASE_URL_SYNC_TEST")
    if env_url:
        yield env_url
        return

    if not _docker_available():
        _no_postgres(
            "Round-trip test requires Postgres. Set DATABASE_URL_SYNC_TEST or run "
            "with Docker Desktop running."
        )

    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        _no_postgres(
            "testcontainers not installed. Install with: "
            "`uv pip install 'testcontainers[postgres]'` or run CI instead."
        )

    # One container per pytest session, shared by every module in this
    # directory. Torn down when the fixture generator exits.
    container = PostgresContainer("postgres:16-alpine")
    container.start()
    try:
        yield container.get_connection_url().replace("postgresql+psycopg2", "postgresql")
    finally:
        container.stop()
