"""A CI job that collects a Postgres-requiring test must have a Postgres (core#841).

The defect
----------
`ci.yml`'s **`test`** job runs `pytest tests/`, which collects everything under
`tests/test_migrations/`. Several of those files need a real Postgres, obtained from the
session fixture `roundtrip_db_url` in `tests/test_migrations/conftest.py`. That fixture
prefers `DATABASE_URL_SYNC_TEST` and otherwise falls through to a **testcontainers**
container — which is flaky on a shared GHA runner.

`test` had neither the service nor the variable. PR #838 went red with **31 errors** across
two files, and passed on re-run with byte-identical code; QA's PR #874 then added a third
file with six more. Same job, same commit, opposite verdicts, depending on the runner.

`test` is a **required** check on `dev`, which makes this worse than an ordinary flake: the
only cheap response is to re-run, and a team that re-runs reflexively can no longer tell
this apart from a genuine break (`docs/QA_RULES.md` §12). With auto-merge armed (core#884)
it is also a check that silently stops merging things.

Why the invariant is shaped like this
-------------------------------------
The tempting narrow fixes were both rejected on the issue, and the auditor is written so
that neither can be reintroduced quietly:

* **Softening the guard to a `skip`.** `conftest._no_postgres` fails on purpose. A migration
  guard that skips when its database is absent produces a green byte-identical to the one it
  would produce had the migration been broken — this project's signature defect. So the
  invariant demands the *database*, never a quieter test.
* **`--ignore=tests/test_migrations/` on `test`.** `migration-roundtrip` names only 3 of the
  14 files in that directory. Ignoring it would drop the other eleven from *every* required
  check — including the two files in #838's report — and nothing would say so. Hence the
  invariant is expressed over "which files does this invocation collect", so shrinking the
  collection is visible rather than rewarding.

Both halves are derived, not listed. The Postgres-requiring files come from **fixture
usage** (closed transitively over `conftest.py` fixtures), and the collected files come from
**the pytest invocation's own path arguments** — so a new migration test, or a new job, is
bound by this the moment it is added rather than when someone remembers to update a list.

What this does NOT claim
------------------------
It does not prove the service is reachable, only that the job declares one and points the
fixture at it. It also treats `-k` / `-m` narrowing as non-narrowing: a job that collects a
Postgres file and then deselects it still has to declare the database. That is the safe
direction — the failure mode being prevented is a *missing* database, so over-demanding one
costs a service container and under-demanding one costs a required check.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CI = ROOT / ".github" / "workflows" / "ci.yml"
TESTS = ROOT / "tests"

#: The fixture that hands out a live Postgres URL. Everything downstream of it needs one.
ROOT_FIXTURE = "roundtrip_db_url"

#: The variable `roundtrip_db_url` reads before it reaches for testcontainers.
DB_ENV = "DATABASE_URL_SYNC_TEST"

_LINE_COMMENT = re.compile(r"(?m)^\s*#.*$")
_CONTINUATION = re.compile(r"\\\s*\n")


# ── half 1: which test files need a Postgres ────────────────────────────────────────────


def _arg_names(tree: ast.AST) -> set[str]:
    """Every parameter name of every function in the module.

    Parameter names are how pytest injects a fixture, so this is the *only* thing that
    matters — and it is why a plain text scan is wrong: `tests/test_fixture_sharing.py`
    names `roundtrip_db_url` in its docstring while using nothing.
    """
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            a = node.args
            for arg in [*a.posonlyargs, *a.args, *a.kwonlyargs]:
                names.add(arg.arg)
    return names


def _fixture_defs(tree: ast.AST) -> dict[str, set[str]]:
    """`{fixture name: its own parameter names}` for functions decorated `@pytest.fixture`."""
    out: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for dec in node.decorator_list:
            target = dec.func if isinstance(dec, ast.Call) else dec
            attr = getattr(target, "attr", None) or getattr(target, "id", None)
            if attr == "fixture":
                a = node.args
                out[node.name] = {arg.arg for arg in [*a.posonlyargs, *a.args, *a.kwonlyargs]}
                break
    return out


def _tainted_fixture_names(tests_dir: Path) -> set[str]:
    """Fixture names that transitively require a Postgres, closed over every conftest."""
    conftest_defs: dict[str, set[str]] = {}
    for path in sorted(tests_dir.rglob("conftest.py")):
        conftest_defs.update(_fixture_defs(ast.parse(path.read_text(encoding="utf-8"))))

    tainted = {ROOT_FIXTURE}
    changed = True
    while changed:  # fixpoint: a fixture built on a tainted fixture is itself tainted
        changed = False
        for name, params in conftest_defs.items():
            if name not in tainted and params & tainted:
                tainted.add(name)
                changed = True
    return tainted


def postgres_requiring_files(tests_dir: Path = TESTS) -> set[str]:
    """Repo-relative paths of test files that cannot run without a real Postgres."""
    tainted = _tainted_fixture_names(tests_dir)
    found: set[str] = set()
    for path in sorted(tests_dir.rglob("test_*.py")):
        if _arg_names(ast.parse(path.read_text(encoding="utf-8"))) & tainted:
            found.add(path.relative_to(ROOT).as_posix())
    return found


# ── half 2: what each CI job collects, and whether it has a database ────────────────────


def _run_steps(job: dict) -> list[dict]:
    return [s for s in (job.get("steps") or []) if isinstance(s.get("run"), str)]


def _pytest_paths(run: str) -> list[str]:
    """Path arguments of every `pytest` invocation in one `run:` block."""
    text = _CONTINUATION.sub(" ", _LINE_COMMENT.sub("", run))
    paths: list[str] = []
    for line in text.splitlines():
        for chunk in re.split(r"(?:^|[;&|]|\$\()\s*", line):
            tokens = chunk.split()
            if not tokens or Path(tokens[0]).name not in {"pytest", "pytest.exe"}:
                continue
            for token in tokens[1:]:
                if token.startswith("-"):
                    continue
                paths.append(token.strip("'\""))
    return paths


def _collects(paths: list[str], target: str) -> bool:
    return any(target == p or target.startswith(p.rstrip("/") + "/") for p in paths)


def _has_postgres_service(job: dict) -> bool:
    services = job.get("services") or {}
    return any("postgres" in str(spec.get("image", "")) for spec in services.values())


def _env_chain(workflow: dict, job: dict, step: dict) -> dict:
    merged = {}
    for scope in (workflow.get("env"), job.get("env"), step.get("env")):
        if isinstance(scope, dict):
            merged.update(scope)
    return merged


def audit(ci_text: str, pg_files: set[str]) -> dict[str, list[str]]:
    """Jobs that collect a Postgres-requiring file without the means to reach one."""
    workflow = yaml.safe_load(ci_text) or {}
    missing_service: list[str] = []
    missing_env: list[str] = []

    for name, job in (workflow.get("jobs") or {}).items():
        for step in _run_steps(job):
            paths = _pytest_paths(step["run"])
            if not any(_collects(paths, f) for f in pg_files):
                continue
            if not _has_postgres_service(job):
                missing_service.append(name)
            if not _env_chain(workflow, job, step).get(DB_ENV):
                missing_env.append(name)

    return {
        "missing_service": sorted(set(missing_service)),
        "missing_env": sorted(set(missing_env)),
    }


# ── the selectors must not be vacuous ───────────────────────────────────────────────────


@pytest.fixture(scope="module")
def ci_text() -> str:
    return CI.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def pg_files() -> set[str]:
    return postgres_requiring_files()


def test_the_fixture_scan_actually_found_the_postgres_tests(pg_files: set[str]) -> None:
    """A selector that matches nothing reports a clean bill of health."""
    assert len(pg_files) >= 5, sorted(pg_files)
    for known in (
        "tests/test_migrations/test_a7b8_password_changed_at_backfill.py",
        "tests/test_migrations/test_c1d2_seed_plan_byte_allotments.py",
        "tests/test_migrations/test_data_preservation_roundtrip.py",
        "tests/test_migrations/test_roundtrip.py",
    ):
        assert known in pg_files, f"{known} missing from {sorted(pg_files)}"


def test_the_fixture_scan_is_not_a_text_grep(pg_files: set[str]) -> None:
    """`test_fixture_sharing.py` names the fixture in prose and uses nothing."""
    assert "tests/test_fixture_sharing.py" not in pg_files


def test_the_invocation_parser_found_both_pytest_jobs(ci_text: str, pg_files) -> None:
    workflow = yaml.safe_load(ci_text)
    collectors = {
        name
        for name, job in workflow["jobs"].items()
        for step in _run_steps(job)
        if any(_collects(_pytest_paths(step["run"]), f) for f in pg_files)
    }
    assert {"test", "migration-roundtrip"} <= collectors, sorted(collectors)


# ── the invariants ──────────────────────────────────────────────────────────────────────


def test_every_job_collecting_postgres_tests_declares_a_postgres(ci_text, pg_files) -> None:
    report = audit(ci_text, pg_files)
    assert report["missing_service"] == [], (
        "these jobs collect tests that need a real Postgres and declare no postgres "
        "service, so the fixture falls through to the testcontainers path that is flaky "
        "on a GHA runner (core#841):\n  " + "\n  ".join(report["missing_service"])
    )


def test_every_job_collecting_postgres_tests_points_the_fixture_at_it(ci_text, pg_files) -> None:
    report = audit(ci_text, pg_files)
    assert report["missing_env"] == [], (
        f"these jobs collect tests that need a real Postgres without setting {DB_ENV}, so "
        "the fixture ignores the service container sitting right beside it and reaches for "
        "testcontainers anyway (core#841):\n  " + "\n  ".join(report["missing_env"])
    )


# ── negative controls ───────────────────────────────────────────────────────────────────
#
# Every one of these is a shape that WAS or COULD BE on `dev`. A guard nobody has watched
# fail is not evidence.

_PG = {"tests/test_migrations/test_roundtrip.py"}

_PRE_FIX = """
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: pytest tests/ --tb=short -q
  migration-roundtrip:
    services:
      postgres: {image: postgres:16-alpine}
    steps:
      - env: {DATABASE_URL_SYNC_TEST: 'postgresql://test:test@localhost:5432/test'}
        run: pytest tests/test_migrations/test_roundtrip.py -v
"""

_SERVICE_BUT_NO_ENV = """
jobs:
  test:
    services:
      postgres: {image: postgres:16-alpine}
    steps:
      - run: pytest tests/ --tb=short -q
"""

_ENV_BUT_NO_SERVICE = """
jobs:
  test:
    steps:
      - env: {DATABASE_URL_SYNC_TEST: 'postgresql://test:test@localhost:5432/test'}
        run: pytest tests/ --tb=short -q
"""

_COLLECTS_NOTHING_RELEVANT = """
jobs:
  lint:
    steps:
      - run: pytest tests/test_deploy/ -q
"""

_MULTILINE_INVOCATION = """
jobs:
  migration-roundtrip:
    steps:
      - run: |
          # pytest tests/test_migrations/test_roundtrip.py   <- a comment, not a call
          pytest tests/test_migrations/test_expand_contract.py \\
                 tests/test_migrations/test_roundtrip.py -v --tb=long
"""


def test_auditor_rejects_the_pre_fix_shape() -> None:
    """`test` had no service and no env; `migration-roundtrip` was already correct."""
    assert audit(_PRE_FIX, _PG) == {"missing_service": ["test"], "missing_env": ["test"]}


def test_auditor_rejects_a_service_the_fixture_is_not_pointed_at() -> None:
    assert audit(_SERVICE_BUT_NO_ENV, _PG) == {"missing_service": [], "missing_env": ["test"]}


def test_auditor_rejects_an_env_var_with_nothing_behind_it() -> None:
    """Worse than nothing: the fixture connects to localhost:5432 and finds no server."""
    assert audit(_ENV_BUT_NO_SERVICE, _PG) == {"missing_service": ["test"], "missing_env": []}


def test_auditor_ignores_a_job_that_collects_no_postgres_tests() -> None:
    assert audit(_COLLECTS_NOTHING_RELEVANT, _PG) == {"missing_service": [], "missing_env": []}


def test_auditor_sees_a_continued_multiline_invocation_and_skips_comments() -> None:
    """The real `migration-roundtrip` invocation spans three lines with backslashes."""
    assert audit(_MULTILINE_INVOCATION, _PG)["missing_service"] == ["migration-roundtrip"]


def test_auditor_catches_the_ignore_directory_shortcut() -> None:
    """`--ignore=tests/test_migrations/` would make `test` green having dropped 11 files.

    The auditor deliberately does NOT treat `--ignore` as narrowing: the shortcut still
    reports as missing a database, so it cannot be used to quiet this guard.
    """
    text = _PRE_FIX.replace(
        "pytest tests/ --tb=short -q", "pytest tests/ --ignore=tests/test_migrations/ -q"
    )
    assert audit(text, _PG)["missing_service"] == ["test"]
