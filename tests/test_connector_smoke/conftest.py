"""Gate for the live-connector smoke suite.

These tests hit real third-party APIs with sandbox credentials. They
are skipped unless ``DATANIKA_CONNECTOR_SMOKE=1`` — the nightly CI
workflow sets it; regular PR CI does not, so adding these tests does
not slow the PR feedback loop.

Per-connector creds come from env vars (see individual test modules for
the vars each expects). In CI, the workflow decodes the
``QA_CONNECTOR_CREDENTIALS`` GitHub secret (base64-encoded env file)
and exports all ``*`` pairs before pytest runs. Locally:

    set -a && source secrets/qa-connectors.env && set +a
    DATANIKA_CONNECTOR_SMOKE=1 uv run pytest tests/test_connector_smoke/ -v
"""

from __future__ import annotations

import os

import pytest

_GATE_ENV = "DATANIKA_CONNECTOR_SMOKE"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip the whole directory unless the gate env var is set.

    Done at collection time so the skip reason is visible in `pytest --collect-only`
    and doesn't count toward "slow test" budget in normal PR runs.
    """
    if os.environ.get(_GATE_ENV) == "1":
        return
    skip_marker = pytest.mark.skip(
        reason=f"Live connector smoke tests skipped. Set {_GATE_ENV}=1 to enable."
    )
    for item in items:
        if "test_connector_smoke" in str(item.fspath):
            item.add_marker(skip_marker)


def _require_env(*names: str) -> dict[str, str]:
    """Fetch required env vars or skip with a clear message."""
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        pytest.skip(f"Missing env vars: {', '.join(missing)}")
    return {n: os.environ[n] for n in names}


@pytest.fixture
def require_env():
    """Fixture wrapper so tests read `env = require_env('FOO', 'BAR')`."""
    return _require_env
