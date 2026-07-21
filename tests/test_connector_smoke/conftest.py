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

## Missing dependencies FAIL — they do not skip

Once the gate is on, a missing credential or an un-importable client
library is a **hard failure**, not a skip.

That is deliberate, and it is the whole point of a monitoring probe. A
probe that skips when its dependency is missing reports green while
testing nothing — strictly worse than one that fails, because the
matrix looks healthiest exactly when it has stopped watching.

Real instance (core#407): the nightly pinned ``kafka-python>=2.0``, but
2.0.x cannot import on Python 3.12 (``kafka.vendor.six.moves`` is
gone). The Kafka probe would have died at import and its
``except ImportError: pytest.skip`` would have turned a completely
broken connector into a passing nightly.

The failure this guards against is mundane and therefore likely:
someone rotates a credential and renames its var, or the base64 bundle
decodes partially. Under skip-on-missing, that connector silently drops
out of the matrix and nobody finds out until a customer does.

Running a subset locally without every credential is the one legitimate
reason to skip instead, so that is an explicit opt-in:

    DATANIKA_CONNECTOR_SMOKE=1 DATANIKA_CONNECTOR_SMOKE_LENIENT=1 pytest ...

Note the direction: lenient mode must be switched **on**. Dropping or
misspelling any env var can only make the suite stricter, never
quieter — so a typo in the workflow surfaces as a red nightly rather
than a silent one. Do not invert this.
"""

from __future__ import annotations

import importlib
import os
from typing import Any

import pytest

_GATE_ENV = "DATANIKA_CONNECTOR_SMOKE"
_LENIENT_ENV = "DATANIKA_CONNECTOR_SMOKE_LENIENT"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip the whole directory unless the gate env var is set.

    Done at collection time so the skip reason is visible in `pytest --collect-only`
    and doesn't count toward "slow test" budget in normal PR runs.

    This gate is the one skip that stays a skip: PR CI legitimately has no
    sandbox credentials. The nightly workflow guards the corresponding risk
    — the gate silently being off there — by failing the job if *any* test
    reports skipped. See `.github/workflows/nightly-connector-smoke.yml`.
    """
    if os.environ.get(_GATE_ENV) == "1":
        return
    skip_marker = pytest.mark.skip(
        reason=f"Live connector smoke tests skipped. Set {_GATE_ENV}=1 to enable."
    )
    for item in items:
        if "test_connector_smoke" in str(item.fspath):
            item.add_marker(skip_marker)


def _missing_dependency(reason: str) -> None:
    """Fail on a missing dependency — or skip, if lenient mode is on.

    See the module docstring for why failing is the default. Never call
    ``pytest.skip`` directly from a probe on a missing dependency: that
    is precisely the silent-green bug this helper exists to prevent.
    """
    if os.environ.get(_LENIENT_ENV) == "1":
        pytest.skip(f"{reason} [lenient mode]")
    pytest.fail(
        f"{reason}\n\n"
        f"Failing rather than skipping is intentional: a smoke probe that skips "
        f"on a missing dependency reports green while testing nothing, so the "
        f"matrix looks healthy exactly when it has stopped watching.\n"
        f"If this is a deliberate partial run on a machine without every "
        f"credential, set {_LENIENT_ENV}=1 to downgrade this to a skip."
    )


def _require_env(*names: str) -> dict[str, str]:
    """Fetch required env vars, or fail loudly if any are absent."""
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        _missing_dependency(f"Missing env vars: {', '.join(missing)}")
    return {n: os.environ[n] for n in names}


def _require_import(module: str, attr: str | None = None) -> Any:
    """Import a connector client library, or fail loudly if it is absent.

    Use this instead of ``try: import x / except ImportError: pytest.skip``.
    An un-importable client library means the probe *cannot run*, which is
    a broken probe — not an absent one, and not something to pass over.
    """
    try:
        mod = importlib.import_module(module)
    except ImportError as exc:
        _missing_dependency(f"Cannot import {module!r} ({exc}). Is it in the nightly's deps?")
    return getattr(mod, attr) if attr else mod


@pytest.fixture
def require_env():
    """Fixture wrapper so tests read `env = require_env('FOO', 'BAR')`."""
    return _require_env


@pytest.fixture
def require_import():
    """Fixture wrapper so tests read `Client = require_import('mod', 'Client')`."""
    return _require_import
