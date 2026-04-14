"""Smoke suite fixtures.

Scoped to `scripts/smoke/` only — not picked up by the main test suite
(`pyproject.toml` pins `testpaths = ["tests"]`). Run explicitly via
``uv run pytest scripts/smoke/``.
"""

from __future__ import annotations

import os

import httpx
import pytest

CORE_DEFAULT = "https://staging-app.datanika.io"
LANDING_DEFAULT = "https://datanika.io"
TIMEOUT_SECONDS = 15.0


def _normalize(url: str) -> str:
    return url.rstrip("/")


@pytest.fixture(scope="session")
def core_base_url() -> str:
    return _normalize(os.environ.get("DATANIKA_SMOKE_CORE_URL", CORE_DEFAULT))


@pytest.fixture(scope="session")
def landing_base_url() -> str:
    return _normalize(os.environ.get("DATANIKA_SMOKE_LANDING_URL", LANDING_DEFAULT))


@pytest.fixture(scope="session")
def core_client(core_base_url: str):
    with httpx.Client(
        base_url=core_base_url,
        timeout=TIMEOUT_SECONDS,
        follow_redirects=True,
        headers={"User-Agent": "datanika-qa-smoke/1"},
    ) as client:
        yield client


@pytest.fixture(scope="session")
def landing_client(landing_base_url: str):
    with httpx.Client(
        base_url=landing_base_url,
        timeout=TIMEOUT_SECONDS,
        follow_redirects=True,
        headers={"User-Agent": "datanika-qa-smoke/1"},
    ) as client:
        yield client
