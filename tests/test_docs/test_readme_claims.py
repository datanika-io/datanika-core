"""Guards the countable claims in the root ``README.md`` against the code.

Why this exists: the README said **32 connectors** in three places for months
after the real number reached 36 — including the Airbyte/Fivetran comparison
table, where the stale number undersold us to exactly the audience the table is
written for. Nothing caught it because nothing was watching. The landing repo has
had a connector-count guard for a while; core had none, which is precisely why
the two drifted apart.

The count is derived from :class:`ConnectionType` rather than hardcoded, so
adding a connector fails this test until the README is updated in the same PR.
"""

import re
from pathlib import Path

import pytest

from datanika.models.connection import ConnectionType

README = Path(__file__).resolve().parents[2] / "README.md"

# Connection types that exist in code but are deliberately not part of the
# marketed connector count. Keep this list short and justified — every entry is
# a claim that users cannot actually use something we shipped an enum member
# for. If you add one, say why.
UNMARKETED_TYPES = {
    # core#310 — OpenAPI source generation is behind a deferred feature and has
    # no connector page, setup guide, or docs entry. Counting it would inflate
    # the public number by one.
    "openapi",
}

EXPECTED_CONNECTOR_COUNT = len(ConnectionType) - len(UNMARKETED_TYPES)


@pytest.fixture(scope="module")
def readme() -> str:
    return README.read_text(encoding="utf-8")


def test_unmarketed_types_actually_exist() -> None:
    """A typo in UNMARKETED_TYPES would silently inflate the expected count."""
    members = {t.value for t in ConnectionType}
    unknown = UNMARKETED_TYPES - members
    assert not unknown, f"UNMARKETED_TYPES names types that aren't in ConnectionType: {unknown}"


def test_expected_count_is_sane() -> None:
    """Tripwire against the exclusion list quietly swallowing real connectors."""
    assert len(ConnectionType) - 1 == EXPECTED_CONNECTOR_COUNT
    assert EXPECTED_CONNECTOR_COUNT > 30, "connector count collapsed — check ConnectionType"


def test_every_readme_connector_count_matches_the_enum(readme: str) -> None:
    """All "<n> connectors" claims in the README must agree with the code.

    Deliberately matches *every* occurrence rather than a known set of line
    numbers: the last drift left three stale copies, and a guard that only
    checked one of them would have passed while the README stayed wrong.
    """
    counts = [int(n) for n in re.findall(r"(\d+)\s+[Cc]onnectors\b", readme)]

    assert counts, "no '<n> connectors' claim found in README.md — did the wording change?"

    wrong = sorted({n for n in counts if n != EXPECTED_CONNECTOR_COUNT})
    assert not wrong, (
        f"README claims {wrong} connectors but ConnectionType has "
        f"{EXPECTED_CONNECTOR_COUNT} marketed types "
        f"({len(ConnectionType)} total minus {sorted(UNMARKETED_TYPES)}). "
        f"Found {len(counts)} count claim(s); update all of them."
    )


def test_comparison_table_carries_the_count(readme: str) -> None:
    """The Airbyte/Fivetran row is the one that cost us competitively.

    It is also the easiest to miss in a find-replace, because it is inside a
    table cell rather than prose.
    """
    row = next((line for line in readme.splitlines() if "Extract + Load" in line), None)
    assert row is not None, "comparison table lost its 'Extract + Load' row"
    assert f"{EXPECTED_CONNECTOR_COUNT} connectors" in row, (
        f"comparison table row does not claim {EXPECTED_CONNECTOR_COUNT} connectors: {row!r}"
    )


def test_mcp_install_uses_the_published_package(readme: str) -> None:
    """``uvx datanika-mcp`` is published; the git+subdirectory form is pre-PyPI.

    Same defect class as the connector count: an instruction that was true when
    written and silently became the harder path. Mirrors the guard the landing
    repo keeps on its own MCP docs.
    """
    assert "uvx datanika-mcp" in readme, "README lost the PyPI install command"
    assert "#subdirectory=datanika-mcp" not in readme, (
        "README regressed to the pre-PyPI git+subdirectory install string"
    )
