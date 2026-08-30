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

import ast
import re
from pathlib import Path

import pytest

from datanika.models.connection import ConnectionType
from datanika.services.connection_service import WITHDRAWN_SOURCE_TYPES

README = Path(__file__).resolve().parents[2] / "README.md"

# Connection types that exist in code but are deliberately not part of the
# marketed connector count. Keep this list short and justified — every entry is
# a claim that users cannot actually use something we shipped an enum member
# for. If you add one, say why.
#
# This list covers only types held back *before* launch. Types that were offered
# and then withdrawn are not listed here — they come from
# ``WITHDRAWN_SOURCE_TYPES`` below, which is the service's own set.
UNMARKETED_TYPES = {
    # core#310 — OpenAPI source generation is behind a deferred feature and has
    # no connector page, setup guide, or docs entry. Counting it would inflate
    # the public number by one.
    "openapi",
}

# Withdrawn types are imported, never re-listed. `connection_service` already
# owns the set that removes a type from `SOURCE_TYPES`, `CONFIG_SCHEMAS` and the
# picker, so importing it makes the README count fall out of the *same* fact
# that makes the connector unreachable in the app.
#
# The alternative — adding "google_ads" to UNMARKETED_TYPES by hand — would make
# the two numbers agree by coincidence, and would silently stop agreeing the next
# time a connector is withdrawn (core#555/#567 is unlikely to be the last: a
# withdrawal is the honest answer whenever a connector cannot work with the
# credentials we collect). With the import, the next withdrawal fails this test
# until the README is updated in the same PR, which is the entire point.
EXCLUDED_FROM_COUNT = UNMARKETED_TYPES | set(WITHDRAWN_SOURCE_TYPES)

EXPECTED_CONNECTOR_COUNT = len(ConnectionType) - len(EXCLUDED_FROM_COUNT)


@pytest.fixture(scope="module")
def readme() -> str:
    return README.read_text(encoding="utf-8")


def test_excluded_types_actually_exist() -> None:
    """A typo in either exclusion set would silently inflate the expected count.

    Covers the imported withdrawn set too: a withdrawal that misspells a type
    would remove nothing from the app while still shrinking this count, so the
    README would be "corrected" to a number matching neither.
    """
    members = {t.value for t in ConnectionType}
    unknown = EXCLUDED_FROM_COUNT - members
    assert not unknown, f"excluded types that aren't in ConnectionType: {sorted(unknown)}"


def test_exclusions_stay_small_and_deliberate() -> None:
    """Tripwire against the exclusion sets quietly swallowing real connectors.

    Deliberately not ``len(ConnectionType) - 1``: that hardcoded the one
    exclusion that existed when this guard was written, so the first withdrawal
    (core#567) broke it for the wrong reason. The invariant worth pinning is
    that exclusions stay few and the marketed count stays plausible, not that
    there is exactly one.
    """
    assert len(EXCLUDED_FROM_COUNT) <= 3, (
        f"too many connectors excluded from the marketed count: "
        f"{sorted(EXCLUDED_FROM_COUNT)}. Each one is something we shipped an "
        f"enum member for and do not sell — if this is growing, say why."
    )
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


# ---------------------------------------------------------------------------
# Claims about OTHER people's products — a different defect class
# ---------------------------------------------------------------------------
#
# Found 2026-08-30 by Growth on the landing repo and swept into core here. The
# comparison row said Airbyte "400+" and Fivetran "500+" while both vendors now
# publish 600+/700+ — stale in the direction that makes our own table look
# either careless or dishonest to the one reader who checks.
#
# ⚠️ The obvious guard is the wrong one, and the landing repo has the scar.
# There, two tests asserted `expect(html).toContain("500+")`. When Growth
# corrected the number to the true one, **the suite went red for the correct
# change.** Growth's phrasing is the right one to keep: *a number pinned in a
# test with no source is a lock, not a guard.* It is the mirror image of an
# over-mocked test — one passes on wrong code, the other fails on right code,
# and neither tracks truth.
#
# So this pins **nothing about the competitors' numbers.** It requires that any
# such claim carries a footnote and that the footnote carries a date, which is
# what lets a reader decide whether to trust it. Staleness becomes visible to
# the reader instead of enforced against the person fixing it.
_ISO_DATE = re.compile(r"\b(20\d{2})-(\d{2})-(\d{2})\b")


def test_competitor_counts_are_sourced_and_dated(readme: str) -> None:
    row = next((line for line in readme.splitlines() if "Extract + Load" in line), None)
    assert row is not None, "comparison table lost its 'Extract + Load' row"

    # Any "NNN+" in that row is a claim about someone else's catalogue.
    foreign_claims = re.findall(r"\b\d{2,4}\+", row)
    if not foreign_claims:
        return  # no claim, nothing to source

    assert "[^1]" in row, (
        f"the comparison row claims {foreign_claims} about other vendors' connector "
        "catalogues with no footnote marker. Cite the source and date it, or drop the "
        "number — we do not track their catalogues and an unsourced count rots silently."
    )
    footnote = next((ln for ln in readme.splitlines() if ln.startswith("[^1]:")), None)
    assert footnote is not None, "README references [^1] but defines no footnote"

    body = readme.split("[^1]:", 1)[1][:400]
    assert _ISO_DATE.search(body), (
        "the competitor-count footnote carries no ISO date, so a reader cannot tell "
        "how old the claim is. That date is the whole guard — this test deliberately "
        "does NOT pin the counts themselves (see the comment above)."
    )


def test_this_guard_does_not_pin_the_competitor_numbers() -> None:
    """Meta-check, and it is load-bearing rather than cute.

    If someone later "strengthens" the guard above by asserting that the row
    contains a specific count, it silently becomes the lock the landing repo
    already had — red for the *correct* change, on a fact we do not own. So this
    fails that edit, here, with the explanation attached.

    Parsed with ``ast`` rather than grepped: comments and docstrings are not in
    the tree, so the prose above (which quotes the landing repo's bad assertion
    verbatim) cannot trip it. A grep version did, on its first run.
    """
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(getattr(node, "body", None), list)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }
    pinned = sorted(
        {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
            and re.fullmatch(r"\d{3,4}\+", node.value)
        }
    )
    assert not pinned, (
        f"a competitor connector count {pinned} has been hardcoded into this file. "
        "That makes the suite fail when someone corrects the number — the exact "
        "defect this section exists to avoid. Guard the citation, never the value."
    )
