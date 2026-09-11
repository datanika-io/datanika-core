"""A blast-radius path that resolves to nothing must REFUSE, not report zero.

core#1276. ``git diff -- <path that does not exist>`` prints nothing and exits 0,
which is byte-identical to "this path did not change". The promotion pre-flight
used one prefix across a repository whose layout is mixed, so three of four
watched paths matched nothing and three assertions in two promotion bodies were
assertions over nothing.

The conclusions happened to be true for both promoted batches -- which is the
uncomfortable part, and the reason this file exists: a check that is right by
luck reports exactly like a check that is right by measurement.

Every assertion below is paired with the shape it must reject, because a guard
against silent emptiness that cannot itself fail would be the same defect.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_MODULE = Path(__file__).resolve().parents[2] / "scripts" / "blast_radius.py"
_spec = importlib.util.spec_from_file_location("blast_radius", _MODULE)
assert _spec and _spec.loader, f"cannot load {_MODULE}"
br = importlib.util.module_from_spec(_spec)
# Registered BEFORE exec_module on purpose: the module uses
# `from __future__ import annotations`, so @dataclass resolves its field types
# through `sys.modules[cls.__module__].__dict__`. Without this line that lookup
# finds None and collection dies with a bare
# "'NoneType' object has no attribute '__dict__'" that names nothing useful.
sys.modules[_spec.name] = br
_spec.loader.exec_module(br)
assert br.WATCHED, "module loaded but its watch list is empty -- this file would test nothing"


@pytest.fixture(scope="module")
def head() -> str:
    """A revision that certainly exists, resolved rather than assumed."""
    r = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, encoding="utf-8"
    )
    assert r.returncode == 0, "cannot resolve HEAD; this whole module tests nothing"
    return r.stdout.strip()


class TestTheWatchListIsReal:
    """Every shipped path must resolve. This is the regression that started it."""

    def test_every_watched_path_matches_tracked_files(self, head: str) -> None:
        unresolved = {
            label: path for label, path in br.WATCHED.items() if br.tracked_count(head, path) == 0
        }
        assert not unresolved, (
            f"watched paths matching no tracked file at HEAD: {unresolved}. "
            "A diff over these would print nothing and exit 0."
        )

    def test_the_historically_wrong_prefixes_would_be_caught(self, head: str) -> None:
        """Anti-vacuity: prove `tracked_count` can return 0, on the real mistake.

        Without this, `test_every_watched_path_matches_tracked_files` could be
        passing because `tracked_count` never returns 0 for anything.
        """
        for wrong in (
            "datanika/deploy/server",
            "datanika/docker-compose.yml",
            "datanika/Dockerfile",
        ):
            assert br.tracked_count(head, wrong) == 0, (
                f"{wrong!r} now resolves -- the layout changed and this test's "
                "negative control has stopped controlling; pick another."
            )

    def test_the_one_prefix_that_was_right_is_still_right(self, head: str) -> None:
        """`datanika/migrations/versions` IS correct -- the mistake was not uniform."""
        assert br.tracked_count(head, "datanika/migrations/versions") > 0


class TestItRefusesRatherThanReportingZero:
    def test_an_unresolvable_path_raises(self, head: str) -> None:
        with pytest.raises(br.PathNotTrackedError) as exc:
            br.collect(head, head, {"bogus": "no/such/directory/anywhere"})
        assert "no/such/directory/anywhere" in str(exc.value)
        assert "indistinguishable" in str(exc.value).lower()

    def test_the_refusal_names_which_path_failed(self, head: str) -> None:
        """A refusal that does not say WHICH path sends someone bisecting a list."""
        with pytest.raises(br.PathNotTrackedError) as exc:
            br.collect(
                head,
                head,
                {"real": "docker-compose.yml", "bogus": "datanika/Dockerfile"},
            )
        msg = str(exc.value)
        assert "datanika/Dockerfile" in msg
        assert "docker-compose.yml" not in msg, "the healthy path must not be blamed too"

    def test_a_resolvable_path_with_no_changes_reports_zero_not_a_refusal(self, head: str) -> None:
        """The distinction the whole module exists to draw, asserted directly.

        Same revision on both sides, so the diff is genuinely empty -- and that
        must read completely differently from the unresolvable case above.
        """
        entries = br.collect(head, head, {"compose": "docker-compose.yml"})
        assert len(entries) == 1
        assert entries[0].count == 0
        assert entries[0].tracked_at_head > 0, (
            "zero changes is only meaningful alongside a non-zero tracked count"
        )

    def test_main_exits_non_zero_on_an_unresolvable_path(
        self, head: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The exit code is what a promotion script reads; a raise nobody catches
        would still be a crash, but a 0 would be the original bug."""
        monkeypatch.setattr(br, "WATCHED", {"bogus": "no/such/path/here"})
        assert br.main([head, head]) == 2

    def test_main_exits_zero_when_everything_resolves(self, head: str) -> None:
        assert br.main([head, head]) == 0


class TestResolutionIsCheckedAtBothRevisions:
    """A directory added by the batch under test is absent at BASE and present at
    HEAD. Refusing that would make the guard reject legitimate additions, and
    people route around guards that cry wolf."""

    def test_present_at_only_one_side_is_accepted(self, head: str) -> None:
        real = {"compose": "docker-compose.yml"}
        entries = br.collect(head, head, real)
        assert entries and entries[0].tracked_at_base > 0

    def test_absent_at_both_sides_is_refused(self, head: str) -> None:
        with pytest.raises(br.PathNotTrackedError):
            br.collect(head, head, {"gone": "definitely/not/here"})
