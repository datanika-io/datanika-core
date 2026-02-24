"""Tests for model catalog state — last_run resolution including pipeline runs."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

from datanika.ui.state.model_state import _pick_latest_run


class TestPickLatestRun:
    def test_none_when_all_none(self):
        assert _pick_latest_run(None, None) is None

    def test_returns_only_candidate(self):
        run = MagicMock(finished_at=datetime(2024, 1, 1, tzinfo=UTC))
        assert _pick_latest_run(None, run) is run
        assert _pick_latest_run(run, None) is run

    def test_picks_more_recent(self):
        older = MagicMock(finished_at=datetime(2024, 1, 1, tzinfo=UTC))
        newer = MagicMock(finished_at=datetime(2024, 6, 1, tzinfo=UTC))
        assert _pick_latest_run(older, newer) is newer
        assert _pick_latest_run(newer, older) is newer

    def test_run_without_finished_at_loses_to_finished(self):
        finished = MagicMock(finished_at=datetime(2024, 1, 1, tzinfo=UTC))
        pending = MagicMock(finished_at=None)
        assert _pick_latest_run(finished, pending) is finished

    def test_both_without_finished_at_returns_first(self):
        a = MagicMock(finished_at=None)
        b = MagicMock(finished_at=None)
        # First non-None wins when neither has finished_at
        assert _pick_latest_run(a, b) is a
