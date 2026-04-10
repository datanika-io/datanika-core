"""Tests for Getting Started onboarding checklist logic.

Pure-function tests for `compute_checklist` — the helper that converts
per-org entity counts into the 5 booleans the dashboard widget renders.
"""

from datanika.services.onboarding import ChecklistProgress, compute_checklist


class TestComputeChecklistFresh:
    def test_fresh_org_has_zero_done(self):
        progress = compute_checklist(
            connection_count=0,
            pipeline_count=0,
            run_count=0,
            transformation_count=0,
            schedule_count=0,
        )
        assert progress.done_count == 0
        assert progress.total == 5
        assert progress.all_done is False
        assert progress.has_connection is False
        assert progress.has_pipeline is False
        assert progress.has_run is False
        assert progress.has_transformation is False
        assert progress.has_schedule is False


class TestComputeChecklistPartial:
    def test_single_connection_marks_first_step(self):
        progress = compute_checklist(
            connection_count=1,
            pipeline_count=0,
            run_count=0,
            transformation_count=0,
            schedule_count=0,
        )
        assert progress.has_connection is True
        assert progress.done_count == 1
        assert progress.all_done is False

    def test_many_connections_still_counts_as_one_step(self):
        # Existence, not cardinality — 42 connections is still "1 out of 5"
        progress = compute_checklist(
            connection_count=42,
            pipeline_count=0,
            run_count=0,
            transformation_count=0,
            schedule_count=0,
        )
        assert progress.has_connection is True
        assert progress.done_count == 1

    def test_three_of_five_done(self):
        progress = compute_checklist(
            connection_count=2,
            pipeline_count=1,
            run_count=5,
            transformation_count=0,
            schedule_count=0,
        )
        assert progress.done_count == 3
        assert progress.all_done is False
        assert progress.has_connection is True
        assert progress.has_pipeline is True
        assert progress.has_run is True
        assert progress.has_transformation is False
        assert progress.has_schedule is False

    def test_four_of_five_done_not_all_done(self):
        progress = compute_checklist(
            connection_count=1,
            pipeline_count=1,
            run_count=1,
            transformation_count=1,
            schedule_count=0,
        )
        assert progress.done_count == 4
        assert progress.all_done is False


class TestComputeChecklistComplete:
    def test_all_five_done(self):
        progress = compute_checklist(
            connection_count=1,
            pipeline_count=1,
            run_count=1,
            transformation_count=1,
            schedule_count=1,
        )
        assert progress.done_count == 5
        assert progress.total == 5
        assert progress.all_done is True
        assert progress.has_connection is True
        assert progress.has_pipeline is True
        assert progress.has_run is True
        assert progress.has_transformation is True
        assert progress.has_schedule is True


class TestChecklistProgressModel:
    def test_defaults(self):
        progress = ChecklistProgress()
        assert progress.has_connection is False
        assert progress.has_pipeline is False
        assert progress.has_run is False
        assert progress.has_transformation is False
        assert progress.has_schedule is False
        assert progress.done_count == 0
        assert progress.total == 5
        assert progress.all_done is False

    def test_total_is_always_five(self):
        # The 5 onboarding steps are a product decision, not a count of
        # whatever tables exist. Keep the denominator stable.
        progress = compute_checklist(
            connection_count=0,
            pipeline_count=0,
            run_count=0,
            transformation_count=0,
            schedule_count=0,
        )
        assert progress.total == 5
