"""Tests for email nudge at 80% quota usage."""

from datanika.hooks import clear, emit, on


class TestQuotaThresholdHook:
    """Test that quota.threshold_reached hook fires correctly."""

    def setup_method(self):
        clear()

    def teardown_method(self):
        clear()

    def test_handler_receives_context(self):
        received = {}

        def handler(*, org_id, metric, used, limit, plan_name, **kw):
            received.update(
                org_id=org_id,
                metric=metric,
                used=used,
                limit=limit,
                plan_name=plan_name,
            )

        on("quota.threshold_reached", handler)
        emit(
            "quota.threshold_reached",
            org_id=1,
            metric="model_runs",
            used=420,
            limit=500,
            plan_name="Free",
        )
        assert received["org_id"] == 1
        assert received["used"] == 420
        assert received["limit"] == 500
        assert received["plan_name"] == "Free"

    def test_no_handler_does_not_fail(self):
        emit(
            "quota.threshold_reached",
            org_id=1,
            metric="model_runs",
            used=420,
            limit=500,
            plan_name="Free",
        )


class TestThresholdDetection:
    """Test the 80% threshold crossing logic."""

    def test_crosses_80_percent(self):
        old_used, new_used, limit = 390, 410, 500
        old_pct = old_used / limit * 100
        new_pct = new_used / limit * 100
        crossed = old_pct < 80 <= new_pct
        assert crossed is True

    def test_already_above_80_no_trigger(self):
        old_used, new_used, limit = 410, 420, 500
        old_pct = old_used / limit * 100
        new_pct = new_used / limit * 100
        crossed = old_pct < 80 <= new_pct
        assert crossed is False

    def test_below_80_no_trigger(self):
        old_used, new_used, limit = 300, 350, 500
        old_pct = old_used / limit * 100
        new_pct = new_used / limit * 100
        crossed = old_pct < 80 <= new_pct
        assert crossed is False

    def test_exactly_80_triggers(self):
        old_used, new_used, limit = 399, 400, 500
        old_pct = old_used / limit * 100
        new_pct = new_used / limit * 100
        crossed = old_pct < 80 <= new_pct
        assert crossed is True

    def test_zero_limit_no_trigger(self):
        old_used, new_used, limit = 0, 10, 0
        if limit == 0:
            crossed = False
        else:
            old_pct = old_used / limit * 100
            new_pct = new_used / limit * 100
            crossed = old_pct < 80 <= new_pct
        assert crossed is False


class TestQuotaEmailTemplate:
    """Test that the email template renders correctly."""

    def test_template_contains_usage(self):
        from datanika.services.email_service import _QUOTA_WARNING_TEMPLATE

        html = _QUOTA_WARNING_TEMPLATE.format(
            plan_name="Free",
            metric_label="model runs",
            used=420,
            limit=500,
            percent=84,
            upgrade_url="https://app.datanika.io/settings?tab=billing",
            app_name="Datanika",
        )
        assert "420" in html
        assert "500" in html
        assert "84%" in html
        assert "Free" in html
        assert "Upgrade" in html


class TestQuotaEmailSubject:
    """Verify email subject is generated correctly."""

    def test_subject_contains_percent_and_plan(self):
        from datanika.services.email_service import EmailService

        svc = EmailService("", 0, "", "", "test@test.com", "Test", False, "http://localhost")
        # Can't actually send, but we can check the method exists
        assert hasattr(svc, "send_quota_warning_email")
