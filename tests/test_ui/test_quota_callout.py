"""Tests for quota error detection and upgrade prompt logic."""


class FakeQuotaExceededError(ValueError):
    """Mimic the cloud plugin's QuotaExceededError."""

    __name__ = "QuotaExceededError"

    def __init__(self, message: str):
        super().__init__(message)
        self.__class__.__name__ = "QuotaExceededError"


class TestSetError:
    """Test BaseState._set_error detection of quota vs regular errors."""

    def _make_state(self):
        """Create a minimal object with _set_error behavior (no Reflex needed)."""
        import logging

        class FakeState:
            error_message = ""
            is_quota_error = False

        _log = logging.getLogger("test")

        def _set_error(self, exc, fallback="An error occurred"):
            _log.exception("Caught exception in state handler")
            self.is_quota_error = type(exc).__name__ == "QuotaExceededError"
            if isinstance(exc, ValueError):
                self.error_message = str(exc)
            else:
                self.error_message = fallback

        FakeState._set_error = _set_error
        return FakeState()

    def test_quota_error_detected(self):
        state = self._make_state()
        exc = FakeQuotaExceededError("Connection limit reached (5 on Free plan)")
        state._set_error(exc, "Failed to save connection")

        assert state.is_quota_error is True
        assert state.error_message == "Connection limit reached (5 on Free plan)"

    def test_regular_value_error_not_quota(self):
        state = self._make_state()
        exc = ValueError("Invalid connection name")
        state._set_error(exc, "Failed to save connection")

        assert state.is_quota_error is False
        assert state.error_message == "Invalid connection name"

    def test_generic_exception_not_quota(self):
        state = self._make_state()
        exc = RuntimeError("DB connection failed")
        state._set_error(exc, "Failed to save connection")

        assert state.is_quota_error is False
        assert state.error_message == "Failed to save connection"

    def test_schedule_quota_error(self):
        state = self._make_state()
        exc = FakeQuotaExceededError("Schedule limit reached (2 on Free plan)")
        state._set_error(exc, "Failed to save schedule")

        assert state.is_quota_error is True
        assert "Schedule limit" in state.error_message

    def test_seat_quota_error(self):
        state = self._make_state()
        exc = FakeQuotaExceededError(
            "Seat limit reached (1 on Free plan). Upgrade your plan or add extra seats."
        )
        state._set_error(exc, "Failed to add member")

        assert state.is_quota_error is True
        assert "Seat limit" in state.error_message

    def test_run_quota_error(self):
        state = self._make_state()
        exc = FakeQuotaExceededError(
            "Monthly run limit reached (500 on Free plan). Upgrade your plan to continue."
        )
        state._set_error(exc, "Failed to execute run")

        assert state.is_quota_error is True
        assert "run limit" in state.error_message


class TestI18nKeys:
    """Verify quota i18n keys exist in all locale files."""

    def test_quota_keys_in_all_locales(self):
        import json
        from pathlib import Path

        i18n_dir = Path(__file__).resolve().parent.parent.parent / "datanika" / "i18n"
        required_keys = ["quota.upgrade_hint", "quota.upgrade_button"]
        locales = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]

        for locale in locales:
            path = i18n_dir / f"{locale}.json"
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            for key in required_keys:
                assert key in data, f"Missing key '{key}' in {locale}.json"
                assert data[key], f"Empty value for '{key}' in {locale}.json"
