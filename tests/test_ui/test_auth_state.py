"""Tests for auth-related rx.Base data model classes and AuthState fields."""

from datanika.ui.state.auth_state import AuthState, OrgInfo, UserInfo


class TestUserInfo:
    def test_create_with_fields(self):
        info = UserInfo(id=1, email="alice@example.com", full_name="Alice")
        assert info.id == 1
        assert info.email == "alice@example.com"
        assert info.full_name == "Alice"

    def test_defaults(self):
        info = UserInfo()
        assert info.id == 0
        assert info.email == ""
        assert info.full_name == ""


class TestOrgInfo:
    def test_create_with_fields(self):
        info = OrgInfo(id=5, name="Acme Corp", slug="acme")
        assert info.id == 5
        assert info.name == "Acme Corp"
        assert info.slug == "acme"

    def test_defaults(self):
        info = OrgInfo()
        assert info.id == 0
        assert info.name == ""
        assert info.slug == ""


class TestAuthStateFields:
    def test_access_token_default(self):
        assert AuthState.__fields__["access_token"].default == ""

    def test_refresh_token_default(self):
        assert AuthState.__fields__["refresh_token"].default == ""

    def test_current_user_default(self):
        field = AuthState.__fields__["current_user"]
        default = field.default_factory() if field.default_factory else field.default
        assert isinstance(default, UserInfo)
        assert default.id == 0

    def test_current_org_default(self):
        field = AuthState.__fields__["current_org"]
        default = field.default_factory() if field.default_factory else field.default
        assert isinstance(default, OrgInfo)
        assert default.id == 0


class TestAuthStateFormFields:
    def test_auth_error_default(self):
        assert AuthState.__fields__["auth_error"].default == ""

    def test_login_accepts_form_data(self):
        """login() accepts a form_data dict (from rx.form on_submit)."""
        import inspect

        fn = AuthState.login.fn if hasattr(AuthState.login, "fn") else AuthState.login
        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())
        assert "form_data" in params

    def test_signup_accepts_form_data(self):
        """signup() accepts a form_data dict (from rx.form on_submit)."""
        import inspect

        fn = AuthState.signup.fn if hasattr(AuthState.signup, "fn") else AuthState.signup
        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())
        assert "form_data" in params

    def test_signup_imports_google_ads_conversion_helper(self):
        """Regression for #96: auth_state must import the conversion-event
        helper so the signup success path can fire it. A future refactor
        that drops this import silently breaks Google Ads attribution.
        """
        import datanika.ui.state.auth_state as auth_state_module

        assert hasattr(auth_state_module, "google_ads_conversion_event_js"), (
            "auth_state.py must import google_ads_conversion_event_js from "
            "datanika.ui.analytics so signup() can fire the Phase 2 "
            "conversion event. See issue #96."
        )

    def test_signup_source_references_call_script_and_conversion_helper(self):
        """Source scan: signup() must reference both rx.call_script and the
        conversion helper. Catches regressions that drop the event fire
        while keeping the import intact (e.g. "oops, removed one call").
        """
        import inspect

        import datanika.ui.state.auth_state as auth_state_module

        source = inspect.getsource(auth_state_module.AuthState.signup.fn)
        assert "google_ads_conversion_event_js" in source, (
            "signup() no longer calls google_ads_conversion_event_js — "
            "the Phase 2 conversion event will not fire"
        )
        assert "call_script" in source, (
            "signup() no longer returns rx.call_script — the conversion "
            "event JS will not be sent to the client"
        )


class TestConfigHasGoogleAdsSettings:
    """Regression for #96: settings fields must exist so the dormant
    check in google_ads_conversion_event_js() doesn't AttributeError.
    """

    def test_google_ads_tag_id_exists(self):
        from datanika.config import settings

        assert hasattr(settings, "google_ads_tag_id")
        assert isinstance(settings.google_ads_tag_id, str)

    def test_google_ads_conversion_label_signup_exists(self):
        from datanika.config import settings

        assert hasattr(settings, "google_ads_conversion_label_signup")
        assert isinstance(settings.google_ads_conversion_label_signup, str)

    def test_defaults_are_empty_strings(self):
        # Dormant-by-default contract: the test environment must not
        # accidentally emit live gtag scripts. Empty string is the
        # dormant signal in both analytics.py helpers.
        from datanika.config import Settings

        fresh = Settings()
        assert fresh.google_ads_tag_id == ""
        assert fresh.google_ads_conversion_label_signup == ""
