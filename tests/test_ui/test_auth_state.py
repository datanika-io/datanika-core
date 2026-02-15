"""Tests for auth-related rx.Base data model classes and AuthState fields."""

from etlfabric.ui.state.auth_state import AuthState, OrgInfo, UserInfo


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
