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
    def test_login_email_default(self):
        assert AuthState.__fields__["login_email"].default == ""

    def test_login_password_default(self):
        assert AuthState.__fields__["login_password"].default == ""

    def test_signup_fields_default(self):
        assert AuthState.__fields__["signup_email"].default == ""
        assert AuthState.__fields__["signup_password"].default == ""
        assert AuthState.__fields__["signup_full_name"].default == ""

    def test_auth_error_default(self):
        assert AuthState.__fields__["auth_error"].default == ""
