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

    def test_signup_does_not_import_analytics_from_core(self):
        """Regression for #99 (open-core refactor): auth_state must NOT
        import anything from ``datanika.ui.analytics`` — that module has
        been deleted and analytics instrumentation lives in the cloud
        plugin. Any import of it from core is the exact regression this
        test catches.
        """
        import inspect

        import datanika.ui.state.auth_state as auth_state_module

        source = inspect.getsource(auth_state_module)
        assert "datanika.ui.analytics" not in source, (
            "auth_state.py imports from datanika.ui.analytics — that "
            "module was deleted in issue #99. Analytics lives in the "
            "cloud plugin now and reaches signup() via the "
            "user.signup_completed hook."
        )
        assert "google_ads_conversion_event_js" not in source, (
            "auth_state.py references google_ads_conversion_event_js "
            "directly — that helper moved to datanika_cloud in #99. Use "
            "hooks.collect_events('user.signup_completed', ...) instead."
        )

    def test_signup_uses_collect_events_hook_for_plugin_contributions(self):
        """Source scan: signup() must call ``collect_events`` on the
        ``user.signup_completed`` event and splice the returned list into
        its Reflex event return value. That's how the cloud plugin's
        Google Ads conversion tracking fires post-refactor.
        """
        import inspect

        import datanika.ui.state.auth_state as auth_state_module

        source = inspect.getsource(auth_state_module.AuthState.signup.fn)
        assert "collect_events" in source, (
            "signup() must use hooks.collect_events to gather plugin-"
            "contributed Reflex events (e.g. Google Ads conversion). "
            "See issue #99."
        )
        assert "user.signup_completed" in source, (
            "signup() must emit the 'user.signup_completed' event so "
            "cloud plugin handlers can fire on successful signup"
        )

    def test_signup_org_slug_includes_user_id_suffix(self):
        """Regression for #127: two users with the same full_name must
        produce distinct org slugs. The password-signup path used to
        call ``_slugify(full_name)`` with no suffix, so a second user
        named ``John Smith`` hit ``Slug already exists`` on create_org
        (then swallowed into the generic toast per #128). The fix
        mirrors ``user_service.find_or_create_oauth_user:308`` which
        suffixes with ``{user.id}`` — making the slug globally unique
        by construction (user ids are autoincrement).
        """
        import inspect
        import re

        import datanika.ui.state.auth_state as auth_state_module

        source = inspect.getsource(auth_state_module.AuthState.signup.fn)

        # The slug construction must reference both _slugify(full_name)
        # and user.id on the same line. A loose substring check would
        # pass on any future refactor that moves them apart.
        pattern = re.compile(r"org_slug\s*=\s*f[\"'][^\"']*\{_slugify\(full_name\)\}-\{user\.id\}")
        assert pattern.search(source), (
            "Expected signup() to build org_slug as "
            '`f"{_slugify(full_name)}-{user.id}"` — #127 regression. '
            "Without the user.id suffix two users with the same full_name "
            "collide on the organizations.slug unique constraint and the "
            "second signup fails."
        )

    def test_signup_slug_pattern_matches_oauth_path(self):
        """Both signup paths must use the same uniqueness strategy —
        otherwise, password-signup and OAuth-signup diverge and the next
        refactor picks whichever is in sight. #127.
        """
        import inspect

        from datanika.services import user_service

        oauth_source = inspect.getsource(user_service.UserService.find_or_create_oauth_user)
        # The OAuth path builds: slug=f"{slug}-{user.id}"
        assert 'f"{slug}-{user.id}"' in oauth_source, (
            "find_or_create_oauth_user no longer uses the "
            'f"{slug}-{user.id}" pattern — if you changed the OAuth '
            "uniqueness strategy, update password-signup in auth_state.py "
            "to match, and update #127."
        )
