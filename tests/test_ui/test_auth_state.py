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
        """Regression for #127: two users with the same full_name must produce
        distinct org slugs.

        ⚠️ **Rewritten for core#655 D4.** This used to pin the literal source
        ``f"{_slugify(full_name)}-{user.id}"``. That construction is now forbidden —
        a slug is an *identifier* (unique-constrained, in URLs, matched by the SSO
        callback), so deriving it from a person's name publishes that name in a
        durable key; §2c measured `organizations.slug` carrying a live
        `users.full_name` in **5 of 5** production rows. The slug is now
        ``org-{user.id}``.

        #127's invariant is unchanged and if anything stronger — the id *is* the
        slug now, rather than a suffix on a name-derived stem. So the test asserts
        the invariant (distinct per user, no part of the name) instead of the
        spelling, which is what it should have done the first time: a source-regex
        assertion cannot distinguish "the strategy changed" from "the strategy
        moved".
        """
        import inspect

        import datanika.ui.state.auth_state as auth_state_module

        source = inspect.getsource(auth_state_module.AuthState.signup.fn)

        assert 'f"org-{user.id}"' in source, (
            'Expected signup() to build org_slug as `f"org-{user.id}"` (core#655 D4). '
            "The user.id keeps #127's uniqueness guarantee; dropping it lets two users "
            "with the same name collide on the organizations.slug unique constraint."
        )
        assert "_slugify(full_name)" not in source, (
            "signup() is deriving the org slug from the user's full name again. That "
            "publishes a person's name in a unique, URL-bearing key that the SSO "
            "callback matches on — core#655 D4, and it is not undone by erasing "
            "`users.full_name`."
        )

    def test_signup_slug_pattern_matches_oauth_path(self):
        """Both signup paths must use the same uniqueness strategy — otherwise
        password-signup and OAuth-signup diverge and the next refactor picks
        whichever is in sight. #127, and now core#655 D4 as well: a name-derived
        slug on *either* path is enough to leak the name.
        """
        import inspect

        from datanika.services import user_service

        oauth_source = inspect.getsource(user_service.UserService.find_or_create_oauth_user)
        assert 'f"org-{user.id}"' in oauth_source, (
            'find_or_create_oauth_user no longer uses the `f"org-{user.id}"` pattern — '
            "if you changed the OAuth uniqueness strategy, update password-signup in "
            "auth_state.py to match, and update #127 / core#655 D4."
        )
        assert "slugify" not in oauth_source.lower(), (
            "the OAuth signup path is deriving the org slug from the user's name again "
            "(core#655 D4)"
        )


class TestSignupExceptionHandling:
    """#128 — ``signup()`` must surface typed ``UserServiceError`` messages
    verbatim and log the catch-all path for observability.

    Before this fix every signup failure (duplicate email, slug exists,
    validation, DB drop, OAuth timeout) was replaced with the generic
    string "Signup failed. Please try again." — users had no recovery
    path and ops had no signal.
    """

    def test_user_service_error_imported(self):
        import datanika.ui.state.auth_state as auth_state_module

        assert hasattr(auth_state_module, "UserServiceError"), (
            "auth_state.py must import UserServiceError so the typed "
            "except branch can distinguish curated user-facing errors "
            "from unexpected failures. See #128."
        )

    def test_module_logger_defined(self):
        import datanika.ui.state.auth_state as auth_state_module

        assert hasattr(auth_state_module, "logger"), (
            "auth_state.py must define a module-level `logger` so the "
            "catch-all except branch can emit traceable root causes. #128."
        )

    def test_signup_has_typed_user_service_error_branch(self):
        import inspect

        import datanika.ui.state.auth_state as auth_state_module

        source = inspect.getsource(auth_state_module.AuthState.signup.fn)
        assert "except UserServiceError" in source, (
            "signup() must have a typed `except UserServiceError` branch "
            "that surfaces the exception message verbatim. See #128."
        )
        assert "self.auth_error = str(exc)" in source, (
            "The UserServiceError branch must set auth_error = str(exc), "
            "not a hardcoded string. #128."
        )

    def test_signup_catch_all_logs_exception(self):
        import inspect

        import datanika.ui.state.auth_state as auth_state_module

        source = inspect.getsource(auth_state_module.AuthState.signup.fn)
        assert "logger.exception" in source, (
            "signup()'s catch-all `except Exception` branch must call "
            "logger.exception(...) so root causes land in container logs. "
            "See #128."
        )

    def test_runtime_user_service_error_surfaces_message(self):
        """End-to-end: a UserServiceError raised by the service layer
        reaches the user as its real message, not the generic toast.

        Uses ``SimpleNamespace`` as the state duck-type because Reflex's
        ``rx.State.__setattr__`` requires full state init (dirty_vars,
        parent state) that's expensive to stand up in a unit test — and
        irrelevant here since the error path only touches ``self.auth_error``.
        """
        from types import SimpleNamespace
        from unittest.mock import MagicMock, patch

        from datanika.services.user_service import UserServiceError
        from datanika.ui.state.auth_state import AuthState

        fake_svc = MagicMock()
        fake_svc.register_user.side_effect = UserServiceError("Email already exists")

        state = SimpleNamespace(
            auth_error="",
            # core#639 — signup consults a rate limiter before the lookup.
            signup_blocked="",
            # core#981 — signup now reads the invite token before it opens a
            # session, so the stand-in needs a router. Without it the read
            # raises AttributeError inside the try and this test's own error
            # path is masked by the generic catch-all.
            invite_notice="",
            router=SimpleNamespace(page=SimpleNamespace(params={})),
            # core#1081 — signup refuses a live session before anything else,
            # so the stand-in has to answer _revalidate_session. These tests are
            # all about a signed-OUT visitor, hence False.
            _revalidate_session=lambda: False,
            _client_ip=lambda: "",
            _get_user_service=lambda: fake_svc,
        )

        with (
            patch("datanika.ui.state.auth_state.CaptchaService") as mock_captcha_cls,
            patch("datanika.ui.state.auth_state.get_sync_session") as mock_session,
            patch("datanika.ui.state.auth_state._allow", return_value=True),
        ):
            mock_captcha_cls.return_value.verify.return_value = True
            mock_session.return_value.__enter__.return_value = MagicMock()
            mock_session.return_value.__exit__.return_value = False

            result = AuthState.signup.fn(
                state,
                {
                    "email": "alice@example.com",
                    "password": "pw12345678",
                    "full_name": "Alice",
                    "captcha_token": "tok",
                },
            )

        assert state.auth_error == "Email already exists"
        assert result is None  # early return on error path

    def test_runtime_unexpected_exception_logs_and_shows_generic_toast(self, caplog):
        """End-to-end: a non-UserServiceError exception keeps the generic
        user-facing message AND writes a log entry with the real cause."""
        import logging
        from types import SimpleNamespace
        from unittest.mock import MagicMock, patch

        from datanika.ui.state.auth_state import AuthState

        fake_svc = MagicMock()
        fake_svc.register_user.side_effect = RuntimeError("DB connection lost")

        state = SimpleNamespace(
            auth_error="",
            # core#639 — signup consults a rate limiter before the lookup.
            signup_blocked="",
            # core#981 — signup now reads the invite token before it opens a
            # session, so the stand-in needs a router. Without it the read
            # raises AttributeError inside the try and this test's own error
            # path is masked by the generic catch-all.
            invite_notice="",
            router=SimpleNamespace(page=SimpleNamespace(params={})),
            # core#1081 — signup refuses a live session before anything else,
            # so the stand-in has to answer _revalidate_session. These tests are
            # all about a signed-OUT visitor, hence False.
            _revalidate_session=lambda: False,
            _client_ip=lambda: "",
            _get_user_service=lambda: fake_svc,
        )

        with (
            patch("datanika.ui.state.auth_state.CaptchaService") as mock_captcha_cls,
            patch("datanika.ui.state.auth_state.get_sync_session") as mock_session,
            patch("datanika.ui.state.auth_state._allow", return_value=True),
            caplog.at_level(logging.ERROR, logger="datanika.ui.state.auth_state"),
        ):
            mock_captcha_cls.return_value.verify.return_value = True
            mock_session.return_value.__enter__.return_value = MagicMock()
            mock_session.return_value.__exit__.return_value = False

            AuthState.signup.fn(
                state,
                {
                    "email": "alice@example.com",
                    "password": "pw12345678",
                    "full_name": "Alice",
                    "captcha_token": "tok",
                },
            )

        assert state.auth_error == "Signup failed. Please try again."
        assert any(
            "DB connection lost" in rec.message
            or (rec.exc_info and "DB connection lost" in str(rec.exc_info[1]))
            for rec in caplog.records
        ), (
            "The catch-all branch must log the exception so ops can see "
            "root causes in container logs. #128."
        )
