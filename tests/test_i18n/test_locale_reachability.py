"""core#696 — translated copy on surfaces whose viewer cannot choose a locale.

`I18nState.locale` is a plain state var defaulting to ``"en"``. The only caller of
``set_locale`` is ``language_switcher()``, which is mounted in exactly one place —
``layout.py``'s ``sidebar_user_section()``, reached only through ``page_layout()``.
No ungated page calls ``page_layout()``. And ``grep -rin "accept.language"`` over
``datanika/`` returns **nothing**: no middleware, no route, nowhere.

So the pre-auth surface is English-only **by construction**, not by placement.
Every non-English translation of every string on it is unreachable by any
sequence of user actions a first-time visitor can perform.

Measured (``plans/qa/notes/probe-696/measure_696.py``), lower bound:

==============================  ======================================
6 registered routes             render translated copy, mount no
                                locale control
78 distinct i18n keys           on those routes
**624 translated values**       ×8 non-English locales, undisplayable
0 ``Accept-Language`` readers   anywhere under ``datanika/``
==============================  ======================================

The six are ``/login``, ``/signup``, ``/forgot-password``, ``/reset-password``,
``/auth/complete`` and ``/oauth/consent``. The last two are **not named on the
issue** and are worth stating precisely, because they are not the same case:
both require a session (``load_consent`` redirects to ``/login`` when
``access_token`` is empty), so they are not *signed-out* surfaces. But neither is
on the ``check_auth`` path that calls ``ensure_loaded``, and neither mounts the
control, so both render the default locale for a user who has never visited a
sidebar page — which is exactly the MCP-client flow: client → ``/oauth/consent``
→ ``/login`` → back to ``/oauth/consent``, never once passing the switcher.

Why the assertion is shaped this way
------------------------------------

🚨 **The first probe written for this answered the reassuring way.** It decided
"can this page select a locale?" from the page module's **import closure** —
and ``login.py`` does ``from datanika.ui.components.layout import legal_links``,
while ``layout.py`` is where ``language_switcher()`` is mounted. So ``/login`` and
``/signup`` both came back *controlled*, i.e. no defect, against a production
measurement of **zero** ``select``/``[role=combobox]`` elements on ``/signup``.

**Import reachability is not render reachability.** They import two constants and
a link row from that file; they never call ``page_layout()``. So this reads each
page module's **own** source for a call to a locale control, and nothing
transitive. The narrower rule is the correct one here, and the wider one was
wrong in the direction that hides the bug.

The ``Accept-Language`` clause is a deliberate global escape: once first render
picks the visitor's language, the strings are reachable for everyone whether or
not any particular page carries a switcher. Either half of the fix satisfies this
file, so it does not vote on which one ships — though only detection fixes *first
render*, and only a switcher lets someone override it.

``xfail(strict=True)``: green on ``dev`` today so it holds no promotion, and it
fails the moment either fix lands unless the marker goes with it.
"""

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
APP_MODULE = ROOT / "datanika" / "datanika.py"
PAGES_DIR = ROOT / "datanika" / "ui" / "pages"
PACKAGE = ROOT / "datanika"

#: Calling either of these puts the sidebar, and therefore the switcher, on screen.
LOCALE_CONTROL_CALLS = ("page_layout(", "language_switcher(")

#: `_t = I18nState.translations` then `_t["your.key"]` — the documented idiom.
TRANSLATION_LOOKUP = re.compile(r'_t\[\s*["\']([a-zA-Z0-9_.]+)["\']\s*\]')

ACCEPT_LANGUAGE = re.compile(r"accept[-_]?language", re.IGNORECASE)


def _registered_pages() -> list[tuple[str, str, bool]]:
    """``(route, page callable, is_gated)`` for every ``app.add_page``.

    Gating is read off ``on_load`` containing ``check_auth`` — the mechanism that
    actually decides it — rather than off a route prefix or a naming convention.
    """
    pages: list[tuple[str, str, bool]] = []
    for node in ast.walk(ast.parse(APP_MODULE.read_text(encoding="utf-8"))):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_page"
        ):
            continue
        route, gated = None, False
        for keyword in node.keywords:
            if keyword.arg == "route" and isinstance(keyword.value, ast.Constant):
                route = keyword.value.value
            if keyword.arg == "on_load":
                gated = "check_auth" in ast.unparse(keyword.value)
        if route and node.args:
            pages.append((route, ast.unparse(node.args[0]).split("(")[0].strip(), gated))
    return pages


def _module_defining(page_callable: str) -> Path | None:
    pattern = re.compile(rf"^def {re.escape(page_callable)}\b", re.MULTILINE)
    for path in sorted(PAGES_DIR.rglob("*.py")):
        if pattern.search(path.read_text(encoding="utf-8")):
            return path
    return None


def _non_docstring_string_literals(source: str) -> list[str]:
    """Every string constant in a module except its docstrings."""
    tree = ast.parse(source)
    docstrings = {
        ast.get_docstring(node, clean=False)
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)
    }
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value not in docstrings
    ]


def app_reads_accept_language() -> bool:
    """Does anything in the package look at the browser's language preference?

    🚨 Searched over **string literals**, not over raw text, and this is not
    fussiness. Reading that header requires its name as a string somewhere —
    ``headers.get("accept-language")``. A raw-text search would also be satisfied
    by a *comment*, including a comment saying we do **not** read it, and by this
    file's own module docstring. That would switch the guard below off silently
    and leave it green: exactly the "count the instruction, not the phrase" trap.
    Docstrings are excluded for the same reason.
    """
    for path in PACKAGE.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        try:
            literals = _non_docstring_string_literals(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - a broken module is lint's problem
            continue
        if any(ACCEPT_LANGUAGE.search(literal) for literal in literals):
            return True
    return False


def page_renders_a_locale_control(source: str) -> bool:
    return any(call in source for call in LOCALE_CONTROL_CALLS)


def page_renders_translated_copy(source: str) -> bool:
    return bool(TRANSLATION_LOOKUP.search(source))


def unreachable_translated_surfaces() -> list[tuple[str, int]]:
    """``(route, distinct keys)`` for pages with translated copy and no control."""
    if app_reads_accept_language():
        return []  # first render is already correct for everyone
    out: list[tuple[str, int]] = []
    for route, page_callable, _gated in sorted(_registered_pages()):
        module = _module_defining(page_callable)
        if module is None:
            continue
        source = module.read_text(encoding="utf-8")
        if page_renders_translated_copy(source) and not page_renders_a_locale_control(source):
            out.append((route, len(set(TRANSLATION_LOOKUP.findall(source)))))
    return out


class TestTheReachabilityGuardCanActuallyFail:
    """Without these, a detector that matched nothing would flip the strict xfail
    to XPASS and read as *"the fix landed"*."""

    def test_the_page_registry_parses(self):
        pages = _registered_pages()
        assert len(pages) >= 15, (
            f"only found {len(pages)} app.add_page calls; every assertion below is "
            "a filter over this list and would be vacuous"
        )
        assert any(gated for _, _, gated in pages), "no page read as auth-gated"
        assert any(not gated for _, _, gated in pages), "no page read as ungated"

    def test_the_detector_discriminates_on_the_real_tree(self):
        """It must find pages that DO carry the control, or it is matching nothing
        and the finding below is an artifact of a broken search."""
        controlled = [
            route
            for route, page_callable, _ in _registered_pages()
            if (module := _module_defining(page_callable))
            and page_renders_a_locale_control(module.read_text(encoding="utf-8"))
        ]
        assert len(controlled) >= 10, (
            f"only {len(controlled)} pages read as carrying a locale control "
            f"({controlled}); the sidebar is on every authenticated page, so a "
            "number this low means the detector, not the app, is broken"
        )

    def test_a_page_with_translated_copy_and_no_control_is_flagged(self):
        source = 'def p():\n    return rx.center(rx.text(_t["auth.email"]))\n'
        assert page_renders_translated_copy(source)
        assert not page_renders_a_locale_control(source)

    def test_a_page_that_renders_the_layout_is_not_flagged(self):
        source = 'def p():\n    return page_layout(rx.text(_t["auth.email"]))\n'
        assert page_renders_a_locale_control(source)

    def test_a_page_with_no_translated_copy_is_not_flagged(self):
        source = 'def p():\n    return rx.center(rx.text("Loading"))\n'
        assert not page_renders_translated_copy(source)

    def test_importing_the_layout_is_not_rendering_it(self):
        """🚨 The case that made the first probe exonerate the bug. ``login.py``
        imports from ``layout.py`` and does not call ``page_layout()``."""
        source = (
            "from datanika.ui.components.layout import legal_links\n"
            'def p():\n    return rx.center(legal_links(), rx.text(_t["auth.email"]))\n'
        )
        assert "layout" in source, "the fixture must actually import the layout module"
        assert not page_renders_a_locale_control(source), (
            "an import of the module that mounts the switcher is not a render of "
            "the switcher — this is exactly what made an import-closure probe "
            "report /login and /signup as controlled"
        )

    def test_the_accept_language_pattern_is_wired(self):
        """The clause must be readable, or the whole check silently depends on
        one grep that could be matching nothing."""
        assert ACCEPT_LANGUAGE.search("Accept-Language")
        assert ACCEPT_LANGUAGE.search("accept_language")
        assert not ACCEPT_LANGUAGE.search("acceptable languages")

    def test_a_real_header_read_satisfies_the_escape(self):
        source = 'def pick(request):\n    return request.headers.get("accept-language", "en")\n'
        assert any(
            ACCEPT_LANGUAGE.search(literal) for literal in _non_docstring_string_literals(source)
        )

    def test_a_comment_mentioning_the_header_does_not_satisfy_the_escape(self):
        """🚨 The hole a raw-text search would have left. A comment saying we do
        not read the header would have switched this whole file off, green."""
        source = "# TODO: we do not read Accept-Language anywhere yet\nX = 1\n"
        assert "Accept-Language" in source
        assert not any(
            ACCEPT_LANGUAGE.search(literal) for literal in _non_docstring_string_literals(source)
        )

    def test_a_docstring_mentioning_the_header_does_not_satisfy_the_escape(self):
        """Including this module's own — it names the header six times."""
        source = (
            '"""Nothing here reads Accept-Language."""\n\n\ndef f():\n    """accept-language"""\n'
        )
        assert not any(
            ACCEPT_LANGUAGE.search(literal) for literal in _non_docstring_string_literals(source)
        )


class TestTranslatedCopyIsReachableInEveryLocale:
    @pytest.mark.xfail(
        strict=True,
        reason=(
            "core#696: no Accept-Language reader exists and no ungated page mounts "
            "the locale switcher, so 8 of 9 locales are undisplayable on the "
            "pre-auth surface. Remove this marker with the fix."
        ),
    )
    def test_no_page_renders_copy_its_viewer_cannot_choose_the_language_of(self):
        offenders = unreachable_translated_surfaces()
        total = sum(count for _, count in offenders)
        assert not offenders, (
            "these routes render translated copy and give their viewer no way to "
            "select a locale, and nothing reads Accept-Language:\n  "
            + "\n  ".join(f"{route} — {count} keys" for route, count in offenders)
            + f"\n{total} distinct keys × 8 non-English locales are undisplayable. "
            "Fix either half: read Accept-Language on first load (fixes first "
            "render for everyone), or mount the switcher on these pages (lets "
            "someone override it). See core#696."
        )
