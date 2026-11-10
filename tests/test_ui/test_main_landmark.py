"""core#1587 — the six app surfaces need one main landmark.

The sweep fails only on critical and serious. ``landmark-one-main`` is moderate, so a
green sweep stays green while every app page has no ``<main>``. This walks the
component tree the pages actually return. A walk that does not descend fails these
assertions, because the unfixed tree contains zero ``main`` tags.
"""

import reflex as rx

from datanika.ui.components.layout import page_layout, sidebar
from datanika.ui.pages.login import login_page
from datanika.ui.pages.signup import signup_page


def _walk(component):
    yield component
    for child in getattr(component, "children", []) or []:
        yield from _walk(child)


def _tagged(component, tag: str) -> list:
    return [node for node in _walk(component) if getattr(node, "tag", None) == tag]


def _style(component) -> dict:
    return {
        key: str(value).strip('"')
        for key, value in dict(getattr(component, "style", {}) or {}).items()
    }


class TestAppSurfacesHaveOneMain:
    def test_login_wraps_its_form_in_one_main(self):
        mains = _tagged(login_page(), "main")
        assert len(mains) == 1
        form = list(_walk(mains[0]))
        named = any(
            getattr(node, "tag", None) == "input" or "login-email" in str(node) for node in form
        )
        assert named

    def test_signup_wraps_its_form_in_one_main(self):
        mains = _tagged(signup_page(), "main")
        assert len(mains) == 1
        assert any("signup-full-name" in str(node) for node in _walk(mains[0]))

    def test_authenticated_shell_has_one_main_and_the_sidebar_is_a_nav(self):
        layout = page_layout(rx.text("landmark-marker"), title="T")
        mains = _tagged(layout, "main")
        navs = _tagged(layout, "nav")
        assert len(mains) == 1
        assert len(navs) == 1
        assert any("landmark-marker" in str(node) for node in _walk(mains[0]))
        assert _tagged(navs[0], "main") == []
        assert _style(mains[0]).get("marginLeft") == "240px"
        assert _style(navs[0]).get("position") == "fixed"
        assert _style(navs[0]).get("width") == "240px"

    def test_the_sidebar_component_itself_is_the_nav(self):
        roots = _tagged(sidebar(), "nav")
        assert len(roots) == 1
        assert _tagged(sidebar(), "main") == []
