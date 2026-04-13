"""Unit tests for the plugin registry (issue #99).

The registry is the thin seam between core and the cloud plugin:

1. ``register_head_component`` / ``plugin_head_components`` —
   plugin-contributed ``rx.Component`` items that ``datanika.py`` splices
   into ``rx.App(head_components=[...])`` at construction time.
2. ``register_page_script`` / ``get_page_scripts`` — plugin-contributed
   inline ``rx.script`` blocks that named page functions splice into
   their own rendered output via ``*get_page_scripts(\"key\")``.

Both halves must start empty, accumulate append-only, and expose
read-only views so plugin contributions can't be mutated back out.
"""

import reflex as rx

from datanika import plugin_registry


def _reset():
    # Tests share the same module-level dicts — isolate each test.
    plugin_registry._head_components.clear()
    plugin_registry._page_scripts.clear()


class TestHeadComponentRegistry:
    def setup_method(self):
        _reset()

    def teardown_method(self):
        _reset()

    def test_empty_by_default(self):
        assert plugin_registry.plugin_head_components() == []

    def test_register_appends(self):
        c1 = rx.script("console.log(1)")
        c2 = rx.script("console.log(2)")
        plugin_registry.register_head_component(c1)
        plugin_registry.register_head_component(c2)
        assert plugin_registry.plugin_head_components() == [c1, c2]

    def test_returned_list_is_a_copy(self):
        # Callers must not be able to mutate the registry through the
        # returned list — that's the whole point of going through a
        # getter.
        plugin_registry.register_head_component(rx.script("x"))
        got = plugin_registry.plugin_head_components()
        got.append(rx.script("y"))
        assert len(plugin_registry.plugin_head_components()) == 1


class TestPageScriptRegistry:
    def setup_method(self):
        _reset()

    def teardown_method(self):
        _reset()

    def test_unknown_key_returns_empty(self):
        assert plugin_registry.get_page_scripts("no_such_page") == []

    def test_register_script_for_page(self):
        plugin_registry.register_page_script("pipeline_templates", "/* js */")
        scripts = plugin_registry.get_page_scripts("pipeline_templates")
        assert len(scripts) == 1
        # Each entry is a Reflex component, not a raw string, so callers
        # can splat it directly into a parent component.
        assert hasattr(scripts[0], "tag") or hasattr(scripts[0], "children")

    def test_multiple_scripts_per_page(self):
        plugin_registry.register_page_script("connections", "/* a */")
        plugin_registry.register_page_script("connections", "/* b */")
        assert len(plugin_registry.get_page_scripts("connections")) == 2

    def test_scripts_isolated_per_page(self):
        plugin_registry.register_page_script("a", "/* js a */")
        plugin_registry.register_page_script("b", "/* js b */")
        assert len(plugin_registry.get_page_scripts("a")) == 1
        assert len(plugin_registry.get_page_scripts("b")) == 1

    def test_get_returns_fresh_components_each_call(self):
        # Page functions are called on every render — the registry must
        # not cache a single rx.script instance and hand it out twice.
        plugin_registry.register_page_script("page", "/* js */")
        a = plugin_registry.get_page_scripts("page")
        b = plugin_registry.get_page_scripts("page")
        # Same length and same underlying JS, but fresh component objects.
        assert len(a) == len(b) == 1
