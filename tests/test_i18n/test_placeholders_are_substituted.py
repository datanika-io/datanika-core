"""Every ``{placeholder}`` in a translated value is substituted where that value is painted.

i18n substitution in core is **per call site, not automatic**. ``_t["some.key"]`` compiles to a
dict lookup and paints the value verbatim, so a value carrying ``{placeholder}`` spans puts the
braces in the DOM (#1540). ``test_all_locales_have_same_keys`` cannot see this: it compares key
*sets*, and every one of these keys exists and is translated in all nine locales. The defect is in
the **painting**.

🚨 **Core has THREE substitution mechanisms, and an instrument that knows some of them reports the
rest as absent.** That error has now been made twice, in both directions:

=========================== ====================================== =====
mechanism                    shape                                  keys
=========================== ====================================== =====
``Var.replace``              ``_t[k].replace("{p}", <Var>)`` →       2
                             JS ``replaceAll``, client-side
``i18n_text.interpolate``    weaves *components* into a template     1
``str.replace`` server-side  ``(await self._translated(k, …))``      7
                             ``.replace("{p}", <str>)``
=========================== ====================================== =====

#1540's own census looked only for ``.replace(`` in page files and reported **eight** broken sites;
``interpolate`` was invisible to it, so the correct signup sentence was scored as broken. The issue
was then written naming **two** mechanisms — and the third, server-side ``str.replace``, covers
**7 of the 10 correct keys**, i.e. the majority. A guard built to the issue's own text would have
reported seven correct keys as defective.

🔑 So the population here is **the key**, never "``_t[...]`` call sites": five of the fifteen keys
never appear at a ``_t[...]`` site at all — they live in a module-level dict and are read through
``self._translated(key, …)`` with a *variable* key.

⚠️ **The assertion is the PRESENCE of substitution, never the absence of braces** (WORKFLOW_RULES
§4). An "no line contains ``{``" form is satisfied by deleting the call site, and would go red on a
correct fix that keeps the template.
"""

from __future__ import annotations

import ast
import json
import pathlib
import re

import pytest

CORE = pathlib.Path(__file__).resolve().parents[2] / "datanika"
I18N = CORE / "i18n"
LOCALES = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]

#: A placeholder span. Deliberately narrow — ``{0}``/``{}`` style is not used in these values, and
#: widening it to ``\{[^}]*\}`` would match CSS/f-string debris in unrelated values.
PLACEHOLDER = re.compile(r"\{([a-z_]+)\}")


def _en() -> dict[str, str]:
    return json.loads((I18N / "en.json").read_text(encoding="utf-8"))


def _placeholder_keys(values: dict[str, str]) -> dict[str, set[str]]:
    return {
        k: set(PLACEHOLDER.findall(v))
        for k, v in values.items()
        if isinstance(v, str) and PLACEHOLDER.search(v)
    }


# --------------------------------------------------------------------------------------------
# Orphan ratchet — a placeholder key that nothing paints.
#
# A key with no consumption site is not a defect: nothing renders it, so nothing shows braces.
# But treating that as silently fine would mean DELETING a call site makes this guard pass, which
# is the exact shape §4 warns about. So every orphan is declared, with its reason, and the ratchet
# is TWO-WAY: if an orphan gains a consumer the entry goes stale and the test fails *for having a
# stale entry*. Exemptions cannot quietly accumulate, and a fix cannot quietly rot.
#
# 🔑 **Imported, not copied.** The same declaration is what stops `test_i18n.py`'s
# `test_no_orphan_keys_in_json` demanding these keys be deleted — its documented remedy for an
# orphan, which here would drop nine translations. Two guards, two independent scanners (that one
# is regex over `datanika/ui`, this one is an AST walk over all of `datanika/`), **one
# declaration**. A second copy would drift from the first, which is what retired the PLAN_*.md
# files.
# --------------------------------------------------------------------------------------------
from tests.test_i18n.test_i18n import RESERVED_UNUSED_KEYS  # noqa: E402


# --------------------------------------------------------------------------------------------
# Finding the consumption scope of a key
# --------------------------------------------------------------------------------------------
def _innermost_functions(tree: ast.Module) -> dict[int, ast.AST]:
    """id(node) -> innermost enclosing function node."""
    owner: dict[int, ast.AST] = {}
    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for sub in ast.walk(fn):
                # Later (deeper) writes win: ast.walk is breadth-first, so a nested function is
                # visited after its parent and overwrites the parent's claim on its children.
                owner[id(sub)] = fn
    return owner


def _module_binding_for(tree: ast.Module, const: ast.Constant) -> str | None:
    """If ``const`` sits inside a module-level assignment, return the bound name.

    Five of the fifteen keys live in ``connection_state._VERDICT_KEYS`` and are read through a
    *variable* key, so there is no ``_t["..."]`` site to scope to.
    """
    for stmt in tree.body:
        if isinstance(stmt, (ast.Assign, ast.AnnAssign)) and any(
            n is const for n in ast.walk(stmt)
        ):
            targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
            for t in targets:
                if isinstance(t, ast.Name):
                    return t.id
    return None


def _functions_loading(tree: ast.Module, name: str) -> list[ast.AST]:
    out = []
    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
            isinstance(n, ast.Name) and n.id == name and isinstance(n.ctx, ast.Load)
            for n in ast.walk(fn)
        ):
            out.append(fn)
    return out


def _substitutions_in(scope: ast.AST) -> set[str]:
    """Placeholder names this subtree substitutes, by any of the three mechanisms."""
    names: set[str] = set()
    for node in ast.walk(scope):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        # (1) Var.replace  and  (3) server-side str.replace — same AST shape.
        if isinstance(func, ast.Attribute) and func.attr == "replace" and node.args:
            first = node.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                m = re.fullmatch(r"\{([a-z_]+)\}", first.value)
                if m:
                    names.add(m.group(1))
        # (2) i18n_text.interpolate — slots are keyword arguments named for the placeholders.
        target = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if target == "interpolate":
            names.update(kw.arg for kw in node.keywords if kw.arg)
    return names


def _consumption_sites() -> dict[str, list[tuple[str, int, set[str]]]]:
    """key -> [(file, line, placeholders substituted in that key's scope)]."""
    wanted = _placeholder_keys(_en())
    sites: dict[str, list[tuple[str, int, set[str]]]] = {k: [] for k in wanted}
    for path in sorted(CORE.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):  # pragma: no cover - no such file today
            continue
        owner = _innermost_functions(tree)
        rel = path.relative_to(CORE.parent).as_posix()
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
                continue
            if node.value not in wanted:
                continue
            fn = owner.get(id(node))
            if fn is not None:
                scopes: list[ast.AST] = [fn]
            else:
                bound = _module_binding_for(tree, node)
                scopes = _functions_loading(tree, bound) if bound else []
            subs: set[str] = set()
            for s in scopes:
                subs |= _substitutions_in(s)
            sites[node.value].append((rel, node.lineno, subs))
    return sites


# --------------------------------------------------------------------------------------------
# Anti-vacuity: prove each half of the instrument RETURNS THE THING before believing a clean run.
# Coverage and sensitivity are different properties (coordinator rule 26).
# --------------------------------------------------------------------------------------------
class TestTheInstrumentCanSee:
    def test_the_placeholder_regex_finds_placeholders(self):
        """A regex that matches nothing makes every case below pass."""
        keys = _placeholder_keys(_en())
        assert len(keys) >= 10, f"only {len(keys)} placeholder-carrying keys — regex is broken"
        assert PLACEHOLDER.findall("{used} / {limit} GB") == ["used", "limit"]
        assert PLACEHOLDER.findall("no placeholders here") == []

    def test_every_placeholder_key_is_found_in_source_or_declared_an_orphan(self):
        """A key nothing paints is fine; a key nothing paints AND nobody declared is not."""
        sites = _consumption_sites()
        undeclared = sorted(k for k, s in sites.items() if not s and k not in RESERVED_UNUSED_KEYS)
        assert not undeclared, (
            "placeholder keys with no consumption site and no RESERVED_UNUSED_KEYS entry: "
            f"{undeclared}. Either something paints them (and this guard cannot see it — say how), "
            "or add an entry naming why nothing does."
        )

    def test_the_substitution_finder_sees_all_three_mechanisms(self):
        """Each arm proved against a real shape. A finder blind to one reports it as absent."""
        var_replace = ast.parse('def f():\n _t["k"].replace("{used}", X).replace("{limit}", Y)\n')
        assert _substitutions_in(var_replace) == {"used", "limit"}

        interp = ast.parse("def f():\n interpolate(t, terms=a, privacy=b)\n")
        assert _substitutions_in(interp) == {"terms", "privacy"}

        server_side = ast.parse(
            'async def f():\n t = await g("k", "d")\n return t.replace("{arg}", v)\n'
        )
        assert _substitutions_in(server_side) == {"arg"}

        assert _substitutions_in(ast.parse("def f():\n return 1\n")) == set()
        # A non-placeholder .replace must NOT be counted — `field.replace("_", "-")` is all over
        # secure_input.py and would otherwise manufacture substitutions out of slug-making.
        assert _substitutions_in(ast.parse('def f():\n s.replace("_", "-")\n')) == set()

    def test_the_known_mechanism_sites_are_actually_detected(self):
        """The three real sites, by file. If a refactor moves them, this says so."""
        sites = _consumption_sites()
        by_file = {k: {f for f, _, _ in v} for k, v in sites.items()}
        assert any("dashboard.py" in f for f in by_file["quota.volume_usage"])
        assert any("signup.py" in f for f in by_file["legal.signup_agreement"])
        assert any("connection_state.py" in f for f in by_file["connections.test_timed_out"])


class TestEveryPaintedPlaceholderIsSubstituted:
    """#1540's AC3. The invariant, not today's fifteen."""

    def test_no_call_site_paints_an_unsubstituted_placeholder(self):
        wanted = _placeholder_keys(_en())
        sites = _consumption_sites()
        broken = []
        for key, needed in sorted(wanted.items()):
            for path, line, subs in sites[key]:
                missing = sorted(needed - subs)
                if missing:
                    broken.append(f"{path}:{line}  {key}  paints {missing} unsubstituted")
        assert not broken, (
            "translated values painted with their braces intact:\n  "
            + "\n  ".join(broken)
            + "\n\nSubstitute at the call site (Var.replace for values, interpolate for "
            "components, str.replace server-side). Re-wording the nine locale values to drop "
            "the braces is NOT equivalent — ru/zh/ar place placeholders mid-phrase (#682)."
        )


class TestTheOrphanRatchetIsTwoWay:
    """An exemption that cannot go stale is how exemptions accumulate."""

    def test_every_declared_orphan_is_still_an_orphan(self):
        sites = _consumption_sites()
        wrong = sorted(k for k in RESERVED_UNUSED_KEYS if sites.get(k))
        assert not wrong, (
            f"RESERVED_UNUSED_KEYS entries that now HAVE a consumption site: {wrong}. "
            "A producer arrived. Delete the entry — the site is now covered by AC3 above."
        )

    def test_every_declared_orphan_still_exists_and_still_has_placeholders(self):
        wanted = _placeholder_keys(_en())
        gone = sorted(k for k in RESERVED_UNUSED_KEYS if k not in wanted)
        assert not gone, (
            f"RESERVED_UNUSED_KEYS entries that are no longer placeholder-carrying keys: {gone}. "
            "The key was deleted or re-worded — delete the entry."
        )

    def test_each_orphan_entry_gives_a_reason(self):
        for key, reason in RESERVED_UNUSED_KEYS.items():
            assert len(reason) > 80, f"{key}: an exemption without a reason is just a hole"
            assert "#1540" in reason, f"{key}: name the issue that owns the gap"


class TestPlaceholderSetsAgreeAcrossLocales:
    """#1540's AC4.

    A translator who renames or drops a placeholder leaves an unsubstituted brace **in that
    locale only** — invisible to any English-only check, and invisible to key parity, which
    compares key sets rather than values.
    """

    @pytest.mark.parametrize("locale", [loc for loc in LOCALES if loc != "en"])
    def test_locale_names_the_same_placeholders_as_english(self, locale):
        en = _en()
        other = json.loads((I18N / f"{locale}.json").read_text(encoding="utf-8"))
        mismatches = []
        for key, expected in sorted(_placeholder_keys(en).items()):
            if key not in other or not isinstance(other[key], str):
                continue
            actual = set(PLACEHOLDER.findall(other[key]))
            if actual != expected:
                mismatches.append(
                    f"{key}: en={sorted(expected)} {locale}={sorted(actual)}"
                    f" (missing={sorted(expected - actual)} extra={sorted(actual - expected)})"
                )
        assert not mismatches, f"placeholder drift in {locale}.json:\n  " + "\n  ".join(mismatches)
