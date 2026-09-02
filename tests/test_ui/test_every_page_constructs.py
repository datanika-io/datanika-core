"""Every Reflex page factory constructs, and none silently substitutes an icon (#701).

**Nothing in CI has ever constructed a Reflex page.** `test_settings_page_compiles.py`
covers `settings.py` and says so; [core#804] added `connections.py` and
`uploads.py` as a side effect of its own tests. Everything else was uncovered,
and the class that leaks through is not cosmetic:

* an invalid prop, or a Var that cannot resolve inside `rx.foreach`, raises at
  **app startup** — in production, on a deploy CI passed;
* an invalid icon tag degrades **silently**, printing to stdout where nothing
  is listening.

### The cost question, measured before building this

The issue hedged that "Reflex compilation is slow" and asked whether the guard
was worth it. That conflates two things. Full `reflex compile` (JS codegen) is
slow; **constructing the component tree is not**. Measured on this tree:

    58 page factories, 3.71 s total, 0 raised, python import 0.001 s

against a full suite of ~940 s. That is **0.4%** for a class whose alternative
detection mechanism is a failed production deploy. Built.

### Two ways this guard could pass while testing nothing

1. **A `parametrize` over an empty list passes as zero tests.** So
   `test_the_factory_list_is_not_silently_empty` asserts a floor.
2. **`redirect_stdout` is load-bearing and fragile.** Reflex *warns* about a bad
   icon tag on stdout; it does not raise. If Reflex ever switches to
   `warnings.warn` or a logger, the icon half of this goes green forever with no
   code change. Re-run the negative control in the docstring below whenever
   Reflex is bumped.

### Negative control (run it by hand; it is the point of the issue)

Add `rx.icon("definitely-not-an-icon")` to any page and confirm
`test_no_page_silently_substitutes_an_icon` goes red naming that factory. Add
`rx.text(nonexistent_var)` and confirm `test_every_page_factory_constructs`
goes red. A guard for silent failure that has never been watched failing is the
same defect it exists to catch.
"""

import importlib
import io
import pathlib
import re
from contextlib import redirect_stdout

import pytest
import reflex as rx

import datanika.ui.pages

PAGES_DIR = pathlib.Path(datanika.ui.pages.__file__).parent


def _page_factories() -> list[tuple[str, str]]:
    """(module, attribute) for every zero-argument `-> rx.Component` factory.

    Discovery rather than a hardcoded list, so a new page is covered the day it
    is written — a hand-maintained list is the version of this guard that goes
    stale silently. The annotation is the criterion because every page function
    in this package carries it, and it excludes helpers that take a row or a
    Var (`member_row(member)`, `_status_color(status)`), which cannot be
    constructed without one.
    """
    out = []
    for path in sorted(PAGES_DIR.glob("*.py")):
        if path.stem == "__init__":
            continue
        mod = importlib.import_module(f"datanika.ui.pages.{path.stem}")
        for attr in sorted(dir(mod)):
            if attr.startswith("_"):
                continue
            obj = getattr(mod, attr, None)
            if not callable(obj) or getattr(obj, "__module__", None) != mod.__name__:
                continue
            code = getattr(obj, "__code__", None)
            if code is None or code.co_argcount != 0:
                continue
            if getattr(obj, "__annotations__", {}).get("return") is not rx.Component:
                continue
            out.append((path.stem, attr))
    return out


FACTORIES = _page_factories()


def test_the_factory_list_is_not_silently_empty():
    """A `parametrize` over an empty list passes as zero tests.

    This is the failure mode a discovery-based guard is most likely to have: an
    annotation changes, discovery matches nothing, and the suite reports a
    clean run over no pages at all. The floor sits well below the current count
    (58) so ordinary churn does not trip it.
    """
    assert len(FACTORIES) >= 45, (
        f"only {len(FACTORIES)} page factories discovered in {PAGES_DIR}; "
        "discovery has stopped matching and every test below is now vacuous"
    )


def test_every_module_contributes_its_page_entry_point():
    """Each `X.py` must contribute `X_page` — the convention holds for all 21.

    ⚠️ **This assertion started as the weaker "every module contributes *some*
    factory", and a mutation showed that was blind.** Dropping the
    `-> rx.Component` annotation from `settings_page` left the test GREEN,
    because `settings.py` also defines `account_card`, `members_card` and five
    other factories — so the module still contributed. The page itself had
    silently fallen out of the parametrize list, which is exactly the failure
    mode this file exists to prevent, and the floor below has enough slack (58
    against 45) that losing one would not trip it either.

    Naming the entry point closes it. Discovery stays generic — this only pins
    the one factory per module whose absence means a *page* stopped being
    covered.
    """
    discovered = {(m, a) for m, a in FACTORIES}
    on_disk = sorted(p.stem for p in PAGES_DIR.glob("*.py") if p.stem != "__init__")
    missing = [m for m in on_disk if (m, f"{m}_page") not in discovered]
    assert not missing, (
        f"these modules no longer contribute a discoverable `<module>_page()`: "
        f"{missing} — the page gained an argument, lost its `-> rx.Component` "
        "annotation, or was renamed. Either way it is no longer being constructed "
        "by anything in CI."
    )


@pytest.mark.parametrize(("module", "attr"), FACTORIES, ids=lambda v: v)
def test_every_page_factory_constructs(module, attr):
    """The half that catches a bad prop or an unresolvable `rx.foreach` Var.

    Those raise at **app startup**, so without this they are found by a deploy
    that CI passed.
    """
    mod = importlib.import_module(f"datanika.ui.pages.{module}")
    component = getattr(mod, attr)()
    assert component is not None, f"{module}.{attr}() returned None"


@pytest.mark.parametrize(("module", "attr"), FACTORIES, ids=lambda v: v)
def test_no_page_silently_substitutes_an_icon(module, attr):
    """The half that catches an invalid lucide tag reached through composition.

    An AST sweep over literal tags (`test_icon_tags_are_real.py`) is the other
    half and catches more, earlier. This one catches it *where a user meets it*:
    the single bad tag on this tree lives in `components/info_tooltip.py` and
    surfaces through four page factories in two modules. Neither guard
    subsumes the other.
    """
    mod = importlib.import_module(f"datanika.ui.pages.{module}")
    buf = io.StringIO()
    with redirect_stdout(buf):
        getattr(mod, attr)()
    out = buf.getvalue()
    bad = sorted(set(re.findall(r"Invalid icon tag: ([\w-]+)", out)))
    assert not bad, (
        f"{module}.{attr} asks for {bad}, which lucide does not have. Reflex "
        "substitutes a different glyph and says so only on stdout, where "
        "nothing is listening in production."
    )
