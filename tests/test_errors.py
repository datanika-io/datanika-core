"""core#1094 — nothing in core reaches the user as a ValueError that is not the marker.

Two guards, one per migration step, and they are complements rather than
duplicates. **AC6** (`TestTheMarkerCoversWhatWeDeclare`) covers the 227 raises
that go through a *named class*: those migrate by inheritance, and the question
is whether every class we declare is under the marker. **AC3**
(`TestNoBareValueErrorInCore`) covers the 39 that did not: a bare
``raise ValueError(...)`` belongs to no class, so inheritance cannot reach it and
only a census can. Together they are the whole surface -- 266 raise sites.

## What this file is defending

core#1032 narrowed ``is_user_facing`` to *"a ``ValueError``, unless it is one of
pydantic's."* That is a **negative** rule, and its failure mode is a dependency
putting a new class under ``ValueError`` in a code path a state handler wraps --
which arrives on a version-bump PR whose diff contains no exception handling and
is reviewed by someone thinking about lockfiles. core#1094 replaces it with the
positive test ``isinstance(exc, UserFacingError)``.

The migration is three steps and this file guards the first two: the 24 core
classes inherit the marker, the 37 core bare sites are converted, and the
predicate is unchanged throughout — so both rules agree on every case and
behaviour is provably identical until step 3 flips it. Cloud's half
(``QuotaExceededError`` plus two bare sites) has the same pair of guards in
``datanika-cloud``'s own ``tests/test_errors.py`` -- deliberately a twin rather
than one cross-repo sweep, because a per-repo guard satisfied by core and blind
to cloud is the shape that let core#943 happen after cloud had already fixed it.

## Why the assertion is made at RUNTIME and not from the source text

The obvious guard is *"every ``class X(ValueError)`` must also name
``UserFacingError``"*, read with ``ast``. **That guard deletes itself the moment
it succeeds.** After step 1 nothing is declared ``(ValueError)`` any more -- they
all read ``(UserFacingError)`` -- so a source-text census of ``ValueError``
subclasses returns the empty set and passes vacuously, forever, including for
class 26 added next month under the old shape.

This is core#1069's lesson at a different layer: *a guard designed to expire
must be readable by the mechanism that expires it.* The mechanism here is the
**MRO**, so the question is put to the MRO. ``offenders`` walks
``ValueError``'s live subclass tree and reports any class of ours that is not
under the marker -- and that stays exactly as armed after the migration as
before it.

Source parsing survives for one job only: naming which modules to import, so
the subclass tree is populated. That import step is the thing that can silently
under-collect (QA measured a guard covering 13 of 17 models because
``Base.registry`` held only what pytest had already imported), so
``test_the_runtime_walk_saw_every_class_the_source_declares`` asserts the two
agree and names the difference.
"""

import ast
import builtins
import contextlib
import gc
import importlib
import pathlib

import pytest

from datanika.errors import UserFacingError

_PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[1] / "datanika"
_PACKAGE = "datanika"

#: Alembic revisions run offline from a migration harness. Nothing there is ever
#: caught by a state handler, and ``env.py`` is not importable as a module.
_SKIP_DIRS = {"migrations", "__pycache__", ".web"}


def _module_name(path: pathlib.Path) -> str:
    rel = path.relative_to(_PACKAGE_ROOT).with_suffix("")
    parts = list(rel.parts)
    if parts and parts[-1] == "__init__":
        parts.pop()
    return ".".join([_PACKAGE, *parts])


def _source_files() -> list[pathlib.Path]:
    return [
        p
        for p in sorted(_PACKAGE_ROOT.rglob("*.py"))
        if not _SKIP_DIRS & set(p.relative_to(_PACKAGE_ROOT).parts)
    ]


def _base_names(cls: ast.ClassDef) -> list[str]:
    """Base names as written. ``Attribute`` bases contribute their final attr."""
    out = []
    for base in cls.bases:
        if isinstance(base, ast.Name):
            out.append(base.id)
        elif isinstance(base, ast.Attribute):
            out.append(base.attr)
    return out


def declared_error_classes() -> dict[str, str]:
    """Map class name -> module, for every class whose declared bases reach ValueError.

    The closure is over *locally declared* names, so a class three levels down a
    chain of our own classes is included. It is only ever used to decide what to
    import -- the assertion itself is made against the MRO -- so a base this
    cannot resolve (an alias imported from elsewhere) costs coverage of the
    import list, not of the check.
    """
    declared: dict[str, tuple[str, list[str]]] = {}
    for path in _source_files():
        tree = ast.parse(path.read_bytes().decode("utf-8"), filename=str(path))
        module = _module_name(path)
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                declared[node.name] = (module, _base_names(node))

    roots = {"ValueError", UserFacingError.__name__}

    def reaches(name: str, seen: frozenset[str]) -> bool:
        if name in roots:
            return True
        if name in seen or name not in declared:
            return False
        return any(reaches(b, seen | {name}) for b in declared[name][1])

    return {
        name: module
        for name, (module, bases) in declared.items()
        if any(reaches(b, frozenset({name})) for b in bases)
    }


def bare_value_error_sites(source: str, label: str) -> list[str]:
    """Every place ``ValueError`` is *named as an exception* in one module's source.

    Two shapes, deliberately, because AC3's failure mode is a **missed** site and
    a missed site is silent:

    * ``raise ValueError`` -- a bare name with no call.
    * **any construction** ``ValueError(...)``, raised or not.

    The second is wider than "raise sites" on purpose. ``err = ValueError(msg)``
    followed by ``raise err`` is an ordinary refactor and it walks straight past
    a raise-only scanner -- the same class of blind spot as core#1069's
    literal-only migration scanner (``ENGINEERING_RULES.md`` §34), which could
    not see a contract migration because contract migrations loop over a
    constant. Measured at the time of writing: **zero** such constructions exist,
    so widening the predicate costs nothing today and closes the shape tomorrow.

    It deliberately does **not** match ``except ValueError``, ``isinstance(e,
    ValueError)`` or a bare annotation. Those read the class, they do not mint an
    instance, and every one of them is correct code that must keep working --
    ``UserFacingError`` is a ``ValueError``, so an existing ``except ValueError``
    still catches everything it caught before. That is what makes the whole
    migration behaviour-neutral.
    """
    tree = ast.parse(source, filename=label)
    hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Raise):
            exc = node.exc
            if isinstance(exc, ast.Name) and exc.id == "ValueError":
                hits.append(f"{label}:{node.lineno}")
        if isinstance(node, ast.Call):
            fn = node.func
            name = getattr(fn, "id", None) or getattr(fn, "attr", None)
            if name == "ValueError":
                hits.append(f"{label}:{node.lineno}")
    return sorted(set(hits))


def bare_value_error_census() -> list[str]:
    """The same predicate over every file in the package.

    ⚠️ **Scope is the whole package minus ``migrations/``, which is wider than
    AC3's ``services/`` + ``ui/`` + ``tasks/``.** Measured before choosing: a
    whole-tree census and a three-layer census return the *same 37 sites* today,
    so the wider scope costs nothing and does not depend on my model of which
    layers a state handler can reach. ``_safe_error`` catches ``Exception``
    around service calls, and services call models, so "the wrapped layers" is
    not a set anyone can enumerate confidently.

    Alembic revisions are excluded because they run offline from a migration
    harness and are never caught by a state handler.
    """
    hits: list[str] = []
    for path in _source_files():
        rel = path.relative_to(_PACKAGE_ROOT.parent).as_posix()
        hits += bare_value_error_sites(path.read_bytes().decode("utf-8"), rel)
    return sorted(hits)


def offenders(root: type, marker: type, package: str) -> list[str]:
    """Classes under ``root``, declared in ``package``, that are not under ``marker``.

    Module-level and parameterised so the check's own discrimination can be armed
    in-suite against a synthetic class, rather than resting on a mutation run
    somebody performed once in a past session.
    """
    found: list[str] = []

    def walk(cls: type) -> None:
        for sub in cls.__subclasses__():
            module = getattr(sub, "__module__", "") or ""
            # Segment match, never a bare prefix: "datanika_cloud".startswith(
            # "datanika") is True, and cloud is guarded by its own twin of this
            # file rather than smuggled into core's verdict.
            own = module == package or module.startswith(package + ".")
            if own and sub is not marker and not issubclass(sub, marker):
                found.append(f"{module}.{sub.__qualname__}")
            walk(sub)

    walk(root)
    return sorted(set(found))


@pytest.fixture(scope="module")
def imported_error_modules() -> dict[str, str]:
    """Import every module declaring an error class, and return the census.

    Without this the subclass tree holds only what the rest of the session
    happened to import, and the guard passes by not looking.
    """
    census = declared_error_classes()
    for module in sorted(set(census.values())):
        importlib.import_module(module)
    return census


class TestTheMarkerCoversWhatWeDeclare:
    def test_the_marker_covers_every_value_error_subclass_we_declare(self, imported_error_modules):
        """AC6. The whole point: class 26 cannot be added under the old shape."""
        rogue = offenders(ValueError, UserFacingError, _PACKAGE)
        assert rogue == [], (
            "these classes are ValueError subclasses but not UserFacingError "
            f"subclasses, so their text stops reaching the user at core#1094's "
            f"contract step: {rogue}"
        )

    def test_the_runtime_walk_saw_every_class_the_source_declares(self, imported_error_modules):
        """Anti-vacuity. A green above means nothing if the tree was not populated."""
        seen = set()

        def walk(cls: type) -> None:
            for sub in cls.__subclasses__():
                seen.add(sub.__name__)
                walk(sub)

        walk(ValueError)
        missing = sorted(set(imported_error_modules) - seen)
        assert missing == [], (
            "the source declares these error classes and the runtime subclass "
            f"tree does not hold them, so the check above did not look at them: {missing}"
        )

    def test_the_census_is_not_empty(self, imported_error_modules):
        """The other half of anti-vacuity: a scanner that walks nothing reports clean."""
        assert len(_source_files()) > 100, "the source scan found almost no files"
        assert len(imported_error_modules) >= 24, (
            "core declared 24 ValueError carriers when core#1094 was measured; "
            f"the census now finds {len(imported_error_modules)}, so either classes "
            "were removed or the scanner stopped seeing them"
        )


@contextlib.contextmanager
def _temporary_subclass(name: str, base: type, module: str):
    """Yield ``"<module>.<name>"`` for a class that exists only inside the block.

    ``__subclasses__()`` holds weak references, so a synthetic control class
    stays in the tree until it is collected -- long enough to be counted by a
    *later* test and make an unrelated assertion red for a reason that is not in
    its own body. The explicit ``gc.collect()`` is what stops these controls
    contaminating the census above.
    """
    cls = type(name, (base,), {"__module__": module})
    try:
        yield f"{module}.{name}"
    finally:
        del cls
        gc.collect()


class TestTheGuardCanStillFail:
    """In-suite arming, so the discrimination is re-proved on every CI run.

    Each control asserts **membership**, never that the whole result is empty --
    an emptiness assertion would silently be testing the state of the tree
    instead of the behaviour of the predicate, and would have to be rewritten
    the first time a genuine offender appeared.
    """

    def test_it_reports_a_class_of_ours_that_missed_the_marker(self):
        with _temporary_subclass("_RogueError", ValueError, f"{_PACKAGE}.services.x") as qualname:
            assert qualname in offenders(ValueError, UserFacingError, _PACKAGE)

    def test_it_stays_quiet_for_a_class_of_ours_that_has_the_marker(self):
        with _temporary_subclass(
            "_GoodError", UserFacingError, f"{_PACKAGE}.services.x"
        ) as qualname:
            assert qualname not in offenders(ValueError, UserFacingError, _PACKAGE)

    def test_it_ignores_a_foreign_class(self):
        """pydantic's ValidationError is the class this whole issue is about."""
        with _temporary_subclass("_ForeignError", ValueError, "somevendor.core") as qualname:
            assert qualname not in offenders(ValueError, UserFacingError, _PACKAGE)

    def test_the_package_filter_is_a_segment_match_not_a_prefix(self):
        """``datanika_cloud`` must not be scored by core's guard, in either direction.

        ``"datanika_cloud".startswith("datanika")`` is ``True``, so a bare prefix
        test would pull cloud into core's verdict -- making it depend on whether
        cloud happened to be installed. Cloud has its own twin of this file, and
        a guard that answers differently depending on the environment is not a
        guard.
        """
        with _temporary_subclass(
            "_SiblingError", ValueError, "datanika_cloud.billing.service"
        ) as qualname:
            assert qualname not in offenders(ValueError, UserFacingError, _PACKAGE)


class TestTheMarkerItself:
    def test_it_is_a_valueerror_subclass(self):
        """Permanent, and load-bearing: ``except ValueError`` sites must keep catching."""
        assert issubclass(UserFacingError, ValueError)

    def test_its_text_survives_str(self):
        assert str(UserFacingError("Schedule limit reached (2 on Free plan)")) == (
            "Schedule limit reached (2 on Free plan)"
        )

    def test_the_predicate_accepts_it(self):
        """The one assertion that had to hold across all three steps, and did.

        Under step 1's negative rule (*"a ``ValueError``, unless pydantic's"*) and
        under step 3's positive one (``isinstance(exc, UserFacingError)``) alike,
        a ``UserFacingError`` reaches the user. That is what made steps 1 and 2
        behaviour-identical and what makes the flip a no-op for every carrier.

        It is deliberately here rather than only in
        ``tests/test_ui/test_safe_error_narrowing.py``: that file tests the
        predicate, this one tests the marker, and the sentence *"the marker is
        the thing the predicate accepts"* is the join between them.
        """
        from datanika.ui.state.base_state import is_user_facing

        assert is_user_facing(UserFacingError("Schedule limit reached")) is True
        # And the contract step's other half, from this side: a bare ValueError
        # is no longer accepted. `test_the_package_raises_no_bare_value_error`
        # below is what says nothing in core relies on that.
        assert is_user_facing(ValueError("Schedule limit reached")) is False


class TestNoBareValueErrorInCore:
    """AC3, core's half. Cloud's twin asserts the same over ``datanika_cloud/``.

    This is what makes core#1094's contract step safe. Once the predicate reads
    ``isinstance(exc, UserFacingError)``, a surviving ``raise ValueError(...)``
    stops reaching the user **silently** -- the handler shows its fallback and
    nothing raises, so there is no error, no log line and no failing test. A
    census is the only instrument that can see that *in advance*, because the
    diff of a missed site is empty.

    The 37 sites this replaced were, by layer: services 30, tasks 3, ui 4.
    """

    def test_the_package_raises_no_bare_value_error(self):
        sites = bare_value_error_census()
        assert sites == [], (
            "these sites name ValueError directly; after core#1094's contract "
            f"step their text stops reaching the user with no error anywhere: {sites}"
        )

    def test_the_census_actually_read_the_tree(self):
        """A scanner handed nothing reports clean.

        The link-auditor instance of this trap -- a directory walker given a file
        -- printed ``0 refs, 0 not resolving`` for a spec nobody had checked.
        """
        assert len(_source_files()) > 100, (
            f"the census walked {len(_source_files())} files; it is reporting on nothing"
        )


class TestTheCensusCanStillSeeOne:
    """AC3's control, in-suite so it is re-proved on every CI run.

    A census that stops matching reports a clean tree, and that is the direction
    this one fails in -- so the arming is not optional. Both polarities, because
    a predicate narrowed until it matches nothing also stops matching real
    violations, which is the worse bug and the silent one.
    """

    def test_it_sees_a_bare_raise(self):
        assert bare_value_error_sites('raise ValueError("nope")\n', "x.py") == ["x.py:1"]

    def test_it_sees_a_raise_with_no_call(self):
        assert bare_value_error_sites("raise ValueError\n", "x.py") == ["x.py:1"]

    def test_it_sees_a_construction_that_is_raised_later(self):
        """The evasion a raise-only scanner walks past, and an ordinary refactor."""
        src = 'def f():\n    err = ValueError("nope")\n    raise err\n'
        assert bare_value_error_sites(src, "x.py") == ["x.py:2"]

    def test_it_does_not_see_the_marker(self):
        assert bare_value_error_sites('raise UserFacingError("fine")\n', "x.py") == []

    def test_it_does_not_see_a_catch(self):
        """``except ValueError`` must keep working -- the marker is a ValueError."""
        src = "try:\n    pass\nexcept ValueError:\n    pass\n"
        assert bare_value_error_sites(src, "x.py") == []

    def test_it_does_not_see_an_isinstance_check(self):
        assert bare_value_error_sites("isinstance(e, ValueError)\n", "x.py") == []

    def test_it_does_not_see_a_class_declaration(self):
        """``class X(ValueError)`` is AC6's business, and is now zero anyway."""
        assert bare_value_error_sites("class X(ValueError):\n    pass\n", "x.py") == []


# ==========================================================================
# core#1113 — the seven sites whose text is for an operator or a developer.
# ==========================================================================

#: The core half of core#1113's table, as (module path -> function name). Cloud's
#: two (``billing/paddle.py``, ``billing/e2e_admin.py``) are guarded by cloud's own
#: twin of this file, for the reason stated at the top: a per-repo guard satisfied
#: by core and blind to cloud is the shape that let core#943 happen.
#:
#: ⚠️ The pairs are named rather than derived, and that is a weakness — so
#: ``test_every_named_function_exists_and_raises_something`` refuses a vacuous pass,
#: and ``TestTheOperatorTextScannerCanStillSeeOne`` arms the scanner in-suite
#: against a synthetic module in exactly the shape of a reverted site.
_OPERATOR_TEXT_SITES: dict[str, str] = {
    "datanika/services/audit_service.py": "_redact",
    "datanika/services/auth_redirects.py": "login_error_path",
    "datanika/ui/components/secure_input.py": "autofill_attrs",
}


def dotted_name(node: ast.expr) -> str | None:
    """``httpx.HTTPStatusError`` stays whole; ``ConfigurationError`` stays bare.

    ⚠️ Keeping only the final attribute is what a first version did, and it throws
    away the one qualifier that makes the name resolvable. Under a resolver that
    fails closed, that turns a legitimate third-party raise into a false red — and
    under one that fails open it turns an unresolvable name into a pass.
    """
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return ".".join(reversed(parts))
    return None


def resolve_raised(dotted: str, module, errors_module) -> object | None:
    """Resolve a dotted name against the raising module, then datanika.errors, then
    builtins. Returns ``None`` when it cannot be resolved, and every caller treats
    that as a failure rather than as absence."""
    head, *rest = dotted.split(".")
    obj = getattr(module, head, None)
    if obj is None:
        obj = getattr(errors_module, head, None)
    if obj is None:
        obj = getattr(builtins, head, None)
    for part in rest:
        if obj is None:
            return None
        obj = getattr(obj, part, None)
    return obj


def raised_class_names(source: str, function: str) -> list[str]:
    """Every class name raised inside ``function``, as written.

    Names only — resolution to a class is done by importing, because the property
    under test is the **MRO** and a source-text answer cannot see inheritance. Same
    reasoning as AC6's guard at the top of this file.
    """
    tree = ast.parse(source, filename=function)
    names: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name != function:
            continue
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Raise) or inner.exc is None:
                continue
            exc = inner.exc
            call = exc.func if isinstance(exc, ast.Call) else exc
            name = dotted_name(call)
            if name:
                names.append(name)
    return names


class TestOperatorTextIsOutsideValueError:
    """core#1113, decided by Product 2026-09-06: option (2) for all seven.

    Not a style preference. ``UserFacingError``'s docstring forbids operator and
    developer text under the marker, and once core#1094's contract step landed the
    marker is what decides whether a string reaches a person. These seven carried an
    env var name, an issue number, an instruction to edit a dict, and an internal
    invariant about a payload the user never composed.

    ⚠️ **Moving a raise out of ``ValueError``'s subtree is a real behaviour change and
    it lands immediately** — callers catching ``ValueError`` stop catching it. Checked
    for all five core sites before the move, by reading the call sites rather than
    trusting the issue: all 4 ``log_action`` callers pass payloads our own code built
    (three inside ``except Exception``, the fourth a literal ``{}``); all 18
    ``login_error_path`` callers pass literals, 13 distinct, every one already in
    ``AUTH_ERROR_KEYS``; ``autofill_attrs`` raises at page build. No
    ``except ValueError`` appears in any of the files involved.
    """

    @pytest.mark.parametrize(("path", "function"), sorted(_OPERATOR_TEXT_SITES.items()))
    def test_the_site_raises_nothing_under_valueerror(self, path, function):
        """🚨 **Fails closed on a name it cannot resolve**, and that is not tidiness.

        The first version treated an unresolvable name as clean, and the mutation
        sweep refuted it: reverting a raise to ``UserFacingError`` **without also
        restoring the import** leaves ``getattr(module, "UserFacingError")`` returning
        ``None``, so the offender list stayed empty and two arms came back GREEN where
        RED was declared. That is §42's family — *"no offender found"* is true when
        there is none and equally true when the lookup could not run.
        """
        import importlib

        module_name = path.removesuffix(".py").replace("/", ".")
        module = importlib.import_module(module_name)
        errors_module = importlib.import_module("datanika.errors")
        source = (_PACKAGE_ROOT.parent / path).read_bytes().decode("utf-8")

        offending, unresolved = [], []
        for name in raised_class_names(source, function):
            cls = resolve_raised(name, module, errors_module)
            if not isinstance(cls, type):
                unresolved.append(name)
            elif issubclass(cls, ValueError):
                offending.append(name)

        assert not unresolved, (
            f"{path}::{function} raises {unresolved}, which resolve to no class here. "
            "Treating that as clean is how a reverted site passes: the raise moves "
            "back to UserFacingError while the import still names something else"
        )
        assert not offending, (
            f"{path}::{function} raises {offending}, which are ValueError subclasses. "
            "After core#1094's contract step that text renders to a user who can do "
            "nothing with it (core#1113)"
        )

    @pytest.mark.parametrize(("path", "function"), sorted(_OPERATOR_TEXT_SITES.items()))
    def test_every_named_function_exists_and_raises_something(self, path, function):
        """Anti-vacuity. A renamed function makes the test above pass on an empty
        list — the census-that-read-nothing failure, one layer in."""
        source = (_PACKAGE_ROOT.parent / path).read_bytes().decode("utf-8")

        assert raised_class_names(source, function), (
            f"no raise found in {path}::{function} — either it was renamed or the "
            "scanner has drifted, and either way this guard is reporting on nothing"
        )

    def test_the_open_question_annotation_is_gone(self):
        """AC3. The ``# core#1113`` comments said the decision had not been taken.

        Banned by the sentence that *made* them open questions, not by the issue
        number: the number is still a legitimate citation for why each class is what
        it is, and a guard banning it would forbid explaining the decision.
        """
        stale = [
            path.relative_to(_PACKAGE_ROOT.parent).as_posix()
            for path in _source_files()
            if "keep core#1094 step 2 behaviour-neutral" in path.read_bytes().decode("utf-8")
        ]

        assert not stale, (
            "these files still carry core#1094's temporary-conversion note, which says "
            f"the core#1113 decision is open. It is not: {stale}"
        )


class TestTheOperatorTextScannerCanStillSeeOne:
    """Product's replacement AC2, part 3: revert a site and confirm it is named.

    In-suite rather than a mutation somebody ran once, for the reason the file's
    other controls exist: a scanner that stops matching reports a clean tree.
    """

    _REVERTED = (
        "from datanika.errors import UserFacingError\n"
        "\n"
        "def login_error_path(reason: str) -> str:\n"
        '    raise UserFacingError("add it to AUTH_ERROR_KEYS with an i18n key")\n'
    )

    def test_it_names_the_class_a_reverted_site_would_raise(self):
        assert raised_class_names(self._REVERTED, "login_error_path") == ["UserFacingError"]

    def test_it_sees_a_raise_of_a_dotted_name(self):
        """``raise errors.UserFacingError(...)`` is the evasion an attribute-blind
        scanner walks past, and it is an ordinary import style."""
        source = 'def f():\n    raise errors.UserFacingError("x")\n'

        assert raised_class_names(source, "f") == ["errors.UserFacingError"]

    def test_it_ignores_raises_in_other_functions(self):
        """Otherwise a neighbouring function's legitimate ``UserFacingError`` would
        fail the site under test, and the guard gets deleted."""
        source = (
            'def other():\n    raise UserFacingError("a refusal for a user")\n\n'
            'def target():\n    raise InternalInvariantError("not for a user")\n'
        )

        assert raised_class_names(source, "target") == ["InternalInvariantError"]

    def test_it_reports_nothing_for_a_function_that_does_not_exist(self):
        """The anti-vacuity test above depends on this being the empty list rather
        than an exception."""
        assert raised_class_names("def f():\n    pass\n", "missing") == []
