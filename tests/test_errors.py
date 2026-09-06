"""core#1094 step 1 — every ValueError carrier we declare must also be a UserFacingError.

## What this file is defending

core#1032 narrowed ``is_user_facing`` to *"a ``ValueError``, unless it is one of
pydantic's."* That is a **negative** rule, and its failure mode is a dependency
putting a new class under ``ValueError`` in a code path a state handler wraps --
which arrives on a version-bump PR whose diff contains no exception handling and
is reviewed by someone thinking about lockfiles. core#1094 replaces it with the
positive test ``isinstance(exc, UserFacingError)``.

The migration is three steps and this file guards **step 1**: the 24 core
classes inherit the marker while the predicate is unchanged, so both rules agree
on every case and behaviour is provably identical. Cloud's single class
(``QuotaExceededError``) has the same guard in ``datanika-cloud``'s own
``tests/test_errors.py`` -- deliberately a twin rather than one cross-repo
sweep, because a per-repo guard satisfied by core and blind to cloud is the
shape that let core#943 happen after cloud had already fixed it.

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

    def test_the_predicate_still_accepts_it_before_the_contract_step(self):
        """Step 1 is behaviour-identical. This is what says so.

        ``is_user_facing`` is unchanged in this release; both the old negative
        rule and the new positive one accept a ``UserFacingError``. When step 3
        flips the predicate, this assertion is the one that must still hold.
        """
        from datanika.ui.state.base_state import is_user_facing

        assert is_user_facing(UserFacingError("Schedule limit reached")) is True
