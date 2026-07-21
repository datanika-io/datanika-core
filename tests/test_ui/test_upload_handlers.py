"""Every ``rx.upload`` handler must satisfy Reflex's own discovery rule.

``POST /_upload`` does not call the handler we name in ``on_drop``. It looks the
handler up by scanning its type hints for a parameter annotated as a *generic
alias* whose first argument is ``UploadFile`` (``reflex/app.py::upload()``)::

    for k, v in get_type_hints(func).items():
        if is_generic_alias(v) and issubclass(get_args(v)[0], UploadFile):
            handler_upload_param = (k, v)
            break
    if not handler_upload_param:
        raise UploadValueError(...)

A bare ``list`` is not a generic alias, so the scan finds nothing and the
endpoint 500s before the handler body ever runs. That shipped: connection file
upload was dead in production for CSV/JSON/Parquet — the zero-credentials
onboarding path — while ``handle_file_upload`` itself was perfectly correct
(#452).

Unit tests could not catch it, and passing ones existed. They called the
handler directly with a list of fakes, which exercises the body and skips the
part that was broken: how Reflex *finds* the body. So this guard asserts the
contract at the boundary that actually enforces it, for every upload surface in
the app rather than the one that happened to break.
"""

import ast
import functools
import importlib
from pathlib import Path
from typing import get_args, get_origin, get_type_hints

import pytest
from reflex.components.core.upload import UploadFile
from reflex.event import EventHandler

import datanika.ui

UI_ROOT = Path(datanika.ui.__file__).parent

# Every ``rx.upload`` site we know about. A scan that silently finds nothing
# would pass this file without asserting anything, so the count is pinned: add
# an upload surface and this number moves with it, deliberately.
EXPECTED_UPLOAD_SITES = 3


def _module_name(path: Path) -> str:
    rel = path.relative_to(UI_ROOT).with_suffix("")
    return "datanika.ui." + ".".join(rel.parts)


def _handler_ref(node: ast.AST) -> tuple[str, str] | None:
    """``State.handler(...)`` or a bare ``State.handler`` -> ("State", "handler")."""
    if isinstance(node, ast.Call):
        node = node.func
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        return node.value.id, node.attr
    return None


def _discover_upload_sites() -> list[tuple[str, str, str, int]]:
    """(module, state class, handler, lineno) for every ``rx.upload(on_drop=...)``."""
    sites: list[tuple[str, str, str, int]] = []

    for path in sorted(UI_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            is_upload = (
                isinstance(func, ast.Attribute)
                and func.attr == "upload"
                and isinstance(func.value, ast.Name)
                and func.value.id == "rx"
            )
            if not is_upload:
                continue

            for keyword in node.keywords:
                if keyword.arg != "on_drop":
                    continue
                ref = _handler_ref(keyword.value)
                assert ref is not None, (
                    f"{path}:{keyword.value.lineno} has an on_drop this guard "
                    "cannot resolve to a State.handler pair — teach it the new "
                    "shape rather than leaving the site unchecked."
                )
                sites.append((_module_name(path), *ref, node.lineno))

    return sites


UPLOAD_SITES = _discover_upload_sites()


def _reflex_upload_param(func) -> tuple[str, object] | None:
    """Reflex's rule, re-expressed with stdlib typing rather than its internals.

    The first version of this called ``reflex.utils.types.is_generic_alias`` and
    ``types._issubclass`` so it would track the rule verbatim. That is the more
    faithful test and it does not survive an upgrade: those are private, and CI
    resolves a Reflex where ``_issubclass`` has become ``safe_issubclass`` — so
    the guard failed on the *correct* handlers for a reason that had nothing to
    do with the contract it exists to check.

    ``get_origin`` / ``get_args`` express the same condition ("a generic alias
    whose first argument is an ``UploadFile`` subclass") out of the standard
    library, which is stable. ``UploadFile`` is still imported from Reflex —
    that part is public, and it is the thing being matched.
    """
    if isinstance(func, EventHandler):
        func = func.fn
    if isinstance(func, functools.partial):
        func = func.func

    for name, hint in get_type_hints(func).items():
        args = get_args(hint)
        if get_origin(hint) is None or not args:
            continue
        if isinstance(args[0], type) and issubclass(args[0], UploadFile):
            return name, hint
    return None


class TestEveryUploadHandlerIsDiscoverable:
    def test_the_scan_found_every_upload_site(self):
        """Otherwise a refactor silently empties the parametrisation below."""
        assert len(UPLOAD_SITES) == EXPECTED_UPLOAD_SITES, (
            f"Found {len(UPLOAD_SITES)} rx.upload sites, expected "
            f"{EXPECTED_UPLOAD_SITES}: {UPLOAD_SITES}"
        )

    @pytest.mark.parametrize(
        ("module_name", "state_name", "handler_name", "lineno"),
        UPLOAD_SITES,
        ids=[f"{s[1]}.{s[2]}" for s in UPLOAD_SITES],
    )
    def test_handler_has_an_uploadfile_annotated_parameter(
        self, module_name, state_name, handler_name, lineno
    ):
        module = importlib.import_module(module_name)
        state_cls = getattr(module, state_name)
        handler = getattr(state_cls, handler_name)

        assert _reflex_upload_param(handler) is not None, (
            f"{state_name}.{handler_name} (used by rx.upload at "
            f"{module_name}:{lineno}) has no parameter annotated "
            "list[rx.UploadFile]. Reflex cannot find the upload parameter and "
            "POST /_upload raises UploadValueError -> HTTP 500 before the "
            "handler runs. A bare `list` is not enough — it is not a generic "
            "alias. See #452."
        )

    @pytest.mark.parametrize(
        ("module_name", "state_name", "handler_name", "lineno"),
        UPLOAD_SITES,
        ids=[f"{s[1]}.{s[2]}" for s in UPLOAD_SITES],
    )
    def test_handler_is_not_a_background_task(self, module_name, state_name, handler_name, lineno):
        """The same endpoint rejects these too, with the same 500."""
        module = importlib.import_module(module_name)
        handler = getattr(getattr(module, state_name), handler_name)

        if isinstance(handler, EventHandler):
            assert not handler.is_background, (
                f"{state_name}.{handler_name} is @rx.event(background=True); "
                "reflex/app.py::upload() raises UploadTypeError for background "
                f"upload handlers ({module_name}:{lineno})."
            )
