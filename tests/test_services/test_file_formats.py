"""Every extension our own guides tell a user to select must be selectable (core#1604).

Three places in this codebase carried their own copy of "which file extensions do we
support", and on 2026-09-26 all three disagreed:

===================================================  =========================================
copy                                                 held
===================================================  =========================================
the New Connection picker's ``accept`` filter        ``.csv``, ``.json``, ``.parquet``
``FileUploadService.ALLOWED_EXTENSIONS`` (refuses)   ``csv``, ``json``, ``parquet``
``dlt_runner.FILE_FORMAT_BY_EXTENSION`` (reads)      the eight below, minus ``.parq``
===================================================  =========================================

🔑 **The issue reported three missing extensions; a structural read finds FIVE.** [core#1604]
was filed from a walk of the ``json`` and ``parquet`` guides and named ``.jsonl``, ``.ndjson``
and ``.parq``. It could not name ``.tsv`` and ``.txt``, which ``csv.md:24`` documents in the
same sentence shape and which are missing from the same two places — because that guide was not
on the walk. **A population taken from a walk expires at the edge of the walk**; the remedy is a
declaration every consumer derives from, which is what this module guards.

⚠️ **And the issue's scope was one layer too shallow, in the direction that matters.** It says
*"this issue is purely the picker's filter"* and *"the loader is not the problem"*. True of the
*reader* — ``read_json`` handles JSON Lines. False of ``FileUploadService``, which is a second
hard-coded list and the one that **rejects**: widening ``accept`` alone would have shipped a
picker offering a file the server then refuses with ``Unsupported file type '.jsonl'``. Counting
the picker alone counts one of three copies.

What the oracles are, and why each is independent of the thing it measures
-------------------------------------------------------------------------

``DOCUMENTED_BY_GUIDE`` below is a **literal**, dated, with the guide line that states each
extension. It is deliberately not derived from :mod:`datanika.services.file_formats`: an
expected value computed by the code under test is satisfied by that code doing nothing
(``WORKFLOW_RULES`` §5b rule 3, and §4's third-door case). The equality is asserted **both
ways**, so adding an extension to the code without a guide naming it is red too.

* the picker is read from the **generated ``useDropzone`` hook**, i.e. the JavaScript Reflex
  will emit — not from the source literal, and not from the component's ``accept`` attribute,
  which is ``None`` on the built tree because ``Upload.create`` folds the prop into a hook
  string. A test that read the attribute would pass on an upload that filters nothing.
* the upload allowlist is read by **calling ``save_file``** with a real session.
* the reader is read by **calling ``_resolve_file_format``**, the function the run path uses.

Each of those has a paired control with an extension we deliberately do **not** support, so the
suite can fail in the too-wide direction as well as the too-narrow one.

Release condition
-----------------

A red here means **the guides and the code disagree**; it does not mean the list is stale.
Decide which side is wrong and change that side. Do not delete the list, and do not widen it to
make a red go away without finding the guide sentence that justifies the extension
(``WORKFLOW_RULES`` §5a).
"""

import ast
import pathlib

import pytest

import datanika.services.file_upload_service as fus_mod
import datanika.ui.components.connection_config_fields as ccf_mod
from datanika.models.user import Organization
from datanika.services.dlt_runner import _resolve_file_format
from datanika.services.file_upload_service import FileUploadService

#: What the connector guides in `datanika-landing` tell a reader to select, read from
#: `origin/main` on **2026-09-26**. The citation is the point: a number or a list in a test is a
#: measurement, and one with no instrument beside it cannot be re-derived (`WORKFLOW_RULES` §5b
#: rule 5).
#:
#: * ``src/content/connectors/csv.md:24``      — "Common extensions: `.csv`, `.tsv`, `.txt`."
#: * ``src/content/connectors/json.md:26``     — "Extension usually `.jsonl` or `.ndjson`."
#: * ``src/content/connectors/json.md:34``     — "drag your `.json` / `.jsonl` / `.ndjson` file"
#: * ``src/content/connectors/parquet.md:24``  — "`.parquet` (standard), `.parq` (rare)."
DOCUMENTED_BY_GUIDE: dict[str, tuple[str, ...]] = {
    "csv": (".csv", ".tsv", ".txt"),
    "json": (".json", ".jsonl", ".ndjson"),
    "parquet": (".parquet", ".parq"),
}

#: Declared by the **reader** and named in no guide. ``.pq`` has been in
#: ``FILE_FORMAT_BY_EXTENSION`` since before core#1604, so an ``s3`` glob of ``*.pq`` resolves
#: today; dropping it to make the guide comparison tidy would be a regression for a shape that
#: already works. Listed separately rather than folded in, so the two reasons stay legible.
READER_DECLARED_ONLY: frozenset[str] = frozenset({".pq"})

#: Not supported, and the control for every "every documented extension works" assertion below.
#: Without these, a declaration that simply said "yes" to everything would pass the whole file.
UNSUPPORTED_CONTROL: tuple[str, ...] = (".exe", ".sql", ".yml", ".gz")


def _all_documented() -> frozenset[str]:
    return frozenset(e for exts in DOCUMENTED_BY_GUIDE.values() for e in exts)


def _expected_declaration() -> frozenset[str]:
    return _all_documented() | READER_DECLARED_ONLY


# --------------------------------------------------------------------------------------------
# 1. The declaration itself
# --------------------------------------------------------------------------------------------


class TestTheDeclarationAgreesWithTheGuides:
    def test_the_declaration_names_every_documented_extension(self):
        from datanika.services.file_formats import FILE_EXTENSIONS_BY_TYPE

        declared = frozenset(e for exts in FILE_EXTENSIONS_BY_TYPE.values() for e in exts)
        missing = _all_documented() - declared
        assert not missing, (
            f"{sorted(missing)} are documented in a connector guide and are in no "
            "FILE_EXTENSIONS_BY_TYPE entry, so the picker will not offer them and the upload "
            "service will refuse them. Add them, or correct the guide."
        )

    def test_the_declaration_names_nothing_a_guide_does_not(self):
        """The other direction, and it is the one that catches a widening nobody documented."""
        from datanika.services.file_formats import FILE_EXTENSIONS_BY_TYPE

        declared = frozenset(e for exts in FILE_EXTENSIONS_BY_TYPE.values() for e in exts)
        extra = declared - _expected_declaration()
        assert not extra, (
            f"{sorted(extra)} are declared supported and no connector guide names them. "
            "Either cite the guide line here, or add it to READER_DECLARED_ONLY with the "
            "reason it is reader-only — an undocumented extension in the picker is a promise "
            "nothing backs."
        )

    def test_each_declared_extension_belongs_to_exactly_one_type(self):
        """Two types claiming one extension makes the reader lookup order-dependent."""
        from datanika.services.file_formats import FILE_EXTENSIONS_BY_TYPE

        seen: dict[str, str] = {}
        for conn_type, exts in FILE_EXTENSIONS_BY_TYPE.items():
            for ext in exts:
                assert ext not in seen, (
                    f"{ext} is claimed by both {seen.get(ext)} and {conn_type}; "
                    "FILE_FORMAT_BY_EXTENSION is a flat dict and one of them would win silently"
                )
                seen[ext] = conn_type

    def test_every_extension_starts_with_a_dot(self):
        """``_resolve_file_format`` looks up ``PurePosixPath(glob).suffix``, which carries one."""
        from datanika.services.file_formats import FILE_EXTENSIONS_BY_TYPE

        bad = [
            e for exts in FILE_EXTENSIONS_BY_TYPE.values() for e in exts if not e.startswith(".")
        ]
        assert not bad, f"{bad} would never match a PurePosixPath suffix"


# --------------------------------------------------------------------------------------------
# 2. The picker — read from the generated hook, not from the source
# --------------------------------------------------------------------------------------------


def _dropzone_hooks(component) -> list[str]:
    """Every ``useDropzone(...)`` hook the built tree will emit.

    ``Upload.create`` moves ``accept`` into a hook string via ``VarData``, so it is reachable
    neither as a component attribute (``None``) nor in ``render()`` (absent). Walking
    ``special_props`` as well as ``_get_all_hooks`` is what finds it.
    """
    out: list[str] = []
    stack = [component]
    while stack:
        node = stack.pop()
        try:
            hooks = node._get_all_hooks()
        except Exception:  # noqa: BLE001 - a node without hooks is not an error here
            hooks = {}
        out.extend(h for h in hooks if "useDropzone" in h)
        for special in getattr(node, "special_props", None) or []:
            var_data = special._get_all_var_data()
            if var_data is not None and getattr(var_data, "hooks", None):
                out.extend(h for h in var_data.hooks if "useDropzone" in h)
        stack.extend(getattr(node, "children", None) or [])
    return out


@pytest.fixture
def file_picker_hook() -> str:
    hooks = _dropzone_hooks(ccf_mod.file_upload_fields())
    assert hooks, (
        "no useDropzone hook was found on the built file-upload fields, so this test read "
        "NOTHING and its verdict is void rather than clean. Reflex folds `accept` into that "
        "hook; if the shape changed, fix _dropzone_hooks before believing any result below."
    )
    return hooks[0]


class TestThePickerOffersEveryDocumentedExtension:
    def test_the_hook_carries_an_accept_filter_at_all(self, file_picker_hook):
        """Anti-vacuity: with no ``accept`` key every assertion below would be about nothing."""
        assert '["accept"]' in file_picker_hook, (
            "the generated useDropzone call has no accept filter, so the extension assertions "
            f"below cannot discriminate. hook={file_picker_hook[:400]}"
        )

    @pytest.mark.parametrize("ext", sorted(_all_documented()))
    def test_the_picker_offers_a_documented_extension(self, ext, file_picker_hook):
        assert f'"{ext}"' in file_picker_hook, (
            f"{ext} is documented in a connector guide and is not in the picker's accept "
            "filter, so a browse dialog does not list the user's file — no error, nothing to "
            "search for. This is core#1604."
        )

    @pytest.mark.parametrize("ext", UNSUPPORTED_CONTROL)
    def test_the_picker_does_not_offer_an_unsupported_extension(self, ext, file_picker_hook):
        """The control. An ``accept`` of everything would pass the parametrisation above."""
        assert f'"{ext}"' not in file_picker_hook, (
            f"{ext} is offered by the picker and nothing can read it"
        )


# --------------------------------------------------------------------------------------------
# 3. Nobody keeps a second copy — the assertion core#1604's AC2 asks for
# --------------------------------------------------------------------------------------------

_DECLARATION_MODULE = "datanika.services.file_formats"


def _names_imported_from_declaration(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == _DECLARATION_MODULE:
            names.update(alias.asname or alias.name for alias in node.names)
        elif isinstance(node, ast.Import):
            names.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == _DECLARATION_MODULE
            )
    return names


def _references(expr: ast.expr, names: set[str]) -> bool:
    """Does *expr* mention any of *names*? Positive form, per §5b rule 3.

    Asserting "is not a dict literal" would be satisfied by deleting the keyword entirely, and
    by a comprehension over a second literal list. Asserting that the shared symbol is
    *referenced* is the claim we actually want.
    """
    for node in ast.walk(expr):
        if isinstance(node, ast.Name) and node.id in names:
            return True
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and (f"{node.value.id}.{node.attr}" in names or node.value.id in names)
        ):
            return True
    return False


def _picker_accept_expr(tree: ast.Module) -> ast.expr:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
        ident = kwargs.get("id")
        if isinstance(ident, ast.Constant) and ident.value == "file_upload":
            accept = kwargs.get("accept")
            assert accept is not None, "the file_upload picker passes no accept filter at all"
            return accept
    raise AssertionError(
        "no call with id='file_upload' found in connection_config_fields.py — this checker "
        "read nothing, so treat its verdict as void"
    )


def _allowed_extensions_expr(tree: ast.Module) -> ast.expr:
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "ALLOWED_EXTENSIONS":
                    return node.value
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "ALLOWED_EXTENSIONS"
        ):
            assert node.value is not None
            return node.value
    raise AssertionError("ALLOWED_EXTENSIONS is not assigned at module level any more")


class TestNoConsumerKeepsItsOwnExtensionList:
    """AC2: the lists must be *derived*. Two literals drift, and core#1604 is what that looks like.

    Both halves in one test, per ``WORKFLOW_RULES`` §4: the shipped shape must pass **and** the
    pre-fix shape must still fail. One without the other is how a control comes to pass by
    gutting its own guard.
    """

    def test_the_picker_derives_its_accept_from_the_declaration(self):
        tree = ast.parse(pathlib.Path(ccf_mod.__file__).read_text(encoding="utf-8"))
        names = _names_imported_from_declaration(tree)
        assert names, (
            f"connection_config_fields.py imports nothing from {_DECLARATION_MODULE}, so its "
            "accept filter cannot be derived from the shared declaration"
        )
        assert _references(_picker_accept_expr(tree), names), (
            "the picker's accept filter does not reference anything from "
            f"{_DECLARATION_MODULE} — it is a second copy of the extension list, which is the "
            "defect core#1604 reported rather than a fix for it"
        )

    def test_the_upload_service_derives_its_allowlist_from_the_declaration(self):
        tree = ast.parse(pathlib.Path(fus_mod.__file__).read_text(encoding="utf-8"))
        names = _names_imported_from_declaration(tree)
        assert names, (
            f"file_upload_service.py imports nothing from {_DECLARATION_MODULE}; its "
            "ALLOWED_EXTENSIONS is the copy that actually *refuses* a user's file"
        )
        assert _references(_allowed_extensions_expr(tree), names), (
            "ALLOWED_EXTENSIONS does not reference the shared declaration"
        )

    def test_the_checker_refuses_the_pre_fix_shape(self):
        """The negative control. Without it, the two tests above could pass on a broken checker."""
        pre_fix = ast.parse(
            "import reflex as rx\n"
            "def f():\n"
            "    return rx.upload(id='file_upload', accept={'text/csv': ['.csv']})\n"
        )
        assert _names_imported_from_declaration(pre_fix) == set(), (
            "the import scanner found a declaration import in a snippet that has none"
        )
        assert not _references(_picker_accept_expr(pre_fix), {"upload_accept_map"}), (
            "the reference check accepted a hard-coded dict literal, so it cannot detect "
            "core#1604 at all"
        )

    def test_the_checker_accepts_a_derived_shape(self):
        """The positive control: the checker must be able to say yes, not only no."""
        derived = ast.parse(
            "from datanika.services.file_formats import upload_accept_map\n"
            "import reflex as rx\n"
            "def f():\n"
            "    return rx.upload(id='file_upload', accept=upload_accept_map())\n"
        )
        names = _names_imported_from_declaration(derived)
        assert names == {"upload_accept_map"}
        assert _references(_picker_accept_expr(derived), names)


# --------------------------------------------------------------------------------------------
# 4. The upload service — the copy that refuses
# --------------------------------------------------------------------------------------------


@pytest.fixture
def upload_svc(tmp_path):
    return FileUploadService(str(tmp_path / "uploads"))


@pytest.fixture
def upload_org(db_session):
    org = Organization(name="ExtOrg", slug="ext-org-1604")
    db_session.add(org)
    db_session.flush()
    return org


class TestTheUploadServiceAcceptsEveryDocumentedExtension:
    @pytest.mark.parametrize("ext", sorted(_all_documented()))
    def test_save_file_accepts_a_documented_extension(
        self, ext, upload_svc, db_session, upload_org
    ):
        record = upload_svc.save_file(db_session, upload_org.id, f"data{ext}", b"a,b\n1,2")
        assert record.original_name == f"data{ext}"

    @pytest.mark.parametrize("ext", UNSUPPORTED_CONTROL)
    def test_save_file_still_refuses_an_unsupported_extension(self, ext, upload_svc, db_session):
        with pytest.raises(ValueError, match="Unsupported file type"):
            upload_svc.save_file(db_session, 1, f"data{ext}", b"x")


# --------------------------------------------------------------------------------------------
# 5. The reader — via the function the run path calls
# --------------------------------------------------------------------------------------------


class TestTheReaderResolvesEveryDocumentedExtension:
    @pytest.mark.parametrize("ext", sorted(_all_documented() | READER_DECLARED_ONLY))
    def test_resolve_file_format_names_a_reader(self, ext):
        """Through ``_resolve_file_format``, not through the dict it reads.

        ``s3`` is the connection type on purpose: for ``csv``/``json``/``parquet`` the format
        comes from ``FILE_FORMAT_BY_TYPE`` and the extension is never consulted, so a test that
        used one of those types would pass with the extension map empty.
        """
        assert _resolve_file_format("s3", f"*{ext}", {}) in {"csv", "json", "parquet"}

    @pytest.mark.parametrize("ext", UNSUPPORTED_CONTROL)
    def test_resolve_file_format_still_refuses_an_unsupported_extension(self, ext):
        from datanika.services.dlt_runner import DltRunnerError

        with pytest.raises(DltRunnerError):
            _resolve_file_format("s3", f"*{ext}", {})
