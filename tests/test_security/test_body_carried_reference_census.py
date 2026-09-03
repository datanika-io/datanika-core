"""core#719 item 2 — every resource reference arriving in a request BODY is accounted for.

Item 1 (``test_boundary_route_drift.py``) closed the *arithmetic*: every mutating
route that takes a resource id **in the path** is pinned by ``MUTATION_ROUTES``.
Item 1's own docstring says it does not close the *shape*, and this is that shape:
an id that arrives in the body sits outside the boundary table by construction,
so lengthening that table can never reach it.

## What this guard is NOT, and why that matters

``test_tenant_fk_boundary.py`` already carries two class-level AST guards —
*no bare ``session.get(Connection, ...)``* and *every tenant-model query
constrains ``org_id``*. Those say: **wherever you resolve a tenant model, you
scope it.** They do not say the body id is resolved **at all**. A handler that
takes ``destination_connection_id`` from the body and stores it without ever
looking it up performs no query, trips neither guard, and writes a cross-org
foreign key. That is core#733's class arriving by a route its own guard cannot
see.

So this file asserts *accounting*, not scoping: every body-carried reference is
pinned, and every pinned reference names a test that proves a foreign id is
refused **and** a control proving the org's own id still works.

## 🚨 The four ways a census fails, and what each one cost here

A census fails at the **predicate** (what counts as a reference), the **matcher**
(how a finding is compared to the table), the **remediation** (what the failure
tells you to do) — and a fourth this project earned the hard way: **an uncovered
path contributes zero reds however broken it is.** All four are guarded below,
and the fourth was not theoretical:

* The first version of this census walked only functions a ``Route`` names
  directly. ``POST /api/v1/import`` reads its body in the handler and hands it to
  ``_validate_import_payload``, so **the entire bulk-import payload was outside
  the walk** — a mutation adding ``u.get("source_connection_id")`` inside that
  helper changed nothing. Fixed by following same-module delegation.
* The second version walked a single expression spine to find loop variables.
  The real code is ``for i, u in enumerate(data.get("uploads", []))``, and the
  spine stops at ``enumerate``. **Still blind, for a second reason, on the same
  path.** Fixed by looking for a body variable anywhere in the iterator.

Both were found by mutating the real ``api_v1_routes.py``, never by a synthetic
fixture — a synthetic control is written from the same mental model as the check
and agrees with it including where the check is wrong.

A delegation the walk cannot follow (a call into another module) is **reported**,
not dropped: ``UNWALKED`` below. Silence about a path is the failure mode; naming
it is the cheapest available fix.
"""

from __future__ import annotations

import ast
import pathlib
import re
from dataclasses import dataclass

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
APP = REPO / "datanika" / "datanika.py"
SERVICES = REPO / "datanika" / "services"
TESTS = REPO / "tests"

MUTATING = {"POST", "PUT", "PATCH", "DELETE"}

# A key read off a request body that names a resource reference: `id`, `ids`,
# `target_id`, `destination_connection_id`. Deliberately anchored so `uuid` and
# `valid` do not match — a census that flags every word ending in "id" is one
# nobody reads.
REF_KEY = re.compile(r"(^|_)ids?$")

# Callables that hand you the parsed request body.
BODY_READERS = {"_body", "_body_sync", "_body_params", "_raw_body_sync", "json", "form"}

ORG_SCOPE = re.compile(r"\borg_id\b")

# Calls that cannot resolve a resource, so passing a reference to one is not a sink.
INERT_SINKS = {"int", "str", "len", "bool", "_error", "_typed_error", "all", "any", "get"}


@dataclass(frozen=True)
class Ref:
    """A pinned body-carried reference.

    ``refuses`` and ``accepts`` are both required, and that is the point. A
    refusal test alone is satisfied by a handler that refuses *everything* — and
    by a request rejected for an unrelated reason before the check is reached,
    which is exactly what ``test_upload_create_rejects_org_b_connection`` did
    until this commit: its payload name contained hyphens, ``validate_upload_name``
    returned 400, and the assertion ``status_code != 201`` was satisfied whatever
    the connection lookup did. Measured — the identical request with the org's
    *own* connections was refused too. The acceptance control is what makes the
    refusal attributable.

    Both are written ``<path under tests/>::<test name>``. A bare name would let
    any same-named test anywhere in a 4,000-test tree satisfy the pin, and would
    make the failure message say *"no such test"* without saying where to put one.
    """

    model: str
    refuses: str
    accepts: str


# (module, handler, body key) -> how it is proved safe.
#
# Every entry is checked three ways: the census must still find it, the named
# tests must still exist, and the reference must still flow into a call carrying
# an org scope. An entry cannot outlive its reason.
_FK = "test_security/test_tenant_fk_boundary.py"
_OAUTH = "test_services/test_mcp_oauth_routes.py"

BODY_REFERENCES: dict[tuple[str, str, str], Ref] = {
    ("api_v1_routes", "create_pipeline", "destination_connection_id"): Ref(
        model="Connection",
        refuses=f"{_FK}::test_pipeline_create_rejects_org_b_connection",
        accepts=f"{_FK}::test_pipeline_create_accepts_own_connection",
    ),
    ("api_v1_routes", "update_pipeline", "destination_connection_id"): Ref(
        model="Connection",
        refuses=f"{_FK}::test_pipeline_update_rejects_org_b_connection",
        accepts=f"{_FK}::test_pipeline_update_accepts_own_connection",
    ),
    ("api_v1_routes", "create_transformation", "destination_connection_id"): Ref(
        model="Connection",
        refuses=f"{_FK}::test_transformation_create_rejects_org_b_connection",
        accepts=f"{_FK}::test_transformation_create_accepts_own_connection",
    ),
    ("api_v1_routes", "update_transformation", "destination_connection_id"): Ref(
        model="Connection",
        refuses=f"{_FK}::test_transformation_update_rejects_org_b_connection",
        accepts=f"{_FK}::test_transformation_update_accepts_own_connection",
    ),
    ("api_v1_routes", "create_upload", "source_connection_id"): Ref(
        model="Connection",
        refuses=f"{_FK}::test_upload_create_rejects_org_b_source_connection",
        accepts=f"{_FK}::test_upload_create_accepts_own_connections",
    ),
    ("api_v1_routes", "create_upload", "destination_connection_id"): Ref(
        model="Connection",
        refuses=f"{_FK}::test_upload_create_rejects_org_b_destination_connection",
        accepts=f"{_FK}::test_upload_create_accepts_own_connections",
    ),
    ("api_v1_routes", "create_schedule", "target_id"): Ref(
        model="Upload|Transformation|Pipeline",
        refuses=f"{_FK}::test_schedule_create_rejects_org_b_target",
        accepts=f"{_FK}::test_schedule_create_accepts_own_target",
    ),
    ("mcp_oauth_routes", "consent", "client_id"): Ref(
        # Not an org-owned resource: an OAuth client identifier. The org on the
        # grant comes from the signed access token, never from the body — the
        # handler says so in a comment and `grant_consent` takes `org_id` from
        # `claims`. Pinned rather than excluded so the day it stops being true
        # is a red here.
        model="OAuthClient (not org-owned)",
        refuses=f"{_OAUTH}::test_consent_takes_the_org_from_the_token_not_the_body",
        accepts=f"{_OAUTH}::test_consent_takes_the_org_from_the_token_not_the_body",
    ),
}

# Calls that hand body data into a module this walk does not open. Each entry is
# a declared, reviewed gap — not a silence.
UNWALKED_DELEGATIONS = {
    # POST /api/v1/pipelines/yaml -> datanika/services/_yaml_import.py. Parses
    # YAML into the same `version: 2` payload `bulk_import` validates, then
    # rejoins the walked path. References there are by connection *name*,
    # resolved against an org-scoped list, so no id crosses this boundary.
    ("api_v1_routes", "bulk_import_yaml", "parse_yaml_import(...)"),
}


# ---------------------------------------------------------------------------
# The census
# ---------------------------------------------------------------------------


def mounted_modules() -> list[str]:
    """Route modules ``datanika.py`` appends to ``app._api.routes``.

    **Derived from the source, never listed.** A module mounted tomorrow joins
    the census with no edit here — the alternative is a hardcoded list that
    silently stops covering the surface, which is the defect one level up.
    """
    tree = ast.parse(APP.read_text(encoding="utf-8"))
    imported: dict[str, str] = {}
    appended: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
            "datanika.services."
        ):
            for alias in node.names:
                imported[alias.asname or alias.name] = node.module.split(".")[-1]
        if isinstance(node, ast.For) and isinstance(node.iter, ast.Name):
            body = ast.dump(ast.Module(body=node.body, type_ignores=[]))
            if "_api" in body and "routes" in body and "append" in body:
                appended.add(node.iter.id)
    return sorted({imported[n] for n in appended if n in imported})


def _mutating_routes(tree: ast.AST) -> dict[str, set[tuple[str, str]]]:
    out: dict[str, set[tuple[str, str]]] = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and getattr(node.func, "id", None) == "Route"):
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        handler = (
            node.args[1].id if len(node.args) > 1 and isinstance(node.args[1], ast.Name) else None
        )
        methods = ["GET"]
        for kw in node.keywords:
            if kw.arg == "methods" and isinstance(kw.value, (ast.List, ast.Tuple)):
                methods = [e.value for e in kw.value.elts if isinstance(e, ast.Constant)]
        if handler:
            for m in methods:
                if m in MUTATING:
                    out.setdefault(handler, set()).add((m, node.args[0].value))
    return out


def _is_body_call(node: ast.AST) -> bool:
    call = node.value if isinstance(node, ast.Await) else node
    if not isinstance(call, ast.Call):
        return False
    name = getattr(call.func, "id", None) or getattr(call.func, "attr", None)
    return name in BODY_READERS


def _propagate(fn: ast.AST, seed: set[str]) -> set[str]:
    """Grow ``seed`` over names derived from a body variable.

    ``mentions`` looks for a body variable *anywhere* in the iterator rather
    than walking a spine, because the real code wraps it:
    ``enumerate(data.get("uploads", []))``. A spine walk stops at ``enumerate``
    and the whole bulk-import payload disappears from the census. Measured.
    """

    def mentions(expr: ast.AST, names: set[str]) -> bool:
        return any(isinstance(n, ast.Name) and n.id in names for n in ast.walk(expr))

    found = set(seed)
    for _ in range(4):  # fixpoint; nesting here is shallow
        before = len(found)
        for node in ast.walk(fn):
            targets = None
            if isinstance(node, (ast.For, ast.AsyncFor, ast.comprehension)) and mentions(
                node.iter, found
            ):
                targets = node.target
            elif isinstance(node, ast.Assign):
                base = node.value
                while isinstance(base, (ast.Subscript, ast.Attribute)):
                    base = base.value
                if isinstance(base, ast.Name) and base.id in found:
                    for t in node.targets:
                        if isinstance(t, ast.Name):
                            found.add(t.id)
            if targets is not None:
                for e in targets.elts if isinstance(targets, ast.Tuple) else [targets]:
                    if isinstance(e, ast.Name):
                        found.add(e.id)
        if len(found) == before:
            break
    return found


def _body_vars(fn: ast.AST) -> set[str]:
    seed: set[str] = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Assign) and _is_body_call(node.value):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    seed.add(t.id)
    return _propagate(fn, seed)


def _ref_reads(fn: ast.AST, bodyvars: set[str]) -> set[str]:
    """Reference-shaped keys read off a body variable, both spellings.

    ``data["x_id"]`` and ``data.get("x_id")`` both count. Matching only one is
    the *predicate* failure: ``create_pipeline`` reads its key with ``.get`` in
    the validation branch and with ``[...]`` in the call, and a census that saw
    only one spelling would still report the key here while missing a handler
    that used the other exclusively.
    """
    keys: set[str] = set()
    for node in ast.walk(fn):
        key = None
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.value, ast.Name)
            and node.value.id in bodyvars
            and isinstance(node.slice, ast.Constant)
            and isinstance(node.slice.value, str)
        ):
            key = node.slice.value
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in bodyvars
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            key = node.args[0].value
        if key and REF_KEY.search(key):
            keys.add(key)
    return keys


def _unscoped_sinks(fn: ast.AST, bodyvars: set[str], keys: set[str]) -> list[str]:
    bad: list[str] = []
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call):
            continue
        src = ast.unparse(node)
        if not any(f'"{k}"' in src or f"'{k}'" in src for k in keys):
            continue
        if not any(v in src for v in bodyvars):
            continue
        fname = getattr(node.func, "id", "") or getattr(node.func, "attr", "")
        if fname in INERT_SINKS:
            continue
        if not ORG_SCOPE.search(src):
            bad.append(f"{fname}(...)")
    return bad


def _delegations(fn: ast.AST, bodyvars: set[str], fns: dict[str, ast.AST]):
    resolvable: list[tuple[str, set[str]]] = []
    unresolvable: list[str] = []
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call):
            continue
        positional = [
            i for i, a in enumerate(node.args) if isinstance(a, ast.Name) and a.id in bodyvars
        ]
        keyword = [
            kw.arg
            for kw in node.keywords
            if isinstance(kw.value, ast.Name) and kw.value.id in bodyvars
        ]
        if not positional and not keyword:
            continue
        name = getattr(node.func, "id", None)
        if name in {"len", "dict", "list", "sorted", "str", "int", "bool", "JSONResponse"}:
            continue
        callee = fns.get(name) if name else None
        if callee is None:
            unresolvable.append(f"{name or ast.unparse(node.func)}(...)")
            continue
        params = [a.arg for a in callee.args.args]
        receiving = {params[i] for i in positional if i < len(params)} | {k for k in keyword if k}
        resolvable.append((name, receiving))
    return resolvable, unresolvable


def census(module_names: list[str] | None = None):
    """-> (references, sinks, unwalked, mutating_route_count)."""
    refs: dict[tuple[str, str, str], set[tuple[str, str]]] = {}
    sinks: list[str] = []
    unwalked: set[tuple[str, str, str]] = set()
    route_count = 0

    for mod in module_names if module_names is not None else mounted_modules():
        path = SERVICES / f"{mod}.py"
        if not path.exists():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        routes = _mutating_routes(tree)
        route_count += sum(len(v) for v in routes.values())
        fns = {
            n.name: n
            for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for handler, sigs in routes.items():
            fn = fns.get(handler)
            if fn is None:
                continue
            seen: set[tuple[str, tuple[str, ...]]] = set()
            queue = [(handler, fn, _body_vars(fn))]
            found: set[str] = set()
            while queue:
                fname, node, bvars = queue.pop()
                mark = (fname, tuple(sorted(bvars)))
                if not bvars or mark in seen:
                    continue
                seen.add(mark)
                found |= _ref_reads(node, bvars)
                sinks += [
                    f"{mod}.{handler} (in {fname}): {s}"
                    for s in _unscoped_sinks(node, bvars, found)
                ]
                deleg, unres = _delegations(node, bvars, fns)
                for u in unres:
                    unwalked.add((mod, handler, u))
                for callee, receiving in deleg:
                    queue.append((callee, fns[callee], _propagate(fns[callee], receiving)))
            for key in found:
                refs.setdefault((mod, handler, key), set()).update(sigs)
    return refs, sinks, unwalked, route_count


def _missing_pinned_tests() -> list[str]:
    """Pinned ``path::name`` references that do not exist on disk.

    Only the files the pins actually name are opened — so a same-named test
    somewhere else in a 4,000-test tree cannot satisfy a pin by accident, and
    the failure message can say which file is short of a test.
    """
    wanted: dict[str, set[str]] = {}
    for ref in BODY_REFERENCES.values():
        for name in (ref.refuses, ref.accepts):
            rel, _, fn = name.partition("::")
            wanted.setdefault(rel, set()).add(fn)

    missing: list[str] = []
    for rel, fns in sorted(wanted.items()):
        path = TESTS / rel
        if not path.exists():
            missing += [f"{rel}::{fn}  (file does not exist)" for fn in sorted(fns)]
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        present = {
            n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        missing += [f"{rel}::{fn}" for fn in sorted(fns - present)]
    return missing


# ---------------------------------------------------------------------------
# Armed-ness. Every assertion below is about a set; if the set is empty they all
# pass and say nothing.
# ---------------------------------------------------------------------------


class TestTheCensusIsArmed:
    def test_it_finds_the_route_modules_the_app_mounts(self) -> None:
        mods = mounted_modules()
        assert len(mods) >= 8, f"only {len(mods)} mounted route modules found: {mods}"
        for expected in ("api_v1_routes", "mcp_oauth_routes", "sso_routes"):
            assert expected in mods, (
                f"{expected} is mounted in datanika.py but the derivation missed it — "
                "an unwalked module contributes zero findings however broken it is"
            )

    def test_it_finds_the_mutating_routes(self) -> None:
        *_, count = census()
        assert count >= 35, (
            f"only {count} mutating routes across every mounted module — the scan is "
            "broken or the app was restructured. Do NOT read this as 'nothing to cover'."
        )

    def test_it_finds_known_present_references(self) -> None:
        refs, *_ = census()
        for known in (
            ("api_v1_routes", "create_upload", "source_connection_id"),
            ("api_v1_routes", "create_schedule", "target_id"),
        ):
            assert known in refs, (
                f"{known} is a body-carried reference in the shipped source and the census "
                "did not see it — the predicate or the walk is broken"
            )


# ---------------------------------------------------------------------------
# The accounting
# ---------------------------------------------------------------------------


class TestEveryBodyCarriedReferenceIsAccountedFor:
    def test_no_reference_is_unpinned(self) -> None:
        refs, *_ = census()
        missing = sorted(set(refs) - set(BODY_REFERENCES))
        assert not missing, (
            f"{len(missing)} resource reference(s) arrive in a request body and are not "
            "pinned, so nothing checks that a foreign id is refused:\n"
            + "\n".join(
                f"  {m}.{h}  body key {k!r}  ({sorted(refs[(m, h, k)])})" for m, h, k in missing
            )
            + "\n\nAdd each to BODY_REFERENCES in this file, naming a test that proves a "
            "cross-org id is refused AND a control proving the org's own id still works "
            "(core#719 item 2). Neither alone is evidence."
        )

    def test_no_pinned_row_outlives_its_reference(self) -> None:
        refs, *_ = census()
        stale = sorted(set(BODY_REFERENCES) - set(refs))
        assert not stale, (
            f"{len(stale)} BODY_REFERENCES row(s) match nothing in the source:\n"
            + "\n".join(f"  {m}.{h}  body key {k!r}" for m, h, k in stale)
            + "\n\nThe handler or the key was renamed or removed — delete the row, or fix "
            "the census if the reference is still there under another spelling."
        )

    def test_every_pinned_reference_names_tests_that_exist(self) -> None:
        """A pin whose evidence does not exist is a pin that proves nothing."""
        missing = _missing_pinned_tests()
        assert not missing, (
            f"{len(missing)} test(s) named by BODY_REFERENCES do not exist:\n"
            + "\n".join(f"  {m}" for m in missing)
            + "\n\nEvery pinned reference needs both a refusal test (a cross-org id is "
            "rejected) and an acceptance control (the org's own id still works). Write "
            "the missing one, or correct the pin."
        )

    def test_the_pin_check_can_fail(self) -> None:
        """Otherwise the assertion above is a lookup that always succeeds."""
        rel, _, _fn = next(iter(BODY_REFERENCES.values())).refuses.partition("::")
        tree = ast.parse((TESTS / rel).read_text(encoding="utf-8"))
        present = {
            n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        assert "test_a_name_no_test_will_ever_have" not in present, (
            "the name lookup matches a test that does not exist, so it can only be green"
        )

    def test_no_reference_reaches_a_call_without_an_org_scope(self) -> None:
        _refs, sinks, *_ = census()
        assert not sinks, (
            f"{len(sinks)} call(s) receive a body-carried resource reference with no "
            "org_id in the same call:\n"
            + "\n".join(f"  {s}" for s in sinks)
            + "\n\nA reference resolved without the caller's org can return another "
            "tenant's row. Pass the org through, or resolve via an org-scoped accessor."
        )

    def test_every_cross_module_delegation_is_declared(self) -> None:
        """A hand-off the walk cannot follow must be a decision, not a silence."""
        *_, unwalked, _count = census()
        undeclared = sorted(unwalked - UNWALKED_DELEGATIONS)
        assert not undeclared, (
            f"{len(undeclared)} handler(s) hand request-body data to a call this census "
            "cannot follow:\n"
            + "\n".join(f"  {m}.{h} -> {c}" for m, h, c in undeclared)
            + "\n\nThe body may carry resource references into code nothing here reads. "
            "Review the callee and add it to UNWALKED_DELEGATIONS with the reason."
        )

    def test_no_declared_delegation_outlives_its_call(self) -> None:
        *_, unwalked, _count = census()
        dead = sorted(UNWALKED_DELEGATIONS - unwalked)
        assert not dead, (
            f"{len(dead)} UNWALKED_DELEGATIONS entr(ies) match no real call:\n"
            + "\n".join(f"  {m}.{h} -> {c}" for m, h, c in dead)
        )


# ---------------------------------------------------------------------------
# Negative controls — one per way a census fails
# ---------------------------------------------------------------------------


class TestTheCensusCanFail:
    """A check that has never failed has never been shown able to.

    These are in-process forms of mutations run against the real
    ``api_v1_routes.py`` (see plans/qa/notes/mutate_719_item2.py). Keeping them
    here means the proof survives; a one-off manual mutation is repeated by
    nobody.
    """

    def _fn(self, src: str) -> ast.AST:
        return ast.parse(src).body[0]

    def test_predicate_sees_both_key_spellings(self) -> None:
        for src in (
            'def h(request):\n data = _body_sync(request)\n x = data["target_id"]\n',
            'def h(request):\n data = _body_sync(request)\n x = data.get("target_id")\n',
        ):
            fn = self._fn(src)
            assert _ref_reads(fn, _body_vars(fn)) == {"target_id"}, f"missed in: {src!r}"

    def test_predicate_ignores_words_that_merely_end_in_id(self) -> None:
        """A census that flags `uuid` and `valid` is one nobody reads."""
        fn = self._fn(
            "def h(request):\n data = _body_sync(request)\n"
            ' a = data.get("uuid")\n b = data.get("is_valid")\n c = data.get("timezone")\n'
        )
        assert _ref_reads(fn, _body_vars(fn)) == set()

    def test_walk_follows_a_loop_element(self) -> None:
        """The shape that made two versions of this census blind to bulk import."""
        fn = self._fn(
            "def h(request):\n data = _body_sync(request)\n"
            ' for i, u in enumerate(data.get("uploads", [])):\n'
            '  x = u.get("source_connection_id")\n'
        )
        found = _ref_reads(fn, _body_vars(fn))
        assert found == {"source_connection_id"}, (
            "the walk did not reach a loop element wrapped in enumerate() — POST "
            "/api/v1/import would contribute zero findings however broken it is"
        )

    def test_walk_follows_a_same_module_helper(self) -> None:
        src = (
            "def helper(payload):\n"
            ' return payload.get("destination_connection_id")\n'
            "def h(request):\n"
            " data = _body_sync(request)\n"
            " return helper(data)\n"
        )
        tree = ast.parse(src)
        fns = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
        handler = fns["h"]
        bvars = _body_vars(handler)
        assert _ref_reads(handler, bvars) == set(), "the key is not read in the handler itself"
        deleg, unres = _delegations(handler, bvars, fns)
        assert deleg == [("helper", {"payload"})], f"delegation not resolved: {deleg} {unres}"
        callee = fns["helper"]
        assert _ref_reads(callee, _propagate(callee, {"payload"})) == {
            "destination_connection_id"
        }, "a reference read inside a delegated helper is invisible to the census"

    def test_an_unscoped_sink_is_detected(self) -> None:
        scoped = self._fn(
            "def h(request, api_key, session):\n data = _body_sync(request)\n"
            ' svc.create(session, api_key.org_id, target_id=int(data["target_id"]))\n'
        )
        unscoped = self._fn(
            "def h(request, api_key, session):\n data = _body_sync(request)\n"
            ' svc.create(session, target_id=int(data["target_id"]))\n'
        )
        assert _unscoped_sinks(scoped, _body_vars(scoped), {"target_id"}) == []
        assert _unscoped_sinks(unscoped, _body_vars(unscoped), {"target_id"}), (
            "a call resolving a body id with no org scope was not reported — this "
            "assertion can only ever be green"
        )

    def test_an_injected_unpinned_reference_is_reported(self) -> None:
        """Force the red the accounting exists to produce, without editing the app."""
        refs, *_ = census()
        injected = ("api_v1_routes", "create_widget", "widget_id")
        assert injected not in refs
        polluted = {**refs, injected: {("POST", "/api/v1/widgets")}}
        missing = sorted(set(polluted) - set(BODY_REFERENCES))
        assert missing == [injected], (
            "the accounting did not flag an unpinned reference — its green means nothing"
        )
        with pytest.raises(AssertionError, match=r"widget_id"):
            assert not missing, (
                f"{len(missing)} resource reference(s) arrive in a request body and are "
                f"not pinned: {missing[0][2]!r}"
            )

    def test_an_unmounted_module_is_not_silently_skipped(self) -> None:
        """The fourth failure mode, directly: scope the walk down and it goes quiet."""
        refs_all, *_ = census()
        refs_narrow, *_ = census(module_names=["health_routes"])
        assert refs_all, "the full census found nothing — every other assertion is vacuous"
        assert not refs_narrow, (
            "a census restricted to a module with no body references still reported some; "
            "the module scoping is not doing what this control assumes"
        )
        assert len(refs_all) > len(refs_narrow), (
            "narrowing the walked module set did not reduce the findings — the census is "
            "not actually driven by the module list, so mounted_modules() proves nothing"
        )
