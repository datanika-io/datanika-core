"""Schema-required config keys the form can write and the gate does not refuse.

core#1607. The population is derived from the AST of ``_build_config`` and
``_validate_connection_form``. It is not a list of the 19 fields named when the
issue was filed — that census expires (WORKFLOW_RULES §5b.5).

A difference here is legitimate (§2.7: the form and the stored-config contract
answer different questions), so the assertion is not "the two sets are equal".
It is SPEC_FIELD_REQUIREDNESS AC2: every remaining difference is an explicit
entry, and the guard fails both when a new unrecorded difference appears and
when a recorded one disappears while its entry remains.

Two walks that look equivalent and are not, both of which reported 0 members
on the first measurement:

* entering the function's first ``If`` (the raw-JSON / blank-name guard) instead
  of the ``conn_type`` / ``t`` chain;
* ``ast.walk`` on an ``elif`` chain, which descends into ``orelse`` and
  attributes every later branch's fields to the first type.

An empty production set is not a clean sweep. The synthetic chain below is the
control that can still see a member.
"""

from __future__ import annotations

import ast
import inspect

import pytest

from datanika.services.connection_schemas import CONFIG_SCHEMAS
from datanika.ui.state import connection_state as cs

#: Remaining (type, config key) pairs where the schema lists the key as required,
#: ``_build_config`` can write it from a form attribute, and the save gate does
#: not refuse a blank. Each entry would carry its reason. core#1607 resolved the
#: filed rows, so the ledger is empty — emptiness is evidence only together with
#: ``TestTheExtractorCanSeeAPlantedMember``.
RECORDED: frozenset[tuple[str, str]] = frozenset()

#: Schema-required keys this instrument writes but cannot attribute to a form
#: field. Named, not treated as clean, and not counted as members.
#:
#: ``kafka`` ``topics`` is NOT here. The assignment is a comprehension, and the
#: iterator is ``self.form_topics``, which this walk sees. The 2026-09-26
#: instrument did not, and listed ``topics`` as unresolved. It is gated.
UNRESOLVED_RECORDED: frozenset[tuple[str, str]] = frozenset(
    {
        ("openapi", "base_url"),
        ("openapi", "resources"),
    }
)


def ledger_mismatch(observed: set[tuple[str, str]], recorded: frozenset[tuple[str, str]]) -> str:
    """'' when the sets are equal. Either side sticking out is the failure."""
    extra = observed - recorded
    stale = recorded - observed
    if not extra and not stale:
        return ""
    return (
        f"unrecorded: {sorted(extra)}; stale record: {sorted(stale)}. "
        "A new pair is a requiredness ruling (SPEC_FIELD_REQUIREDNESS §2.10). "
        "A stale pair means the entry outlived the difference."
    )


def _form_attrs(node: ast.AST) -> set[str]:
    found: set[str] = set()
    for child in ast.walk(node):
        if (
            isinstance(child, ast.Attribute)
            and isinstance(child.value, ast.Name)
            and child.value.id == "self"
            and child.attr.startswith("form_")
        ):
            found.add(child.attr)
    return found


def _types_of(test: ast.AST, db_types: set[str], table_types: set[str]) -> set[str] | None:
    """The connection types ``test`` selects, or None when it is not a type test."""
    if isinstance(test, ast.BoolOp) and isinstance(test.op, ast.And):
        narrowed = [
            s
            for v in test.values
            if (s := _types_of(v, db_types, table_types)) is not None
        ]
        if len(narrowed) == 1:
            return narrowed[0]
        if len(narrowed) > 1:
            return set.intersection(*narrowed)
        return None
    if isinstance(test, ast.BoolOp) and isinstance(test.op, ast.Or):
        parts = [_types_of(v, db_types, table_types) for v in test.values]
        if any(p is None for p in parts):
            return None
        return set.union(*parts)
    if not (
        isinstance(test, ast.Compare)
        and len(test.ops) == 1
        and isinstance(test.left, ast.Name)
        and test.left.id in {"t", "conn_type"}
    ):
        return None
    op = test.ops[0]
    right = test.comparators[0]
    if isinstance(op, ast.Eq) and isinstance(right, ast.Constant) and isinstance(right.value, str):
        return {right.value}
    if isinstance(op, ast.In):
        return _in_set(right, db_types, table_types)
    return None


def _in_set(node: ast.AST, db_types: set[str], table_types: set[str]) -> set[str] | None:
    if isinstance(node, ast.Name) and node.id == "_DB_TYPES":
        return set(db_types)
    if isinstance(node, ast.Name) and node.id == "_REQUIRED_FORM_FIELDS":
        return set(table_types)
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        out: set[str] = set()
        for elt in node.elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                out.add(elt.value)
            else:
                return None
        return out
    return None


def _type_chain(fn: ast.FunctionDef, db_types: set[str], table_types: set[str]):
    """``(types, body)`` for each branch of the type chain.

    The chain is the first ``If`` whose test selects on ``t`` / ``conn_type``.
    Earlier ``If``s (blank name, raw JSON) are not it. ``elif`` is ``orelse``,
    and only that one statement is the next branch — never ``ast.walk``.
    """
    for stmt in fn.body:
        node = stmt
        if not isinstance(node, ast.If):
            continue
        if _types_of(node.test, db_types, table_types) is None:
            continue
        while isinstance(node, ast.If) and _types_of(node.test, db_types, table_types) is not None:
            yield _types_of(node.test, db_types, table_types), node.body
            nxt = node.orelse
            if (
                len(nxt) == 1
                and isinstance(nxt[0], ast.If)
                and _types_of(nxt[0].test, db_types, table_types) is not None
            ):
                node = nxt[0]
                continue
            break
        return


def _collect_writes(
    body,
    types: set[str],
    db_types: set[str],
    table_types: set[str],
    writes,
    unresolved,
):
    for stmt in body:
        if isinstance(stmt, ast.If):
            narrowed = _types_of(stmt.test, db_types, table_types)
            if narrowed is not None:
                _collect_writes(stmt.body, narrowed, db_types, table_types, writes, unresolved)
                _collect_writes(stmt.orelse, types, db_types, table_types, writes, unresolved)
            else:
                _collect_writes(stmt.body, types, db_types, table_types, writes, unresolved)
                _collect_writes(stmt.orelse, types, db_types, table_types, writes, unresolved)
            continue
        if isinstance(stmt, (ast.For, ast.With, ast.Try)):
            nested = []
            if isinstance(stmt, (ast.For, ast.With)):
                nested.append(stmt.body)
            else:
                for handler in stmt.handlers:
                    nested.append(handler.body)
                nested.append(stmt.body)
                nested.append(stmt.orelse)
                nested.append(stmt.finalbody)
            for block in nested:
                _collect_writes(block, types, db_types, table_types, writes, unresolved)
            continue
        if not isinstance(stmt, ast.Assign):
            continue
        value_attrs = _form_attrs(stmt.value)
        for target in stmt.targets:
            if not (
                isinstance(target, ast.Subscript)
                and isinstance(target.value, ast.Name)
                and target.value.id == "config"
                and isinstance(target.slice, ast.Constant)
                and isinstance(target.slice.value, str)
            ):
                continue
            key = target.slice.value
            for conn_type in types:
                if value_attrs:
                    writes.setdefault(conn_type, {}).setdefault(key, set()).update(value_attrs)
                else:
                    unresolved.setdefault(conn_type, set()).add(key)


def _refused_params(body) -> set[str]:
    """Parameter names a branch refuses when blank: ``if not user.strip()``."""
    found: set[str] = set()
    for stmt in body:
        if not isinstance(stmt, ast.If):
            continue
        found.update(_stripped_names(stmt.test))
        found.update(_refused_params(stmt.body))
        # A nested ``else`` inside one branch is still that branch. The type
        # chain's next ``elif`` is never passed in here — ``_type_chain``
        # yields each body separately.
        found.update(_refused_params(stmt.orelse))
    return found


def _stripped_names(test: ast.AST) -> set[str]:
    found: set[str] = set()
    if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
        found.update(_stripped_names(test.operand))
        return found
    if isinstance(test, ast.BoolOp):
        for value in test.values:
            found.update(_stripped_names(value))
        return found
    if (
        isinstance(test, ast.Call)
        and isinstance(test.func, ast.Attribute)
        and test.func.attr == "strip"
        and isinstance(test.func.value, ast.Name)
    ):
        found.add(test.func.value.id)
    return found


def analyze(source: str, *, db_types: set[str], required_fields: dict, schemas: dict):
    """Return ``(differences, unresolved_required)``.

    ``differences`` are schema-required keys written from a form attribute the
    gate does not refuse. ``unresolved_required`` are schema-required keys
    written from something that is not a form attribute.
    """
    table_types = set(required_fields)
    tree = ast.parse(source)
    # ``ast.walk`` only to FIND the functions. ``_build_config`` is a method, so
    # it is not a child of the module. The chain itself is never walked this way:
    # ``orelse`` on an ``elif`` would attribute every later branch to the first.
    functions = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
    build = next(n for n in functions if n.name == "_build_config")
    gate = next(n for n in functions if n.name == "_validate_connection_form")
    writes: dict[str, dict[str, set[str]]] = {}
    unresolved: dict[str, set[str]] = {}
    for types, body in _type_chain(build, db_types, table_types):
        _collect_writes(body, types, db_types, table_types, writes, unresolved)

    refused: dict[str, set[str]] = {}
    for types, body in _type_chain(gate, db_types, table_types):
        if types <= table_types and types == table_types:
            for conn_type in types:
                refused[conn_type] = {field for field, _label in required_fields[conn_type]}
            continue
        params = _refused_params(body)
        for conn_type in types:
            # A type that also has a table row keeps the table. No current
            # branch is both; the import is the tail, and it is one branch.
            refused[conn_type] = params

    differences: set[tuple[str, str]] = set()
    unresolved_required: set[tuple[str, str]] = set()
    for conn_type, schema in schemas.items():
        required = set(schema.get("required", ()))
        gated_attrs = {f"form_{name}" for name in refused.get(conn_type, ())}
        for key, attrs in writes.get(conn_type, {}).items():
            if key not in required:
                continue
            if attrs and attrs <= gated_attrs:
                continue
            differences.add((conn_type, key))
        for key in unresolved.get(conn_type, ()):
            if key in required:
                unresolved_required.add((conn_type, key))
    return differences, unresolved_required


def _module_source() -> str:
    return inspect.getsource(cs)


def live_differences() -> set[tuple[str, str]]:
    differences, _unresolved = analyze(
        _module_source(),
        db_types=set(cs._DB_TYPES),
        required_fields=cs._REQUIRED_FORM_FIELDS,
        schemas=CONFIG_SCHEMAS,
    )
    return differences


def live_unresolved() -> set[tuple[str, str]]:
    _differences, unresolved = analyze(
        _module_source(),
        db_types=set(cs._DB_TYPES),
        required_fields=cs._REQUIRED_FORM_FIELDS,
        schemas=CONFIG_SCHEMAS,
    )
    return unresolved


#: A chain shaped like the two bugs. ``alpha`` is the first type branch and
#: writes ``user`` and ``password``. ``beta`` writes ``group_id`` in the
#: ``elif``. ``gamma`` is the table tail. A decoy ``If`` sits above the chain.
_SYNTHETIC = '''
_DB_TYPES = {"alpha"}
_REQUIRED_FORM_FIELDS = {"gamma": (("token", "Token"),)}

def _build_config(self):
    config = {}
    if self.form_use_raw_json:
        config["decoy"] = self.form_decoy
        return config
    t = self.form_type
    if t in _DB_TYPES:
        if self.form_user:
            config["user"] = self.form_user
        if self.form_password:
            config["password"] = self.form_password
    elif t == "beta":
        if self.form_group_id:
            config["group_id"] = self.form_group_id
        config["topics"] = [item.strip() for item in self.form_topics.split(",") if item.strip()]
    elif t in _REQUIRED_FORM_FIELDS:
        if self.form_token:
            config["token"] = self.form_token
    elif t == "openapi":
        config["base_url"] = parsed.base_url
        config["resources"] = parsed.resources
    return config

def _validate_connection_form(name, conn_type, use_raw_json, *, host="", user="", token=""):
    if not name.strip():
        return "Connection name is required"
    if conn_type in _DB_TYPES:
        if not host.strip():
            return "Host is required"
    elif conn_type == "beta":
        if not user.strip():
            return "User is required"
    elif conn_type in _REQUIRED_FORM_FIELDS:
        for field, label in _REQUIRED_FORM_FIELDS[conn_type]:
            if not token.strip():
                return f"{label} is required"
    return ""
'''

_SYNTHETIC_SCHEMAS = {
    "alpha": {"required": ["host", "user", "password"]},
    "beta": {"required": ["group_id", "topics"]},
    "gamma": {"required": ["token"]},
    "openapi": {"required": ["base_url", "resources"]},
}


def _synthetic():
    return analyze(
        _SYNTHETIC,
        db_types={"alpha"},
        required_fields={"gamma": (("token", "Token"),)},
        schemas=_SYNTHETIC_SCHEMAS,
    )


class TestTheExtractorCanSeeAPlantedMember:
    def test_the_first_branch_does_not_absorb_the_elif(self):
        """``ast.walk`` on the first ``If`` descends into ``orelse``.

        That attributes ``group_id`` to ``alpha``. The first measurement
        reported 0 members for exactly this reason once the walk was "fixed"
        to enter an ``If`` — the empty set looked like a clean sweep.
        """
        differences, unresolved = _synthetic()
        assert ("alpha", "group_id") not in differences
        assert ("alpha", "user") in differences
        assert ("alpha", "password") in differences
        assert ("beta", "group_id") in differences

    def test_a_comprehension_still_names_its_form_attribute(self):
        """The 2026-09-26 instrument could not see ``topics``. This one must.

        ``topics`` is gated in the synthetic chain's schema and written from
        ``self.form_topics``, but the gate branch for ``beta`` refuses ``user``,
        not ``topics``. So ``topics`` is a resolved member, not unresolved.
        """
        differences, unresolved = _synthetic()
        assert ("beta", "topics") in differences
        assert ("beta", "topics") not in unresolved

    def test_the_decoy_if_above_the_chain_is_not_the_chain(self):
        differences, _unresolved = _synthetic()
        assert ("alpha", "decoy") not in differences
        assert all(key != "decoy" for _conn, key in differences)

    def test_a_parsed_object_stays_unresolved_rather_than_clean(self):
        _differences, unresolved = _synthetic()
        assert unresolved == {("openapi", "base_url"), ("openapi", "resources")}

    def test_the_table_tail_is_gated_by_import(self):
        """A walker that only reads ``if not x.strip()`` misses the tail's loop
        and reports every table type as ungated. ``gamma``'s ``token`` is
        required, written, and refused by the imported row, so it is not a member.
        """
        differences, _unresolved = _synthetic()
        assert ("gamma", "token") not in differences

    def test_a_new_difference_and_a_stale_entry_both_fail(self):
        fresh = ledger_mismatch({("alpha", "password")}, frozenset())
        stale = ledger_mismatch(set(), frozenset({("alpha", "password")}))
        assert "password" in fresh
        assert "unrecorded" in fresh
        assert "password" in stale
        assert "stale" in stale
        assert ledger_mismatch(set(), frozenset()) == ""

    def test_ast_walk_would_have_failed_this_control(self):
        """The bug, stated on the same source, so the control is not circular.

        Walking the first type ``If`` visits the ``elif`` bodies. The extractor
        must not.
        """
        tree = ast.parse(_SYNTHETIC)
        build = next(
            n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_build_config"
        )
        first = next(
            stmt
            for stmt in build.body
            if isinstance(stmt, ast.If) and _types_of(stmt.test, {"alpha"}, {"gamma"}) is not None
        )
        absorbed: set[str] = set()
        for node in ast.walk(first):
            if (
                isinstance(node, ast.Subscript)
                and isinstance(node.slice, ast.Constant)
                and isinstance(node.slice.value, str)
            ):
                absorbed.add(node.slice.value)
        assert "group_id" in absorbed
        differences, _unresolved = _synthetic()
        assert ("alpha", "group_id") not in differences


class TestTheLivePopulationMatchesTheLedger:
    def test_every_remaining_difference_is_recorded(self):
        observed = live_differences()
        assert ledger_mismatch(observed, RECORDED) == ""

    def test_what_the_instrument_cannot_attribute_is_named(self):
        observed = live_unresolved()
        assert ledger_mismatch(observed, UNRESOLVED_RECORDED) == ""


#: core#1607. User and password are not one verdict. The seven SQL types keep
#: ``user`` required and stop requiring ``password``. Snowflake keeps
#: ``password`` and stops requiring ``warehouse`` and ``schema``.
_SQL_TYPES = ("postgres", "mysql", "mssql", "oracle", "redshift", "clickhouse", "synapse")


def _state(conn_type: str, **fields) -> cs.ConnectionState:
    state = cs.ConnectionState()
    state.form_name = "n"
    state.form_type = conn_type
    state.form_use_raw_json = False
    for name, value in fields.items():
        setattr(state, name, value)
    return state


class TestEachFieldWasRuledOnItsOwn:
    """The direction, through the real state object. Dropping the wrong key
    satisfies the empty ledger and still fails here."""

    @pytest.mark.parametrize("conn_type", _SQL_TYPES)
    def test_a_blank_sql_user_is_refused_and_a_blank_password_is_not(self, conn_type):
        refused = _state(
            conn_type,
            form_host="h",
            form_port="5432",
            form_database="db",
            form_user="",
            form_password="secret",
        )._validate_form()
        assert refused == "User is required"
        accepted = _state(
            conn_type,
            form_host="h",
            form_port="5432",
            form_database="db",
            form_user="role",
            form_password="",
        )._validate_form()
        assert accepted == ""
        required = CONFIG_SCHEMAS[conn_type]["required"]
        assert "user" in required
        assert "password" not in required

    def test_snowflake_password_is_required_and_warehouse_and_schema_are_not(self):
        refused = _state(
            "snowflake",
            form_account="acct",
            form_user="u",
            form_database="db",
            form_password="",
            form_warehouse="wh",
            form_schema="public",
        )._validate_form()
        assert refused == "Password is required"
        accepted = _state(
            "snowflake",
            form_account="acct",
            form_user="u",
            form_database="db",
            form_password="secret",
            form_warehouse="",
            form_schema="",
        )._validate_form()
        assert accepted == ""
        required = CONFIG_SCHEMAS["snowflake"]["required"]
        assert "password" in required
        assert "warehouse" not in required
        assert "schema" not in required

    def test_bigquery_keyfile_stays_optional_and_leaves_the_schema_required_list(self):
        accepted = _state(
            "bigquery",
            form_project="proj",
            form_dataset="raw",
            form_keyfile_json="",
        )._validate_form()
        assert accepted == ""
        assert "keyfile_json" in CONFIG_SCHEMAS["bigquery"]["properties"]
        assert "keyfile_json" not in CONFIG_SCHEMAS["bigquery"]["required"]

    def test_kafka_group_id_stays_optional_and_leaves_the_schema_required_list(self):
        accepted = _state(
            "kafka",
            form_bootstrap_servers="broker:9092",
            form_topics="events",
            form_group_id="",
        )._validate_form()
        assert accepted == ""
        assert "group_id" in CONFIG_SCHEMAS["kafka"]["properties"]
        assert "group_id" not in CONFIG_SCHEMAS["kafka"]["required"]
