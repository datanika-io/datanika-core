"""Every `ConnectionType` must be gated by `_validate_connection_form`, or named as not gated.

core#1547 AC5. `_validate_connection_form` (`ui/state/connection_state.py`) is the **only** gate on
the connection save path: `save_connection` calls it and returns on its message, with nothing else
between the form and `ConnectionService`. It is a flat `if / elif` chain over `conn_type` with **no
`else`**, and its last statement is `return ""` — *valid*. So a type with no branch is saved with
every type-specific field blank, and **17 of 37 members have a branch**.

That is a user-visible defect rather than a tidiness one: 14 of the 20 ungated types already print a
`*` and announce `required` to assistive technology, and then save the field empty. The failure
surfaces at connect time — `connection_service.py` builds `databricks://token:{token}@{host}` and
`duckdb:///{path}` — by which point the user has left the form that caused it.

## Why this ships as a ratchet rather than a green

Adding the 20 branches is not a one-line change per type: `_validate_form` passes **13** named
kwargs, and `api_key`, `token`, `http_path`, `catalog`, `owner`, `repo`, `bootstrap_servers`,
`topics` and the rest **are not parameters of the gate at all**, so each field has to reach the
validator first. Holding a guard back until that is done leaves the *mechanism* unwatched for
however long it takes, and the mechanism is what keeps producing new instances — a connector
added next year joins the ungated population in silence.

So :data:`UNGATED_TODAY` records the population and the check forbids it from **growing**;
:meth:`TestTheLedgerDoesNotOutliveTheDefect.test_a_gated_type_is_not_listed_as_ungated` forbids it
from rotting, so an entry must be deleted the moment its branch lands. Same shape as
`tests/test_ui/test_connection_config_roundtrip.py`'s `_DROPPED_ON_SAVE`, deliberately.

## How the population is derived, because the obvious method cannot see this

🔑 **The finding is a *missing* branch, so a grep for a connector name returns `0` — which is
indistinguishable from a grep aimed at the wrong file, the wrong spelling, or a path that does not
exist.** The branch conditions are enumerated from the **AST** of `_validate_connection_form`
and the **set difference** taken against `ConnectionType`. Product hand-read the same chain
first and concluded *"databricks and duckdb"*, using `stripe` as a control when `stripe` is an
instance; the set difference returned 20. A reading of a list is not an enumeration of it, and
the error ran in the flattering direction.

⚠️ The extractor resolves `_DB_TYPES` by *importing* it rather than by parsing its literal, so the
seven-member set cannot drift out of this test's view.
"""

from __future__ import annotations

import ast
import inspect

from datanika.models.connection import ConnectionType
from datanika.ui.state import connection_state as cs

#: Connection types `_validate_connection_form` does not gate, measured 2026-09-25 by set
#: difference.
#: **A debt ledger with an issue number, not a parking space** — every entry is a form that prints
#: `*`, announces `required`, and saves the field empty (core#1547).
UNGATED_TODAY = frozenset(
    {
        "airtable",
        "asana",
        "databricks",
        "duckdb",
        "facebook_ads",
        "freshdesk",
        "github",
        "google_ads",
        "google_analytics",
        "hubspot",
        "jira",
        "kafka",
        "notion",
        "openapi",
        "pipedrive",
        "salesforce",
        "shopify",
        "slack",
        "stripe",
        "zendesk",
    }
)

#: ⚠️ `openapi` is in the ledger above and is the one member whose correct end state may be *no*
#: branch at all. Its Base URL is filled from the spec's `servers` entry and is honestly unmarked
#: (SPEC_FIELD_REQUIREDNESS §2.7), and `_build_config` **already refuses** a spec that yields no
#: usable base URL — measured 2026-09-25: no `servers` + blank Base URL raises
#: `UserFacingError("No base URL found in the spec — set the Base URL field")`, while a spec WITH
#: `servers` and a blank Base URL saves and is filled. `UserFacingError` subclasses `ValueError`, so
#: `save_connection`'s handler renders it.
#:
#: 🚨 So do **not** discharge `openapi` by adding `base_url` to the gate: that breaks the
#: fill-from-the-spec path §2.7 ruled correct. It is listed here because its *other* fields are
#: ungated, not because its Base URL needs gating. Recorded on core#1547.
OPENAPI_IS_A_SPECIAL_CASE = "openapi"


def _gated_types() -> set[str]:
    """The `conn_type` values `_validate_connection_form` branches on, read from its AST.

    Not a grep: a missing branch produces zero matches for the connector's name, which reads exactly
    like a search aimed at the wrong place.
    """
    tree = ast.parse(inspect.getsource(cs))
    fn = next(
        f
        for f in ast.walk(tree)
        if isinstance(f, ast.FunctionDef) and f.name == "_validate_connection_form"
    )
    found: set[str] = set()

    def literals(node) -> set[str]:
        out: set[str] = set()
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            out.add(node.value)
        elif isinstance(node, (ast.Tuple, ast.Set, ast.List)):
            for elt in node.elts:
                out |= literals(elt)
        elif isinstance(node, ast.Name) and node.id == "_DB_TYPES":
            # Imported rather than parsed, so the seven-member set cannot drift out of view.
            out |= set(cs._DB_TYPES)
        return out

    def walk(body) -> None:
        for stmt in body:
            if isinstance(stmt, ast.If):
                for cmp_ in ast.walk(stmt.test):
                    if isinstance(cmp_, ast.Compare):
                        for comparator in cmp_.comparators:
                            found.update(literals(comparator))
                walk(stmt.orelse)

    walk(fn.body)
    return found & {m.value for m in ConnectionType}


GATED = _gated_types()
MEMBERS = {m.value for m in ConnectionType}


class TestTheExtractorCanSee:
    """Every assertion below is vacuous if the AST walk finds nothing."""

    def test_it_found_the_gated_types(self):
        assert len(GATED) >= 15, (
            f"the AST walk found only {len(GATED)} gated types; it has stopped matching the "
            "chain's shape, and an empty result would report every type as ungated"
        )

    def test_it_resolved_the_db_type_set_rather_than_a_name(self):
        """`_DB_TYPES` is a `Name`, not a literal. A walker that only reads literals silently drops
        seven gated types and reports them as part of the defect."""
        assert {"postgres", "mysql", "clickhouse", "oracle", "synapse"} <= GATED

    def test_the_enum_is_populated(self):
        assert len(MEMBERS) >= 30, (
            f"only {len(MEMBERS)} ConnectionType members; the import is stale"
        )


class TestTheUngatedPopulationCannotGrow:
    def test_no_new_connection_type_is_left_ungated(self):
        """A type added without a branch joins the defect silently. This is where it stops."""
        ungated = MEMBERS - GATED
        new = ungated - UNGATED_TODAY
        assert not new, (
            f"{sorted(new)} have no branch in `_validate_connection_form`, so the form saves them "
            "with every type-specific field blank — while most of them print `*` and announce "
            "`required`. Add a branch (and add the fields it checks as PARAMETERS of the gate; "
            "`_validate_form` passes only 13 today). Do not add them to UNGATED_TODAY instead: "
            "that ledger is core#1547's debt, not a place to park new debt."
        )

    def test_the_ledger_is_exactly_the_ungated_set(self):
        """Two-way. Listing a type that IS gated would excuse a defect that no longer exists, and
        `test_a_gated_type_is_not_listed_as_ungated` below names which."""
        assert MEMBERS - GATED == UNGATED_TODAY, (
            f"ungated but unlisted: {sorted((MEMBERS - GATED) - UNGATED_TODAY)}\n"
            f"listed but gated:    {sorted(UNGATED_TODAY - (MEMBERS - GATED))}"
        )


class TestTheLedgerDoesNotOutliveTheDefect:
    def test_a_gated_type_is_not_listed_as_ungated(self):
        """The ratchet's other direction: an entry must be deleted when its branch lands, so the
        list shrinks to nothing as core#1547 is worked off and cannot become a permanent exemption.
        """
        stale = sorted(UNGATED_TODAY & GATED)
        assert not stale, (
            f"{stale} now have a branch in `_validate_connection_form` — delete them from "
            "UNGATED_TODAY rather than leaving a dead exemption behind (WORKFLOW_RULES §5a)"
        )

    def test_every_ledger_entry_names_a_real_connection_type(self):
        """A typo here silently excuses nothing at all, which is worse than excusing the wrong
        thing: the misspelt entry never matches, so the type it was meant to cover stays unlisted
        and `test_no_new_connection_type_is_left_ungated` fails for a reason nobody can find."""
        unknown = sorted(UNGATED_TODAY - MEMBERS)
        assert not unknown, f"{unknown} are not ConnectionType members"

    def test_the_chain_still_has_no_final_else(self):
        """The reason a missing branch is *silent* rather than an error.

        If someone adds an `else: return "Unsupported connection type"`, the defect class closes by
        a different route and this whole module's framing changes — so it should fail here and be
        re-read, not quietly keep passing against a premise that no longer holds.
        """
        tree = ast.parse(inspect.getsource(cs))
        fn = next(
            f
            for f in ast.walk(tree)
            if isinstance(f, ast.FunctionDef) and f.name == "_validate_connection_form"
        )

        def has_else(body) -> bool:
            for stmt in body:
                if isinstance(stmt, ast.If):
                    if stmt.orelse and not isinstance(stmt.orelse[0], ast.If):
                        return True
                    if has_else(stmt.orelse):
                        return True
            return False

        assert not has_else(fn.body), (
            "`_validate_connection_form`'s if/elif chain has gained a final `else`. If it refuses "
            "unknown types, core#1547's silent-fallthrough premise no longer holds and this module "
            "needs re-reading rather than updating."
        )
