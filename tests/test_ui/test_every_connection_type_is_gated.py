"""Every `ConnectionType` must be gated by `_validate_connection_form`, or named as not gated.

core#1547 AC5. `_validate_connection_form` (`ui/state/connection_state.py`) is the **only** gate on
the connection save path: `save_connection` calls it and returns on its message, with nothing else
between the form and `ConnectionService`. It is a flat `if / elif` chain over `conn_type` with **no
`else`**, and its last statement is `return ""` — *valid*. So a type with no branch is saved with
every type-specific field blank, and **17 of 37 members had one** when this was filed.

That is a user-visible defect rather than a tidiness one: 14 of the 20 ungated types already print a
`*` and announce `required` to assistive technology, and then save the field empty. The failure
surfaces at connect time — `connection_service.py` builds `databricks://token:{token}@{host}` and
`duckdb:///{path}` — by which point the user has left the form that caused it.

## It shipped as a ratchet first, and the ledger is now empty

Adding the 20 branches was not a one-line change per type: `_validate_form` passed **13** named
kwargs, and `api_key`, `token`, `http_path`, `catalog`, `owner`, `repo`, `bootstrap_servers`,
`topics` and the rest **were not parameters of the gate at all**, so each field had to reach the
validator first. Holding a guard back until that was done would have left the *mechanism* unwatched
for however long it took, and the mechanism is what keeps producing new instances — a connector
added next year joins the ungated population in silence. So AC5 shipped first, as a ratchet.

🟢 **AC1 landed 2026-09-25**: `_REQUIRED_FORM_FIELDS` in `connection_state.py` gives the 19 a
table-driven tail after the existing chain, and the gate gained the 19 missing parameters.
:data:`UNGATED_TODAY` is now **empty** and :data:`GATED_ELSEWHERE_BY_DESIGN` holds the one member
(`openapi`) that is refused elsewhere on purpose.

⚠️ **The counts above are the figures AS FILED and are deliberately past-tense.** A live count in a
comment goes stale inside a day — measured twice on this codebase — so the numbers that must be
current are asserted, not written: see `TestTheExtractorCanSee` and
`test_the_debt_ledger_is_empty`.

⚠️ **The extractor resolves BOTH `_DB_TYPES` and `_REQUIRED_FORM_FIELDS` by importing them**, so
neither the seven-member set nor the 19-row table can drift out of this test's view. A walker that
only read literals would report all 19 as still ungated — red on the change that fixes the defect.

## How the population is derived, because the obvious method cannot see this

🔑 **The finding is a *missing* branch, so a grep for a connector name returns `0` — which is
indistinguishable from a grep aimed at the wrong file, the wrong spelling, or a path that does not
exist.** The branch conditions are enumerated from the **AST** of `_validate_connection_form`
and the **set difference** taken against `ConnectionType`. Product hand-read the same chain
first and concluded *"databricks and duckdb"*, using `stripe` as a control when `stripe` is an
instance; the set difference returned 20. A reading of a list is not an enumeration of it, and
the error ran in the flattering direction.

"""

from __future__ import annotations

import ast
import inspect

from datanika.models.connection import ConnectionType
from datanika.ui.state import connection_state as cs

#: Connection types `_validate_connection_form` does not gate, measured 2026-09-25 by set
#: difference.
#: **A debt ledger with an issue number, not a parking space** — every entry was a form that prints
#: `*`, announces `required`, and saves the field empty (core#1547).
#:
#: 🟢 **EMPTY since 2026-09-25 (core#1547 AC1).** All 19 gained a branch via the table-driven tail
#: `_REQUIRED_FORM_FIELDS`; the twentieth, `openapi`, was never debt and moved to
#: :data:`GATED_ELSEWHERE_BY_DESIGN` below.
#:
#: ⚠️ **Keep this set and keep it empty — do not delete the mechanism with the debt.** Its job was
#: never the list: `test_no_new_connection_type_is_left_ungated` is what stops a connector added
#: next year from rejoining the population in silence, and an empty ledger makes that check
#: *stricter*, not redundant (`WORKFLOW_RULES` §5a — repoint a guard at the invariant, never delete
#: it because today's instance is gone).
UNGATED_TODAY: frozenset[str] = frozenset()

#: Types that legitimately have no branch in `_validate_connection_form`, with the refusal that
#: covers them instead. **Not an exemption list to grow** — an entry needs a named alternative gate.
#:
#: `openapi`'s Base URL is filled from the spec's `servers` entry and is honestly unmarked
#: (SPEC_FIELD_REQUIREDNESS §2.7), and `_build_config` **already refuses** a spec that yields no
#: usable base URL — measured 2026-09-25: no `servers` + blank Base URL raises
#: `UserFacingError("No base URL found in the spec — set the Base URL field")`, while a spec WITH
#: `servers` and a blank Base URL saves and is filled. `UserFacingError` subclasses `ValueError`, so
#: `save_connection`'s handler renders it. It also refuses an empty spec, a relative server URL, and
#: a spec yielding no loadable endpoint.
#:
#: 🚨 So do **not** discharge `openapi` by adding `base_url` to `_REQUIRED_FORM_FIELDS`: that breaks
#: the fill-from-the-spec path §2.7 ruled correct (core#1547 AC4, measured and recorded on the
#: issue). It is here because it has no *other* required field, not because its Base URL is ungated.
GATED_ELSEWHERE_BY_DESIGN = frozenset({"openapi"})


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
        elif isinstance(node, ast.Name) and node.id == "_REQUIRED_FORM_FIELDS":
            # core#1547 AC1's table-driven tail, resolved by IMPORT for the same reason as
            # `_DB_TYPES` above. 🚨 Without this the extractor reads the tail's `elif conn_type in
            # _REQUIRED_FORM_FIELDS` as a comparison against an opaque Name, finds no literals, and
            # reports all 19 of those types as still ungated — a guard that would go red on the
            # change that fixes the defect it watches (`WORKFLOW_RULES` §5a, and the reason the
            # extractor resolves names at all).
            out |= set(cs._REQUIRED_FORM_FIELDS)
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


#: Every member that is allowed to have no branch, for either reason.
EXCUSED = UNGATED_TODAY | GATED_ELSEWHERE_BY_DESIGN


class TestTheUngatedPopulationCannotGrow:
    def test_no_new_connection_type_is_left_ungated(self):
        """A type added without a branch joins the defect silently. This is where it stops.

        🔑 With `UNGATED_TODAY` now empty this is the whole guard rather than a ratchet: any type
        without a branch, other than `openapi`, fails here.
        """
        ungated = MEMBERS - GATED
        new = ungated - EXCUSED
        assert not new, (
            f"{sorted(new)} have no branch in `_validate_connection_form`, so the form saves them "
            "with every type-specific field blank — while most of them print `*` and announce "
            "`required`. Add a row to `_REQUIRED_FORM_FIELDS` (and add any field it names as a "
            "PARAMETER of the gate, passed by `_validate_form`). Do not add them to UNGATED_TODAY "
            "instead: that ledger is core#1547's debt, it is empty, and it is not a place to park "
            "new debt."
        )

    def test_the_ledger_is_exactly_the_ungated_set(self):
        """Two-way. Listing a type that IS gated would excuse a defect that no longer exists, and
        `test_a_gated_type_is_not_listed_as_ungated` below names which."""
        assert MEMBERS - GATED == EXCUSED, (
            f"ungated but unlisted: {sorted((MEMBERS - GATED) - EXCUSED)}\n"
            f"listed but gated:    {sorted(EXCUSED - (MEMBERS - GATED))}"
        )

    def test_the_debt_ledger_is_empty(self):
        """core#1547 AC1 is discharged, and this is what says so in one line.

        ⚠️ If a future change needs to re-open the ledger, that is a decision to record on an
        issue — not a quiet re-population. The message here is the place it will be noticed.
        """
        assert not UNGATED_TODAY, (
            f"{sorted(UNGATED_TODAY)} are parked as ungated debt again. Every connection type "
            "except `openapi` has had a gate since core#1547; re-opening this ledger needs an "
            "issue, not an entry."
        )

    def test_the_by_design_exemption_has_not_grown(self):
        """The exemption set is the one remaining way to be ungated and stay green.

        An entry must name an alternative refusal, which is a thing a reviewer has to check by
        hand — so the set is pinned to exactly what was ruled, and adding to it goes red here.
        """
        assert sorted(GATED_ELSEWHERE_BY_DESIGN) == ["openapi"], (
            f"{sorted(GATED_ELSEWHERE_BY_DESIGN)} claim to be gated somewhere other than "
            "`_validate_connection_form`. Only `openapi` has been ruled so (AC4, §2.7). Adding a "
            "type here exempts it from the only gate on the save path — name the refusal that "
            "covers it, on an issue, first."
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
        unknown = sorted(EXCUSED - MEMBERS)
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
