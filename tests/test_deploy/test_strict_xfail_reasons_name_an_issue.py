"""Every strict xfail must name the issue it is pinning, readably (core#1025, first half).

A `pytest.mark.xfail(strict=True)` is an assertion that *"this is broken, and CI must fail if
it stops being broken."* core#896 was closed COMPLETED while four of them named the very defect
it was closed for, and nobody read them — a suite reporting `6 passed, 4 xfailed` is the same
colour as one reporting `10 passed`.

core#1025's remaining half is a **scheduled, network-reading** check that resolves those issue
numbers against GitHub and refuses to let a strict xfail outlive a CLOSED issue. **This file is
its prerequisite**, and the reason is the part that is easy to skip: that check can only read
the reasons it can parse, and **a marker whose reason it cannot parse is silently skipped —
indistinguishable from a marker whose issue is still open.** So its coverage *is* the set of
readable reasons, and until this file existed nothing measured that set.

🆕 **The TABLE RESOLVER at the bottom of this file is now shipped (2026-09-06)** and it is what
takes that coverage from **4 of 9 to 9 of 9** — by reading `AWAITING_PROVISIONING` rather than
giving up at the call site. It is offline and deterministic, so it lives here in the `test` job;
only the GitHub read is still unbuilt, and it is unbuilt for the tier reason core#1025 states —
a network call must not be able to red-light an unrelated PR.

## 🚨 The correction this file exists to carry, and it is against my own earlier measurement

core#1025 records *"4 of 7 reasons parse — the check would cover 57%"*. That figure counts
**call sites**, and it is the wrong unit. Re-derived here:

| unit | count |
|---|---|
| strict xfail **call sites** in core | 7 |
| static `reason=` naming an issue token | **4 of 4** |
| dynamic call sites (reason built at runtime) | 3 |
| ↳ **dormant** — their table is literally `{}`, so they emit **no marker at all** | **2** |
| ↳ live — `AWAITING_PROVISIONING`, 5 entries | 1 |
| **strict xfail markers actually applied today** | **9** = 4 static + 5 from that table |
| of those, issue mechanically discoverable | **9 of 9** |
| of those, discoverable *from the call site alone* | 4 of 9 — **44%**, not 57% |

`KNOWN_VIOLATIONS` in `test_alerting_config.py` and `KNOWN_BROKEN` in
`test_connection_test_observes_before_reporting.py` are both `= {}`. Their `.get()` returns
`None`, no marker is built, and neither has ever contributed an unreadable reason. The third
site's reasons carry `pin.tracking` — `cloud#160` on all five — which is a perfectly readable
token sitting one indirection from the call site.

🔑 **So the remedy is not an exception list, it is to read the table**, and the second half can
reach 9 of 9 rather than settling for 4 of 7.

⚠️ **The instrument lesson, third session running, and it is the reason this table is in the
docstring rather than in a comment.** A `grep 'strict=True' | grep xfail` says 14 in core; it
over-counts by docstrings *about* xfail markers. I replaced it with an AST scan, which fixed
that unit error and introduced another: **it counts the places a marker is written, not the
markers that exist.** Both instruments returned a plausible number. The tell was in plain
sight — two of the three "unparseable" sites are `.get()` lookups into a dict that is `{}` on
its own declaration line. **Ask what your instrument counts, not merely whether it parsed.**

## What this guard requires — POSITIVELY

Not *"no reason may be unparseable"* — a ban is satisfied by deleting the marker, and by an
artifact that merely denies the banned shape (`WORKFLOW_RULES.md` §4). The requirement is that
**every strict xfail's issue attribution is reachable**: either the reason is a static literal
carrying a `core#N`/`cloud#N` token, or the site is a declared table-driven factory whose
carrier this file names and whose state it checks in both directions.

Staleness is checked both ways, per core#1025's acceptance:

* a dynamic site missing from `DYNAMIC_SITES` → red (new unreadable marker);
* a `DYNAMIC_SITES` entry whose file or carrier no longer exists → red (stale entry sitting
  there asserting nothing);
* an entry declared `dormant` whose table has gained an entry → red, because it now emits real
  markers and the count in this docstring is wrong;
* an entry declared `live` whose table has been emptied → red, for the same reason inverted.

**Disjoint from `datanika-cloud/tests/test_xfail_markers_report_something.py`** — that one
guards *non-strict* markers appearing; this guards *strict* markers' reasons being readable.
Neither can mask the other, which matters, because a redundant guard can suppress the only
signal that would catch a regression beside it (cloud#176).
"""

from __future__ import annotations

import ast
import re
import warnings
from dataclasses import dataclass
from pathlib import Path

import pytest

CORE_TESTS = Path(__file__).resolve().parents[1]
REPO_ROOT = CORE_TESTS.parent
#: Sibling worktree, when one exists. Core CI has no cloud checkout, so the cloud arm is
#: conditional — but it must never *silently* skip; see `test_the_cloud_arm_states_its_state`.
CLOUD_TESTS = REPO_ROOT.parent / "datanika-cloud-qa" / "tests"

ISSUE_TOKEN = re.compile(r"\b(?:core|cloud)#\d+")


@dataclass(frozen=True)
class DynamicSite:
    """A strict xfail whose reason is built at runtime from a table.

    `carrier` is the module-level name holding the pins; `dormant` records whether that table
    is empty today, which decides whether the site emits any marker at all.
    """

    carrier: str
    dormant: bool
    note: str


#: Every strict xfail call site whose `reason=` is not a static literal. Keyed by repo and
#: path — NOT by line number, which drifts on every edit above it and would turn this into a
#: source of false reds.
DYNAMIC_SITES: dict[tuple[str, str], DynamicSite] = {
    ("core", "tests/test_connector_smoke/conftest.py"): DynamicSite(
        carrier="AWAITING_PROVISIONING",
        dormant=False,
        note=(
            "Live: 5 pins. The issue is interpolated from `pin.tracking` — cloud#160 on all "
            "five — so it IS readable, one indirection from the call site. This is the site "
            "core#1025's second half should learn to read, rather than skip."
        ),
    ),
    ("core", "tests/test_deploy/test_alerting_config.py"): DynamicSite(
        carrier="KNOWN_VIOLATIONS",
        dormant=True,
        note=(
            "Dormant: `KNOWN_VIOLATIONS: dict[tuple[str, str], str] = {}`. `.get()` returns "
            "None and `_mark` builds no marker, so this site has never produced an unreadable "
            "reason. It was counted as one because the scan counted call sites."
        ),
    ),
    ("core", "tests/test_services/test_connection_test_observes_before_reporting.py"): DynamicSite(
        carrier="KNOWN_BROKEN",
        dormant=True,
        note=(
            "Dormant: `KNOWN_BROKEN: dict[ConnectionType, str] = {}`. Both original pins were "
            "cleared by mutating the fix back and confirming each went red for its own stated "
            "reason, which is the bar this project sets for clearing one."
        ),
    ),
}

#: Ratchets. Both must only ever go DOWN, and both are exact so that a decrease forces the
#: number here to be lowered deliberately rather than drifting.
EXPECTED_DYNAMIC_SITES = 3
EXPECTED_LIVE_DYNAMIC_SITES = 1


@dataclass(frozen=True)
class StrictXfail:
    repo: str
    relpath: str
    lineno: int
    #: the literal text, iff `reason=` is a static string
    static_reason: str | None
    #: `ast.unparse` of the expression, iff it is not
    dynamic_reason: str | None

    @property
    def is_dynamic(self) -> bool:
        return self.dynamic_reason is not None

    @property
    def key(self) -> tuple[str, str]:
        return (self.repo, self.relpath)


def _is_xfail_call(node: ast.Call) -> bool:
    """`pytest.mark.xfail(...)` / `mark.xfail(...)`, however it was imported."""
    parts: list[str] = []
    func: ast.expr = node.func
    while isinstance(func, ast.Attribute):
        parts.append(func.attr)
        func = func.value
    if isinstance(func, ast.Name):
        parts.append(func.id)
    return bool(parts) and parts[0] == "xfail" and "mark" in parts


def strict_xfails(tests_root: Path, repo: str) -> list[StrictXfail]:
    """THE SCANNER. Every `xfail(strict=True)` call site under one tests tree."""
    found: list[StrictXfail] = []
    for path in sorted(tests_root.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        try:
            # Parsing source as DATA must not emit the warnings importing it would. At least
            # one file in the tree carries a `\{` escape, and without this the scan prints a
            # `SyntaxWarning` attributed to `<unknown>:103` on every CI run — a warning that
            # names no file, belongs to someone else, and would be read as this guard's.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SyntaxWarning)
                tree = ast.parse(source)
        except SyntaxError:  # pragma: no cover - a tree that will not parse fails CI anyway
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not _is_xfail_call(node):
                continue
            kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
            strict = kwargs.get("strict")
            if not (isinstance(strict, ast.Constant) and strict.value is True):
                continue
            reason = kwargs.get("reason")
            static = (
                reason.value
                if isinstance(reason, ast.Constant) and isinstance(reason.value, str)
                else None
            )
            found.append(
                StrictXfail(
                    repo=repo,
                    relpath=path.relative_to(tests_root.parent).as_posix(),
                    lineno=node.lineno,
                    static_reason=static,
                    dynamic_reason=(
                        None if static is not None else (ast.unparse(reason) if reason else "")
                    ),
                )
            )
    return found


def unattributed(markers: list[StrictXfail]) -> list[str]:
    """THE PREDICATE. Strict xfails whose issue attribution is not reachable.

    Module-level so the arming tests below drive the same code path the real assertion does — a
    control written *beside* a check keeps passing when the check's own transform is removed.
    """
    problems: list[str] = []
    for m in markers:
        if not m.is_dynamic:
            if not ISSUE_TOKEN.search(m.static_reason or ""):
                problems.append(
                    f"{m.relpath}:{m.lineno} is xfail(strict=True) with reason "
                    f"{(m.static_reason or '')[:70]!r}, which names no core#N/cloud#N issue. "
                    "core#1025's closed-issue check reads that token; without it this marker "
                    "is silently skipped, which looks exactly like an issue still being open."
                )
            continue
        site = DYNAMIC_SITES.get(m.key)
        if site is None:
            problems.append(
                f"{m.relpath}:{m.lineno} builds its reason at runtime "
                f"(`{m.dynamic_reason}`) and is not in DYNAMIC_SITES. Add it, naming the table "
                "its attribution lives in — or make the reason a literal carrying the issue "
                "token. An unlisted dynamic reason is one core#1025's check cannot read."
            )
    return problems


def carrier_state(relpath: str, carrier: str, repo_root: Path) -> str:
    """`missing` | `dormant` | `live` — read from the real module, never restated."""
    path = repo_root / relpath
    if not path.exists():
        return "missing"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        targets: list[ast.expr] = []
        if isinstance(node, ast.AnnAssign):
            targets = [node.target]
        elif isinstance(node, ast.Assign):
            targets = list(node.targets)
        else:
            continue
        if not any(isinstance(t, ast.Name) and t.id == carrier for t in targets):
            continue
        value = node.value
        if isinstance(value, ast.Dict):
            return "dormant" if not value.keys else "live"
        if isinstance(value, (ast.List, ast.Set, ast.Tuple)):
            return "dormant" if not value.elts else "live"
        return "live"
    return "missing"


def _core_markers() -> list[StrictXfail]:
    return strict_xfails(CORE_TESTS, "core")


# ---------------------------------------------------------------------------
# Positive control first. A scanner that finds nothing is indistinguishable from
# a clean tree and makes every assertion below vacuously true.
# ---------------------------------------------------------------------------


def test_control_the_scanner_finds_the_real_markers() -> None:
    markers = _core_markers()
    assert markers, (
        "no xfail(strict=True) found anywhere in core's tests/. Either the tree genuinely has "
        "none — in which case delete this guard deliberately — or the scanner has stopped "
        "matching, which would make every assertion in this file pass while checking nothing."
    )
    assert any(not m.is_dynamic for m in markers), "no STATIC reason found; the parser is blind"
    assert any(m.is_dynamic for m in markers), (
        "no DYNAMIC reason found. The dynamic branch is the half this file exists for, so a "
        "scanner that cannot see one is not exercising it."
    )


def test_control_the_scanner_can_be_wrong_about_a_marker() -> None:
    """Both classifications must be reachable from real syntax, and `strict=False` excluded."""
    src = (
        "import pytest\n"
        '@pytest.mark.xfail(reason="core#1: static", strict=True)\n'
        "def a(): ...\n"
        "@pytest.mark.xfail(reason=build(), strict=True)\n"
        "def b(): ...\n"
        '@pytest.mark.xfail(reason="core#2: not strict", strict=False)\n'
        "def c(): ...\n"
        '@pytest.mark.xfail(reason="core#3: no strict kwarg")\n'
        "def d(): ...\n"
    )
    tmp = CORE_TESTS / "test_deploy" / "_scanner_control_fixture.py"
    tmp.write_text(src, encoding="utf-8")
    try:
        found = strict_xfails(CORE_TESTS, "core")
        mine = [m for m in found if m.relpath.endswith("_scanner_control_fixture.py")]
        seen = [(m.lineno, m.static_reason) for m in mine]
        assert len(mine) == 2, (
            f"expected exactly the two strict markers, got {seen}. A scanner that also picks "
            "up strict=False or a bare xfail would demand issue tokens where this project does "
            "not require them, and get itself deleted."
        )
        assert [m.is_dynamic for m in sorted(mine, key=lambda m: m.lineno)] == [False, True]
    finally:
        tmp.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# The assertions.
# ---------------------------------------------------------------------------


def test_every_strict_xfail_names_a_readable_issue() -> None:
    problems = unattributed(_core_markers())
    assert not problems, (
        "strict xfail markers whose issue attribution core#1025's check cannot read:\n  "
        + "\n  ".join(problems)
    )


def test_every_dynamic_site_entry_still_describes_the_tree() -> None:
    """Staleness, both directions. An entry that no longer matches reality asserts nothing."""
    live_markers = _core_markers()
    dynamic_keys = {m.key for m in live_markers if m.is_dynamic}

    for (repo, relpath), site in DYNAMIC_SITES.items():
        assert repo == "core", f"{relpath}: only core is scanned here; see the cloud arm below"
        assert (repo, relpath) in dynamic_keys, (
            f"DYNAMIC_SITES lists {relpath}, but no dynamic-reason strict xfail is there any "
            "more. If it was made a literal or removed, delete this entry — a stale exemption "
            "sits in the tree asserting nothing and quietly widens what the guard permits."
        )
        state = carrier_state(relpath, site.carrier, REPO_ROOT)
        assert state != "missing", (
            f"{relpath}: `{site.carrier}` is no longer assigned at module level. This entry "
            "names the wrong carrier, so its dormant/live judgement is unfounded."
        )
        expected = "dormant" if site.dormant else "live"
        assert state == expected, (
            f"{relpath}: `{site.carrier}` is {state}, but DYNAMIC_SITES says {expected}.\n"
            + (
                "It has gained entries, so this site now emits real strict xfail markers whose "
                "reasons are built at runtime — the count in this module's docstring is stale "
                "and core#1025's check now has a genuine blind spot. Set dormant=False and "
                "make the attribution readable."
                if state == "live"
                else "Its table has been emptied, so the site emits no markers at all. Set "
                "dormant=True and lower EXPECTED_LIVE_DYNAMIC_SITES — an exemption for a "
                "marker that does not exist is the kind of entry that outlives its reason."
            )
        )


def test_the_dynamic_ratchet_only_goes_down() -> None:
    markers = _core_markers()
    dynamic = {m.key for m in markers if m.is_dynamic}
    live = {k for k in dynamic if not DYNAMIC_SITES[k].dormant}

    assert len(dynamic) <= EXPECTED_DYNAMIC_SITES, (
        f"{len(dynamic)} strict xfail sites build their reason at runtime, up from "
        f"{EXPECTED_DYNAMIC_SITES}. Each one is a marker core#1025's closed-issue check reads "
        "as absent rather than as unresolved."
    )
    assert len(dynamic) == EXPECTED_DYNAMIC_SITES, (
        f"good news: only {len(dynamic)} dynamic sites remain. Lower EXPECTED_DYNAMIC_SITES to "
        "match, so the ratchet keeps its grip — a floor left above the real number permits the "
        "next regression for free."
    )
    assert len(live) == EXPECTED_LIVE_DYNAMIC_SITES, (
        f"{len(live)} dynamic sites are LIVE (their table has entries), expected "
        f"{EXPECTED_LIVE_DYNAMIC_SITES}. This is the number that decides how many real markers "
        "carry a runtime-built reason, and it is the figure core#1025's coverage claim rests "
        "on — 9 live markers today, 4 readable from the call site alone."
    )


def test_the_cloud_arm_states_its_state_rather_than_skipping() -> None:
    """Cross-repo, and it must never pass by having found nothing to look at.

    Core CI has no cloud checkout, so this cannot be an unconditional assertion. It can still
    refuse to be silent: when the tree is absent that is *recorded*, and when it is present the
    scan runs for real. `pytest.skip` would be the wrong tool — a skip is the same colour as a
    pass, which is the failure mode this whole file is about.
    """
    if not CLOUD_TESTS.exists():
        print(f"cloud tree absent at {CLOUD_TESTS}; core CI has no cloud checkout (expected)")
        return
    problems = unattributed(strict_xfails(CLOUD_TESTS, "cloud"))
    assert not problems, "cloud strict xfails with unreadable attribution:\n  " + "\n  ".join(
        problems
    )


# ---------------------------------------------------------------------------
# In-suite arming. A guard proved discriminating once by an external harness is a
# claim about a past session; this runs every time CI does.
# ---------------------------------------------------------------------------


def test_arming_a_static_reason_naming_no_issue_is_caught() -> None:
    bad = StrictXfail("core", "tests/test_x.py", 1, "the API is flaky, revisit later", None)
    assert unattributed([bad]), (
        "a static reason with no issue token passed the predicate. That is precisely the "
        "marker core#1025's check would skip while reading as coverage."
    )


def test_arming_an_unlisted_dynamic_reason_is_caught() -> None:
    bad = StrictXfail("core", "tests/test_brand_new.py", 1, None, "_reason_for(pin)")
    assert unattributed([bad]), "a dynamic reason from an unlisted file passed the predicate"


def test_arming_the_predicate_is_green_on_the_real_tree() -> None:
    """The control that stops the two arming tests above passing by a predicate that always
    fails. A check narrowed until nothing satisfies it is a worse bug than the one it fixed,
    and a silent one."""
    assert unattributed(_core_markers()) == []


def test_arming_carrier_state_reads_the_real_file_both_ways() -> None:
    """`carrier_state` is what turns dormant/live from a claim into a reading."""
    assert (
        carrier_state("tests/test_deploy/test_alerting_config.py", "KNOWN_VIOLATIONS", REPO_ROOT)
        == "dormant"
    )
    assert (
        carrier_state("tests/test_connector_smoke/conftest.py", "AWAITING_PROVISIONING", REPO_ROOT)
        == "live"
    )
    assert (
        carrier_state("tests/test_deploy/test_alerting_config.py", "NO_SUCH_NAME", REPO_ROOT)
        == "missing"
    )
    assert carrier_state("tests/does_not_exist.py", "ANY", REPO_ROOT) == "missing"


@pytest.mark.parametrize(
    ("relpath", "carrier"),
    [(rel, site.carrier) for (_repo, rel), site in DYNAMIC_SITES.items()],
)
def test_every_listed_carrier_is_a_real_module_level_name(relpath: str, carrier: str) -> None:
    """Named separately from the staleness test so a rename reports the carrier, not the state."""
    assert carrier_state(relpath, carrier, REPO_ROOT) != "missing", (
        f"{relpath} has no module-level `{carrier}`. DYNAMIC_SITES is describing a tree that "
        "no longer exists."
    )


# ===========================================================================
# core#1025, SECOND HALF — read the TABLE, not the call site.
#
# The first half (everything above) pins that every strict xfail's attribution is
# *reachable*. It stops there: for a table-driven site it accepts the site as declared and
# never opens the table. That leaves the closed-issue check able to read **4 of 9** real
# markers — the four static ones — because the other five are built from
# `AWAITING_PROVISIONING` and their issue lives one indirection away, in `pin.tracking`.
#
# 🔑 **The remedy is not an exception list, it is to read the table.** Resolving `**_PIPEDRIVE`
# to the module-level dict it names takes the same check from 4 of 9 to 9 of 9, and it does it
# per ENTRY rather than per table — so a single pin losing its `tracking` reds naming that pin,
# instead of the table as a whole still looking attributed because its four siblings are.
#
# ⚠️ **No count ratchet on entries here, deliberately.** A sixth dead vendor account is
# legitimate work; a guard that reds on it would be deleted, and correctly. What is pinned is
# scale-free: **100% attribution**, plus the claim that reading the table strictly beats reading
# the call site — if that ever stops being true this half is doing nothing and should be removed
# on purpose rather than left as decoration.
# ===========================================================================


@dataclass(frozen=True)
class Marker:
    """One strict xfail marker that is actually APPLIED — not one place a marker is written.

    The unit error core#1025's first half corrected, now carried in the type: a call site
    inside a `.get()` on an empty dict emits nothing, and five markers come out of one site.
    """

    relpath: str
    #: `None` for a static marker; the table key for a table-driven one.
    entry: str | None
    tokens: frozenset[str]
    #: True when the token was only reachable by following the table.
    via_table: bool

    @property
    def label(self) -> str:
        return f"{self.relpath}" + (f"[{self.entry}]" if self.entry else "")


def module_bindings(tree: ast.Module) -> dict[str, ast.expr]:
    """Module-level `NAME = <expr>` bindings, annotated or not.

    Only module level: a name rebound inside a function is not what a `**SPREAD` at module
    level resolves to, and following it would invent attribution that is not there.
    """
    out: dict[str, ast.expr] = {}
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value:
            out[node.target.id] = node.value
        elif isinstance(node, ast.Assign) and node.value:
            for target in node.targets:
                if isinstance(target, ast.Name):
                    out[target.id] = node.value
    return out


def tokens_reachable_from(node: ast.AST, binds: dict[str, ast.expr], depth: int = 0) -> set[str]:
    """Every `core#N`/`cloud#N` token reachable from an expression.

    Follows `Name` references and `**SPREAD` into module-level bindings, which is the whole
    point — `AwaitingProvisioning(**_PIPEDRIVE, raises=AssertionError)` carries its issue in
    `_PIPEDRIVE["tracking"]` and nowhere else.

    `depth` is bounded so a self-referential binding cannot hang the suite. A guard that can be
    made to spin is a guard someone disables.
    """
    if depth > 6:
        return set()
    found: set[str] = set()
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return set(ISSUE_TOKEN.findall(node.value))
    if isinstance(node, ast.Name):
        target = binds.get(node.id)
        return tokens_reachable_from(target, binds, depth + 1) if target is not None else set()
    for child in ast.iter_child_nodes(node):
        found |= tokens_reachable_from(child, binds, depth + 1)
    if isinstance(node, ast.Call):
        # `**SPREAD` is a keyword with `arg=None`, which `iter_child_nodes` does reach, but
        # spelling it out keeps the intent legible: this line is the second half.
        for kw in node.keywords:
            found |= tokens_reachable_from(kw.value, binds, depth + 1)
    return found


def table_markers(relpath: str, carrier: str, repo_root: Path) -> list[Marker]:
    """Expand one table-driven site into the markers it actually applies, one per entry."""
    path = repo_root / relpath
    if not path.exists():
        return []
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", SyntaxWarning)
        tree = ast.parse(path.read_text(encoding="utf-8"))
    binds = module_bindings(tree)
    table = binds.get(carrier)
    if not isinstance(table, ast.Dict):
        return []
    out: list[Marker] = []
    for key, value in zip(table.keys, table.values, strict=False):
        name = (
            key.value
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
            else (ast.unparse(key) if key is not None else "<**spread>")
        )
        out.append(
            Marker(
                relpath=relpath,
                entry=str(name),
                tokens=frozenset(tokens_reachable_from(value, binds)),
                via_table=True,
            )
        )
    return out


def applied_markers(repo_root: Path = REPO_ROOT) -> list[Marker]:
    """THE CENSUS. Every strict xfail marker the tree applies, with its issue tokens."""
    out: list[Marker] = []
    for site in strict_xfails(repo_root / "tests", "core"):
        if not site.is_dynamic:
            out.append(
                Marker(
                    relpath=site.relpath,
                    entry=None,
                    tokens=frozenset(ISSUE_TOKEN.findall(site.static_reason or "")),
                    via_table=False,
                )
            )
            continue
        declared = DYNAMIC_SITES.get(site.key)
        if declared is None:
            # The first half already reds on this; emit an unattributed marker so this half
            # cannot report full coverage on a tree the other half is failing.
            out.append(Marker(site.relpath, "<undeclared site>", frozenset(), True))
            continue
        out.extend(table_markers(site.relpath, declared.carrier, repo_root))
    return out


def unresolved(markers: list[Marker]) -> list[str]:
    """THE PREDICATE for the second half. Markers whose issue is still not discoverable."""
    return [
        f"{m.label} applies xfail(strict=True) but no core#N/cloud#N token is reachable from "
        "it — not in its reason and not in the table entry it is built from. core#1025's "
        "closed-issue check skips it silently, which is the same colour as an issue still "
        "being open."
        for m in markers
        if not m.tokens
    ]


def test_control_the_marker_census_is_not_empty_and_covers_both_kinds() -> None:
    """A census that found nothing would make every assertion below vacuously true."""
    markers = applied_markers()
    assert markers, "the marker census is empty; the scanner or the resolver has gone blind"
    assert any(not m.via_table for m in markers), "no static marker in the census"
    assert any(m.via_table for m in markers), (
        "no table-driven marker in the census. That is the half this section exists for, so a "
        "census without one is not exercising it — and would report 100% coverage trivially."
    )


def test_reading_the_table_resolves_every_applied_marker() -> None:
    """THE ASSERTION: 9 of 9 today, N of N always — attribution reachable for every marker."""
    markers = applied_markers()
    problems = unresolved(markers)
    assert not problems, (
        f"{len(problems)} of {len(markers)} applied strict xfail markers have no reachable "
        "issue:\n  " + "\n  ".join(problems)
    )


def test_reading_the_table_strictly_beats_reading_the_call_site() -> None:
    """The claim that justifies this half existing at all — measured, not asserted.

    If table resolution ever stops adding attribution, delete this section deliberately rather
    than leaving it as a check that cannot discriminate.
    """
    markers = applied_markers()
    from_call_site = sum(1 for m in markers if m.tokens and not m.via_table)
    from_table = sum(1 for m in markers if m.tokens)
    print(
        f"strict xfail markers applied: {len(markers)}; "
        f"attributable from the call site alone: {from_call_site}; "
        f"attributable by reading the table: {from_table}"
    )
    assert from_table > from_call_site, (
        f"reading the table resolved {from_table} markers and the call sites alone resolve "
        f"{from_call_site} — so this half adds nothing. Either every dynamic table has been "
        "emptied (delete this section and the DYNAMIC_SITES entries with it) or the resolver "
        "has stopped following `**SPREAD`, in which case coverage above is passing for the "
        "wrong reason."
    )


def test_arming_the_resolver_on_the_real_live_table() -> None:
    """Drives the REAL `AWAITING_PROVISIONING`, not a fixture.

    Its five pins all carry `tracking="cloud#160"` through a `**_VENDOR` spread, so this is the
    exact indirection the second half was built to cross. A synthetic fixture would pass with a
    resolver that only handled the shape the fixture used.
    """
    live = table_markers(
        "tests/test_connector_smoke/conftest.py", "AWAITING_PROVISIONING", REPO_ROOT
    )
    assert live, "the real live table resolved to no markers; the resolver is not reading it"
    assert unresolved(live) == [], f"real pins with no reachable issue: {unresolved(live)}"
    assert all("cloud#" in t or "core#" in t for m in live for t in m.tokens)


def test_arming_a_table_entry_that_loses_its_issue_is_caught_per_entry() -> None:
    """The mutation that matters, and it must name the ONE pin — not the table.

    A predicate asking "does this table mention an issue anywhere?" stays green when a single
    pin loses its tracking, because four siblings still carry `cloud#160`. That is the whole
    reason resolution is per entry.
    """
    src = (
        'GOOD = {"vendor": "V", "tracking": "cloud#160"}\n'
        'BARE = {"vendor": "W"}\n'
        "TABLE = {\n"
        '    "test_a": Pin(**GOOD, raises=AssertionError),\n'
        '    "test_b": Pin(**BARE, raises=None),\n'
        "}\n"
    )
    fixture = CORE_TESTS / "test_deploy" / "_table_resolver_fixture.py"
    fixture.write_text(src, encoding="utf-8")
    try:
        markers = table_markers(f"tests/test_deploy/{fixture.name}", "TABLE", REPO_ROOT)
        assert [m.entry for m in markers] == ["test_a", "test_b"]
        problems = unresolved(markers)
        assert len(problems) == 1, (
            f"expected exactly the one untracked pin to be reported, got {problems}. A "
            "table-level check would have reported none of them."
        )
        assert "test_b" in problems[0] and "test_a" not in problems[0]
    finally:
        fixture.unlink(missing_ok=True)


def test_arming_the_resolver_does_not_follow_a_function_local_binding() -> None:
    """Attribution must come from module level; inventing it from a local is worse than none."""
    src = (
        "def build():\n"
        '    LOCAL = {"tracking": "core#1"}\n'
        "    return LOCAL\n"
        'TABLE = {"test_a": Pin(**LOCAL)}\n'
    )
    fixture = CORE_TESTS / "test_deploy" / "_local_binding_fixture.py"
    fixture.write_text(src, encoding="utf-8")
    try:
        markers = table_markers(f"tests/test_deploy/{fixture.name}", "TABLE", REPO_ROOT)
        assert markers and unresolved(markers), (
            "the resolver resolved a token out of a FUNCTION-LOCAL binding. `**LOCAL` at "
            "module level is a NameError at import, so any attribution read from it is "
            "fictional — and a guard that manufactures coverage is worse than one that admits "
            "it has none."
        )
    finally:
        fixture.unlink(missing_ok=True)
