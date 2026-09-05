"""Every strict xfail must name the issue it is pinning, readably (core#1025, first half).

A `pytest.mark.xfail(strict=True)` is an assertion that *"this is broken, and CI must fail if
it stops being broken."* core#896 was closed COMPLETED while four of them named the very defect
it was closed for, and nobody read them — a suite reporting `6 passed, 4 xfailed` is the same
colour as one reporting `10 passed`.

core#1025's second half is a scheduled check that reads issue numbers out of `reason=` strings
and refuses to let a strict xfail outlive a CLOSED issue. **This file is its prerequisite**, and
the reason is the part that is easy to skip: that check can only read the reasons it can parse,
and **a marker whose reason it cannot parse is silently skipped — indistinguishable from a
marker whose issue is still open.** So the second half's coverage *is* the set of readable
reasons, and until this file existed nothing measured that set.

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
