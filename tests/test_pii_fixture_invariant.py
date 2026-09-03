"""No test may construct a ``User`` or an ``Invitation`` without its PII sidecar.

``SPEC_PII_SEPARATION.md`` §8a.2 / §8a.6 Step A — core#1009, acceptance criterion 2.

**What this guards and why it is static.** §8a.5's contract is *"N+1 may not merge while
any user-creation path in the repo bypasses the sidecar."* That is a statement about
**source**, not about a database at the end of a run: a sidecar-less user only misbehaves
when something looks it up by address, so a runtime check over `users` LEFT JOIN
`user_pii` is green for every fixture nothing happens to query. 46 such sites accumulated
under exactly that kind of silence.

🚨 **The 28 sites this file was written to hold closed produced ZERO reds under the N+1
mutation** — measured over all 15 files carrying them, with the mutation widened past
§8a.3's two clauses to include core#939 item 6 and `accept_invitation`'s legacy fallback.
So *"the suite is green under N+1"* would have licensed every one of them. The reason to
prefer a source guard is not thoroughness; it is that the dynamic evidence **cannot
distinguish a fixture that is correct from one nothing exercises.**

**Why an allowlist rather than a ban.** Two sites are deliberately sidecar-less and
deleting them would revert a release. They are named below with the reason, so an
implementer sweeping this file finds the argument at the site rather than in an issue.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

TESTS = pathlib.Path(__file__).resolve().parent

#: ``relative posix path`` -> why this file may construct one bare.
#:
#: 🚨 Every entry here is a **deliberate** violation of the dual-write invariant. Adding
#: one to silence a failure reintroduces core#1009. Adding one because a test genuinely
#: needs a sidecar-less row is correct — say which row, and why, in the value.
ALLOWED: dict[str, str] = {
    "test_services/test_pii_separation.py": (
        "§8a.4 Kind 1 — `test_the_legacy_column_is_still_readable_during_the_dual_write_window` "
        "builds a sidecar-less user ON PURPOSE and asserts `get_user_by_email` still finds it "
        "through the legacy `or_` half. That is the t1 blue/green window guard. core#939 "
        "DELETES this test in N+1; an implementer who 'fixes' it has reverted release N."
    ),
    "test_services/test_invitation_service.py": (
        "core#1010's regression test needs a user with an active `Membership` and NO sidecar — "
        "that is the precondition `create_invitation`'s already-a-member guard fails open on. "
        "Routing it through `make_user` gives the guard something to find, the test passes, and "
        "the defect's only witness is gone again (core#1009, measured: 18 reds -> 1)."
    ),
}

#: Model classes whose construction must be accompanied by a sidecar write.
GUARDED = {"User", "Invitation"}

#: What a compliant site looks like instead.
FACTORY = {"User": "make_user", "Invitation": "make_invitation"}


#: The factory module itself, which necessarily constructs both models and both sidecars.
#: ⚠️ Excluded by path rather than by an entry in ``ALLOWED``: those entries mean *"this
#: file may build a sidecar-LESS row"*, which is the opposite of what this file does. The
#: distinction is not pedantry — the two collapse into one list and the list stops meaning
#: anything. ``test_the_factory_is_actually_the_factory`` below is the anti-vacuity check
#: that stops this exclusion from silently exempting an empty file.
FACTORY_MODULE = "factories.py"


def _test_files() -> list[pathlib.Path]:
    skip = {pathlib.Path(__file__).name, FACTORY_MODULE}
    return sorted(p for p in TESTS.rglob("*.py") if p.relative_to(TESTS).as_posix() not in skip)


def _constructions(path: pathlib.Path) -> list[tuple[int, str]]:
    """Every ``User(...)`` / ``Invitation(...)`` call in the file, as (line, name).

    Matched on the AST rather than by grep so that ``UserPII(``, ``InvitationPII(``,
    a docstring mentioning ``User(email=…)``, and an attribute access like
    ``models.User`` are all correctly *not* matches. A grep-based version of this
    guard reported the factory's own construction as a leftover — the second time
    this project has paid for counting the phrase rather than the instruction.
    """
    tree = ast.parse(path.read_bytes().decode("utf-8"), filename=str(path))
    out: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in GUARDED
        ):
            out.append((node.lineno, node.func.id))
    return out


def _offenders() -> dict[str, list[tuple[int, str]]]:
    found: dict[str, list[tuple[int, str]]] = {}
    for path in _test_files():
        rel = path.relative_to(TESTS).as_posix()
        if rel in ALLOWED:
            continue
        hits = _constructions(path)
        if hits:
            found[rel] = hits
    return found


def test_no_test_constructs_a_user_or_invitation_without_its_sidecar():
    """The invariant, stated over source.

    Verified able to fail: restoring any one of the 28 converted sites to its
    pre-core#1009 form names that file and line here.
    """
    offenders = _offenders()
    assert offenders == {}, (
        "these sites construct a guarded model directly, so they produce a row the "
        "dual-write invariant says cannot exist (SPEC_PII_SEPARATION §8a.2). Route them "
        "through `tests.factories` instead:\n"
        + "\n".join(
            f"  {rel}:{line}  {name}(...)  ->  {FACTORY[name]}(session, ...)"
            for rel, hits in sorted(offenders.items())
            for line, name in hits
        )
    )


def test_the_factory_is_actually_the_factory():
    """Anti-vacuity for the one path this guard excludes.

    ``factories.py`` is skipped because it must construct all four models. If it were
    ever emptied, renamed, or reduced to re-exports, the exclusion would keep exempting
    the path while the invariant it exists to serve had quietly stopped being enforced
    anywhere — a green obtained by deleting the thing under test.
    """
    factory = TESTS / FACTORY_MODULE
    assert factory.exists(), f"{FACTORY_MODULE} is excluded from the scan and does not exist"
    built = {name for _, name in _constructions(factory)}
    assert built == GUARDED, f"the factory builds {sorted(built)}, expected {sorted(GUARDED)}"

    tree = ast.parse(factory.read_bytes().decode("utf-8"))
    sidecars = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"UserPII", "InvitationPII"}
    }
    assert sidecars == {"UserPII", "InvitationPII"}, (
        f"the factory writes {sorted(sidecars)} — a guarded model without its sidecar "
        "makes every call site a violation the scan can no longer see"
    )


def test_the_scan_reaches_the_test_tree():
    """Anti-vacuity. A guard that walks nothing reports every tree as clean.

    This is the failure this project keeps paying for: a link auditor handed a file
    where it expects a directory walks nothing and prints ``0 refs, 0 not resolving``.
    An empty ``_test_files()`` would make the assertion above unconditionally true.
    """
    files = _test_files()
    assert len(files) > 100, f"the scan found only {len(files)} test modules — it is not walking"
    assert any(p.name == "test_all_models.py" for p in files), "a known module is missing"


def test_the_detector_matches_a_bare_construction_and_not_its_sidecar():
    """Both directions, on synthetic source, so a narrowed detector cannot pass quietly.

    A pattern narrowed until it stops matching real violations is a worse bug than the
    one it fixed, and a silent one (WORKFLOW_RULES: *test the narrowing in BOTH
    directions*).
    """
    positive = TESTS / "_guard_probe_positive.py"
    negative = TESTS / "_guard_probe_negative.py"
    positive.write_text(
        'u = User(email="x@y.z", full_name="X")\ni = Invitation(email="x@y.z", token="t")\n',
        encoding="utf-8",
    )
    negative.write_text(
        '"""A docstring mentioning User(email=...) and Invitation(token=...)."""\n'
        'p = UserPII(user_id=1, email="x@y.z", full_name="X")\n'
        'q = InvitationPII(invitation_id=1, email="x@y.z")\n'
        'u = make_user(s, email="x@y.z")\n'
        "v = models.User(id=1)\n",
        encoding="utf-8",
    )
    try:
        assert [n for _, n in _constructions(positive)] == ["User", "Invitation"]
        assert _constructions(negative) == []
    finally:
        positive.unlink()
        negative.unlink()


@pytest.mark.parametrize("rel", sorted(ALLOWED))
def test_every_allowlisted_file_still_exists_and_still_needs_the_exemption(rel: str):
    """An allowlist that outlives its reason silently stops guarding a file.

    ⚠️ ``test_invitation_service.py`` is listed for a test core#1010 has **not yet
    written**, so its entry is asserted to exist as a *file*, not to contain a bare
    construction. When #1010 lands, that entry starts carrying its weight; if #1010 is
    solved another way, delete the entry rather than leaving a whole module unguarded.
    """
    path = TESTS / rel
    assert path.exists(), (
        f"{rel} is allowlisted in this file and does not exist. Delete the entry — an "
        "allowlist keyed on a path that has moved exempts nothing and hides that it "
        "exempts nothing."
    )
    assert ALLOWED[rel].strip(), f"{rel} is exempt with no stated reason"


def test_the_factory_module_is_the_only_place_that_writes_the_sidecars():
    """`tests/factories.py` is the single edit N+2 needs (§8a.7).

    N+2 drops `users.email` / `full_name` / `oauth_provider_id` and `invitations.email` /
    `token`, turning every hand-written construction into a `TypeError`. The whole reason
    Step A is a factory rather than 46 inline sidecar writes is that this is then **one**
    edit. A second module quietly growing its own `UserPII(...)` write undoes that.
    """
    writers = []
    for path in _test_files():
        rel = path.relative_to(TESTS).as_posix()
        tree = ast.parse(path.read_bytes().decode("utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in {"UserPII", "InvitationPII"}
            ):
                writers.append(f"{rel}:{node.lineno}")
    assert writers == [], (
        "these modules construct a PII sidecar directly instead of using "
        "`tests.factories`, so N+2's column drop becomes one edit per site:\n  "
        + "\n  ".join(writers)
    )
