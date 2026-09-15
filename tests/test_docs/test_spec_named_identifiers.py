"""A spec that names a constant is asserting the constant exists (core#1150).

`SPEC_ORG_ROLES` §3.4 said an ownership transfer is *"audited as its own action
(``transfer_ownership``)"*. ``AuditAction`` has no such member, so ``BaseState._audit``'s
``AuditAction(action)`` raised, the deliberate swallow dropped the row, and **for the life of
the product the highest-privilege operation in it wrote no audit history** (core#1127).

🔑 **Nothing there was a typo and no review was wrong.** The implementation was faithful to a
clause that could not be satisfied -- code compliant, review correct, row gone, every check
green. That is the one class of spec defect a diff review cannot catch, because *doing exactly
what the spec says is the failure mode*. It is also mechanically checkable, which is what this
module does.

What is resolved, and against what
----------------------------------

======================  ====================================================================
reference in a spec     artifact that defines it
======================  ====================================================================
``Class.MEMBER``        the enum's members (and the methods it declares)
``Class("value")``      the enum's values
`` `a.b.c` ``           ``datanika/i18n/en.json`` -- but only after the reference survives the
                        disambiguation below, because a dotted lowercase token is also how
                        this corpus writes filenames, DB columns, settings, hostnames and
                        attribute paths
======================  ====================================================================

Product measured those false-positive classes on the real corpus before this guard existed
(core#1150, 2026-09-11): **7 of 10 raw i18n-shaped hits were filenames, DB columns or settings
attributes**. Reporting those as spec defects is how a guard gets switched off -- three false
alarms and the fourth, real one reads as noise. So each class resolves against its own artifact
rather than being pattern-matched away:

* a **filename** ends in a source extension, or matches a file in the tree;
* a **DB column** resolves on the model whose ``__tablename__`` the first segment names;
* a **settings attribute** resolves on ``Settings``;
* an **attribute path** (``app.add_page``, ``session.new``) appears as a real attribute chain in
  the package source;
* a **hostname** ends in a public suffix (``app.datanika.io``).

Two more classes come from the corpus rather than from theory:

* a **declaration**: a table row ``| `key` | English |`` or ``key = "text"`` is a spec asking
  for a key to be created, not claiming one exists. Unimplemented is a backlog item; the clause
  is satisfiable, which is the line this guard draws.
* an **obituary**: prose naming an identifier in order to record that it is gone --
  *"**Dropped** ``PipelineMode.AUTO``"*, *"``auth.current_password`` **does not exist**"*, or a
  quotation of the wording a correction replaced. core#1150's own acceptance criterion asks for
  this, and Engineering measured why: a substring match cannot tell an assertion from its own
  obituary.

⚠️ **The obituary rule is deliberately token-adjacent, not paragraph-wide.** A correction
paragraph is exactly where the *corrected* identifier gets written down, so exempting the whole
paragraph would hide a typo in the fix -- the failure mode the rule exists to prevent, one step
later. ``test_the_obituary_exemption_does_not_swallow_its_paragraph`` drives that case.

What is NOT resolved, stated rather than implied
------------------------------------------------

Metric names, env vars and route paths from core#1150's table: two classes measured beats five
claimed. **Bare enum values are not resolved either** -- and that is the form the founding
defect took: ``transfer_ownership`` in prose, with no class name attached, is indistinguishable
from a method name without guessing. What this guard would have caught is the *qualified* form;
what closes the bare form is a convention (name the class) rather than a resolver.

Whether a particular clause is *right* is also out of scope. This finds clauses that are
**unsatisfiable**, which is strictly smaller and fully mechanical.
"""

from __future__ import annotations

import ast
import enum
import importlib
import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE = REPO_ROOT / "datanika"
SPEC_DIR = REPO_ROOT / "docs" / "specs"
EN_JSON = PACKAGE / "i18n" / "en.json"

_ENUM_BASES = {"Enum", "StrEnum", "IntEnum", "Flag", "IntFlag"}

#: Measurements taken on ``origin/dev d79c46f`` (2026-09-15), not numbers anybody chose. They
#: exist because every assertion below is satisfied by finding nothing: a walker that stops
#: seeing the package reports a corpus with no defects, which is the same colour as a clean one.
#:
#: ⚠️ If you legitimately delete an enum, a key or a spec, lower the figure **in the same diff**
#: as the deletion. That is the cost of the exemption being visible, and it is the point.
FLOOR_ENUM_CLASSES = 21
FLOOR_ENUM_MEMBERS = 128
FLOOR_I18N_KEYS = 753
FLOOR_SPECS = 28
FLOOR_RESOLVED_REFERENCES = 190

#: Unresolved references that are REAL findings, each waiting on a spec correction by its owner.
#: An entry is an admission, not an exemption: it says the corpus asserts something untrue today.
#:
#: 🚨 Keyed by (spec, token) rather than by line, because line numbers drift and a stale entry
#: would silently exempt a different clause. ``test_no_accounted_entry_has_gone_stale`` fails
#: when one of these starts resolving, so a fix removes its own entry.
ACCOUNTED_UNRESOLVED: dict[tuple[str, str], str] = {
    (
        "SPEC_DUAL_MODE_UX.md",
        "PipelineMode.AUTO",
    ): "§9.1's PipelineTemplate code block still defaults to a member the same spec's 2026-04-15 "
    "reconciliation note dropped. Product's: update the block or mark it superseded (core#1150).",
    (
        "SPEC_DUAL_MODE_UX.md",
        "pipelines.volume_estimate_gb",
    ): "the migration bullet claims a column the reconciliation note above it dropped in favour "
    "of Upload.volume_estimate_gb. Product's (core#1150).",
    (
        "SPEC_DUAL_MODE_UX.md",
        "pipelines.elt_nudge_dismissed_at",
    ): "same bullet; the column lives on pipeline_prefs per §8 of the same spec, not on "
    "pipelines. Product's (core#1150).",
    (
        "SPEC_PASSWORD_RESET.md",
        "account.set_password",
    ): "the variant table names a key en.json does not hold; settings.py renders "
    "auth.set_password for that button. Same shape as the auth.current_password defect "
    "PR #1271 corrected. Product's (core#1150).",
}

_QUALIFIED = re.compile(r"\b([A-Z][A-Za-z0-9]+)\.([A-Za-z_][A-Za-z0-9_]*)\b")
_CALL_VALUE = re.compile(r"\b([A-Z][A-Za-z0-9]+)\(\s*[\"']([^\"']+)[\"']\s*\)")
_BACKTICKED = re.compile(r"`([^`\n]+)`")
_I18N_SHAPE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z0-9_]+)+$")
_HOSTNAME = re.compile(r"^[a-z0-9-]+(?:\.[a-z0-9-]+)+\.(?:io|com|org|net|dev|cloud|pro|gr|co|ai)$")
_SOURCE_EXTENSIONS = {
    "py",
    "md",
    "json",
    "ts",
    "tsx",
    "yml",
    "yaml",
    "toml",
    "sh",
    "js",
    "html",
    "txt",
    "sql",
    "astro",
    "env",
    "lock",
    "cfg",
    "ini",
}

#: Wording that marks the identifier right beside it as gone. Adjacency is the whole design:
#: see the module docstring on why a paragraph-wide rule is wrong.
_GONE_BEFORE = (
    "dropped",
    "removed",
    "deleted",
    "retired",
    "renamed",
    "withdrawn",
    "we proposed",
    "proposed",
    "used to read",
    "it read",
    "previously read",
    "formerly",
    "superseded",
)
_GONE_AFTER = (
    "does not exist",
    "doesn't exist",
    "is not a member",
    "is not one of",
    "no longer exists",
    "was dropped",
    "was removed",
    "was renamed",
    "holds zero",
    "holds 0",
)
_OLD_WORDING = ("used to read", "it read:", "previously read")


class Finding(tuple):
    """(spec, line, token, clause, artifact) -- a reference resolving against nothing."""

    __slots__ = ()

    def __new__(cls, spec: str, line: int, token: str, clause: str, artifact: str):
        return super().__new__(cls, (spec, line, token, clause, artifact))

    spec = property(lambda self: self[0])
    line = property(lambda self: self[1])
    token = property(lambda self: self[2])
    clause = property(lambda self: self[3])
    artifact = property(lambda self: self[4])


def _base_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    return node.attr if isinstance(node, ast.Attribute) else None


def _package_trees() -> dict[str, ast.Module]:
    trees = {}
    for py in sorted(PACKAGE.rglob("*.py")):
        if "__pycache__" in py.parts:
            continue
        trees[py.relative_to(REPO_ROOT).as_posix()] = ast.parse(
            py.read_bytes().decode("utf-8"), filename=str(py)
        )
    return trees


def enum_vocabulary(trees: dict[str, ast.Module]) -> dict[str, dict]:
    """Every Enum subclass in the package, by AST: members, values, declared methods."""
    classes: dict[str, list[tuple[str, ast.ClassDef]]] = {}
    for rel, tree in trees.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                classes.setdefault(node.name, []).append((rel, node))

    names: set[str] = set()
    changed = True
    while changed:  # a subclass of one of ours is one of ours
        changed = False
        for name, defs in classes.items():
            if name in names:
                continue
            for _rel, cd in defs:
                bases = {_base_name(b) for b in cd.bases}
                if bases & _ENUM_BASES or bases & names:
                    names.add(name)
                    changed = True
                    break

    vocabulary: dict[str, dict] = {}
    for name in names:
        for rel, cd in classes[name]:
            members, values, methods = set(), set(), set()
            for stmt in cd.body:
                if (
                    isinstance(stmt, ast.Assign)
                    and len(stmt.targets) == 1
                    and isinstance(stmt.targets[0], ast.Name)
                    and not stmt.targets[0].id.startswith("_")
                ):
                    members.add(stmt.targets[0].id)
                    if isinstance(stmt.value, ast.Constant):
                        values.add(stmt.value.value)
                elif isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef):
                    methods.add(stmt.name)
            vocabulary[name] = {
                "file": rel,
                "members": members,
                "values": values,
                "methods": methods,
            }
    return vocabulary


def runtime_enum_vocabulary() -> dict[str, set[str]]:
    """The same vocabulary, obtained by IMPORTING instead of parsing.

    Every package module whose source mentions ``Enum`` is imported -- a superset filter, so a
    class the AST walk misreads is still imported -- and the runtime is asked which classes are
    Enum subclasses defined under ``datanika.``. An import failure is raised, never skipped: a
    module that cannot be imported is one whose enums are invisible, which reads as agreement.
    """
    for py in sorted(PACKAGE.rglob("*.py")):
        rel = py.relative_to(REPO_ROOT)
        if "__pycache__" in rel.parts or "migrations" in rel.parts:
            continue
        if b"Enum" not in py.read_bytes():
            continue
        importlib.import_module(".".join(rel.with_suffix("").parts))

    found: dict[str, set[str]] = {}
    seen: set[type] = set()
    stack = list(enum.Enum.__subclasses__())
    while stack:
        cls = stack.pop()
        if cls in seen:
            continue
        seen.add(cls)
        stack.extend(cls.__subclasses__())
        if cls.__module__.startswith("datanika."):
            found[cls.__name__] = {member.name for member in cls}
    return found


def attribute_chains(trees: dict[str, ast.Module]) -> set[str]:
    """Dotted attribute paths the package actually writes: ``app.add_page``, ``session.new``."""
    chains: set[str] = set()
    for tree in trees.values():
        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute):
                continue
            parts: list[str] = []
            cur: ast.AST = node
            while isinstance(cur, ast.Attribute):
                parts.append(cur.attr)
                cur = cur.value
            if isinstance(cur, ast.Name):
                parts.append(cur.id)
                ordered = list(reversed(parts))
                for size in range(2, len(ordered) + 1):
                    chains.add(".".join(ordered[:size]))
    return chains


def model_columns(trees: dict[str, ast.Module]) -> tuple[dict[str, set[str]], set[str]]:
    tables: dict[str, set[str]] = {}
    mixins: set[str] = set()
    for rel, tree in trees.items():
        if not rel.startswith("datanika/models/"):
            continue
        for cd in [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]:
            table, columns = None, set()
            for stmt in cd.body:
                if (
                    isinstance(stmt, ast.Assign)
                    and isinstance(stmt.value, ast.Constant)
                    and any(
                        isinstance(t, ast.Name) and t.id == "__tablename__" for t in stmt.targets
                    )
                ):
                    table = stmt.value.value
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                    columns.add(stmt.target.id)
            if table:
                tables.setdefault(table, set()).update(columns)
            if "Mixin" in cd.name:
                mixins |= columns
    return tables, mixins


def settings_fields(trees: dict[str, ast.Module]) -> set[str]:
    fields: set[str] = set()
    for cd in [
        n
        for n in ast.walk(trees["datanika/config.py"])
        if isinstance(n, ast.ClassDef) and n.name == "Settings"
    ]:
        fields |= {
            s.target.id
            for s in cd.body
            if isinstance(s, ast.AnnAssign) and isinstance(s.target, ast.Name)
        }
    return fields


class Corpus:
    """Everything a reference can resolve against, gathered once."""

    def __init__(self) -> None:
        self.trees = _package_trees()
        self.enums = enum_vocabulary(self.trees)
        self.keys = set(json.loads(EN_JSON.read_bytes().decode("utf-8")))
        self.namespaces = {key.split(".", 1)[0] for key in self.keys}
        self.chains = attribute_chains(self.trees)
        self.tables, self.mixin_columns = model_columns(self.trees)
        self.settings = settings_fields(self.trees)
        self.filenames = {path.name for path in REPO_ROOT.rglob("*") if path.is_file()} | {
            rel.rsplit("/", 1)[-1] for rel in self.trees
        }
        self.resolved = 0

    def specs(self) -> list[tuple[str, str]]:
        return [
            (path.name, path.read_bytes().decode("utf-8"))
            for path in sorted(SPEC_DIR.glob("SPEC_*.md"))
        ]


def _paragraphs(text: str) -> list[tuple[int, list[str]]]:
    blocks: list[tuple[int, list[str]]] = []
    current: list[str] = []
    start = 1
    for number, line in enumerate(text.splitlines(), 1):
        if line.strip() in ("", ">"):
            if current:
                blocks.append((start, current))
                current = []
        else:
            if not current:
                start = number
            current.append(line)
    if current:
        blocks.append((start, current))
    return blocks


def _plain(text: str) -> str:
    return text.replace("*", "").replace("`", "").replace(">", "").lower()


def _is_obituary(paragraph: str, span: tuple[int, int]) -> bool:
    """Is the identifier at ``span`` named in order to say it is GONE?"""
    before = _plain(paragraph[max(0, span[0] - 90) : span[0]]).rstrip(" (:-,—")
    after = _plain(paragraph[span[1] : span[1] + 90]).lstrip(" ):-,.—")
    if any(before.endswith(cue) for cue in _GONE_BEFORE):
        return True
    if any(after.startswith(cue) for cue in _GONE_AFTER):
        return True
    if any(marker in _plain(paragraph) for marker in _OLD_WORDING):
        quotes = [m.start() for m in re.finditer('"', paragraph)]
        for opening, closing in zip(quotes[0::2], quotes[1::2], strict=False):
            if opening < span[0] and span[1] < closing:
                return True
    return False


def _is_declaration(line: str, token: str) -> bool:
    """A spec asking for a key to be CREATED is not claiming it exists."""
    quoted = re.escape("`" + token + "`")
    return bool(
        re.match(r"^\s*\|\s*" + quoted + r"\s*\|", line)
        or re.search(quoted + r"\s*=\s*[\"“]", line)
    )


def unresolved_references(name: str, text: str, corpus: Corpus) -> list[Finding]:
    """Every identifier this spec names that resolves against nothing."""
    findings: list[Finding] = []
    for start, lines in _paragraphs(text):
        paragraph = "\n".join(lines)
        for match in _QUALIFIED.finditer(paragraph):
            cls, member = match.group(1), match.group(2)
            if cls not in corpus.enums:
                continue
            entry = corpus.enums[cls]
            known = entry["members"] | entry["methods"] | {"value", "name"}
            if member in known:
                corpus.resolved += 1
                continue
            if _is_obituary(paragraph, match.span()):
                continue
            line = start + paragraph[: match.start()].count("\n")
            findings.append(
                Finding(
                    name,
                    line,
                    f"{cls}.{member}",
                    lines[line - start].strip()[:160],
                    f"{entry['file']}::{cls} -- members: {sorted(entry['members'])}",
                )
            )
        for match in _CALL_VALUE.finditer(paragraph):
            cls, value = match.group(1), match.group(2)
            if cls not in corpus.enums:
                continue
            if value in corpus.enums[cls]["values"]:
                corpus.resolved += 1
                continue
            if _is_obituary(paragraph, match.span()):
                continue
            line = start + paragraph[: match.start()].count("\n")
            findings.append(
                Finding(
                    name,
                    line,
                    f'{cls}("{value}")',
                    lines[line - start].strip()[:160],
                    f"{corpus.enums[cls]['file']}::{cls} -- values: "
                    f"{sorted(corpus.enums[cls]['values'])}",
                )
            )
        for match in _BACKTICKED.finditer(paragraph):
            token = match.group(1).strip()
            if not _I18N_SHAPE.match(token):
                continue
            line = start + paragraph[: match.start()].count("\n")
            raw = lines[line - start]
            head, tail = token.split(".", 1)
            if token in corpus.keys:
                corpus.resolved += 1
                continue
            # Disambiguation, each against the artifact that defines that class.
            if _HOSTNAME.match(token):
                continue
            if token.rsplit(".", 1)[-1] in _SOURCE_EXTENSIONS or token in corpus.filenames:
                continue
            if token in corpus.chains:
                continue
            if head in corpus.tables and tail in (corpus.tables[head] | corpus.mixin_columns):
                continue
            if head == "settings" and tail in corpus.settings:
                continue
            if head not in corpus.namespaces:
                continue  # not an i18n claim at all
            if _is_declaration(raw, token) or _is_obituary(paragraph, match.span()):
                continue
            findings.append(
                Finding(
                    name,
                    line,
                    token,
                    raw.strip()[:160],
                    f"datanika/i18n/en.json -- {len(corpus.keys)} keys, none named {token!r}",
                )
            )
    return findings


@pytest.fixture(scope="module")
def corpus() -> Corpus:
    return Corpus()


@pytest.fixture(scope="module")
def findings(corpus: Corpus) -> list[Finding]:
    out: list[Finding] = []
    for name, text in corpus.specs():
        out.extend(unresolved_references(name, text, corpus))
    return out


def test_the_two_enum_walks_agree(corpus: Corpus) -> None:
    """🔑 The control that makes every resolution below mean something.

    A class missing from the vocabulary does not produce a false alarm -- it produces SILENCE,
    because ``Foo.BAR`` is then indistinguishable from a method call and is skipped. So a
    partial walker reads exactly like a clean corpus. Parsing and importing are independent
    instruments; if they disagree, the vocabulary is not the real one.
    """
    parsed = {name: entry["members"] for name, entry in corpus.enums.items()}
    imported = runtime_enum_vocabulary()
    assert parsed and imported, "one of the two walks found no enums at all"
    assert set(parsed) == set(imported), (
        "the AST walk and the import walk disagree about which enums exist:\n"
        f"  only parsed:   {sorted(set(parsed) - set(imported))}\n"
        f"  only imported: {sorted(set(imported) - set(parsed))}"
    )
    differing = {
        name: sorted(parsed[name] ^ imported[name])
        for name in parsed
        if parsed[name] != imported[name]
    }
    assert not differing, f"the two walks disagree about members: {differing}"


def test_the_vocabulary_is_not_empty_or_collapsed(corpus: Corpus) -> None:
    """Anti-vacuity, with floors that are measurements rather than opinions."""
    members = sum(len(entry["members"]) for entry in corpus.enums.values())
    assert len(corpus.enums) >= FLOOR_ENUM_CLASSES, (
        f"{len(corpus.enums)} enum classes found, floor {FLOOR_ENUM_CLASSES}. Either the walk "
        "broke or enums were deleted -- if deleted, lower the floor in that same diff."
    )
    assert members >= FLOOR_ENUM_MEMBERS, f"{members} enum members, floor {FLOOR_ENUM_MEMBERS}"
    assert len(corpus.keys) >= FLOOR_I18N_KEYS, (
        f"{len(corpus.keys)} keys in en.json, floor {FLOOR_I18N_KEYS}"
    )
    assert len(corpus.specs()) >= FLOOR_SPECS, f"{len(corpus.specs())} specs, floor {FLOOR_SPECS}"


def test_the_extractor_actually_resolves_references(
    corpus: Corpus, findings: list[Finding]
) -> None:
    """A regex that stops matching reports a perfectly clean corpus.

    This counts the references that RESOLVED, so it cannot be satisfied by an extractor that
    finds nothing -- the failure mode every other assertion here shares.
    """
    assert corpus.resolved >= FLOOR_RESOLVED_REFERENCES, (
        f"only {corpus.resolved} spec references resolved, floor {FLOOR_RESOLVED_REFERENCES}. "
        "The extractor has probably stopped matching; a zero here is indistinguishable from a "
        "corpus with no defects."
    )


def test_every_identifier_a_spec_names_resolves(findings: list[Finding]) -> None:
    """The assertion this module exists for."""
    unaccounted = [f for f in findings if (f.spec, f.token) not in ACCOUNTED_UNRESOLVED]
    assert not unaccounted, (
        "a spec names an identifier that does not exist -- an implementer following the clause "
        "writes code that cannot work, and the review compares it against the same clause:\n"
        + "\n".join(
            f"  {f.spec}:{f.line}  {f.token}\n"
            f"      clause:   {f.clause}\n"
            f"      resolved against: {f.artifact}"
            for f in unaccounted
        )
        + "\n\nFix the spec, or -- if the clause is historical -- say so beside the identifier "
        "('Dropped `X`', '`X` does not exist'), which is how the corpus already records removals."
    )


def test_no_accounted_entry_has_gone_stale(findings: list[Finding]) -> None:
    """An entry that now resolves is an exemption nobody will revisit.

    ⚠️ This is the half that makes ACCOUNTED_UNRESOLVED a ratchet rather than a drawer: a spec
    correction removes its own entry, and cannot be merged while the entry stands.
    """
    live = {(f.spec, f.token) for f in findings}
    stale = sorted(key for key in ACCOUNTED_UNRESOLVED if key not in live)
    assert not stale, (
        "these accounted entries now resolve (or their clause is gone) -- delete them from "
        f"ACCOUNTED_UNRESOLVED in the same diff as the fix: {stale}"
    )


# -- arming: every rule below is driven with REAL spec text, mutated in memory ----------------


def _spec(name: str) -> str:
    return (SPEC_DIR / name).read_bytes().decode("utf-8")


def _tokens(findings: list[Finding]) -> set[str]:
    return {f.token for f in findings}


def test_a_nonexistent_enum_member_in_a_real_spec_is_caught(corpus: Corpus) -> None:
    """core#1150's acceptance: armed by mutating a real SPEC, not a synthetic fixture."""
    text = _spec("SPEC_ORG_ROLES.md")
    assert "`MemberRole.OWNER`" in text, "the anchor is gone; this test now asserts nothing"
    clean = unresolved_references("SPEC_ORG_ROLES.md", text, corpus)
    assert "MemberRole.OWNER" not in _tokens(clean), "a real member was reported as missing"

    mutated = unresolved_references(
        "SPEC_ORG_ROLES.md", text.replace("`MemberRole.OWNER`", "`MemberRole.OWNERR`", 1), corpus
    )
    caught = [f for f in mutated if f.token == "MemberRole.OWNERR"]
    assert caught, "a spec naming a member that does not exist was not reported"
    assert "MemberRole" in caught[0].artifact and "OWNER" in caught[0].artifact, (
        "the failure does not name the artifact it could not resolve against"
    )
    assert caught[0].line > 0 and caught[0].clause, "the failure names no clause"


def test_the_real_obituary_is_exempt_and_the_exemption_is_what_holds_it(corpus: Corpus) -> None:
    """🚨 The control that separates 'exempt' from 'never seen'.

    ``PipelineMode.AUTO`` inside *"Dropped `PipelineMode.AUTO` from the persisted enum"* must
    not be reported. But a guard whose extractor simply missed the token would look identical.
    So the same text is run with the cue word removed: the reference must then appear.
    """
    text = _spec("SPEC_DUAL_MODE_UX.md")
    line = "> **Reconciled with Eng (2026-04-15)**: Dropped `PipelineMode.AUTO`"
    assert line in text, "the real obituary clause has changed; re-derive this control"

    at_579 = [
        f
        for f in unresolved_references("SPEC_DUAL_MODE_UX.md", text, corpus)
        if f.token == "PipelineMode.AUTO" and "Reconciled" in f.clause
    ]
    assert not at_579, "the obituary clause was reported as a defect"

    without_cue = unresolved_references(
        "SPEC_DUAL_MODE_UX.md", text.replace(line, line.replace("Dropped ", ""), 1), corpus
    )
    assert [f for f in without_cue if "Reconciled" in f.clause], (
        "with the removal cue gone the same clause is still not reported -- the exemption is "
        "not what was holding it, so this rule has never been shown to do anything"
    )


def test_the_obituary_exemption_does_not_swallow_its_paragraph(corpus: Corpus) -> None:
    """A correction paragraph is where the CORRECTED identifier is written down.

    Exempting the paragraph would hide a typo in the fix -- the same defect one step later. The
    exemption is token-adjacent, so an ordinary assertion inside an obituary paragraph is still
    reported.
    """
    text = _spec("SPEC_DUAL_MODE_UX.md")
    anchor = "Dropped `PipelineMode.AUTO` from the persisted enum."
    assert anchor in text
    injected = text.replace(anchor, anchor + " Use `PipelineMode.HYBRID` instead.", 1)
    caught = [
        f
        for f in unresolved_references("SPEC_DUAL_MODE_UX.md", injected, corpus)
        if f.token == "PipelineMode.HYBRID"
    ]
    assert caught, (
        "a live clause naming a non-existent member went unreported because it shares a "
        "paragraph with an obituary -- the exemption is too wide"
    )


def test_a_mistyped_i18n_key_in_prose_is_caught_but_a_declaration_table_is_not(
    corpus: Corpus,
) -> None:
    """The pair that makes the declaration rule honest: ONE token, two contexts.

    PR #1271 corrected `SPEC_PII_SEPARATION` to *"Reuse **`account.current_password`**"*. A typo
    there must be caught. The same spec's key tables declare keys that do not exist yet, and
    those must not be -- a declaration is satisfiable, which is the line this guard draws.
    """
    text = _spec("SPEC_PII_SEPARATION.md")
    assert "`account.current_password`" in text

    clean = _tokens(unresolved_references("SPEC_PII_SEPARATION.md", text, corpus))
    assert "account.current_password" not in clean, "a real key was reported as missing"
    assert "auth.current_password" not in clean, (
        "the correction note quoting the OLD key was reported -- the obituary rule is not "
        "holding the one real i18n obituary in the corpus"
    )
    assert "account.change_email" not in clean, (
        "a key the spec DECLARES in a table was reported as a defect; unimplemented is a "
        "backlog item, not an unsatisfiable clause"
    )

    typo = unresolved_references(
        "SPEC_PII_SEPARATION.md",
        text.replace(
            "Reuse **`account.current_password`**", "Reuse **`account.curent_password`**", 1
        ),
        corpus,
    )
    assert "account.curent_password" in _tokens(typo), (
        "a mistyped key in a REUSE instruction was not reported -- that is the defect class "
        "core#1150 was filed for"
    )


def test_the_false_positive_classes_absorb_what_they_should(corpus: Corpus) -> None:
    """Product measured these on the real corpus: 7 of 10 raw i18n hits were not i18n keys.

    Each line pairs a token that must be absorbed with the artifact that absorbs it, and the
    last pair is the control: a token of the same shape resolving against NOTHING is reported,
    so this test cannot pass by absorbing everything.
    """
    sample = "\n\n".join(
        [
            "A filename: `runs.py` and `uploads.py`.",
            "A column: `connections.config_encrypted` and `runs.error_message`.",
            "A settings attribute: `settings.smtp_host`.",
            "An attribute path: `app.add_page`.",
            "A hostname: `app.datanika.io`.",
            "A key that exists: `account.current_password`.",
            "A key that does not: `runs.no_such_key_at_all`.",
        ]
    )
    reported = _tokens(unresolved_references("SPEC_SAMPLE.md", sample, corpus))
    assert reported == {"runs.no_such_key_at_all"}, (
        "the disambiguation absorbed the wrong set; reported " + repr(sorted(reported))
    )
