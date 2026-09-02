"""Every Telegram alert must be deliverable whatever text gets interpolated (core#926).

What this defends
-----------------
Our alerts are the only thing that turns a silent-green pipeline loud. On 2026-09-01 the
flaky-gating alert fired for the first time and **did not deliver**: HTTP 400,
``can't parse entities``. The issue was filed, the page was not sent, and nobody noticed
because it was a red step inside an already-red job in a run whose conclusion is
``failure`` on every ``dev`` push anyway.

The measurement that matters is not about that one template. Every payload interpolates
text nobody controls -- ``$FIRST_LINE`` (the commit subject) and ``$FLAKY_SPECS``
(Playwright spec titles) -- into a legacy-Markdown payload with no escaping. Legacy
Markdown rejects unbalanced ``_``, ``*`` and ``` ` ``` with a 400.

Measured on 2026-09-02 over the last 400 ``origin/master`` commit subjects:

* **31 of 400 are unbalanced** -- 30 on an underscore, 4 on an asterisk (one subject,
  ``Scrub GIT_* around the probe``, on both). **1 in 12.9.**
* ``deploy-pointer.yml``'s **PROD smoke failed** alert -- the one that pages when
  production breaks -- interpolates ``$FIRST_LINE`` and had a 0-underscore template. Its
  three successes to date were balanced commits. Nothing about the template protected it.

The two firings that exist prove the mechanism at the right unit of analysis, and this is
the part that decides what the guard must check:

===========  ===================================  ========
commit       payload underscores                  result
===========  ===================================  ========
``d4a49ff3``  1 template + 1 from a spec title = 2  200
``11ab292c``  1 template + 0                   = 1  **400**
===========  ===================================  ========

Same template, same job, seven hours apart. The alert that *worked* worked because a
Playwright test happened to be named ``template_selected``, supplying a second underscore
that closed the template's lone one. **So the invariant is over the assembled payload, not
over the template.** A guard that only checked template text would go green while the PROD
alert stayed a coin flip -- the same defect wearing a guard's clothes.

The fix, and why HTML rather than balancing
-------------------------------------------
Legacy Markdown has **no reliable escape**, so "keep Markdown and escape the values" is not
available; and "balance the template's underscores" weights the coin without removing it --
it would still have left the 4 asterisk-unbalanced subjects. Telegram's HTML parse mode has
a **closed, three-character** escape rule (``&``, ``<``, ``>``), so escaping is total and
checkable. Every call site therefore:

1. sends ``parse_mode=HTML``;
2. defines ``esc()``, which escapes ``&`` **first** (escaping ``<`` first would turn
   ``&`` into ``&amp;lt;``);
3. passes **every** interpolated value through it -- uniformly, including values that look
   safe today like ``$RUN_URL``, because "escape the ones that need it" is a judgement
   call at every future call site while "escape everything" is a property a machine can
   check.

The escaping is written inline at each call site rather than shared through a composite
action **on purpose**. These steps run only when something else has already failed, so a
shared action would put action-resolution and checkout-ordering on the one path that must
never fail silently -- trading a 1-in-13 failure for a possible total one. Duplication plus
this guard is the safer trade; the guard is what stops the copies drifting.

Vacuity
-------
The discovery below is derived by globbing, never enumerated, so a tenth call site added
tomorrow is covered without editing this file. Two anti-vacuity assertions matter as much
as the invariant itself: a file that mentions ``api.telegram.org`` but yields no parsed
call site is a **parser failure and goes red**, and the total count has a floor. A guard
that silently stops finding anything is this project's signature defect.

Refs #926, #757, #873.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"

#: Tags we allow a template to contain unescaped. Telegram's HTML mode supports more;
#: this is the set our alerts actually use, kept narrow so a stray ``<`` is a failure.
ALLOWED_TAGS = ("<b>", "</b>", "<i>", "</i>", "<code>", "</code>", "<pre>", "</pre>")

SENDMESSAGE = "api.telegram.org"


def _shell_words(s: str) -> list[str]:
    """Split ``s`` into shell words, respecting quotes and ``$( )`` nesting."""
    words: list[str] = []
    cur = ""
    depth = 0
    inq = False
    i = 0
    while i < len(s):
        c = s[i]
        if c == "$" and s[i : i + 2] == "$(":
            depth += 1
            cur += "$("
            i += 2
            continue
        if c == ")" and depth > 0:
            depth -= 1
            cur += c
            i += 1
            continue
        if c == '"' and depth == 0:
            inq = not inq
            cur += c
            i += 1
            continue
        if c.isspace() and depth == 0 and not inq:
            if cur.strip():
                words.append(cur.strip())
            cur = ""
            i += 1
            continue
        cur += c
        i += 1
    if cur.strip():
        words.append(cur.strip())
    return words


class CallSite:
    """One ``curl ... sendMessage`` invocation, with the shell that assembles its text."""

    def __init__(self, path: Path, line: int, block: str) -> None:
        self.path = path
        self.name = path.name
        self.line = line
        self.block = block

    def __repr__(self) -> str:  # pragma: no cover - test identifiers only
        return f"{self.name}:{self.line}"

    @property
    def parse_mode(self) -> str | None:
        m = re.search(r"parse_mode=([A-Za-z0-9]+)", self.block)
        return m.group(1) if m else None

    @property
    def printf_format(self) -> str:
        """The literal single-quoted printf format that builds ``TEXT``."""
        m = re.search(r"TEXT=\$\(printf\s+'(.*?)'\s", self.block, re.S)
        return m.group(1) if m else ""

    @property
    def printf_args(self) -> list[str]:
        """The argument list handed to that printf, one shell word per entry.

        ⚠️ This needs a real scanner, not a regex. The safe form of an argument is
        ``"$(esc "$VAR")"`` -- double quotes nested inside a command substitution
        that is itself inside double quotes. A regex like ``"[^"]*"`` matches
        ``"$(esc "`` and then reports the trailing ``")"`` as an unescaped value,
        i.e. it flags the *fixed* code as broken. That is the guard's own breakage
        biasing toward the alarming answer rather than the reassuring one, which is
        the only reason it was caught quickly.
        """
        m = re.search(r"TEXT=\$\(printf\s+'.*?'\s", self.block, re.S)
        if not m:
            return []
        tail = self.block[m.end() :]
        end = tail.find(")\n")
        tail = tail[: end if end != -1 else len(tail)]
        return _shell_words(tail.replace("\\\n", " "))


def _split_steps(text: str) -> list[tuple[int, str]]:
    """Split a workflow into ``- name:``-delimited step blocks, keeping 1-based lines."""
    lines = text.splitlines()
    starts = [i for i, ln in enumerate(lines) if re.match(r"\s*-\s+name:", ln)]
    out: list[tuple[int, str]] = []
    for idx, s in enumerate(starts):
        e = starts[idx + 1] if idx + 1 < len(starts) else len(lines)
        out.append((s + 1, "\n".join(lines[s:e])))
    return out


def _discover() -> tuple[list[CallSite], set[str]]:
    """Return every call site, plus the names of files that mention Telegram at all.

    The second value exists so a parser that stops matching produces a failure rather
    than an empty, reassuring pass.
    """
    sites: list[CallSite] = []
    mentions: set[str] = set()
    for path in sorted(WORKFLOWS.glob("*.y*ml")):
        text = path.read_text(encoding="utf-8")
        if SENDMESSAGE not in text:
            continue
        mentions.add(path.name)
        for line, block in _split_steps(text):
            if SENDMESSAGE in block:
                sites.append(CallSite(path, line, block))
    return sites, mentions


CALL_SITES, MENTIONING_FILES = _discover()


# --------------------------------------------------------------------------------------
# Anti-vacuity. These run first and are the reason a green here means something.
# --------------------------------------------------------------------------------------


def test_the_workflow_directory_is_where_we_think_it_is() -> None:
    assert WORKFLOWS.is_dir(), WORKFLOWS
    assert list(WORKFLOWS.glob("*.y*ml")), f"no workflows under {WORKFLOWS}"


def test_every_file_mentioning_telegram_yields_at_least_one_parsed_call_site() -> None:
    """A file we cannot parse must go RED, never quietly contribute nothing."""
    parsed = {s.name for s in CALL_SITES}
    unparsed = MENTIONING_FILES - parsed
    assert not unparsed, (
        f"these workflows call {SENDMESSAGE} but no call site was parsed out of them: "
        f"{sorted(unparsed)} -- the discovery in this guard has stopped working, which "
        f"would otherwise read as 'nothing to check'"
    )


def test_call_site_count_has_a_floor() -> None:
    """Nine existed when this landed. Fewer means discovery broke, not that alerts left."""
    assert len(CALL_SITES) >= 9, [repr(s) for s in CALL_SITES]


def test_every_call_site_yielded_a_printf_format() -> None:
    empty = [repr(s) for s in CALL_SITES if not s.printf_format]
    assert not empty, f"could not extract the payload template for: {empty}"


def test_every_call_site_yielded_at_least_one_printf_argument() -> None:
    """No payload here is argument-free; zero args would mean the scanner gave up."""
    empty = [repr(s) for s in CALL_SITES if not s.printf_args]
    assert not empty, f"parsed no printf arguments for: {empty}"


def test_the_argument_scanner_handles_the_nested_form_it_exists_for() -> None:
    """Control for the scanner itself -- a regex gets this wrong (see printf_args)."""
    line = '"$(esc "$SHORT_SHA")" "$(esc "$FIRST_LINE")" "$(esc "${ISSUE:-?}")"'
    assert _shell_words(line) == [
        '"$(esc "$SHORT_SHA")"',
        '"$(esc "$FIRST_LINE")"',
        '"$(esc "${ISSUE:-?}")"',
    ]
    # And it must still split the OLD, unsafe form into individually-flaggable words,
    # or the guard could not report the defect it exists to report.
    assert _shell_words('"$SHORT_SHA" "$FIRST_LINE"') == ['"$SHORT_SHA"', '"$FIRST_LINE"']


# --------------------------------------------------------------------------------------
# The invariant.
# --------------------------------------------------------------------------------------


def _violations(site: CallSite) -> list[str]:
    """Every way this call site could fail to deliver. Empty list == safe."""
    problems: list[str] = []

    mode = site.parse_mode
    if mode is not None and mode.lower().startswith("markdown"):
        problems.append(
            f"parse_mode={mode}: legacy Markdown has no reliable escape, so any "
            f"interpolated value can 400 the send"
        )
    elif mode not in (None, "HTML"):
        problems.append(f"parse_mode={mode}: expected HTML (or none)")

    if mode == "HTML":
        if "esc()" not in site.block:
            problems.append("no esc() helper defined in this step")
        else:
            amp = site.block.find(r"s/&/\&amp;/g")
            lt = site.block.find(r"s/</\&lt;/g")
            if amp == -1:
                problems.append("esc() does not escape '&'")
            elif lt != -1 and amp > lt:
                problems.append(
                    "esc() escapes '<' before '&', which double-escapes: "
                    "'<' becomes '&lt;' and then '&amp;lt;'"
                )

        bare = [a for a in site.printf_args if not a.startswith('"$(esc ')]
        if bare:
            problems.append(
                f"these values reach the payload unescaped: {bare} -- every printf "
                f'argument must be wrapped in $(esc "...")'
            )

        stripped = site.printf_format
        for tag in ALLOWED_TAGS:
            stripped = stripped.replace(tag, "")
        for ch in ("<", ">"):
            if ch in stripped:
                problems.append(
                    f"template contains a bare {ch!r} outside {ALLOWED_TAGS}: "
                    f"HTML mode will try to parse it as a tag"
                )
        # A bare '&' in the template is only safe as part of an entity we wrote.
        for m in re.finditer(r"&(?!amp;|lt;|gt;)", stripped):
            problems.append(f"template contains a bare '&' at offset {m.start()}")

    return problems


@pytest.mark.parametrize("site", CALL_SITES, ids=repr)
def test_payload_cannot_be_broken_by_interpolated_text(site: CallSite) -> None:
    problems = _violations(site)
    assert not problems, f"{site!r}\n  - " + "\n  - ".join(problems)


def test_no_workflow_anywhere_still_sends_legacy_markdown() -> None:
    """The class assertion: stated over files, so a new file is covered too."""
    offenders = []
    for path in sorted(WORKFLOWS.glob("*.y*ml")):
        text = path.read_text(encoding="utf-8")
        if SENDMESSAGE not in text:
            continue
        for m in re.finditer(r"parse_mode=(Markdown[A-Za-z0-9]*)", text):
            offenders.append(f"{path.name}: {m.group(1)}")
    assert not offenders, offenders


# --------------------------------------------------------------------------------------
# Negative controls. Mutate the REAL artifact, in memory, and confirm each check fires.
# A synthetic fixture would be written from the same mental model as the check and would
# agree with it including where it is wrong.
# --------------------------------------------------------------------------------------


def _real_site() -> CallSite:
    """The PROD smoke alert -- the call site whose silent failure motivated all of this."""
    for s in CALL_SITES:
        if s.name == "deploy-pointer.yml":
            return s
    pytest.fail("deploy-pointer.yml has no Telegram call site; discovery is broken")


def test_control_the_real_prod_alert_is_currently_clean() -> None:
    assert _violations(_real_site()) == []


def test_control_reverting_the_real_payload_to_markdown_is_caught() -> None:
    site = _real_site()
    mutated = CallSite(
        site.path, site.line, site.block.replace("parse_mode=HTML", "parse_mode=Markdown")
    )
    problems = _violations(mutated)
    assert any("legacy Markdown" in p for p in problems), problems


def test_control_unescaping_one_value_in_the_real_payload_is_caught() -> None:
    """The exact defect: the commit subject reaching the parser raw."""
    site = _real_site()
    mutated = CallSite(
        site.path, site.line, site.block.replace('"$(esc "$FIRST_LINE")"', '"$FIRST_LINE"')
    )
    assert mutated.block != site.block, "mutation anchor did not match -- control is inert"
    problems = _violations(mutated)
    assert any("unescaped" in p for p in problems), problems


def test_control_dropping_the_escaper_from_the_real_payload_is_caught() -> None:
    site = _real_site()
    mutated = CallSite(site.path, site.line, site.block.replace("esc()", "noop()"))
    assert mutated.block != site.block, "mutation anchor did not match -- control is inert"
    problems = _violations(mutated)
    assert any("esc() helper" in p for p in problems), problems


def test_control_escaping_ampersand_last_is_caught() -> None:
    """Order is load-bearing: '<' first turns '&' into '&amp;lt;'."""
    site = _real_site()
    block = site.block
    amp = r"s/&/\&amp;/g"
    lt = r"s/</\&lt;/g"
    assert amp in block and lt in block, "escaper anchors did not match -- control is inert"
    swapped = block.replace(amp, "@@AMP@@").replace(lt, amp).replace("@@AMP@@", lt)
    problems = _violations(CallSite(site.path, site.line, swapped))
    assert any("before '&'" in p for p in problems), problems


def test_control_a_bare_angle_bracket_in_the_template_is_caught() -> None:
    site = _real_site()
    mutated = CallSite(
        site.path, site.line, site.block.replace("<b>PROD smoke failed</b>", "<PROD smoke failed>")
    )
    assert mutated.block != site.block, "mutation anchor did not match -- control is inert"
    problems = _violations(mutated)
    assert any("bare" in p for p in problems), problems
