"""Compute the E2E tier graduation streak from run history (core#1130).

`docs/QA_RULES.md` §10 calls graduation *"mechanical: 3 consecutive greens on `dev`"*. Until
this script it was not mechanical — nothing computed it. A reviewer grepped the last N runs and
counted, and `ci.yml` says out loud that a collision-induced red *"RESETS the
three-consecutive-greens counter"*: a counter that existed only in somebody's head.

The sentence is also underspecified in a way that decides the answer. **It never says what
sequence "consecutive" ranges over**, and on a job whose history is mostly runs that measured
nothing, the candidate readings disagree.

Measured on `dev`, 2026-09-06, the nine completed `e2e-sso` runs after the SAML binding fix::

    17:23  e9e5b510  specs_failed  FAIL         1 failed, 3 skipped, 12 passed
    18:54  6a9a1d0d  wrong_build   UNMEASURED
    18:57  615cafe7  wrong_build   UNMEASURED
    19:01  88f707ac  wrong_build   UNMEASURED   3 skipped, 13 passed   <- SAML full flow PASSED
    19:13  be0bd9b9  no_verdict    UNMEASURED
    19:22  2e92cd6c  wrong_build   UNMEASURED   3 skipped, 13 passed
    19:26  680ef967  wrong_build   UNMEASURED   3 skipped, 13 passed
    19:32  51849052  cancelled     UNMEASURED
    19:38  df0c391c  clean         PASS         3 skipped, 13 passed

**Seven of nine runs carried no reading**, and four runs in which every SSO spec passed
contribute one green between them. Three readings of the same sentence:

* **calendar** — three *adjacent* runs, all green. Cannot be satisfied at this unmeasured rate,
  and an unsatisfiable bar gets lowered rather than met.
* **tally** — three greens *anywhere* in the window. Already satisfied on 2026-09-06 by greens
  that were never adjacent, on a day when exactly one run could report on its own commit.
* **measured** — three adjacent greens in the subsequence of runs that actually measured
  something. **This is what the script implements.**

Two asymmetries make the measured reading honest, and they are the part worth arguing about:

* an **UNMEASURED** run is transparent. `wrong_build` means "this run cannot report on this
  commit"; we know it carried no reading, so it neither advances nor resets.
* an **UNREADABLE** run breaks the streak. We do *not* know what it carried, and treating it as
  transparent is assuming it was not a red — the reassuring assumption, which is the one this
  project keeps paying for.

A streak assembled from a window that measured almost nothing reports **`sparse`** rather than
`graduate`. That is deliberately a third state and not a silent pass: the same three greens can
mean "stable across three runs" or "the only three readings in a fortnight", and only a human
should decide which. Same shape as the `empty` / `unknown` / `no-evidence` states this codebase
already uses wherever a verdict can be absent.

🚨 **`sparse` grades DILUTION, never LENGTH** (core#1154). The first implementation asked
``span > max_span`` — the number of calendar runs the streak reaches back through. That number
grows for two opposite reasons: because unmeasured runs sit *between* the greens (diluted), or
because there are simply *many consecutive greens* (the opposite). So the healthier a tier got,
the more likely it was to be refused — `e2e-staging` at **14 measured greens out of 14 runs**
returned `sparse`, any streak longer than `max_span` was unconditionally `sparse` however dense,
and on the real `e2e-sso` history the verdict moved **`graduate` → `sparse` on the arrival of a
sixth consecutive green**. The predicate is now ``gaps > max_gaps`` where ``gaps = span -
streak``, which is the field that records the property the state is named for.

Usage
-----
::

    python scripts/e2e_tier_streak.py                      # e2e-sso on dev
    python scripts/e2e_tier_streak.py --job e2e-staging    # the informational tier
    python scripts/e2e_tier_streak.py --runs 40 --required 3

Exit 0 when the tier has graduated, 1 otherwise — including `no-data` and `sparse`, both of
which mean *"do not graduate on this evidence"* rather than *"the tier is red"*.

⚠️ **Actions logs expire.** GitHub retains them for 90 days by default, so a verdict older than
that reads as UNREADABLE and breaks a streak. That is the correct direction (an unreadable run is
not a green) but it means graduation evidence decays and cannot be reconstructed later. Record
the SHAs in the spec's tier header when a spec graduates, as `golden-path.spec.ts` already does.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass

REPO = "datanika-io/datanika-core"
WORKFLOW = ".github/workflows/ci.yml"

#: A run produced a green reading of the tier.
PASS = "PASS"  # noqa: S105  - a verdict class, not a credential
#: A run produced a red reading of the tier.
FAIL = "FAIL"
#: A run produced no reading, and we know why. Transparent to the streak.
UNMEASURED = "UNMEASURED"
#: A run produced no reading we can recover. Breaks the streak — see the module docstring.
UNREADABLE = "UNREADABLE"
#: A run that executed somewhere that is **not** the deployed system — a per-agent local
#: stack, a developer's laptop. It may be perfectly green; it describes a different machine.
#: **Breaks the streak**, and that is not the same call as `UNMEASURED` (core#1232).
#:
#: 🔑 `wrong_build`, `no_verdict` and `cancelled` are UNMEASURED because they are **CI's own
#: report about a CI run**: the stream is trusted and the run simply did not grade. `LOCAL`
#: says the *stream* is not trusted. Transparent would hide that a reader is looking at a
#: source it should not be reading, and a contaminated history would read clean — which is
#: the failure this class exists for, not the one UNMEASURED handles.
LOCAL = "LOCAL"

#: `ci.yml`, `e2e-sso`, step "Classify what this job's result means".
SSO_VERDICTS: dict[str, str] = {
    "clean": PASS,
    # The specs ran and passed; the job is red for a non-test reason (artifact upload, or the
    # Authentik cleanup). core#873. That is a green reading of the SSO surface.
    "infra_only": PASS,
    "specs_failed": FAIL,
    # The specs may well have passed — four such runs on 2026-09-06 each carried `13 passed` —
    # but staging was not running this commit, so as the classifier's own message puts it,
    # "a green here would not have been this commit's green either". core#876.
    "wrong_build": UNMEASURED,
    "no_verdict": UNMEASURED,
    "cancelled": UNMEASURED,
}

#: `staging.yml`, `e2e-staging`, step "Report informational tier result".
INFORMATIONAL_VERDICTS: dict[str, str] = {
    "success": PASS,
    "failure": FAIL,
    # The informational tier is legitimately empty between graduations. Three greens over an
    # empty tier would "graduate" nothing at all.
    "empty": UNMEASURED,
    "unknown": UNMEASURED,
}

#: `staging.yml`, `e2e-staging`, step "Classify what this job's result means" — the **GATING**
#: tier. Identical to the SSO vocabulary except that a spec failure is spelled `gating_failed`.
#:
#: 🚨 This map did not exist until core#1205, and its absence was not a gap in coverage — it was
#: a WRONG ANSWER. `parse_verdict_line` tried the SSO and informational patterns in turn and
#: returned the LAST match over the whole log. An `e2e-staging` log carries no gating pattern to
#: match, so the scan fell through to `INFORMATIONAL_RESULT=` and reported the **informational,
#: explicitly non-gating** tier as the gating verdict. A promotion pre-flight read
#: `conclusion=success, verdict=FAIL` on a run whose own log said
#: `gating step outcome: success / job status: success / verdict: clean`, 21 of 21 steps green.
GATING_VERDICTS: dict[str, str] = {
    "clean": PASS,
    "infra_only": PASS,
    "gating_failed": FAIL,
    "wrong_build": UNMEASURED,
    "no_verdict": UNMEASURED,
    "cancelled": UNMEASURED,
}

#: The one environment value that means *"the deployed system, reached the way CI reaches it"*.
#: Anything else a run attests to is not that, and is graded `LOCAL` — including a value nobody
#: has thought of yet, which is the direction that fails closed.
CI_ENVIRONMENT = "staging"

#: The token an environment veto produces. **Deliberately in no tier's vocabulary**, and
#: `tests/test_deploy/test_local_run_cannot_be_evidence.py` asserts it appears in **no workflow**
#: — that absence is its defining property, not an exemption from the anti-vacuity control.
LOCAL_VERDICT = "local_environment"

#: Its own map, kept separate so the workflow-derived controls over the three tier vocabularies
#: stay exactly as strict as they were.
LOCAL_VERDICTS: dict[str, str] = {LOCAL_VERDICT: LOCAL}

#: `informational_spec_results.py`'s vocabulary (core#1221). `no_evidence` is what an empty or
#: unreadable report produces — a report carrying no spec at all, which is what a crashed or
#: misdirected run leaves behind. It is UNMEASURED, never a green.
INFORMATIONAL_SPEC_VERDICTS: dict[str, str] = {
    "success": PASS,
    "failure": FAIL,
    "no_evidence": UNMEASURED,
}

#: Verdict token -> class. **All three vocabularies**, because one policy is emitted by two jobs
#: and `e2e-staging` emits two tiers of its own.
#:
#: `tests/test_deploy/test_e2e_tier_streak.py` pins this against the two workflow files in
#: BOTH directions, and the second direction is the one that matters:
#:
#: * forward — every token a classifier can emit must appear here, so a new verdict state is
#:   triaged deliberately instead of being silently absorbed as UNREADABLE;
#: * backward — every token in each TIER map must still be found in the workflow. That is the
#:   anti-vacuity control, and it is derived rather than a floor. A floor of "at least 5 states"
#:   was the first attempt and it was measured tolerating the exact regression it existed to
#:   catch: the classifier emits six, so dropping one left five and the control stayed green.
#:
#: ⚠️ **Those controls are scoped to the three TIER maps, not to this union** — corrected
#: 2026-09-09 when core#1232 added a token no workflow emits and all 92 tests stayed green.
#: They were right and this comment was wrong: read literally it promised a guarantee over
#: `VERDICT_CLASS` that nothing checked, which would have told the next reader that
#: `local_environment` was covered. `LOCAL_VERDICTS` sits outside them **on purpose** and has
#: the opposite control instead — `test_local_run_cannot_be_evidence.py` asserts its token
#: appears in **no** workflow, because that absence is what makes it un-forgeable by CI.
VERDICT_CLASS: dict[str, str] = {
    **SSO_VERDICTS,
    **GATING_VERDICTS,
    **INFORMATIONAL_VERDICTS,
    **INFORMATIONAL_SPEC_VERDICTS,
    **LOCAL_VERDICTS,
}

#: Tier -> the vocabulary that tier's classifier emits. The caller names the tier it is asking
#: about; nothing infers it from whatever the log happens to contain.
VERDICTS_BY_TIER: dict[str, dict[str, str]] = {
    "sso": SSO_VERDICTS,
    "gating": GATING_VERDICTS,
    "informational": INFORMATIONAL_VERDICTS,
}

#: `STATE=<token>` assignments in a shell block. Anchored to an assignment so that a *read*
#: (`[ "$JOB_STATUS" = "success" ]`) cannot enter the vocabulary.
_STATE_ASSIGN = re.compile(r"(?<![\w$])STATE=([a-z_][a-z0-9_]*)")

#: The two verdict lines, as EXECUTED. Every runner log line carries an ISO stamp, and the
#: classifier's own source is echoed into the `##[group]Run` header with the variables
#: unexpanded — matching that would be measuring the script rather than the run, so both
#: patterns require a literal token where the shell source has `$STATE`.
_SSO_VERDICT = re.compile(
    r"SSO specs outcome: (?P<specs>[a-z_]+) / job status: [a-z_]+ / "
    r"verdict: (?P<verdict>[a-z_]+)\s*$"
)
_INFO_VERDICT = re.compile(r"(?:^|\s)INFORMATIONAL_RESULT=([a-z_]+)\s*$")

#: `staging.yml`'s gating classifier. Same shape as the SSO line, different nouns — and its
#: absence is what core#1205 was.
_GATING_VERDICT = re.compile(
    r"gating step outcome: (?P<specs>[a-z_]+) / job status: [a-z_]+ / "
    r"verdict: (?P<verdict>[a-z_]+)\s*$"
)

#: `e2e/scripts/informational_spec_results.py`, one line per spec FILE (core#1221).
#: Graduation is per spec; the tier line is one boolean for the whole tier, so a new spec
#: entering zeroed every incumbent and one red spec blocked everyone.
_INFO_SPEC_VERDICT = re.compile(
    r"(?:^|\s)INFORMATIONAL_SPEC_RESULT=(?P<spec>[^:\s]+):(?P<verdict>[a-z_]+)\s*$"
)

#: Tier -> the pattern that tier's classifier prints.
_PATTERN_BY_TIER = {"sso": _SSO_VERDICT, "gating": _GATING_VERDICT, "informational": _INFO_VERDICT}

#: The name of the marker a run uses to say where it ran. One definition, because the emitter
#: and the parser must be the same string: a local runner that hardcodes it would keep working
#: after a rename here, and the veto would be silently disarmed while everything stayed green.
ENVIRONMENT_MARKER = "E2E_ENVIRONMENT"

#: What a run says about where it ran. `E2E_ENVIRONMENT=<value>`, same shape as the other
#: markers so it survives the same log mangling. The echoed shell source carries `$VAR`, which
#: the character class rejects, so this reads the executed line and not the script.
_ENVIRONMENT = re.compile(rf"(?:^|\s){ENVIRONMENT_MARKER}=(?P<env>[a-z0-9_-]+)\s*$")


def environment_attestation(environment: str) -> str:
    """The line a runner prints to say where it executed. **Call this; do not hardcode it.**

    ```sh
    python -c "from scripts.e2e_tier_streak import environment_attestation as a; print(a('local'))"
    ```

    A local runner emitting `environment_attestation("local")` is graded `LOCAL` by every
    consumer of this module — it cannot advance a graduation streak and it cannot satisfy the
    promotion pre-flight. That is the point: **the way to make a local run honest is to say so
    once, in a string this module owns.**

    ⚠️ It refuses a value the parser could not read back. A marker that does not match
    :data:`_ENVIRONMENT` prints fine, reaches a log, and is *silently invisible* to the veto —
    which is a local run that looks exactly like a staging one, produced by the very call that
    was supposed to prevent it.
    """
    line = f"{ENVIRONMENT_MARKER}={environment}"
    if _ENVIRONMENT.search(line) is None:
        raise ValueError(
            f"{environment!r} cannot be read back by the environment veto "
            f"(allowed: lowercase letters, digits, `_`, `-`). A line the parser cannot read "
            f"is one the veto cannot act on, so this would produce a local run that reads as "
            f"a staging one."
        )
    return line


class AmbiguousVerdictError(RuntimeError):
    """The log carries more than one tier's verdict and the caller did not say which it wants.

    Raised rather than guessed. core#1205 is what guessing looks like: an `e2e-staging` log
    carries a gating verdict AND an informational one, and picking whichever matched last
    reported the non-gating tier as the gating result — to a promotion pre-flight.
    """


_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def verdict_states_in_workflow(run_block: str) -> set[str]:
    """Every verdict token a classifier `run:` body can assign to `STATE`."""
    return set(_STATE_ASSIGN.findall(run_block))


def classify_verdict(verdict: str | None, specs_outcome: str | None = None) -> str:
    """Map a verdict token to its class. Anything unrecognised is UNREADABLE, never a PASS.

    `specs_outcome` disambiguates `wrong_build` (core#1151). A run whose specs FAILED is not a
    non-measurement just because the build could not be attributed: for a *graduation* question
    the property is stability, and specs failing anywhere in the window is exactly what should
    reset the streak -- even when that failure belongs to somebody else's build.

    ⚠️ Deliberately NOT "wrong_build always resets". Seven of ten runs were unmeasured on
    2026-09-06; resetting on all of them makes the bar unsatisfiable, which is the failure mode
    `docs/QA_RULES.md` §10 exists to avoid.
    """
    if verdict is None:
        return UNREADABLE
    if verdict in ("wrong_build", "cancelled") and specs_outcome == "failure":
        return FAIL
    return VERDICT_CLASS.get(verdict, UNREADABLE)


def attested_environment(log_lines: list[str]) -> str | None:
    """Where this run says it executed, or `None` if it did not say.

    `None` is **not** proof of CI and this function does not claim otherwise — every log
    written before core#1232 is silent, and grading silence as `LOCAL` would red the entire
    history. Silence is covered by a different mechanism:
    `tests/test_deploy/test_local_run_cannot_be_evidence.py` asserts that nothing outside
    `.github/workflows/` can print a verdict marker at all, so a silent log cannot have come
    from a local runner in the first place. **One guard fails closed on presence, the other on
    absence; neither is sufficient alone.**

    The last attestation wins, for the same reason the verdict line's does: the classifier's
    source is echoed into the `##[group]Run` header before it is executed.
    """
    found: str | None = None
    for raw in log_lines:
        line = _ANSI.sub("", raw).rstrip()
        if line.endswith('"') or line.endswith("'"):
            continue
        m = _ENVIRONMENT.search(line)
        if m:
            found = m.group("env")
    return found


def parse_verdict_line(log_lines: list[str], *, tier: str = "auto") -> str | None:
    """The verdict token for ONE TIER of a job log, or `None` if that tier printed none.

    `None` is the honest answer for an empty log (a cancelled job's log is zero bytes) and for
    a log whose classifier never ran. It is deliberately not a class: the caller decides
    whether an absent line is UNMEASURED (because the job conclusion explains it) or
    UNREADABLE.

    The **last** match wins *within a tier*. The classifier line appears twice in a normal log —
    once as the echoed script source inside the `##[group]Run` header, and once as real output —
    and only the second describes this run.

    🚨 **`tier` is the fix for core#1205 and it is the whole point.** This used to try every
    pattern and return the last that matched anywhere in the log. On an `e2e-staging` log that
    silently answered a question nobody asked: the informational tier's result, handed to a
    promotion pre-flight that was asking about the **gating** tier. *Report a mechanism's status
    only from the field that records THAT mechanism.*

    `tier="auto"` is kept for the single-tier logs where it is unambiguous (`e2e-sso` emits no
    `INFORMATIONAL_RESULT=` line at all — measured: 0 occurrences across SSO logs, against 5 in a
    staging log, so the two cannot be confused). Where a log carries **more than one** tier's
    verdict, `auto` raises rather than picking. That is the case that was wrong.

    🚨 **An environment attestation naming anything but `CI_ENVIRONMENT` is a VETO, not a
    fallback** (core#1232). It is read first and it wins over every verdict line in the log,
    including one that appears **after** it and says `clean`. A fallback would be the core#1205
    defect in a new costume: a green further down the file outvoting the line that says the
    green describes a different machine. Local stacks make that ordering ordinary — the suite
    prints its own result after the harness prints where it ran — so the veto has to be
    order-independent, and this one is.
    """
    where = attested_environment(log_lines)
    if where is not None and where != CI_ENVIRONMENT:
        return LOCAL_VERDICT

    per_tier: dict[str, str] = {}
    for raw in log_lines:
        line = _ANSI.sub("", raw).rstrip("\r\n")
        # The echoed source ends in a quote and carries `$STATE`; requiring the line to END at
        # the token rejects it without having to model the header's shape.
        if line.endswith('"') or line.endswith("'"):
            continue
        for name, pattern in _PATTERN_BY_TIER.items():
            m = pattern.search(line)
            if m:
                # The LAST match wins within a tier: the classifier line appears twice, once as
                # echoed source in the `##[group]Run` header and once as real output.
                per_tier[name] = m.group("verdict") if "verdict" in m.groupdict() else m.group(1)

    if tier != "auto":
        if tier not in _PATTERN_BY_TIER:
            raise ValueError(f"unknown tier {tier!r}; expected one of {sorted(_PATTERN_BY_TIER)}")
        return per_tier.get(tier)

    if len(per_tier) > 1:
        raise AmbiguousVerdictError(
            f"this log carries {len(per_tier)} tiers' verdicts ({', '.join(sorted(per_tier))}) "
            "and no tier was named. Pass tier='gating' or tier='informational' — picking one "
            "silently is core#1205, where the informational result was reported as the gating "
            "verdict to a promotion pre-flight."
        )
    return next(iter(per_tier.values()), None)


def parse_specs_outcome(log_lines: list[str]) -> str | None:
    """`SPECS_OUTCOME` from the same classifier line the verdict comes from (core#1151).

    `wrong_build` is a verdict about **attribution**, and `ci.yml` decides it *before* it looks
    at the specs -- deliberately, since a run on the wrong build cannot report either way. So
    the token conflates `success`, `skipped` and `failure`, and reading it alone as "this run
    carried no reading" is a claim about the specs drawn from a field that records attribution
    (`ENGINEERING_RULES` §39).

    Returns `None` for lines that carry no such field (the `INFORMATIONAL_RESULT=` form), and
    for the echoed script source, whose `$SPECS_OUTCOME` is unexpanded.
    """
    found: str | None = None
    for raw in log_lines:
        line = _ANSI.sub("", raw).rstrip()
        if line.endswith('"') or line.endswith("'"):
            continue
        m = _SSO_VERDICT.search(line)
        if m:
            found = m.group("specs")
    return found


def parse_spec_verdicts(log_lines: list[str]) -> dict[str, str]:
    """`spec file -> verdict token` from a log's per-spec lines. Empty when it carries none.

    An empty dict means *"this run predates core#1221"*, which is a different fact from
    *"this run graded no spec"* — the caller has to tell them apart, and
    :func:`classify_for_spec` is where that happens.
    """
    found: dict[str, str] = {}
    for raw in log_lines:
        line = _ANSI.sub("", raw).rstrip()
        if line.endswith('"') or line.endswith("'"):
            continue
        m = _INFO_SPEC_VERDICT.search(line)
        if m:
            found[m.group("spec")] = m.group("verdict")
    return found


def classify_for_spec(spec: str, per_spec: dict[str, str], tier_verdict: str | None) -> str:
    """What ONE spec's run means, from a log that may or may not carry per-spec lines.

    🚨 **The asymmetry is the whole fix, and it is not symmetric on purpose.**

    A run that carries per-spec lines answers directly. A run that predates them carries only
    the tier's boolean, and the two directions of that boolean do **not** carry the same
    information about one spec:

    * tier ``success`` -> this spec passed. A green tier means every spec in it was green,
      so the attribution is sound.
    * tier ``failure`` -> **UNMEASURED, not FAIL.** The line says *something* in the tier
      failed and cannot say what. Grading it as this spec's failure is exactly the defect
      core#1221 is about, one level down: it is how ``reflex-wire.spec.ts`` lost seven greens
      to a spec that arrived beside it.

    ⚠️ UNMEASURED is transparent to the streak, so an incumbent's history survives — and
    ``Reading.from_classes`` then reports it ``sparse`` once the unattributable runs
    outnumber ``max_gaps``, which is the correct answer: *the streak is intact and nobody has
    measured it lately; a human decides.* That fell out of core#1154's fix rather than being
    added here, which is the reason to trust it.

    A spec absent from a run that DID carry per-spec lines is UNMEASURED too: it did not run,
    or did not exist yet. Neither is a failure.
    """
    if per_spec:
        token = per_spec.get(spec)
        if token is None:
            return UNMEASURED
        return VERDICT_CLASS.get(token, UNREADABLE)

    if tier_verdict == "success":
        return PASS
    if tier_verdict == "failure":
        return UNMEASURED
    return classify_verdict(tier_verdict)


def streak(classes: list[str]) -> int:
    """Trailing consecutive PASSes over the MEASURED subsequence.

    `classes` is oldest-first. Trailing rather than "anywhere" because `ci.yml` already says a
    red *resets* the counter, and a counter that resets is by definition read from the end.
    """
    n = 0
    for cls in reversed(classes):
        if cls == UNMEASURED:
            continue  # transparent: we know this run carried no reading
        if cls != PASS:
            break  # FAIL resets; UNREADABLE blocks, because we cannot rule out a FAIL
        n += 1
    return n


@dataclass(frozen=True)
class Reading:
    """What the run history supports, and how much of it was actually a measurement."""

    streak: int
    required: int
    measured: int
    total: int
    span: int
    gaps: int
    state: str

    @property
    def graduated(self) -> bool:
        return self.state == "graduate"

    @classmethod
    def from_classes(cls, classes: list[str], *, required: int = 3, max_gaps: int = 7) -> Reading:
        """Classify a history.

        `max_gaps` bounds how many runs may have measured NOTHING *inside* the streak's window
        before it is reported `sparse`. It is a judgement, not a measurement, and it is exposed
        as a flag so that whoever changes it has to say so.

        🚨 **It deliberately does NOT bound the window's length** (core#1154). The predicate was
        `span > max_span`, and `span` grows for two opposite reasons: because unmeasured runs
        sit between the greens (diluted — what `sparse` means), or because there are simply many
        consecutive greens (the opposite). Grading on `span` therefore penalised a tier for
        getting healthier: `e2e-staging` at **14 measured greens out of 14 runs** — the densest
        reading this instrument can take — returned `sparse`, and any streak longer than
        `max_span` was unconditionally `sparse` however dense. On the real `e2e-sso` history the
        verdict moved `graduate` → `sparse` on the arrival of a sixth consecutive green.

        `gaps = span - streak` is the field that records the property the state is named for.
        At `required=3` the old and new thresholds are the same condition (`span <= 10` ⟺
        `gaps <= 7`), so this is behaviour-preserving everywhere the suite ever exercised it.
        """
        total = len(classes)
        measured = sum(1 for c in classes if c in (PASS, FAIL, UNREADABLE))
        n = streak(classes)

        # How many calendar runs the trailing streak reaches back through.
        span = 0
        seen = 0
        for c in reversed(classes):
            span += 1
            if c == UNMEASURED:
                continue
            if c != PASS:
                span -= 1  # the blocking run is not part of the streak's span
                break
            seen += 1
            if seen == n:
                break

        # Runs inside the streak's own window that carried no reading. This is the dilution the
        # `sparse` state is named for; the window's absolute length is not (core#1154).
        gaps = span - n

        if total < required:
            state = "no-data"
        elif n < required:
            state = "not-yet"
        elif gaps > max_gaps:
            state = "sparse"
        else:
            state = "graduate"
        return cls(
            streak=n,
            required=required,
            measured=measured,
            total=total,
            span=span,
            gaps=gaps,
            state=state,
        )


# --------------------------------------------------------------------------------------
# GitHub API layer
# --------------------------------------------------------------------------------------


def _gh(path: str, *, raw: bool = False) -> object:
    # S603/S607: fixed argv, no shell. `gh` is resolved from PATH deliberately — it lives at
    # /d/Tools/gh/bin on the dev machine and /usr/bin on a runner.
    #
    # `encoding="utf-8"` is not decoration: the Windows locale codec here is cp1251 and a bare
    # `text=True` mis-decodes every non-ASCII byte (WORKFLOW_RULES §7).
    out = subprocess.run(  # noqa: S603
        ["gh", "api", path],  # noqa: S607
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if out.returncode != 0:
        raise SystemExit(f"gh api {path} failed:\n{out.stderr.strip()}")
    return out.stdout if raw else json.loads(out.stdout)


def _gh_log_or_none(repo: str, job_id: int) -> str | None:
    """A job's raw log, or `None` when GitHub will not serve it.

    Not every completed job has a retrievable log: a **cancelled** job's is zero bytes, and
    Actions logs are **expired after 90 days**, both answering `404`. Neither is a reason to
    crash, and — more importantly — neither is a reason to report a pass. The caller turns
    `None` into UNMEASURED when the job conclusion explains it and UNREADABLE otherwise, and
    UNREADABLE blocks a streak.
    """
    out = subprocess.run(  # noqa: S603
        ["gh", "api", f"repos/{repo}/actions/jobs/{job_id}/logs"],  # noqa: S607
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return out.stdout if out.returncode == 0 else None


def collect(
    repo: str, branch: str, job_name: str, runs: int, spec: str | None = None
) -> list[tuple[str, str, str]]:
    """`(created_at, short_sha, class)` per completed run, oldest first.

    ``event=push`` is not optional. A `dev` head carries a `merge_group` run too, whose staging
    jobs are `skipped` **by design** — byte-identical to the condition that holds a promotion,
    produced by a run nobody asked for.
    """
    payload = _gh(f"repos/{repo}/actions/runs?branch={branch}&event=push&per_page={runs}")
    out: list[tuple[str, str, str]] = []
    fetched = failed = 0
    for run in payload.get("workflow_runs", []):  # type: ignore[union-attr]
        if run.get("path") != WORKFLOW or run.get("status") != "completed":
            continue
        jobs = _gh(f"repos/{repo}/actions/runs/{run['id']}/jobs?per_page=100")
        job = next(
            (j for j in jobs.get("jobs", []) if job_name in j["name"]),  # type: ignore[union-attr]
            None,
        )
        # An IN-PROGRESS job has no verdict YET, which is a different fact from a completed
        # job whose verdict cannot be read. Conflating them makes a healthy running job read
        # as an instrument failure, and a check that reds on a healthy system gets deleted.
        if job is None or job.get("status") != "completed":
            continue

        log = _gh_log_or_none(repo, job["id"])
        if log is None:
            failed += 1
        else:
            fetched += 1
        lines = log.splitlines() if log is not None else []
        # Name the tier. This script's question is about GRADUATION, so on `e2e-staging` it
        # wants the INFORMATIONAL tier — that is the tier specs graduate out of — and on
        # `e2e-sso` the SSO one. Stating it is the core#1205 fix: the old call took whichever
        # verdict matched last, which on a staging log is now ambiguous by construction.
        tier = "sso" if "sso" in job_name else "informational"
        verdict = parse_verdict_line(lines, tier=tier) if log is not None else None
        specs = parse_specs_outcome(lines) if log is not None else None

        if verdict is None and job.get("conclusion") == "cancelled":
            # A cancelled job's log is zero bytes by construction. That is a KNOWN
            # non-measurement, not a broken instrument — fall back to the job conclusion
            # rather than reporting the reader as broken.
            verdict = "cancelled"

        # core#1232's veto composes with core#1221's per-spec reading, and it has to be
        # applied HERE rather than inside either classifier: a log that attests to a
        # non-CI environment says nothing about production whichever question is asked of
        # it, and a per-spec green from a laptop is exactly the false verdict with a build
        # behind it that #1232 exists to refuse.
        where = attested_environment(lines) if log is not None else None
        if where is not None and where != CI_ENVIRONMENT:
            klass = LOCAL
        elif spec is not None:
            klass = classify_for_spec(
                spec, parse_spec_verdicts(lines), parse_verdict_line(lines, tier="informational")
            )
        else:
            klass = classify_verdict(verdict, specs)

        out.append((run["created_at"], run["head_sha"][:8], klass))

    # If NOTHING could be fetched, this is an instrument failure and must be loud. Silently
    # classifying every run UNREADABLE blocks a streak, which is the safe direction — and it
    # reads identically to a tier that is genuinely never measured.
    if failed and not fetched:
        raise SystemExit(
            f"could not fetch a single job log ({failed} attempts, all failed). "
            "This says nothing about the tier — fix the reader before reading the verdict."
        )
    return list(reversed(out))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo", default=REPO)
    ap.add_argument("--branch", default="dev")
    ap.add_argument("--job", default="e2e-sso", help="job name substring (e2e-sso, e2e-staging)")
    ap.add_argument("--runs", type=int, default=25)
    ap.add_argument(
        "--spec",
        help="grade ONE informational spec file (e.g. reflex-wire.spec.ts) instead of the "
        "whole tier — core#1221. Graduation is per spec; the tier line is one boolean.",
    )
    ap.add_argument("--required", type=int, default=3)
    ap.add_argument(
        "--max-gaps",
        type=int,
        default=7,
        help="runs inside the streak's window that may have measured NOTHING before it "
        "reports `sparse` (a judgement). NOT a bound on the window's length — core#1154",
    )
    args = ap.parse_args(argv)

    history = collect(args.repo, args.branch, args.job, args.runs, spec=args.spec)
    for created, sha, cls in history:
        print(f"{created}  {sha}  {cls}")

    r = Reading.from_classes(
        [c for _, _, c in history], required=args.required, max_gaps=args.max_gaps
    )
    print()
    subject = f"{args.job} on {args.branch}"
    if args.spec:
        subject += f"   spec: {args.spec}"
    print(f"job            : {subject}")
    print(f"runs read      : {r.total}  (measured: {r.measured})")
    print(
        f"trailing streak: {r.streak} / {r.required}   spanning {r.span} calendar run(s), "
        f"{r.gaps} of which measured nothing"
    )
    print(f"verdict        : {r.state}")
    if r.state == "sparse":
        print(
            f"  -> {r.gaps} of the {r.span} runs this streak reaches back through carried no\n"
            "     reading. Three greens across a fortnight are not three greens across three\n"
            "     runs. A human decides; this script will not graduate it."
        )
    if r.state == "no-data":
        print(
            f"  -> fewer than {r.required} completed runs were read. This is NOT 'the tier is\n"
            "     red' — it is 'there is nothing here to read'."
        )
    return 0 if r.graduated else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
