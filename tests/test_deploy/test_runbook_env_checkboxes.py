"""A runbook checkbox may not certify a config flag's value from a FILE.

core#1084. The class: *an acceptance criterion verifiable only against the repo, for a
property that lives in production.* A runbook is the worst place for it -- an operator
follows a runbook under pressure, at 3am, without re-deriving anything.

The discriminator is one question: **can this step be satisfied without reading the
running system?** Six checkboxes across three V2 runbooks failed it, e.g.::

    - [ ] `.env.docker` shows `DATANIKA_BYTES_QUOTA_ENFORCE=true`

That is [core#646] exactly. ``REDIS_URL`` was present, correctly spelled, non-empty and
pointing at a healthy Redis -- and Reflex reads ``REFLEX_REDIS_URL``, so it silently used a
per-process store and **48% of production reconnects served a stale session, which in this
app is a logout.** A variable a process does not read produces no error, no log line and no
warning. **A variable present in `.env.docker` is not a setting the process read.**

The same file also stated ``(currently =false)`` for a flag Infra measured ``True`` on all
three containers six weeks earlier ([cloud#117]) -- because the deploy *preserves*
``.env.docker`` rather than shipping it, so no promotion disturbed it and no diff showed it.

The required form is a read taken from the process, which already exists twice in these
repos (``RUNBOOK_DEV_TO_MASTER.md``, cloud's ``RUNBOOK_GRANT_EXTRA_SEATS.md``)::

    docker exec datanika-celery /app/.venv/bin/python -c \
      "from datanika_cloud.billing.config import cloud_settings as c; print(c.bytes_quota_enforce)"

Design notes, each of which is a defect this guard would otherwise have had:

* **The requirement is POSITIVE, not a ban on `.env.docker`.** A negative assertion banning a
  phrase is satisfied by the phrase's own *denial*: a corrected checkbox reading "``docker
  exec ...`` prints ``True`` -- **not** ``.env.docker``, which the process never reads" still
  contains the banned string. ``CONTROL_DENIAL`` below pins that. (Same trap as a QA control
  that banned "has drifted" from a message beginning *"Nothing has drifted"*.)
* **Detection is lexical (``DATANIKA_<NAME>``), not list-driven**, so a flag added tomorrow --
  in *either* repo -- is covered without anyone remembering this issue. The parsed config
  list is load-bearing separately, in ``test_every_runbook_flag_is_a_real_setting``, which is
  what catches a runbook still naming a renamed or retired flag.
* **Only the checkbox line is examined.** The checkbox is what the operator ticks; a correct
  command in a fence above it does not make the tick mean anything, and accepting the fence
  would let the ``.env.docker`` checkbox survive underneath a correct command.
* **Action steps stay legal.** Editing, backing up or retiring a flag is not a claim about
  its value. ``CONTROL_ACTION``/``CONTROL_BACKUP``/``CONTROL_LOCATION`` pin all three, and
  the P4 runbook's edit step is correct and must not be disturbed.

Disjoint from ``test_runbook_metric_assertions.py`` (core#907), which scans **fenced code
blocks only** and never looks at prose bullets. Neither can mask the other. (cloud#176: a
redundant guard can suppress the only signal that would catch a regression beside it.)

Refs [cloud#177], [core#646], [cloud#117].
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
RUNBOOKS = sorted((_REPO / "docs" / "runbooks").glob("*.md"))

CORE_CONFIG = _REPO / "datanika" / "config.py"

# Where a sibling cloud tree may be found. Core CI's `test` job checks out core ONLY
# (`ci.yml` pulls datanika-cloud for image-probe/build-push, not for pytest), so the cloud
# flag names below are restated rather than imported. They are cross-checked against the
# real file wherever a cloud tree IS present -- see `test_cloud_flag_list_matches_source`,
# which asserts unconditionally and never skips.
_CLOUD_CANDIDATES = (
    _REPO.parent / "datanika-cloud" / "datanika_cloud" / "billing" / "config.py",
    _REPO.parent / "datanika-cloud-qa" / "datanika_cloud" / "billing" / "config.py",
    _REPO.parent.parent / "datanika-cloud" / "datanika_cloud" / "billing" / "config.py",
)

CLOUD_FLAGS = frozenset(
    {
        "DATANIKA_BYTES_QUOTA_ENFORCE",
        "DATANIKA_OVERAGE_CHARGE_ENABLE",
        "DATANIKA_E2E_ADMIN_ENABLE",
    }
)

CHECKBOX = re.compile(r"^\s*[-*]\s*\[[ xX]\]\s*(?P<text>.*)$")
FLAG = re.compile(r"\bDATANIKA_[A-Z0-9_]+\b")

# A checkbox ASSERTS a value when it pairs the flag with one, either literally or through a
# state verb. Anything else is an action -- editing, backing up, retiring -- and is legal.
VALUE = re.compile(r"=\s*[`'\"]?(true|false|1|0)\b", re.I)
STATE = re.compile(
    r"\b(shows?|has|have|exists?|enabled|in force|enforced|already live|live on"
    r"|set (?:on|to|in)|resolves? to|reads? (?:as )?(?:true|false))\b",
    re.I,
)

# The only thing that reads the running process. `docker exec` is short enough to write
# inside a checkbox, and naming it is the whole point -- the operator must be told WHERE the
# reading is taken, not just what it should say.
PROCESS_READ = re.compile(r"docker\s+exec", re.I)


def _core_flags(source: str) -> set[str]:
    """`DATANIKA_*` env names derivable from core's Settings.

    Core's `SettingsConfigDict` carries no `env_prefix`, so a field's env name is its own
    name uppercased -- and the `DATANIKA_*` flags are exactly the fields already named
    `datanika_*`. Derived, not restated, so a flag added later is covered.
    """
    return {
        "DATANIKA_" + m.group(1).upper()
        for m in re.finditer(r"^\s{4}datanika_([a-z0-9_]+)\s*[:=]", source, re.M)
    }


def _cloud_flags(source: str) -> set[str]:
    """`DATANIKA_*` names cloud declares via `validation_alias`."""
    return set(re.findall(r'validation_alias\s*=\s*"(DATANIKA_[A-Z0-9_]+)"', source))


def offending_checkboxes(text: str) -> list[str]:
    """Checkboxes claiming a flag's VALUE without naming a read of the process."""
    out = []
    for line in text.splitlines():
        m = CHECKBOX.match(line)
        if not m:
            continue
        body = m.group("text")
        if not FLAG.search(body):
            continue
        if not (VALUE.search(body) or STATE.search(body)):
            continue  # an action about a flag, not a claim about its value
        if PROCESS_READ.search(body):
            continue
        out.append(body.strip())
    return out


# --------------------------------------------------------------------------
# Negative controls -- the EXACT pre-fix lines. If these stop being rejected the
# guard is gutted and every assertion below is vacuous.
# --------------------------------------------------------------------------

CONTROL_FILE_SHOWS = "- [ ] `.env.docker` shows `DATANIKA_BYTES_QUOTA_ENFORCE=true`"
CONTROL_FILE_HAS = "- [ ] `.env.docker` has `DATANIKA_DUAL_MODE_UX_ENABLED=true`"
CONTROL_FILE_EXISTS = (
    "- [ ] `DATANIKA_BYTES_QUOTA_ENFORCE` env var exists in `.env.docker` on Hetzner "
    "(currently `=false`)"
)
CONTROL_NO_SOURCE = (
    "- [ ] `DATANIKA_BYTES_QUOTA_ENFORCE=false` on staging (dry-run mode - NOT enforcing yet)"
)
CONTROL_UI_PROXY = (
    "- [ ] `DATANIKA_DUAL_MODE_UX_ENABLED=true` already live on prod "
    "(from P1 - confirmed via UI check)"
)

# Permissive controls -- legitimate checkboxes that must stay green.
CONTROL_ACTION = (
    "- [ ] **Product**: close the `DATANIKA_DUAL_MODE_UX_ENABLED` flag - remove the "
    "`if settings.x else rx.fragment()` guards and make the V2 surfaces unconditional"
)
CONTROL_BACKUP = (
    "- [ ] `.env.docker` backup taken: `cp .env.docker .env.docker.bak-$(date +%Y%m%d-%H%M)`"
)
CONTROL_LOCATION = (
    "- [ ] You know the current staging `.env` location "
    "(typically `/opt/datanika/datanika/.env.docker`)"
)
CONTROL_GOOD = (
    '- [ ] `docker exec datanika-celery /app/.venv/bin/python -c "from '
    "datanika_cloud.billing.config import cloud_settings as c; "
    'print(c.bytes_quota_enforce)"` prints `True`'
)
CONTROL_DENIAL = (
    '- [ ] `docker exec datanika-app /app/.venv/bin/python -c "..."` reads '
    "`DATANIKA_BYTES_QUOTA_ENFORCE` as `true` - **not** `.env.docker`, "
    "which is not the check"
)
CONTROL_PROSE = (
    "A checkbox reading `.env.docker` shows `DATANIKA_BYTES_QUOTA_ENFORCE=true` cannot "
    "tell a working flip from a variable the process never reads."
)


@pytest.mark.parametrize(
    "control",
    [
        CONTROL_FILE_SHOWS,
        CONTROL_FILE_HAS,
        CONTROL_FILE_EXISTS,
        CONTROL_NO_SOURCE,
        CONTROL_UI_PROXY,
    ],
    ids=["file-shows", "file-has", "file-exists", "no-source", "ui-proxy"],
)
def test_control_rejects_every_pre_fix_line(control: str):
    assert offending_checkboxes(control), f"guard no longer rejects: {control}"


@pytest.mark.parametrize(
    "control",
    [
        CONTROL_ACTION,
        CONTROL_BACKUP,
        CONTROL_LOCATION,
        CONTROL_GOOD,
        CONTROL_DENIAL,
        CONTROL_PROSE,
    ],
    ids=["action", "backup", "location", "process-read", "denial", "prose"],
)
def test_control_accepts_legitimate_lines(control: str):
    assert offending_checkboxes(control) == [], f"guard wrongly rejects: {control}"


def test_control_denial_is_not_caught_by_a_phrase_ban():
    """The corrected line still contains `.env.docker` -- on purpose.

    A negative assertion banning the phrase would be tripped by the very sentence that
    denies it. The requirement here is the PRESENCE of a process read, which a denial
    satisfies and a file-read does not.
    """
    assert ".env.docker" in CONTROL_DENIAL
    assert offending_checkboxes(CONTROL_DENIAL) == []
    assert offending_checkboxes(CONTROL_FILE_SHOWS), "the banned shape must still be caught"


# --------------------------------------------------------------------------
# Flag derivation -- load-bearing, and armed.
# --------------------------------------------------------------------------


def test_core_flag_derivation_is_not_empty_and_tracks_new_fields():
    flags = _core_flags(CORE_CONFIG.read_text(encoding="utf-8"))
    assert "DATANIKA_DUAL_MODE_UX_ENABLED" in flags
    assert "DATANIKA_EDITION" in flags
    # Armed: a field added tomorrow is picked up with no edit here.
    synthetic = "class Settings(BaseSettings):\n    datanika_brand_new_flag: bool = False\n"
    assert _core_flags(synthetic) == {"DATANIKA_BRAND_NEW_FLAG"}
    # ...and a non-`datanika_` field is not mistaken for one.
    assert _core_flags("    redis_url: str = ''\n") == set()


def test_cloud_flag_list_matches_source():
    """`CLOUD_FLAGS` is restated (core CI has no cloud checkout) -- so cross-check it.

    Asserts unconditionally: where a cloud tree is present the two must agree, and where it
    is not, the list must still be non-empty. A guard that silently skips when its source is
    missing is the vacuity this file exists to police.
    """
    assert CLOUD_FLAGS, "restated cloud flag list is empty - every check below is weaker"
    found = next((p for p in _CLOUD_CANDIDATES if p.exists()), None)
    if found is None:
        return
    parsed = _cloud_flags(found.read_text(encoding="utf-8"))
    assert parsed, f"parsed no validation_alias flags out of {found} - parser is broken"
    assert parsed == CLOUD_FLAGS, (
        f"CLOUD_FLAGS disagrees with {found}: "
        f"missing={sorted(parsed - CLOUD_FLAGS)} stale={sorted(CLOUD_FLAGS - parsed)}"
    )


# --------------------------------------------------------------------------
# The real assertions.
# --------------------------------------------------------------------------


def test_runbooks_exist():
    assert RUNBOOKS, "no runbooks found - path wrong, so every test below is vacuous"


def test_at_least_one_runbook_checkbox_names_a_flag():
    """Anti-vacuity: if no runbook ever names a flag, the guard below cannot fail."""
    named = [
        rb.name
        for rb in RUNBOOKS
        for line in rb.read_text(encoding="utf-8").splitlines()
        if (m := CHECKBOX.match(line)) and FLAG.search(m.group("text"))
    ]
    assert named, "no runbook checkbox names a DATANIKA_* flag - the guard watches nothing"


@pytest.mark.parametrize("rb", RUNBOOKS, ids=lambda p: p.name)
def test_no_runbook_checkbox_certifies_a_flag_from_a_file(rb: Path):
    offenders = offending_checkboxes(rb.read_text(encoding="utf-8"))
    assert not offenders, (
        f"{rb.name}: a checkbox claims a DATANIKA_* flag's value without naming a read of "
        f"the running process. A variable present in `.env.docker` is not a setting the "
        f"process read (core#646). Use `docker exec <container> /app/.venv/bin/python -c "
        f"...` and say so in the checkbox itself. Offending: {offenders}"
    )


@pytest.mark.parametrize("rb", RUNBOOKS, ids=lambda p: p.name)
def test_every_runbook_flag_is_a_real_setting(rb: Path):
    """A runbook naming a renamed or retired flag is rot the lexical guard cannot see."""
    known = _core_flags(CORE_CONFIG.read_text(encoding="utf-8")) | CLOUD_FLAGS
    named = {
        f
        for line in rb.read_text(encoding="utf-8").splitlines()
        if (m := CHECKBOX.match(line))
        for f in FLAG.findall(m.group("text"))
    }
    unknown = sorted(named - known)
    assert not unknown, (
        f"{rb.name}: checkbox names {unknown}, which is not a field in datanika/config.py "
        f"nor a validation_alias in datanika_cloud/billing/config.py. Either the flag was "
        f"renamed and the runbook was not, or the name is wrong."
    )
