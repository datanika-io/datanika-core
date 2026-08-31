"""The locked tree must clear the advisories we know about (core#819).

`image-cve` (a non-required CI job added in #815) scanned the image the deploy
actually builds and returned **61 fixed-available CRITICAL/HIGH findings**. The
canary in that same run — an unfiltered scan of the same image, asserted to
fail — is what rules out "the scanner found nothing because it scanned
nothing". Everything below is the lockfile half of that finding.

Three properties this guard has that a scanner alone does not:

* **It runs in `test`, which gates `dev`.** `image-cve` is deliberately *not* a
  required check, precisely so a newly-published advisory cannot freeze `dev`
  for a PR author who introduced nothing. That is the right trade for a scanner
  whose input changes without us touching anything, and it means nothing stops
  a bump from being silently reverted. A floor asserted against our own lock
  does not move on its own, so it can gate.
* **It states the floor we resolved, not the floor the scanner named.** Trivy
  reports the fix version of each individual advisory; several of these
  packages have *chains* of advisories where later ones are bypasses of the
  earlier fix. GitPython is the worst instance: the run named 3.1.47, and
  3.1.46 is inside **19** CRITICAL/HIGH records whose fixes run to 3.1.58 —
  nine of them explicitly bypasses of an earlier patch. Taking the first fix
  version would have cleared 4 findings and left 15, while reading as done.
* **It records what we could NOT fix, and asserts the reason still holds.**
  `TestKnownUnfixableAdvisories` fails when a blocker is lifted, so the day the
  dbt pin moves is the day the CRITICAL redshift RCE becomes actionable rather
  than the day it is forgotten.

⚠️ **This checks the lock, which is not the artifact.** The image installs
`/cloud` and `./datanika-mcp` *after* `uv sync --frozen`; both are bound to the
lock by `--constraint` (core#602), and `test_dependency_pins.py` guards that
binding. `image-cve` remains the independent reading of the built image. Two
readings, neither sufficient alone.
"""

import pathlib
import re
import tomllib

from packaging.version import Version

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_PYPROJECT = _ROOT / "pyproject.toml"
_LOCK = _ROOT / "uv.lock"


def _locked_versions() -> dict[str, str]:
    text = _LOCK.read_text(encoding="utf-8")
    return {
        name.lower(): version
        for name, version in re.findall(
            r'^name = "([^"]+)"\nversion = "([^"]+)"', text, re.MULTILINE
        )
    }


#: package -> (minimum safe version, why).
#:
#: Every floor is the **highest** fix version across all CRITICAL/HIGH
#: advisories that affect the version we were shipping — not the first one a
#: scanner happens to name. Derived from the OSV API on 2026-08-31; the
#: derivation is in the PR body so the next reader can re-run it rather than
#: trust the table.
SECURITY_FLOORS: dict[str, tuple[str, str]] = {
    "aiohttp": ("3.14.3", "CVE-2026-69244 out-of-bounds heap read in the C response parser"),
    "gitpython": (
        "3.1.58",
        "19 CRITICAL/HIGH records against 3.1.46, nine of them bypasses of an earlier "
        "fix (CVE-2026-44244 bypasses CVE-2026-42215, CVE-2026-67325 bypasses that, ...). "
        "Reached via dbt-core.",
    ),
    "granian": ("2.7.4", "CVE-2026-42544 unauthenticated DoS via a WebSocket subprotocol header"),
    "lxml": ("6.1.0", "CVE-2026-41066 iterparse()/ETCompatXMLParser() default config allows XXE"),
    "mako": (
        "1.3.12",
        "CVE-2026-41205 (1.3.11) and CVE-2026-44307 (1.3.12) TemplateLookup path traversal",
    ),
    "pyasn1": (
        "0.6.4",
        "CVE-2026-30922 (0.6.3) unbounded recursion, then three more DoS records fixed in 0.6.4",
    ),
    "pyjwt": (
        "2.13.0",
        "CVE-2026-48526 — a public-key JWK accepted as an HMAC secret enables forged HS256 "
        "tokens. 2.12.0 fixes only CVE-2026-32597; the auth-bypass needs 2.13.0.",
    ),
    "pyarrow": ("23.0.1", "CVE-2026-25087 use-after-free reading an IPC file with pre-buffering"),
    "python-engineio": (
        "4.13.2",
        "CVE-2026-48802 unbound thread allocation and CVE-2026-48809 unenforced max payload "
        "size — this is the transport under Reflex's /_event socket",
    ),
    "python-multipart": (
        "0.0.30",
        "CVE-2026-42561 (0.0.27) unbounded part headers and CVE-2026-53539 (0.0.30) "
        "quadratic querystring parsing — both reachable from unauthenticated uploads",
    ),
    "python-socketio": ("5.16.2", "CVE-2026-48804 binary attachment accumulation DoS"),
    "soupsieve": ("2.8.4", "CVE-2026-49476 memory exhaustion and CVE-2026-49477 ReDoS"),
}


#: package -> (version we need, why we cannot have it).
#:
#: Advisories we are knowingly still exposed to because a declared bound of a
#: dependency we do not control forbids the fix. **Every entry here is blocked
#: by the same single line**, `dbt-core>=1.7.19,<1.8`, and that is the most
#: useful thing on this page: one pin gates the CRITICAL RCE *and* the library
#: we encrypt customer warehouse credentials with.
#:
#: Two of these are not predictable from reading the advisory list, which is
#: why they are spelled out rather than summarised. `cryptography` is blocked
#: through `cffi`, a package no advisory here names; `pyopenssl` is blocked
#: through `cryptography`, one further hop out.
BLOCKED_BY_DBT_1_7: dict[str, tuple[str, str]] = {
    "redshift-connector": (
        "2.1.14",
        "CVE-2026-8838, CRITICAL, remote code execution. dbt-redshift 1.7.7 declares "
        "redshift-connector <2.0.918. The first dbt-redshift release admitting the 2.1 "
        "line is 1.9.x, which requires dbt-core >=1.8.0b3. This driver is on the "
        "warehouse-credential path in both directions: dbt_project.py lists redshift in "
        "SUPPORTED_ADAPTERS and dlt_runner.py maps it to redshift+redshift_connector.",
    ),
    "cryptography": (
        "46.0.5",
        "CVE-2026-26007 (46.0.5), GHSA-537c-gmf6-5ccf vulnerable bundled OpenSSL (48.0.1), "
        "CVE-2026-69249 (49.0.0), CVE-2026-69247 PKCS#7 Bleichenbacher oracle (50.0.0) — "
        "four HIGH against the library EncryptionService uses on customer warehouse "
        "credentials. Blocked through cffi: every cryptography >=46.0.1 requires "
        "cffi>=2.0.0 on Python >=3.9, and dbt-core 1.7.19 declares cffi<2.0.0. 46.0.0 is "
        "the only release with a cffi<2 branch and it fixes none of the four.",
    ),
    "pyopenssl": (
        "26.0.0",
        "CVE-2026-27459 DTLS cookie callback buffer overflow. pyOpenSSL 26.0.0 requires "
        "cryptography>=46.0.0, so it inherits the cffi block above.",
    ),
    "sqlparse": (
        "0.6.0",
        "CVE-2026-71491, CVE-2026-59893 and CVE-2026-54284 — three HIGH DoS records. "
        "dbt-core 1.7.19 declares sqlparse <0.6.0; dbt-core 1.11.14 is the first to admit it.",
    ),
    "protobuf": (
        "5.29.6",
        "CVE-2026-0994 JSON recursion depth bypass. dbt-core 1.7.19 declares protobuf <5. "
        "The 4.x -> 5.x move is a major-version decision, and it is not ours to take "
        "independently: dbt-core 1.11+ requires protobuf>=6.0, so the version we land on "
        "is decided by which dbt we move to.",
    ),
}

#: The pin whose existence is the blocker above. If this stops being true, every
#: entry in ``BLOCKED_BY_DBT_1_7`` needs re-deriving.
_DBT_CEILING_SPEC = re.compile(r"^dbt-core>=1\.7[0-9.]*,<1\.8$")


def _declared_runtime_specs() -> list[str]:
    data = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    return list(data["project"]["dependencies"])


class TestLockedTreeClearsKnownAdvisories:
    def test_every_package_with_a_floor_is_present_in_the_lock(self):
        """A vanished package silently satisfies a floor check. Fail instead."""
        locked = _locked_versions()
        missing = [name for name in SECURITY_FLOORS if name not in locked]
        assert not missing, (
            "Packages named in SECURITY_FLOORS are not in uv.lock. Either the "
            "dependency was dropped (delete the entry, and say so) or this "
            "table has rotted -- a floor on an absent package passes without "
            "checking anything:\n  " + "\n  ".join(missing)
        )

    def test_no_locked_version_sits_below_its_security_floor(self):
        locked = _locked_versions()
        below = []
        for name, (floor, why) in sorted(SECURITY_FLOORS.items()):
            version = locked.get(name)
            if version is None:
                continue
            if Version(version) < Version(floor):
                below.append(f"{name} {version} < {floor} -- {why}")
        assert not below, (
            "uv.lock resolves a version inside a published CRITICAL/HIGH "
            "advisory (core#819). Raise the floor in pyproject.toml "
            "([tool.uv] constraint-dependencies for transitive packages) and "
            "re-run `uv lock`:\n  " + "\n  ".join(below)
        )

    def test_every_floor_carries_a_reason(self):
        """A floor with no advisory behind it is an unexplained pin."""
        for name, (floor, why) in SECURITY_FLOORS.items():
            assert why.strip(), f"{name} has a floor of {floor} with no stated reason"
            assert re.search(r"(CVE-|GHSA-)", why), (
                f"{name}'s reason names no advisory: {why!r}. This table exists "
                "to be re-derivable; a floor whose justification cannot be "
                "looked up cannot be safely lowered or raised later."
            )


class TestKnownUnfixableAdvisories:
    """The exposures we are carrying, and the reason each one still stands.

    Every assertion here is written so that **the blocker being lifted turns it
    red**. That is deliberate: a bump of the dbt stack is exactly the moment
    someone must revisit a CRITICAL RCE in a warehouse driver, and it is
    exactly the moment nobody would think to.
    """

    def test_the_blocker_for_these_advisories_still_exists(self):
        specs = _declared_runtime_specs()
        dbt_core = next((s for s in specs if s.replace(" ", "").startswith("dbt-core")), None)
        assert dbt_core is not None, "dbt-core is no longer a declared dependency"

        assert _DBT_CEILING_SPEC.match(dbt_core.replace(" ", "")), (
            f"dbt-core is now declared as {dbt_core!r} rather than a 1.7 pin.\n"
            "That pin was the ONLY reason these advisories were unfixable "
            "(core#819):\n  "
            + "\n  ".join(
                f"{name}: needs >={need} -- {why}"
                for name, (need, why) in sorted(BLOCKED_BY_DBT_1_7.items())
            )
            + "\nRe-derive each one, move whatever is now reachable into "
            "SECURITY_FLOORS, and delete it from BLOCKED_BY_DBT_1_7."
        )

    def test_the_blocked_set_and_the_floor_set_are_disjoint(self):
        """A package in both tables means one of them is stale."""
        both = sorted(set(SECURITY_FLOORS) & set(BLOCKED_BY_DBT_1_7))
        assert not both, (
            "A package is listed as both fixed and blocked, so one table is "
            "wrong: " + ", ".join(both)
        )

    def test_each_blocked_package_is_still_actually_below_its_needed_version(self):
        """If the resolver moved it anyway, stop calling it blocked."""
        locked = _locked_versions()
        no_longer_blocked = []
        for name, (need, why) in sorted(BLOCKED_BY_DBT_1_7.items()):
            version = locked.get(name)
            if version is None:
                continue
            if Version(version) >= Version(need):
                no_longer_blocked.append(f"{name} {version} >= {need} -- {why}")
        assert not no_longer_blocked, (
            "These are recorded as blocked but the lock already satisfies "
            "them. Move them to SECURITY_FLOORS so a future `uv lock` cannot "
            "roll them back:\n  " + "\n  ".join(no_longer_blocked)
        )
