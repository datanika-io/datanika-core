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
  `TestDeferredCapability` fails when a blocker is lifted, so the day a
  deferral becomes actionable is the day someone is told, rather than the day
  it is forgotten.

  🔑 **This mechanism has now fired once, and it worked** (core#825). Its
  predecessor `TestKnownUnfixableAdvisories` went red the moment the dbt pin
  moved and handed over six packages with their needed versions, including the
  CRITICAL redshift RCE and the credential-encryption library. All six are in
  `SECURITY_FLOORS` above. The successor guards a *capability* deferral rather
  than an advisory one, but keeps the shape, because the shape is what worked.

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
    # ---- Moved out of BLOCKED_BY_DBT_1_7 in core#825 --------------------
    # These six were unfixable for as long as `dbt-core>=1.7.19,<1.8` stood.
    # Dropping the abandoned `dbt-mysql` adapter freed the dbt stack to 1.11
    # and the resolver delivered all six. They are floored HERE, not left to
    # the resolver, because three of them (pyopenssl, urllib3, protobuf)
    # needed no floor to arrive -- and a version the resolver happens to pick
    # is not a version the next re-lock is obliged to keep.
    "cryptography": (
        "50.0.0",
        "CVE-2026-26007 (46.0.5), GHSA-537c-gmf6-5ccf vulnerable bundled OpenSSL (48.0.1), "
        "CVE-2026-69249 (49.0.0), CVE-2026-69247 PKCS#7 Bleichenbacher oracle (50.0.0) -- "
        "four HIGH against the library EncryptionService uses on customer warehouse "
        "credentials. Was blocked through cffi by TWO independent pins: dbt-core 1.7's "
        "cffi<2.0.0 and snowflake-connector-python 3.x's identical one.",
    ),
    "protobuf": (
        "5.29.6",
        "CVE-2026-0994 JSON recursion depth bypass. The locked version is 6.x, not 5.x, "
        "and that major is FORCED rather than chosen: every dbt-core >=1.9.9 requires "
        "protobuf>=6.0, and cryptography needs dbt-core >=1.10. Founder-accepted "
        "2026-08-31. The floor stays at the advisory's fix version because that is what "
        "this table records; the ceiling in pyproject.toml is what bounds the major.",
    ),
    "pyopenssl": (
        "26.0.0",
        "CVE-2026-27459 DTLS cookie callback buffer overflow. Needed cryptography>=46.0.0, "
        "so it inherited the cffi block and cleared with it -- no floor was required to "
        "reach it, which is exactly why one is recorded now.",
    ),
    "redshift-connector": (
        "2.1.14",
        "CVE-2026-8838, CRITICAL, remote code execution in the driver that holds and uses "
        "customer Redshift warehouse credentials. Needed dbt-redshift >=1.9, which needs "
        "dbt-core >=1.8.0b3.",
    ),
    "sqlparse": (
        "0.6.0",
        "CVE-2026-71491, CVE-2026-59893 and CVE-2026-54284 -- three HIGH DoS records. "
        "dbt-core 1.11.14 is the first release to admit 0.6.0.",
    ),
    "urllib3": (
        "2.7.0",
        "CVE-2025-66418 (2.6.0) unbounded decompression chain, CVE-2025-66471 (2.6.0) "
        "streaming-API handling of highly compressed data, CVE-2026-21441 (2.6.3) "
        "decompression-bomb safeguards bypassed on redirects, CVE-2026-44431 (2.7.0) "
        "sensitive headers forwarded across origins in proxied low-level redirects. "
        "dbt-core 1.7 declared urllib3~=1.0, holding us on a major upstream has ended, "
        "so there was no patch-level escape. This is the HTTP client the whole product "
        "uses to reach customer sources -- not a build-time dependency.",
    ),
}


#: ``BLOCKED_BY_DBT_1_7`` lived here and is **deliberately gone** (core#825).
#: All six of its entries are now in ``SECURITY_FLOORS`` above, cleared by
#: moving dbt-core 1.7 -> 1.11, which in turn only needed the abandoned
#: ``dbt-mysql`` adapter dropped. The table is not kept as an empty dict: an
#: empty blocked-set is indistinguishable from a table nobody maintains, and
#: every assertion over it would pass vacuously. What replaces it is below --
#: a different deferral, with the same "fail the day the blocker lifts" shape,
#: because that shape is the part that worked.


#: capability -> (the package that would restore it, why it is unreachable).
#:
#: This is NOT an advisory table. Nothing here is a security exposure; it
#: records a **capability we ship documentation for and cannot currently
#: resolve**, so that it is re-checked rather than quietly forgotten.
#:
#: 🚨 The reason this needs a guard rather than a comment: the shape of this
#: conflict has now been mis-diagnosed three times in a row, each time by
#: someone reasoning from one half of it. Both halves are real, they sit on
#: either side of a version gap with no release in it, and each one names a
#: package that looks like the culprit and is not.
BLOCKED_BY_S3FS_CONFLICT: dict[str, tuple[str, str]] = {
    "s3fs": (
        "2026.1.0",
        "`s3://` bucket URLs on the file-source connector. dlt's filesystem source "
        "dispatches through fsspec, which maps the s3 protocol to s3fs specifically, so "
        "no s3fs means no s3:// -- while gs:// (gcsfs) and az:// (adlfs) keep working. "
        "TWO independent constraints exclude it, one on each side of a gap: "
        "(1) s3fs<=2025.12.0 requires aiobotocore<3.0.0, which caps botocore around "
        "1.41, but redshift-connector>=2.1.14 -- the CVE-2026-8838 RCE fix -- floors "
        "boto3>=1.42.22; "
        "(2) s3fs>=2026.1.0 requires fsspec>=2026.1, which drags gcsfs up to needing "
        "google-cloud-storage>=3.7.0, but every dbt-bigquery through 1.12.0 declares "
        "google-cloud-storage>=2.4,<3.2. "
        "Constraint (1) is ALREADY DISSOLVED upstream and is not the one to watch: "
        "aiobotocore 3.0.0 (2025-12-10) crossed into botocore 1.42 and s3fs 2026.1.0 "
        "(2026-01-09) raised its ceiling to aiobotocore<4.0.0; aiobotocore 3.9.0 and "
        "redshift-connector 2.1.16 resolve together in this tree today. Constraint (2) "
        "is the live one, and it is a BigQuery/S3 collision inside Google's own client "
        "libraries -- unrelated to security, and unrelated to dbt-core.",
    ),
}

#: The mechanical re-check trigger for ``BLOCKED_BY_S3FS_CONFLICT``.
#:
#: dbt-bigquery's ``google-cloud-storage<3.2`` ceiling is the live blocker, and
#: we cannot read dbt-bigquery's metadata offline -- but we can read its
#: *consequence*: while that ceiling is in force the resolver cannot take
#: google-cloud-storage to the 3.7+ that modern gcsfs (and therefore modern
#: s3fs) requires. So the day this lock carries google-cloud-storage >= 3.7,
#: the ceiling has moved and s3fs is worth re-testing.
#:
#: ⚠️ Deliberately NOT asserted as "s3fs is absent". That would be a tautology
#: over our own pyproject.toml -- it would fail only when somebody had already
#: done the work, which is the one moment a reminder is useless.
_S3FS_RECHECK_GCS_VERSION = "3.7.0"


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

    def test_the_dbt_pin_that_gates_six_of_these_floors_has_not_reverted(self):
        """Six floors above are only reachable at dbt-core >= 1.10 (core#825).

        Reverting the dbt declaration would make them unsatisfiable, and `uv
        lock` would say so -- but it would say it by naming ``cffi``, which is
        in none of the six advisories and points at nothing. That exact
        misdirection cost a day once already: ``cffi<2.0.0`` was declared
        independently by dbt-core 1.7 AND by snowflake-connector-python 3.x, so
        moving dbt to 1.8 alone produced a byte-identical error and reads as
        "the dbt pin is still the problem".

        This test is the sentence the resolver will not print.
        """
        specs = _declared_runtime_specs()
        dbt_core = next((s for s in specs if s.replace(" ", "").startswith("dbt-core")), None)
        assert dbt_core is not None, "dbt-core is no longer a declared dependency"

        floor = re.search(r">=\s*(\d+(?:\.\d+)*)", dbt_core)
        assert floor is not None, f"dbt-core spec {dbt_core!r} states no lower bound"
        assert Version(floor.group(1)) >= Version("1.10"), (
            f"dbt-core is declared as {dbt_core!r}, whose floor is below 1.10.\n"
            "cryptography, redshift-connector, sqlparse, urllib3, pyopenssl and "
            "protobuf in SECURITY_FLOORS are all unreachable below dbt-core "
            "1.10 -- cryptography specifically needs 1.10, not 1.8, because "
            "snowflake-connector-python 3.x pins cffi<2.0.0 independently of "
            "dbt and dbt-snowflake only admits the 4.x line from 1.10.6.\n"
            "If this pin genuinely has to come back down, those six floors have "
            "to move back into a blocked table in the same change -- not be "
            "left asserting versions the resolver can no longer produce."
        )

    def test_the_abandoned_adapter_has_not_come_back(self):
        """``dbt-mysql`` is what held the 1.7 pin, and it is still abandoned.

        Its last release is 1.7.0 (2024-04-26) and it declares
        ``dbt-core~=1.7.0``, so re-adding it silently drags the whole stack back
        to 1.7 and every floor above with it. A future engineer restoring MySQL
        transforms would be reintroducing six advisories, including a CRITICAL
        RCE, and nothing else in the tree would say so.
        """
        offenders = [
            s for s in _declared_runtime_specs() if s.replace(" ", "").startswith("dbt-mysql")
        ]
        assert not offenders, (
            f"dbt-mysql is declared again: {offenders}. Its newest release pins "
            "dbt-core~=1.7.0, which is incompatible with the >=1.10 floor the "
            "six SECURITY_FLOORS entries above depend on. There is no "
            "maintained MySQL dbt adapter on PyPI (checked 2026-08-31, "
            "core#825). If MySQL transforms must return, that needs an adapter "
            "decision, not a dependency line."
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


class TestDeferredCapability:
    """The capability we are knowingly not shipping, and why it is still stuck.

    Successor to ``TestKnownUnfixableAdvisories`` (core#825), which did its job:
    it fired the day the dbt pin moved and handed over six packages and their
    needed versions. Those are now in ``SECURITY_FLOORS``.

    The shape is kept because the shape is what worked -- **every assertion
    here goes red when the blocker lifts**, so the day `s3://` becomes
    resolvable again is the day someone is told, rather than a day four months
    from now when nobody remembers the trade was made.

    ⚠️ Do not "simplify" this into an assertion that s3fs is absent from
    pyproject.toml. That reads as equivalent and is the opposite: it would fail
    only *after* someone had already restored s3fs, i.e. only when the reminder
    is no longer needed. The assertion has to be on the *external* condition.
    """

    def test_the_blocker_for_the_deferred_capability_still_exists(self):
        """dbt-bigquery's google-cloud-storage ceiling is what still excludes s3fs.

        Read through its consequence in the lock rather than from dbt-bigquery's
        metadata, so the check needs no network and cannot pass because a fetch
        failed.
        """
        locked = _locked_versions()
        gcs = locked.get("google-cloud-storage")
        assert gcs is not None, (
            "google-cloud-storage is no longer in the lock. It was the package "
            "whose ceiling excluded s3fs, so this guard can no longer see its "
            "own blocker -- re-derive BLOCKED_BY_S3FS_CONFLICT from scratch "
            "rather than deleting this test."
        )
        assert Version(gcs) < Version(_S3FS_RECHECK_GCS_VERSION), (
            f"google-cloud-storage resolved to {gcs}, at or above "
            f"{_S3FS_RECHECK_GCS_VERSION}. That ceiling "
            "(dbt-bigquery: google-cloud-storage>=2.4,<3.2) was the LIVE reason "
            "these capabilities were unreachable:\n  "
            + "\n  ".join(
                f"{name}: needs >={need} -- {why}"
                for name, (need, why) in sorted(BLOCKED_BY_S3FS_CONFLICT.items())
            )
            + "\nRe-run `uv lock` with s3fs declared. If it resolves, restore "
            "the capability, re-enable the tests marked with "
            "`requires_s3fs`, and delete this table."
        )

    def test_the_deferred_set_and_the_floor_set_are_disjoint(self):
        """A package in both tables means one of them is stale."""
        both = sorted(set(SECURITY_FLOORS) & set(BLOCKED_BY_S3FS_CONFLICT))
        assert not both, (
            "A package is listed as both floored and deferred, so one table is "
            "wrong: " + ", ".join(both)
        )

    def test_each_deferred_package_is_still_actually_absent_from_the_lock(self):
        """If the resolver took it anyway, stop calling it deferred."""
        locked = _locked_versions()
        present = [
            f"{name} {locked[name]} is in the lock (deferral claims >={need} is "
            f"unreachable) -- {why}"
            for name, (need, why) in sorted(BLOCKED_BY_S3FS_CONFLICT.items())
            if name in locked
        ]
        assert not present, (
            "These are recorded as unreachable but the lock already carries "
            "them, so the deferral is stale and the capability may in fact be "
            "shipping untested:\n  " + "\n  ".join(present)
        )

    def test_the_deferral_records_a_capability_not_an_advisory(self):
        """This table must never quietly become a place to park a CVE.

        ``SECURITY_FLOORS`` exists for advisories and is asserted against the
        lock on every run. A security exposure filed here instead would be
        recorded as an accepted product trade-off and never re-derived.
        """
        for name, (_need, why) in BLOCKED_BY_S3FS_CONFLICT.items():
            offenders = [
                token
                for token in re.findall(r"\b(?:CVE|GHSA)-[\w-]+", why)
                if token != "CVE-2026-8838"  # named only as the CAUSE of the conflict
            ]
            assert not offenders, (
                f"{name}'s deferral names {offenders}, which suggests a security "
                "advisory is being carried as a capability trade-off. Advisories "
                "belong in SECURITY_FLOORS, which is checked against the lock."
            )


#: A fix version named inside a justification is written in parentheses --
#: ``"CVE-2026-26007 (46.0.5), ... CVE-2026-69247 ... (50.0.0)"``. That is the
#: convention ``TestFloorIsTheMaximumFixVersionNamed`` below relies on, and it
#: is what makes the tables mechanically checkable rather than merely prose.
_PARENTHESISED = re.compile(r"\(([^)]*)\)")
_BARE_VERSION = re.compile(r"\b\d+\.\d+(?:\.\d+)*\b")


def _fix_versions_named_in(reason: str) -> list[str]:
    """Fix versions a justification names, by the parenthesis convention.

    Deliberately narrow. Only dotted numerals inside parentheses count, so
    ``(CVE-2026-44244 bypasses CVE-2026-42215, ...)`` yields nothing -- a CVE id
    carries no dot -- and a version mentioned in running prose ("dbt-core 1.7.19
    declares sqlparse <0.6.0") is not mistaken for a fix version of *our*
    package.
    """
    found: list[str] = []
    for group in _PARENTHESISED.findall(reason):
        found.extend(_BARE_VERSION.findall(group))
    return found


class TestFloorIsTheMaximumFixVersionNamed:
    """A floor may never sit below a fix version its own justification names.

    This is the one mistake this whole file exists to prevent, and it had been
    made *inside* the file (core#843): ``cryptography`` recorded a floor of
    46.0.5 while its own reason went on to name 48.0.1, 49.0.0 and 50.0.0.

    It mattered because the table it guarded was not documentation -- it was
    the hand-off. The old ``BLOCKED_BY_DBT_1_7`` fired on the day the dbt pin
    moved and printed ``f"{name}: needs >={need}"``, so an understated floor was
    the number the next engineer actually applied. Taking 46.0.5 clears one of
    four HIGH records and reads as done, which is precisely the PyJWT (2.12.0 vs
    2.13.0) and GitPython (3.1.47 vs 3.1.58) failure the module docstring warns
    about.

    ⚠️ **That is not a historical note.** core#825 is the day that hand-off
    fired, and ``cryptography`` moved into ``SECURITY_FLOORS`` carrying the
    corrected 50.0.0 that this check forced. Had the check not existed, the
    floor applied tonight would have been 46.0.5 and three of the four HIGH
    records would have survived the bump silently.

    Only ``SECURITY_FLOORS`` is scanned now, and that is deliberate rather than
    an omission: it is the only remaining table whose numbers are *advisory fix
    versions*. ``BLOCKED_BY_S3FS_CONFLICT`` records a capability restore
    version, where "the highest number named in the prose" has no such meaning
    -- scanning it would assert something that is not true of it.
    ``TestDeferredCapability.test_the_deferral_records_a_capability_not_an_advisory``
    is what keeps an advisory from being filed there to dodge this check.

    Prose cannot be trusted to agree with a number that sits beside it; only a
    check can.
    """

    def test_no_recorded_floor_is_below_a_fix_version_it_names(self):
        understated = []
        for table_name, table in (("SECURITY_FLOORS", SECURITY_FLOORS),):
            for name, (floor, why) in sorted(table.items()):
                named = _fix_versions_named_in(why)
                if not named:
                    continue
                highest = max(named, key=Version)
                if Version(highest) > Version(floor):
                    understated.append(
                        f"{table_name}[{name!r}] records {floor} but its own "
                        f"reason names a fix at {highest} (all named: "
                        f"{', '.join(sorted(set(named), key=Version))})"
                    )
        assert not understated, (
            "A floor is below a fix version named in its own justification, so "
            "the table understates the version needed to clear the advisories "
            "it lists. Derive every floor as max(fixed) over ALL records "
            "affecting the shipped version -- never one advisory's fixed "
            "field:\n  " + "\n  ".join(understated)
        )
