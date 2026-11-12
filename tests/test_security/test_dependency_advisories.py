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
    "aiohttp": (
        "3.14.4",
        "CVE-2026-69244 out-of-bounds heap read, then CVE-2026-111064 (3.14.4) missing "
        "initial request timeout and CVE-2026-111066 multipart descriptor exhaustion. "
        "image-cve run 42700808834.",
    ),
    "gitpython": (
        "3.2.1",
        "CVE-2026-114145 (3.2.1) submodule path validation. The earlier chain through "
        "3.1.58 stays cleared; GHSA-23mf-xhv8-69c2 is fixed in 3.2.0, below this floor. "
        "Reached via dbt-core. image-cve run 42700808834.",
    ),
    "granian": ("2.7.4", "CVE-2026-42544 unauthenticated DoS via a WebSocket subprotocol header"),
    "lxml": ("6.1.0", "CVE-2026-41066 iterparse()/ETCompatXMLParser() default config allows XXE"),
    "mako": (
        "1.3.12",
        "CVE-2026-41205 (1.3.11) and CVE-2026-44307 (1.3.12) TemplateLookup path traversal",
    ),
    "msgpack": (
        "1.2.3",
        "GHSA-6v7p-g79w-8964 out-of-bounds read on Unpacker reuse, then CVE-2026-114613 "
        "(1.2.3) memory leak during map deserialization. Reached via mashumaro under dbt. "
        "kombu declares msgpack==1.1.2 under its optional `msgpack` extra, which this tree "
        "does not enable, so that pin never enters the resolution.",
    ),
    "starlette": (
        "1.3.1",
        "CVE-2026-48818 (1.1.0) and CVE-2026-54283 (1.3.1). The major under Reflex 0.8.26 is "
        "measured on core#1347; these were the two image-cve waivers, retired with this floor.",
    ),
    "pyasn1": (
        "0.6.5",
        "CVE-2026-30922 was fixed in 0.6.4. CVE-2026-119194 (0.6.5), CVE-2026-119195, "
        "CVE-2026-119196 and CVE-2026-119197 are fixed in that same release.",
    ),
    "pyjwt": (
        "2.14.0",
        "CVE-2026-102268 (2.14.0) public-key validation bypass. The same release fixes "
        "CVE-2026-102266, CVE-2026-102267, CVE-2026-102271, CVE-2026-102272, "
        "CVE-2026-102273 and GHSA-pxh4-856f-4h89. 2.13.0 fixed only CVE-2026-48526.",
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
    "snowflake-sqlalchemy": (
        "1.11.0",
        "CVE-2026-15736 / GHSA-8g6f-qw9x-4q6q (1.11.0) -- SQL injection and local file "
        "disclosure, HIGH. Affects every release from 1.1.6, so every version this "
        "project has ever shipped was inside it. This is the SQLAlchemy dialect that "
        "backs Snowflake as an extract SOURCE, so it runs against a warehouse whose "
        "credentials an org handed us. "
        "⚠️ Unlike almost every other entry in this table it is a DIRECT dependency: "
        "the floor that actually moves the resolver is the pin in "
        "[project.dependencies], and this row is the durable re-check, not the "
        "mechanism. A constraint-dependencies entry would be the wrong tool -- that "
        "block is for transitive packages we do not declare, and adding a second "
        "authority for a package that already has one is how two numbers drift apart.",
    ),
    "soupsieve": ("2.8.4", "CVE-2026-49476 memory exhaustion and CVE-2026-49477 ReDoS"),
    "thrift": (
        "0.24.0",
        "CVE-2026-41608 (0.24.0) DoS via deeply nested structures and CVE-2026-43871 (0.24.0) "
        "DoS via an infinite loop, both HIGH, in the Thrift client under "
        "databricks-sql-connector. Moved here from BLOCKED_BY_DATABRICKS_THRIFT_CEILING: "
        "connector 4.2.5 declared thrift<0.21.0, and 4.4.0 raised that bound to "
        ">=0.24.0,<0.25.0. The connector is now locked at 4.5.0.",
    ),
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
        "2.8.0",
        "CVE-2025-66418 (2.6.0) unbounded decompression chain, CVE-2025-66471 (2.6.0) "
        "streaming-API handling of highly compressed data, CVE-2026-21441 (2.6.3) "
        "decompression-bomb safeguards bypassed on redirects, CVE-2026-44431 (2.7.0) "
        "sensitive headers forwarded across origins in proxied low-level redirects, "
        "CVE-2026-97687 (2.8.0) HTTPS proxy TLS configuration override and CVE-2026-97689 "
        "unbounded chunk-parser allocation. This is the HTTP client the whole product "
        "uses to reach customer sources -- not a build-time dependency.",
    ),
    "certifi": (
        "2026.10.18",
        "GHSA-6q48-6c7w-9gw2 (2026.10.18) emSign root CAs with compromised intermediate "
        "keys. Transitive via requests and snowflake-connector-python.",
    ),
    "dnspython": (
        "2.9.0",
        "CVE-2026-114605 (2.9.0) DoS via excessive DNS compression pointer traversal. "
        "pymongo 4.18.3 allows dnspython<3,>=2.7.0.",
    ),
    "filelock": (
        "4.2.1",
        "CVE-2026-112940 (4.2.1) arbitrary file overwrite via insecure database file "
        "reopening. No 3.x fix. snowflake-connector-python 4.7.2 requires filelock<4; "
        "4.8.1 allows filelock<5, so that connector moves to 4.8.1 with this floor.",
    ),
    "oauthlib": (
        "4.0.1",
        "CVE-2026-118669 (4.0.0) client-identity confusion and CVE-2026-49272 (4.0.1) "
        "missing device-code validation. databricks-sql-connector 4.5.0 requires "
        "oauthlib<4; 4.7.0 allows oauthlib<5, so that connector moves with this floor.",
    ),
    "pymongo": (
        "4.18.3",
        "CVE-2026-106435 (4.18.3) out-of-bounds read on malformed BSON. CVE-2026-96748 "
        "and CVE-2026-96749 are fixed in 4.18.2, below this floor. Direct dependency: "
        "the pin in [project.dependencies] is what moves the resolver.",
    ),
    # ---- A lock refresh to the first patched release of each (2026-09-17) ---------------------
    # Each moved to exactly the version its advisory names, and nothing else in the lock moved.
    "ecdsa": (
        "0.19.2",
        "CVE-2026-33936 (0.19.2) DoS via improper DER length validation in crafted private keys. "
        "Separate from CVE-2024-23342, which has no fixed release.",
    ),
    "idna": ("3.15", "CVE-2026-45409 (3.15) crafted idna.encode() input bypasses an earlier fix"),
    "pydantic-settings": (
        "2.14.2",
        "GHSA-4xgf-cpjx-pc3j (2.14.2) the nested secrets source follows symlinks out of "
        "secrets_dir",
    ),
    "pygments": ("2.20.0", "CVE-2026-4539 (2.20.0) ReDoS in GUID matching"),
    "pytest": ("9.0.3", "CVE-2025-71176 (9.0.3) tmpdir handling. A dev dependency, not imaged"),
    "python-dotenv": ("1.2.2", "CVE-2026-28684 (1.2.2) symlink following in set_key"),
    "requests": ("2.33.0", "CVE-2026-25645 (2.33.0) temp file reuse in extract_zipped_paths()"),
    "setuptools": ("83.0.0", "CVE-2026-59890 (83.0.0) MANIFEST.in exclusion bypass in an sdist"),
    "fsspec": (
        "2026.6.0",
        "CVE-2026-104851 (2026.6.0) / GHSA-27vj-qcqg-25rc server-side template injection "
        "in ReferenceFileSystem. gcsfs 2025.12.0 selects the vulnerable release. gcsfs "
        "2026.4.0 is the first published release that accepts this fsspec, and it requires "
        "google-cloud-storage>=3.9, which published dbt-bigquery 1.12.3 allows. Reached "
        "via dlt's bigquery extra. image-cve run 42931652036.",
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


#: capability -> (the package that would restore it, why this change does not).
#:
#: This is NOT an advisory table. Nothing here is a security exposure.
#: Published s3fs 2026.6.0 resolves against this lock and is still not declared.
#: The old "gap with no release" description does not survive dbt-bigquery 1.12.3.
BLOCKED_BY_S3FS_CONFLICT: dict[str, tuple[str, str]] = {
    "s3fs": (
        "2026.6.0",
        "`s3://` bucket URLs on the file-source connector. dlt's filesystem source "
        "dispatches through fsspec, which maps the s3 protocol to s3fs specifically, so "
        "no s3fs means no s3:// -- while gs:// (gcsfs) and az:// (adlfs) keep working. "
        "The two old constraints are dissolved, and neither is the live blocker. "
        "(1) s3fs<=2025.12.0 required aiobotocore<3.0.0, against the boto3 floor in "
        "redshift-connector>=2.1.14 (the CVE-2026-8838 RCE fix). aiobotocore 3.x and "
        "that connector resolve together. "
        "(2) s3fs>=2026.1.0 required fsspec>=2026.1 and, through gcsfs, "
        "google-cloud-storage>=3.7. dbt-bigquery 1.12.0, 1.12.1 and 1.12.2 declare "
        "google-cloud-storage>=2.4,<3.2. Published 1.12.3 declares >=2.4,<4.0, and "
        "this lock takes 1.12.3. Calling <3.2 the live blocker describes 1.12.2 and "
        "earlier. Published s3fs 2026.6.0 requires fsspec>=2026.6.0,<2026.6.1, the "
        "fsspec this lock holds, and a scratch resolve of that release was accepted. "
        "It is not in this change. Restoring s3:// means declaring s3fs, putting s3 "
        "back into SOURCE_TYPES, and re-enabling the requires_s3fs tests.",
    ),
}

#: ``BLOCKED_BY_DATABRICKS_THRIFT_CEILING`` lived here and is **deliberately gone**, like
#: ``BLOCKED_BY_DBT_1_7`` above. Its one entry, ``thrift``, is in ``SECURITY_FLOORS``:
#: ``databricks-sql-connector`` 4.4.0 raised its ``thrift`` bound past the fix version, and the
#: connector is now locked at 4.5.0.
#:
#: ⚠️ Recorded so the next deferral of this shape knows what its guard cannot see. The guard,
#: ``TestThriftDeferralStillHolds``, read the ceiling from the **installed** connector's metadata,
#: so it could only fire after somebody had already bumped the connector. It never saw 4.4.0
#: (2026-07-22) lift the bound upstream, and the deferral stood for seven weeks after its reason
#: had gone. Reading the installed distribution was right about not trusting a number typed into
#: this file; it was blind to the third party changing without us.


#: The old mechanical trigger for ``BLOCKED_BY_S3FS_CONFLICT`` was
#: ``google-cloud-storage < 3.7``. That was the consequence of dbt-bigquery
#: 1.12.0 through 1.12.2 declaring ``google-cloud-storage>=2.4,<3.2``. Published
#: 1.12.3 allows ``<4.0``. The lock is now at or above 3.7, which is the fact
#: the assertion below guards. s3fs stays undeclared on purpose; absence is
#: checked by ``test_each_deferred_package_is_still_actually_absent_from_the_lock``.
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

    The storage ceiling that used to make ``s3://`` unresolvable has lifted.
    The assertions below record that fact and still fail if s3fs appears in
    the lock while this table still calls it deferred.

    ⚠️ Do not "simplify" this into an assertion that s3fs is absent from
    pyproject.toml. That reads as equivalent and is the opposite: it would fail
    only *after* someone had already restored s3fs, i.e. only when the reminder
    is no longer needed. The assertion has to be on the *external* condition.
    """

    def test_the_storage_ceiling_that_excluded_s3fs_has_lifted(self):
        """dbt-bigquery 1.12.3 removed the <3.2 ceiling. s3fs is still not added.

        Read the lock, not dbt-bigquery's metadata, so the check needs no
        network. google-cloud-storage below 3.7 would mean that ceiling is
        back and this table's reason is stale in the other direction.
        """
        locked = _locked_versions()
        gcs = locked.get("google-cloud-storage")
        assert gcs is not None, (
            "google-cloud-storage is no longer in the lock. Re-derive "
            "BLOCKED_BY_S3FS_CONFLICT rather than deleting this test."
        )
        assert Version(gcs) >= Version(_S3FS_RECHECK_GCS_VERSION), (
            f"google-cloud-storage resolved to {gcs}, below "
            f"{_S3FS_RECHECK_GCS_VERSION}. dbt-bigquery 1.12.0 through 1.12.2 "
            "declare google-cloud-storage>=2.4,<3.2; published 1.12.3 allows "
            "<4.0. A lock back under 3.7 makes the old ceiling the blocker "
            "again:\n  "
            + "\n  ".join(
                f"{name}: needs >={need} -- {why}"
                for name, (need, why) in sorted(BLOCKED_BY_S3FS_CONFLICT.items())
            )
        )
        bigquery = locked.get("dbt-bigquery")
        assert bigquery is not None and Version(bigquery) >= Version("1.12.3"), (
            f"dbt-bigquery resolved to {bigquery}. 1.12.3 is the first 1.12 "
            "release that allows google-cloud-storage<4.0."
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
            f"{name} {locked[name]} is in the lock while the deferral still "
            f"withholds it (would need >={need}) -- {why}"
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
