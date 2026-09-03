"""What a red on `image-cve` means (core#835, core#836, core#819).

`image-cve` had been red on **every** `dev` push for weeks, and a permanently-red
check teaches everyone to read red as normal. That is not a hypothetical here:
the nightly connector smoke reported success over 12 failures for 41 runs, two
alert rules stayed green for weeks while structurally unable to fire, and an
`e2e-staging` wedge nearly went unread because six reds looked like the usual two.

The goal is therefore **not** that red disappears. It is that a red means *a new,
actionable finding*. Two halves, and this file guards both.

**Half one — stop reporting things that are not there** (`core#835`). 6 of the 11
HIGH findings, and 51 of 308 scanned targets, came from uv's download cache: an
unpacked `archive-v0/` tree in a single-stage image, whose `*.dist-info/METADATA`
files trivy reads as installed packages. Not one of them is importable from the
application. `TestTheImageDoesNotShipUvsDownloadCache` pins the fix and, more
importantly, pins its *ordering* — a clean placed before `uv run reflex init` is
silently undone.

**Half two — make the remainder legible.** What is left is real, and one of it
(`starlette`) we have decided not to fix yet. A waiver has to look different from
a fix, or the distinction it exists to draw is lost. Three ways to get that wrong:

1. **Make the job non-failing.** Deleting `exit-code: "1"`, or adding
   `continue-on-error`, produces a green that proves nothing — the single most
   repeated defect in this codebase. `TestTheGateStillFails` refuses it.
2. **Let a waiver reach the canary.** The canary scans the same image at every
   severity, unfixed included, and is *asserted to fail*; it is the only thing
   standing between "the gate is green" and "trivy did not run". A waiver that
   could suppress its findings could disarm it. `TestOnlyTheGateIsFiltered`
   refuses it.
3. **Waive without an expiry.** A waiver nobody revisits is indistinguishable
   from a fix, and reads better than one. `expired_at` makes trivy un-waive on
   its own, so the review date reviews itself rather than depending on someone
   remembering. `TestEveryWaiverIsAccountable` refuses an entry without one.

⚠️ **What this file cannot check** is whether a waiver's *reasoning* is still
true. It asserts the reasoning is present, cites an issue and expires; a human
still has to read it. That limit is why the expiry is the load-bearing part.
"""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_IGNOREFILE = _ROOT / ".trivyignore.yaml"
_CI_WORKFLOW = _ROOT / ".github" / "workflows" / "ci.yml"
_DOCKERFILE = _ROOT / "Dockerfile"

#: The CVE ids we have deliberately decided not to fix yet, and nothing else.
#:
#: This is a whitelist, not documentation. A waiver added to the YAML without
#: being added here fails, so "one more line in the ignore file" cannot be a
#: quiet way to turn the job green — which is the exact pressure a job that has
#: been red for weeks creates.
DECIDED_WAIVERS = {
    "CVE-2026-48818": "starlette StaticFiles UNC/NTLM — Windows-specific, prod is Linux",
    "CVE-2026-54283": "starlette request.form() limits ignored — memory DoS, reachable",
    "CVE-2026-41608": "thrift DoS — capped below the fix by databricks-sql-connector<0.21.0",
    "CVE-2026-43871": "thrift DoS — same pin, same decision",
}

#: Waived id -> the package its `paths` entries must name.
#:
#: Without this, `test_each_waiver_is_scoped_to_a_path` degenerates into "any
#: path at all", and a starlette waiver pointed at thrift would pass.
WAIVER_PACKAGE = {
    "CVE-2026-48818": "starlette",
    "CVE-2026-54283": "starlette",
    "CVE-2026-41608": "thrift",
    "CVE-2026-43871": "thrift",
}


def _ignorefile() -> dict:
    assert _IGNOREFILE.is_file(), (
        f"{_IGNOREFILE} is missing. CI passes it to trivy by path, and trivy "
        f"treats a missing ignore file as 'nothing ignored' rather than as an "
        f"error — so its absence looks exactly like a job that got worse."
    )
    return yaml.safe_load(_IGNOREFILE.read_text(encoding="utf-8")) or {}


def _waivers() -> list[dict]:
    return _ignorefile().get("vulnerabilities") or []


def _ci() -> dict:
    return yaml.safe_load(_CI_WORKFLOW.read_text(encoding="utf-8"))


def _image_cve_steps() -> list[dict]:
    job = _ci()["jobs"]["image-cve"]
    return job["steps"]


def _step_named(fragment: str) -> dict:
    matches = [s for s in _image_cve_steps() if fragment.lower() in (s.get("name") or "").lower()]
    assert len(matches) == 1, (
        f"expected exactly one image-cve step whose name contains {fragment!r}, "
        f"found {[s.get('name') for s in matches]}. A renamed step makes every "
        f"assertion below vacuous rather than red."
    )
    return matches[0]


class TestTheImageDoesNotShipUvsDownloadCache:
    """core#835 — the majority of the signal was packages that are not installed.

    The build is single-stage, so `/root/.cache/uv` ships. It holds unpacked
    `archive-v0/` trees and `sdists-v9/` sources, each carrying a real
    `*.dist-info/METADATA` or `*.egg-info/PKG-INFO`, and trivy's python analyzer
    reads those as **installed packages**.

    Measured on the `image-cve` run for `dev 89e7e2b`: **51 of 308** scanned
    targets were cache paths, and **6 of the 11 HIGH findings** came from
    packages the application cannot import. `lxml` is the clearest instance —
    the venv ships 6.1.2 (floored in `pyproject.toml` for CVE-2026-41066) while
    the cache still holds the 6.0.2 sdist the build resolved through, so the
    scanner reported a CVE we had already fixed. `jaraco.context` and `wheel`
    are worse still: setuptools' *vendored* copies, inside a cache entry.

    ⚠️ This does not turn the job green on its own, and saying so matters —
    the routing brief guessed it might. Five findings remain in the venv that
    genuinely ships.
    """

    def _dockerfile(self) -> str:
        """The Dockerfile's INSTRUCTIONS, with comment lines removed.

        🚨 Not decoration — mutation caught two of these assertions passing
        against a Dockerfile whose fix had been deleted. The comment block
        explaining the change quotes both `uv cache clean` and
        `/root/.cache/uv`, so a whole-file substring search stayed green after
        the `RUN` line was replaced with `RUN true`.

        That is WORKFLOW_RULES §4's counting trap — *"a guide corrected to deny
        an old behaviour still contains the old phrase"* — arriving from the
        other direction: here the prose DOCUMENTING a fix satisfied the test for
        the fix. Comments are where the reasoning goes, so the better the
        comment, the more likely it fools a naive grep.
        """
        assert _DOCKERFILE.is_file(), _DOCKERFILE
        lines = _DOCKERFILE.read_text(encoding="utf-8").splitlines()
        return "\n".join(ln for ln in lines if not ln.lstrip().startswith("#"))

    def test_the_comment_stripper_actually_strips(self):
        """Negative control for the helper above.

        If `_dockerfile()` ever returned the raw text again, every assertion in
        this class would silently go back to matching prose. If it stripped too
        much, they would all pass vacuously against an empty string.
        """
        raw = _DOCKERFILE.read_text(encoding="utf-8")
        stripped = self._dockerfile()

        assert "core#835" in raw, "the explanatory comment is gone from the Dockerfile"
        assert "core#835" not in stripped, "comments are not being stripped"
        assert "FROM python:3.12-slim" in stripped, "instructions were stripped too"

    def test_the_build_clears_the_cache(self):
        text = self._dockerfile()

        assert "uv cache clean" in text, (
            "the image ships uv's download cache, so `image-cve` reports CVEs "
            "for packages that are not installed — including ones we fixed"
        )

    def test_the_clean_runs_after_every_uv_command_that_populates_it(self):
        """Order is the whole correctness of this change.

        `uv run reflex init` re-syncs from the lock, so a clean placed before it
        is undone. A clean placed before any `uv pip install` is undone twice.
        """
        text = self._dockerfile()

        clean_at = text.index("uv cache clean")
        populators = [
            text.rindex("uv sync --frozen"),
            text.rindex("uv run reflex init"),
            text.rindex("uv pip install"),
        ]

        assert clean_at > max(populators), (
            "`uv cache clean` runs before something that repopulates the cache, "
            "so the cache is back in the image by the time it is scanned"
        )

    def test_the_build_asserts_the_cache_is_actually_gone(self):
        """A command that is expected to remove something must be checked, in
        the artifact, or it is a hope. Same reasoning as the `/mcp` import
        assertion two lines below it in the Dockerfile (core#602)."""
        text = self._dockerfile()

        assert "/root/.cache/uv" in text, (
            "nothing verifies the cache is absent from the built image. "
            "`uv cache clean` exiting 0 is not evidence: it respects "
            "UV_CACHE_DIR, so it can succeed having cleaned a different "
            "directory than the one that ships."
        )

    def test_the_venv_import_assertion_still_runs_after_the_clean(self):
        """The control for the clean, and the reason it is safe.

        `uv sync` hardlinks into the venv, so removing the cache's link leaves
        the data alive under the venv's. That is a claim, and this is what
        checks it: if `uv cache clean` ever did gut the venv, the existing
        `/mcp` import assertion fails the BUILD — before anything reaches a
        registry, let alone production.
        """
        text = self._dockerfile()

        clean_at = text.index("uv cache clean")
        import_assert_at = text.rindex("import datanika_mcp.server")

        assert import_assert_at > clean_at, (
            "the import assertion no longer runs after the cache clean, so "
            "nothing in the build would notice if cleaning broke the venv"
        )

    def test_something_in_the_build_still_populates_a_cache(self):
        """Anti-vacuity. Every assertion above is satisfied by a Dockerfile that
        installs nothing at all."""
        text = self._dockerfile()

        assert "uv sync --frozen" in text and "uv pip install" in text


class TestEveryWaiverIsAccountable:
    def test_the_ignore_file_parses(self):
        """A malformed ignore file makes trivy exit non-zero for a reason that
        has nothing to do with security, which is a red that means the wrong
        thing — the failure mode this whole change exists to remove."""
        doc = _ignorefile()

        assert isinstance(doc, dict), doc
        assert "vulnerabilities" in doc

    def test_the_waived_set_is_exactly_what_was_decided(self):
        """No id may be waived without being listed here first."""
        waived = {w["id"] for w in _waivers()}

        assert waived == set(DECIDED_WAIVERS), (
            f"the ignore file waives {sorted(waived)} but the decided set is "
            f"{sorted(DECIDED_WAIVERS)}. Adding a line to the YAML is not the "
            f"decision; this table is."
        )

    @pytest.mark.parametrize("cve", sorted(DECIDED_WAIVERS))
    def test_each_waiver_states_a_reason_and_cites_an_issue(self, cve):
        """'Accepted risk' is not a reason. The next reader must be able to
        re-derive the judgement without re-doing the investigation."""
        entry = next(w for w in _waivers() if w["id"] == cve)

        statement = (entry.get("statement") or "").strip()
        assert len(statement) > 120, f"{cve}: statement is too short to be a reason: {statement!r}"
        assert "core#" in statement, f"{cve}: statement cites no tracking issue"

    @pytest.mark.parametrize("cve", sorted(DECIDED_WAIVERS))
    def test_each_waiver_expires(self, cve):
        """The review date that reviews itself.

        On `expired_at` trivy stops honouring the entry and `image-cve` goes red
        again with no human involved. That is deliberately the *only* automatic
        consequence: this test does NOT fail on expiry, because it runs in the
        required `test` job and would block every merge in the repo over a
        third-party advisory nobody in the PR introduced — the exact reason
        `image-cve` is non-required in the first place.
        """
        entry = next(w for w in _waivers() if w["id"] == cve)

        expires = entry.get("expired_at")
        assert expires is not None, (
            f"{cve} has no `expired_at`. A waiver with no expiry is a decision "
            f"nobody revisits, and it reads better than a fix."
        )
        assert isinstance(expires, dt.date), (
            f"{cve}: `expired_at` parsed as {type(expires).__name__} "
            f"({expires!r}), not a date. Quoting it makes trivy ignore the "
            f"expiry silently, so the waiver becomes permanent while looking "
            f"time-boxed."
        )

    @pytest.mark.parametrize("cve", sorted(DECIDED_WAIVERS))
    def test_each_waiver_is_scoped_to_a_path(self, cve):
        """A bare id waives that CVE *everywhere*, including against a package
        it was never assessed for."""
        entry = next(w for w in _waivers() if w["id"] == cve)

        paths = entry.get("paths") or []
        assert paths, f"{cve} is waived globally rather than for a named path"
        pkg = WAIVER_PACKAGE[cve]
        assert all(pkg in p for p in paths), (
            f"{cve} is waived for {paths}, which does not name {pkg!r}. A "
            f"waiver scoped to the wrong package suppresses the finding it was "
            f"assessed for nowhere, and something else everywhere."
        )


class TestTheGateStillFails:
    """The founder's constraint, mechanised: do not make the job non-failing.

    Every assertion here describes a one-line edit that turns `image-cve`
    permanently green while looking like housekeeping.
    """

    def test_the_gate_step_still_exits_non_zero_on_a_finding(self):
        gate = _step_named("Scan the built image")

        assert str(gate["with"].get("exit-code")) == "1", (
            "the gate no longer fails on a finding, so `image-cve` reports "
            "green whatever trivy finds"
        )

    def test_the_gate_step_does_not_swallow_its_own_failure(self):
        gate = _step_named("Scan the built image")

        assert "continue-on-error" not in gate, (
            "`continue-on-error` on the gate quarantines the verdict: the scan "
            "runs, findings are printed, and the job goes green anyway"
        )

    def test_the_gate_has_not_been_widened_to_ignore_everything(self):
        """`ignore-unfixed: true` is deliberate and narrow — an unfixable CVE is
        a base-image decision, not an action for a PR author. Flipping severity
        to CRITICAL-only, or dropping HIGH, would silently retire every finding
        this job has ever produced: the run that motivated it was 61 findings,
        **0 of them CRITICAL**."""
        gate = _step_named("Scan the built image")

        severities = {s.strip() for s in str(gate["with"]["severity"]).split(",")}
        assert "HIGH" in severities and "CRITICAL" in severities, severities

    def test_the_canary_and_its_assertion_are_both_still_there(self):
        """Two steps, and removing either one is enough. Without the canary a
        green gate cannot distinguish 'no findings' from 'trivy never ran';
        without the assertion the canary's own result is discarded."""
        canary = _step_named("prove the scanner can actually fail")
        assertion = _step_named("Assert the canary went red")

        assert canary.get("continue-on-error") is True, (
            "the canary must tolerate its own (expected) failure, or the job "
            "dies before the assertion can read its outcome"
        )
        assert "steps.canary.outcome" in assertion["run"], (
            "the assertion no longer reads the canary's outcome, so it passes "
            "whatever the canary did"
        )


class TestOnlyTheGateIsFiltered:
    """A waiver must never be able to reach the canary."""

    def test_the_gate_is_given_the_ignore_file(self):
        gate = _step_named("Scan the built image")

        ignores = gate["with"].get("trivyignores")
        assert ignores, (
            "the gate is not passed `trivyignores`, so the waiver file is "
            "inert. ⚠️ trivy does NOT find it on its own here: it looks in the "
            "working directory, and this job checks the repo out to `core/`, "
            "so an unreferenced ignore file at the repo root is never read — "
            "and the job simply stays red, which looks identical to the "
            "waivers not having been decided."
        )
        assert _IGNOREFILE.name in str(ignores), ignores

    def test_the_canary_is_not_given_the_ignore_file(self):
        """The one assertion in this file that protects the instrument itself."""
        canary = _step_named("prove the scanner can actually fail")

        assert "trivyignores" not in (canary.get("with") or {}), (
            "the canary is being filtered. It exists to prove trivy can fail at "
            "all, and it is the reason a green gate is evidence of anything. "
            "Filtering it means a waiver can disarm the check that validates "
            "every other result in this job."
        )

    def test_the_ignore_path_matches_where_the_repo_is_checked_out(self):
        """The path is relative to the runner's workspace, not to the repo.

        `Checkout core (this repo)` uses `path: core`, so `.trivyignore.yaml`
        lives at `core/.trivyignore.yaml` on disk. A bare `.trivyignore.yaml`
        resolves to nothing and trivy treats a missing ignore file as 'nothing
        to ignore' — no warning, no error, job stays red.
        """
        checkout = _step_named("Checkout core")
        gate = _step_named("Scan the built image")

        prefix = checkout["with"]["path"].strip("/")
        assert str(gate["with"]["trivyignores"]).startswith(f"{prefix}/"), (
            f"trivyignores must be prefixed with the checkout path {prefix!r}: "
            f"got {gate['with']['trivyignores']!r}"
        )


class TestEveryGuardNamedHereExists:
    """A comment that names a guard must name one that exists.

    Every file in this change argues the same point: a signal is only worth
    reading if it is *discriminating*. The prose carries a lot of that weight —
    `.trivyignore.yaml` tells the next reader which test refuses an unexpiring
    waiver, and the `Dockerfile` tells them which test pins the cache-clean
    ordering. A reader who follows one of those pointers and finds nothing
    concludes the guard does not exist, and the honest conclusion from that is
    "this constraint is unenforced" — which is worse than silence, because the
    comment spent their trust before failing them.

    That is not hypothetical: this class was added because `.trivyignore.yaml`
    twice named `test_image_cve_waivers.py`, a file that has never existed. The
    guard it described is real and lives in *this* file; only the name was
    wrong. Nothing else in the repo could have caught it — the YAML parses, the
    workflow runs, and every assertion the comment advertises genuinely passes.
    """

    _SOURCES = (_IGNOREFILE, _DOCKERFILE, _CI_WORKFLOW)

    @staticmethod
    def _refs(text: str) -> tuple[set[str], set[str]]:
        """Split prose references into repo-relative paths and bare filenames."""
        full = set(re.findall(r"tests/[A-Za-z0-9_/]+\.py", text))
        bare = set(re.findall(r"\btest_[A-Za-z0-9_]+\.py", text))
        # A bare name that is merely the tail of a full path is already covered.
        bare = {b for b in bare if not any(f.endswith(f"/{b}") for f in full)}
        return full, bare

    def test_the_scan_sees_the_prose(self):
        """Negative control.

        Every assertion below is of the form "everything found resolves", which
        a scan finding nothing passes trivially. If a rename or a rewrite ever
        empties this, the two tests below stop testing anything while staying
        green — the precise failure mode this whole file exists to refuse.
        """
        found = {}
        for src in self._SOURCES:
            assert src.is_file(), f"{src} is missing; the scan below would be vacuous"
            full, bare = self._refs(src.read_text(encoding="utf-8"))
            found[src.name] = full | bare

        assert found[".trivyignore.yaml"], (
            "no test file is named in the waiver file's prose. Either the "
            "pointers were removed, or the pattern stopped matching them."
        )
        assert found["Dockerfile"], "no test file is named in the Dockerfile's prose"
        assert found["ci.yml"], "no test file is named in ci.yml's prose"

    @pytest.mark.parametrize("src", _SOURCES, ids=lambda p: p.name)
    def test_every_repo_relative_path_named_in_prose_exists(self, src: Path):
        full, _ = self._refs(src.read_text(encoding="utf-8"))
        missing = sorted(ref for ref in full if not (_ROOT / ref).is_file())
        assert not missing, (
            f"{src.name} points readers at {missing}, which do not exist. A "
            f"pointer to a missing guard reads as 'this rule is unenforced'. "
            f"Fix the NAME if the guard moved; do not delete the pointer."
        )

    @pytest.mark.parametrize("src", _SOURCES, ids=lambda p: p.name)
    def test_every_bare_test_filename_named_in_prose_exists(self, src: Path):
        _, bare = self._refs(src.read_text(encoding="utf-8"))
        tests_root = _ROOT / "tests"
        missing = sorted(name for name in bare if not any(tests_root.rglob(name)))
        assert not missing, (
            f"{src.name} names {missing}, which exist nowhere under tests/. "
            f"A bare filename is still a pointer; it just costs the reader a "
            f"grep before it disappoints them."
        )
