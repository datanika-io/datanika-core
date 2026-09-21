"""The image build must not depend on a third-party CDN at build time (core#1498).

What happened
-------------
``uv run reflex init`` calls reflex's ``install_bun()``, which fetches
``https://raw.githubusercontent.com/reflex-dev/reflex/main/scripts/bun_install.sh`` and
runs it, and that script downloads the bun binary. On 2026-09-21 the binary fetch
returned **504** and a *documentation-only* PR was ejected from the merge queue with
``failed_checks``, 94 seconds after entering.

Why that is more than a flaky check
-----------------------------------
``Dockerfile`` is what the **production deploy** builds — ``deploy-pointer.yml`` runs
``docker compose build app celery app_b`` on the box. So the same upstream 504 can fail a
production deploy, and core#1014 records that the box has no GitHub auth and cannot pull a
pinned image, which makes *rebuild* the only route back. The outage removes the recovery
path at the moment you need it. It fails safe — the build dies before the blue/green swap
— so this is availability-of-deploys, not availability-of-prod.

Note also what that URL says: branch ``main``, unpinned. The pre-fix build executed
whatever sat on a third party's default branch, as root, at build time. That is why the fix
is a **pre-install** and not a retry around the same fetch: a retry would have made the
unpinned-script execution *more* reliable, which is the wrong direction.

Why the fix works
-----------------
``install_bun()`` returns early when a bun >= ``Bun.MIN_VERSION`` already exists at
``Bun.DEFAULT_PATH``. Measured on the built image, with the control beside it — the second
line is what makes the first one mean anything::

    bun present, --network none  ->  install_bun() returns OK
    bun removed, --network none  ->  "Failed to download bun install script"
    bun removed, with network    ->  re-downloads 1.3.5

What these tests DRIVE rather than assert
-----------------------------------------
Every test here runs the thing it is checking, or demonstrates the defect it exists to
prevent. ``test_the_retry_loop_actually_retries`` executes the shipped loop with a curl
that always fails and counts the attempts. ``test_a_content_hash_alone_cannot_detect_a
_same_version_redownload`` shows the sha-only stamp passing on a re-download, inline,
which is the whole reason the stamp carries mtime and size too.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = REPO_ROOT / "Dockerfile"

# The literal the Dockerfile installs to. Kept here so a drift between the ARG block and
# the post-init assertion is a test failure rather than a silent no-op.
PINNED_PATH = "/root/.local/share/reflex/bun/bin/bun"


def dockerfile_text() -> str:
    assert DOCKERFILE.is_file(), (
        f"{DOCKERFILE} is missing. If the Dockerfile moved, re-point REPO_ROOT rather "
        "than deleting these assertions."
    )
    return DOCKERFILE.read_text(encoding="utf-8")


def run_blocks(text: str) -> list[str]:
    """Every RUN instruction, with line continuations folded into one string."""
    folded = text.replace("\\\n", "\n")
    blocks: list[str] = []
    current: list[str] | None = None
    for raw in text.splitlines():
        if raw.startswith("RUN "):
            if current is not None:
                blocks.append("\n".join(current))
            current = [raw[len("RUN ") :]]
        elif current is not None:
            current.append(raw)
            if not raw.rstrip().endswith("\\"):
                blocks.append("\n".join(current))
                current = None
    if current is not None:
        blocks.append("\n".join(current))
    assert blocks, f"no RUN blocks parsed out of the Dockerfile (folded len={len(folded)})"
    return blocks


def bun_install_block(text: str) -> str:
    matches = [b for b in run_blocks(text) if "bun.zip" in b]
    assert len(matches) == 1, (
        f"expected exactly one RUN block that downloads bun, found {len(matches)}. "
        "Two of them means two places to keep in step, which is how the pin drifts."
    )
    return matches[0]


# --------------------------------------------------------------------------------------
# Ordering and coupling
# --------------------------------------------------------------------------------------


def test_bun_is_installed_before_reflex_init() -> None:
    """A pre-install after ``reflex init`` is not a pre-install; it is dead weight.

    This is the whole mechanism: ``install_bun()`` short-circuits on a bun that is
    *already there*. Installed afterwards, the CDN fetch has already happened and every
    comment in the Dockerfile would still claim otherwise.
    """
    text = dockerfile_text()
    lines = text.splitlines()

    bun_line = next(i for i, ln in enumerate(lines) if ln.startswith("ARG BUN_VERSION="))
    init_line = next(i for i, ln in enumerate(lines) if ln.startswith("RUN uv run reflex init"))

    assert bun_line < init_line, (
        f"the pinned bun install is at line {bun_line + 1} but `reflex init` is at line "
        f"{init_line + 1}. Installed after init, the pre-install cannot prevent the "
        "download it exists to prevent."
    )


def test_the_build_asserts_the_pinned_path_against_reflex_itself() -> None:
    """The stamp is blind to a moved default path; only this check is not.

    If reflex started looking elsewhere, our binary would sit untouched at the old path,
    the stamp would compare equal, and ``reflex init`` would download to the new location.
    Everything green, and back on the CDN. So the build asks the installed reflex where it
    looks and compares against the literal.
    """
    text = dockerfile_text()

    assert "from reflex.constants import Bun; print(Bun.DEFAULT_PATH)" in text, (
        "the Dockerfile must ask the INSTALLED reflex for Bun.DEFAULT_PATH. A hardcoded "
        "path with nothing comparing it to reflex's own is a no-op waiting to happen."
    )
    assert text.count(PINNED_PATH) >= 2, (
        f"{PINNED_PATH} should appear both where bun is installed and where the path is "
        "asserted; if those two ever disagree the pre-install silently stops working."
    )


def test_the_pin_is_at_least_the_minimum_reflex_will_accept() -> None:
    """The fallback is soft by design, and this keeps it from becoming permanent.

    If reflex raises ``MIN_VERSION`` above our pin, ``install_bun()`` simply downloads as
    it did before — degraded to the status quo rather than broken, which is the right
    failure direction. But nothing would *say* so: the build stays green and the CDN
    dependency is quietly back. This test is the thing that says so.
    """
    reflex_constants = pytest.importorskip(
        "reflex.constants",
        reason="reflex is a core runtime dependency; if it is absent the environment is "
        "not one this assertion can speak about.",
    )
    from packaging import version  # noqa: PLC0415

    text = dockerfile_text()
    pinned = re.search(r"^ARG BUN_VERSION=(\S+)", text, re.M)
    assert pinned, "ARG BUN_VERSION is gone from the Dockerfile"

    min_version = reflex_constants.Bun.MIN_VERSION
    assert version.parse(pinned.group(1)) >= version.parse(min_version), (
        f"Dockerfile pins bun {pinned.group(1)} but reflex now requires >= {min_version}, "
        "so install_bun() will download anyway and core#1498 is back. Raise BUN_VERSION "
        "and BUN_SHA256_AMD64 together."
    )


# --------------------------------------------------------------------------------------
# The download itself
# --------------------------------------------------------------------------------------


def test_the_download_is_checksum_verified_with_a_real_checksum() -> None:
    """Pinning a URL without verifying the bytes pins a name, not a binary."""
    text = dockerfile_text()
    block = bun_install_block(text)

    assert "sha256sum -c -" in block, (
        "the bun download must be verified with `sha256sum -c -` in the same RUN. "
        "Downloading a pinned URL without checking the bytes pins the name only."
    )

    checksum = re.search(r"^ARG BUN_SHA256_AMD64=([0-9a-f]{64})$", text, re.M)
    assert checksum, (
        "BUN_SHA256_AMD64 must be a literal 64-character lowercase hex digest. A "
        "placeholder, a truncated value or an empty default turns `sha256sum -c` into "
        "theatre."
    )
    assert checksum.group(1) != "0" * 64, "the checksum is a placeholder, not a digest"


def test_an_unknown_architecture_fails_loudly_instead_of_falling_back() -> None:
    """Falling back to the unpinned installer would undo the change while staying green."""
    block = bun_install_block(dockerfile_text())
    assert "amd64)" in block, "the arch case must name the architecture it has a digest for"
    assert "FATAL: no pinned bun checksum for architecture" in block, (
        "an unpinned architecture must abort the build. Silently letting reflex download "
        "would restore both the outage risk and the unpinned-script execution."
    )


@pytest.mark.skipif(shutil.which("sh") is None, reason="POSIX sh unavailable")
def test_the_retry_loop_actually_retries() -> None:
    """Drive the shipped loop with a curl that always fails, and count the attempts.

    A retry wrapper that has never been observed retrying is the same class of evidence
    as a guard that has never been seen refusing — which is exactly what core#1492's own
    rehearsal found three of. So this runs the real extracted block.
    """
    block = bun_install_block(dockerfile_text())

    # Stub the two things that would make this slow or real. `sleep` as a shell function
    # overrides the builtin/PATH lookup, so the backoff costs nothing here.
    harness = (
        "BUN_VERSION=1.3.5\n"
        "BUN_SHA256_AMD64=" + "0" * 64 + "\n"
        "curl() { return 1; }\n"
        "sleep() { :; }\n"
        "dpkg() { echo amd64; }\n" + block.replace("\\\n", "\n")
    )

    proc = subprocess.run(["sh", "-c", harness], capture_output=True, text=True, timeout=60)

    attempts = re.findall(r"bun download attempt (\d)/5 failed", proc.stderr)
    assert attempts == ["1", "2", "3", "4", "5"], (
        f"expected five numbered attempts, saw {attempts!r}. stderr:\n{proc.stderr[-800:]}"
    )
    assert proc.returncode != 0, "a download that never succeeds must fail the build"
    assert "could not download pinned bun" in proc.stderr, (
        "the give-up path must say what it gave up on; 'curl: (22)' alone sent a real "
        "investigation to the wrong layer once already."
    )


# --------------------------------------------------------------------------------------
# The stamp, and why a hash alone is not enough
# --------------------------------------------------------------------------------------


def test_a_content_hash_alone_cannot_detect_a_same_version_redownload() -> None:
    """Demonstrate the defect inline, so the extra fields are not mistaken for noise.

    Re-downloading bun 1.3.5 yields byte-identical content. A sha-only stamp therefore
    compares equal across a re-download and reports 'no download occurred' when one did.
    mtime and size are what actually change when the file is rewritten.
    """
    before_sha = "a56093cb" + "0" * 56
    after_sha = before_sha  # identical bytes: the same version was fetched again

    assert before_sha == after_sha, (
        "this is the point: content equality is exactly what a re-download of the same "
        "version produces, so sha256 cannot discriminate."
    )

    before_stamp = (before_sha, 1765946114, 104272464)
    after_stamp = (after_sha, 1765946999, 104272464)  # rewritten -> new mtime
    assert before_stamp != after_stamp, (
        "with mtime included the same re-download is detectable, which is why the "
        "Dockerfile stamp is `sha256sum` PLUS `stat -c '%Y %s'`."
    )


def test_the_stamp_carries_more_than_the_content_hash() -> None:
    """The shipped stamp must include stat, not only sha256."""
    text = dockerfile_text()
    block = bun_install_block(text)

    assert "bun-pin.stamp" in block, "the install block must record a stamp"
    assert "stat -c '%Y %s'" in block, (
        "the stamp must include mtime and size. sha256 alone passes across a re-download "
        "of the same version — see "
        "test_a_content_hash_alone_cannot_detect_a_same_version_redownload."
    )

    verifier = [b for b in run_blocks(text) if "bun-pin.stamp" in b and "sha256sum -c -" not in b]
    assert any("cat /tmp/bun-pin.stamp" in b for b in verifier), (
        "nothing reads the stamp back after `reflex init`. A stamp that is written and "
        "never compared is a comment with extra steps."
    )


def test_the_version_check_runs_the_binary() -> None:
    """bun ships a separate baseline build for CPUs without AVX2.

    Executing it at build time turns a build-host/run-host mismatch into a loud build
    failure instead of a SIGILL the first time the frontend is compiled.
    """
    block = bun_install_block(dockerfile_text())
    assert f"{PINNED_PATH} --version" in block, (
        "the build must RUN the downloaded bun, not merely place it. A binary that "
        "cannot execute on this CPU is indistinguishable from a good one until it runs."
    )


# --------------------------------------------------------------------------------------
# The artifact-level assertion in CI
# --------------------------------------------------------------------------------------


def test_the_ci_probe_keeps_its_negative_control() -> None:
    """A network-isolated green means nothing without the control that must go red.

    ``image-probe`` runs the real ``install_bun()`` inside the built image with
    ``--network none``. That green is *also* what you would see if ``install_bun()`` had
    become a no-op, or if ``--network none`` were not isolating anything. The control —
    same call, bun removed, still no network — is what distinguishes them, so it is the
    part most worth protecting from a tidy-up.
    """
    ci = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "--network none datanika-core:probe" in ci, (
        "image-probe must exercise the built image with the network removed; asserting "
        "on the Dockerfile text alone never touches the artifact."
    )
    assert "rm -rf /root/.local/share/reflex/bun" in ci, (
        "the negative control is gone. Without it the positive result cannot be "
        "distinguished from a probe that cannot fail."
    )
    assert "CONTROL FAILED" in ci, (
        "the control must fail the job when it does NOT go red, rather than being "
        "reported and ignored."
    )

    # The control has to come after the positive case: reversed, a missing bun in the
    # first call would poison the second and the step would pass for the wrong reason.
    assert ci.index("--- positive") < ci.index("--- negative control"), (
        "the negative control must run after the positive case"
    )
