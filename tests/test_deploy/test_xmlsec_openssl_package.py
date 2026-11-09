"""The runner and the image install different xmlsec openssl packages.

Ubuntu 26.04 (the ubuntu-latest runner on 2026-11-09) has no installation
candidate for libxmlsec1-openssl. apt names libxmlsec1-openssl1 as the
replacement. That failed nightly-connector-smoke run 42606322342 at
Install dependencies, before any connector test ran, and it failed the
same step in lint, test, migration-roundtrip and cloud-suite.

The production image is Debian bookworm at a pinned digest. Its apt line
still installs libxmlsec1-openssl, and that step succeeded in the same
promotion run. A workflow that copies the image's package name onto the
runner is red on every job. A Dockerfile that copies the runner's new
name is red on the image build.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"
DOCKERFILE = REPO / "Dockerfile"

BARE_INSTALL = "libxmlsec1-dev libxmlsec1-openssl \\"
DEV_PACKAGE = "libxmlsec1-dev"


def _selects_a_candidate(text: str) -> bool:
    return (
        "Candidate: (none)" in text
        and "xmlsec_openssl_pkg=libxmlsec1-openssl1" in text
        and 'libxmlsec1-dev "$xmlsec_openssl_pkg"' in text
    )


def test_the_old_line_is_what_this_catches() -> None:
    """The detector has to match the line that failed. A helper that
    rejects nothing leaves the loop green."""
    old = "libxml2-dev libxslt1-dev libxmlsec1-dev libxmlsec1-openssl \\\n"
    assert BARE_INSTALL in old
    assert not _selects_a_candidate(old)


def test_every_runner_install_selects_a_candidate() -> None:
    seen = 0
    offenders: list[str] = []
    for path in sorted(WORKFLOWS.glob("*.yml")):
        text = path.read_text(encoding="utf-8")
        if DEV_PACKAGE not in text:
            continue
        seen += 1
        if BARE_INSTALL in text or not _selects_a_candidate(text):
            offenders.append(path.name)
    assert seen >= 5, f"the scan saw {seen} workflows, so it cannot fail"
    assert offenders == []


def test_the_dockerfile_keeps_the_debian_package() -> None:
    text = DOCKERFILE.read_text(encoding="utf-8")
    assert "libxmlsec1-dev libxmlsec1-openssl " in text
    assert "libxmlsec1-openssl1" not in text


def test_the_image_removes_the_lock_uv_cache_clean_leaves() -> None:
    """uv cache clean leaves the directory and its lock. image-probe
    counted that one path and failed the build (run 42618611906)."""
    text = DOCKERFILE.read_text(encoding="utf-8")
    assert "uv cache clean" in text
    assert 'cache_dir="$(uv cache dir)"' in text
    assert 'rm -rf "$cache_dir"' in text
    clean_at = text.index("uv cache clean")
    remove_at = text.index('rm -rf "$cache_dir"')
    assert remove_at > clean_at
