"""Choose the apt package that provides xmlsec's OpenSSL backend.

Ubuntu 24.04 and Debian ship ``libxmlsec1-openssl``. Ubuntu 26.04 removed that
binary package; the installable name there is ``libxmlsec1-openssl1``.
``apt-get install`` of the old name exits 100 before any test runs (PR 1620,
lint job, Actions run 40660378168, 2026-10-28).

``apt-cache policy`` prints ``Candidate: (none)`` when a name has no installable
version. That line is the signal this script reads.
"""

from __future__ import annotations

import subprocess
import sys

_PACKAGES = ("libxmlsec1-openssl", "libxmlsec1-openssl1")


def candidate(policy_text: str) -> str | None:
    """Return the installable version from ``apt-cache policy`` output."""
    for raw in policy_text.splitlines():
        line = raw.strip()
        if not line.startswith("Candidate:"):
            continue
        value = line.split(":", 1)[1].strip()
        if value and value != "(none)":
            return value
        return None
    return None


def select_package(policy_for: dict[str, str]) -> str:
    """Prefer the historical package name when it still has a candidate.

    24.04 and the Debian image still ship ``libxmlsec1-openssl``. Selecting the
    renamed package there would change a dependency those builds already resolve.
    """
    for pkg in _PACKAGES:
        if pkg not in policy_for:
            continue
        if candidate(policy_for[pkg]) is not None:
            return pkg
    raise SystemExit("neither libxmlsec1-openssl nor libxmlsec1-openssl1 has an apt candidate")


def policy_via_apt(package: str) -> str:
    completed = subprocess.run(  # noqa: S603
        ["apt-cache", "policy", package],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 and not completed.stdout:
        detail = completed.stderr.strip()
        raise SystemExit(f"apt-cache policy {package} failed ({completed.returncode}): {detail}")
    return completed.stdout


def main() -> int:
    policies = {pkg: policy_via_apt(pkg) for pkg in _PACKAGES}
    print(select_package(policies))
    return 0


if __name__ == "__main__":
    sys.exit(main())
