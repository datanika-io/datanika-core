"""The image carries a working FreeTDS ODBC driver that encrypts by default (core#1379).

THE DEFECT
----------
``dlt[mssql,synapse]`` load through pyodbc, and the image installed no ODBC runtime at all, so every
SQL Server or Synapse destination failed at dlt's ``sync`` step with
``ImportError: libodbc.so.2`` — measured on the image production builds from.

🚨 **Installing only the runtime is the fix that looks like progress.** Measured on #1379:
``apt-get install unixodbc`` clears the ImportError and leaves ``pyodbc.drivers() == []``, so the
failure moves to *"No supported ODBC driver found"* with the build green. That is why the build
asserts the driver is REGISTERED, in the artifact, and not merely that a package name is listed.

WHY A GLOBAL ``encryption = require``
-------------------------------------
Two consumers reach FreeTDS through ODBC: dlt's loader and dbt-sqlserver. dlt's connection string is
ours and carries ``ENCRYPTION=require``. dbt-sqlserver writes Microsoft's ``Encrypt=Yes``, which
FreeTDS ignores without an error — a request for encryption that connects in cleartext and reports
success. The global setting is the one place that covers both.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = REPO_ROOT / "Dockerfile"


def _runs(text: str) -> list[str]:
    """Every RUN instruction, continuation lines joined."""
    joined = re.sub(r"\\\n", " ", text)
    return [ln.strip()[4:] for ln in joined.splitlines() if ln.strip().startswith("RUN ")]


def _stage_of(text: str, needle: str) -> str | None:
    stage = None
    for line in text.splitlines():
        m = re.match(r"^FROM\s+\S+(?:\s+AS\s+(\S+))?", line.strip(), flags=re.I)
        if m:
            stage = m.group(1)
        if needle in line:
            return stage
    return None


def freetds_problems(text: str) -> list[str]:
    from datanika.services.dlt_mssql_freetds import FREETDS_DRIVER

    problems: list[str] = []
    runs = _runs(text)

    installs = [r for r in runs if "apt-get install" in r]
    if not installs:
        return ["no `apt-get install` step found -- the parser is reading the wrong file"]
    packages = set(re.findall(r"[a-z0-9][a-z0-9.+-]+", " ".join(installs)))
    for pkg in ("unixodbc", "tdsodbc"):
        if pkg not in packages:
            problems.append(
                f"`{pkg}` is not installed -- dlt's mssql/synapse destinations cannot load"
            )

    assertion = [r for r in runs if "pyodbc.drivers()" in r]
    if not assertion:
        problems.append(
            "no build step asserts the ODBC driver is registered -- `unixodbc` alone builds green "
            "and leaves pyodbc.drivers() empty (measured on core#1379)"
        )
    elif f"'{FREETDS_DRIVER}'" not in assertion[0] and f'"{FREETDS_DRIVER}"' not in assertion[0]:
        problems.append(
            f"the registration assertion does not name {FREETDS_DRIVER!r}, the driver the loader "
            "and the dbt profile ask for"
        )

    conf_runs = [r for r in runs if "/etc/freetds/freetds.conf" in r]
    if not conf_runs:
        problems.append(
            "nothing writes /etc/freetds/freetds.conf -- dbt-sqlserver would connect in cleartext"
        )
    else:
        conf = " ".join(conf_runs)
        if not re.search(r"\[global\]", conf):
            problems.append("the FreeTDS configuration has no [global] section")
        if not re.search(r"encryption\s*=\s*require", conf):
            problems.append("the FreeTDS [global] section does not set `encryption = require`")
        if "grep" not in conf:
            problems.append("the written FreeTDS configuration is never read back in the build")

    stage = _stage_of(text, "pyodbc.drivers()")
    if stage not in (None, "final"):
        problems.append(f"the driver assertion runs in stage {stage!r}; it must run in `final`")
    return problems


def test_the_dockerfile_carries_a_registered_encrypting_freetds() -> None:
    assert not freetds_problems(DOCKERFILE.read_text(encoding="utf-8"))


def test_the_dbt_profile_and_the_loader_name_the_same_driver() -> None:
    from datanika.services.dbt_project import DbtProjectService
    from datanika.services.dlt_mssql_freetds import FREETDS_DRIVER

    output = DbtProjectService._build_profile_output("mssql", {"host": "h"})
    assert output["driver"] == FREETDS_DRIVER


_MUTATIONS = {
    "the runtime alone, no driver": (
        lambda t: re.sub(r"(?m)^(\s*)unixodbc tdsodbc", r"\1unixodbc", t),
        "`tdsodbc` is not installed",
    ),
    "no ODBC runtime at all (the shipped defect)": (
        lambda t: re.sub(r"(?m)^(\s*)unixodbc tdsodbc", r"\1ca-certificates", t),
        "`unixodbc` is not installed",
    ),
    "no registration assertion": (
        lambda t: t.replace("pyodbc.drivers()", "pyodbc.version"),
        "no build step asserts the ODBC driver is registered",
    ),
    "encryption not required": (
        lambda t: re.sub(r"encryption\s*=\s*require", "encryption = request", t),
        "does not set `encryption = require`",
    ),
    "configuration written but never read back": (
        lambda t: re.sub(r"grep[^;&|]*freetds\.conf", "true", t),
        "never read back",
    ),
}


@pytest.mark.parametrize("name", list(_MUTATIONS))
def test_each_check_rejects_the_shape_it_exists_for(name: str) -> None:
    real = DOCKERFILE.read_text(encoding="utf-8")
    mutate, expected = _MUTATIONS[name]
    mutated = mutate(real)
    assert mutated != real, f"mutation {name!r} changed nothing -- it would test nothing"
    assert not freetds_problems(real), (
        "the real Dockerfile must pass before a mutant means anything"
    )
    problems = freetds_problems(mutated)
    assert any(expected in p for p in problems), f"mutation {name!r} not detected: {problems}"
