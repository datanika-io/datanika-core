"""The pymssql driver runs with a FreeTDS configuration this repository ships (core#1441).

``pymssql`` carries its own FreeTDS inside the wheel, and every SQL Server and Synapse connection
that goes through it (Test Connection, the upload source, catalog reads) takes its client settings
from a FreeTDS configuration file found through ``FREETDSCONF``. Importing the ``datanika``
package points that variable at ``datanika/pymssql_freetds.conf``.

Each test asks a FRESH interpreter, because the thing under test is what happens at import, and
the interpreter running pytest imported ``datanika`` long before any test began.

⚠️ These tests pin the mechanism: the variable, the file and the setting in it. That is necessary
and not sufficient. A setting in a file is not a reading of a session, so the session property
is asserted from the server's side, in
``tests/test_services/test_pymssql_sessions_are_encrypted.py``.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SHIPPED = REPO / "datanika" / "pymssql_freetds.conf"


def _freetds_conf_after_import(freetdsconf: str | None) -> str | None:
    """``FREETDSCONF`` as a fresh interpreter sees it after ``import datanika``.

    ``freetdsconf=None`` removes the variable from the child's environment entirely.
    """
    env = {k: v for k, v in os.environ.items() if k != "FREETDSCONF"}
    if freetdsconf is not None:
        env["FREETDSCONF"] = freetdsconf
    code = "import os, datanika; print(repr(os.environ.get('FREETDSCONF')))"
    done = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 0, f"the interpreter did not import datanika:\n{done.stderr[-2000:]}"
    return ast.literal_eval(done.stdout.strip().splitlines()[-1])


def freetds_global_settings(text: str) -> dict[str, str]:
    """The ``[global]`` settings of a FreeTDS configuration, read the way FreeTDS reads them.

    FreeTDS (``tds_read_conf_section``) skips lines whose first non-blank character is ``;`` or
    ``#``, matches section names without regard to case, lower-cases option names and collapses
    the blanks inside them, and stops a value at ``;`` or ``#``.
    """
    settings: dict[str, str] = {}
    section = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line[0] in ";#":
            continue
        if line.startswith("["):
            section = line[1 : line.index("]")].strip().lower()
            continue
        if section != "global" or "=" not in line:
            continue
        name, _, value = line.partition("=")
        for stop in ";#":
            value = value.split(stop, 1)[0]
        settings[" ".join(name.lower().split())] = value.strip()
    return settings


class TestTheProcessReadsTheShippedConfiguration:
    def test_importing_datanika_points_freetds_at_the_shipped_file(self):
        value = _freetds_conf_after_import(None)
        assert value is not None, (
            "importing datanika left FREETDSCONF unset, so the pymssql driver runs with whatever "
            "client settings its wheel was built with"
        )
        assert Path(value).resolve() == SHIPPED.resolve()

    def test_the_shipped_file_requires_encryption(self):
        assert SHIPPED.is_file(), f"{SHIPPED} is missing"
        settings = freetds_global_settings(SHIPPED.read_text(encoding="utf-8"))
        assert settings.get("encryption") == "require", settings

    def test_an_operator_configuration_is_kept(self, tmp_path):
        """An operator who sets FreeTDS's own variable has chosen a file, and it is theirs."""
        operator = str(tmp_path / "operator-freetds.conf")
        assert _freetds_conf_after_import(operator) == operator

    def test_an_empty_value_counts_as_unset(self):
        """Compose writes ``FREETDSCONF=`` for an interpolated variable that is not set.

        FreeTDS cannot open an empty path, so honouring it would drop the shipped settings on a
        typo rather than on a choice.
        """
        value = _freetds_conf_after_import("")
        assert value, "an empty FREETDSCONF was kept"
        assert Path(value).resolve() == SHIPPED.resolve()


class TestTheParserReadsWhatFreetdsReads:
    """The parser above is an instrument, so it is shown to tell settings apart."""

    def test_a_commented_setting_does_not_count(self):
        assert "encryption" not in freetds_global_settings("[global]\n; encryption = require\n")
        assert "encryption" not in freetds_global_settings("[global]\n# encryption = require\n")

    def test_a_setting_in_another_section_does_not_count(self):
        text = "[global]\n    tds version = auto\n[myserver]\n    encryption = require\n"
        assert "encryption" not in freetds_global_settings(text)

    def test_section_and_option_names_ignore_case_and_blanks(self):
        text = "[GLOBAL]\n  Encryption   =   require\n  CA   File = /x.pem\n"
        assert freetds_global_settings(text) == {"encryption": "require", "ca file": "/x.pem"}
