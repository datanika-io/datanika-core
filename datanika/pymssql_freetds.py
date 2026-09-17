"""The FreeTDS configuration the pymssql driver reads (core#1441).

``pymssql`` carries its own FreeTDS inside the wheel, so its client settings do not come from
anything the image installs: not Debian's ``/etc/freetds``, and not the ODBC driver the SQL Server
destinations use (``datanika/services/dlt_mssql_freetds.py``). They come from a configuration file
FreeTDS looks up through ``FREETDSCONF`` each time a connection logs in.

:func:`use_shipped_configuration` points that variable at ``pymssql_freetds.conf``, next to this
module. ``datanika/__init__.py`` calls it, so it runs before any connection in every process that
imports the package: the web app, the Celery worker, the scheduler and the test suite.

Two rules, each with a reason:

* **An operator's own file is kept.** ``FREETDSCONF`` is FreeTDS's own variable, and an operator who
  sets it has chosen a file. That file replaces this one entirely, including ``encryption``.
* **An empty value counts as unset.** Compose writes ``FREETDSCONF=`` for an interpolated variable
  that is not set, and FreeTDS cannot open an empty path. Keeping it would drop the shipped
  settings on an omission rather than on a choice.

⚠️ The file's setting is not the evidence that a session is encrypted. That is read from SQL
Server's own ``sys.dm_exec_connections`` in
``tests/test_services/test_pymssql_sessions_are_encrypted.py``.
"""

from __future__ import annotations

import os
from collections.abc import MutableMapping
from pathlib import Path

#: The configuration shipped with the package.
SHIPPED_CONFIGURATION = Path(__file__).resolve().with_name("pymssql_freetds.conf")


def use_shipped_configuration(environ: MutableMapping[str, str] = os.environ) -> str:
    """Point ``FREETDSCONF`` at the shipped file unless an operator chose one. Returns the value."""
    if not environ.get("FREETDSCONF"):
        environ["FREETDSCONF"] = str(SHIPPED_CONFIGURATION)
    return environ["FREETDSCONF"]
