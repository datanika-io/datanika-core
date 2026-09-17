"""Datanika.

⚠️ Runs before any other ``datanika`` module in every process, which is why the call below lives
here. See ``datanika/pymssql_freetds.py``.
"""

from datanika.pymssql_freetds import use_shipped_configuration

use_shipped_configuration()
