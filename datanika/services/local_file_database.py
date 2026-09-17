"""Opening a local-file database (SQLite, DuckDB) so that the open cannot create it.

Test Connection (core#979) and an upload run (core#1401) both open these files, and they do it
in different containers: Test Connection in the web app, the run in the worker. Both opens must
be unable to bring the database into existence, and both must open the file the user named. The
spelling lives here so that the two halves cannot drift apart again. They already had: core#979
fixed Test Connection, and the run kept creating the empty database Test Connection had stopped
creating.
"""

from urllib.parse import quote

#: Values of ``path`` that name no file at all. An in-memory database is created fresh on every
#: connect by definition, so "does it already exist?" is not a question about it, and read-only
#: is not a mode it has. Measured: duckdb refuses ``:memory:`` with ``read_only=True`` outright.
IN_MEMORY_PATHS = frozenset({":memory:", ""})

#: URL query that makes pysqlite treat the database as a ``file:`` URI and open it read-only.
#: Without ``uri=true`` the driver does not parse the URI at all.
SQLITE_READ_ONLY_QUERY = {"mode": "ro", "uri": "true"}

#: DuckDB's configuration option for the same. duckdb_engine passes URL query parameters through
#: as DuckDB configuration. Measured: with it, a path that does not exist fails with ``database
#: does not exist`` and nothing is created, and an existing file is read and left unmodified.
DUCKDB_READ_ONLY_QUERY = {"access_mode": "read_only"}


def sqlite_uri_filename(path: str) -> str:
    """``path`` as a SQLite URI filename that names exactly that file.

    SQLite reads ``#`` as the start of a fragment, ``?`` as the start of the query and ``%`` as an
    escape. Measured with the path left unencoded on a read-only open: ``hash#1.sqlite`` opened, and
    **created**, a database named ``hash``. That open was read-write, because ``?mode=ro`` fell into
    the fragment. ``pct%20.sqlite`` did not open at all. Percent-encoding everything except path
    separators and a drive letter's colon opens the named file in both cases, and in names with
    spaces or non-ASCII characters too.
    """
    return "file:" + quote(path, safe="/:\\")
