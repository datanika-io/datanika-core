"""A GitHub upload loads closed issues and closed or merged pull requests (core#1403).

The GitHub branch of ``build_source`` runs the shared REST fallback, because the
verified-source import raises. Its default ``issues`` and ``pulls`` resources
request the list endpoints with no query string. GitHub's REST reference gives
``state`` a default of ``open``, so a run loads only the items open at run time.
The fallback writes with ``replace``, so the next run drops anything that closed.

The witness runs that branch through the real ``execute()``, into DuckDB. Only
the host is rewritten, to a local server that implements the documented contract:

* Without ``state``, both list endpoints return the open item and no next page.
* ``state=all`` returns that open item, then a ``Link`` whose URL still carries
  ``state=all``. dlt's header-link paginator drops the resource params on every
  later page and follows that URL alone, which is where the closed issue and the
  closed and merged pull requests are.
* A later page that does not ask for ``state=all`` does not receive them.

The rows are read back from the destination. Checking the request parameters
alone would pass against a stub that ignores them.
"""

from __future__ import annotations

import http.server
import json
import threading
import urllib.parse
import urllib.request

import duckdb
import pytest

from datanika.services.dlt_runner import DltRunnerService
from tests.test_services.test_rerun_lands_each_record_once import form_config

OWNER = "datanika-io"
REPO = "probe"
PREFIX = f"/repos/{OWNER}/{REPO}"

OPEN_ISSUE = {"id": 101, "state": "open", "title": "open issue"}
CLOSED_ISSUE = {"id": 102, "state": "closed", "title": "closed issue"}
OPEN_PR = {"id": 201, "state": "open", "title": "open pr", "merged_at": None}
CLOSED_PR = {"id": 202, "state": "closed", "title": "closed pr", "merged_at": None}
MERGED_PR = {
    "id": 203,
    "state": "closed",
    "title": "merged pr",
    "merged_at": "2026-08-01T10:00:00Z",
}


class _GitHubStub(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        parsed = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        self.server.requests.append((parsed.path, query))
        if parsed.path == f"{PREFIX}/issues":
            self._collection(parsed.path, query, [OPEN_ISSUE], [CLOSED_ISSUE])
        elif parsed.path == f"{PREFIX}/pulls":
            self._collection(parsed.path, query, [OPEN_PR], [CLOSED_PR, MERGED_PR])
        elif parsed.path in (f"{PREFIX}/commits", f"{PREFIX}/stargazers"):
            self._json([])
        else:
            self.send_error(404)

    def _collection(self, path: str, query: dict, open_rows: list, rest: list) -> None:
        # GitHub's default is open. Closed and merged rows exist only on the
        # second page of a request that asked for every state, and only because
        # the next URL still says so. The paginator does not re-send params.
        state = query.get("state", ["open"])[0]
        page = query.get("page", ["1"])[0]
        if state == "all" and page == "2":
            self._json(rest)
            return
        headers = {}
        if state == "all":
            host = self.headers["Host"]
            headers["Link"] = f'<http://{host}{path}?state=all&page=2>; rel="next"'
        self._json(open_rows, headers=headers)

    def _json(self, payload: list, *, headers: dict | None = None) -> None:
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def github_stub():
    server = http.server.HTTPServer(("127.0.0.1", 0), _GitHubStub)
    server.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def allow_loopback(monkeypatch):
    """The three patches `test_source_builders_move_rows.allow_loopback` documents."""
    monkeypatch.setattr("datanika.services.dlt_runner.validate_egress_host", lambda url: None)
    monkeypatch.setattr("datanika.services.egress_guard.validate_egress_host", lambda url: None)
    monkeypatch.setattr(
        "datanika.services.egress_guard.resolve_public_ip", lambda hostname: "127.0.0.1"
    )


@pytest.fixture
def host_rewritten_to_stub(monkeypatch, github_stub, allow_loopback):
    """Send the GitHub branch's fallback to the stub, keeping everything else it built."""
    seen: dict = {}
    original = DltRunnerService._rest_api_fallback.__func__
    stub_url = f"http://127.0.0.1:{github_stub.server_port}/"

    def fallback(cls, base_url, auth, resources, *, paginator, headers=None):
        seen["base_url"] = base_url
        seen["resources"] = resources
        return original(cls, stub_url, auth, resources, paginator=paginator, headers=headers)

    monkeypatch.setattr(DltRunnerService, "_rest_api_fallback", classmethod(fallback))
    return seen


def _rows(tmp_path, table: str) -> list[tuple]:
    con = duckdb.connect(str(tmp_path / "dest.duckdb"))
    try:
        return con.execute(f'SELECT id, state FROM "probe"."{table}" ORDER BY id').fetchall()
    finally:
        con.close()


def _merged_at(tmp_path) -> dict:
    con = duckdb.connect(str(tmp_path / "dest.duckdb"))
    try:
        rows = con.execute('SELECT id, merged_at FROM "probe"."pulls"').fetchall()
    finally:
        con.close()
    return {row[0]: row[1] for row in rows}


def _run(tmp_path) -> None:
    DltRunnerService(pipelines_dir=str(tmp_path / "dlt")).execute(
        pipeline_id=7,
        source_type="github",
        source_config={"owner": OWNER, "repo": REPO, "access_token": "ghp_probe"},
        destination_type="duckdb",
        destination_config={"path": str(tmp_path / "dest.duckdb")},
        dlt_config=form_config("github"),
        dataset_name="probe",
        run_id=1,
    )


class TestClosedIssuesAndPullsLand:
    def test_closed_issues_and_closed_or_merged_pulls_land(
        self, tmp_path, github_stub, host_rewritten_to_stub
    ):
        _run(tmp_path)

        assert host_rewritten_to_stub["base_url"] == "https://api.github.com/"
        assert _rows(tmp_path, "issues") == [(101, "open"), (102, "closed")], (
            "a GitHub upload must load closed issues; GitHub's default is open only"
        )
        assert _rows(tmp_path, "pulls") == [
            (201, "open"),
            (202, "closed"),
            (203, "closed"),
        ], "a GitHub upload must load closed pull requests and merged ones"
        assert _merged_at(tmp_path)[203] not in (None, ""), (
            "the merged pull request must land with merged_at set"
        )
        assert _merged_at(tmp_path)[202] in (None, ""), (
            "a closed pull request that was not merged must land too"
        )


class TestTheStubWithholdsClosedItems:
    def test_anti_vacuity_no_state_means_open_only(self, github_stub):
        """Otherwise a stub that ignores ``state`` would put closed rows in the destination."""
        base = f"http://127.0.0.1:{github_stub.server_port}{PREFIX}"
        for name, open_id in (("issues", 101), ("pulls", 201)):
            with urllib.request.urlopen(f"{base}/{name}") as response:  # noqa: S310
                payload = json.load(response)
                link = response.headers.get("Link")
            assert [row["id"] for row in payload] == [open_id]
            assert link is None

    def test_anti_vacuity_a_later_page_without_state_has_no_closed_items(self, github_stub):
        """The paginator follows the next URL with the resource params removed."""
        base = f"http://127.0.0.1:{github_stub.server_port}{PREFIX}"
        for name, closed_ids in (("issues", {102}), ("pulls", {202, 203})):
            with urllib.request.urlopen(f"{base}/{name}?page=2") as response:  # noqa: S310
                payload = json.load(response)
            assert closed_ids.isdisjoint(row["id"] for row in payload)
