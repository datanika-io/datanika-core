#!/usr/bin/env python3
"""Decide whether a CI job may skip steps it has already passed (core#1713).

The merge queue rewrites the commit id. The proof is the tree, plus every input
the job reads that is not in that tree: ``origin/master`` for the two test jobs,
and the datanika-cloud commit for the three jobs that check that repo out.

``pull_request`` always misses. The queue can only enter after the real checks
exist, so a pull request must run them. ``merge_group`` and ``push`` hit only
when a *completed success* of the same job carries the same proof key. An empty
lookup, a red or unfinished donor, a different key, and any API failure miss.
Missing runs the real steps. The script exits 0 either way: a failed lookup that
failed the step would skip those steps via ``success()``.

Tree-only jobs synthesise the key from the donor commit's tree. Jobs with an
extra input trust only a ``ci-reuse-key`` notice recorded by a run that actually
passed, and only when that notice names the same tree. A success from before
this script existed has no notice, so it does not count.
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any, Protocol

API = "https://api.github.com"
RUN_PAGE = 30
NOTICE_TITLE = "ci-reuse-key"
FAST_EVENTS = frozenset({"merge_group", "push"})

_SHA = re.compile(r"^[0-9a-f]{40}$")
_REPO = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_KEY = re.compile(r"^v1 tree=([0-9a-f]{40}) extra=(-|[0-9a-f]{40})$")
_CHECK_RUN = re.compile(r"/check-runs/(\d+)$")


class LookupError(Exception):
    """The lookup could not be read. Callers treat this as a miss."""


class Transport(Protocol):
    def __call__(self, url: str, body: dict[str, Any] | None = None) -> Any: ...


def proof_key(tree: str, extra: str) -> str:
    """``v1 tree=<sha> extra=<sha or ->``. Both halves are required to be shas."""
    if not _SHA.fullmatch(tree):
        raise ValueError(f"tree is not a 40-hex sha: {tree!r}")
    if extra != "-" and not _SHA.fullmatch(extra):
        raise ValueError(f"extra is not a 40-hex sha or '-': {extra!r}")
    return f"v1 tree={tree} extra={extra}"


def decide(
    event: str,
    job: str,
    key: str,
    donors: list[dict[str, str]] | None,
) -> bool:
    """True only for a fast-path event with one completed success of this job and key.

    A red or unfinished donor does not veto a separate completed success. It also
    does not count as one. ``None`` and ``[]`` are both misses: a failed lookup
    and an empty lookup are not successes.
    """
    if event not in FAST_EVENTS or not job or _KEY.fullmatch(key) is None:
        return False
    if not donors:
        return False
    for donor in donors:
        if not isinstance(donor, dict):
            continue
        if donor.get("job") != job or donor.get("key") != key:
            continue
        if donor.get("status") == "completed" and donor.get("conclusion") == "success":
            return True
    return False


def lookup_hit(
    event: str,
    job: str,
    key: str,
    fetch_donors: Callable[[], list[dict[str, str]] | None],
) -> bool:
    """``decide`` around a fetch. Any exception is a miss, not a skip."""
    if event not in FAST_EVENTS:
        return False
    try:
        donors = fetch_donors()
    except Exception as exc:
        print(f"ci-reuse lookup failed: {exc}", file=sys.stderr)
        return False
    return decide(event, job, key, donors)


def key_from_annotations(payload: Any) -> str | None:
    """The recorded proof, or None when it is missing or disagrees with itself."""
    items = payload
    if isinstance(payload, dict):
        items = payload.get("annotations")
    if not isinstance(items, list):
        return None
    found: list[str] = []
    for item in items:
        if not isinstance(item, dict) or item.get("title") != NOTICE_TITLE:
            continue
        message = str(item.get("message") or "").strip()
        if _KEY.fullmatch(message):
            found.append(message)
    if len(set(found)) == 1:
        return found[0]
    return None


def collect_donors(
    transport: Transport,
    repo: str,
    job: str,
    tree: str,
    current_run_id: str,
    needs_extra: bool,
) -> list[dict[str, str]]:
    """Completed successes of ``job`` whose commit tree is ``tree``.

    Raises :class:`LookupError` when the response is not readable. An empty run
    list is a real empty lookup and returns ``[]``. The current run is never a
    donor. Jobs that need an extra input contribute a donor only when a notice
    records that input and names this same tree.
    """
    if not _SHA.fullmatch(tree):
        raise LookupError(f"tree is not a sha: {tree!r}")
    if not _REPO.fullmatch(repo):
        raise LookupError(f"repo is not owner/name: {repo!r}")
    if not job:
        raise LookupError("job name is empty")

    runs_url = (
        f"{API}/repos/{repo}/actions/workflows/ci.yml/runs?per_page={RUN_PAGE}&status=completed"
    )
    payload = transport(runs_url)
    runs = payload.get("workflow_runs") if isinstance(payload, dict) else None
    if not isinstance(runs, list):
        raise LookupError("workflow runs payload has no workflow_runs list")

    selected: list[tuple[str, str]] = []
    shas: list[str] = []
    for run in runs:
        if not isinstance(run, dict):
            continue
        run_id = str(run.get("id", ""))
        if not run_id or run_id == str(current_run_id):
            continue
        sha = str(run.get("head_sha") or "")
        if not _SHA.fullmatch(sha):
            continue
        selected.append((run_id, sha))
        if sha not in shas:
            shas.append(sha)
    if not selected:
        return []

    trees = _trees(transport, repo, shas)
    donors: list[dict[str, str]] = []
    for run_id, sha in selected:
        run_tree = trees.get(sha)
        if run_tree != tree:
            continue
        jobs_payload = transport(f"{API}/repos/{repo}/actions/runs/{run_id}/jobs?per_page=100")
        jobs = jobs_payload.get("jobs") if isinstance(jobs_payload, dict) else None
        if not isinstance(jobs, list):
            raise LookupError(f"run {run_id} jobs payload has no jobs list")
        for item in jobs:
            if not isinstance(item, dict) or item.get("name") != job:
                continue
            status = str(item.get("status") or "")
            conclusion = str(item.get("conclusion") or "")
            if status != "completed" or conclusion != "success":
                continue
            if needs_extra:
                recorded = _recorded_key(transport, repo, item)
                parsed = _KEY.fullmatch(recorded or "")
                if parsed is None or parsed.group(1) != run_tree:
                    continue
                donor_key = parsed.group(0)
            else:
                donor_key = proof_key(run_tree, "-")
            donors.append(
                {"job": job, "key": donor_key, "status": status, "conclusion": conclusion}
            )
    return donors


def _trees(transport: Transport, repo: str, shas: list[str]) -> dict[str, str]:
    owner, name = repo.split("/", 1)
    fields = " ".join(
        f's{index}: object(expression: "{sha}^{{tree}}") {{ ... on Tree {{ oid }} }}'
        for index, sha in enumerate(shas)
    )
    query = (
        "query { repository(owner: "
        + json.dumps(owner)
        + ", name: "
        + json.dumps(name)
        + ") { "
        + fields
        + " } }"
    )
    payload = transport(f"{API}/graphql", {"query": query})
    if not isinstance(payload, dict) or payload.get("errors"):
        raise LookupError("graphql tree lookup failed")
    repository = payload.get("data", {})
    repository = repository.get("repository") if isinstance(repository, dict) else None
    if not isinstance(repository, dict):
        raise LookupError("graphql tree lookup returned no repository")
    found: dict[str, str] = {}
    for index, sha in enumerate(shas):
        node = repository.get(f"s{index}")
        oid = node.get("oid") if isinstance(node, dict) else None
        if isinstance(oid, str) and _SHA.fullmatch(oid):
            found[sha] = oid
    return found


def _recorded_key(transport: Transport, repo: str, job: dict[str, Any]) -> str | None:
    match = _CHECK_RUN.search(str(job.get("check_run_url") or ""))
    if match is None:
        return None
    url = f"{API}/repos/{repo}/check-runs/{match.group(1)}/annotations?per_page=100"
    return key_from_annotations(transport(url))


def _emit(hit: bool, key: str) -> None:
    # stdout is the fallback for a local run. On a runner the step reads GITHUB_OUTPUT,
    # and a log line on stdout would become a second output.
    text = f"hit={'true' if hit else 'false'}\nkey={key}\n"
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        sys.stdout.write(text)
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(text)


def _urlopen_transport(token: str) -> Transport:
    def transport(url: str, body: dict[str, Any] | None = None) -> Any:
        # noqa S310: the scheme and host are the API constant above. ``url`` is built
        # from that constant plus a repo, a run id, or ``graphql``, so it cannot be
        # redirected at a file: URL. Same suppression as cloud_pairing_gate.py.
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(url, data=data)  # noqa: S310
        request.add_header("Accept", "application/vnd.github+json")
        request.add_header("X-GitHub-Api-Version", "2022-11-28")
        request.add_header("Authorization", f"Bearer {token}")
        if body is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
                loaded = json.load(response)
        except urllib.error.HTTPError as exc:
            raise LookupError(f"HTTP {exc.code} from the actions API") from exc
        return loaded

    return transport


def main() -> int:
    """Write ``hit`` and ``key`` for the workflow. Always exits 0."""
    key = ""
    try:
        event = os.environ.get("REUSE_EVENT", "")
        job = os.environ.get("REUSE_JOB", "")
        tree = os.environ.get("REUSE_TREE", "")
        extra = os.environ.get("REUSE_EXTRA", "-") or "-"
        needs_extra = os.environ.get("REUSE_NEEDS_EXTRA", "0") == "1"
        repo = os.environ.get("REUSE_REPO", "")
        run_id = os.environ.get("REUSE_RUN_ID", "")
        token = os.environ.get("GITHUB_TOKEN", "")
        if needs_extra and not _SHA.fullmatch(extra):
            print("ci-reuse: extra input did not resolve; running the real steps", file=sys.stderr)
            _emit(False, "")
            return 0
        key = proof_key(tree, extra if needs_extra else "-")
        if event not in FAST_EVENTS or not token:
            _emit(False, key)
            return 0
        transport = _urlopen_transport(token)
        hit = lookup_hit(
            event,
            job,
            key,
            lambda: collect_donors(transport, repo, job, tree, run_id, needs_extra),
        )
        _emit(hit, key)
    except Exception as exc:
        print(f"ci-reuse: {exc}", file=sys.stderr)
        _emit(False, key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
