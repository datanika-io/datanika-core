#!/usr/bin/env python3
"""core#1123 — refuse a production deploy whose cloud half is not on cloud ``master``.

## The eight-minute window

Cloud ships *inside* core's image: ``deploy-pointer.yml`` checks ``datanika-cloud`` out at a
pinned ``ref: master`` and tars both trees to the box, which rebuilds. So the cloud tree that
reaches production is whatever ``master`` points at **at the moment this workflow runs**.

On 2026-09-06 a core promotion came within one merge of shipping ``is_user_facing`` as a
positive ``isinstance(exc, UserFacingError)`` test against a cloud ``master`` whose
``QuotaExceededError`` still inherited ``ValueError``. Every quota refusal in the product
would have rendered *"An error occurred."* — with every container healthy and every check
green. Cloud had been promoted at 14:34:48Z; the cloud half of the pair merged to cloud
``dev`` at 14:42Z, **eight minutes later**.

🔑 ``"cloud was promoted"`` and ``"cloud is in the image"`` are different facts that look
identical on a branch listing. The ordering rule was already written down — in the runbook,
in the handoff, and in the promotion body — and it did not help, because the question a
promoter needs answered is not *"have I read the constraint"* but *"is the pairing true right
now"*.

## What this gate does, and where it runs

One API call: ``repos/datanika-io/datanika-cloud/compare/master...dev``. **Only ``identical``
passes.** Anything else exits non-zero and the deploy stops before the tarball is built,
before the box is touched, and before any image exists — production keeps serving whatever it
was already serving.

It runs in ``deploy-pointer.yml``, immediately after the core checkout and **before** the
cloud checkout it grades. That placement is the point:

* It reads **after the merge**, not at PR-open time. A reading taken when the promotion PR is
  created is exactly the reading that was true and then stopped being true inside eight
  minutes.
* It is the last reading before the cloud tree is fetched, so it describes the very tree that
  is about to be baked into the image rather than a proxy for it.
* It cannot be skipped. ``master`` protection is bypassed by ``--admin`` on every promotion by
  design (``reviews=1`` is unsatisfiable with one identity), so a ``pull_request`` check is
  advisory no matter how it is labelled. CD is the one step in the promotion path that no
  flag walks past.

🚨 **There is deliberately no ``pull_request`` counterpart.** A green check on the promotion PR
would be read at exactly the moment that already failed, and a *reassuring* stale reading is
worse than none — the promoter would have been shown a green eight minutes before the merge
that made it false.

## Deliberately blunt

Cloud being ahead does not always mean *this* core batch needs it. This gate will therefore
sometimes refuse a promotion that would have been fine.

**That is the correct trade.** A false positive costs one cloud promotion — often of doc
commits — and a re-run. A false negative costs a silent, product-wide failure that no
container health check, no smoke test and no alert rule can see.

⚠️ **Do not make it clever by diffing which files moved.** The pairing is semantic: a marker
class in one repo, a predicate in the other, in files whose paths have nothing in common. No
path heuristic sees it.

⚠️ **A refusal means "promote cloud first", never "override it".** There is no override input,
no ``force`` env var and no allow-list. The repair is the promotion the ordering rule asked
for in the first place.

## Fail-closed

Every unhappy path refuses: no token, HTTP error, 404, malformed JSON, a missing ``status``
key, or a value GitHub has never documented. An instrument that cannot read the pairing has
not established it, and this gate exists precisely because "no signal" reads like "fine".

Refs core#1094, cloud#151, core#1071.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

API_ROOT = "https://api.github.com"

#: The production pair. Overridable only so the gate can be armed against a real
#: non-identical comparison; ``deploy-pointer.yml`` sets none of these, and
#: ``tests/test_deploy/test_cloud_pairing_gate.py`` asserts that it does not.
DEFAULT_REPO = "datanika-io/datanika-cloud"
DEFAULT_BASE = "master"
DEFAULT_HEAD = "dev"

#: Every value GitHub's compare endpoint documents for ``status``.
KNOWN_STATUSES = ("identical", "ahead", "behind", "diverged")

#: The only one that lets a deploy through.
PASSING_STATUS = "identical"

TOKEN_VARS = ("CLOUD_REPO_TOKEN", "GH_TOKEN", "GITHUB_TOKEN")


def verdict(status: object) -> tuple[bool, str]:
    """``(allowed, reason)`` for one compare status. Pure, so it can be exhaustively tested.

    Only :data:`PASSING_STATUS` is allowed. ``behind`` and ``diverged`` refuse too: cloud
    ``master`` carrying commits ``dev`` does not have means somebody pushed to production
    without resyncing, which is its own thing to stop on.
    """
    if status == PASSING_STATUS:
        return True, (
            f"compare status is {PASSING_STATUS!r} — every commit tested on the head ref is "
            f"already on the ref this deploy builds from"
        )
    if isinstance(status, str) and status in KNOWN_STATUSES:
        return False, (
            f"compare status is {status!r}, not {PASSING_STATUS!r} — the tree this deploy "
            f"would bake into the image is not the tree that has been tested on the head ref"
        )
    return False, (
        f"could not read a documented compare status (got {status!r}) — an instrument that "
        f"cannot read the pairing has not established it"
    )


def read_status(repo: str, base: str, head: str, token: str | None) -> tuple[object, str | None]:
    """``(status, base head sha)`` for a compare — ``(None, None)`` if anything went wrong.

    Never raises: every failure is folded into a ``None`` status so :func:`verdict` refuses
    it. The reason is printed, because a gate whose diagnosis is swallowed is a gate people
    override.
    """
    url = f"{API_ROOT}/repos/{repo}/compare/{base}...{head}"
    # noqa S310 (both lines): the scheme is fixed by the API_ROOT literal above. Only the
    # path segments are interpolated, and they name refs, so no input can turn this into
    # `file:` or a custom scheme. Same reasoning and same suppression as scripts/slo_report.py.
    request = urllib.request.Request(url)  # noqa: S310
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("X-GitHub-Api-Version", "2022-11-28")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            payload = json.load(response)
    except urllib.error.HTTPError as exc:  # 401/403/404 on the private repo, rate limits
        print(f"  API HTTPError {exc.code} for {repo} {base}...{head}", file=sys.stderr)
        return None, None
    except Exception as exc:  # network, TLS, malformed JSON — all equally disqualifying
        print(f"  API call failed for {repo} {base}...{head}: {exc!r}", file=sys.stderr)
        return None, None
    if not isinstance(payload, dict):
        print(f"  compare payload is {type(payload).__name__}, not an object", file=sys.stderr)
        return None, None
    base_commit = payload.get("base_commit")
    base_sha = base_commit.get("sha") if isinstance(base_commit, dict) else None
    return payload.get("status"), base_sha


def main() -> int:
    repo = os.environ.get("PAIRING_REPO") or DEFAULT_REPO
    base = os.environ.get("PAIRING_BASE") or DEFAULT_BASE
    head = os.environ.get("PAIRING_HEAD") or DEFAULT_HEAD
    token = next((os.environ[name] for name in TOKEN_VARS if os.environ.get(name)), None)

    print(f"core#1123 cloud pairing gate: {repo} {base}...{head}")
    if token is None:
        print(f"  no token in any of {TOKEN_VARS} — refusing rather than guessing")

    status, base_sha = read_status(repo, base, head, token)
    allowed, reason = verdict(status)

    if base_sha:
        print(f"  cloud {base} is {base_sha[:8]} — this is what the image would be built from")

    if allowed:
        print(f"  PASS: {reason}")
    else:
        print(f"  REFUSED: {reason}", file=sys.stderr)
        print(
            f"  Fix by promoting {repo} {head} -> {base}, then re-running this deploy.\n"
            "  Do NOT override: cloud reaches production only inside this image, so a core\n"
            "  deploy against a stale cloud master ships a pairing nothing has ever run.\n"
            "  See docs/runbooks/RUNBOOK_DEV_TO_MASTER.md, 'Order: cloud first'.",
            file=sys.stderr,
        )

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        headline = "PASS" if allowed else "REFUSED"
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(f"### Cloud pairing gate (core#1123) — {headline}\n\n")
            handle.write(f"- `{repo}` `{base}...{head}` status: `{status}`\n")
            handle.write(f"- reason: {reason}\n")
            if base_sha:
                handle.write(f"- cloud `{base}` head: `{base_sha}`\n")
            if not allowed:
                handle.write(
                    "\nPromote `datanika-cloud` to `master`, then re-run this deploy. "
                    "There is no override.\n"
                )

    return 0 if allowed else 1


if __name__ == "__main__":
    sys.exit(main())
