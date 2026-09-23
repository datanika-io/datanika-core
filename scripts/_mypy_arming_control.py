"""TEMPORARY. core#1288 AC1's arming control — **deleted in the next commit of this PR.**

AC1 names `scripts/verify_e2e_attribution.py:309` as the control and says it *"must be shown
failing against today's tree before the gate is trusted."* **That line no longer exists.**
core#1285 merged, the caller moved to :400, and it now unpacks the tuple correctly. So the
criterion passes today — which `QA_RULES` §20 rule 2 says makes it a control, not a criterion,
and an AC that can no longer fail arms nothing.

This file is the replacement, and it is a better one: it reproduces the exact core#1273 shape
against the **real** helper, from a **new** consumer. That is the actual future risk — the next
module to import `_gh_log_or_none` and unpack it as a bare string — rather than the one seam
somebody has already debugged.

The commit that adds this file must make CI's `lint` job go RED on the runner, at:

    scripts/_mypy_arming_control.py: error: "tuple[str | None, str]" has no attribute
                                            "splitlines"  [attr-defined]

The commit that deletes it must make the same job go green. Both are in this PR's check
history on purpose: a gate nobody has watched fail is not evidence (`QA_RULES` §2).

⚠️ Nothing imports or executes this module. It exists to be type-checked and removed.
"""

from __future__ import annotations

from e2e_tier_streak import _gh_log_or_none


def first_log_line(repo: str, job_id: int) -> str:
    """The core#1273 crash, restated: `_gh_log_or_none` returns `(log, reason)`."""
    log = _gh_log_or_none(repo, job_id)
    return log.splitlines()[0]
