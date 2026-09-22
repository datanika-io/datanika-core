"""Run-status lists as they appear in published prose, derived from the model (core#657).

``SPEC_RUN_CANCELLATION`` §4 retired seven hand-maintained lists of run statuses in code. Two more
lived in **prose** that ships with the product — the ``?wait=true`` outcomes in the OpenAPI
document and in the agent guide read *"still pending/running"* and *"(`failed`, `cancelled`)"* —
and the first went stale the day ``cancelling`` was added: a ``cancelling`` run at a wait timeout
is a ``408``, and neither document said so.

Both documents now read these constants, so a status added to the enum reaches the prose the
moment it is placed in a set.
"""

from __future__ import annotations

from collections.abc import Iterable

from datanika.errors import InternalInvariantError
from datanika.models.run import NON_TERMINAL_RUN_STATUSES, TERMINAL_RUN_STATUSES, RunStatus


def statuses_in_prose(statuses: Iterable[RunStatus]) -> str:
    """``{PENDING, RUNNING, CANCELLING}`` -> "`cancelling`, `pending` or `running`" (sorted).

    An empty set is a programming error, not an input to render: the published sentence would
    read "still  when the wait expired". ``InternalInvariantError``, never a bare ``ValueError``,
    whose text core#1094's contract stops from reaching anyone.
    """
    names = [f"`{s.value}`" for s in sorted(statuses)]
    if not names:
        raise InternalInvariantError("an empty run-status set has no prose")
    return names[0] if len(names) == 1 else f"{', '.join(names[:-1])} or {names[-1]}"


#: Still going: a ``?wait=true`` that times out on one of these returns ``408``.
STILL_GOING = statuses_in_prose(NON_TERMINAL_RUN_STATUSES)

#: Finished, and not successfully: a ``?wait=true`` that ends on one of these returns ``422``.
TERMINAL_NOT_SUCCESS = statuses_in_prose(TERMINAL_RUN_STATUSES - {RunStatus.SUCCESS})
