"""A run status must be classified, and every consumer must read the classification.

``SPEC_RUN_CANCELLATION`` §4. Seven places used to enumerate run statuses by hand with nothing
linking them, and **two of them produced a confident wrong answer** rather than an error when a
status was missing:

* the ``?wait=true`` timeout branch tested two string literals to mean *still going*, so a
  non-terminal status added later returned **`422` terminal-not-success** for a run that was
  still working;
* ``cleanup_orphaned_dlt_dirs`` protected only ``RUNNING``/``PENDING`` directories, so any other
  non-terminal status had its working directory **deleted while the worker was writing to it** —
  live since ``datanika-beat`` began running that sweep hourly.

🚨 **The assertion that matters is not "the sets contain what they contain today".** That is an
eighth hand-maintained list, it would have passed on every one of the seven defects, and it can
only ever be satisfied by editing it. The assertion is **totality**: a status that exists in the
enum and in neither set fails the build, so adding one forces a deliberate choice.

``test_the_guard_can_see_an_unclassified_status`` is the control. It synthesises a throwaway
eighth status and asserts the check reports it **by name** — because a totality check that cannot
fail is exactly the defect this file exists to prevent, one level up.
"""

from __future__ import annotations

import enum

import pytest

from datanika.models.run import (
    CANCEL_REQUESTED_RUN_STATUSES,
    CANCELLABLE_RUN_STATUSES,
    NON_TERMINAL_RUN_STATUSES,
    TERMINAL_RUN_STATUSES,
    RunStatus,
)


def _unclassified(statuses, terminal, non_terminal) -> set[str]:
    """Statuses the partition does not place. The predicate under test, as one function."""
    return (
        {s.value for s in statuses} - {s.value for s in terminal} - {s.value for s in non_terminal}
    )


def test_every_status_is_either_terminal_or_not():
    unplaced = _unclassified(RunStatus, TERMINAL_RUN_STATUSES, NON_TERMINAL_RUN_STATUSES)
    assert unplaced == set(), (
        f"{sorted(unplaced)} exist in RunStatus and in neither TERMINAL_RUN_STATUSES nor "
        "NON_TERMINAL_RUN_STATUSES. Every consumer derives from those two sets, so an "
        "unclassified status is invisible to all of them at once — including the "
        "`?wait=true` timeout branch and the dlt-directory sweep, which answer wrongly "
        "rather than raising. Put it in one of the two sets deliberately."
    )


def test_the_guard_can_see_an_unclassified_status():
    """The control. Without it, the assertion above is satisfied by a predicate that returns ∅."""

    class _EighthStatus(enum.StrEnum):
        PENDING = "pending"
        RUNNING = "running"
        CANCELLING = "cancelling"
        SUCCESS = "success"
        FAILED = "failed"
        CANCELLED = "cancelled"
        QUARANTINED = "quarantined"  # throwaway: classified nowhere

    unplaced = _unclassified(_EighthStatus, TERMINAL_RUN_STATUSES, NON_TERMINAL_RUN_STATUSES)

    assert unplaced == {"quarantined"}, unplaced


def test_the_two_sets_do_not_overlap():
    """A status in both is a consumer's coin flip, depending which set it reads."""
    both = set(TERMINAL_RUN_STATUSES) & set(NON_TERMINAL_RUN_STATUSES)
    assert both == set(), sorted(s.value for s in both)


@pytest.mark.parametrize(
    "name,members",
    [
        ("TERMINAL_RUN_STATUSES", TERMINAL_RUN_STATUSES),
        ("NON_TERMINAL_RUN_STATUSES", NON_TERMINAL_RUN_STATUSES),
        ("CANCELLABLE_RUN_STATUSES", CANCELLABLE_RUN_STATUSES),
        ("CANCEL_REQUESTED_RUN_STATUSES", CANCEL_REQUESTED_RUN_STATUSES),
    ],
)
def test_no_set_names_a_status_the_enum_cannot_produce(name, members):
    """The other direction: a stale entry is a decision about nothing, and reads as a live one."""
    unknown = {s for s in members if s not in set(RunStatus)}
    assert unknown == set(), f"{name} names {sorted(unknown)}, which RunStatus cannot produce"


def test_a_cancellable_status_is_never_terminal():
    """Cancelling a finished run is the `409` case, so the two sets must not intersect."""
    assert not (set(CANCELLABLE_RUN_STATUSES) & set(TERMINAL_RUN_STATUSES))


def test_cancel_requested_spans_the_partition_on_purpose():
    """It is cross-cutting, not a third bucket — and that is why it is not in the totality check.

    `CANCELLING` is non-terminal and `CANCELLED` is terminal. What they share is that an ordinary
    outcome must not overwrite either. Asserting the span keeps someone from "tidying" it into one
    side, which would silently let a completed worker report `success` over a requested stop.
    """
    assert set(CANCEL_REQUESTED_RUN_STATUSES) & set(TERMINAL_RUN_STATUSES)
    assert set(CANCEL_REQUESTED_RUN_STATUSES) & set(NON_TERMINAL_RUN_STATUSES)
