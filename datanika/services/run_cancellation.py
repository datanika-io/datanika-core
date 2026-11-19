"""What cancelling a run does, in the words every surface uses (core#657).

``SPEC_RUN_CANCELLATION`` AC10 and AC12 put two sentences on three surfaces — the confirmation
dialog, the API response body and the docs — *worded the same*. A property of three surfaces
cannot be kept by any one of them, so the English lives here once and each surface reads it:

* the dialog: ``runs.cancel_body`` and ``runs.cancel_billing`` in ``i18n/en.json`` equal these
  constants (``tests/test_services/test_run_cancellation_wording.py``);
* the API: ``POST /api/v1/runs/{id}/cancel`` returns :func:`cancellation_notice` as ``notice``;
* the reference: the OpenAPI cancel operation's description is built from :data:`CANCEL_EFFECT`.

The pre-flight checkpoint (``ExecutionService.skip_if_cancelled``) still stops a run that has
not reached its engine, and it still records zero rows. A cancel that becomes visible between
dlt chunks stops further loading (``stop_further_chunks``): rows already written stay, and
``rows_loaded`` is that partial count. One dbt invoke is still one call, so a model already
running finishes and later loads do not start. ``SPEC_RUN_CANCELLATION`` D3a keeps the earlier
2a wording; the 2026-11-19 note is the flip, and D3's sentence is what ships for loads.
"""

from __future__ import annotations

from datanika.config import settings

#: What a stop does, what stays, and what a re-run costs. The first sentence is the pre-flight
#: checkpoint. The rest is D3 for a load a cancel can cut between chunks, plus the dbt boundary:
#: one invoke already running finishes.
#:
#: ⚠️ **The disposition clause names no object on purpose (Product, 2026-09-23).** This text is
#: shown for every ``Run``, and ``Run.target_type`` is a
#: :class:`~datanika.models.dependency.NodeType` — ``upload``, ``transformation`` or
#: ``pipeline``. It once said *"re-running an upload that appends"*, so two of the three read an
#: example about an object they are not running; worse, a dbt user could read the upload-specific
#: clause as an exemption, and an ``incremental`` materialisation appends exactly as an
#: ``append`` upload does. Do not put the noun back. If a per-type sentence is ever wanted, it
#: has to branch here, because the API returns this string as ``notice`` for every run type too.
CANCEL_EFFECT = (
    "A run that has not started its work yet stops before anything is read or written. "
    "Cancelling stops further loading. Data already written to your destination stays there. "
    "A model already running finishes; later loads do not start. "
    "Re-running the pipeline reloads from the source according to the pipeline's write "
    "disposition — replace overwrites it, append will duplicate the partial rows, merge "
    "reconciles on the primary key."
)

#: AC12's sentence. Only true where something bills — see :func:`bills_usage`.
CANCEL_BILLING = "You are billed for what was processed before the run stopped."


def bills_usage() -> bool:
    """Does this deployment bill for runs at all?

    ``DATANIKA_EDITION=cloud`` is the setting that gates billing everywhere else, so it gates the
    billing sentence too. The open-source edition bills nobody, and telling a self-hoster *"you
    are billed for what was processed"* would be the one false sentence in an honest dialog.

    Read at call time, not import time, so a test (or a process) sees the edition it runs under.
    """
    return settings.datanika_edition == "cloud"


def cancellation_notice() -> str:
    """The plain-language statement an accepted cancel returns, for the edition in force."""
    return f"{CANCEL_EFFECT} {CANCEL_BILLING}" if bills_usage() else CANCEL_EFFECT
