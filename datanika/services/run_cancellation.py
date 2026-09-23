"""What cancelling a run does, in the words every surface uses (core#657).

``SPEC_RUN_CANCELLATION`` AC10 and AC12 put two sentences on three surfaces — the confirmation
dialog, the API response body and the docs — *worded the same*. A property of three surfaces
cannot be kept by any one of them, so the English lives here once and each surface reads it:

* the dialog: ``runs.cancel_body`` and ``runs.cancel_billing`` in ``i18n/en.json`` equal these
  constants (``tests/test_services/test_run_cancellation_wording.py``);
* the API: ``POST /api/v1/runs/{id}/cancel`` returns :func:`cancellation_notice` as ``notice``;
* the reference: the OpenAPI cancel operation's description is built from :data:`CANCEL_EFFECT`.

🔑 **This is the 2a wording, and the spec's D3 sentence is not.** D3 was written for a
mid-flight stop — *"Cancelling stops further loading … `append` will duplicate the partial
rows"* — and §7.2 declares that stop unbuildable today: both engines do their work in one opaque
call, so a run already inside it runs to the end. What IS shipped is the pre-flight checkpoint
(``ExecutionService.skip_if_cancelled``): a run that has not reached its engine never starts it.
The sentences below say exactly that much. ``SPEC_RUN_CANCELLATION`` D3a records the change and
the condition that reverses it — an engine call a cancel can interrupt.
"""

from __future__ import annotations

from datanika.config import settings

#: D3 in its 2a form: what a stop does and does not do, and what is left behind.
#:
#: ⚠️ **The last sentence names no object on purpose (Product, 2026-09-23).** This text is shown
#: for every ``Run``, and ``Run.target_type`` is a :class:`~datanika.models.dependency.NodeType`
#: — ``upload``, ``transformation`` or ``pipeline``. It said *"re-running an upload that
#: appends"*, so two of the three read an example about an object they are not running; worse, a
#: dbt user could read the upload-specific clause as an exemption, and an ``incremental``
#: materialisation appends exactly as an ``append`` upload does. Do not put the noun back. If a
#: per-type sentence is ever wanted, it has to branch here, because the API returns this string
#: as ``notice`` for every run type too.
CANCEL_EFFECT = (
    "A run that has not started its work yet stops before anything is read or written. "
    "Work already in progress cannot be interrupted: it runs to the end, and the run is then "
    "marked cancelled. Data already written to your destination stays there, so a re-run that "
    "appends loads those rows again."
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
