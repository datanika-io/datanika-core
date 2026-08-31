"""Beat-scheduled billing tick — core's announcer for the cloud overage path.

cloud#129. Cloud's ``charge_cycle_overages`` and ``emit_charge_incoming_notices``
describe themselves as running hourly, live in a module called ``tasks.py``, and
are documented as *"two hourly Celery tasks"* in the billing contract. **Neither
is a Celery task and neither was in any beat schedule** — ``datanika_cloud``
registers no tasks at all, and the only caller in either repo was the
``DATANIKA_E2E_ADMIN_ENABLE``-gated admin route, which is off in production. So
the 2026-07-24 V2 P5 cutover armed ``DATANIKA_OVERAGE_CHARGE_ENABLE`` on
functions nothing invoked.

Core must not import cloud, so it cannot schedule a cloud function directly.
The seam is the one the open-core boundary already uses everywhere else: core
announces, cloud subscribes. This module owns the *schedule*; cloud owns what
happens on the tick.

Nothing here is billing-specific beyond the event name — no amounts, no plan
knowledge, no Paddle. Core edition has no subscriber and the task is a
well-defined no-op returning ``{}``.
"""

import logging

from datanika.hooks import announce
from datanika.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)

BILLING_TICK_EVENT = "billing.hourly_tick"


@celery_app.task(name="datanika.billing_tick")
def billing_tick_task() -> dict:
    """Announce the hourly billing tick and return what subscribers reported.

    ``announce`` rather than ``emit``, deliberately. Under ``emit`` a raising
    subscriber propagates *and* starves every subscriber behind it — core#456,
    where exactly that turned completed runs into ``FAILED`` rows and stopped
    cloud's byte metering from ever recording. A tick that fans out to more
    than one subscriber must not inherit it.

    The returned dict is the readback channel. Subscribers write their counts
    into the context, the task returns it, and the Celery result backend holds
    it for 24h — a source independent of the billing module's own log lines,
    which is what cloud#120 asks for when the first real overage cycle lands.

    ⚠️ Consequently an empty ``{}`` is ambiguous between *"core edition, no
    subscriber"* and *"a subscriber ran and reported nothing"*. That is
    deliberate at this layer: core cannot tell those apart without knowing what
    cloud is, and the discriminator that matters — whether the tick fired at
    all — is the presence of a task result, not its contents.
    """
    results: dict = {}
    announce(BILLING_TICK_EVENT, context=results)
    logger.info("Billing tick complete: %s", results)
    return results
