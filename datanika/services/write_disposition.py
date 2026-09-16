"""Which write disposition a load uses when its upload form never showed the control (core#1336).

The upload form renders its Write Disposition select only for SQL database sources. Until
core#1336 it still stored that select's default, ``append``, for every structured upload, and the
runner passed it to dlt at run level, where it overrides every resource. Every run of a SaaS,
OpenAPI or file upload re-fetches everything, because no dlt state crosses runs, so each re-run
landed a full second copy of every record. Both runs read green.

This module has no dependencies on purpose. The loader imports it to decide what to forward, and
the upload form imports it to recognise rows saved before the fix.
"""

from __future__ import annotations

from collections.abc import Mapping

#: The value the upload form's Write Disposition select starts on. Before core#1336 the form stored
#: it for every structured upload, including sources whose form never renders the select.
FORM_DEFAULT_WRITE_DISPOSITION = "append"

#: What the loader chooses for a source that re-fetches every record on every run. ``replace``
#: leaves the destination holding each record once. ``merge`` would need a primary key, which the
#: SaaS resources do not declare.
FULL_FETCH_WRITE_DISPOSITION = "replace"


def is_serialized_form_default(dlt_config: Mapping) -> bool:
    """Whether ``dlt_config`` carries the pair the structured form stored, rather than a choice.

    The structured form always writes ``mode``, which no non-SQL loader reads. So ``append``
    together with ``mode`` is the form's untouched default. A disposition sent without ``mode``
    came from raw JSON or the API, and it is a choice.

    Only meaningful for source types whose form hides the control. For a SQL source the same pair
    is what the user saw selected.
    """
    stored = dlt_config.get("write_disposition")
    return stored == FORM_DEFAULT_WRITE_DISPOSITION and "mode" in dlt_config
