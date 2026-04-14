"""Regression test: the connection-type picker must expose every
ConnectionType the backend knows about.

Context: core#120 shipped with the picker list hardcoded to 15 of 32
types. Seven connector setup guides were stranded because users
following them hit a UI that didn't list the connector at all. This
test makes that drift impossible — if the enum grows and the picker
list doesn't, CI fails here.
"""

from datanika.models.connection import ConnectionType
from datanika.ui.pages.connections import PICKER_TYPES


def test_picker_covers_every_connection_type():
    """Every ConnectionType enum value must appear in the picker list."""
    enum_values = {t.value for t in ConnectionType}
    picker_values = set(PICKER_TYPES)

    missing_from_picker = enum_values - picker_values
    assert not missing_from_picker, (
        f"The connection picker is missing {len(missing_from_picker)} "
        f"type(s) the backend supports: {sorted(missing_from_picker)}. "
        f"Add them to PICKER_TYPES in datanika/ui/pages/connections.py."
    )


def test_picker_has_no_unknown_types():
    """Every picker entry must be a real ConnectionType — no typos."""
    enum_values = {t.value for t in ConnectionType}
    picker_values = set(PICKER_TYPES)

    unknown_in_picker = picker_values - enum_values
    assert not unknown_in_picker, (
        f"The picker lists types the backend does not support: "
        f"{sorted(unknown_in_picker)}. Typo? Or remove them from PICKER_TYPES."
    )


def test_picker_has_no_duplicates():
    """Catches copy-paste mistakes when adding types."""
    assert len(PICKER_TYPES) == len(set(PICKER_TYPES)), (
        f"PICKER_TYPES contains duplicates: "
        f"{[t for t in PICKER_TYPES if PICKER_TYPES.count(t) > 1]}"
    )
