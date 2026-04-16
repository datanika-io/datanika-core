"""Destination adapters for ELT raw landing.

Per SPEC_ELT_IR_ARCHITECTURE.md §5.3: each adapter implements the RawLander
protocol for its destination type. Unsupported destinations fall back to
the dlt-based lander.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from datanika.services.ir.schema import IR


class RawLander(Protocol):
    """Protocol for destination-specific raw data landing."""

    def land(
        self,
        source: object,
        ir: IR,
        destination_config: dict,
        run_id: int,
    ) -> dict:
        """Land source data into the destination's raw schema.

        Returns dict with keys: rows, bytes_in, bytes_out, batches.
        """
        ...


# Registry of destination type → lander
_LANDERS: dict[str, RawLander] = {}


def register_lander(destination_type: str, lander: RawLander) -> None:
    """Register a destination adapter."""
    _LANDERS[destination_type] = lander


def get_lander(destination_type: str) -> RawLander:
    """Get the lander for a destination type, falling back to dlt."""
    if destination_type in _LANDERS:
        return _LANDERS[destination_type]
    from datanika.services.destinations._dlt_fallback import DltFallbackLander

    return DltFallbackLander()


# Register built-in landers on import
def _register_builtins() -> None:
    from datanika.services.destinations.postgres import PostgresLander

    register_lander("postgres", PostgresLander())


_register_builtins()
