"""Shared naming utilities — validation and snake_case conversion."""

import re

from datanika.errors import UserFacingError

_NAME_RE = re.compile(r"^[a-zA-Z0-9 ]+$")


def validate_name(name: str, entity_label: str) -> None:
    """Validate that *name* is non-empty and contains only alphanumeric chars + spaces."""
    stripped = name.strip()
    if not stripped:
        raise UserFacingError(f"{entity_label} name cannot be empty")
    if not _NAME_RE.match(stripped):
        raise UserFacingError(
            f"{entity_label} name must contain only alphanumeric characters and spaces"
        )


def to_snake_case(name: str) -> str:
    """Convert a human-readable name to snake_case."""
    return re.sub(r"\s+", "_", name.strip()).lower()
