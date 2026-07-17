"""Shared OpenAPI / JSON-Schema utilities.

Used by both directions of our OpenAPI handling:
- ``openapi_inline.py`` — *emits* OpenAPI schemas from our config SoT.
- ``openapi_import.py`` — *consumes* an external OpenAPI 3.x spec into our config.

Keeping ``$ref`` resolution, name conversion, and JSON-Schema → column-type
mapping here means there is one implementation, tested once.
"""

from __future__ import annotations

import re
from typing import Any

MAX_REF_DEPTH = 50


def to_pascal(slug: str) -> str:
    """``google_sheets`` → ``GoogleSheets``, ``rest_api`` → ``RestApi``."""
    return "".join(part.capitalize() for part in re.split(r"[_\-]", slug) if part)


def resolve_ref(
    spec: dict,
    node: Any,
    *,
    _depth: int = 0,
    _seen: frozenset[str] | None = None,
) -> Any:
    """Follow a local ``$ref`` chain to the referenced node.

    Only resolves local refs (``#/...``). Non-ref nodes are returned unchanged.
    Cycle-safe (tracks seen refs) and depth-bounded — returns ``{}`` rather than
    looping or raising on a cycle, external ref, or over-deep chain.
    """
    if not isinstance(node, dict) or "$ref" not in node:
        return node
    seen = _seen or frozenset()
    ref = node["$ref"]
    if (
        _depth >= MAX_REF_DEPTH
        or not isinstance(ref, str)
        or ref in seen
        or not ref.startswith("#/")
    ):
        return {}
    target: Any = spec
    for part in ref[2:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        if not isinstance(target, dict) or part not in target:
            return {}
        target = target[part]
    return resolve_ref(spec, target, _depth=_depth + 1, _seen=seen | {ref})


def json_schema_to_column_type(schema: dict) -> str:
    """Map an (already ref-resolved) JSON-Schema node to a coarse column type.

    Flat mapping — nested objects/arrays collapse to ``json`` (columns are one
    level deep; we do not recursively expand nested structures).
    """
    if not isinstance(schema, dict):
        return "text"
    t = schema.get("type")
    if isinstance(t, list):  # OpenAPI 3.1 nullable unions, e.g. ["string", "null"]
        t = next((x for x in t if x != "null"), "string")
    fmt = schema.get("format", "")
    if t == "integer":
        return "bigint" if fmt in ("int64", "long") else "integer"
    if t == "number":
        return "double"
    if t == "boolean":
        return "boolean"
    if t in ("object", "array"):
        return "json"
    return "text"  # string / unspecified
