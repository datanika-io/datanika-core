"""YAML import — parse YAML pipeline configs for /api/v1/pipelines/yaml (E8).

Thin format-layer over the existing JSON v2 bulk-import schema. YAML is
a hand-friendlier alternative to JSON for power users + CLI workflows; the
on-disk schema is identical (same ``version: 2`` contract, same
connections/uploads/pipelines/transformations keys), only the
serialization differs.

Why not just accept YAML in the existing /api/v1/import endpoint?
Content-type negotiation on Starlette is doable but splits the docs story
("POST /api/v1/import accepts JSON OR YAML depending on Content-Type")
and adds a failure mode where JSON parse errors look like YAML parse
errors. A separate endpoint keeps the API stability tiers clean: JSON
import is stable, YAML is the power-user entry.

Ref: SPEC in PLAN_ENGINEERING.md §ELT Optimizations E8; Whisk YAML
shape at D:/Projects/whisk/data_repos/DB data transfer architecture.md
§"YAML Table Configuration".
"""

from __future__ import annotations

import yaml


class YamlImportError(ValueError):
    """Raised when YAML cannot be parsed or has the wrong top-level shape."""


def parse_yaml_import(body: bytes | str) -> dict:
    """Parse a YAML import body and return the dict the bulk_import path expects.

    Raises YamlImportError on parse errors or invalid top-level types.
    The caller is responsible for running the JSON-path validator
    (``_validate_import_payload``) on the result.
    """
    if isinstance(body, bytes):
        try:
            body = body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise YamlImportError(f"YAML body is not valid UTF-8: {exc}") from exc

    try:
        data = yaml.safe_load(body)
    except yaml.YAMLError as exc:
        # yaml.YAMLError includes MarkedYAMLError — line/column when available.
        raise YamlImportError(f"YAML parse error: {exc}") from exc

    if data is None:
        raise YamlImportError("YAML body is empty")

    if not isinstance(data, dict):
        raise YamlImportError(f"YAML top-level must be a mapping, got {type(data).__name__}")

    return data
