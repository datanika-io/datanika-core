"""Print resolved reviewable settings for the drift check (datanika-io/datanika-cloud#141).

Runs inside a container. The caller passes env names on argv. Each value is
read from the settings object, which is the check core#646 requires: an env
var can exist under a name the process never reads.

A name matching the secret pattern (*_KEY, *_SECRET, *_PASSWORD, *_TOKEN,
*_DSN, *_URL) is skipped. This file never reads the process environment and
never prints those values.
"""

from __future__ import annotations

import importlib
import re
import sys
from typing import Any

_SECRET = re.compile(r"(?:^|_)(?:KEY|SECRET|PASSWORD|TOKEN|DSN|URL)$")
_NAME = re.compile(r"[A-Z][A-Z0-9_]*")


def _env_name(field_name: str, field: Any) -> str:
    alias = getattr(field, "validation_alias", None)
    if isinstance(alias, str) and alias:
        return alias
    return field_name.upper()


def _format(value: object) -> str | None:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    text = str(value)
    if "\n" in text or "\r" in text:
        return None
    return text


def _take(found: dict[str, object], obj: object) -> None:
    fields = getattr(obj, "model_fields", None)
    if not isinstance(fields, dict):
        return
    for field_name, field in fields.items():
        if not isinstance(field_name, str):
            continue
        env_name = _env_name(field_name, field)
        if _SECRET.search(env_name):
            continue
        found[env_name] = getattr(obj, field_name)


def _collect() -> dict[str, object]:
    found: dict[str, object] = {}
    from datanika.config import settings

    _take(found, settings)
    try:
        cloud = importlib.import_module("datanika_cloud.billing.config")
    except ImportError:
        cloud = None
    if cloud is not None:
        cloud_settings = getattr(cloud, "cloud_settings", None)
        if cloud_settings is not None:
            _take(found, cloud_settings)
    return found


def main(argv: list[str]) -> int:
    values = _collect()
    for name in argv:
        if _NAME.fullmatch(name) is None or _SECRET.search(name):
            continue
        if name not in values:
            print(f"UNREAD {name}")
            continue
        text = _format(values[name])
        if text is None:
            print(f"UNREAD {name}")
            continue
        print(f"{name}={text}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
