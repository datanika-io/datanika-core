"""Structured JSON logging configuration.

Import and call ``setup_logging()`` early in the application lifecycle
(before any logger is used). In production, logs are emitted as JSON
for easy aggregation in Grafana/Loki. In debug mode, human-readable
format is used instead.
"""

import json
import logging
import sys
from datetime import UTC, datetime


class JSONFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1]:
            entry["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "org_id"):
            entry["org_id"] = record.org_id
        if hasattr(record, "run_id"):
            entry["run_id"] = record.run_id
        return json.dumps(entry, default=str)


def setup_logging(*, debug: bool = False) -> None:
    """Configure root logger with JSON (prod) or text (dev) output."""
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Remove any existing handlers
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)

    if debug:
        fmt = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
        handler.setFormatter(logging.Formatter(fmt, datefmt="%H:%M:%S"))
    else:
        handler.setFormatter(JSONFormatter())

    root.addHandler(handler)

    # Quiet noisy third-party loggers
    for name in ("httpx", "httpcore", "urllib3", "sqlalchemy.engine"):
        logging.getLogger(name).setLevel(logging.WARNING)
