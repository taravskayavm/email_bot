"""Audit helpers for logging drops and suppressions."""

from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from pathlib import Path

from utils.paths import expand_path, ensure_parent

_DEFAULT_AUDIT_PATH = expand_path("var/audit.csv")
AUDIT_PATH = os.getenv("AUDIT_PATH", str(_DEFAULT_AUDIT_PATH))


def _audit_path() -> Path:
    return expand_path(os.getenv("AUDIT_PATH", AUDIT_PATH))


def write_audit_drop(email: str, reason: str, details: str = "") -> None:
    """Append a drop record to ``audit.csv``."""

    path = _audit_path()
    ensure_parent(path)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        if not exists:
            writer.writerow(["ts", "email", "action", "reason", "details"])
        writer.writerow(
            [
                datetime.now(timezone.utc).isoformat(),
                email,
                "drop",
                reason,
                details,
            ]
        )
