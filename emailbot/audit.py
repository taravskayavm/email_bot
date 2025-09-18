"""Audit helpers for logging drops and suppressions."""

from __future__ import annotations

import csv
import os
from datetime import datetime, timezone

AUDIT_PATH = os.getenv("AUDIT_PATH", "var/audit.csv")


def _audit_path() -> str:
    path = os.getenv("AUDIT_PATH", AUDIT_PATH)
    return path


def write_audit_drop(email: str, reason: str, details: str = "") -> None:
    """Append a drop record to ``audit.csv``."""

    path = _audit_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as fp:
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
