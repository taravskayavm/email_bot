"""Audit helpers for logging drops and suppressions."""

from __future__ import annotations

import csv
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from utils.paths import expand_path, ensure_parent

logger = logging.getLogger(__name__)

_DEFAULT_AUDIT_PATH = expand_path("var/audit.csv")
AUDIT_PATH = os.getenv("AUDIT_PATH", str(_DEFAULT_AUDIT_PATH))
_DEFAULT_EVENTS_PATH = expand_path("var/audit_events.jsonl")
AUDIT_EVENTS_PATH = os.getenv("AUDIT_EVENTS_PATH", str(_DEFAULT_EVENTS_PATH))


def _audit_path() -> Path:
    return expand_path(os.getenv("AUDIT_PATH", AUDIT_PATH))


def _events_path() -> Path:
    return expand_path(os.getenv("AUDIT_EVENTS_PATH", AUDIT_EVENTS_PATH))


def _json_ready(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_ready(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    return str(value)


class AuditWriter:
    """Append structured audit records for bulk send operations."""

    def __init__(self, path: Path, label: str, *, enabled: bool = True) -> None:
        self.path = path
        self.label = label
        self.enabled = enabled and bool(path)
        self._lock = threading.Lock()
        self._started_at = datetime.now(timezone.utc)
        if self.enabled:
            try:
                ensure_parent(path)
            except Exception:
                logger.debug("bulk audit ensure_parent failed", exc_info=True)
                self.enabled = False

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _write_meta(self) -> None:
        self._write_record(
            {
                "type": "meta",
                "label": self.label,
                "ts": self._started_at.isoformat(),
                "path": str(self.path),
                "pid": os.getpid(),
            }
        )

    def _write_record(self, record: dict[str, Any]) -> None:
        if not self.enabled:
            return
        try:
            payload = json.dumps({str(k): _json_ready(v) for k, v in record.items()}, ensure_ascii=False)
        except Exception:
            logger.debug("bulk audit json encode failed", exc_info=True)
            return
        try:
            with self._lock:
                with self.path.open("a", encoding="utf-8") as fh:
                    fh.write(payload)
                    fh.write("\n")
        except Exception:
            self.enabled = False
            logger.debug("bulk audit append failed", exc_info=True)

    def log_sent(self, email: str) -> None:
        self._write_record({"type": "sent", "email": email, "ts": self._now_iso()})

    def log_skip(
        self,
        email: str,
        reason: str,
        meta: Mapping[str, Any] | None = None,
    ) -> None:
        record: dict[str, Any] = {
            "type": "skip",
            "email": email,
            "reason": reason,
            "ts": self._now_iso(),
        }
        if meta:
            record["meta"] = _json_ready(dict(meta))
        self._write_record(record)

    def log_error(
        self,
        email: str,
        reason: str,
        meta: Mapping[str, Any] | None = None,
    ) -> None:
        record: dict[str, Any] = {
            "type": "error",
            "email": email,
            "reason": reason,
            "ts": self._now_iso(),
        }
        if meta:
            record["meta"] = _json_ready(dict(meta))
        self._write_record(record)


def write_audit(
    event: str,
    *,
    email: str | None = None,
    meta: Mapping[str, Any] | None = None,
) -> None:
    """Append a structured event record to ``audit_events.jsonl``."""

    path = _events_path()
    try:
        ensure_parent(path)
    except Exception:
        logger.debug("audit events ensure_parent failed", exc_info=True)
        return
    record: dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
    }
    if email is not None:
        record["email"] = email
    if meta:
        try:
            record["meta"] = _json_ready(dict(meta))
        except Exception:
            record["meta"] = _json_ready(meta)
    try:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False))
            fh.write("\n")
    except Exception:
        logger.debug("audit events append failed", exc_info=True)


def start_audit(label: str) -> AuditWriter:
    """Return an :class:`AuditWriter` writing to ``var/bulk_audit_*.jsonl``."""

    base_dir = expand_path("var")
    try:
        base_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        logger.debug("bulk audit base dir create failed", exc_info=True)
    timestamp = int(time.time())
    last_candidate: Path | None = None
    for offset in range(5):
        candidate = base_dir / f"bulk_audit_{timestamp + offset}.jsonl"
        writer = AuditWriter(candidate, label)
        if writer.enabled:
            writer._write_meta()
            return writer
        last_candidate = candidate
    fallback = last_candidate or (base_dir / f"bulk_audit_{timestamp}.jsonl")
    return AuditWriter(fallback, label, enabled=False)


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


__all__ = ["AuditWriter", "start_audit", "write_audit", "write_audit_drop"]
