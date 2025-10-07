"""Runtime self-diagnostic helpers."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from typing import List
from zoneinfo import ZoneInfo

from emailbot.settings import REPORT_TZ


@dataclass
class Check:
    name: str
    ok: bool
    note: str = ""


def _tcp_ping(host: str, port: int, timeout: float = 5.0) -> bool:
    if not host or port <= 0:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def run_selfcheck() -> List[Check]:
    checks: List[Check] = []

    required_env = ["EMAIL_ADDRESS", "EMAIL_PASSWORD", "IMAP_HOST", "IMAP_PORT"]
    missing = [name for name in required_env if not os.getenv(name)]
    checks.append(
        Check("ENV", not missing, "ok" if not missing else "нет: " + ", ".join(sorted(missing)))
    )

    try:
        ZoneInfo(REPORT_TZ)
        checks.append(Check("TZ", True, REPORT_TZ))
    except Exception:
        checks.append(Check("TZ", False, f"bad tz: {REPORT_TZ}"))

    for path in ("var", "var/sent_log.csv"):
        if not os.path.exists(path):
            checks.append(Check(f"FS:{path}", False, "нет файла/папки"))
            continue
        readable = os.access(path, os.R_OK)
        writable = os.access(path, os.W_OK)
        checks.append(Check(f"FS:{path}", readable and writable, "rw ok" if readable and writable else "нет прав rw"))

    imap_host = os.getenv("IMAP_HOST", "")
    imap_port = int(os.getenv("IMAP_PORT", "993") or 0)
    smtp_host = os.getenv("SMTP_HOST", imap_host)
    smtp_port = int(os.getenv("SMTP_PORT", "465") or 0)
    checks.append(Check("TCP:IMAP", _tcp_ping(imap_host, imap_port), f"{imap_host}:{imap_port}"))
    checks.append(Check("TCP:SMTP", _tcp_ping(smtp_host, smtp_port), f"{smtp_host}:{smtp_port}"))

    return checks


def format_checks(checks: List[Check]) -> str:
    lines = ["🩺 Диагностика:"]
    for item in checks:
        prefix = "✅" if item.ok else "❌"
        note = f" — {item.note}" if item.note else ""
        lines.append(f"{prefix} {item.name}{note}")
    return "\n".join(lines)
