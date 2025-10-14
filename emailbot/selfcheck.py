"""Runtime self-diagnostic helpers and startup pre-flight checks."""

from __future__ import annotations

import importlib
import logging
import os
import socket
from pathlib import Path
from dataclasses import dataclass
from typing import List
from zoneinfo import ZoneInfo

from emailbot.settings import REPORT_TZ
from emailbot.suppress_list import get_blocked_count

logger = logging.getLogger(__name__)

CRITICAL_ENV_VARS = [
    "EMAIL_ADDRESS",
    "EMAIL_PASSWORD",
]


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


def startup_selfcheck() -> List[str]:
    """Run basic startup diagnostics and return a list of errors."""

    errors: List[str] = []

    # 1) Проверка импорта хендлера рассылки
    try:
        mod = importlib.import_module("emailbot.handlers.manual_send")
        fn = getattr(mod, "send_all", None)
        if not callable(fn):
            errors.append("emailbot.handlers.manual_send.send_all not found/callable")
    except Exception as exc:  # pragma: no cover - defensive diagnostics
        errors.append(f"manual_send import failed: {exc!r}")

    # 2) Ключевые переменные окружения
    for var in CRITICAL_ENV_VARS:
        if not os.getenv(var):
            hint = ""
            try:
                cwd_env = Path.cwd() / ".env"
                if cwd_env.exists():
                    hint = f" (.env: {cwd_env})"
            except Exception:
                pass
            errors.append(f"ENV missing: {var}{hint}")

    # 3) Воркеры
    try:
        from emailbot import settings as _settings

        workers = getattr(_settings, "SEND_MAX_WORKERS", None) or getattr(
            _settings, "PARSE_MAX_WORKERS", None
        )
        if not isinstance(workers, int) or workers <= 0:
            errors.append(f"Bad workers value: {workers!r}")
    except Exception as exc:  # pragma: no cover - diagnostic path
        errors.append(f"settings import failed: {exc!r}")

    if errors:
        logger.error("startup_selfcheck failed", extra={"errors": errors})

    return errors


def run_selfcheck() -> List[Check]:
    checks: List[Check] = []
    warnings: List[str] = []

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
        checks.append(
            Check(
                f"FS:{path}",
                readable and writable,
                "rw ok" if readable and writable else "нет прав rw",
            )
        )

    imap_host = os.getenv("IMAP_HOST", "")
    imap_port = int(os.getenv("IMAP_PORT", "993") or 0)
    smtp_host = os.getenv("SMTP_HOST") or "smtp.mail.ru"
    smtp_port = int(os.getenv("SMTP_PORT", "465") or 0)
    if smtp_host.lower().startswith("imap."):
        warnings.append(
            f"SMTP_HOST='{smtp_host}' выглядит как IMAP. Используйте 'smtp.'"
        )
    checks.append(Check("TCP:IMAP", _tcp_ping(imap_host, imap_port), f"{imap_host}:{imap_port}"))
    checks.append(Check("TCP:SMTP", _tcp_ping(smtp_host, smtp_port), f"{smtp_host}:{smtp_port}"))

    if warnings:
        checks.append(Check("WARN", True, " | ".join(warnings)))

    try:
        checks.append(Check("STOPLIST", True, f"blocked={get_blocked_count()}"))
    except Exception as exc:
        checks.append(Check("STOPLIST", False, f"error: {exc}"))

    return checks


def format_checks(checks: List[Check]) -> str:
    lines = ["🩺 Диагностика:"]
    for item in checks:
        prefix = "✅" if item.ok else "❌"
        note = f" — {item.note}" if item.note else ""
        lines.append(f"{prefix} {item.name}{note}")
    return "\n".join(lines)
