"""Utility helpers for quick diagnostic commands."""

from __future__ import annotations

import json
import os
import platform
import ssl
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

import imaplib
import smtplib

from . import history_service, messaging, settings as S

MASK = "****"
_TRUTHY = {"1", "true", "yes", "on"}


def _mask_email(addr: str) -> str:
    try:
        local, _, domain = (addr or "").partition("@")
        if not local or not domain:
            return addr or ""
        head = local[:2]
        tail = local[-1:] if len(local) > 3 else ""
        return f"{head}{MASK}{tail}@{domain}"
    except Exception:
        return addr or ""


def _as_int(value: object, default: int) -> int:
    try:
        text = str(value).strip()
        if not text:
            return default
        return int(text)
    except Exception:
        return default


def _resolve_smtp_mode(raw_mode: str, ssl_flag: str) -> Tuple[str, bool, bool]:
    mode = (raw_mode or "").strip().lower()
    flag = (ssl_flag or "").strip().lower()
    if mode in {"ssl", "starttls", "plain"}:
        resolved = mode
    elif not mode or mode == "auto":
        resolved = "ssl" if flag in _TRUTHY or not flag else "starttls"
    else:
        resolved = mode
    use_ssl = resolved == "ssl"
    use_starttls = resolved == "starttls"
    return resolved, use_ssl, use_starttls


def env_snapshot() -> Dict[str, str]:
    return {
        "EMAIL_ADDRESS": _mask_email(os.getenv("EMAIL_ADDRESS", "")),
        "SMTP_HOST": os.getenv("SMTP_HOST", "smtp.mail.ru"),
        "SMTP_PORT": os.getenv("SMTP_PORT", ""),
        "SMTP_SSL": os.getenv("SMTP_SSL", ""),
        "SMTP_MODE": os.getenv("SMTP_MODE", ""),
        "IMAP_HOST": os.getenv("IMAP_HOST", "imap.mail.ru"),
        "IMAP_PORT": os.getenv("IMAP_PORT", ""),
        "IMAP_TIMEOUT": os.getenv("IMAP_TIMEOUT", ""),
        "SENT_MAILBOX": os.getenv("SENT_MAILBOX", ""),
        "DAILY_SEND_LIMIT": os.getenv("DAILY_SEND_LIMIT", str(S.DAILY_SEND_LIMIT)),
        "COOLDOWN_DAYS": os.getenv("COOLDOWN_DAYS", "180"),
        "REPORT_TZ": os.getenv("REPORT_TZ", ""),
        "OBFUSCATION_ENABLE": os.getenv("OBFUSCATION_ENABLE", ""),
        "CONFUSABLES_NORMALIZE": os.getenv("CONFUSABLES_NORMALIZE", ""),
        "STRICT_DOMAIN_VALIDATE": os.getenv("STRICT_DOMAIN_VALIDATE", ""),
        "IDNA_DOMAIN_NORMALIZE": os.getenv("IDNA_DOMAIN_NORMALIZE", ""),
    }


@dataclass
class PingResult:
    ok: bool
    detail: str
    latency_ms: int


def smtp_settings(snapshot: Dict[str, str]) -> Tuple[str, int, str, bool, bool]:
    host = snapshot.get("SMTP_HOST") or "smtp.mail.ru"
    port = _as_int(snapshot.get("SMTP_PORT"), 465)
    mode, use_ssl, use_starttls = _resolve_smtp_mode(
        snapshot.get("SMTP_MODE", ""), snapshot.get("SMTP_SSL", "")
    )
    if snapshot.get("SMTP_PORT") in (None, ""):
        if mode == "starttls" and port == 465:
            port = 587
        elif mode == "plain" and port == 465:
            port = 25
        elif mode == "ssl":
            port = 465
    return host, port, mode, use_ssl, use_starttls


def smtp_ping(
    host: str,
    port: int,
    mode: str,
    *,
    use_ssl: bool,
    use_starttls: bool,
    timeout: float = 5.0,
) -> PingResult:
    start = time.perf_counter()
    try:
        if use_ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, timeout=timeout, context=context) as client:
                client.noop()
        else:
            with smtplib.SMTP(host, port, timeout=timeout) as client:
                client.ehlo()
                if use_starttls:
                    context = ssl.create_default_context()
                    client.starttls(context=context)
                    client.ehlo()
                client.noop()
        latency = int((time.perf_counter() - start) * 1000)
        return PingResult(True, "connected", latency)
    except Exception as exc:  # pragma: no cover - network dependent
        latency = int((time.perf_counter() - start) * 1000)
        return PingResult(False, f"{type(exc).__name__}: {exc}", latency)


def imap_ping(host: str, port: int, *, timeout: float = 5.0) -> PingResult:
    start = time.perf_counter()
    try:
        with imaplib.IMAP4_SSL(host, port, timeout=timeout) as client:
            client.logout()
        latency = int((time.perf_counter() - start) * 1000)
        return PingResult(True, "connected", latency)
    except Exception as exc:  # pragma: no cover - network dependent
        latency = int((time.perf_counter() - start) * 1000)
        return PingResult(False, f"{type(exc).__name__}: {exc}", latency)


def history_db_info() -> Dict[str, object]:
    history_service.ensure_initialized()
    path_obj = getattr(history_service, "_INITIALIZED_PATH", None)
    if path_obj is None:
        raw = os.getenv("HISTORY_DB_PATH")
        if raw:
            try:
                from utils.paths import expand_path

                resolved = expand_path(raw)
            except Exception:
                resolved = Path(raw)
        else:
            try:
                from utils.paths import expand_path

                resolved = expand_path("var/state.db")
            except Exception:
                resolved = Path("var/state.db")
        path = Path(resolved)
    else:
        path = Path(path_obj)

    info: Dict[str, object] = {
        "path": str(path),
        "exists": False,
        "size_bytes": 0,
        "mtime": 0,
    }
    try:
        stat = path.stat()
    except FileNotFoundError:
        return info
    except Exception as exc:
        info["error"] = f"{type(exc).__name__}: {exc}"
        return info
    info["exists"] = True
    info["size_bytes"] = stat.st_size
    info["mtime"] = int(stat.st_mtime)
    return info


def sent_folder_status() -> Dict[str, object]:
    path = messaging.IMAP_FOLDER_FILE
    info: Dict[str, object] = {
        "path": str(path),
        "cached": False,
        "name": "",
        "size_bytes": 0,
        "mtime": 0,
    }
    try:
        if path.exists():
            stat = path.stat()
            info["size_bytes"] = stat.st_size
            info["mtime"] = int(stat.st_mtime)
            name = path.read_text(encoding="utf-8").strip()
            info["name"] = name
            info["cached"] = bool(name)
        return info
    except Exception as exc:
        info["error"] = f"{type(exc).__name__}: {exc}"
        return info


def build_diag_text() -> str:
    snapshot = env_snapshot()
    smtp_host, smtp_port, smtp_mode, use_ssl, use_starttls = smtp_settings(snapshot)
    imap_host = snapshot.get("IMAP_HOST") or "imap.mail.ru"
    imap_port = _as_int(snapshot.get("IMAP_PORT"), 993)

    smtp_result = smtp_ping(
        smtp_host, smtp_port, smtp_mode, use_ssl=use_ssl, use_starttls=use_starttls
    )
    imap_result = imap_ping(imap_host, imap_port)
    sent = sent_folder_status()
    history = history_db_info()

    try:
        import aiohttp
    except Exception:  # pragma: no cover - optional dependency
        aiohttp_version = "n/a"
    else:
        aiohttp_version = getattr(aiohttp, "__version__", "?")

    try:
        import telegram
    except Exception:  # pragma: no cover - optional dependency
        telegram_version = "n/a"
    else:
        telegram_version = getattr(telegram, "__version__", "?")

    python_version = platform.python_version()
    platform_info = platform.platform()

    lines = [
        "🔎 Диагностика (быстрая)",
        (
            f"• Python: {python_version} | Platform: {platform_info}"
            f" | telegram={telegram_version} aiohttp={aiohttp_version}"
        ),
        (
            "• SMTP: "
            f"{smtp_host}:{smtp_port} mode={smtp_mode} ⇒ "
            f"{'OK' if smtp_result.ok else 'FAIL'} ({smtp_result.latency_ms} ms)"
            + (f" – {smtp_result.detail}" if not smtp_result.ok else "")
        ),
        (
            "• IMAP: "
            f"{imap_host}:{imap_port} ⇒ {'OK' if imap_result.ok else 'FAIL'}"
            f" ({imap_result.latency_ms} ms)"
            + (f" – {imap_result.detail}" if not imap_result.ok else "")
        ),
    ]

    sent_status = "cached" if sent.get("cached") else "empty"
    sent_name = sent.get("name") or ""
    sent_path = sent.get("path") or ""
    if sent_name:
        sent_status += f" ({sent_name})"
    lines.append(f"• Sent cache: {sent_status} | file={sent_path}")

    history_line = (
        f"• History DB: {history.get('path', '')} | exists={history.get('exists')}"
        f" | size={history.get('size_bytes')} | mtime={history.get('mtime')}"
    )
    lines.append(history_line)

    lines.append("• ENV snapshot:")
    lines.append("  " + json.dumps(snapshot, ensure_ascii=False))
    return "\n".join(lines)


__all__ = [
    "PingResult",
    "build_diag_text",
    "env_snapshot",
    "history_db_info",
    "imap_ping",
    "sent_folder_status",
    "smtp_ping",
    "smtp_settings",
]
