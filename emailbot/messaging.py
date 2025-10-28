"""Message building and sending utilities."""

from __future__ import annotations

import asyncio
import csv
import email
import hashlib
import html
import imaplib
import json
import logging
import os
import re
import time
import secrets
import smtplib
import uuid

try:  # pragma: no cover - optional dependency in lightweight deployments
    import idna
except Exception:  # pragma: no cover - used when idna is absent
    idna = None
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from enum import Enum
from email.message import EmailMessage
from email.utils import formataddr, parseaddr, getaddresses
from pathlib import Path
from itertools import count
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    TYPE_CHECKING,
)
from threading import RLock

from .extraction import normalize_email, strip_html
from utils.email_clean import strip_invisibles
from .cooldown import (
    audit_emails,
    build_cooldown_service,
    normalize_email as cooldown_normalize_email,
)
from . import settings as settings_module

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from .cooldown import CooldownService
from .sanitizer import sanitize_batch
from emailbot import history_service
from utils import rules
from .smtp_client import SmtpClient, RobustSMTP, send_with_retry
from .audit import write_audit as audit_write_audit
from .utils import log_error
from .settings import REPORT_TZ
from emailbot import history_service
from .messaging_utils import (
    add_bounce,
    canonical_for_history,
    ensure_sent_log_schema,
    is_soft_bounce,
    is_hard_bounce,
    is_foreign,
    is_suppressed,
    suppress_add,
    upsert_sent_log,
    load_sent_log,
    load_seen_events,
    save_seen_events,
    was_sent_within,
    was_sent_today_same_content,
    SYNC_SEEN_EVENTS_PATH,
)
from . import suppress_list
from .suppress_list import add_to_blocklist, blocklist_path
from .run_control import register_task
from .cancel import is_cancelled
from .net_imap import imap_connect_ssl, get_imap_timeout

_TASK_SEQ = count()

logger = logging.getLogger(__name__)

_BULK_COOLDOWN_SERVICE: "CooldownService" | None = None
_HISTORY_SHIM_WARNED_ONCE = False

DEBUG_SAVE_EML = os.getenv("DEBUG_SAVE_EML", "0") == "1"


def write_audit(
    event: str,
    *,
    email: str | None = None,
    meta: Mapping[str, Any] | None = None,
) -> None:
    """Safely record structured audit events."""

    try:
        audit_write_audit(event, email=email, meta=meta)
    except Exception:
        logger.debug("write_audit failed", exc_info=True)


def _normalize_from_header(msg: EmailMessage) -> None:
    """Force the ``From`` header to use the configured SMTP address."""

    existing = msg.get("From", "")
    name, _addr = parseaddr(existing)
    display_name = os.getenv("EMAIL_FROM_NAME", "").strip() or name
    normalized = formataddr((display_name or "", EMAIL_ADDRESS))
    if "From" in msg:
        msg.replace_header("From", normalized)
    else:
        msg["From"] = normalized


def _reset_history_shim_warning() -> None:
    """Allow the history shim warning to be logged again."""

    global _HISTORY_SHIM_WARNED_ONCE
    _HISTORY_SHIM_WARNED_ONCE = False


def _get_bulk_cooldown_service():
    """Return a cached :class:`CooldownService` instance for bulk sends."""

    global _BULK_COOLDOWN_SERVICE
    if _BULK_COOLDOWN_SERVICE is None:
        try:
            _BULK_COOLDOWN_SERVICE = build_cooldown_service(settings_module.SETTINGS)
        except AttributeError:
            # ``SETTINGS`` may be absent in environments that import the module
            # directly; fall back to passing the module itself.
            _BULK_COOLDOWN_SERVICE = build_cooldown_service(settings_module)
        except Exception:  # pragma: no cover - defensive initialisation
            logger.debug("bulk cooldown init failed", exc_info=True)
            _BULK_COOLDOWN_SERVICE = build_cooldown_service(settings_module)
    return _BULK_COOLDOWN_SERVICE


# [EBOT-087] Запуск корутины Telegram строго в PTB-лупе из любого потока
def run_in_app_loop(application, coro):
    return asyncio.run_coroutine_threadsafe(coro, application.loop)


def log_domain_rate_limit(domain: str, sleep_s: float | int) -> None:
    """Log a per-domain throttle event and pause for ``sleep_s`` seconds."""

    delay = max(float(sleep_s or 0.0), 0.0)
    logger.info("rate-limit: domain=%s sleep=%.2fs", domain, delay)
    if delay > 0:
        time.sleep(delay)


def _normalize_recipient_list(values: Iterable[str]) -> list[str]:
    addresses: list[str] = []
    for _, addr in getaddresses(values):
        if not addr:
            continue
        addresses.append(addr.strip().lower())
    return sorted(dict.fromkeys(addresses))


def _make_send_key(msg: EmailMessage, *, now: datetime | None = None) -> str:
    """Return a stable idempotency key for ``msg`` within the current day."""

    timestamp = (now or datetime.now(timezone.utc)).date().isoformat()
    payload = {
        "from": (msg.get("From", "") or "").strip().lower(),
        "to": _normalize_recipient_list(msg.get_all("To", [])),
        "cc": _normalize_recipient_list(msg.get_all("Cc", [])),
        "subject": (msg.get("Subject", "") or "").strip(),
        "day": timestamp,
    }
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def was_sent_recently(msg: EmailMessage, *, window: float | None = None) -> bool:
    """Return ``True`` if ``msg`` was marked as sent within ``window`` seconds."""

    ttl = float(window if window is not None else _SENT_IDS_TTL)
    if ttl <= 0:
        return False
    key = _make_send_key(msg)
    now = time.time()
    with _sent_history_lock:
        _ensure_sent_history_loaded_locked()
        _prune_sent_history_locked(now)
        ts = _sent_history.get(key)
    if ts is None:
        return False
    return now - ts < ttl


def mark_sent(msg: EmailMessage) -> None:
    """Record that ``msg`` has been sent just now."""

    key = _make_send_key(msg)
    ts = time.time()
    with _sent_history_lock:
        _ensure_sent_history_loaded_locked()
        _prune_sent_history_locked(ts)
        _sent_history[key] = ts
        try:
            path = SENT_IDS_FILE
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handler:
                handler.write(json.dumps({"key": key, "ts": ts}, ensure_ascii=False) + "\n")
        except Exception:
            logger.debug("Failed to append to sent idempotency file", exc_info=True)


async def send_bulk(emails: Iterable[str], template_key: str) -> Tuple[int, int, int]:
    """Send e-mails in bulk using the same cooldown service as the preview step."""

    candidates = list(emails)
    if not candidates:
        return 0, 0, 0

    _reset_history_shim_warning()

    try:
        service = _get_bulk_cooldown_service()
        ready, hits = service.filter_ready(candidates)
    except Exception:  # pragma: no cover - defensive logging
        logger.debug("bulk send: cooldown filter failed", exc_info=True)
        ready = candidates
        hits = []

    skipped_180 = len(hits)
    ready_list = list(ready)
    if not ready_list:
        return 0, skipped_180, 0

    def _send_ready() -> Tuple[int, int, int]:
        template_path = TEMPLATE_MAP.get(template_key) or template_key
        template_label = template_key or Path(template_path).stem

        smtp_client = RobustSMTP()
        imap_client = None
        sent_folder = ""
        sent_count = 0
        extra_cooldown = 0
        errors = 0

        try:
            host = os.getenv("IMAP_HOST", "imap.mail.ru")
            port = _parse_int(os.getenv("IMAP_PORT"), 993)
            timeout = get_imap_timeout()
            imap_client = imap_connect_ssl(host, port, timeout)
            imap_client.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            sent_folder = get_preferred_sent_folder(imap_client)
            imap_client.select(f'"{sent_folder}"')
        except Exception:
            logger.exception("bulk send: IMAP initialisation failed")
            try:
                smtp_client.close()
            except Exception:
                pass
            if imap_client is not None:
                try:
                    imap_client.logout()
                except Exception:
                    pass
            return 0, 0, len(ready_list)

        try:
            for email_addr in ready_list:
                try:
                    outcome, _, _, _ = send_email_with_sessions(
                        smtp_client,
                        imap_client,
                        sent_folder,
                        email_addr,
                        template_path,
                        subject=DEFAULT_SUBJECT,
                        group_title=template_label,
                        group_key=template_key,
                    )
                except Exception:
                    errors += 1
                    logger.exception("bulk send: SMTP failure for %s", email_addr)
                    continue

                if outcome == SendOutcome.SENT:
                    sent_count += 1
                elif outcome == SendOutcome.COOLDOWN:
                    extra_cooldown += 1
                elif outcome == SendOutcome.ERROR:
                    errors += 1
        finally:
            try:
                smtp_client.close()
            except Exception:
                pass
            if imap_client is not None:
                try:
                    imap_client.logout()
                except Exception:
                    pass

        return sent_count, extra_cooldown, errors

    sent, extra_skipped, errors = await asyncio.to_thread(_send_ready)
    return sent, skipped_180 + extra_skipped, errors


# --------------------------------------------------------------------------------------
# Совместимость/утилиты
# --------------------------------------------------------------------------------------
# [EBOT-088] Backward-compat shim:
# Ранее части кода вызывали messaging._normalize_key(...) для унификации chat_id/ключей.
# Восстанавливаем утилиту, чтобы не падали старые вызовы.
def _normalize_key(val: Any) -> Optional[int | str]:
    """
    Нормализует вход (context/update/chat/message/chat_id/строку) в int chat_id или строковый ключ.
    Возвращает:
      - int (если ушёл chat_id),
      - str (если не удалось привести к int),
      - None (если val пустой).
    Поведение максимально безопасное и «толерантное» к типу входа.
    """

    if val is None:
        return None

    try:
        # Частые случаи: сам chat_id, Update/Message/Chat, объект с .chat_id/.chat.id/.id
        # 1) Прямо int
        if isinstance(val, int):
            return val

        # 2) У объекта есть .chat_id
        chat_id = getattr(val, "chat_id", None)
        if isinstance(chat_id, int):
            return chat_id

        # 3) У объекта есть .chat.id
        chat = getattr(val, "chat", None)
        if chat is not None:
            cid = getattr(chat, "id", None)
            if isinstance(cid, int):
                return cid

        # 4) У объекта есть .id (и это int)
        obj_id = getattr(val, "id", None)
        if isinstance(obj_id, int):
            return obj_id

        # 5) Строки/прочее: попробуем привести к int, иначе вернём строку
        s = str(val).strip()
        if s.isdigit():
            try:
                return int(s)
            except Exception:
                pass
        return s
    except Exception:
        # Никогда не падаем из-за утилиты
        try:
            return int(str(val).strip())
        except Exception:
            return str(val).strip()


# [EBOT-090] Backward-compat shim:
# Обёртка над history_service для правила «1 письмо на 1 адрес раз в N дней».
# Поддерживает старые вызовы с разными сигнатурами и названиями функций.
def _should_skip_by_history(
    email: str,
    days: Optional[int] = None,
    chat_id: Optional[int] = None,
    **kwargs,
) -> tuple[bool, str]:
    """
    Возвращает пару (skip, reason), где skip == True означает, что по истории отправок адрес
    нужно пропустить (кулдаун не прошёл).
    Аргументы:
      email: адрес получателя
      days: период в днях (если не задан — берём из settings, по умолчанию 180)
      chat_id: игнорируется (для совместимости со старыми вызовами)
      **kwargs: игнорируются, но не ломают вызов
    """

    try:
        # Ленивая загрузка, чтобы не создавать циклические импорты на старте
        from . import history_service as _hist, settings as _settings
    except Exception as exc:  # pragma: no cover - логирование важнее возврата
        logger.warning(
            "history shim: cannot import settings/history_service: %r",
            exc,
        )
        # безопасная деградация: не скипаем (правило 180д должно отработать дальше в пайплайне)
        return False, ""

    try:
        cooldown = int(days) if days is not None else int(
            getattr(_settings, "SEND_COOLDOWN_DAYS", 180)
        )
    except Exception:
        cooldown = 180

    def _normalize_result(result: Any) -> Optional[tuple[bool, str]]:
        if result is None:
            return None
        if isinstance(result, tuple):
            if not result:
                return False, ""
            skip = bool(result[0])
            reason = ""
            if len(result) > 1:
                try:
                    reason = str(result[1])
                except Exception:
                    reason = ""
            if skip and not reason:
                reason = "cooldown"
            return skip, reason
        try:
            skip = bool(result)
        except Exception:
            return None
        reason = "cooldown" if skip else ""
        return skip, reason

    def _try(fn: Callable, *args, **kwargs_inner) -> Optional[tuple[bool, str]]:
        try:
            return _normalize_result(fn(*args, **kwargs_inner))
        except TypeError:
            variants = (
                {"email": email, "cooldown_days": cooldown},
                {"email": email, "days": cooldown},
                {"email": email, "cutoff_days": cooldown},
                {"email": email},
            )
            for variant in variants:
                try:
                    return _normalize_result(fn(**variant))
                except Exception:
                    continue
        except Exception:
            return None
        return None

    candidates = (
        "was_sent_within",
        "was_sent_recent",
        "check_recent",
        "should_skip_by_history",
        "skip_if_recent",
    )
    for name in candidates:
        fn = getattr(_hist, name, None)
        if callable(fn):
            result = _try(fn, email, cooldown)
            if isinstance(result, tuple):
                return result

    global _HISTORY_SHIM_WARNED_ONCE
    if not _HISTORY_SHIM_WARNED_ONCE:
        logger.info("history shim: no suitable function in history_service; fallback=False")
        _HISTORY_SHIM_WARNED_ONCE = True
    return False, ""


def __getattr__(name: str):
    """Backward-compatibility shims for legacy helpers.

    Older modules occasionally referenced private helpers from ``messaging``.  To avoid
    import errors when those helpers move or become internal, we expose safe defaults
    here on demand.
    """

    alias_map = {
        "_normalize_key": _normalize_key,
        "_should_skip_by_history": _should_skip_by_history,
        "run_in_app_loop": run_in_app_loop,
    }
    if name in alias_map:
        value = alias_map[name]
        globals()[name] = value
        return value

    if name.startswith("_send_"):
        async def _noop(*args: Any, **kwargs: Any) -> None:
            return None

        globals()[name] = _noop
        return _noop

    raise AttributeError(name)

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def parse_emails_from_text(text: str) -> list[str]:
    """Extract e-mail addresses from arbitrary text.

    The parser accepts comma, semicolon, whitespace and newline separators, trims
    surrounding punctuation, normalizes case and removes duplicates while
    preserving order of first appearance.
    """

    if not text:
        return []

    found = EMAIL_RE.findall(text)
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in found:
        normalized = raw.strip().strip(",;").lower()
        if not normalized or normalized in seen:
            continue
        cleaned.append(normalized)
        seen.add(normalized)
    return cleaned

# Resolve the project root (one level above this file) and use shared
# directories located at the repository root.
from utils.paths import expand_path
from emailbot.policy import decide, Decision
from emailbot import ledger
SCRIPT_DIR = Path(__file__).resolve().parent.parent
DOWNLOAD_DIR = str(SCRIPT_DIR / "downloads")
# Был жёсткий путь /mnt/data/sent_log.csv → падало на Windows/Linux без /mnt.
LOG_FILE = str(expand_path(os.getenv("SENT_LOG_PATH", "var/sent_log.csv")))
# Путь к блок-листу фиксирован относительно корня репозитория.
BLOCKED_FILE = str(blocklist_path())
try:
    logger.info("Stoplist path resolved: %s", BLOCKED_FILE)
except Exception:
    logger.debug("Unable to log stoplist path", exc_info=True)
MAX_EMAILS_PER_DAY = int(os.getenv("MAX_EMAILS_PER_DAY", "300"))

SYNC_STATE_PATH = str(expand_path(os.getenv("SYNC_STATE_PATH", "var/sync_state.json")))

def _parse_int(value: str | None, default: int) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default

EMAIL_SYNC_INTERVAL_HOURS = _parse_int(os.getenv("EMAIL_SYNC_INTERVAL_HOURS"), 24)
EMAIL_LOOKBACK_DAYS = _parse_int(
    os.getenv("EMAIL_LOOKBACK_DAYS", os.getenv("HALF_YEAR_DAYS")),
    180,
)

_BLOCK_READY = False


def ensure_blocklist_ready() -> None:
    """Initialise the shared block-list lazily."""

    global _BLOCK_READY
    if _BLOCK_READY:
        return
    try:
        suppress_list.init_blocked()
        _BLOCK_READY = True
    except Exception:
        logger.debug("blocklist init failed", exc_info=True)


def _to_idna(domain: str) -> str:
    if not domain or idna is None:
        return domain
    try:
        return idna.encode(domain, uts46=True).decode("ascii")
    except Exception:
        return domain


def _normalize_email_for_blocklist(addr: str) -> str:
    addr = (addr or "").strip().lower()
    try:
        local, dom = addr.split("@", 1)
        dom_idna = _to_idna(dom)
        return f"{local}@{dom_idna}"
    except Exception:
        return addr


def add_blocked_email(email: str) -> bool:
    """Persist ``email`` to the shared block-list file if it is new."""

    ensure_blocklist_ready()
    try:
        norm = _normalize_email_for_blocklist(email)

        if not norm or "@" not in norm:
            return False

        added = add_to_blocklist(norm)
        if added:
            logger.info("blocked_emails: added %s -> %s", norm, BLOCKED_FILE)
        return added
    except Exception:
        logger.exception("Failed to add email to blocklist: %r", email)
        return False


def _flag_enabled(value: str | None) -> bool:
    if value is None:
        return False
    lowered = value.strip().lower()
    return lowered in {"1", "true", "yes", "on"}


def _extra_blocklist() -> set[str]:
    raw = os.getenv("FILTER_BLOCKLIST", "")
    if not raw:
        return set()
    items: set[str] = set()
    for token in raw.split(","):
        candidate = _normalize_email_for_blocklist(token)
        if candidate and "@" in candidate:
            items.add(candidate)
    return items


def _should_block_support_addresses() -> bool:
    return _flag_enabled(os.getenv("FILTER_SUPPORT"))


def _is_blocklisted(email: str) -> bool:
    """Return ``True`` if ``email`` is rejected by local block-list rules."""

    if not email:
        return False
    ensure_blocklist_ready()
    suppress_list.refresh_if_changed()

    normalized = _normalize_email_for_blocklist(email)
    if not normalized or "@" not in normalized:
        return False

    if normalized in _DEFAULT_BLOCKLIST:
        return True
    if normalized in _extra_blocklist():
        return True

    local = normalized.split("@", 1)[0]
    if _should_block_support_addresses() and local.lower().startswith("support"):
        return True

    try:
        return suppress_list.is_blocked(normalized)
    except Exception:
        logger.debug("blocklist lookup failed", exc_info=True)
        return False


# HTML templates are stored at the root-level ``templates`` directory.
TEMPLATES_DIR = str(SCRIPT_DIR / "templates")
TEMPLATE_MAP = {
    "beauty": os.path.join(TEMPLATES_DIR, "beauty.html"),
    "geography": os.path.join(TEMPLATES_DIR, "geography.html"),
    "highmedicine": os.path.join(TEMPLATES_DIR, "highmedicine.html"),
    "medicalcybernetics": os.path.join(
        TEMPLATES_DIR, "medicalcybernetics.html"
    ),
    "lowmedicine": os.path.join(TEMPLATES_DIR, "lowmedicine.html"),
    "nursing": os.path.join(TEMPLATES_DIR, "nursing.html"),
    "pharmacy": os.path.join(TEMPLATES_DIR, "pharmacy.html"),
    "preventiomed": os.path.join(TEMPLATES_DIR, "preventiomed.html"),
    "psychology": os.path.join(TEMPLATES_DIR, "psychology.html"),
    "sport": os.path.join(TEMPLATES_DIR, "sport.html"),
    "stomatology": os.path.join(TEMPLATES_DIR, "stomatology.html"),
    "tourism": os.path.join(TEMPLATES_DIR, "tourism.html"),
    # Дополнительные коды для обратной совместимости
    "medicine": os.path.join(TEMPLATES_DIR, "medicine.html"),
    "bioinformatics": os.path.join(TEMPLATES_DIR, "bioinformatics.html"),
}


DEFAULT_SUBJECT = "Издательство Лань приглашает к сотрудничеству"


class SendOutcome(Enum):
    SENT = "sent"
    COOLDOWN = "cooldown"
    BLOCKED = "blocked"
    ERROR = "error"
    DUPLICATE = "duplicate"


def _outcome_for_decision(decision: Decision) -> SendOutcome:
    if decision is Decision.SKIP_COOLDOWN:
        return SendOutcome.COOLDOWN
    if decision in {Decision.SKIP_BLOCKED, Decision.SKIP_ROLE, Decision.SKIP_DOMAIN_POLICY}:
        return SendOutcome.BLOCKED
    return SendOutcome.ERROR

# Text of the signature without styling. The surrounding block and
# font settings are injected dynamically based on the template used for
# the message.
SIGNATURE_TEXT = (
    "--<br>С уважением,<br>"
    "Таравская Владлена Михайловна<br>"
    "Заведующая редакцией литературы по медицине, спорту и туризму<br>"
    "ООО Издательство «ЛАНЬ»<br><br>"
    "8 (812) 336-90-92, доб. 208<br><br>"
    "196105, Санкт-Петербург, проспект Юрия Гагарина, д.1 лит.А<br><br>"
    "Рабочие часы: 10.00-18.00<br><br>"
    "med@lanbook.ru<br>"
    '<a href="https://www.lanbook.com">www.lanbook.com</a>'
)
SIGNATURE_HTML = SIGNATURE_TEXT
EMAIL_ADDRESS = ""
EMAIL_PASSWORD = ""

IMAP_FOLDER_FILE = SCRIPT_DIR / "imap_sent_folder.txt"

_last_domain_send: Dict[str, float] = {}
_DOMAIN_RATE_LIMIT = 1.0  # seconds between sends per domain
_sent_idempotency: Set[str] = set()

_DEFAULT_BLOCKLIST = {
    "no-reply@site.com",
    "mailer-daemon@site.com",
}

SENT_IDS_FILE: Path = Path(os.getenv("SENT_IDS_FILE", "var/sent_ids.jsonl"))
_SENT_IDS_TTL = int(os.getenv("SENT_IDS_TTL", "86400"))
_sent_history_lock = RLock()
_sent_history: Dict[str, float] = {}
_sent_history_loaded = False


def _ensure_sent_history_loaded_locked() -> None:
    global _sent_history_loaded
    if _sent_history_loaded:
        return
    path = SENT_IDS_FILE
    try:
        with path.open("r", encoding="utf-8") as handler:
            for raw in handler:
                try:
                    record = json.loads(raw)
                except Exception:
                    continue
                key = record.get("key") or record.get("id")
                ts = record.get("ts") or record.get("timestamp")
                if not key or not isinstance(ts, (int, float)):
                    continue
                _sent_history[str(key)] = float(ts)
    except FileNotFoundError:
        pass
    except Exception:
        logger.debug("Failed to load sent idempotency file", exc_info=True)
    _sent_history_loaded = True


def _rewrite_sent_history_locked() -> None:
    path = SENT_IDS_FILE
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handler:
            for key, ts in sorted(_sent_history.items(), key=lambda item: item[1]):
                handler.write(json.dumps({"key": key, "ts": ts}, ensure_ascii=False) + "\n")
    except Exception:
        logger.debug("Failed to rewrite sent idempotency file", exc_info=True)


def _prune_sent_history_locked(now: float) -> None:
    if not _sent_history:
        return
    cutoff = now - float(max(_SENT_IDS_TTL, 0))
    stale = [key for key, ts in _sent_history.items() if ts < cutoff]
    if not stale:
        return
    for key in stale:
        _sent_history.pop(key, None)
    _rewrite_sent_history_locked()

_RE_JINJA = re.compile(r"\{\{\s*([A-Z0-9_]+)\s*\}\}")
_RE_FMT = re.compile(r"\{([A-Z0-9_]+)\}")


@dataclass
class TemplateRenderError(Exception):
    path: str
    missing: set[str]
    found: set[str]

    def __str__(self) -> str:
        return f"Unresolved placeholders: {sorted(self.missing)} in {self.path}"


def _find_placeholders(text: str) -> set[str]:
    keys = {match.group(1) for match in _RE_JINJA.finditer(text)}
    keys |= {match.group(1) for match in _RE_FMT.finditer(text)}
    return keys


def _ensure_sent_log_schema(path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists() or p.stat().st_size == 0:
        with p.open("w", encoding="utf-8", newline="") as f:
            f.write("ts,email,subject,message_id\n")


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        cleaned = value.strip().replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except Exception:
        return None


def _load_recent_sent(days: int) -> set[str]:
    if days <= 0:
        return set()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    recent: set[str] = set()
    try:
        _ensure_sent_log_schema(LOG_FILE)
        with open(LOG_FILE, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                email_raw = (row.get("email") or row.get("key") or "").strip().lower()
                ts = _parse_ts(
                    row.get("ts")
                    or row.get("time")
                    or row.get("sent_at")
                    or row.get("last_sent_at")
                )
                if not email_raw or not ts or ts < cutoff:
                    continue
                try:
                    email_norm = normalize_email(email_raw)
                except Exception:
                    email_norm = email_raw
                recent.add(email_norm)
    except FileNotFoundError:
        return set()
    except Exception as exc:
        logger.warning("recent-sent load failed: %s", exc)
    return recent


def _seen_in_local_history(addr: str, days: int) -> bool:
    if days <= 0:
        return False
    path = rules.HISTORY_PATH
    if not path.exists():
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                email = str(rec.get("email", "")).strip().lower()
                if email != addr:
                    continue
                raw_ts = rec.get("ts")
                if not isinstance(raw_ts, str) or not raw_ts.strip():
                    continue
                try:
                    ts = datetime.fromisoformat(raw_ts)
                except Exception:
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                else:
                    ts = ts.astimezone(timezone.utc)
                if ts >= cutoff:
                    return True
    except Exception:
        return False
    return False


def _read_sync_state() -> dict:
    try:
        with open(SYNC_STATE_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_sync_state(state: dict) -> None:
    try:
        path = Path(SYNC_STATE_PATH)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False)
    except Exception as exc:
        logger.debug("sync state write failed: %s", exc)


def _need_sync_now(now: datetime) -> tuple[bool, Optional[datetime]]:
    if EMAIL_SYNC_INTERVAL_HOURS <= 0:
        return True, None
    state = _read_sync_state()
    last_iso = state.get("last_sync_iso") if isinstance(state, dict) else None
    last_dt: Optional[datetime] = None
    if isinstance(last_iso, str):
        try:
            parsed = datetime.fromisoformat(last_iso)
            last_dt = parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
        except Exception:
            last_dt = None
    if not last_dt:
        return True, None
    delta = now - last_dt
    if delta.total_seconds() >= EMAIL_SYNC_INTERVAL_HOURS * 3600:
        return True, last_dt
    return False, last_dt


def _mark_synced(now: datetime) -> None:
    _write_sync_state({"last_sync_iso": now.isoformat()})


def _validate_email_basic(email_value: str) -> bool:
    return bool(EMAIL_RE.fullmatch(email_value))


def _render_placeholders(text: str, ctx: dict[str, object]) -> str:
    """Safely substitute ``{{KEY}}`` and ``{KEY}`` placeholders from ``ctx``."""

    def _sub_jinja(match: re.Match[str]) -> str:
        key = match.group(1)
        return str(ctx.get(key, match.group(0)))

    def _sub_fmt(match: re.Match[str]) -> str:
        key = match.group(1)
        return str(ctx.get(key, match.group(0)))

    text = _RE_JINJA.sub(_sub_jinja, text)
    text = _RE_FMT.sub(_sub_fmt, text)
    return text


def _has_unresolved_placeholders(text: str) -> bool:
    """Return ``True`` if ``text`` still contains unresolved placeholders."""

    return bool(_RE_JINJA.search(text) or _RE_FMT.search(text))


def _has_placeholder(text: str, key: str) -> bool:
    key_upper = key.upper()
    for match in _RE_JINJA.finditer(text):
        if match.group(1) == key_upper:
            return True
    for match in _RE_FMT.finditer(text):
        if match.group(1) == key_upper:
            return True
    return False


def text_to_html(text: str) -> str:
    """Convert plain text body to a simple HTML representation."""

    if not text:
        return ""
    lines = html.escape(text).splitlines()
    return "<br>".join(lines)


def build_signature_text() -> str:
    """Return the plain-text representation of the default signature."""

    return strip_html(SIGNATURE_HTML).strip()


def build_email_body(template_path: str, variables: Optional[dict[str, object]]) -> tuple[str, str]:
    """Return rendered text and HTML bodies for a template file."""

    path = Path(template_path)
    tpl = path.read_text(encoding="utf-8")

    ctx: dict[str, object] = dict(variables or {})
    if "SIGNATURE" not in ctx:
        ctx["SIGNATURE"] = build_signature_text()

    found_keys = _find_placeholders(tpl)
    if "BODY" in found_keys and "BODY" not in ctx:
        body_candidates = [
            path.with_name(path.stem + ".body" + path.suffix),
            path.with_suffix(".body.txt"),
            path.parent / "body" / path.name,
        ]
        for body_path in body_candidates:
            if body_path.exists():
                try:
                    ctx["BODY"] = body_path.read_text(encoding="utf-8").strip()
                    logger.info("Loaded BODY from %s", body_path)
                    break
                except Exception as exc:
                    logger.warning("Can't read BODY file %s: %s", body_path, exc)

    text_body = _render_placeholders(tpl, ctx)
    if _has_unresolved_placeholders(text_body):
        unresolved = _find_placeholders(text_body)
        missing = {key for key in unresolved if key not in ctx}
        raise TemplateRenderError(str(path), missing=missing, found=found_keys)

    html_body = text_to_html(text_body)
    return text_body, html_body


def _read_template_file(path: str) -> str:
    if not os.path.exists(path):
        alt = os.path.splitext(path)[0] + ".html"
        if os.path.exists(alt):
            path = alt
    with open(path, encoding="utf-8") as f:
        return f.read()


def _extract_fonts(html: str) -> tuple[str, int]:
    """Return font-family and base font-size from the HTML template.

    The function searches for the most common ``font-size`` declaration in
    the template and uses the first ``font-family`` declaration. Defaults are
    provided if not found.
    """

    # Font family
    fam_match = re.search(r"font-family:\s*([^;]+);", html, flags=re.IGNORECASE)
    font_family = "Arial,sans-serif"
    if fam_match:
        font_family = fam_match.group(1).replace("!important", "").strip()

    # Font size – choose the most common value in the document
    sizes = re.findall(r"font-size:\s*(\d+)px", html, flags=re.IGNORECASE)
    font_size = 16
    if sizes:
        from collections import Counter

        font_size = int(Counter(sizes).most_common(1)[0][0])

    return font_family, font_size


def _rate_limit_domain(recipient: str) -> None:
    """Simple per-domain rate limiter."""

    domain = recipient.rsplit("@", 1)[-1].lower()
    now = time.monotonic()
    last = _last_domain_send.get(domain)
    if last is not None:
        elapsed = now - last
        if elapsed < _DOMAIN_RATE_LIMIT:
            delay = _DOMAIN_RATE_LIMIT - elapsed
            log_domain_rate_limit(domain, delay)
            now = last + _DOMAIN_RATE_LIMIT
    _last_domain_send[domain] = now


def _register_send(recipient: str, batch_id: str | None) -> bool:
    """Register send attempt and enforce idempotency inside a batch."""

    if not batch_id:
        return True
    key = f"{normalize_email(recipient)}|{batch_id}"
    if key in _sent_idempotency:
        return False
    _sent_idempotency.add(key)
    return True


def get_preferred_sent_folder(imap: imaplib.IMAP4_SSL) -> str:
    """Return the preferred "Sent" folder, validating it on the server."""

    if IMAP_FOLDER_FILE.exists():
        name = IMAP_FOLDER_FILE.read_text(encoding="utf-8").strip()
        if name:
            status, _ = imap.select(f'"{name}"')
            if status == "OK":
                return name
            logger.warning("Stored sent folder %s not selectable, falling back", name)
    detected = detect_sent_folder(imap)
    status, _ = imap.select(f'"{detected}"')
    if status == "OK":
        return detected
    logger.warning("Detected sent folder %s not selectable, using Sent", detected)
    return "Sent"


def send_raw_smtp_with_retry(raw_message: str, recipient: str, max_tries=3):
    last_exc: Exception | None = None
    for attempt in range(max_tries):
        _rate_limit_domain(recipient)
        try:
            host = os.getenv("SMTP_HOST", "smtp.mail.ru")
            port = int(os.getenv("SMTP_PORT", "465"))
            ssl_env = os.getenv("SMTP_SSL")  # None/"" -> авто по порту
            use_ssl = None if not ssl_env else (ssl_env == "1")
            with SmtpClient(
                host,
                port,
                EMAIL_ADDRESS,
                EMAIL_PASSWORD,
                use_ssl=use_ssl,
            ) as client:
                client.send(EMAIL_ADDRESS, recipient, raw_message)
            logger.info("Email sent", extra={"event": "send", "email": recipient})
            return
        except smtplib.SMTPResponseException as e:
            code = getattr(e, "smtp_code", None)
            msg = getattr(e, "smtp_error", b"")
            if isinstance(msg, (bytes, bytearray)):
                msg_text = msg.decode("utf-8", "ignore")
            else:
                msg_text = str(msg or "")
            try:
                write_audit(
                    "smtp_error", email=recipient, meta={"code": code, "message": msg_text}
                )
            except Exception:
                logger.debug("smtp_error audit logging failed", exc_info=True)
            add_bounce(recipient, code, msg, "send")
            if is_soft_bounce(code, msg) and attempt < max_tries - 1:
                delay = 2**attempt
                logger.info(
                    "Soft bounce for %s (%s), retrying in %s s", recipient, code, delay
                )
                time.sleep(delay)
                last_exc = e
                continue
            if is_hard_bounce(code, msg):
                suppress_add(recipient, code, "hard bounce")
            last_exc = e
            break
        except Exception as e:
            last_exc = e
            logger.warning("SMTP send failed to %s: %s", recipient, e)
            if attempt < max_tries - 1:
                time.sleep(2**attempt)
    if last_exc:
        raise last_exc


def save_to_sent_folder(
    raw_message: EmailMessage | str | bytes,
    imap: Optional[imaplib.IMAP4_SSL] = None,
    folder: Optional[str] = None,
):
    try:
        close = False
        if imap is None:
            host = os.getenv("IMAP_HOST", "imap.mail.ru")
            port = int(os.getenv("IMAP_PORT", "993"))
            imap = imaplib.IMAP4_SSL(host, port)
            imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            close = True
        if folder is None:
            folder = get_preferred_sent_folder(imap)
        status = "OK"
        if hasattr(imap, "select"):
            status, _ = imap.select(folder)
            if status != "OK":
                logger.warning("select %s failed (%s), using Sent", folder, status)
                folder = "Sent"
                imap.select(folder)

        if isinstance(raw_message, EmailMessage):
            msg_bytes = raw_message.as_bytes()
        elif isinstance(raw_message, bytes):
            msg_bytes = raw_message
        else:
            msg_bytes = raw_message.encode("utf-8")

        res = imap.append(
            folder,
            "(\\Seen)",
            imaplib.Time2Internaldate(time.time()),
            msg_bytes,
        )
        logger.info("imap.append to %s: %s", folder, res)
    except Exception as e:
        log_error(f"save_to_sent_folder: {e}")
    finally:
        if close and imap is not None:
            try:
                imap.logout()
            except Exception as e:
                log_error(f"save_to_sent_folder logout: {e}")


def build_message(
    to_addr: str,
    html_path: str,
    subject: str,
    *,
    override_180d: bool = False,
) -> tuple[EmailMessage, str]:
    html_body = _read_template_file(html_path)
    host = os.getenv("HOST", "example.com")
    font_family, base_size = _extract_fonts(html_body)
    sig_size = max(base_size - 1, 1)
    signature_html = (
        f'<div style="margin-top:20px;font-family:{font_family};'
        f'font-size:{sig_size}px;color:#222;line-height:1.4;">{SIGNATURE_TEXT}</div>'
    )
    inline_logo = os.getenv("INLINE_LOGO", "1") == "1"
    if not inline_logo:
        html_body = re.sub(r"<img[^>]+cid:logo[^>]*>", "", html_body, flags=re.IGNORECASE)
    signature_placeholder_present = _has_placeholder(html_body, "SIGNATURE")
    html_body = _render_placeholders(html_body, {"SIGNATURE": signature_html})
    if _has_unresolved_placeholders(html_body):
        raise ValueError("Unresolved placeholders in template")
    token = secrets.token_urlsafe(16)
    link = f"https://{host}/unsubscribe?email={to_addr}&token={token}"
    unsub_html = (
        f'<div style="margin-top:8px"><a href="{link}" '
        'style="display:inline-block;padding:6px 12px;font-size:12px;background:#eee;' \
        'color:#333;text-decoration:none;border-radius:4px">Отписаться</a></div>'
    )
    if not signature_placeholder_present:
        html_body = html_body.replace("</body>", f"{signature_html}</body>")
    html_body = html_body.replace("</body>", f"{unsub_html}</body>")
    text_body = strip_html(html_body) + f"\n\nОтписаться: {link}"
    msg = EmailMessage()
    default_from_name = os.getenv(
        "EMAIL_FROM_NAME", "Редакция литературы по медицине, спорту и туризму"
    )
    msg["From"] = formataddr((default_from_name or "", EMAIL_ADDRESS))
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Reply-To"] = EMAIL_ADDRESS
    msg["List-Unsubscribe"] = (
        f"<mailto:{EMAIL_ADDRESS}?subject=unsubscribe>, <{link}>"
    )
    msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")
    if override_180d:
        # Явный, осознанный обход кулдауна (для ручного режима "всем")
        msg["X-EBOT-Override-180d"] = "1"
    logo_path = SCRIPT_DIR / "Logo.png"
    if inline_logo and logo_path.exists():
        try:
            with logo_path.open("rb") as img:
                img_bytes = img.read()
            msg.get_payload()[-1].add_related(
                img_bytes, maintype="image", subtype="png", cid="<logo>"
            )
        except Exception as e:
            log_error(f"attach_logo: {e}")
    _normalize_from_header(msg)
    return msg, token


def send_email(
    recipient: str,
    html_path: str,
    subject: str = DEFAULT_SUBJECT,
    notify_func=None,
    batch_id: str | None = None,
    *,
    override_180d: bool = False,
) -> SendOutcome:
    try:
        campaign = Path(html_path).stem
        now = datetime.now(timezone.utc)
        decision, reason = decide(recipient, campaign, now)
        if decision is Decision.SKIP_COOLDOWN and override_180d:
            decision = Decision.SEND_NOW
        if decision is not Decision.SEND_NOW:
            logger.info(
                "skip %s: reason=%s campaign=%s", recipient, reason, campaign
            )
            return _outcome_for_decision(decision)
        transport_recipient = _prepare_transport_email(recipient)
        msg, token = build_message(
            transport_recipient,
            html_path,
            subject,
            override_180d=override_180d,
        )
        html_part = msg.get_body("html")
        html_body = html_part.get_content() if html_part else ""
        body_for_hash = html_body.replace(token, "{token}") if token else html_body
        subject_norm = subject or ""
        if was_sent_today_same_content(recipient, subject_norm, body_for_hash):
            logger.info("Skipping duplicate content for %s within 24h", recipient)
            return SendOutcome.DUPLICATE
        if not _register_send(recipient, batch_id):
            logger.info("Skipping duplicate send to %s for batch %s", recipient, batch_id)
            return SendOutcome.COOLDOWN
        key = canonical_for_history(recipient)
        content_hash = None
        if key:
            payload = f"{key}|{subject_norm}|{body_for_hash}".encode("utf-8")
            content_hash = hashlib.sha1(payload).hexdigest()
        raw = msg.as_string()
        send_raw_smtp_with_retry(raw, transport_recipient, max_tries=3)
        save_to_sent_folder(raw)
        try:
            ledger.record_send(
                recipient,
                campaign,
                datetime.now(timezone.utc),
                message_id=msg.get("Message-ID"),
                run_id=batch_id,
            )
        except Exception:
            logger.debug("ledger record_send failed", exc_info=True)
        log_sent_email(
            recipient,
            campaign,
            status="ok",
            filename=html_path,
            unsubscribe_token=token,
            subject=subject_norm,
            content_hash=content_hash,
        )
        return SendOutcome.SENT
    except Exception as e:
        log_error(f"send_email: {recipient}: {e}")
        if notify_func:
            notify_func(f"❌ Ошибка при отправке на {recipient}: {e}")
        raise



async def async_send_email(recipient: str, html_path: str) -> SendOutcome:
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, send_email, recipient, html_path)
    except Exception as e:
        logger.exception(e)
        log_error(e)
        raise


def create_task_with_logging(
    coro,
    notify_func: Callable[[str], Awaitable[None]] | None = None,
    *,
    task_name: str | None = None,
):
    async def runner():
        try:
            await asyncio.shield(coro)
        except asyncio.CancelledError:
            logger.info(
                "Background task %s cancelled", task_name or f"task-{id(coro):x}"
            )
            raise
        except Exception as e:
            logger.exception(e)
            log_error(e)
            if notify_func:
                try:
                    await notify_func(f"❌ Ошибка: {e}")
                except Exception as inner:
                    logger.exception(inner)
                    log_error(inner)

    task = asyncio.create_task(runner())
    register_task(task_name or f"task_{next(_TASK_SEQ)}", task)
    return task


def send_email_with_sessions(
    client: SmtpClient,
    imap: imaplib.IMAP4_SSL,
    sent_folder: str,
    recipient: str,
    html_path: str,
    subject: str = DEFAULT_SUBJECT,
    batch_id: str | None = None,
    *,
    # параметры, которые уже пробрасывает manual_send.py — принимаем их,
    # чтобы не падать и в нужный момент использовать при логе
    fixed_from: str | None = None,
    group_title: str | None = None,
    group_key: str | None = None,
    override_180d: bool = False,
    append_message: bool = True,
    return_raw: bool = False,
) -> tuple[SendOutcome, str, str | None, str | None]:
    # 0) Проверка кулдауна (если не запросили явный override)
    campaign = group_key or group_title or Path(html_path).stem
    now = datetime.now(timezone.utc)
    decision, reason = decide(recipient, campaign, now)
    if decision is Decision.SKIP_COOLDOWN and override_180d:
        decision = Decision.SEND_NOW
    if decision is not Decision.SEND_NOW:
        logger.info("skip %s: reason=%s campaign=%s", recipient, reason, campaign)
        outcome = _outcome_for_decision(decision)
        return outcome, "", None, None

    msg, token = build_message(
        recipient,
        html_path,
        subject,
        override_180d=override_180d,
    )
    html_part = msg.get_body("html")
    html_body = html_part.get_content() if html_part else ""
    body_for_hash = html_body.replace(token, "{token}") if token else html_body
    subject_norm = subject or ""
    if was_sent_today_same_content(recipient, subject_norm, body_for_hash):
        logger.info("Skipping duplicate content for %s within 24h", recipient)
        return SendOutcome.DUPLICATE, "", None, None

    if not _register_send(recipient, batch_id):
        logger.info("Skipping duplicate send to %s for batch %s", recipient, batch_id)
        return SendOutcome.COOLDOWN, "", None, None

    key = canonical_for_history(recipient)
    content_hash = None
    if key:
        payload = f"{key}|{subject_norm}|{body_for_hash}".encode("utf-8")
        content_hash = hashlib.sha1(payload).hexdigest()
    if fixed_from:
        try:
            msg.replace_header("From", fixed_from)
        except KeyError:
            msg["From"] = fixed_from
    _normalize_from_header(msg)

    # 2) Отправка
    try:
        raw = msg.as_string()
        raw_bytes = msg.as_bytes()
        if DEBUG_SAVE_EML:
            try:
                out_dir = Path("var/debug_outbox")
                out_dir.mkdir(parents=True, exist_ok=True)
                (out_dir / f"{uuid.uuid4()}.eml").write_text(
                    raw, encoding="utf-8", errors="ignore"
                )
            except Exception:
                logger.debug("debug EML save failed", exc_info=True)
        try:
            client.send(msg)
        except TypeError:
            client.send(EMAIL_ADDRESS, recipient, raw)
        if append_message:
            save_to_sent_folder(raw_bytes, imap=imap, folder=sent_folder)
    except Exception as exc:
        code = getattr(exc, "smtp_code", None)
        msg_obj = getattr(exc, "smtp_error", None)

        if isinstance(exc, smtplib.SMTPRecipientsRefused) and getattr(exc, "recipients", None):
            try:
                _addr, (code, msg_obj) = next(iter(exc.recipients.items()))
            except Exception:
                pass

        if isinstance(exc, smtplib.SMTPSenderRefused):
            if code is None and len(getattr(exc, "args", ())) >= 1:
                code = exc.args[0]
            if msg_obj is None and len(getattr(exc, "args", ())) >= 2:
                msg_obj = exc.args[1]

        if isinstance(exc, smtplib.SMTPResponseException):
            code = getattr(exc, "smtp_code", code)
            msg_obj = getattr(exc, "smtp_error", msg_obj)

        if msg_obj is None and exc.args:
            msg_obj = exc.args[0]

        if isinstance(msg_obj, (bytes, bytearray)):
            try:
                msg_text = msg_obj.decode("utf-8", errors="ignore")
            except Exception:
                msg_text = repr(msg_obj)
        else:
            msg_text = msg_obj or str(exc)

        try:
            write_audit(
                "smtp_error",
                email=recipient,
                meta={
                    "code": code,
                    "message": msg_text,
                    "exc": type(exc).__name__,
                },
            )
        except Exception:
            logger.debug("smtp_error audit logging failed", exc_info=True)

        logger.error(
            "SMTP send failed for %s (code=%s, msg=%s)",
            recipient,
            code,
            msg_text,
            exc_info=True,
        )
        return SendOutcome.ERROR, "", None, None

    # 3) Зафиксировать отправку для кулдауна
    try:
        ledger.record_send(
            recipient,
            campaign,
            datetime.now(timezone.utc),
            message_id=msg.get("Message-ID"),
            run_id=batch_id,
        )
    except Exception:
        logger.debug("ledger record_send failed", exc_info=True)

    log_source = group_key or group_title or Path(html_path).stem or "session"
    log_key = log_sent_email(
        recipient,
        log_source,
        status="ok",
        filename=html_path,
        unsubscribe_token=token,
        subject=subject_norm,
        content_hash=content_hash,
    )

    if return_raw:
        return SendOutcome.SENT, token, log_key, content_hash, raw_bytes
    return SendOutcome.SENT, token, log_key, content_hash


def process_unsubscribe_requests():
    try:
        host = os.getenv("IMAP_HOST", "imap.mail.ru")
        port = _parse_int(os.getenv("IMAP_PORT"), 993)
        timeout = get_imap_timeout()
        imap = imap_connect_ssl(host, port, timeout)
        imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        imap.select("INBOX")
        result, data = imap.search(None, '(UNSEEN SUBJECT "unsubscribe")')
        for num in data[0].split() if data and data[0] else []:
            _, msg_data = imap.fetch(num, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            sender = email.utils.parseaddr(msg.get("From"))[1]
            if sender:
                handle_unsubscribe(sender, source="imap")
            imap.store(num, "+FLAGS", "\\Seen")
        imap.logout()
    except Exception as e:
        log_error(f"process_unsubscribe_requests: {e}")


def get_blocked_emails() -> Set[str]:
    ensure_blocklist_ready()
    return suppress_list.get_blocked_set()


def dedupe_blocked_file():
    ensure_blocklist_ready()
    keep = suppress_list.load_blocked_set()
    if not keep:
        suppress_list.save_blocked_set([])
        return
    suppress_list.save_blocked_set(keep)


def verify_unsubscribe_token(email_addr: str, token: str) -> bool:
    if not os.path.exists(LOG_FILE):
        return False
    email_norm = normalize_email(email_addr)
    with open(LOG_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("email") == email_norm and row.get("unsubscribe_token") == token:
                return True
    return False


def _mark_unsubscribed_block_only(email_addr: str, token: str | None = None) -> bool:
    """Записать отписавшийся адрес в блок-лист (идемпотентно).
    Ничего более не трогаем.
    """

    added = False
    try:
        addr = normalize_email(email_addr)
        if not addr or "@" not in addr:
            logger.warning("mark_unsubscribed: invalid email %r", email_addr)
        else:
            added = add_blocked_email(addr)
            if added:
                logger.info("Unsubscribe -> blocklist: %s", addr)
            # поддерживаем файл в чистом виде (без дублей)
            dedupe_blocked_file()
    except Exception as exc:  # pragma: no cover - defensive logging
        log_error(f"mark_unsubscribed: {email_addr}: {exc}")

    return added


@dataclass
class MarkUnsubscribedResult:
    csv_updated: bool
    block_added: bool

    def __bool__(self) -> bool:  # pragma: no cover - convenience for legacy callers
        return self.csv_updated or self.block_added


def mark_unsubscribed(email_addr: str, token: str | None = None) -> MarkUnsubscribedResult:
    """Mark ``email_addr`` as unsubscribed and persist the blocklist entry.

    The ``token`` argument is accepted for compatibility, but is not used here.
    """

    block_added = _mark_unsubscribed_block_only(email_addr, token)

    email_norm = normalize_email(email_addr)
    p = Path(LOG_FILE)
    rows: list[dict] = []
    changed = False
    if p.exists():
        with p.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        for row in rows:
            if row.get("email") == email_norm:
                row["unsubscribed"] = "1"
                row["unsubscribed_at"] = datetime.utcnow().isoformat()
                changed = True
    if changed:
        headers = rows[0].keys() if rows else [
            "key",
            "email",
            "last_sent_at",
            "source",
            "status",
            "user_id",
            "filename",
            "error_msg",
            "unsubscribe_token",
            "unsubscribed",
            "unsubscribed_at",
        ]
        with p.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(headers))
            writer.writeheader()
            writer.writerows(rows)
    return MarkUnsubscribedResult(csv_updated=changed, block_added=block_added)


def handle_unsubscribe(email: str, source: str | None = None) -> MarkUnsubscribedResult:
    result = mark_unsubscribed(email)
    try:
        added = add_to_blocklist(email)
        logger.info("Unsubscribe -> blocklist: %s (added=%s, source=%s)", email, added, source)
        if added or result.block_added:
            return MarkUnsubscribedResult(csv_updated=result.csv_updated, block_added=added or result.block_added)
        return result
    except Exception as exc:
        logger.exception("Failed to append to blocked_emails.txt for %s: %s", email, exc)
        return result


def log_sent_email(
    email_addr,
    group,
    status="ok",
    user_id=None,
    filename=None,
    error_msg=None,
    unsubscribe_token="",
    unsubscribed="",
    unsubscribed_at="",
    *,
    key: str | None = None,
    ts: datetime | None = None,
    subject: str | None = None,
    content_hash: str | None = None,
) -> str | None:
    if status not in {"ok", "sent", "success"}:
        return None

    tz = ZoneInfo(REPORT_TZ)
    ts_local = ts.astimezone(tz) if ts and ts.tzinfo else (ts.replace(tzinfo=tz) if ts else datetime.now(tz))
    extra = {
        "user_id": user_id or "",
        "filename": filename or "",
        "error_msg": error_msg or "",
        "unsubscribe_token": unsubscribe_token,
        "unsubscribed": unsubscribed,
        "unsubscribed_at": unsubscribed_at,
    }
    if subject is not None:
        extra["subject"] = subject
    if content_hash is not None:
        extra["content_hash"] = content_hash
    normalized = normalize_email(email_addr)
    used_key = key or str(uuid.uuid4())
    _, _ = upsert_sent_log(
        LOG_FILE,
        normalized,
        ts_local,
        group,
        status=status,
        extra=extra,
        key=used_key,
    )
    global _log_cache
    _log_cache = None
    return used_key


def _parse_list_line(line: bytes):
    s = line.decode(errors="ignore")
    m = re.match(r'^\((?P<flags>[^)]*)\)\s+"(?P<delim>[^"]*)"\s+"?(?P<name>.+?)"?$', s)
    if not m:
        return None, ""
    return m.group("name"), m.group("flags")


def detect_sent_folder(imap: imaplib.IMAP4_SSL) -> str:
    status, data = imap.list()
    if status != "OK" or not data:
        return "Sent"
    candidates = []
    for line in data:
        name, flags = _parse_list_line(line)
        if not name:
            continue
        if "\\Sent" in flags or "\\sent" in flags:
            candidates.append(name)
    if candidates:
        return candidates[0]
    return "Sent"


_log_cache: dict[str, List[datetime]] | None = None


def _load_sent_log() -> Dict[str, datetime]:
    global _log_cache
    if _log_cache is not None:
        return _log_cache
    p = Path(LOG_FILE)
    cache = load_sent_log(p)
    _log_cache = cache
    return cache


def was_sent_within(email: str, days: int = 180) -> bool:
    """Return True if ``email`` was sent to within ``days`` days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cache = _load_sent_log()
    key = canonical_for_history(email)
    dt = cache.get(key)
    if dt and dt >= cutoff:
        return True
    recent = get_recently_contacted_emails_cached()
    return key in recent


def was_emailed_recently(email: str, days: int = 180) -> bool:  # pragma: no cover
    """[DEPRECATED] Используйте :func:`emailbot.messaging_utils.was_sent_within`.

    Оставлено в качестве совместимой прослойки для старых импортов. Новая
    реализация располагается в :mod:`emailbot.messaging_utils`.
    """

    from .messaging_utils import was_sent_within

    try:
        return was_sent_within(email, days=days)
    except Exception:
        return False


def get_recent_6m_union() -> Set[str]:
    tz = ZoneInfo(REPORT_TZ)
    cutoff = datetime.now(tz) - timedelta(days=180)
    result: Set[str] = set()
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts_raw = (row.get("last_sent_at") or "").strip()
                if not ts_raw:
                    continue
                try:
                    dt = datetime.fromisoformat(ts_raw)
                except Exception:
                    continue
                if dt.tzinfo is None:
                    dt_local = dt.replace(tzinfo=tz)
                else:
                    dt_local = dt.astimezone(tz)
                if dt_local < cutoff:
                    continue
                key = (row.get("key") or "").strip()
                if not key:
                    key = canonical_for_history(row.get("email", ""))
                if key:
                    result.add(key)
    return result


_recent_cache = {"at": None, "set": set(), "ttl": 600}


def get_recently_contacted_emails_cached() -> Set[str]:
    now = time.time()
    at = _recent_cache["at"]
    if at is None or (now - at) > _recent_cache["ttl"]:
        _recent_cache["set"] = get_recent_6m_union()
        _recent_cache["at"] = now
    return _recent_cache["set"]


def clear_recent_sent_cache():
    _recent_cache["at"] = None
    _recent_cache["set"] = set()


def get_sent_today() -> Set[str]:
    if not os.path.exists(LOG_FILE):
        return set()
    tz = ZoneInfo(REPORT_TZ)
    now_local = datetime.now(tz)
    day_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    sent: Set[str] = set()
    with open(LOG_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = (row.get("status") or "ok").strip().lower()
            if status not in {"ok", "sent", "success"}:
                continue
            ts_raw = (row.get("last_sent_at") or "").strip()
            if not ts_raw:
                continue
            try:
                dt = datetime.fromisoformat(ts_raw)
            except Exception:
                continue
            if dt.tzinfo is None:
                dt_local = dt.replace(tzinfo=tz)
            else:
                dt_local = dt.astimezone(tz)
            if not (day_start <= dt_local < day_end):
                continue
            key = (row.get("key") or "").strip()
            if not key:
                key = canonical_for_history(row.get("email", ""))
            if key:
                sent.add(key)
    return sent


def count_sent_today() -> int:
    return len(get_sent_today())


def prepare_mass_mailing(
    emails: list[str],
    group: str | None = None,
    chat_id: int | None = None,
    *,
    ignore_cooldown: bool = False,
) -> tuple[list[str], list[str], list[str], list[str], dict[str, object]]:
    """Filter ``emails`` for manual/preview sends.

    The function never raises exceptions to the caller. Instead it returns empty
    results along with a digest containing an ``"error"`` key when something goes
    wrong. The caller is expected to present a friendly message to the user.
    """

    _reset_history_shim_warning()

    try:
        batch = sanitize_batch(emails)
        cleaned = batch.emails
        dup_skipped = batch.duplicates
        norm_map = batch.normalized
        if not cleaned:
            return [], [], [], [], {"total": 0, "input_total": 0}

        blocked_invalid: list[str] = []
        blocked_foreign: list[str] = []
        skipped_recent: list[str] = []

        invalid_basic = [addr for addr in cleaned if not _validate_email_basic(addr)]
        candidates = [addr for addr in cleaned if addr not in invalid_basic]

        blocked_set: set[str] = set()
        try:
            blocked_set = {normalize_email(e) for e in get_blocked_emails()}
        except Exception as exc:
            logger.warning("blocked list load failed: %s", exc)

        queue_after_block: list[str] = []
        for addr in candidates:
            norm = norm_map.get(addr) or normalize_email(addr)
            if blocked_set and norm in blocked_set:
                blocked_invalid.append(addr)
                continue
            if rules.is_blocked(norm):
                blocked_invalid.append(addr)
                continue
            try:
                if is_suppressed(addr):
                    blocked_invalid.append(addr)
                    continue
            except Exception as exc:
                logger.warning("suppression check failed for %s: %s", addr, exc)
            queue_after_block.append(addr)

        queue_after_foreign: list[str] = []
        block_foreign_enabled = os.getenv("FOREIGN_BLOCK", "1") == "1"
        for addr in queue_after_block:
            try:
                if block_foreign_enabled and is_foreign(addr):
                    blocked_foreign.append(addr)
                    continue
            except Exception as exc:
                logger.warning("foreign check failed for %s: %s", addr, exc)
            queue_after_foreign.append(addr)

        raw_lookback = os.getenv("HALF_YEAR_DAYS", os.getenv("EMAIL_LOOKBACK_DAYS", "180"))
        try:
            lookback_days = int(raw_lookback)
        except (TypeError, ValueError):
            lookback_days = 180
        if lookback_days < 0:
            lookback_days = 0
        if ignore_cooldown:
            lookback_days = 0
        recent = _load_recent_sent(lookback_days)
        now_utc = datetime.now(timezone.utc)
        cooldown_result = (
            audit_emails(queue_after_foreign, days=lookback_days, now=now_utc)
            if lookback_days > 0
            else None
        )
        cooldown_under = (
            set(cooldown_result.get("under", set())) if cooldown_result else set()
        )

        ready: list[str] = []
        for addr in queue_after_foreign:
            blocked_recent = False
            norm = cooldown_normalize_email(addr) if lookback_days > 0 else addr
            norm_key = norm or addr
            if lookback_days > 0 and norm_key in cooldown_under:
                skipped_recent.append(addr)
                blocked_recent = True
            if not blocked_recent:
                try:
                    if lookback_days > 0 and was_sent_within(addr, days=lookback_days):
                        skipped_recent.append(addr)
                        blocked_recent = True
                    elif (
                        lookback_days > 0
                        and history_service.was_sent_within_days(
                            addr, group or "", lookback_days
                        )
                    ):
                        skipped_recent.append(addr)
                        blocked_recent = True
                except Exception as exc:
                    logger.warning("history lookup failed for %s: %s", addr, exc)
            if blocked_recent:
                continue
            canon = norm_key if lookback_days > 0 else addr
            if lookback_days > 0 and canon in recent:
                skipped_recent.append(addr)
                continue
            if lookback_days > 0 and _seen_in_local_history(canon, lookback_days):
                skipped_recent.append(addr)
                continue
            if lookback_days > 0 and rules.seen_within_window(canon):
                skipped_recent.append(addr)
                continue
            ready.append(addr)

        combined_invalid: list[str] = []
        seen_invalid: set[str] = set()
        for addr in blocked_invalid + invalid_basic:
            if addr in seen_invalid:
                continue
            seen_invalid.add(addr)
            combined_invalid.append(addr)
        blocked_invalid = combined_invalid

        digest = {
            "total": len(cleaned),
            "ready": len(ready),
            "invalid": len(invalid_basic),
            "blocked_foreign": len(blocked_foreign),
            "blocked_invalid": len(blocked_invalid),
            "skipped_recent": len(skipped_recent),
            "input_total": len(cleaned),
            "after_suppress": len(queue_after_block),
            "foreign_blocked": len(blocked_foreign),
            "ready_after_cooldown": max(
                len(queue_after_foreign) - len(skipped_recent), 0
            ),
            "ready_final": len(ready),
            "set_planned": len(ready),
            "sent_planned": len(ready),
            "removed_duplicates_in_batch": dup_skipped,
            "unique_ready_to_send": len(ready),
            "skipped_suppress": len(blocked_invalid),
            "skipped_180d": len(skipped_recent),
            "skipped_foreign": len(blocked_foreign),
            "removed_recent_180d": len(skipped_recent),
            "removed_invalid": len(blocked_invalid),
            "removed_foreign": len(blocked_foreign),
            "removed_today": 0,
        }
        return ready, blocked_foreign, blocked_invalid, skipped_recent, digest
    except Exception as exc:
        logger.exception("prepare_mass_mailing hard-fail: %s", exc)
        return [], [], [], [], {
            "total": 0,
            "input_total": 0,
            "error": str(exc),
        }


def sync_log_with_imap(
    since_dt: Optional[datetime] = None,
    *,
    chat_id: Optional[int] = None,
) -> Dict[str, int]:
    imap = None
    stats = {
        "new_contacts": 0,
        "updated_contacts": 0,
        "skipped_events": 0,
        "total_rows_after": 0,
        "cancelled": False,
    }
    ensure_sent_log_schema(LOG_FILE)
    try:
        host = os.getenv("IMAP_HOST", "imap.mail.ru")
        port = _parse_int(os.getenv("IMAP_PORT"), 993)
        timeout = get_imap_timeout()
        imap = imap_connect_ssl(host, port, timeout)
        imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        sent_folder = get_preferred_sent_folder(imap)
        status, _ = imap.select(f'"{sent_folder}"')
        if status != "OK":
            logger.warning("select %s failed (%s), using Sent", sent_folder, status)
            sent_folder = "Sent"
            imap.select(f'"{sent_folder}"')
        now = datetime.utcnow()
        cutoff = now - timedelta(days=max(EMAIL_LOOKBACK_DAYS, 0))
        if since_dt:
            if since_dt.tzinfo:
                since_dt = since_dt.replace(tzinfo=None)
            if since_dt > cutoff:
                cutoff = since_dt
        date_cutoff = cutoff.strftime("%d-%b-%Y")
        result, data = imap.search(None, f"SINCE {date_cutoff}")
        seen_events = load_seen_events(SYNC_SEEN_EVENTS_PATH)
        changed_events = False
        message_ids = data[0].split() if data and data[0] else []
        cancelled = False
        for num in message_ids:
            if chat_id is not None and is_cancelled(chat_id):
                cancelled = True
                break
            _, msg_data = imap.fetch(num, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            msgid = msg.get("Message-ID", "").strip()
            addresses = []
            for hdr in ["To", "Cc", "Bcc"]:
                addresses.extend(email.utils.getaddresses([msg.get(hdr) or ""]))
            for _, addr in addresses:
                if not addr:
                    continue
                key = canonical_for_history(addr)
                if msgid and (msgid, key) in seen_events:
                    stats["skipped_events"] += 1
                    continue
                try:
                    dt = email.utils.parsedate_to_datetime(msg.get("Date"))
                    if dt and dt.tzinfo:
                        dt = dt.replace(tzinfo=None)
                    if dt and dt < cutoff:
                        continue
                except Exception:
                    dt = None
                unique_marker = msgid or f"uid:{num.decode() if isinstance(num, bytes) else num}"
                event_key = f"imap:{unique_marker}:{key}"
                inserted, updated = upsert_sent_log(
                    LOG_FILE,
                    normalize_email(addr),
                    dt or datetime.utcnow(),
                    "imap_sync",
                    status="external",
                    key=event_key,
                )
                if inserted:
                    stats["new_contacts"] += 1
                elif updated:
                    stats["updated_contacts"] += 1
                if msgid:
                    seen_events.add((msgid, key))
                    changed_events = True
        if cancelled:
            stats["cancelled"] = True
            return stats
        if changed_events:
            save_seen_events(SYNC_SEEN_EVENTS_PATH, seen_events)
        stats["total_rows_after"] = len(load_sent_log(Path(LOG_FILE)))
        added_count = int(stats.get("new_contacts", 0)) + int(
            stats.get("updated_contacts", 0)
        )
        logger.info(
            "imap_sent_sync",
            extra={
                "folder": sent_folder,
                "added": added_count,
                "lookback_days": EMAIL_LOOKBACK_DAYS,
            },
        )
        return stats
    except Exception as e:
        log_error(f"sync_log_with_imap: {e}")
        raise
    finally:
        if imap is not None:
            try:
                imap.logout()
            except Exception as e:
                log_error(f"sync_log_with_imap logout: {e}")


def maybe_sync_before_send(
    logger: Optional[logging.Logger] = None,
    *,
    chat_id: Optional[int] = None,
) -> tuple[bool, int, int]:
    """Perform IMAP sync if the cached state is stale."""

    now = datetime.now()
    need_sync, last_dt = _need_sync_now(now)
    if not need_sync:
        if logger:
            logger.info(
                "IMAP sync skipped (cached < %dh)",
                EMAIL_SYNC_INTERVAL_HOURS,
            )
        return False, 0, EMAIL_LOOKBACK_DAYS

    stats = sync_log_with_imap(since_dt=last_dt, chat_id=chat_id)
    if stats.get("cancelled"):
        if logger:
            logger.info("IMAP sync cancelled")
        return False, 0, EMAIL_LOOKBACK_DAYS

    added = int(stats.get("new_contacts", 0)) + int(stats.get("updated_contacts", 0))
    _mark_synced(now)
    if logger:
        window_label = (
            f"since {last_dt.isoformat()}" if last_dt else f"last {EMAIL_LOOKBACK_DAYS}d"
        )
        logger.info("IMAP sync done: +%d, window=%s", added, window_label)
    return True, added, EMAIL_LOOKBACK_DAYS


def periodic_unsubscribe_check(stop_event):
    while not stop_event.is_set():
        try:
            process_unsubscribe_requests()
        except Exception as e:
            log_error(f"periodic_unsubscribe_check: {e}")
        time.sleep(300)


def check_env_vars():
    for var in ["TELEGRAM_BOT_TOKEN", "EMAIL_ADDRESS", "EMAIL_PASSWORD"]:
        if not os.getenv(var):
            raise EnvironmentError(f"Переменная окружения {var} не задана!")


__all__ = [
    "SendOutcome",
    "DOWNLOAD_DIR",
    "LOG_FILE",
    "BLOCKED_FILE",
    "MAX_EMAILS_PER_DAY",
    "TEMPLATE_MAP",
    "SIGNATURE_HTML",
    "TemplateRenderError",
    "build_email_body",
    "build_signature_text",
    "EMAIL_ADDRESS",
    "EMAIL_PASSWORD",
    "IMAP_FOLDER_FILE",
    "send_raw_smtp_with_retry",
    "save_to_sent_folder",
    "get_preferred_sent_folder",
    "build_message",
    "send_email",
    "async_send_email",
    "create_task_with_logging",
    "send_email_with_sessions",
    "send_bulk",
    "process_unsubscribe_requests",
    "ensure_blocklist_ready",
    "log_domain_rate_limit",
    "get_blocked_emails",
    "add_blocked_email",
    "dedupe_blocked_file",
    "verify_unsubscribe_token",
    "write_audit",
    "MarkUnsubscribedResult",
    "mark_unsubscribed",
    "handle_unsubscribe",
    "log_sent_email",
    "detect_sent_folder",
    "get_recent_6m_union",
    "get_recently_contacted_emails_cached",
    "clear_recent_sent_cache",
    "get_sent_today",
    "count_sent_today",
    "parse_emails_from_text",
    "prepare_mass_mailing",
    "sync_log_with_imap",
    "maybe_sync_before_send",
    "periodic_unsubscribe_check",
    "check_env_vars",
    "was_sent_within",
    "was_emailed_recently",
    "was_sent_recently",
    "mark_sent",
    "_make_send_key",
    "_is_blocklisted",
]
def _prepare_transport_email(raw: str) -> str:
    """Return an SMTP-safe address preserving the original local-part."""

    cleaned = (raw or "").strip()
    cleaned = strip_invisibles(cleaned)
    cleaned = cleaned.strip("()[]{}<>,;\"'`«»„“”‚‘’ ")
    if "@" in cleaned:
        local, _, domain = cleaned.partition("@")
        return f"{local}@{(domain or '').strip().lower()}"
    return cleaned.lower()
