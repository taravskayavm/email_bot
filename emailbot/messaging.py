"""Message building and sending utilities."""

from __future__ import annotations

import asyncio
import csv
import email
import hashlib
import html
import imaplib
import logging
import os
import re
import time
import secrets
import smtplib
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from enum import Enum
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Set

from .extraction import normalize_email, strip_html
from .smtp_client import SmtpClient
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

logger = logging.getLogger(__name__)

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
from emailbot.services.cooldown import should_skip_by_cooldown, mark_sent as cooldown_mark_sent
SCRIPT_DIR = Path(__file__).resolve().parent.parent
DOWNLOAD_DIR = str(SCRIPT_DIR / "downloads")
# Был жёсткий путь /mnt/data/sent_log.csv → падало на Windows/Linux без /mnt.
LOG_FILE = str(expand_path(os.getenv("SENT_LOG_PATH", "var/sent_log.csv")))
BLOCKED_FILE = str(SCRIPT_DIR / "blocked_emails.txt")
MAX_EMAILS_PER_DAY = int(os.getenv("MAX_EMAILS_PER_DAY", "300"))

suppress_list.init_blocked(BLOCKED_FILE)

# HTML templates are stored at the root-level ``templates`` directory.
TEMPLATES_DIR = str(SCRIPT_DIR / "templates")
TEMPLATE_MAP = {
    "sport": os.path.join(TEMPLATES_DIR, "sport.html"),
    "tourism": os.path.join(TEMPLATES_DIR, "tourism.html"),
    "medicine": os.path.join(TEMPLATES_DIR, "medicine.html"),
    "bioinformatics": os.path.join(TEMPLATES_DIR, "bioinformatics.html"),
    "geography": os.path.join(TEMPLATES_DIR, "geography.html"),
    "psychology": os.path.join(TEMPLATES_DIR, "psychology.html"),
    "beauty": os.path.join(TEMPLATES_DIR, "beauty.html"),
}


DEFAULT_SUBJECT = "Издательство Лань приглашает к сотрудничеству"


class SendOutcome(Enum):
    SENT = "sent"
    COOLDOWN = "cooldown"
    BLOCKED = "blocked"
    ERROR = "error"
    DUPLICATE = "duplicate"

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


def _validate_email_basic(email_value: str) -> bool:
    return bool(EMAIL_RE.fullmatch(email_value))


def _sanitize_batch(emails: Iterable[str]) -> tuple[list[str], int]:
    sanitized: list[str] = []
    seen: set[str] = set()
    dup_skipped = 0
    for raw in emails or []:
        em = (str(raw) or "").strip().strip(",;").lower()
        if not em:
            continue
        if em in seen:
            dup_skipped += 1
            continue
        seen.add(em)
        sanitized.append(em)
    return sanitized, dup_skipped


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
            time.sleep(_DOMAIN_RATE_LIMIT - elapsed)
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
            imap = imaplib.IMAP4_SSL("imap.mail.ru")
            imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            close = True
        if folder is None:
            folder = get_preferred_sent_folder(imap)
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
            "\\Seen",
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
    msg["From"] = formataddr(
        ("Редакция литературы по медицине, спорту и туризму", EMAIL_ADDRESS)
    )
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
        if not override_180d:
            try:
                skip, reason = should_skip_by_cooldown(recipient)
                if skip:
                    logger.info("Cooldown skip for %s: %s", recipient, reason)
                    return SendOutcome.COOLDOWN
            except Exception:
                logger.exception("cooldown check failed")
                return SendOutcome.ERROR
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
        send_raw_smtp_with_retry(raw, recipient, max_tries=3)
        save_to_sent_folder(raw)
        try:
            cooldown_mark_sent(recipient)
        except Exception:
            logger.debug("cooldown mark_sent failed (non-fatal)", exc_info=True)
        try:
            history_service.mark_sent(
                recipient,
                Path(html_path).stem,
                msg.get("Message-ID"),
                datetime.now(timezone.utc),
                smtp_result="ok",
            )
        except Exception:
            logger.debug("history mark_sent failed (non-fatal)", exc_info=True)
        log_sent_email(
            recipient,
            Path(html_path).stem,
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
):
    async def runner():
        try:
            await coro
        except Exception as e:
            logger.exception(e)
            log_error(e)
            if notify_func:
                try:
                    await notify_func(f"❌ Ошибка: {e}")
                except Exception as inner:
                    logger.exception(inner)
                    log_error(inner)

    return asyncio.create_task(runner())


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
) -> tuple[SendOutcome, str, str | None, str | None]:
    # 0) Проверка кулдауна (если не запросили явный override)
    if not override_180d:
        try:
            skip, reason = should_skip_by_cooldown(recipient)
            if skip:
                logger.info("Cooldown skip for %s: %s", recipient, reason)
                return SendOutcome.COOLDOWN, "", None, None
        except Exception:
            # Не роняем поток из-за побочного сервиса — fail-open запрещён,
            # поэтому при ошибке проверку считаем «нет допуска»
            logger.exception("cooldown check failed")
            return SendOutcome.ERROR, "", None, None

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

    # 2) Отправка
    try:
        raw = msg.as_string()
        client.send(EMAIL_ADDRESS, recipient, raw)
        save_to_sent_folder(raw, imap=imap, folder=sent_folder)
    except Exception:
        logger.exception("SMTP send failed for %s", recipient)
        return SendOutcome.ERROR, "", None, None

    # 3) Зафиксировать отправку для кулдауна
    try:
        cooldown_mark_sent(recipient)
    except Exception:
        logger.debug("cooldown mark_sent failed (non-fatal)", exc_info=True)
    try:
        history_service.mark_sent(
            recipient,
            group_key or group_title or Path(html_path).stem,
            msg.get("Message-ID"),
            datetime.now(timezone.utc),
            smtp_result="ok",
        )
    except Exception:
        logger.debug("history mark_sent failed (non-fatal)", exc_info=True)

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

    return SendOutcome.SENT, token, log_key, content_hash


def process_unsubscribe_requests():
    try:
        imap = imaplib.IMAP4_SSL("imap.mail.ru")
        imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        imap.select("INBOX")
        result, data = imap.search(None, '(UNSEEN SUBJECT "unsubscribe")')
        for num in data[0].split() if data and data[0] else []:
            _, msg_data = imap.fetch(num, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            sender = email.utils.parseaddr(msg.get("From"))[1]
            if sender:
                mark_unsubscribed(sender)
            imap.store(num, "+FLAGS", "\\Seen")
        imap.logout()
    except Exception as e:
        log_error(f"process_unsubscribe_requests: {e}")


def _canonical_blocked(email_str: str) -> str:
    e = normalize_email(email_str)
    e = re.sub(r"^\.+", "", e)
    e = re.sub(r"^\d{1,2}(?=[A-Za-z])", "", e)
    return e


def get_blocked_emails() -> Set[str]:
    return suppress_list.get_blocked_set()


def add_blocked_email(email_str: str) -> bool:
    email_norm = _canonical_blocked(email_str)
    if not email_norm or "@" not in email_norm:
        return False
    existing = get_blocked_emails()
    if email_norm in existing:
        return False
    with open(BLOCKED_FILE, "a", encoding="utf-8") as f:
        f.write(email_norm + "\n")
    suppress_list.refresh_if_changed()
    return True


def dedupe_blocked_file():
    if not os.path.exists(BLOCKED_FILE):
        return
    with open(BLOCKED_FILE, "r", encoding="utf-8") as f:
        keep = {_canonical_blocked(line) for line in f if "@" in line}
    with open(BLOCKED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(keep)) + "\n")
    suppress_list.refresh_if_changed()


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


def mark_unsubscribed(email_addr: str, token: str | None = None) -> bool:
    email_norm = normalize_email(email_addr)
    p = Path(LOG_FILE)
    rows: list[dict] = []
    changed = False
    if p.exists():
        with p.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        for row in rows:
            if row.get("email") == email_norm and (token is None or row.get("unsubscribe_token") == token):
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
    add_blocked_email(email_norm)
    return changed


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
) -> tuple[list[str], list[str], list[str], list[str], dict[str, object]]:
    """Filter ``emails`` for manual/preview sends.

    The function never raises exceptions to the caller. Instead it returns empty
    results along with a digest containing an ``"error"`` key when something goes
    wrong. The caller is expected to present a friendly message to the user.
    """

    try:
        normalized, dup_skipped = _sanitize_batch(emails)
        if not normalized:
            return [], [], [], [], {"total": 0, "input_total": 0}

        blocked_invalid: list[str] = []
        blocked_foreign: list[str] = []
        skipped_recent: list[str] = []

        invalid_basic = [addr for addr in normalized if not _validate_email_basic(addr)]
        candidates = [addr for addr in normalized if addr not in invalid_basic]

        blocked_set: set[str] = set()
        try:
            blocked_set = {normalize_email(e) for e in get_blocked_emails()}
        except Exception as exc:
            logger.warning("blocked list load failed: %s", exc)

        queue_after_block: list[str] = []
        for addr in candidates:
            norm = normalize_email(addr)
            if blocked_set and norm in blocked_set:
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
        recent = _load_recent_sent(lookback_days)

        ready: list[str] = []
        for addr in queue_after_foreign:
            blocked_recent = False
            try:
                if lookback_days > 0 and was_sent_within(addr, days=lookback_days):
                    skipped_recent.append(addr)
                    blocked_recent = True
            except Exception as exc:
                logger.warning("history lookup failed for %s: %s", addr, exc)
            if blocked_recent:
                continue
            if lookback_days > 0:
                try:
                    canon = normalize_email(addr)
                except Exception:
                    canon = addr
            else:
                canon = addr
            if lookback_days > 0 and canon in recent:
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
            "total": len(normalized),
            "ready": len(ready),
            "invalid": len(invalid_basic),
            "blocked_foreign": len(blocked_foreign),
            "blocked_invalid": len(blocked_invalid),
            "skipped_recent": len(skipped_recent),
            "input_total": len(normalized),
            "after_suppress": len(queue_after_block),
            "foreign_blocked": len(blocked_foreign),
            "after_180d": len(queue_after_foreign),
            "sent_planned": len(ready),
            "skipped_by_dup_in_batch": dup_skipped,
            "unique_ready_to_send": len(ready),
            "skipped_suppress": len(blocked_invalid),
            "skipped_180d": len(skipped_recent),
            "skipped_foreign": len(blocked_foreign),
        }
        return ready, blocked_foreign, blocked_invalid, skipped_recent, digest
    except Exception as exc:
        logger.exception("prepare_mass_mailing hard-fail: %s", exc)
        return [], [], [], [], {
            "total": 0,
            "input_total": 0,
            "error": str(exc),
        }


def sync_log_with_imap() -> Dict[str, int]:
    imap = None
    stats = {
        "new_contacts": 0,
        "updated_contacts": 0,
        "skipped_events": 0,
        "total_rows_after": 0,
    }
    ensure_sent_log_schema(LOG_FILE)
    try:
        imap = imaplib.IMAP4_SSL("imap.mail.ru")
        imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        sent_folder = get_preferred_sent_folder(imap)
        status, _ = imap.select(f'"{sent_folder}"')
        if status != "OK":
            logger.warning("select %s failed (%s), using Sent", sent_folder, status)
            sent_folder = "Sent"
            imap.select(f'"{sent_folder}"')
        date_180 = (datetime.utcnow() - timedelta(days=180)).strftime("%d-%b-%Y")
        result, data = imap.search(None, f"SINCE {date_180}")
        seen_events = load_seen_events(SYNC_SEEN_EVENTS_PATH)
        changed_events = False
        for num in data[0].split() if data and data[0] else []:
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
                    if dt and dt < datetime.utcnow() - timedelta(days=180):
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
        if changed_events:
            save_seen_events(SYNC_SEEN_EVENTS_PATH, seen_events)
        stats["total_rows_after"] = len(load_sent_log(Path(LOG_FILE)))
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
    "process_unsubscribe_requests",
    "get_blocked_emails",
    "add_blocked_email",
    "dedupe_blocked_file",
    "verify_unsubscribe_token",
    "mark_unsubscribed",
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
    "periodic_unsubscribe_check",
    "check_env_vars",
    "was_sent_within",
    "was_emailed_recently",
]
