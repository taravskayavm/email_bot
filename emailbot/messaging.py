"""Message building and sending utilities."""

from __future__ import annotations

import asyncio
import csv
import email
import uuid
import email.utils as eut
import hashlib
import imaplib
import json
import logging
import os
import re
import secrets
import smtplib
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional, Set

from email import message_from_bytes, message_from_string, policy

from services.templates import get_template, get_template_by_path

from . import history_service
from .edit_service import apply_edits as apply_saved_edits

from .extraction import normalize_email, strip_html
from .messaging_utils import (
    SYNC_SEEN_EVENTS_PATH,
    add_bounce,
    append_to_sent,
    canonical_for_history,
    detect_sent_folder,
    ensure_sent_log_schema,
    is_foreign,
    is_hard_bounce,
    is_soft_bounce,
    is_suppressed,
    load_seen_events,
    load_sent_log,
    save_seen_events,
    suppress_add,
    upsert_sent_log,
)
from .utils import log_error as log_internal_error
from utils.send_stats import log_error, log_success
from utils.smtp_client import RobustSMTP, send_with_retry

logger = logging.getLogger(__name__)

# Resolve the project root (one level above this file) and use shared
# directories located at the repository root.
SCRIPT_DIR = Path(__file__).resolve().parent.parent
DOWNLOAD_DIR = str(SCRIPT_DIR / "downloads")
LOG_FILE = str(Path("/mnt/data") / "sent_log.csv")
BLOCKED_FILE = str(SCRIPT_DIR / "blocked_emails.txt")
MAX_EMAILS_PER_DAY = 200

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
EMAIL_ADDRESS = ""
EMAIL_PASSWORD = ""
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.mail.ru")

IMAP_FOLDER_FILE = SCRIPT_DIR / "imap_sent_folder.txt"

_last_domain_send: Dict[str, float] = {}
_DOMAIN_RATE_LIMIT = 1.0  # seconds between sends per domain
_batch_idempotency: Set[str] = set()

# Persistent idempotency storage (24h)
MODULE_DIR = Path(__file__).resolve().parent
SENT_IDS_FILE = MODULE_DIR / "sent_ids.jsonl"
_sent_idempotency: Set[str] = set()


def _smtp_reason(exc: Exception) -> str:
    reason = getattr(exc, "smtp_error", None)
    if isinstance(reason, (bytes, bytearray)):
        try:
            return reason.decode()
        except Exception:
            return repr(reason)
    if reason:
        return str(reason)
    return str(exc)


def _raw_to_text(raw_message: str | bytes) -> str:
    if isinstance(raw_message, (bytes, bytearray)):
        try:
            return raw_message.decode()
        except Exception:
            return raw_message.decode("utf-8", errors="ignore")
    return str(raw_message)



def _normalize_template_code(code: str) -> str:
    return (code or "").strip().lower()


def _new_signature_codes() -> set[str]:
    raw = os.getenv(
        "TEMPLATES_NEW_SIGNATURE",
        "bioinformatics,geography,psychology",
    )
    return {
        part.strip().lower()
        for part in raw.split(",")
        if part.strip()
    }


def _resolve_signature_mode(code: str) -> str:
    info = get_template(code)
    if info:
        mode = info.get("signature")
        if isinstance(mode, str):
            mode_norm = mode.strip().lower()
            if mode_norm in {"new", "old"}:
                return mode_norm
    return "new" if _normalize_template_code(code) in _new_signature_codes() else "old"


def _choose_from_header(group: str) -> str:
    """
    Имя отправителя в заголовке From — зависит от направления.
    Старые: 'Редакция литературы по медицине, спорту и туризму'
    Новые:  'Редакция литературы'
    """
    mode = _resolve_signature_mode(group)
    if mode == "new":
        return "Редакция литературы"
    return "Редакция литературы по медицине, спорту и туризму"


def _apply_from(msg: EmailMessage, group: str) -> None:
    """Гарантированно проставить корректный From по группе (без точки на конце)."""
    from_addr = os.getenv("EMAIL_ADDRESS", "")
    from_name = _choose_from_header(group).strip()
    # убрать точку/пробелы на конце, если вдруг есть
    while from_name.endswith((".", " ", " ")):  # пробел и NBSP
        from_name = from_name[:-1]
    if "From" in msg:
        try:
            del msg["From"]
        except Exception:
            pass
    msg["From"] = formataddr((from_name, from_addr))

_SIGNATURE_OLD = """--
С уважением,
Таравская Владлена Михайловна
Заведующая редакцией литературы по медицине, спорту и туризму
ООО Издательство «ЛАНЬ»

8 (812) 336-90-92, доб. 208

196105, Санкт-Петербург, проспект Юрия Гагарина, д.1 лит.А

Рабочие часы: 10.00-18.00

med@lanbook.ru
www.lanbook.com"""

_SIGNATURE_NEW = """--
С уважением,
Таравская Владлена Михайловна
Заведующая редакцией литературы 
ООО Издательство «ЛАНЬ»

8 (812) 336-90-92, доб. 208

196105, Санкт-Петербург, проспект Юрия Гагарина, д.1 лит.А

Рабочие часы: 10.00-18.00

med@lanbook.ru
www.lanbook.com"""

def _choose_signature(group: str) -> str:
    text = _SIGNATURE_NEW if _resolve_signature_mode(group) == "new" else _SIGNATURE_OLD
    return text.replace("\n", "<br>")


def _inject_signature(html_body: str, signature_html: str) -> str:
    """Вставляет подпись в письмо, заменяя {{SIGNATURE}} или добавляя перед </body>."""
    if not html_body:
        return signature_html
    if "{{SIGNATURE}}" in html_body:
        return html_body.replace("{{SIGNATURE}}", signature_html)
    lower = html_body.lower()
    closing = lower.rfind("</body>")
    if closing != -1:
        return html_body[:closing] + "\n" + signature_html + "\n" + html_body[closing:]
    return html_body + "\n" + signature_html + "\n"

def _read_template_file(path: str) -> str:
    if not os.path.exists(path):
        alt = os.path.splitext(path)[0] + ".html"
        if os.path.exists(alt):
            path = alt
    with open(path, encoding="utf-8") as f:
        return f.read()


def _render_template_for_group(group: str, context: Dict[str, str]) -> str:
    """Загрузить HTML шаблон для указанного кода и дополнить подписью."""

    info = get_template(group)
    path = info.get("path") if info else ""
    if path and os.path.exists(path):
        html = _read_template_file(path)
    else:
        html = "<html><body>{{SIGNATURE}}</body></html>"
    signature_html = _choose_signature(group)
    return _inject_signature(html, signature_html)


def log_domain_rate_limit(domain: str, sleep_s: float) -> None:
    """Log diagnostic message for per-domain rate limiting.

    The real sending code sleeps for ``sleep_s`` seconds; in tests we pass
    ``0`` to avoid delays and simply verify that the log entry is emitted.
    """

    try:
        sleep_s = float(sleep_s)
    except Exception:
        sleep_s = 0.0
    logging.getLogger(__name__).info(
        "rate-limit: sleeping %.3fs for domain %s", sleep_s, domain
    )


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


def build_messages_for_group(
    group: str, recipients: List[str], base_context: Dict[str, str]
) -> List[EmailMessage]:
    """
    Собирает письма для указанного направления на основе шаблона.
    """
    body_html = _render_template_for_group(group, base_context)
    out: List[EmailMessage] = []
    inline_logo = os.getenv("INLINE_LOGO", "1") == "1"
    logo_path = os.getenv("LOGO_PATH", "")
    logo_cid = os.getenv("LOGO_CID", "logo")
    for rcpt in recipients:
        msg = EmailMessage()
        msg["To"] = rcpt
        text = strip_html(body_html)
        msg.set_content(text)
        msg.add_alternative(body_html, subtype="html")
        if inline_logo and logo_path and os.path.exists(logo_path):
            try:
                with open(logo_path, "rb") as img:
                    img_bytes = img.read()
                msg.get_payload()[-1].add_related(
                    img_bytes,
                    maintype="image",
                    subtype="png",
                    cid=f"<{logo_cid}>",
                )
            except Exception:
                pass
        if group:
            msg["X-EBOT-Group"] = group
            msg["X-EBOT-Group-Key"] = group
        # В самом конце — зафиксировать корректный From по группе
        _apply_from(msg, group)
        out.append(msg)
    return out


def _rate_limit_domain(recipient: str) -> None:
    """Simple per-domain rate limiter."""

    domain = recipient.rsplit("@", 1)[-1].lower()
    now = time.monotonic()
    last = _last_domain_send.get(domain)
    if last is not None:
        elapsed = now - last
        if elapsed < _DOMAIN_RATE_LIMIT:
            pause = _DOMAIN_RATE_LIMIT - elapsed
            log_domain_rate_limit(domain, pause)
            time.sleep(pause)
            now = last + _DOMAIN_RATE_LIMIT
    _last_domain_send[domain] = now


def _register_send(recipient: str, batch_id: str | None) -> bool:
    """Register send attempt and enforce idempotency inside a batch."""

    if not batch_id:
        return True
    key = f"{normalize_email(recipient)}|{batch_id}"
    if key in _batch_idempotency:
        return False
    _batch_idempotency.add(key)
    return True


# === Blocklist helper ===
_DEFAULT_BLOCK_PARTS = [
    "no-reply",
    "noreply",
    "do-not-reply",
    "mailer-daemon",
    "postmaster",
    "webmaster",
    "admin@",
]


def _is_blocklisted(email: str) -> bool:
    e = (email or "").lower()
    parts = set(_DEFAULT_BLOCK_PARTS)
    if os.getenv("FILTER_SUPPORT", "0") == "1":
        parts.add("support@")
    extra = os.getenv("FILTER_BLOCKLIST", "")
    if extra:
        for token in extra.split(","):
            token = token.strip().lower()
            if token:
                parts.add(token)
    return any(p in e for p in parts)


# === Idempotency (24h persist) ===


def _make_send_key(msg) -> str:
    """Return stable key for a message for the current day."""

    from_ = (msg.get("From") or "").strip().lower()
    to_ = (msg.get("To") or "").strip().lower()
    subj = (msg.get("Subject") or "").strip().lower()
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw = f"{from_}|{to_}|{subj}|{day}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _load_sent_ids() -> Set[str]:
    ids: Set[str] = set()
    if SENT_IDS_FILE.exists():
        try:
            with SENT_IDS_FILE.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        key = obj.get("key")
                        ts = float(obj.get("ts", 0))
                        if (
                            datetime.now(timezone.utc).timestamp() - ts
                        ) <= 86400 and key:
                            ids.add(key)
                    except Exception:
                        continue
        except Exception:
            pass
    return ids


def _persist_sent_key(key: str) -> None:
    try:
        with SENT_IDS_FILE.open("a", encoding="utf-8") as f:
            f.write(
                json.dumps({"key": key, "ts": datetime.now(timezone.utc).timestamp()})
                + "\n"
            )
    except Exception:
        pass


def _primary_recipient(msg) -> str:
    values = msg.get_all("To", []) if hasattr(msg, "get_all") else []
    addresses = eut.getaddresses(values)
    for _, addr in addresses:
        if addr:
            return addr
    return (msg.get("To") or "").strip()


def _message_group_key(msg) -> str:
    """Extract a stable group identifier from message headers."""

    if msg is None or not hasattr(msg, "get"):
        return ""
    key = msg.get("X-EBOT-Group-Key", "") or msg.get("X-EBOT-Group", "") or ""
    return str(key).strip()


def was_sent_recently(msg) -> bool:
    recipient = _primary_recipient(msg)
    group = _message_group_key(msg)
    try:
        return history_service.was_sent_within_days(recipient, group, 1)
    except Exception:  # pragma: no cover - defensive fallback
        logger.debug("history_service was_sent_recently failed", exc_info=True)
    global _sent_idempotency
    if not _sent_idempotency:
        _sent_idempotency = _load_sent_ids()
    key = _make_send_key(msg)
    return key in _sent_idempotency


def mark_sent(msg) -> None:
    recipient = _primary_recipient(msg)
    group = _message_group_key(msg)
    msg_id = (msg.get("Message-ID") or "").strip() or None
    try:
        history_service.mark_sent(
            recipient,
            group,
            msg_id,
            datetime.now(timezone.utc),
        )
        return
    except Exception:  # pragma: no cover - defensive fallback
        logger.debug("history_service mark_sent failed", exc_info=True)
    key = _make_send_key(msg)
    _sent_idempotency.add(key)
    _persist_sent_key(key)


def get_preferred_sent_folder(imap: imaplib.IMAP4_SSL) -> str:
    """Return the preferred "Sent" folder, validating it on the server."""

    if IMAP_FOLDER_FILE.exists():
        name = IMAP_FOLDER_FILE.read_text(encoding="utf-8").strip()
        if name:
            status, _ = imap.select(name)
            if status == "OK":
                return name
            logger.warning("Stored sent folder %s not selectable, falling back", name)
    detected = detect_sent_folder(imap)
    status, _ = imap.select(detected)
    if status == "OK":
        return detected
    logger.warning("Detected sent folder %s not selectable, using Sent", detected)
    return "Sent"


def send_raw_smtp_with_retry(raw_message: str | bytes, recipient: str, max_tries=3):
    last_exc: Exception | None = None
    grp = ""
    eb_uuid = ""
    mid = ""
    if isinstance(raw_message, EmailMessage):
        msg = raw_message
        grp = _message_group_key(msg)
        eb_uuid = msg.get("X-EBOT-UUID", "") or ""
        mid = msg.get("Message-ID", "") or ""
    else:
        try:
            parsed = (
                message_from_bytes(raw_message, policy=policy.default)
                if isinstance(raw_message, (bytes, bytearray))
                else message_from_string(str(raw_message), policy=policy.default)
            )
            if isinstance(parsed, EmailMessage):
                msg = parsed
            else:
                msg = EmailMessage()
                msg.set_content(_raw_to_text(raw_message))
            grp = _message_group_key(msg)
            eb_uuid = msg.get("X-EBOT-UUID", "") or ""
            mid = msg.get("Message-ID", "") or ""
        except Exception:
            msg = EmailMessage()
            msg.set_content(_raw_to_text(raw_message))
    if not msg.get("To"):
        msg["To"] = recipient
    if not msg.get("From") and EMAIL_ADDRESS:
        msg["From"] = EMAIL_ADDRESS

    smtp = RobustSMTP()
    try:
        for attempt in range(max_tries):
            _rate_limit_domain(recipient)
            try:
                send_with_retry(smtp, msg)
                logger.info("Email sent", extra={"event": "send", "email": recipient})
                try:
                    log_success(
                        recipient,
                        grp,
                        extra={"uuid": eb_uuid, "message_id": mid},
                    )
                except Exception:
                    pass
                try:
                    history_service.mark_sent(
                        recipient,
                        grp,
                        mid,
                        datetime.now(timezone.utc),
                    )
                except Exception:  # pragma: no cover - defensive fallback
                    logger.debug("history_service mark_sent failed", exc_info=True)
                return
            except smtplib.SMTPResponseException as e:
                code = getattr(e, "smtp_code", None)
                msg_bytes = getattr(e, "smtp_error", b"")
                add_bounce(recipient, code, msg_bytes, "send")
                if is_soft_bounce(code, msg_bytes) and attempt < max_tries - 1:
                    delay = 2**attempt
                    logger.info(
                        "Soft bounce for %s (%s), retrying in %s s",
                        recipient,
                        code,
                        delay,
                    )
                    time.sleep(delay)
                    last_exc = e
                    continue
                if is_hard_bounce(code, msg_bytes):
                    suppress_add(recipient, code, "hard bounce")
                last_exc = e
                try:
                    err = (
                        msg_bytes.decode()
                        if isinstance(msg_bytes, (bytes, bytearray))
                        else msg_bytes
                    )
                    log_error(
                        recipient,
                        grp,
                        f"{code} {err}",
                        extra={"uuid": eb_uuid, "message_id": mid},
                    )
                except Exception:
                    pass
                break
            except Exception as e:
                last_exc = e
                logger.warning("SMTP send failed to %s: %s", recipient, e)
                try:
                    log_error(
                        recipient,
                        grp,
                        _smtp_reason(e),
                        extra={"uuid": eb_uuid, "message_id": mid},
                    )
                except Exception:
                    pass
                if attempt < max_tries - 1:
                    time.sleep(2**attempt)
        if last_exc:
            raise last_exc
    finally:
        smtp.close()


def save_to_sent_folder(
    raw_message: EmailMessage | str | bytes,
    imap: Optional[imaplib.IMAP4_SSL] = None,
    folder: Optional[str] = None,
):
    """Унифицированное сохранение в «Отправленные»:
    - Берём IMAP_HOST/IMAP_PORT/EMAIL_* из .env,
    - Определяем папку через detect_sent_folder(imap),
    - Выполняем append_to_sent(imap, folder, msg_bytes).
    """
    try:
        close = False
        if isinstance(raw_message, EmailMessage):
            msg_bytes = raw_message.as_bytes()
        elif isinstance(raw_message, bytes):
            msg_bytes = raw_message
        else:
            msg_bytes = (raw_message or "").encode("utf-8")

        if imap is None:
            imap_host = os.getenv("IMAP_HOST", "")
            imap_port = int(os.getenv("IMAP_PORT", "993"))
            user = os.getenv("EMAIL_ADDRESS", "")
            pwd = os.getenv("EMAIL_PASSWORD", "")
            if not (imap_host and user and pwd):
                logger.warning("IMAP creds are incomplete; skip APPEND")
                return
            imap = imaplib.IMAP4_SSL(imap_host, imap_port)
            imap.login(user, pwd)
            close = True

        if folder is None:
            folder = detect_sent_folder(imap)

        status, _ = append_to_sent(imap, folder, msg_bytes)
        logger.info("IMAP APPEND to %s: %s", folder, status)
    except Exception as e:
        log_internal_error(f"save_to_sent_folder: {e}")
    finally:
        if close and imap is not None:
            try:
                imap.logout()
            except Exception:
                pass


def build_message(
    to_addr: str,
    html_path: str,
    subject: str,
    *,
    group_title: str | None = None,
    group_key: str | None = None,
) -> tuple[EmailMessage, str]:
    html_body = _read_template_file(html_path)
    host = os.getenv("HOST", "example.com")
    font_family, base_size = _extract_fonts(html_body)
    sig_size = max(base_size - 1, 1)
    template_info = get_template_by_path(html_path)
    info_code = ""
    info_label = ""
    if template_info:
        raw_code = template_info.get("code")
        if raw_code:
            info_code = str(raw_code).strip()
        raw_label = template_info.get("label")
        if raw_label:
            info_label = str(raw_label).strip()
    fallback_code = _normalize_template_code(Path(html_path).stem)
    resolved_key = str(group_key or info_code or fallback_code or "").strip()
    if not resolved_key:
        resolved_key = fallback_code
    resolved_title = str(group_title or info_label or resolved_key or "").strip()
    if not resolved_title:
        resolved_title = resolved_key
    signature_html = (
        f'<div style="margin-top:20px;font-family:{font_family};'
        f'font-size:{sig_size}px;color:#222;line-height:1.4;">{_choose_signature(resolved_key)}</div>'
    )
    inline_logo = os.getenv("INLINE_LOGO", "1") == "1"
    if not inline_logo:
        html_body = re.sub(
            r"<img[^>]+cid:logo[^>]*>", "", html_body, flags=re.IGNORECASE
        )
    token = secrets.token_urlsafe(16)
    link = f"https://{host}/unsubscribe?email={to_addr}&token={token}"
    unsub_html = (
        f'<div style="margin-top:8px"><a href="{link}" '
        'style="display:inline-block;padding:6px 12px;font-size:12px;background:#eee;'
        'color:#333;text-decoration:none;border-radius:4px">Отписаться</a></div>'
    )
    html_body = _inject_signature(html_body, signature_html)
    html_body = html_body.replace("</body>", f"{unsub_html}</body>")
    text_body = strip_html(html_body) + f"\n\nОтписаться: {link}"
    msg = EmailMessage()
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Reply-To"] = EMAIL_ADDRESS
    msg["List-Unsubscribe"] = f"<mailto:{EMAIL_ADDRESS}?subject=unsubscribe>, <{link}>"
    msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")
    _apply_from(msg, resolved_key)
    if resolved_title:
        msg["X-EBOT-Template-Label"] = resolved_title
    logo_path = SCRIPT_DIR / "Logo.png"
    if inline_logo and logo_path.exists():
        try:
            with logo_path.open("rb") as img:
                img_bytes = img.read()
            msg.get_payload()[-1].add_related(
                img_bytes, maintype="image", subtype="png", cid="<logo>"
            )
        except Exception as e:
            log_internal_error(f"attach_logo: {e}")

    if not msg.get("Message-ID"):
        msg["Message-ID"] = eut.make_msgid()

    eb_uuid = str(uuid.uuid4())
    msg["X-EBOT-UUID"] = eb_uuid
    msg["X-EBOT-Recipient"] = to_addr
    if resolved_title:
        msg["X-EBOT-Group"] = resolved_title
    if resolved_key:
        msg["X-EBOT-Group-Key"] = resolved_key
    elif resolved_title:
        msg["X-EBOT-Group-Key"] = resolved_title
    return msg, token, eb_uuid


def send_email(
    recipient: str,
    html_path: str,
    subject: str = "Издательство Лань приглашает к сотрудничеству",
    notify_func=None,
    batch_id: str | None = None,
):
    try:
        if not _register_send(recipient, batch_id):
            logger.info(
                "Skipping duplicate send to %s for batch %s", recipient, batch_id
            )
            return ""
        msg, token, _ = build_message(recipient, html_path, subject)
        send_raw_smtp_with_retry(msg, recipient, max_tries=3)
        save_to_sent_folder(msg)
        return token
    except Exception as e:
        log_internal_error(f"send_email: {recipient}: {e}")
        if notify_func:
            notify_func(f"❌ Ошибка при отправке на {recipient}: {e}")
        raise


async def async_send_email(recipient: str, html_path: str) -> str:
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, send_email, recipient, html_path)
    except Exception as e:
        logger.exception(e)
        log_internal_error(e)
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
            log_internal_error(e)
            if notify_func:
                try:
                    await notify_func(f"❌ Ошибка: {e}")
                except Exception as inner:
                    logger.exception(inner)
                    log_internal_error(inner)

    return asyncio.create_task(runner())


def send_email_with_sessions(
    smtp: RobustSMTP,
    imap: imaplib.IMAP4_SSL,
    sent_folder: str,
    recipient: str,
    html_path: str,
    subject: str = "Издательство Лань приглашает к сотрудничеству",
    batch_id: str | None = None,
    fixed_from: str | None = None,
    *,
    group_title: str | None = None,
    group_key: str | None = None,
):
    if not _register_send(recipient, batch_id):
        logger.info("Skipping duplicate send to %s for batch %s", recipient, batch_id)
        return ""
    msg, token, eb_uuid = build_message(
        recipient,
        html_path,
        subject,
        group_title=group_title,
        group_key=group_key,
    )
    group_code = _message_group_key(msg)
    try:
        send_with_retry(smtp, msg)
        save_to_sent_folder(msg, imap=imap, folder=sent_folder)
        try:
            extra = {
                "uuid": eb_uuid,
                "message_id": msg.get("Message-ID", ""),
            }
            if fixed_from:
                extra["fixed_from"] = fixed_from
            log_success(recipient, group_code, extra=extra)
        except Exception:
            pass
        try:
            history_service.mark_sent(
                recipient,
                group_code,
                msg.get("Message-ID", ""),
                datetime.now(timezone.utc),
            )
        except Exception:  # pragma: no cover - defensive fallback
            logger.debug("history_service mark_sent failed", exc_info=True)
        return token
    except smtplib.SMTPResponseException as e:
        code = getattr(e, "smtp_code", None)
        msg_bytes = getattr(e, "smtp_error", b"")
        add_bounce(recipient, code, msg_bytes, "send")
        try:
            err = msg_bytes.decode() if isinstance(msg_bytes, (bytes, bytearray)) else msg_bytes
            extra = {
                "uuid": eb_uuid,
                "message_id": msg.get("Message-ID", ""),
            }
            if fixed_from:
                extra["fixed_from"] = fixed_from
            log_error(
                recipient,
                group_code,
                f"{code} {err}",
                extra=extra,
            )
        except Exception:
            pass
        raise
    except Exception as e:
        logger.warning("SMTP send failed to %s: %s", recipient, e)
        try:
            extra = {
                "uuid": eb_uuid,
                "message_id": msg.get("Message-ID", ""),
            }
            if fixed_from:
                extra["fixed_from"] = fixed_from
            log_error(
                recipient,
                group_code,
                _smtp_reason(e),
                extra=extra,
            )
        except Exception:
            pass
        raise


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
        log_internal_error(f"process_unsubscribe_requests: {e}")


def _canonical_blocked(email_str: str) -> str:
    e = normalize_email(email_str)
    e = re.sub(r"^\.+", "", e)
    e = re.sub(r"^\d{1,2}(?=[A-Za-z])", "", e)
    return e


def get_blocked_emails() -> Set[str]:
    if not os.path.exists(BLOCKED_FILE):
        return set()
    with open(BLOCKED_FILE, "r", encoding="utf-8") as f:
        return {_canonical_blocked(line) for line in f if "@" in line}


def add_blocked_email(email_str: str) -> bool:
    email_norm = _canonical_blocked(email_str)
    if not email_norm or "@" not in email_norm:
        return False
    existing = get_blocked_emails()
    if email_norm in existing:
        return False
    with open(BLOCKED_FILE, "a", encoding="utf-8") as f:
        f.write(email_norm + "\n")
    return True


def dedupe_blocked_file():
    if not os.path.exists(BLOCKED_FILE):
        return
    with open(BLOCKED_FILE, "r", encoding="utf-8") as f:
        keep = {_canonical_blocked(line) for line in f if "@" in line}
    with open(BLOCKED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(keep)) + "\n")


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
            if row.get("email") == email_norm and (
                token is None or row.get("unsubscribe_token") == token
            ):
                row["unsubscribed"] = "1"
                row["unsubscribed_at"] = datetime.utcnow().isoformat()
                changed = True
    if changed:
        headers = (
            rows[0].keys()
            if rows
            else [
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
        )
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
):
    if status not in {"ok", "sent", "success"}:
        return
    extra = {
        "user_id": user_id or "",
        "filename": filename or "",
        "error_msg": error_msg or "",
        "unsubscribe_token": unsubscribe_token,
        "unsubscribed": unsubscribed,
        "unsubscribed_at": unsubscribed_at,
    }
    upsert_sent_log(
        LOG_FILE,
        normalize_email(email_addr),
        datetime.utcnow(),
        group,
        status=status,
        extra=extra,
    )
    global _log_cache
    _log_cache = None


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
    try:
        return history_service.was_sent_within_days(email, "", days)
    except Exception:  # pragma: no cover - defensive fallback
        logger.debug("history_service was_sent_within failed", exc_info=True)
    cutoff = datetime.utcnow() - timedelta(days=days)
    cache = _load_sent_log()
    key = canonical_for_history(email)
    dt = cache.get(key)
    if dt and dt >= cutoff:
        return True
    recent = get_recently_contacted_emails_cached()
    return key in recent


def was_emailed_recently(
    email_addr: str,
    since_days: int = 180,
    imap: Optional[imaplib.IMAP4_SSL] = None,
    folder: Optional[str] = None,
) -> bool:
    cutoff = datetime.utcnow() - timedelta(days=since_days)
    cache = _load_sent_log()
    key = canonical_for_history(email_addr)
    dt = cache.get(key)
    if dt and dt >= cutoff:
        return True
    try:
        close = False
        if imap is None:
            imap = imaplib.IMAP4_SSL("imap.mail.ru")
            imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            close = True
        if folder is None:
            folder = get_preferred_sent_folder(imap)
        status, _ = imap.select(f'"{folder}"')
        if status != "OK":
            logger.warning("select %s failed (%s), using Sent", folder, status)
            folder = "Sent"
            imap.select(f'"{folder}"')
        date_str = (datetime.utcnow() - timedelta(days=since_days)).strftime("%d-%b-%Y")
        status, data = imap.search(None, f'(SINCE {date_str} HEADER To "{email_addr}")')
        return status == "OK" and bool(data and data[0])
    except Exception as e:
        log_internal_error(f"was_emailed_recently: {e}")
        return False
    finally:
        if close and imap is not None:
            try:
                imap.logout()
            except Exception as e:
                log_internal_error(f"was_emailed_recently logout: {e}")


def get_recent_6m_union() -> Set[str]:
    cutoff = datetime.utcnow() - timedelta(days=180)
    result: Set[str] = set()
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    dt = datetime.fromisoformat(row["last_sent_at"])
                except Exception:
                    continue
                if dt >= cutoff:
                    result.add(row["key"])
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
    today = datetime.utcnow().date()
    sent = set()
    with open(LOG_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = row.get("status", "ok")
            if status not in {"ok", "sent", "success"}:
                continue
            try:
                dt = datetime.fromisoformat(row["last_sent_at"])
            except Exception:
                continue
            if dt.date() == today:
                sent.add(row["email"].lower())
    return sent


def count_sent_today() -> int:
    return len(get_sent_today())


def start_manual_mass_send(group: str, emails: List[str], *args, **kwargs) -> None:
    logger.info("Manual mass send: group=%s count=%d", group, len(emails))
    # Guard от пустых рассылок
    if not emails:
        logger.info("Manual mass send skipped: empty recipient list")
        notify_user("Отправка не запущена: нет адресов для отправки.")
        return
    notify_user("Запущено — выполняю в фоне...")
    notify_user(f"✉️ Рассылка начата. Отправляем {len(emails)} писем...")

    messages = kwargs.get("messages", [])
    sent = _send_batch(
        messages,
        host=SMTP_HOST,
        user=EMAIL_ADDRESS,
        password=EMAIL_PASSWORD,
    )
    notify_user(f"✅ Отправлено писем: {sent}")


def prepare_mass_mailing(
    emails: list[str], group: str = "", chat_id: int | None = None
):
    """Apply all mass-mailing filters and return ready e-mails.

    Returns a tuple ``(ready, blocked_foreign, blocked_invalid, skipped_recent, digest)``
    where ``ready`` is the list of addresses allowed to send, ``blocked_foreign`` are
    addresses filtered due to foreign TLDs, ``blocked_invalid`` – suppressed or
    blocked addresses, and ``skipped_recent`` – addresses found in the 180 day
    history. ``digest`` contains counters for logging.
    """

    source = list(emails)
    if chat_id is not None:
        try:
            source = apply_saved_edits(source, chat_id)
        except Exception:  # pragma: no cover - defensive fallback
            source = list(emails)

    blocked = get_blocked_emails()
    sent_today = get_sent_today()
    lookup_days = history_service.get_days_rule_default()

    blocked_foreign: list[str] = []
    blocked_invalid: list[str] = []
    skipped_recent: list[str] = []

    queue: list[str] = []
    for e in source:
        if e in blocked or e in sent_today:
            continue
        if is_foreign(e):
            blocked_foreign.append(e)
        else:
            queue.append(e)

    queue2: list[str] = []
    for e in queue:
        if is_suppressed(e):
            blocked_invalid.append(e)
        else:
            queue2.append(e)

    try:
        queue3, skipped_recent = history_service.filter_by_days(queue2, group, lookup_days)
    except Exception:  # pragma: no cover - defensive fallback
        logger.debug("history_service filter_by_days failed", exc_info=True)
        queue3 = list(queue2)
        skipped_recent = []

    deduped: list[str] = []
    seen: set[str] = set()
    dup_skipped = 0
    for e in queue3:
        norm = normalize_email(e)
        if norm in seen:
            dup_skipped += 1
        else:
            seen.add(norm)
            deduped.append(e)

    digest = {
        "input_total": len(source),
        "after_suppress": len(queue2),
        "foreign_blocked": len(blocked_foreign),
        "after_180d": len(queue3),
        "sent_planned": len(deduped),
        "skipped_by_dup_in_batch": dup_skipped,
        "unique_ready_to_send": len(deduped),
        "skipped_suppress": len(blocked_invalid),
        "skipped_180d": len(skipped_recent),
        "skipped_foreign": len(blocked_foreign),
    }

    return deduped, blocked_foreign, blocked_invalid, skipped_recent, digest


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
                inserted, updated = upsert_sent_log(
                    LOG_FILE,
                    normalize_email(addr),
                    dt or datetime.utcnow(),
                    "imap_sync",
                    status="external",
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
        log_internal_error(f"sync_log_with_imap: {e}")
        raise
    finally:
        if imap is not None:
            try:
                imap.logout()
            except Exception as e:
                log_internal_error(f"sync_log_with_imap logout: {e}")


def periodic_unsubscribe_check(stop_event):
    while not stop_event.is_set():
        try:
            process_unsubscribe_requests()
        except Exception as e:
            log_internal_error(f"periodic_unsubscribe_check: {e}")
        time.sleep(300)


def check_env_vars():
    for var in ["TELEGRAM_BOT_TOKEN", "EMAIL_ADDRESS", "EMAIL_PASSWORD"]:
        if not os.getenv(var):
            raise EnvironmentError(f"Переменная окружения {var} не задана!")


__all__ = [
    "DOWNLOAD_DIR",
    "LOG_FILE",
    "BLOCKED_FILE",
    "MAX_EMAILS_PER_DAY",
    "SIGNATURE_TEXT",
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
    "prepare_mass_mailing",
    "sync_log_with_imap",
    "periodic_unsubscribe_check",
    "check_env_vars",
    "was_sent_within",
    "was_emailed_recently",
]
