"""Message building and sending utilities."""

from __future__ import annotations

import asyncio
import csv
import email
import imaplib
import logging
import os
import re
import time
import secrets
import smtplib
from datetime import datetime, timedelta
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional, Set

from .extraction import normalize_email, strip_html
from .smtp_client import SmtpClient
from .utils import log_error
from .messaging_utils import (
    add_bounce,
    canonical_for_history,
    is_soft_bounce,
    is_hard_bounce,
    suppress_add,
    upsert_sent_log,
    load_sent_log,
    load_seen_events,
    save_seen_events,
    SYNC_SEEN_EVENTS_PATH,
)

logger = logging.getLogger(__name__)

# Resolve the project root (one level above this file) and use shared
# directories located at the repository root.
SCRIPT_DIR = Path(__file__).resolve().parent.parent
DOWNLOAD_DIR = str(SCRIPT_DIR / "downloads")
LOG_FILE = str(Path("/mnt/data") / "sent_log.csv")
BLOCKED_FILE = str(SCRIPT_DIR / "blocked_emails.txt")
MAX_EMAILS_PER_DAY = 200

# HTML templates are stored at the root-level ``templates`` directory.
TEMPLATES_DIR = str(SCRIPT_DIR / "templates")
TEMPLATE_MAP = {
    "спорт": os.path.join(TEMPLATES_DIR, "sport.htm"),
    "туризм": os.path.join(TEMPLATES_DIR, "tourism.htm"),
    "медицина": os.path.join(TEMPLATES_DIR, "medicine.htm"),
}

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

IMAP_FOLDER_FILE = SCRIPT_DIR / "imap_sent_folder.txt"

_last_domain_send: Dict[str, float] = {}
_DOMAIN_RATE_LIMIT = 1.0  # seconds between sends per domain
_sent_idempotency: Set[str] = set()


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
            with SmtpClient(
                "smtp.mail.ru", 465, EMAIL_ADDRESS, EMAIL_PASSWORD
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


def build_message(to_addr: str, html_path: str, subject: str) -> tuple[EmailMessage, str]:
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
    token = secrets.token_urlsafe(16)
    link = f"https://{host}/unsubscribe?email={to_addr}&token={token}"
    unsub_html = (
        f'<div style="margin-top:8px"><a href="{link}" '
        'style="display:inline-block;padding:6px 12px;font-size:12px;background:#eee;' \
        'color:#333;text-decoration:none;border-radius:4px">Отписаться</a></div>'
    )
    html_body = html_body.replace("</body>", f"{signature_html}{unsub_html}</body>")
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
    subject: str = "Издательство Лань приглашает к сотрудничеству",
    notify_func=None,
    batch_id: str | None = None,
):
    try:
        if not _register_send(recipient, batch_id):
            logger.info("Skipping duplicate send to %s for batch %s", recipient, batch_id)
            return ""
        msg, token = build_message(recipient, html_path, subject)
        raw = msg.as_string()
        send_raw_smtp_with_retry(raw, recipient, max_tries=3)
        save_to_sent_folder(raw)
        return token
    except Exception as e:
        log_error(f"send_email: {recipient}: {e}")
        if notify_func:
            notify_func(f"❌ Ошибка при отправке на {recipient}: {e}")
        raise


async def async_send_email(recipient: str, html_path: str) -> str:
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
    subject: str = "Издательство Лань приглашает к сотрудничеству",
    batch_id: str | None = None,
):
    if not _register_send(recipient, batch_id):
        logger.info("Skipping duplicate send to %s for batch %s", recipient, batch_id)
        return ""
    msg, token = build_message(recipient, html_path, subject)
    raw = msg.as_string()
    client.send(EMAIL_ADDRESS, recipient, raw)
    save_to_sent_folder(raw, imap=imap, folder=sent_folder)
    return token


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
):
    if status not in {"ok", "sent", "success"}:
        return
    extra = {
        "status": status,
        "user_id": user_id or "",
        "filename": filename or "",
        "error_msg": error_msg or "",
        "unsubscribe_token": unsubscribe_token,
        "unsubscribed": unsubscribed,
        "unsubscribed_at": unsubscribed_at,
    }
    upsert_sent_log(LOG_FILE, normalize_email(email_addr), datetime.utcnow(), group, extra)
    global _log_cache
    _log_cache = None


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
        log_error(f"was_emailed_recently: {e}")
        return False
    finally:
        if close and imap is not None:
            try:
                imap.logout()
            except Exception as e:
                log_error(f"was_emailed_recently logout: {e}")


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


def sync_log_with_imap() -> Dict[str, int]:
    imap = None
    stats = {
        "scanned_messages": 0,
        "recipients_seen": 0,
        "new_contacts": 0,
        "updated_contacts": 0,
        "skipped_duplicates": 0,
    }
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
        sent_log_cache = load_sent_log(Path(LOG_FILE))
        seen_events = load_seen_events(SYNC_SEEN_EVENTS_PATH)
        changed_events = False
        for num in data[0].split() if data and data[0] else []:
            stats["scanned_messages"] += 1
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
                stats["recipients_seen"] += 1
                key = canonical_for_history(addr)
                if msgid and (msgid, key) in seen_events:
                    stats["skipped_duplicates"] += 1
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
                    {"status": "external"},
                )
                if inserted:
                    stats["new_contacts"] += 1
                    sent_log_cache[key] = dt or datetime.utcnow()
                elif updated:
                    stats["updated_contacts"] += 1
                    sent_log_cache[key] = dt or datetime.utcnow()
                if msgid:
                    seen_events.add((msgid, key))
                    changed_events = True
        if changed_events:
            save_seen_events(SYNC_SEEN_EVENTS_PATH, seen_events)
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
    "DOWNLOAD_DIR",
    "LOG_FILE",
    "BLOCKED_FILE",
    "MAX_EMAILS_PER_DAY",
    "TEMPLATE_MAP",
    "SIGNATURE_HTML",
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
    "sync_log_with_imap",
    "periodic_unsubscribe_check",
    "check_env_vars",
    "was_sent_within",
    "was_emailed_recently",
]
