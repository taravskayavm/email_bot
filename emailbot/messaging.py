"""Message building and sending utilities."""

from __future__ import annotations

import os
import re
import csv
import time
import imaplib
import email
import logging
import asyncio
from datetime import datetime, timedelta
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from typing import Set, List, Dict, Optional, Callable, Awaitable

from .smtp_client import SmtpClient
from .utils import log_error
from .extraction import normalize_email, strip_html


logger = logging.getLogger(__name__)

# Resolve the project root (one level above this file) and use shared
# directories located at the repository root.
SCRIPT_DIR = Path(__file__).resolve().parent.parent
DOWNLOAD_DIR = str(SCRIPT_DIR / "downloads")
LOG_FILE = str(SCRIPT_DIR / "sent_log.csv")
BLOCKED_FILE = str(SCRIPT_DIR / "blocked_emails.txt")
MAX_EMAILS_PER_DAY = 200

# HTML templates are stored at the root-level ``templates`` directory.
TEMPLATES_DIR = str(SCRIPT_DIR / "templates")
TEMPLATE_MAP = {
    "спорт": os.path.join(TEMPLATES_DIR, "sport.htm"),
    "туризм": os.path.join(TEMPLATES_DIR, "tourism.htm"),
    "медицина": os.path.join(TEMPLATES_DIR, "medicine.htm"),
}

SIGNATURE_HTML = (
    '<div style="margin-top:20px;font-size:12px;color:#666">'
    '—<br>Если вы больше не хотите получать письма — ответьте на это письмо словом <b>Unsubscribe</b>.'
    "</div>"
)

PRIVACY_NOTICE_HTML = (
    '<div style="margin-top:16px;font:12px/1.4 -apple-system,Segoe UI,Roboto,Arial,sans-serif;color:#666">'
    '<div style="border-top:1px solid #e5e5e5;margin:12px 0 8px"></div>'
    '<b>Почему вы получили это письмо?</b>'
    '<div>Мы пишем по профессиональному адресу, опубликованному в открытых источниках '
    '(официальные сайты/профили публикаций), с предложением профильного сотрудничества.</div>'
    '<div style="margin-top:6px"><b>Правовое основание:</b> legitimate interests (ст. 6(1)(f) GDPR / '
    'профильные интересы в РФ). <b>Цели:</b> экспертное/издательское сотрудничество. '
    '<b>Источник:</b> публичные страницы организации/автора.</div>'
    '<div style="margin-top:6px"><b>Ваши права:</b> вы можете возразить против подобных писем и/или отписаться — '
    'ответьте <b>Unsubscribe</b> на это письмо; мы добавим адрес в список исключений. '
    'Срок хранения контакта — не более необходимого для коммуникации, записи об отписке — дольше, '
    'чтобы не писать повторно.</div>'
    '<div style="margin-top:6px">Политику конфиденциальности и контакты для запросов можно получить по запросу.</div>'
    '</div>'
)

EMAIL_ADDRESS = ""
EMAIL_PASSWORD = ""

IMAP_FOLDER_FILE = SCRIPT_DIR / "imap_sent_folder.txt"


def _read_template_file(path: str) -> str:
    if not os.path.exists(path):
        alt = os.path.splitext(path)[0] + ".html"
        if os.path.exists(alt):
            path = alt
    with open(path, encoding="utf-8") as f:
        return f.read()


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
    last_exc = None
    for _ in range(max_tries):
        try:
            with SmtpClient("smtp.mail.ru", 465, EMAIL_ADDRESS, EMAIL_PASSWORD) as client:
                client.send(EMAIL_ADDRESS, recipient, raw_message)
            logger.info("Email sent to %s", recipient)
            return
        except Exception as e:
            last_exc = e
            logger.warning("SMTP send failed to %s: %s", recipient, e)
            time.sleep(2)
    raise last_exc


def save_to_sent_folder(
    raw_message: str,
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
        res = imap.append(
            folder,
            "\\Seen",
            imaplib.Time2Internaldate(time.time()),
            raw_message.encode("utf-8"),
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
    to_addr: str, html_path: str, subject: str, extra_html: str | None = None
) -> EmailMessage:
    html_body = _read_template_file(html_path)
    html_body = html_body.replace("</body>", f"{SIGNATURE_HTML}</body>")
    if extra_html:
        html_body = html_body.replace("</body>", f"{extra_html}</body>")
    text_body = strip_html(html_body)
    msg = EmailMessage()
    msg["From"] = formataddr(
        ("Редакция литературы по медицине, спорту и туризму", EMAIL_ADDRESS)
    )
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Reply-To"] = EMAIL_ADDRESS
    msg["List-Unsubscribe"] = f"<mailto:{EMAIL_ADDRESS}?subject=unsubscribe>"
    msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
    msg.set_content(text_body)
    # Attach the HTML version explicitly as ``text/html``.
    msg.add_alternative(html_body, subtype="html")
    logo_path = SCRIPT_DIR / "Logo.png"
    if logo_path.exists():
        try:
            with logo_path.open("rb") as img:
                img_bytes = img.read()
            msg.get_payload()[-1].add_related(
                img_bytes, maintype="image", subtype="png", cid="<logo>"
            )
        except Exception as e:
            log_error(f"attach_logo: {e}")
    return msg


def _is_first_contact(recipient: str) -> bool:
    recent = get_recent_6m_union()
    return normalize_email(recipient) not in recent


def send_email(
    recipient: str,
    html_path: str,
    subject: str = "Издательство Лань приглашает к сотрудничеству",
    notify_func=None,
):
    try:
        extra_html = PRIVACY_NOTICE_HTML if _is_first_contact(recipient) else None
        msg = build_message(recipient, html_path, subject, extra_html=extra_html)
        raw = msg.as_string()
        send_raw_smtp_with_retry(raw, recipient, max_tries=3)
        save_to_sent_folder(raw)
    except Exception as e:
        log_error(f"send_email: {recipient}: {e}")
        if notify_func:
            notify_func(f"❌ Ошибка при отправке на {recipient}: {e}")
        raise


async def async_send_email(recipient: str, html_path: str):
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, send_email, recipient, html_path)
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
):
    extra_html = PRIVACY_NOTICE_HTML if _is_first_contact(recipient) else None
    msg = build_message(recipient, html_path, subject, extra_html=extra_html)
    raw = msg.as_string()
    client.send(EMAIL_ADDRESS, recipient, raw)
    save_to_sent_folder(raw, imap=imap, folder=sent_folder)


def process_unsubscribe_requests():
    try:
        imap = imaplib.IMAP4_SSL("imap.mail.ru")
        imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        imap.select("INBOX")
        result, data = imap.search(None, '(UNSEEN SUBJECT "unsubscribe")')
        for num in (data[0].split() if data and data[0] else []):
            _, msg_data = imap.fetch(num, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            sender = email.utils.parseaddr(msg.get("From"))[1]
            if sender:
                add_blocked_email(sender)
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


def log_sent_email(
    email_addr, group, status="ok", user_id=None, filename=None, error_msg=None
):
    os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                datetime.utcnow().isoformat(),
                normalize_email(email_addr),
                group,
                status,
                user_id if user_id else "",
                filename if filename else "",
                error_msg if error_msg else "",
            ]
        )
        f.flush()
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


def _load_sent_log() -> dict[str, List[datetime]]:
    global _log_cache
    if _log_cache is not None:
        return _log_cache
    cache: Dict[str, List[datetime]] = {}
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2:
                    continue
                try:
                    dt = datetime.fromisoformat(row[0])
                    if dt.tzinfo is not None:
                        dt = dt.replace(tzinfo=None)
                except Exception:
                    continue
                cache.setdefault(normalize_email(row[1]), []).append(dt)
    _log_cache = cache
    return cache


def was_emailed_recently(
    email_addr: str,
    since_days: int = 180,
    imap: Optional[imaplib.IMAP4_SSL] = None,
    folder: Optional[str] = None,
) -> bool:
    cutoff = datetime.utcnow() - timedelta(days=since_days)
    cache = _load_sent_log()
    lst = cache.get(normalize_email(email_addr), [])
    if any(dt >= cutoff for dt in lst):
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
        date_str = (datetime.utcnow() - timedelta(days=since_days)).strftime(
            "%d-%b-%Y"
        )
        status, data = imap.search(
            None, f'(SINCE {date_str} HEADER To "{email_addr}")'
        )
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
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2:
                    continue
                try:
                    dt = datetime.fromisoformat(row[0])
                    if dt.tzinfo is not None:
                        dt = dt.replace(tzinfo=None)
                except Exception:
                    continue
                if dt >= cutoff:
                    result.add(normalize_email(row[1]))
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
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2:
                continue
            try:
                dt = datetime.fromisoformat(row[0])
                if dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
            except Exception:
                continue
            if dt.date() == today:
                sent.add(normalize_email(row[1]))
    return sent


def sync_log_with_imap() -> int:
    imap = None
    try:
        imap = imaplib.IMAP4_SSL("imap.mail.ru")
        imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        sent_folder = get_preferred_sent_folder(imap)
        status, _ = imap.select(f'"{sent_folder}"')
        if status != "OK":
            logger.warning("select %s failed (%s), using Sent", sent_folder, status)
            sent_folder = "Sent"
            imap.select(f'"{sent_folder}"')
        existing = get_recent_6m_union()
        date_180 = (datetime.utcnow() - timedelta(days=180)).strftime("%d-%b-%Y")
        result, data = imap.search(None, f"SINCE {date_180}")
        added = 0
        for num in (data[0].split() if data and data[0] else []):
            _, msg_data = imap.fetch(num, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            to_addr = email.utils.parseaddr(msg.get("To"))[1]
            if not to_addr:
                continue
            if normalize_email(to_addr) in existing:
                continue
            try:
                dt = email.utils.parsedate_to_datetime(msg.get("Date"))
                if dt and dt.tzinfo:
                    dt = dt.replace(tzinfo=None)
                if dt and dt < datetime.utcnow() - timedelta(days=180):
                    continue
            except Exception:
                dt = None
            with open(LOG_FILE, "a", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        (dt or datetime.utcnow()).isoformat(),
                        normalize_email(to_addr),
                        "imap_sync",
                        "external",
                        "",
                        "",
                        "",
                    ]
                )
                f.flush()
            added += 1
        return added
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
    "PRIVACY_NOTICE_HTML",
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
    "log_sent_email",
    "detect_sent_folder",
    "get_recent_6m_union",
    "get_recently_contacted_emails_cached",
    "clear_recent_sent_cache",
    "get_sent_today",
    "sync_log_with_imap",
    "periodic_unsubscribe_check",
    "check_env_vars",
    "was_emailed_recently",
]

