import datetime
import email
import imaplib
import os
import re
from email.header import decode_header, make_header
from typing import Set

from config import (
    BLOCKED_EMAILS_PATH,
    BOUNCE_SINCE_DAYS,
    EMAIL_ADDRESS,
    EMAIL_PASSWORD,
    IMAP_HOST,
    IMAP_PORT,
    INBOX_MAILBOX,
)
from utils.blocked_store import BlockedStore
from utils.email_normalize import normalize_email

LIST_UNSUB_RE = re.compile(r"<mailto:([^>]+)>", re.I)
MAILTO_RE = re.compile(r"mailto:([^>\s]+)", re.I)
ADDR_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
UNSUB_TOKENS = ("unsubscribe", "отписаться", "удалите меня", "remove me")


def _decode(value: str) -> str:
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def _search_addrs_from_message(msg) -> Set[str]:
    found: Set[str] = set()
    for header_name in ("List-Unsubscribe", "List-Unsubscribe-Post"):
        header_value = msg.get(header_name)
        if not header_value:
            continue
        for match in LIST_UNSUB_RE.findall(header_value) + MAILTO_RE.findall(header_value):
            found.update(ADDR_RE.findall(match))
    for header_name in ("Reply-To", "From"):
        header_value = msg.get(header_name)
        if not header_value:
            continue
        _, addr = email.utils.parseaddr(header_value)
        if addr:
            found.add(addr)
    subject = _decode(msg.get("Subject", "") or "")
    lowered_subject = subject.lower()
    if any(token in lowered_subject for token in UNSUB_TOKENS):
        found.update(ADDR_RE.findall(subject))
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_maintype() != "text":
                continue
            try:
                payload = part.get_payload(decode=True) or b""
                text = payload.decode(part.get_content_charset() or "utf-8", "ignore")
            except Exception:
                continue
            lowered = text.lower()
            if any(token in lowered for token in UNSUB_TOKENS):
                found.update(ADDR_RE.findall(text))
    else:
        payload = msg.get_payload(decode=True) or b""
        text = payload.decode(msg.get_content_charset() or "utf-8", "ignore")
        lowered = text.lower()
        if any(token in lowered for token in UNSUB_TOKENS):
            found.update(ADDR_RE.findall(text))
    normalised: Set[str] = set()
    for addr in found:
        norm = normalize_email(addr)
        if norm:
            normalised.add(norm)
    return normalised


def run_once() -> int:
    store = BlockedStore(BLOCKED_EMAILS_PATH)
    since_days = int(os.getenv("BOUNCE_SINCE_DAYS", BOUNCE_SINCE_DAYS))
    date_since = (datetime.datetime.utcnow() - datetime.timedelta(days=since_days)).strftime("%d-%b-%Y")

    mailbox = os.getenv("INBOX_MAILBOX", INBOX_MAILBOX)
    host = os.getenv("IMAP_HOST", IMAP_HOST)
    port = int(os.getenv("IMAP_PORT", IMAP_PORT))
    user = os.getenv("EMAIL_ADDRESS", EMAIL_ADDRESS)
    password = os.getenv("EMAIL_PASSWORD", EMAIL_PASSWORD)

    if not host or not user or not password:
        return 0

    conn = imaplib.IMAP4_SSL(host, port)
    try:
        conn.login(user, password)
        conn.select(mailbox)
        status, data = conn.search(None, f'(SINCE "{date_since}")')
        if status != "OK":
            return 0
        collected: Set[str] = set()
        ids = data[0].split()
        for uid in ids[-2000:]:
            status, msg_data = conn.fetch(uid, "(RFC822)")
            if status != "OK" or not msg_data:
                continue
            msg = email.message_from_bytes(msg_data[0][1])
            collected.update(_search_addrs_from_message(msg))
        return store.add_many(collected)
    finally:
        try:
            conn.logout()
        except Exception:
            pass


if __name__ == "__main__":
    added = run_once()
    print(f"Unsubscribe scanner: added {added} blocked emails")
