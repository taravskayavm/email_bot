"""Helpers for reading metadata from the IMAP "Sent" folder."""

from __future__ import annotations

import email
import email.utils as eut
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from contextlib import closing

try:  # pragma: no cover - optional dependency
    from imapclient.imapclient import imap_utf7
except Exception:  # pragma: no cover - fallback to internal helper
    imap_utf7 = None

from emailbot.messaging_utils import _imap_utf7_encode as _imap_utf7_encode
from emailbot.messaging_utils import parse_imap_date_to_utc
from emailbot.net_imap import get_imap_timeout, imap_connect_ssl


def _encode_mailbox(name: str) -> str:
    if not name:
        return name
    if imap_utf7 is not None:
        try:
            return imap_utf7.encode(name)
        except Exception:  # pragma: no cover - fallback
            pass
    try:
        return _imap_utf7_encode(name)
    except Exception:  # pragma: no cover - give up
        return name
from emailbot.services.cooldown import normalize_email_for_key


def find_last_sent_at(email_norm: str, mailbox: str, days: int) -> Optional[datetime]:
    """Return the most recent ``Date`` of a message sent to ``email_norm``."""

    host = os.getenv("IMAP_HOST")
    port_raw = os.getenv("IMAP_PORT", "993")
    user = os.getenv("EMAIL_ADDRESS")
    password = os.getenv("EMAIL_PASSWORD")
    if not host or not user or not password:
        return None

    try:
        port = int(port_raw)
    except Exception:
        port = 993

    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%d-%b-%Y")

    timeout = get_imap_timeout(15.0)
    with closing(imap_connect_ssl(host, port, timeout=timeout)) as client:
        client.login(user, password)
        status, _ = client.select(_encode_mailbox(mailbox))
        if status != "OK":  # pragma: no cover - depends on server dialect
            status, _ = client.select(f'"{mailbox}"')
        if status != "OK":
            return None
        search_criteria = [f"(SENTSINCE {since})", f"(SINCE {since})"]
        data = None
        for criteria in search_criteria:
            status, payload = client.search(None, criteria)
            if status == "OK":
                data = payload
                break
        if not data:
            return None
        ids = data[0].split() if data and data[0] else []
        last: Optional[datetime] = None
        for msg_id in ids:
            status, msgdata = client.fetch(msg_id, "(BODY.PEEK[HEADER.FIELDS (TO DATE)])")
            if status != "OK" or not msgdata or not msgdata[0]:
                continue
            header_bytes = msgdata[0][1]
            if not header_bytes:
                continue
            header = email.message_from_bytes(header_bytes)
            addresses: list[str] = []
            for value in header.get_all("To", []):
                for _, addr in eut.getaddresses([value]):
                    if addr:
                        addresses.append(addr)
            normalised = {normalize_email_for_key(addr) for addr in addresses if addr}
            if email_norm not in normalised:
                continue
            dt_raw = header.get("Date")
            if not dt_raw:
                continue
            parsed = parse_imap_date_to_utc(dt_raw)
            if last is None or parsed > last:
                last = parsed
        return last
