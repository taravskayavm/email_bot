import csv
import io
import os
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple
from zoneinfo import ZoneInfo

import email as py_email
import imaplib
from email.utils import getaddresses, parsedate_to_datetime

from .settings import REPORT_TZ, RECONCILE_SINCE_DAYS

IMAP_HOST = os.getenv("IMAP_HOST", "imap.mail.ru")
IMAP_PORT = int(os.getenv("IMAP_PORT", "993"))
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
SENT_MAILBOX = os.getenv("SENT_MAILBOX", "Sent")

LOG_FILE = "var/sent_log.csv"


def _norm_email(addr: str) -> str:
    return (addr or "").strip().lower()


def _load_csv_set(tz: ZoneInfo, since_days: int) -> Set[Tuple[str, datetime]]:
    """Load sent emails from the CSV log within the window."""

    items: Set[Tuple[str, datetime]] = set()
    if not os.path.exists(LOG_FILE):
        return items

    now_local = datetime.now(tz)
    start_local = now_local - timedelta(days=since_days)

    with open(LOG_FILE, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            status = (row.get("status") or "").strip().lower()
            if status not in {"ok", "sent", "success"}:
                continue

            email = _norm_email(row.get("email", ""))
            if not email:
                continue

            ts = (row.get("last_sent_at") or "").strip()
            if not ts:
                continue

            try:
                dt = datetime.fromisoformat(ts)
            except Exception:
                continue

            if dt.tzinfo is None:
                dt_local = dt.replace(tzinfo=tz)
            else:
                dt_local = dt.astimezone(tz)

            if dt_local >= start_local:
                midnight = dt_local.replace(hour=0, minute=0, second=0, microsecond=0)
                items.add((email, midnight))

    return items


def _imap_fetch_since(since_days: int) -> List[bytes]:
    """Fetch raw IMAP headers for messages in the Sent folder."""

    tz = ZoneInfo(REPORT_TZ)
    start_local = datetime.now(tz) - timedelta(days=since_days)
    imap_since = start_local.strftime("%d-%b-%Y")

    conn = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    conn.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    try:
        conn.select(SENT_MAILBOX)
        typ, data = conn.search(None, f'(SINCE "{imap_since}")')
        if typ != "OK":
            return []

        ids = data[0].split()
        headers: List[bytes] = []
        for mid in ids:
            typ, msg_data = conn.fetch(mid, "(BODY.PEEK[HEADER.FIELDS (DATE TO)])")
            if typ == "OK" and msg_data:
                for part in msg_data:
                    if isinstance(part, tuple) and part[1]:
                        headers.append(part[1])
        return headers
    finally:
        try:
            conn.close()
        except Exception:
            pass
        conn.logout()


def _parse_to_date(headers_bytes: bytes, tz: ZoneInfo) -> Tuple[List[str], datetime | None]:
    msg = py_email.message_from_bytes(headers_bytes)
    tos = msg.get_all("To", [])
    addresses = [addr for _, addr in getaddresses(tos)] if tos else []

    date_hdr = msg.get("Date")
    dt_local = None
    if date_hdr:
        try:
            dt = parsedate_to_datetime(date_hdr)
            if dt.tzinfo is None:
                dt_local = dt.replace(tzinfo=tz)
            else:
                dt_local = dt.astimezone(tz)
        except Exception:
            dt_local = None

    return ([_norm_email(addr) for addr in addresses if addr], dt_local)


def _imap_to_set(tz: ZoneInfo, since_days: int) -> Set[Tuple[str, datetime]]:
    items: Set[Tuple[str, datetime]] = set()
    for headers in _imap_fetch_since(since_days):
        addresses, dt_local = _parse_to_date(headers, tz)
        if not dt_local:
            continue
        midnight = dt_local.replace(hour=0, minute=0, second=0, microsecond=0)
        for email in addresses:
            if email:
                items.add((email, midnight))
    return items


def reconcile_csv_vs_imap(since_days: int | None = None) -> Dict[str, object]:
    days = since_days if since_days is not None else RECONCILE_SINCE_DAYS
    tz = ZoneInfo(REPORT_TZ)

    csv_set = _load_csv_set(tz, days)
    imap_set = _imap_to_set(tz, days)

    only_csv = sorted(csv_set - imap_set, key=lambda item: (item[1], item[0]))
    only_imap = sorted(imap_set - csv_set, key=lambda item: (item[1], item[0]))

    return {
        "since_days": days,
        "csv_count": len(csv_set),
        "imap_count": len(imap_set),
        "only_csv": [(email, dt.isoformat()) for email, dt in only_csv],
        "only_imap": [(email, dt.isoformat()) for email, dt in only_imap],
    }


def build_summary_text(res: Dict[str, object]) -> str:
    days = res["since_days"]
    csv_count = res["csv_count"]
    imap_count = res["imap_count"]
    only_csv = res["only_csv"]
    only_imap = res["only_imap"]

    lines = [
        f"🔄 Сверка логов и IMAP за {days} дн.",
        f"CSV (лог): {csv_count}",
        f"IMAP (Отправленные): {imap_count}",
        f"Только в логах: {len(only_csv)}",
        f"Только в IMAP: {len(only_imap)}",
    ]

    hints: List[str] = []
    if only_csv:
        hints.append(
            "• «Только в логах» — письмо записано локально, но не найдено в IMAP (проверь доставку/копирование в Sent)."
        )
    if only_imap:
        hints.append(
            "• «Только в IMAP» — письмо есть на сервере, но не записано в sent_log.csv (ручная отправка/другой процесс/пропущен log_sent)."
        )
    if hints:
        lines.append("")
        lines.extend(hints)

    return "\n".join(lines)


def to_csv_bytes(rows: List[Tuple[str, str]], header: Tuple[str, str] = ("email", "date")) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    for email, date in rows:
        writer.writerow([email, date])
    return buf.getvalue().encode("utf-8")
