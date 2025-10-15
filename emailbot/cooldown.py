"""Utilities for working with the 180-day cooldown window."""

from __future__ import annotations

import csv
import logging
import sqlite3
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Tuple

from . import settings

logger = logging.getLogger(__name__)

_ZWSP_CHARS = [
    "\u200b",  # ZERO WIDTH SPACE
    "\u200c",  # ZERO WIDTH NON-JOINER
    "\u200d",  # ZERO WIDTH JOINER
    "\u200e",  # LEFT-TO-RIGHT MARK
    "\u200f",  # RIGHT-TO-LEFT MARK
    "\ufeff",  # ZERO WIDTH NO-BREAK SPACE
]
_SOFT_HYPHEN = "\u00ad"
_REMOVE_TRANSLATION = {ord(ch): None for ch in _ZWSP_CHARS + [_SOFT_HYPHEN]}

_EMAIL_HINTS = ("email", "addr")
_DATE_HINTS = ("date", "time", "sent", "ts", "created", "updated", "last")


@dataclass(frozen=True)
class CooldownHit:
    """Cooldown match for an e-mail address."""

    email: str
    last_sent: datetime
    source: str


@dataclass
class CooldownService:
    """Service that filters e-mails according to the cooldown policy."""

    csv_path: Path
    db_path: Path
    days: int
    use_csv: bool
    use_db: bool
    tz: timezone = timezone.utc

    def _normalize_dt(self, value: datetime | None) -> datetime | None:
        if not isinstance(value, datetime):
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(self.tz)

    def _load_csv(self) -> dict[str, datetime]:
        if not self.use_csv:
            return {}
        history = _load_history_from_csv(self.csv_path)
        normalized: dict[str, datetime] = {}
        for email, dt in history.items():
            fixed = self._normalize_dt(dt)
            if fixed is None:
                continue
            normalized[email] = fixed
        return normalized

    def _load_db(self) -> dict[str, datetime]:
        if not self.use_db:
            return {}
        history = _load_history_from_db(self.db_path)
        normalized: dict[str, datetime] = {}
        for email, dt in history.items():
            fixed = self._normalize_dt(dt)
            if fixed is None:
                continue
            normalized[email] = fixed
        return normalized

    def _merged_history(self) -> tuple[dict[str, datetime], dict[str, str]]:
        combined: dict[str, datetime] = {}
        sources: dict[str, str] = {}
        if self.use_csv:
            csv_map = self._load_csv()
            for email, dt in csv_map.items():
                combined[email] = dt
                sources[email] = "csv"
        if self.use_db:
            db_map = self._load_db()
            for email, dt in db_map.items():
                current = combined.get(email)
                if current is None or dt > current:
                    combined[email] = dt
                    sources[email] = "db"
                elif current == dt and sources.get(email) != "db":
                    sources[email] = "db"
        return combined, sources

    def filter_ready(
        self, emails: Iterable[str], *, now: datetime | None = None
    ) -> tuple[list[str], list[CooldownHit]]:
        window = timedelta(days=max(0, self.days))
        if now is None:
            now = datetime.now(self.tz)
        elif now.tzinfo is None:
            now = now.replace(tzinfo=self.tz)
        else:
            now = now.astimezone(self.tz)

        history, sources = self._merged_history()

        ready: list[str] = []
        hits: list[CooldownHit] = []
        seen: set[str] = set()

        for email in emails:
            norm = normalize_email(email)
            if not norm or norm in seen:
                continue
            seen.add(norm)

            last = history.get(norm)
            if not last or window == timedelta(0):
                ready.append(email)
                continue

            last_checked = last.astimezone(self.tz)
            delta = now - last_checked
            if delta >= window:
                ready.append(email)
                continue

            source = sources.get(norm, "")
            hits.append(
                CooldownHit(email=email, last_sent=last_checked, source=source or "")
            )

        return ready, hits


def normalize_email(email: str) -> str:
    """Return a canonical representation of ``email`` for cooldown lookups."""

    if not email:
        return ""

    value = unicodedata.normalize("NFKC", str(email)).translate(_REMOVE_TRANSLATION)
    value = value.strip()
    if not value:
        return ""

    value = value.replace("\n", " ")
    value = value.replace("\r", " ")
    # Normalise whitespace around the ``@`` sign.
    parts = value.split("@")
    if len(parts) >= 2:
        local = parts[0].strip()
        domain = "@".join(parts[1:]).strip()
    else:
        return value.lower()

    local = "".join(local.split())
    domain = "".join(domain.split())
    try:
        domain_idna = domain.encode("idna").decode("ascii")
    except Exception:
        domain_idna = domain
    if not domain_idna:
        return local.lower()
    if not local:
        return f"@{domain_idna.lower()}"
    return f"{local.lower()}@{domain_idna.lower()}"


def _parse_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        try:
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text)
        except Exception:
            try:
                dt = parsedate_to_datetime(text)
            except Exception:
                try:
                    ts = float(text)
                except Exception:
                    return None
                try:
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                except Exception:
                    return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _select_columns(columns) -> Tuple[str | None, str | None]:
    email_candidates: list[str] = []
    date_candidates: list[str] = []
    for col in columns:
        name = col[1] if isinstance(col, tuple) else col["name"]
        if not name:
            continue
        lowered = str(name).lower()
        if any(hint in lowered for hint in _EMAIL_HINTS):
            email_candidates.append(name)
        if any(hint in lowered for hint in _DATE_HINTS):
            date_candidates.append(name)
    email_col = email_candidates[0] if email_candidates else None
    date_col = date_candidates[0] if date_candidates else None
    return email_col, date_col


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _load_history_from_db(path) -> dict[str, datetime]:
    result: dict[str, datetime] = {}
    db_path = Path(path)
    if not db_path.exists():
        return result
    try:
        conn = sqlite3.connect(db_path)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("history db connect failed: %s", exc)
        return result
    conn.row_factory = sqlite3.Row
    try:
        tables = [
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("history db list tables failed: %s", exc)
        conn.close()
        return result

    for table in tables:
        try:
            columns = conn.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
        except Exception:
            continue
        email_col, date_col = _select_columns(columns)
        if not email_col or not date_col:
            continue
        email_q = _quote_identifier(email_col)
        date_q = _quote_identifier(date_col)
        table_q = _quote_identifier(table)
        query = (
            f"SELECT {email_q} AS email, MAX({date_q}) AS last"
            f" FROM {table_q} WHERE {email_q} IS NOT NULL"
            f" GROUP BY {email_q}"
        )
        try:
            rows = conn.execute(query)
        except Exception:
            continue
        count = 0
        for row in rows:
            email_raw = row["email"] if isinstance(row, sqlite3.Row) else row[0]
            last_raw = row["last"] if isinstance(row, sqlite3.Row) else row[1]
            norm = normalize_email(email_raw)
            if not norm:
                continue
            dt = _parse_datetime(last_raw)
            if not dt:
                continue
            current = result.get(norm)
            if current is None or dt > current:
                result[norm] = dt
            count += 1
        if count:
            break
    conn.close()
    return result


def _load_history_from_csv(path) -> dict[str, datetime]:
    result: dict[str, datetime] = {}
    csv_path = Path(path)
    if not csv_path.exists():
        return result
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as fh:
            sample = fh.read(4096)
            fh.seek(0)
            has_header = csv.Sniffer().has_header(sample)
            fh.seek(0)
            if has_header:
                reader = csv.DictReader(fh)
                header = reader.fieldnames or []
                email_col = next(
                    (name for name in header if name and any(h in name.lower() for h in _EMAIL_HINTS)),
                    None,
                )
                date_col = next(
                    (name for name in header if name and any(h in name.lower() for h in _DATE_HINTS)),
                    None,
                )
                if not email_col or not date_col:
                    return result
                for row in reader:
                    email_raw = row.get(email_col)
                    last_raw = row.get(date_col)
                    norm = normalize_email(email_raw)
                    if not norm:
                        continue
                    dt = _parse_datetime(last_raw)
                    if not dt:
                        continue
                    current = result.get(norm)
                    if current is None or dt > current:
                        result[norm] = dt
            else:
                reader = csv.reader(fh)
                for row in reader:
                    if len(row) < 2:
                        continue
                    email_raw, last_raw = row[0], row[1]
                    norm = normalize_email(email_raw)
                    if not norm:
                        continue
                    dt = _parse_datetime(last_raw)
                    if not dt:
                        continue
                    current = result.get(norm)
                    if current is None or dt > current:
                        result[norm] = dt
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("history csv load failed: %s", exc)
        return result
    return result


def _merged_history_map() -> dict[str, datetime]:
    service = build_cooldown_service(settings)
    history, _ = service._merged_history()
    return history


def is_under_cooldown(
    email: str,
    *,
    days: int,
    now: datetime | None = None,
    _cache: dict[str, datetime] | None = None,
) -> tuple[bool, datetime | None]:
    if not email or days <= 0:
        return False, None
    norm = normalize_email(email)
    if not norm:
        return False, None
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    cache = _cache or _merged_history_map()
    last = cache.get(norm)
    if not last:
        return False, None
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    window = timedelta(days=max(days, 0))
    if now - last < window:
        return True, last
    return False, last


def audit_emails(
    emails: Iterable[str],
    *,
    days: int,
    now: datetime | None = None,
) -> dict[str, dict[str, datetime] | set[str]]:
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)

    if days <= 0:
        ready_norms = {
            normalize_email(email)
            for email in emails
            if normalize_email(email)
        }
        return {"ready": ready_norms, "under": set(), "last_contact": {}}

    normalized_order: list[str] = []
    display_map: dict[str, str] = {}
    for email in emails:
        norm = normalize_email(email)
        if not norm:
            continue
        if norm not in display_map:
            display_map[norm] = str(email)
            normalized_order.append(norm)

    cache = _merged_history_map()
    ready: set[str] = set()
    under: set[str] = set()
    last_contact: dict[str, datetime] = {}
    for norm in normalized_order:
        skip, last = is_under_cooldown(norm, days=days, now=now, _cache=cache)
        if skip:
            under.add(norm)
        else:
            ready.add(norm)
        if last:
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            last_contact[norm] = last
    return {"ready": ready, "under": under, "last_contact": last_contact}


def build_cooldown_service(config=settings) -> CooldownService:
    """Create a :class:`CooldownService` instance based on ``config``."""

    sources_raw = getattr(config, "COOLDOWN_SOURCES", ("csv", "db"))
    if isinstance(sources_raw, str):
        sources = tuple(
            part.strip().lower() for part in sources_raw.split(",") if part.strip()
        )
    else:
        sources = tuple(
            str(part).strip().lower()
            for part in sources_raw
            if str(part).strip()
        )
    if not sources:
        sources = ("csv", "db")

    use_csv = "csv" in sources
    use_db = "db" in sources

    csv_path = Path(getattr(config, "SENT_LOG_PATH", settings.SENT_LOG_PATH))
    db_path = Path(getattr(config, "HISTORY_DB", settings.HISTORY_DB))
    raw_days = getattr(config, "SEND_COOLDOWN_DAYS", settings.SEND_COOLDOWN_DAYS)
    try:
        days = int(raw_days)
    except Exception:
        days = settings.SEND_COOLDOWN_DAYS

    return CooldownService(
        csv_path=csv_path,
        db_path=db_path,
        days=days,
        use_csv=use_csv,
        use_db=use_db,
    )


__all__ = [
    "CooldownHit",
    "CooldownService",
    "audit_emails",
    "is_under_cooldown",
    "normalize_email",
    "_load_history_from_csv",
    "_load_history_from_db",
    "_merged_history_map",
    "build_cooldown_service",
]
