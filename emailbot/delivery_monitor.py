"""Associate asynchronous delivery failures with a finished mailing."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

from utils import send_stats
from utils.bounce import sync_bounces
from .notify import notify
from .services.cooldown import normalize_email_for_key

logger = logging.getLogger(__name__)
_WATCH_TASKS: set[asyncio.Task] = set()
_BOUNCE_CACHE_SIGNATURE = None
_BOUNCE_CACHE: dict[str, list[dict]] = {}


def _cached_bounces():
    global _BOUNCE_CACHE_SIGNATURE, _BOUNCE_CACHE

    path = send_stats._stats_path()
    try:
        stat = path.stat()
    except FileNotFoundError:
        return {}
    signature = (str(path), stat.st_mtime_ns, stat.st_size)
    if signature != _BOUNCE_CACHE_SIGNATURE:
        rows: dict[str, list[dict]] = {}
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                    if row.get("status") != "bounce":
                        continue
                    address = normalize_email_for_key(str(row.get("recipient") or row.get("email", "")))
                    rows.setdefault(address, []).append(row)
                except (ValueError, TypeError, AttributeError):
                    continue
        _BOUNCE_CACHE = rows
        _BOUNCE_CACHE_SIGNATURE = signature
    return _BOUNCE_CACHE


def batch_bounces(recipients, started_at: datetime) -> dict[str, dict]:
    recipients = {normalize_email_for_key(str(a)): str(a).strip().lower() for a in recipients}
    cutoff = started_at - timedelta(seconds=5)
    result = {}
    failures = _cached_bounces()
    for address, original in recipients.items():
        for row in failures.get(address, []):
            try:
                timestamp = row.get("original_sent_at") or row.get("ts") or ""
                ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    result[original] = row
            except (ValueError, TypeError, AttributeError):
                continue
    return result


async def check_batch_delivery(recipients, started_at):
    warning = ""
    try:
        await asyncio.to_thread(sync_bounces)
    except Exception:
        logger.exception("Post-send delivery check failed")
        warning = "⚠️ Проверка возвратов недоступна. Доставка писем не подтверждена."
    return batch_bounces(recipients, started_at), warning


def corrected_summary(text, sent, undeliverable):
    text = re.sub(r"(?m)^✅ Отправлено: \d+$", f"✅ Отправлено: {sent}", text)
    if re.search(r"(?m)^📬 Недоставляемые: \d+$", text):
        return re.sub(r"(?m)^📬 Недоставляемые: \d+$", f"📬 Недоставляемые: {undeliverable}", text)
    return re.sub(r"(?m)^(✅ Отправлено: \d+)$", rf"\1\n📬 Недоставляемые: {undeliverable}", text)


async def _watch(message, recipients, started_at, known, audit_writer, *,
                 summary_message=None, summary_text="", sent_count=None, undeliverable_count=0):
    interval = max(10, int(os.getenv("BOUNCE_CHECK_INTERVAL_SECONDS", "60")))
    duration = max(0, int(os.getenv("BOUNCE_WATCH_SECONDS", "604800")))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + duration
    initial_known = set(known)
    if sent_count is None:
        sent_count = len(set(recipients) - initial_known)
    edit_pending = False
    while loop.time() + interval <= deadline:
        await asyncio.sleep(interval)
        rows = batch_bounces(recipients, started_at)
        new = {a: row for a, row in rows.items() if a not in known}
        if new and audit_writer:
            for address, row in new.items():
                audit_writer.log_error(address, "delivery_bounce", row)
        known.update(new)
        late_count = len(known - initial_known)
        remaining = max(0, sent_count - late_count)
        undeliverable = undeliverable_count + late_count
        edit_pending = edit_pending or bool(new)
        if edit_pending and summary_message is not None and summary_text:
            # notify() can split long reports; edit only its first message.
            from .notify import _split_for_telegram, MAX_TG

            first = summary_text if len(summary_text) <= MAX_TG else _split_for_telegram(summary_text)[0]
            try:
                await summary_message.edit_text(corrected_summary(first, remaining, undeliverable))
                edit_pending = False
            except Exception:
                logger.warning("Unable to update delivery summary; will retry", exc_info=True)
        if new:
            details = "\n".join(f"• {a}: {r.get('reason', '')[:300]}" for a, r in new.items())
            try:
                await notify(message,
                             f"📬 Получены поздние возвраты: {len(new)}.\n"
                             f"✅ Отправлено с учётом возвратов: {remaining}\n"
                             f"📬 Недоставляемые: {undeliverable}\n{details}", event="error")
            except Exception:
                logger.warning("Unable to send delivery notice; continuing summary updates", exc_info=True)


def start_delivery_watch(application, message, recipients, started_at, known, audit_writer=None, **summary):
    if application is None or not recipients:
        return
    task = asyncio.create_task(_watch(message, list(recipients), started_at, set(known), audit_writer, **summary),
                               name="delivery-bounce-watch")
    _WATCH_TASKS.add(task)

    def done(completed):
        _WATCH_TASKS.discard(completed)
        if not completed.cancelled() and completed.exception():
            error = completed.exception()
            logger.error("Delivery watch failed", exc_info=(type(error), error, error.__traceback__))

    task.add_done_callback(done)
