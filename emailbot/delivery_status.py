"""Match delivery failures to sends without changing send history or limits."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from collections.abc import Iterable, Mapping

from .services.cooldown import normalize_email_for_key

BOUNCE_STATUSES = {"bounce", "bounced", "soft_bounce", "soft-bounce", "hard_bounce", "hard-bounce"}
_CLOCK_TOLERANCE = timedelta(seconds=5)
_DUPLICATE_TOLERANCE = timedelta(seconds=1)


def timestamp(value) -> datetime | None:
    if isinstance(value, datetime):
        result = value
    else:
        try:
            result = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def event_time(event: Mapping) -> datetime | None:
    return timestamp(event.get("ts_utc") or event.get("ts") or event.get("last_sent_at"))


def recipient(event: Mapping) -> str:
    return normalize_email_for_key(str(event.get("recipient") or event.get("email") or ""))


def failure_time(event: Mapping) -> datetime | None:
    return timestamp(event.get("original_sent_at")) or event_time(event)


def unique_bounces(events: Iterable[Mapping]) -> list[Mapping]:
    """Several DSNs about the same original send represent one failed send."""
    result = {}
    for event in events:
        if str(event.get("status", "")).lower() not in BOUNCE_STATUSES:
            continue
        if event.get("dsn_action") in {"delivered", "relayed", "expanded"}:
            continue
        if str(event.get("dsn_status", "")).startswith("2."):
            continue
        original_time = timestamp(event.get("original_sent_at"))
        identity = (event.get("message_id") or (original_time.isoformat() if original_time else "")
                    or event.get("bounce_id") or event.get("ts") or event.get("ts_utc"))
        key = (recipient(event), str(identity))
        result[key] = event
    return list(result.values())


def failed_send_indices(sends: list[Mapping], failures: Iterable[Mapping]) -> set[int]:
    """Match by original Message-ID, or by recipient and original send time.

    Receipt time is only a fallback for old records without original metadata.
    A different known Message-ID must never cancel a later successful resend.
    """
    by_address: dict[str, list[int]] = {}
    for index, event in enumerate(sends):
        address = recipient(event)
        if address and event_time(event) is not None:
            by_address.setdefault(address, []).append(index)
    failed = set()
    for bounce in unique_bounces(failures):
        candidates = by_address.get(recipient(bounce), [])
        original_id = str(bounce.get("message_id") or "").strip()
        exact = [i for i in candidates if original_id and str(sends[i].get("message_id") or "").strip() == original_id]
        if exact:
            failed.update(exact)
            # Compatibility CSV rows can represent the same send without its ID.
            candidates = [i for i in candidates if not sends[i].get("message_id")]
            original_time = event_time(sends[exact[0]])
        else:
            candidates = [i for i in candidates if not original_id or not sends[i].get("message_id")]
            original_time = timestamp(bounce.get("original_sent_at"))
        if not candidates:
            continue
        if original_time is None:
            received = event_time(bounce)
            older = [i for i in candidates if received and event_time(sends[i]) <= received]
            if not older:
                continue
            original_time = max(event_time(sends[i]) for i in older)
        nearby = [i for i in candidates
                  if abs(event_time(sends[i]) - original_time) <= _CLOCK_TOLERANCE]
        if nearby:
            closest = min(nearby, key=lambda i: abs(event_time(sends[i]) - original_time))
            failed.update(i for i in nearby
                          if abs(event_time(sends[i]) - event_time(sends[closest])) <= _DUPLICATE_TOLERANCE)
    return failed


def successful_send_count(sends: list[Mapping], failed: set[int]) -> int:
    """Count a send once when both CSV and JSONL contain its success."""
    groups: dict[str, list[list[int]]] = {}
    for index in sorted(range(len(sends)), key=lambda i: event_time(sends[i])):
        address = recipient(sends[index]) or f"__missing_{index}"
        candidates = groups.setdefault(address, [])
        mid = sends[index].get("message_id")
        same = next((g for g in candidates if mid and any(sends[i].get("message_id") == mid for i in g)), None)
        if same is None:
            same = next((g for g in reversed(candidates)
                         if abs(event_time(sends[index]) - event_time(sends[g[-1]])) <= _DUPLICATE_TOLERANCE
                         and (not mid or not any(sends[i].get("message_id") for i in g))), None)
        if same is None:
            candidates.append([index])
        else:
            same.append(index)
    count = 0
    for candidates in groups.values():
        for group in candidates:
            identified = [i for i in group if sends[i].get("message_id")]
            if not any(i in failed for i in (identified or group)):
                count += 1
    return count
