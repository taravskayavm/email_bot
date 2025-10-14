"""Shared helpers for preparing and sending e-mails."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Iterable, List, Optional, Sequence

from . import messaging
from .messaging import SendOutcome, log_sent_email, was_emailed_recently
from .smtp_client import SmtpClient
from .cancel import is_cancelled


def build_send_list(
    candidates: Iterable[str],
    blocked: Sequence[str] | set[str],
    sent_today: Sequence[str] | set[str],
    *,
    lookup_days: Optional[int] = 180,
    recent_checker: Callable[[str, int], bool] | None = None,
    on_recent_skip: Optional[Callable[[str], None]] = None,
) -> List[str]:
    """Return addresses allowed for sending after basic filtering."""

    blocked_set = set(blocked)
    sent_set = set(sent_today)
    check_recent = recent_checker or was_emailed_recently
    result: List[str] = []
    for email in candidates:
        if email in blocked_set or email in sent_set:
            continue
        if lookup_days and lookup_days > 0:
            try:
                if check_recent(email, lookup_days):
                    if on_recent_skip:
                        on_recent_skip(email)
                    continue
            except Exception:
                # Fail open — assume not recently contacted on errors.
                pass
        result.append(email)
    return result


async def run_smtp_send(
    client: SmtpClient,
    to_send: List[str],
    *,
    template_path: str,
    group_code: str,
    imap,
    sent_folder: str,
    chat_id: int,
    sleep_between: float = 1.5,
    cancel_event=None,
    should_stop_cb: Optional[Callable[[], bool]] = None,
    on_sent: Optional[Callable[[str, str, str | None, str | None], None]] = None,
    on_duplicate: Optional[Callable[[str], None]] = None,
    on_cooldown: Optional[Callable[[str], None]] = None,
    on_blocked: Optional[Callable[[str], None]] = None,
    on_error: Optional[
        Callable[[str, Exception, Optional[int], Optional[str]], None]
    ] = None,
    on_unknown: Optional[Callable[[str], None]] = None,
    after_each: Optional[Callable[[str], None]] = None,
    subject: str = messaging.DEFAULT_SUBJECT,
    batch_id: str | None = None,
    override_180d: bool = False,
    on_heartbeat: Optional[Callable[[], Awaitable[None]]] = None,
) -> tuple[int, bool]:
    """Send e-mails sequentially and dispatch callbacks for outcomes."""

    sent_count = 0
    aborted = False
    if on_heartbeat:
        try:
            await on_heartbeat()
        except Exception:
            pass
    while to_send:
        if should_stop_cb and should_stop_cb():
            aborted = True
            break
        if cancel_event and getattr(cancel_event, "is_set", lambda: False)():
            aborted = True
            break
        if is_cancelled(chat_id):
            aborted = True
            break

        email_addr = to_send.pop(0)
        try:
            outcome, token, log_key, content_hash = messaging.send_email_with_sessions(
                client,
                imap,
                sent_folder,
                email_addr,
                template_path,
                subject=subject,
                batch_id=batch_id,
                override_180d=override_180d,
            )
        except messaging.TemplateRenderError:
            # Let caller handle template errors.
            to_send.insert(0, email_addr)
            raise
        except Exception as exc:  # pragma: no cover - depends on SMTP runtime
            code: Optional[int] = None
            msg: Optional[str] = None
            if (
                hasattr(exc, "recipients")
                and isinstance(exc.recipients, dict)
                and email_addr in exc.recipients
            ):
                code = exc.recipients[email_addr][0]
                msg = exc.recipients[email_addr][1]
            elif hasattr(exc, "smtp_code"):
                code = getattr(exc, "smtp_code", None)
                msg = getattr(exc, "smtp_error", None)
            if on_error:
                on_error(email_addr, exc, code, msg)
            log_sent_email(
                email_addr,
                group_code,
                "error",
                chat_id,
                template_path,
                str(exc),
            )
            if after_each:
                after_each(email_addr)
            continue

        if outcome == SendOutcome.SENT:
            log_sent_email(
                email_addr,
                group_code,
                "ok",
                chat_id,
                template_path,
                unsubscribe_token=token,
                key=log_key,
                subject=subject,
                content_hash=content_hash,
            )
            sent_count += 1
            if on_sent:
                on_sent(email_addr, token, log_key, content_hash)
            if after_each:
                after_each(email_addr)
            await asyncio.sleep(sleep_between)
        elif outcome == SendOutcome.DUPLICATE:
            if on_duplicate:
                on_duplicate(email_addr)
            if after_each:
                after_each(email_addr)
        elif outcome == SendOutcome.COOLDOWN:
            if on_cooldown:
                on_cooldown(email_addr)
            if after_each:
                after_each(email_addr)
        elif outcome == SendOutcome.BLOCKED:
            if on_blocked:
                on_blocked(email_addr)
            if after_each:
                after_each(email_addr)
        else:
            if on_unknown:
                on_unknown(email_addr)
            if after_each:
                after_each(email_addr)

    return sent_count, aborted
