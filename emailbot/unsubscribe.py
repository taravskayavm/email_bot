from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping as MappingABC
from typing import Mapping

from aiohttp import web

from utils.email_clean import parse_emails_unified

from .messaging import (
    BLOCKED_FILE,
    MarkUnsubscribedResult,
    ensure_blocklist_ready,
    mark_unsubscribed,
    verify_unsubscribe_token,
)

logger = logging.getLogger(__name__)
UNSUB_SOFT = os.getenv("UNSUBSCRIBE_ALLOW_WITHOUT_TOKEN", "1") == "1"


def _clean_single_email(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    parsed = parse_emails_unified(raw)
    if parsed:
        return parsed[0].strip().lower()
    lowered = raw.lower()
    return lowered if "@" in lowered else ""


def _ok_html(message: str = "Вы отписались от рассылки") -> str:
    return (
        "<html><head><meta charset=\"utf-8\"/></head><body>"
        f"<h3>{message}</h3>"
        "<p>Ваш адрес больше не будет получать письма.</p>"
        "</body></html>"
    )


def _form_value(data: Mapping[str, str], key: str) -> str:
    for variant in (key, key.lower(), key.upper()):
        if variant in data:
            return data[variant]
    return data.get(key.replace("-", "_"), "")


async def _extract_email_any(
    request: web.Request,
    form: Mapping[str, str] | None,
    query: Mapping[str, str],
) -> str:
    """Try to find an email address across form, query, headers and JSON body."""

    candidates: list[str] = []
    keys = ("recipient", "email", "mail", "mailto", "address")

    if form:
        for key in keys:
            value = (_form_value(form, key) or "").strip()
            if value:
                candidates.append(value)

    for key in keys:
        raw = (query.get(key) or "").strip()
        if raw:
            candidates.append(raw)

    for header in ("X-Original-Recipient", "X-Original-To", "X-Envelope-To"):
        raw = (request.headers.get(header) or "").strip()
        if raw:
            candidates.append(raw)

    if not form and request.can_read_body:
        try:
            payload = await request.text()
        except Exception:
            payload = ""
        if payload:
            try:
                data = json.loads(payload)
            except Exception:
                data = None
            if isinstance(data, MappingABC):
                for key in keys:
                    raw = (str(data.get(key) or "").strip())
                    if raw:
                        candidates.append(raw)

    for candidate in candidates:
        cleaned = _clean_single_email(candidate)
        if cleaned:
            return cleaned
    return ""


async def handle(request: web.Request) -> web.Response:
    ensure_blocklist_ready()

    if request.method == "POST":
        content_type = (request.content_type or "").lower()
        is_form = content_type.startswith("application/x-www-form-urlencoded") or content_type.startswith(
            "multipart/form-data"
        )
        data: Mapping[str, str] | None = None
        if is_form:
            try:
                data = await request.post()
            except Exception:
                logger.debug("unsubscribe POST: failed to parse form body", exc_info=True)
                data = None

        raw_marker = (data and (_form_value(data, "List-Unsubscribe") or "")) or ""
        marker = raw_marker.strip().lower() or (request.headers.get("List-Unsubscribe-Post") or "").strip().lower()

        query = request.rel_url.query
        addr = await _extract_email_any(request, data, query)

        form_email_raw = (data and (_form_value(data, "email") or "")) or ""
        token = (data and (_form_value(data, "token") or "")) or ""
        form_email = _clean_single_email(form_email_raw) or _clean_single_email(
            query.get("email", "")
        )
        token = token.strip() or (query.get("token") or "").strip()

        if token:
            email = form_email or addr
            email = _clean_single_email(email)
            if not email:
                raise web.HTTPBadRequest(text="Missing email/token")
            if not verify_unsubscribe_token(email, token):
                if UNSUB_SOFT:
                    logger.warning(
                        "unsubscribe soft-allow: email=%s token_invalid=1", email
                    )
                    result = mark_unsubscribed(email)
                    logger.info(
                        "unsubscribe POST(token-soft): email=%s csv=%s block=%s block_file=%s",
                        email,
                        result.csv_updated,
                        result.block_added,
                        BLOCKED_FILE,
                    )
                    return web.Response(text=_ok_html(), content_type="text/html")
                logger.warning("unsubscribe denied: email=%s token_invalid=1", email)
                raise web.HTTPForbidden(text="Invalid token")
            result: MarkUnsubscribedResult = mark_unsubscribed(email)
            logger.info(
                "unsubscribe POST(token): email=%s csv=%s block=%s block_file=%s",
                email,
                result.csv_updated,
                result.block_added,
                BLOCKED_FILE,
            )
            return web.Response(text=_ok_html(), content_type="text/html")

        if not addr:
            logger.warning(
                "unsubscribe POST: no address in request (marker=%r, headers=%r)",
                marker or None,
                dict(request.headers),
            )
            raise web.HTTPBadRequest(text="Missing recipient")

        result: MarkUnsubscribedResult = mark_unsubscribed(addr)
        logger.info(
            "unsubscribe POST(one-click): email=%s csv=%s block=%s block_file=%s",
            addr,
            result.csv_updated,
            result.block_added,
            BLOCKED_FILE,
        )
        return web.Response(text="OK", content_type="text/plain")

    email_raw = request.query.get("email", "")
    token = request.query.get("token", "").strip()
    email = _clean_single_email(email_raw)
    if not email:
        raise web.HTTPBadRequest(text="Missing email/token")

    if token and verify_unsubscribe_token(email, token):
        result: MarkUnsubscribedResult = mark_unsubscribed(email)
        logger.info(
            "unsubscribe GET: email=%s csv=%s block=%s block_file=%s",
            email,
            result.csv_updated,
            result.block_added,
            BLOCKED_FILE,
        )
        return web.Response(text=_ok_html(), content_type="text/html")

    if not token:
        if UNSUB_SOFT:
            logger.warning("unsubscribe soft-allow: email=%s token_missing=1", email)
            result = mark_unsubscribed(email)
            logger.info(
                "unsubscribe GET(token-soft-missing): email=%s csv=%s block=%s block_file=%s",
                email,
                result.csv_updated,
                result.block_added,
                BLOCKED_FILE,
            )
            return web.Response(text=_ok_html(), content_type="text/html")
        raise web.HTTPBadRequest(text="Missing email/token")

    if UNSUB_SOFT:
        logger.warning("unsubscribe soft-allow: email=%s token_invalid=1", email)
        result = mark_unsubscribed(email)
        logger.info(
            "unsubscribe GET(token-soft): email=%s csv=%s block=%s block_file=%s",
            email,
            result.csv_updated,
            result.block_added,
            BLOCKED_FILE,
        )
        return web.Response(text=_ok_html(), content_type="text/html")

    logger.warning("unsubscribe denied: email=%s token_invalid=1", email)
    raise web.HTTPForbidden(text="Invalid token")


async def _ping(_: web.Request) -> web.Response:
    return web.Response(text="pong", content_type="text/plain")


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/unsubscribe", handle)
    app.router.add_post("/unsubscribe", handle)
    app.router.add_get("/unsubscribe/ping", _ping)
    return app
