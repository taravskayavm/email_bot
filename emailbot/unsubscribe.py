from __future__ import annotations

import json
import logging
from collections.abc import Mapping as MappingABC
from typing import Mapping

from aiohttp import web

from .messaging import (
    BLOCKED_FILE,
    ensure_blocklist_ready,
    mark_unsubscribed,
    verify_unsubscribe_token,
)

logger = logging.getLogger(__name__)


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
        if "@" in candidate:
            return candidate
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
        header_marker = (request.headers.get("List-Unsubscribe-Post") or "").strip()
        marker = raw_marker.strip()
        marker_lower = marker.lower()
        header_marker_lower = header_marker.lower()
        normalized_marker = marker_lower or header_marker_lower

        query = request.rel_url.query
        addr = await _extract_email_any(request, data, query)

        form_email = (data and (_form_value(data, "email") or "")) or ""
        token = (data and (_form_value(data, "token") or "")) or ""
        if not form_email:
            form_email = (query.get("email") or "").strip()
        else:
            form_email = form_email.strip()
        token = token.strip() or (query.get("token") or "").strip()

        if token:
            email = form_email or addr
            if not email:
                raise web.HTTPBadRequest(text="Missing email/token")
            if not verify_unsubscribe_token(email, token):
                logger.warning("unsubscribe denied: email=%s token_invalid=1", email)
                raise web.HTTPForbidden(text="Invalid token")
            added = mark_unsubscribed(email)
            logger.info(
                "unsubscribe POST(token): email=%s added=%s block_file=%s",
                email,
                added,
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

        if "one-click" not in normalized_marker:
            logger.warning(
                "unsubscribe POST denied: missing one-click marker marker=%r header_marker=%r",
                raw_marker or None,
                header_marker or None,
            )
            raise web.HTTPForbidden(text="Invalid unsubscribe request")

        added = mark_unsubscribed(addr)
        logger.info(
            "unsubscribe POST: email=%s added=%s block_file=%s",
            addr,
            added,
            BLOCKED_FILE,
        )
        return web.Response(text="OK", content_type="text/plain")

    email = request.query.get("email", "").strip()
    token = request.query.get("token", "").strip()
    if not email or not token:
        raise web.HTTPBadRequest(text="Missing email/token")
    if not verify_unsubscribe_token(email, token):
        logger.warning("unsubscribe denied: email=%s token_invalid=1", email)
        raise web.HTTPForbidden(text="Invalid token")
    added = mark_unsubscribed(email)
    logger.info(
        "unsubscribe GET: email=%s added=%s block_file=%s",
        email,
        added,
        BLOCKED_FILE,
    )
    return web.Response(text=_ok_html(), content_type="text/html")


async def _ping(_: web.Request) -> web.Response:
    return web.Response(text="pong", content_type="text/plain")


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/unsubscribe", handle)
    app.router.add_post("/unsubscribe", handle)
    app.router.add_get("/unsubscribe/ping", _ping)
    return app
