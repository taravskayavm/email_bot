from __future__ import annotations

import logging
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


async def handle(request: web.Request) -> web.Response:
    ensure_blocklist_ready()

    if request.method == "POST":
        data = await request.post()
        raw_marker = (_form_value(data, "List-Unsubscribe") or "").strip()
        marker = raw_marker.lower()
        if marker == "one-click":
            email = (
                _form_value(data, "recipient")
                or _form_value(data, "mailto")
                or _form_value(data, "email")
            ).strip()
            if not email:
                raise web.HTTPBadRequest(text="Missing recipient")
            token = (
                _form_value(data, "token")
                or request.query.get("token", "")
            ).strip()
            if not token:
                raise web.HTTPBadRequest(text="Missing token")
            if not verify_unsubscribe_token(email, token):
                logger.warning(
                    "unsubscribe denied(one-click): email=%s token_invalid=1",
                    email,
                )
                raise web.HTTPForbidden(text="Invalid token")
            added = mark_unsubscribed(email)
            logger.info(
                "unsubscribe POST(one-click): email=%s added=%s block_file=%s",
                email,
                added,
                BLOCKED_FILE,
            )
            return web.Response(text="OK", content_type="text/plain")

        logger.warning("unsubscribe POST ignored: marker=%r", raw_marker or None)

        email = data.get("email", "").strip()
        token = data.get("token", "").strip()
        if not email or not token:
            raise web.HTTPBadRequest(text="Missing email/token")
        if not verify_unsubscribe_token(email, token):
            logger.warning("unsubscribe denied: email=%s token_invalid=1", email)
            raise web.HTTPForbidden(text="Invalid token")
        added = mark_unsubscribed(email)
        logger.info(
            "unsubscribe POST(form): email=%s added=%s block_file=%s",
            email,
            added,
            BLOCKED_FILE,
        )
        return web.Response(text=_ok_html(), content_type="text/html")

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


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/unsubscribe", handle)
    app.router.add_post("/unsubscribe", handle)
    return app
