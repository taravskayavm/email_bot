from __future__ import annotations

import logging

from aiohttp import web

from .messaging import (
    BLOCKED_FILE,
    verify_unsubscribe_token,
    mark_unsubscribed,
    ensure_blocklist_ready,
)

logger = logging.getLogger(__name__)


async def handle(request: web.Request) -> web.Response:
    ensure_blocklist_ready()
    email = request.query.get("email", "")
    token = request.query.get("token", "")
    if request.method == "POST":
        data = await request.post()
        email = data.get("email", "")
        token = data.get("token", "")
        if verify_unsubscribe_token(email, token):
            added = mark_unsubscribed(email, token)
            logger.info(
                "unsubscribe: email=%s added=%s block_file=%s",
                email,
                added,
                BLOCKED_FILE,
            )
            html = """<html><head><meta charset=\"utf-8\"/></head><body>
            <h3>Вы отписались от рассылки</h3>
            <p>Ваш адрес больше не будет получать письма.</p>
            <p>Если вы передумаете — просто напишите нам.</p>
            <p>Вопросы: <a href='mailto:med@lanbook.ru'>med@lanbook.ru</a></p>
            </body></html>"""
            return web.Response(text=html, content_type="text/html")
        logger.warning("unsubscribe denied: email=%s token_invalid=1", email)
        return web.Response(
            text="Если хотите отписаться — ответьте Unsubscribe или свяжитесь по med@lanbook.ru",
            content_type="text/html",
        )
    if verify_unsubscribe_token(email, token):
        html = f"""<html><body style='font-family:Arial,sans-serif;'>
<p>Нажмите кнопку, чтобы подтвердить отписку.</p>
<form method='post'>
<input type='hidden' name='email' value='{email}'>
<input type='hidden' name='token' value='{token}'>
<button type='submit'>Подтвердить отписку</button>
</form>
<p>Вопросы: <a href='mailto:med@lanbook.ru'>med@lanbook.ru</a></p>
</body></html>"""
        return web.Response(text=html, content_type="text/html")
    return web.Response(
        text="Если хотите отписаться — ответьте Unsubscribe или свяжитесь по med@lanbook.ru",
        content_type="text/html",
    )


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/unsubscribe", handle)
    app.router.add_post("/unsubscribe", handle)
    return app
