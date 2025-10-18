from __future__ import annotations

from aiohttp import web

from .messaging import verify_unsubscribe_token, mark_unsubscribed
from .suppress_list import init_blocked, add_blocked


async def handle(request: web.Request) -> web.Response:
    email = request.query.get("email", "")
    token = request.query.get("token", "")
    if request.method == "POST":
        data = await request.post()
        email = data.get("email", "")
        token = data.get("token", "")
        if verify_unsubscribe_token(email, token):
            mark_unsubscribed(email, token)
            try:
                init_blocked()
                add_blocked([email], reason="unsubscribe")
            except Exception:
                pass
            html = """<html><head><meta charset=\"utf-8\"/></head><body>
            <h3>Вы отписаны от рассылки</h3>
            <p>Спасибо! Мы больше не будем присылать письма на ваш адрес.</p>
            <p>Если вы передумаете — просто напишите нам.</p>
            <p>Вопросы: <a href='mailto:med@lanbook.ru'>med@lanbook.ru</a></p>
            </body></html>"""
            return web.Response(text=html, content_type="text/html")
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
