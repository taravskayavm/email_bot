import asyncio
import sys
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web

sys.path.append(str(Path(__file__).resolve().parents[1]))

import emailbot.extraction as extraction


def test_preclean_merges_hyphen_newlines_and_spaces():
    raw = "user-\nname @ example. c o m"
    assert extraction._preclean_text_for_emails(raw) == "username@example.com"


def test_preclean_keeps_common_provider_email():
    assert extraction._preclean_text_for_emails("user@gmail.com") == "user@gmail.com"


def test_extract_clean_emails_handles_variants_and_truncations():
    text = (
        "user-\nname @ example. c o m\n"
        "info@example.org\n"
        "1john@example.com 2john@example.com\n"
        "Vilena\n33 @mail. r u"
    )
    expected = {"username@example.com", "john@example.com", "vilena33@mail.ru"}
    assert extraction.extract_clean_emails_from_text(text) == expected


def test_extract_clean_emails_from_text_allows_provider_email():
    assert extraction.extract_clean_emails_from_text("Contact: user@gmail.com") == {
        "user@gmail.com"
    }


@pytest.mark.parametrize(
    "candidates,expected",
    [
        ({"33@mail.ru", "vilena33@mail.ru"}, [("33@mail.ru", "vilena33@mail.ru")]),
        ({"33@mail.ru", "anna33@mail.ru", "olga33@mail.ru"}, []),
        ({"33@mail.ru"}, []),
    ],
)
def test_detect_numeric_truncations(candidates, expected):
    assert sorted(extraction.detect_numeric_truncations(candidates)) == sorted(expected)


def test_find_prefix_repairs_detects_cases():
    raw = "M\norgachov-ilya@yandex.ru\nVilena\n33 @mail.ru"
    pairs = extraction.find_prefix_repairs(raw)
    assert set(pairs) == {
        ("orgachov-ilya@yandex.ru", "morgachov-ilya@yandex.ru"),
        ("33@mail.ru", "vilena33@mail.ru"),
    }


def test_remove_invisibles_strips_zero_width_and_nbsp():
    raw = "a\u00adb\u2011c\u200b\xa0d"
    assert extraction.remove_invisibles(raw) == "abc d"


def test_is_allowed_tld_accepts_com_and_subdomain():
    expected = "com" in extraction.ALLOWED_TLDS
    assert extraction.is_allowed_tld("user@mail.google.com") == expected
    assert extraction.is_allowed_tld("user@domain.com,") == expected
    assert extraction.is_allowed_tld("user@domain.com\u00a0") == expected


def _run_async(coro):
    return asyncio.run(coro)


async def _serve(handler):
    app = web.Application()
    app.router.add_get("/", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    url = f"http://localhost:{port}/"
    async with aiohttp.ClientSession() as session:
        result = await extraction.async_extract_emails_from_url(url, session)
    await runner.cleanup()
    return url, result


def test_async_extract_emails_from_url_success():
    async def handler(request):
        return web.Response(text="contact: test@example.com foreign@example.de")

    url, result = _run_async(_serve(handler))
    assert result[0] == url
    assert set(result[1]) == {"test@example.com"}
    assert set(result[2]) == {"foreign@example.de"}


def test_async_extract_emails_from_url_http_error(monkeypatch):
    async def handler(request):
        return web.Response(status=404)

    logged: list[str] = []
    monkeypatch.setattr(extraction, "log_error", lambda msg: logged.append(msg))

    url, result = _run_async(_serve(handler))
    assert result == (url, [], [], [])
    assert logged and "HTTP 404" in logged[0]
