"""Utility to capture offline HTML snapshots of dynamic sites."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from urllib.parse import quote, urlparse

from playwright.async_api import async_playwright


SITES = [
    "https://biathlonrus.com/union/region/",
    "https://alpfederation.ru/search/club/#JTdCJTdE",
    "https://russwimming.ru/federations/",
    "https://rusclimbing.ru/regions/",
]


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "tests/fixtures/sites"


def _safe_name(url: str) -> str:
    parsed = urlparse(url)
    fragment = f"_{quote(parsed.fragment)}" if parsed.fragment else ""
    path = f"{parsed.netloc}{parsed.path}{fragment}".strip("/")
    return path.replace("/", "_") or "index"


async def snapshot(url: str, page) -> None:
    await page.goto(url, wait_until="networkidle", timeout=90_000)
    await page.wait_for_timeout(1_200)
    html = await page.content()
    name = _safe_name(url)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / f"{name}.html").write_text(html, encoding="utf-8", errors="ignore")
    meta = {"url": url}
    (OUT_DIR / f"{name}.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    rel = OUT_DIR.relative_to(ROOT)
    print(f"[ok] {url} -> {rel}/{name}.html")


async def main() -> None:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(locale="ru-RU", user_agent="email-bot QA")
        page = await context.new_page()
        for url in SITES:
            try:
                await snapshot(url, page)
            except Exception as exc:  # pragma: no cover - diagnostic output
                print(f"[warn] {url}: {exc}")
        await context.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
