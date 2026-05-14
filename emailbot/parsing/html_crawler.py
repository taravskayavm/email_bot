import asyncio
import re
from urllib.parse import urljoin, urldefrag, urlparse

import aiohttp
from bs4 import BeautifulSoup


class Crawler:
    def __init__(self, base_url: str, max_pages: int = 200, same_host_only: bool = True, delay_ms: int = 150):
        self.base_url = base_url
        self.max_pages = max_pages
        self.same_host_only = same_host_only
        self.delay_ms = delay_ms
        self._seen: set[str] = set()
        self._host = urlparse(base_url).netloc

    def _norm(self, url: str) -> str:
        u = urldefrag(url)[0]
        # убрать якоря/дубли слэшей
        u = re.sub(r"/{2,}", "/", u.replace("://", "§§")).replace("§§", "://")
        return u

    def _allow(self, url: str) -> bool:
        if self.same_host_only:
            return urlparse(url).netloc == self._host
        return True

    async def crawl(self):
        q: asyncio.Queue[str] = asyncio.Queue()
        await q.put(self.base_url)
        async with aiohttp.ClientSession() as session:
            pages = 0
            while not q.empty() and pages < self.max_pages:
                url = await q.get()
                url = self._norm(url)
                if url in self._seen or not self._allow(url):
                    continue
                self._seen.add(url)
                try:
                    async with session.get(url, timeout=15) as resp:
                        ct = resp.headers.get("content-type", "")
                        if "text/html" not in ct:
                            continue
                        html = await resp.text(errors="ignore")
                        yield url, html
                        pages += 1
                        # парсим ссылки
                        soup = BeautifulSoup(html, "html.parser")
                        for a in soup.find_all("a", href=True):
                            nxt = urljoin(url, a["href"])
                            if nxt not in self._seen and self._allow(nxt):
                                await q.put(nxt)
                except Exception:
                    pass
                await asyncio.sleep(self.delay_ms / 1000)
