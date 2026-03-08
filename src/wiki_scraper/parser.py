import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

from wiki_scraper.url_utils import normalize_url, is_allowed_host, is_wiki_article

logger = logging.getLogger(__name__)


@dataclass
class ParseResult:
    title: str
    outgoing_links: list[str]


def _parse_html(html: str, base_url: str, allowed_host: str) -> ParseResult:
    import lxml.html

    doc = lxml.html.fromstring(html)

    heading = doc.xpath('//h1[@id="firstHeading"]')
    title = heading[0].text_content().strip() if heading else ""

    raw_hrefs = doc.xpath('//div[@id="mw-content-text"]//a/@href')

    links = []
    seen = set()
    for href in raw_hrefs:
        normalized = normalize_url(href, base_url=base_url)
        if normalized is None:
            continue
        if not is_allowed_host(normalized, allowed_host):
            continue
        if not is_wiki_article(normalized):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        links.append(normalized)

    return ParseResult(title=title, outgoing_links=links)


class ContentParser:
    def __init__(self, max_workers: int, allowed_host: str) -> None:
        self._pool = ProcessPoolExecutor(max_workers=max_workers)
        self._allowed_host = allowed_host

    async def parse(self, html: str, url: str) -> ParseResult:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._pool, _parse_html, html, url, self._allowed_host
        )

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False)
