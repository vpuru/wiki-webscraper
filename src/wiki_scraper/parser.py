import logging
from dataclasses import dataclass

import lxml.html

from wiki_scraper.url_utils import normalize_url, is_allowed_host, is_wiki_article

logger = logging.getLogger(__name__)


@dataclass
class ParseResult:
    title: str
    outgoing_links: list[str]


class ContentParser:
    def __init__(self, allowed_host: str) -> None:
        self._allowed_host = allowed_host

    def parse(self, html: str, url: str) -> ParseResult:
        doc = lxml.html.fromstring(html)

        heading = doc.xpath('//h1[@id="firstHeading"]')
        title = heading[0].text_content().strip() if heading else ""

        raw_hrefs = doc.xpath('//div[@id="mw-content-text"]//a/@href')

        links = []
        seen = set()
        for href in raw_hrefs:
            normalized = normalize_url(href, base_url=url)
            if normalized is None:
                continue
            if not is_allowed_host(normalized, self._allowed_host):
                continue
            if not is_wiki_article(normalized):
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            links.append(normalized)

        return ParseResult(title=title, outgoing_links=links)

    def shutdown(self) -> None:
        pass
