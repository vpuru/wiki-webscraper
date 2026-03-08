import asyncio
import logging
import signal

from wiki_scraper.config import CrawlConfig
from wiki_scraper.fetcher import HttpFetcher
from wiki_scraper.frontier import UrlFrontier
from wiki_scraper.parser import ContentParser
from wiki_scraper.robots import RobotsChecker
from wiki_scraper.storage import DynamoDbCrawlStore, PageRecord
from wiki_scraper.url_utils import normalize_url

logger = logging.getLogger(__name__)


class CrawlOrchestrator:
    def __init__(self, config: CrawlConfig) -> None:
        self._config = config
        self._frontier = UrlFrontier(config.bloom_expected_items, config.bloom_fp_rate)
        self._fetcher = HttpFetcher(config)
        self._parser = ContentParser(config.parser_workers, config.allowed_host)
        self._robots = RobotsChecker(config.user_agent)
        self._store = DynamoDbCrawlStore(config.dynamo_table_name, config.max_links_per_page)
        self._shutting_down = False
        self._pages_crawled = 0

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._request_shutdown)

        logger.info("Loading robots.txt...")
        await self._robots.load(self._config.robots_url)

        # Crash recovery: re-seed frontier from incomplete DynamoDB items
        incomplete = await self._store.load_incomplete_urls()
        if incomplete:
            logger.info("Re-seeding frontier with %d incomplete URLs", len(incomplete))
            for url, depth in incomplete:
                await self._frontier.enqueue(url, depth)

        # Seed the frontier
        seed = normalize_url(self._config.seed_url)
        if seed:
            await self._frontier.enqueue(seed, depth=0)

        await self._fetcher.start()

        workers = [
            asyncio.create_task(self._worker(i))
            for i in range(self._config.max_concurrent_requests)
        ]

        await asyncio.gather(*workers)
        await self._shutdown()

    def _request_shutdown(self) -> None:
        if not self._shutting_down:
            logger.info("Shutdown requested, finishing in-flight work...")
            self._shutting_down = True

    async def _shutdown(self) -> None:
        logger.info("Flushing DynamoDB buffer...")
        await self._store.flush()
        await self._fetcher.close()
        self._parser.shutdown()
        self._store.shutdown()
        logger.info(
            "Crawl complete. Pages crawled: %d, URLs seen: %d",
            self._pages_crawled,
            self._frontier.estimated_seen(),
        )

    async def _worker(self, worker_id: int) -> None:
        while not self._shutting_down:
            try:
                item = await asyncio.wait_for(
                    self._frontier.dequeue(),
                    timeout=self._config.worker_idle_timeout,
                )
            except asyncio.TimeoutError:
                logger.info("Worker %d idle for %.0fs, exiting", worker_id, self._config.worker_idle_timeout)
                return

            try:
                await self._process_url(item.url, item.depth)
            except Exception:
                logger.exception("Worker %d error processing %s", worker_id, item.url)
            finally:
                self._frontier.task_done()

    async def _process_url(self, url: str, depth: int) -> None:
        if not self._robots.is_allowed(url):
            logger.debug("Blocked by robots.txt: %s", url)
            return

        if depth > self._config.max_depth:
            return

        html = await self._fetcher.fetch(url)
        if html is None:
            await self._store.put_page(PageRecord(
                url=url, status="failed", depth=depth, error="fetch failed",
            ))
            return

        result = await self._parser.parse(html, url)

        await self._store.put_page(PageRecord(
            url=url,
            status="completed",
            depth=depth,
            title=result.title,
            outgoing_links=result.outgoing_links,
            links_count=len(result.outgoing_links),
        ))

        self._pages_crawled += 1
        if self._pages_crawled % 100 == 0:
            logger.info(
                "Progress: %d pages crawled, %d in queue, %d total seen",
                self._pages_crawled,
                self._frontier.qsize(),
                self._frontier.estimated_seen(),
            )

        # Enqueue discovered links at next depth
        await self._frontier.enqueue_batch(result.outgoing_links, depth + 1)
