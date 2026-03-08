import asyncio
import logging
import random
import time

import aiohttp

from wiki_scraper.config import CrawlConfig

logger = logging.getLogger(__name__)


class TokenBucket:
    def __init__(self, rate: float, capacity: float) -> None:
        self._rate = rate
        self._capacity = capacity
        self._tokens = capacity
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    async def acquire(self) -> None:
        while True:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            wait = (1.0 - self._tokens) / self._rate
            await asyncio.sleep(wait)


class HttpFetcher:
    def __init__(self, config: CrawlConfig) -> None:
        self._config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrent_requests)
        self._bucket = TokenBucket(config.requests_per_second, config.requests_per_second)
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        connector = aiohttp.TCPConnector(
            limit=self._config.max_connections,
            limit_per_host=self._config.max_concurrent_requests,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": self._config.user_agent},
        )

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def fetch(self, url: str) -> str | None:
        assert self._session is not None, "Call start() before fetch()"

        for attempt in range(self._config.max_retries + 1):
            try:
                async with self._semaphore:
                    await self._bucket.acquire()
                    async with self._session.get(url) as resp:
                        if resp.status == 200:
                            return await resp.text()
                        if resp.status == 429:
                            delay = self._backoff_delay(attempt)
                            logger.warning("429 on %s, backing off %.1fs", url, delay)
                            await asyncio.sleep(delay)
                            continue
                        if resp.status >= 500:
                            delay = self._backoff_delay(attempt)
                            logger.warning("%d on %s, backing off %.1fs", resp.status, url, delay)
                            await asyncio.sleep(delay)
                            continue
                        logger.warning("Skipping %s: HTTP %d", url, resp.status)
                        return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                delay = self._backoff_delay(attempt)
                logger.warning("Error fetching %s: %s, backing off %.1fs", url, e, delay)
                await asyncio.sleep(delay)

        logger.error("Failed after %d retries: %s", self._config.max_retries, url)
        return None

    def _backoff_delay(self, attempt: int) -> float:
        delay = min(
            self._config.backoff_base * (2 ** attempt),
            self._config.backoff_max,
        )
        delay += random.uniform(0, self._config.backoff_jitter)
        return delay
