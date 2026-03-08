import asyncio
import time

import pytest
from aioresponses import aioresponses

from wiki_scraper.config import CrawlConfig
from wiki_scraper.fetcher import HttpFetcher, TokenBucket


class TestTokenBucket:
    async def test_immediate_acquire_when_full(self):
        bucket = TokenBucket(rate=10.0, capacity=10.0)
        start = time.monotonic()
        await bucket.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.05

    async def test_blocks_when_empty(self):
        bucket = TokenBucket(rate=10.0, capacity=1.0)
        await bucket.acquire()  # drain the single token
        start = time.monotonic()
        await bucket.acquire()  # must wait for refill
        elapsed = time.monotonic() - start
        assert 0.05 < elapsed < 0.3

    async def test_refills_over_time(self):
        bucket = TokenBucket(rate=100.0, capacity=5.0)
        for _ in range(5):
            await bucket.acquire()
        # All tokens drained, wait for refill
        await asyncio.sleep(0.05)
        # Should have ~5 tokens back
        start = time.monotonic()
        await bucket.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.05


class TestHttpFetcher:
    @pytest.fixture
    def config(self):
        return CrawlConfig(
            max_concurrent_requests=5,
            max_connections=10,
            requests_per_second=1000.0,  # high rate so tests aren't slow
            max_retries=2,
            backoff_base=0.01,
            backoff_max=0.05,
            backoff_jitter=0.01,
        )

    @pytest.fixture
    async def fetcher(self, config):
        f = HttpFetcher(config)
        await f.start()
        yield f
        await f.close()

    async def test_fetch_success(self, fetcher):
        with aioresponses() as m:
            m.get("https://en.wikipedia.org/wiki/Python", body="<html>OK</html>")
            result = await fetcher.fetch("https://en.wikipedia.org/wiki/Python")
            assert result == "<html>OK</html>"

    async def test_fetch_404_returns_none(self, fetcher):
        with aioresponses() as m:
            m.get("https://en.wikipedia.org/wiki/Missing", status=404)
            result = await fetcher.fetch("https://en.wikipedia.org/wiki/Missing")
            assert result is None

    async def test_fetch_429_retries(self, fetcher):
        with aioresponses() as m:
            m.get("https://en.wikipedia.org/wiki/Busy", status=429)
            m.get("https://en.wikipedia.org/wiki/Busy", body="<html>OK</html>")
            result = await fetcher.fetch("https://en.wikipedia.org/wiki/Busy")
            assert result == "<html>OK</html>"

    async def test_fetch_500_retries(self, fetcher):
        with aioresponses() as m:
            m.get("https://en.wikipedia.org/wiki/Error", status=500)
            m.get("https://en.wikipedia.org/wiki/Error", status=500)
            m.get("https://en.wikipedia.org/wiki/Error", body="<html>OK</html>")
            result = await fetcher.fetch("https://en.wikipedia.org/wiki/Error")
            assert result == "<html>OK</html>"

    async def test_fetch_exhausts_retries(self, fetcher):
        with aioresponses() as m:
            # 1 initial + 2 retries = 3 attempts, all 500
            m.get("https://en.wikipedia.org/wiki/Down", status=500)
            m.get("https://en.wikipedia.org/wiki/Down", status=500)
            m.get("https://en.wikipedia.org/wiki/Down", status=500)
            result = await fetcher.fetch("https://en.wikipedia.org/wiki/Down")
            assert result is None
