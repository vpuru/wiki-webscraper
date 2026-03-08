import asyncio
from dataclasses import dataclass

from wiki_scraper.bloom import BloomFilter


@dataclass
class FrontierItem:
    url: str
    depth: int


class UrlFrontier:
    def __init__(self, expected_items: int, fp_rate: float) -> None:
        self._queue: asyncio.Queue[FrontierItem] = asyncio.Queue()
        self._bloom = BloomFilter(expected_items, fp_rate)

    async def enqueue(self, url: str, depth: int) -> bool:
        if url in self._bloom:
            return False
        self._bloom.add(url)
        await self._queue.put(FrontierItem(url=url, depth=depth))
        return True

    async def dequeue(self) -> FrontierItem:
        return await self._queue.get()

    async def enqueue_batch(self, urls: list[str], depth: int) -> int:
        added = 0
        for url in urls:
            if await self.enqueue(url, depth):
                added += 1
        return added

    def qsize(self) -> int:
        return self._queue.qsize()

    def task_done(self) -> None:
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()

    def estimated_seen(self) -> int:
        return self._bloom.estimated_count()
