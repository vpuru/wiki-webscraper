import asyncio

import pytest

from wiki_scraper.frontier import UrlFrontier


@pytest.fixture
def frontier():
    return UrlFrontier(expected_items=1000, fp_rate=0.01)


async def test_enqueue_dequeue_ordering(frontier: UrlFrontier):
    await frontier.enqueue("https://en.wikipedia.org/wiki/A", depth=0)
    await frontier.enqueue("https://en.wikipedia.org/wiki/B", depth=0)
    await frontier.enqueue("https://en.wikipedia.org/wiki/C", depth=1)

    item = await frontier.dequeue()
    assert item.url == "https://en.wikipedia.org/wiki/A"
    assert item.depth == 0

    item = await frontier.dequeue()
    assert item.url == "https://en.wikipedia.org/wiki/B"


async def test_dedup_rejects_duplicates(frontier: UrlFrontier):
    assert await frontier.enqueue("https://en.wikipedia.org/wiki/A", depth=0)
    assert not await frontier.enqueue("https://en.wikipedia.org/wiki/A", depth=1)
    assert frontier.qsize() == 1


async def test_enqueue_batch(frontier: UrlFrontier):
    urls = [
        "https://en.wikipedia.org/wiki/A",
        "https://en.wikipedia.org/wiki/B",
        "https://en.wikipedia.org/wiki/A",  # duplicate
        "https://en.wikipedia.org/wiki/C",
    ]
    added = await frontier.enqueue_batch(urls, depth=1)
    assert added == 3
    assert frontier.qsize() == 3


async def test_estimated_seen(frontier: UrlFrontier):
    await frontier.enqueue("https://en.wikipedia.org/wiki/A", depth=0)
    await frontier.enqueue("https://en.wikipedia.org/wiki/B", depth=0)
    await frontier.enqueue("https://en.wikipedia.org/wiki/A", depth=0)  # dup
    assert frontier.estimated_seen() == 2


async def test_dequeue_blocks_until_item(frontier: UrlFrontier):
    result = []

    async def consumer():
        item = await frontier.dequeue()
        result.append(item.url)

    async def producer():
        await asyncio.sleep(0.05)
        await frontier.enqueue("https://en.wikipedia.org/wiki/Late", depth=0)

    await asyncio.gather(consumer(), producer())
    assert result == ["https://en.wikipedia.org/wiki/Late"]


async def test_join_completes_after_task_done(frontier: UrlFrontier):
    await frontier.enqueue("https://en.wikipedia.org/wiki/A", depth=0)
    await frontier.enqueue("https://en.wikipedia.org/wiki/B", depth=0)

    joined = False

    async def worker():
        nonlocal joined
        while frontier.qsize() > 0:
            await frontier.dequeue()
            frontier.task_done()
        joined = True

    await worker()
    await asyncio.wait_for(frontier.join(), timeout=1.0)
    assert joined


async def test_join_blocks_without_task_done(frontier: UrlFrontier):
    await frontier.enqueue("https://en.wikipedia.org/wiki/A", depth=0)
    await frontier.dequeue()
    # No task_done() called — join should not complete
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(frontier.join(), timeout=0.1)
