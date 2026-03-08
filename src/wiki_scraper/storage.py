import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger(__name__)


@dataclass
class PageRecord:
    url: str
    status: str = "pending"
    depth: int = 0
    title: str = ""
    outgoing_links: list[str] = field(default_factory=list)
    links_count: int = 0
    fetched_at: str = ""
    retry_count: int = 0
    error: str = ""


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


class DynamoDbCrawlStore:
    def __init__(self, table_name: str, max_links_per_page: int = 3000) -> None:
        self._table_name = table_name
        self._max_links = max_links_per_page
        self._buffer: list[dict] = []
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._resource = boto3.resource("dynamodb")
        self._table = self._resource.Table(table_name)

    def _to_item(self, record: PageRecord) -> dict:
        links = record.outgoing_links[:self._max_links]
        return {
            "url_hash": _url_hash(record.url),
            "url": record.url,
            "status": record.status,
            "depth": record.depth,
            "title": record.title,
            "outgoing_links": links,
            "links_count": len(record.outgoing_links),
            "fetched_at": record.fetched_at or datetime.now(timezone.utc).isoformat(),
            "retry_count": record.retry_count,
            "error": record.error,
        }

    async def put_page(self, record: PageRecord) -> None:
        self._buffer.append(self._to_item(record))
        if len(self._buffer) >= 25:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return
        items = list(self._buffer)
        self._buffer.clear()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, partial(self._flush_sync, items))

    def _flush_sync(self, items: list[dict]) -> None:
        with self._table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
        logger.debug("Flushed %d items to DynamoDB", len(items))

    async def get_page(self, url: str) -> dict | None:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self._executor,
            partial(self._table.get_item, Key={"url_hash": _url_hash(url)}),
        )
        return resp.get("Item")

    async def load_incomplete_urls(self) -> list[tuple[str, int]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, self._load_incomplete_sync)

    def _load_incomplete_sync(self) -> list[tuple[str, int]]:
        results = []
        for status in ("pending", "in_progress"):
            resp = self._table.query(
                IndexName="StatusIndex",
                KeyConditionExpression=Key("status").eq(status),
            )
            for item in resp.get("Items", []):
                results.append((item["url"], item["depth"]))
            while resp.get("LastEvaluatedKey"):
                resp = self._table.query(
                    IndexName="StatusIndex",
                    KeyConditionExpression=Key("status").eq(status),
                    ExclusiveStartKey=resp["LastEvaluatedKey"],
                )
                for item in resp.get("Items", []):
                    results.append((item["url"], item["depth"]))
        logger.info("Loaded %d incomplete URLs for crash recovery", len(results))
        return results

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False)
