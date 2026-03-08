import boto3
import pytest
from moto import mock_aws

from wiki_scraper.storage import DynamoDbCrawlStore, PageRecord

TABLE_NAME = "WikiCrawlState"


@pytest.fixture
def aws_setup(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")

    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[{"AttributeName": "url_hash", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "url_hash", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
                {"AttributeName": "fetched_at", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "StatusIndex",
                    "KeySchema": [
                        {"AttributeName": "status", "KeyType": "HASH"},
                        {"AttributeName": "fetched_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield


async def test_put_and_get(aws_setup):
    store = DynamoDbCrawlStore(TABLE_NAME)
    record = PageRecord(
        url="https://en.wikipedia.org/wiki/Python",
        status="completed",
        depth=1,
        title="Python (programming language)",
        outgoing_links=["https://en.wikipedia.org/wiki/Java"],
        links_count=1,
        fetched_at="2026-01-01T00:00:00+00:00",
    )
    await store.put_page(record)
    await store.flush()

    item = await store.get_page("https://en.wikipedia.org/wiki/Python")
    assert item is not None
    assert item["title"] == "Python (programming language)"
    assert item["status"] == "completed"
    assert item["depth"] == 1
    assert len(item["outgoing_links"]) == 1


async def test_auto_flush_at_25(aws_setup):
    store = DynamoDbCrawlStore(TABLE_NAME)
    for i in range(25):
        record = PageRecord(
            url=f"https://en.wikipedia.org/wiki/Page_{i}",
            status="completed",
            fetched_at=f"2026-01-01T00:00:{i:02d}+00:00",
        )
        await store.put_page(record)

    item = await store.get_page("https://en.wikipedia.org/wiki/Page_0")
    assert item is not None


async def test_load_incomplete_urls(aws_setup):
    store = DynamoDbCrawlStore(TABLE_NAME)
    for i, status in enumerate(["pending", "in_progress", "completed"]):
        record = PageRecord(
            url=f"https://en.wikipedia.org/wiki/{status}_{i}",
            status=status,
            depth=i,
            fetched_at=f"2026-01-01T00:00:{i:02d}+00:00",
        )
        await store.put_page(record)
    await store.flush()

    incomplete = await store.load_incomplete_urls()
    urls = [url for url, _ in incomplete]
    assert "https://en.wikipedia.org/wiki/pending_0" in urls
    assert "https://en.wikipedia.org/wiki/in_progress_1" in urls
    assert "https://en.wikipedia.org/wiki/completed_2" not in urls


async def test_truncates_links(aws_setup):
    store = DynamoDbCrawlStore(TABLE_NAME, max_links_per_page=5)
    record = PageRecord(
        url="https://en.wikipedia.org/wiki/Big",
        status="completed",
        outgoing_links=[f"https://en.wikipedia.org/wiki/Link_{i}" for i in range(100)],
        links_count=100,
        fetched_at="2026-01-01T00:00:00+00:00",
    )
    await store.put_page(record)
    await store.flush()

    item = await store.get_page("https://en.wikipedia.org/wiki/Big")
    assert item is not None
    assert len(item["outgoing_links"]) == 5
    assert item["links_count"] == 100
