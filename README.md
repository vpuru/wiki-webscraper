# WikiScraper

<img width="811" height="233" alt="Screenshot 2026-03-08 at 5 23 22 PM" src="https://github.com/user-attachments/assets/9ea5eb31-e511-45db-9d7e-5162b308ce3d" />

## Abstract
High-throughput async web scraper for English Wikipedia. Crawls pages via BFS, extracts page titles and outgoing links, and persists everything to DynamoDB. Built to handle ~7.2M pages.

## Usage

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Deploy DynamoDB table

Requires AWS credentials configured (`aws configure`) and the CDK CLI (`npm install -g aws-cdk`).

```bash
pip install ".[infra]"
export CDK_DEFAULT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_DEFAULT_REGION=$(aws configure get region)
cd infra && cdk bootstrap && cdk deploy
```

### Run the scraper

```bash
python -m wiki_scraper.main \
  --seed-url "https://en.wikipedia.org/wiki/Python_(programming_language)" \
  --max-depth 2 \
  --workers 200 \
  --concurrency 50 \
  --rate 20
```

| Flag | Default | Description |
|---|---|---|
| `--seed-url` | Main Page | Starting URL for BFS crawl |
| `--max-depth` | 100 | Maximum BFS depth |
| `--workers` | 500 | Number of async worker coroutines |
| `--concurrency` | 200 | Max concurrent in-flight requests (semaphore) |
| `--rate` | 200 | Requests per second (token bucket) |
| `--table-name` | WikiCrawlState | DynamoDB table name |
| `--user-agent` | WikiScraper/1.0 | User-Agent header |
| `--log-level` | INFO | Logging level |

### Run tests

```bash
pytest tests/ -v
```

## Architecture

```
wiki-scraper/
├── src/wiki_scraper/
│   ├── main.py        # CLI entry point
│   ├── config.py      # CrawlConfig dataclass
│   ├── crawler.py     # CrawlOrchestrator — wires components, runs BFS workers
│   ├── fetcher.py     # HttpFetcher + TokenBucket rate limiter
│   ├── frontier.py    # UrlFrontier — asyncio.Queue + BloomFilter dedup
│   ├── bloom.py       # BloomFilter — bitarray + mmh3
│   ├── parser.py      # ContentParser — lxml title + link extraction
│   ├── storage.py     # DynamoDbCrawlStore — batch writes, crash recovery
│   ├── url_utils.py   # URL normalization, namespace filtering
│   └── robots.py      # RobotsChecker — stdlib RobotFileParser
├── infra/             # CDK stack for DynamoDB table
└── tests/             # 44 unit tests
```

## Key Design Decisions

**asyncio over threads/multiprocessing** — Network I/O is the bottleneck, not CPU. Hundreds of coroutines sharing one event loop gives us high concurrency with minimal overhead. Workers are cheap — we spawn 200-500 of them and they all share a semaphore-gated connection pool.

**Workers decoupled from concurrency** — Worker count (coroutines pulling from the queue) is separate from the concurrency limit (semaphore capping in-flight requests). This ensures the queue is always being drained even when many workers are blocked waiting for their turn to fetch.

**Bloom filter for dedup** — With 7.2M URLs, a `set()` would work but a Bloom filter (bitarray + mmh3) uses ~14 MB at 0.1% false positive rate vs ~500 MB+ for a set. We accept ~7,200 false skips out of 7.2M URLs as an acceptable tradeoff.

**In-memory frontier, not DynamoDB** — The URL queue lives in an `asyncio.Queue` for O(1) enqueue/dequeue. Using DynamoDB as a live queue would require expensive GSI queries per dequeue. DynamoDB is write-once storage for completed page metadata, not a work queue.

**Inline parsing instead of ProcessPoolExecutor** — We originally used a ProcessPoolExecutor (4 workers) to keep lxml parsing off the event loop. In practice, parsing takes 1-5ms vs 100-500ms for network I/O, and the process pool crashed under high concurrency from memory pressure when many large HTML pages were serialized across process boundaries simultaneously. Inline parsing is simpler and fast enough.

**boto3 with ThreadPoolExecutor instead of aioboto3** — `aioboto3`/`aiobotocore` has compatibility issues with `moto` for testing (missing `raw_headers` on mock responses). Since DynamoDB writes aren't the bottleneck, sync `boto3` calls in a thread pool work fine and test cleanly with `moto`.

**4-layer rate limiting** — (1) `asyncio.Semaphore` caps concurrent requests, (2) `TokenBucket` smooths to N req/s, (3) `aiohttp.TCPConnector` limits TCP connections, (4) exponential backoff with jitter on 429/5xx. Wikipedia starts throttling around 20-50 req/s from a single IP.

**BFS crawl order** — Discovers hub pages early for better coverage breadth. Workers exit after 30s idle when the frontier empties.

**Crash recovery** — DynamoDB stores a `status` field per page. On restart, the orchestrator queries the `StatusIndex` GSI for `pending`/`in_progress` items and re-seeds the frontier.

**DynamoDB PK is SHA-256(url)** — Uniform partition distribution, fixed size, avoids Unicode/length issues with raw URLs as keys.


## Additional Thoughts
I built this mainly to learn/refresh on a couple topics
- parallelism / concurrency
- bloom filters
- token bucket implementations

Figured it was worth publishing as a showcase.
