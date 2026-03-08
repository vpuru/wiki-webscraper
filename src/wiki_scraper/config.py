from dataclasses import dataclass


@dataclass(frozen=True)
class CrawlConfig:
    seed_url: str = "https://en.wikipedia.org/wiki/Main_Page"
    max_depth: int = 100
    max_concurrent_requests: int = 50
    max_connections: int = 100
    requests_per_second: float = 20.0
    bloom_expected_items: int = 8_000_000
    bloom_fp_rate: float = 0.001
    parser_workers: int = 4
    backoff_base: float = 1.0
    backoff_max: float = 60.0
    backoff_jitter: float = 1.0
    max_retries: int = 3
    dynamo_table_name: str = "WikiCrawlState"
    dynamo_batch_size: int = 25
    user_agent: str = "Mozilla/5.0 (compatible; WikiScraper/0.1; educational project; contact: wikiscraper@example.com)"
    worker_idle_timeout: float = 30.0
    max_links_per_page: int = 3000
    allowed_host: str = "en.wikipedia.org"
    robots_url: str = "https://en.wikipedia.org/robots.txt"
