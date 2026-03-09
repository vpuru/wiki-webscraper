import argparse
import asyncio
import logging

from wiki_scraper.config import CrawlConfig
from wiki_scraper.crawler import CrawlOrchestrator


def main() -> None:
    parser = argparse.ArgumentParser(description="Wikipedia link scraper")
    parser.add_argument(
        "--seed-url",
        default=CrawlConfig.seed_url,
        help="Starting URL for the crawl",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=CrawlConfig.max_depth,
        help="Maximum BFS depth",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=CrawlConfig.num_workers,
        help="Number of async worker coroutines",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=CrawlConfig.max_concurrent_requests,
        help="Max concurrent in-flight requests (semaphore)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=CrawlConfig.requests_per_second,
        help="Requests per second",
    )
    parser.add_argument(
        "--table-name",
        default=CrawlConfig.dynamo_table_name,
        help="DynamoDB table name",
    )
    parser.add_argument(
        "--user-agent",
        default=CrawlConfig.user_agent,
        help="User-Agent header string",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = CrawlConfig(
        seed_url=args.seed_url,
        max_depth=args.max_depth,
        num_workers=args.workers,
        max_concurrent_requests=args.concurrency,
        requests_per_second=args.rate,
        dynamo_table_name=args.table_name,
        user_agent=args.user_agent,
    )

    orchestrator = CrawlOrchestrator(config)
    asyncio.run(orchestrator.run())


if __name__ == "__main__":
    main()
