import logging
from urllib.robotparser import RobotFileParser

import aiohttp

logger = logging.getLogger(__name__)


class RobotsChecker:
    def __init__(self, user_agent: str) -> None:
        self._user_agent = user_agent
        self._parser = RobotFileParser()
        self._loaded = False

    async def load(self, robots_url: str) -> None:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(robots_url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        self._parser.parse(text.splitlines())
                        self._loaded = True
                        logger.info("Loaded robots.txt from %s", robots_url)
                    else:
                        logger.warning("Failed to fetch robots.txt: HTTP %d, allowing all", resp.status)
                        self._loaded = False
        except aiohttp.ClientError as e:
            logger.warning("Error fetching robots.txt: %s, allowing all", e)
            self._loaded = False

    def is_allowed(self, url: str) -> bool:
        if not self._loaded:
            return True
        return self._parser.can_fetch(self._user_agent, url)
