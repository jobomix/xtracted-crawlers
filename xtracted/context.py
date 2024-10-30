from abc import ABC, abstractmethod
from typing import Any

from pydantic import AnyHttpUrl
from redis.asyncio import RedisCluster
from redis.asyncio.client import Redis

from xtracted.model import CrawlUrl, CrawlUrlStatus
from xtracted.storage import Storage


class CrawlSyncer(ABC):
    @abstractmethod
    async def update_url_status(self, status: CrawlUrlStatus) -> None:
        pass

    @abstractmethod
    async def ack(self) -> None:
        pass


class CrawlContext(ABC):
    @abstractmethod
    def get_crawl_url(self) -> CrawlUrl:
        pass

    @abstractmethod
    async def enqueue(self, url: AnyHttpUrl) -> bool:
        pass

    @abstractmethod
    async def fail(self) -> None:
        pass

    @abstractmethod
    async def complete(self, data: dict[str, Any]) -> None:
        pass


class RedisCrawlSyncer(CrawlSyncer):
    def __init__(
        self, *, redis: Redis | RedisCluster, crawl_url: CrawlUrl, message_id: str
    ) -> None:
        self.redis = redis
        self.crawl_url = crawl_url
        self.message_id = message_id

    async def ack(self) -> None:
        await self.redis.xack('crawl', 'crawlers', self.message_id)

    async def update_url_status(self, status: CrawlUrlStatus) -> None:
        self.crawl_url.status = status
        await self.redis.hset(
            self.crawl_url.crawl_url_id, mapping=self.crawl_url.model_dump(mode='json')
        )  # type: ignore
        return None


class DefaultCrawlContext(CrawlContext):
    def __init__(
        self,
        *,
        storage: Storage,
        crawl_syncer: CrawlSyncer,
        crawl_url: CrawlUrl,
        message_id: str,
    ) -> None:
        self._storage = storage
        self._crawl_syncer = crawl_syncer
        self._crawl_url = crawl_url
        self._message_id = message_id

    def get_crawl_url(self) -> CrawlUrl:
        return self._crawl_url

    async def enqueue(self, url: AnyHttpUrl) -> bool:
        return False

    async def fail(self) -> None:
        pass

    async def complete(self, data: dict[str, Any]) -> None:
        await self._crawl_syncer.update_url_status(CrawlUrlStatus.complete)
        await self._storage.append(self._crawl_url, data)
        await self._crawl_syncer.ack()
