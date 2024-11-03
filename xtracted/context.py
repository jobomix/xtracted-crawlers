from abc import ABC, abstractmethod
from typing import Any

from pydantic import AnyHttpUrl
from redis.asyncio import RedisCluster
from redis.asyncio.client import Redis

from xtracted.model import XtractedUrl
from xtracted.storage import Storage


class CrawlSyncer(ABC):
    @abstractmethod
    async def sync(self, crawl_url: XtractedUrl) -> None:
        pass

    @abstractmethod
    async def ack(self, message_id: str) -> None:
        pass


class CrawlContext(ABC):
    @abstractmethod
    def get_crawl_url(self) -> XtractedUrl:
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
    def __init__(self, *, redis: Redis | RedisCluster) -> None:
        self.redis = redis

    async def ack(self, message_id: str) -> None:
        await self.redis.xack('crawl', 'crawlers', message_id)

    async def sync(self, crawl_url: XtractedUrl) -> None:
        await self.redis.hset(
            crawl_url.url_id, mapping=crawl_url.model_dump(mode='json')
        )  # type: ignore
        return None


class DefaultCrawlContext(CrawlContext):
    def __init__(
        self,
        *,
        storage: Storage,
        crawl_syncer: CrawlSyncer,
        crawl_url: XtractedUrl,
        message_id: str,
    ) -> None:
        self._storage = storage
        self._crawl_syncer = crawl_syncer
        self._crawl_url = crawl_url
        self._message_id = message_id

    def get_crawl_url(self) -> XtractedUrl:
        return self._crawl_url

    async def enqueue(self, url: AnyHttpUrl) -> bool:
        return False

    async def fail(self) -> None:
        pass

    async def complete(self, data: dict[str, Any]) -> None:
        self._crawl_url.set_url_complete()
        await self._storage.append(self._crawl_url, data)
        await self._crawl_syncer.sync(self._crawl_url)
        await self._crawl_syncer.ack(self._message_id)
