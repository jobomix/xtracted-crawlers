import logging
from abc import ABC, abstractmethod
from typing import Any, Optional

from pydantic import AnyHttpUrl
from redis.asyncio import RedisCluster
from redis.asyncio.client import Redis
from xtracted_common.configuration import XtractedConfig
from xtracted_common.model import CrawlUrlStatus, UrlFactory, XtractedUrl
from xtracted_common.storage import Storage

logger = logging.getLogger('crawljob-syncer')


class CrawlSyncer(ABC):
    @abstractmethod
    async def sync(self, crawl_url: XtractedUrl) -> None:
        pass

    @abstractmethod
    async def ack(self, message_id: str | int) -> None:
        pass

    @abstractmethod
    async def replay(self, crawl_url: XtractedUrl) -> None:
        pass

    @abstractmethod
    async def enqueue(self, to_enqueue: XtractedUrl) -> bool:
        pass


class CrawlContext(ABC):
    @abstractmethod
    def get_crawl_url(self) -> XtractedUrl:
        pass

    @abstractmethod
    async def enqueue(self, url: AnyHttpUrl) -> Optional[XtractedUrl]:
        pass

    @abstractmethod
    async def fail(self) -> None:
        pass

    @abstractmethod
    async def complete(self, data: dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def set_running(self) -> None:
        pass


class PostgresCrawlSyncer(CrawlSyncer):
    def __init__(self, config: XtractedConfig):
        self.config = config

    async def ack(self, message_id: str | int) -> None:
        queue = await self.config.new_pgmq_client()
        await queue.archive('job_urls', msg_id=message_id)

    async def sync(self, crawl_url: XtractedUrl) -> None:
        conn = await self.config.new_db_client()
        try:
            logger.info(crawl_url)
            await conn.execute(
                """update job_urls set status = $1 where job_id = $2 and user_id = $3 and url_id = $4""",
                crawl_url.status,
                crawl_url.job_id,
                crawl_url.uid,
                crawl_url._url_id,
            )
        except Exception as e:
            logger.error(e)
        finally:
            await conn.close()

    async def replay(self, crawl_url: XtractedUrl) -> None:
        pass

    async def enqueue(self, to_enqueue: XtractedUrl) -> bool:
        return False


class RedisCrawlSyncer(CrawlSyncer):
    def __init__(self, *, redis: Redis | RedisCluster) -> None:
        self.redis = redis

    async def ack(self, message_id: str | int) -> None:
        await self.redis.xack('crawl', 'crawlers', message_id)

    async def sync(self, crawl_url: XtractedUrl) -> None:
        pipeline = self.redis.pipeline()
        for status in CrawlUrlStatus:
            pipeline.srem(
                f'u:{crawl_url.uid}:job:{crawl_url.job_id}:urls:{status.name}',
                crawl_url._url_id,
            )
            pipeline.sadd(
                f'u:{crawl_url.uid}:job:{crawl_url.job_id}:urls:{crawl_url.status.name}',
                crawl_url._url_id,
            )
        pipeline.hset(
            crawl_url._url_key,
            mapping=crawl_url.model_dump(mode='json'),
        )
        await pipeline.execute()
        return None

    async def replay(self, crawl_url: XtractedUrl) -> None:
        await self.redis.xadd(name='crawl', fields=crawl_url.model_dump(mode='json'))  # type: ignore

    async def enqueue(self, to_enqueue: XtractedUrl) -> bool:
        exist = await self.redis.hget(name=to_enqueue._url_key, key='url')  # type: ignore

        if not exist:
            pipeline = self.redis.pipeline()
            pipeline.sadd(
                f'u:{to_enqueue.uid}:job:{to_enqueue.job_id}:urls',
                to_enqueue._url_id,
            )
            pipeline.sadd(
                f'u:{to_enqueue.uid}:job:{to_enqueue.job_id}:urls:{to_enqueue.status.name}',
                to_enqueue._url_id,
            )
            url_mapping = to_enqueue.model_dump(mode='json')
            pipeline.hset(
                name=to_enqueue._url_key,
                mapping=url_mapping,
            )
            pipeline.xadd(name='crawl', fields=url_mapping)  # type: ignore
            await pipeline.execute()
            return True
        return False


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

    async def enqueue(self, url: AnyHttpUrl) -> Optional[XtractedUrl]:
        to_enqueue = UrlFactory.new_url(
            job_id=self._crawl_url.job_id, url=url, uid=self._crawl_url.uid
        )
        if to_enqueue:
            if await self._crawl_syncer.enqueue(to_enqueue):
                return to_enqueue
        return None

    async def fail(self) -> None:
        self._crawl_url.error()
        await self._crawl_syncer.sync(crawl_url=self._crawl_url)
        await self._crawl_syncer.ack(self._message_id)
        if self._crawl_url.retries < 3:
            await self._crawl_syncer.replay(self._crawl_url)

    async def set_running(self) -> None:
        self._crawl_url.status = CrawlUrlStatus.running
        await self._crawl_syncer.sync(self._crawl_url)

    async def complete(self, data: dict[str, Any]) -> None:
        self._crawl_url.status = CrawlUrlStatus.complete
        await self._storage.append_crawled_data(self._crawl_url, data)
        await self._crawl_syncer.sync(self._crawl_url)
        await self._crawl_syncer.ack(self._message_id)
