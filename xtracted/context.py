import json
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
    async def ack(self, msg_id: str | int) -> None:
        pass

    @abstractmethod
    async def report_error(
        self, crawl_url: XtractedUrl, msg_id: str | int, error: Exception
    ) -> None:
        pass

    @abstractmethod
    async def enqueue(self, to_enqueue: XtractedUrl) -> bool:
        pass

    @abstractmethod
    async def complete(
        self, crawl_url: XtractedUrl, msg_id: int | str, data: dict[str, Any]
    ) -> None:
        pass


class CrawlContext(ABC):
    @abstractmethod
    def get_crawl_url(self) -> XtractedUrl:
        pass

    @abstractmethod
    async def enqueue(self, url: AnyHttpUrl) -> Optional[XtractedUrl]:
        pass

    @abstractmethod
    async def fail(self, error: Exception) -> None:
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

    async def ack(self, msg_id: str | int) -> None:
        queue = await self.config.new_pgmq_client()
        conn = await self.config.new_db_client()
        try:
            await queue.archive('job_urls', msg_id=msg_id, conn=conn)
        except Exception as e:
            logger.error(e)
        finally:
            await conn.close()

    async def report_error(
        self, crawl_url: XtractedUrl, msg_id: str | int, error: Exception
    ) -> None:
        conn = await self.config.new_db_client()
        queue = await self.config.new_pgmq_client()
        try:
            await conn.execute(
                """update job_urls set errors = errors || $1, retries = retries + 1 where user_id = $2 and job_id = $3 and url_id = $4 """,
                [repr(error)],
                crawl_url.uid,
                crawl_url.job_id,
                crawl_url._url_id,
            )
            if crawl_url.retries >= 3:
                await queue.archive('job_urls', msg_id=msg_id, conn=conn)
        except Exception as e:
            logger.error(e)
        finally:
            await conn.close()

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

    async def complete(
        self, crawl_url: XtractedUrl, msg_id: int | str, data: dict[str, Any]
    ) -> None:
        conn = await self.config.new_db_client()
        queue = await self.config.new_pgmq_client()
        try:
            async with conn.transaction():
                await conn.execute(
                    'update job_urls set status = $1, data = $2 where job_id = $3 and user_id = $4 and url_id = $5',
                    crawl_url.status,
                    json.dumps(data),
                    crawl_url.job_id,
                    crawl_url.uid,
                    crawl_url._url_id,
                )  # update urls
                await queue.archive('job_urls', msg_id=msg_id, conn=conn)

        except Exception as e:
            logger.error(e)
        finally:
            await conn.close()

    async def enqueue(self, to_enqueue: XtractedUrl) -> bool:
        conn = await self.config.new_db_client()
        try:
            exist = await conn.fetchrow(
                """select * from job_urls where user_id = $1 and job_id = $2 and url_id = $3""",
                to_enqueue.uid,
                to_enqueue.job_id,
                to_enqueue._url_id,
            )
            if exist:
                return False
            queue = await self.config.new_pgmq_client()
            async with conn.transaction():
                await conn.execute(
                    """insert into job_urls(user_id,job_id,url,url_id) values($1,$2,$3,$4)""",
                    to_enqueue.uid,
                    to_enqueue.job_id,
                    str(to_enqueue.url),
                    to_enqueue._url_id,
                )

                await queue.send(
                    'job_urls',
                    {
                        'event': 'new_url',
                        'job_id': to_enqueue.job_id,
                        'user_id': to_enqueue.uid,
                        'url_id': to_enqueue._url_id,
                        'url': str(to_enqueue.url),
                    },
                    conn=conn,
                )

            return True
        except Exception as e:
            logger.error(e)
            return False
        finally:
            await conn.close()


class RedisCrawlSyncer(CrawlSyncer):
    def __init__(self, *, redis: Redis | RedisCluster) -> None:
        self.redis = redis

    async def ack(self, msg_id: str | int) -> None:
        await self.redis.xack('crawl', 'crawlers', msg_id)

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

    async def report_error(
        self, crawl_url: XtractedUrl, msg_id: str | int, error: Exception
    ) -> None:
        pass

    async def replay(self, crawl_url: XtractedUrl) -> None:
        await self.redis.xadd(name='crawl', fields=crawl_url.model_dump(mode='json'))  # type: ignore

    async def complete(
        self, crawl_url: XtractedUrl, msg_id: int | str, data: dict[str, Any]
    ) -> None:
        return None

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
        message_id: str | int,
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

    async def fail(self, error: Exception) -> None:
        await self._crawl_syncer.report_error(self._crawl_url, self._message_id, error)

    async def set_running(self) -> None:
        self._crawl_url.status = CrawlUrlStatus.running
        await self._crawl_syncer.sync(self._crawl_url)

    async def complete(self, data: dict[str, Any]) -> None:
        self._crawl_url.status = CrawlUrlStatus.complete
        await self._crawl_syncer.complete(self._crawl_url, self._message_id, data)
