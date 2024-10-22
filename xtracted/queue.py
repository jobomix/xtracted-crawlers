import asyncio
import uuid
from abc import ABC, abstractmethod
from typing import Any, Optional, cast

from redis.asyncio import ResponseError

from xtracted.configuration import XtractedConfig
from xtracted.model import (
    CrawlJob,
    CrawlJobInput,
    CrawlJobStatus,
    CrawlUrl,
)


class Queue(ABC):
    @abstractmethod
    async def submit_crawl_job(self, crawlJobInput: CrawlJobInput) -> CrawlJob:
        pass

    @abstractmethod
    def enqueue(self) -> None:
        pass

    @abstractmethod
    async def get_crawl_job(self, job_id: str) -> Optional[CrawlJob]:
        pass

    @abstractmethod
    async def consume(self, consumer: str) -> Optional[CrawlUrl]:
        pass


class RedisQueue(Queue):
    def __init__(self, config: XtractedConfig) -> None:
        self.config = config

    async def _create_crawlers_group(self) -> None:
        redis = self.config.new_client()
        try:
            await redis.xinfo_stream('crawl')
        except ResponseError as e:
            if e.args and e.args[0] == 'no such key':
                await redis.xgroup_create('crawl', 'crawlers', mkstream=True)
        finally:
            await redis.aclose()

    def _crawl_url_to_redis_hset(
        self, crawl_url: CrawlUrl
    ) -> dict[
        bytes | memoryview | str | int | float, bytes | memoryview | str | int | float
    ]:
        import json

        json_model = crawl_url.model_dump_json()
        return cast(
            dict[
                bytes | memoryview | str | int | float,
                bytes | memoryview | str | int | float,
            ],
            json.loads(json_model),
        )

    async def get_crawl_job(self, job_id: str) -> Optional[CrawlJob]:
        redis = self.config.new_client()
        try:
            ucnt = await redis.get(f'ucnt:{job_id}')
            if ucnt:
                crawl_job = CrawlJob(job_id=job_id, status=CrawlJobStatus.pending)
                for i in range(int(ucnt)):
                    value = await redis.hgetall(f'crawl_url:{job_id}:{i}')  # type: ignore
                    if value:
                        crawl_job.urls.add(CrawlUrl(**value))
        finally:
            await redis.aclose()
        return crawl_job

    async def submit_crawl_job(self, jobInput: CrawlJobInput) -> CrawlJob:
        job_id = str(uuid.uuid1())
        redis = self.config.new_client()
        crawl_job = CrawlJob(job_id=job_id, status=CrawlJobStatus.pending)
        await self._create_crawlers_group()
        await redis.set(f'ucnt:{crawl_job.job_id}', '0')

        # add those urls to the main crawl stream
        for url in jobInput.urls:
            ucnt = await redis.get(f'ucnt:{crawl_job.job_id}')

            await redis.zadd(f'job:{job_id}:pending', {str(ucnt): ucnt})

            crawl_url = CrawlUrl(
                crawl_url_id=f'crawl_url:{job_id}:{ucnt}',
                url=url.__str__(),
            )

            url_mapping = self._crawl_url_to_redis_hset(crawl_url)

            await redis.hset(  # type: ignore
                name=f'crawl_url:{job_id}:{ucnt}', mapping=url_mapping
            )

            await redis.xadd(name='crawl', fields=url_mapping)

            await redis.incr(f'ucnt:{crawl_job.job_id}')

            crawl_job.urls.add(crawl_url)

        await redis.aclose()
        return crawl_job

    def enqueue(self) -> None:
        pass

    async def consume(self, consumer: str) -> Optional[CrawlUrl]:
        redis = self.config.new_client()
        res = await redis.xreadgroup(
            groupname='crawlers',
            consumername=consumer,
            streams={'crawl': '>'},
            count=1,
            block=5000,
            noack=False,
        )

        if res and len(res) == 1:
            x = res[0][1]
            mapping = x[0][1]
            await redis.xack('crawl', 'crawlers', x[0][0])
            await redis.aclose()
            return CrawlUrl(**mapping)

        await redis.aclose()
        return None
