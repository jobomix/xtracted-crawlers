import uuid
from abc import ABC, abstractmethod
from typing import Optional, cast

from redis.asyncio import ResponseError

from xtracted.configuration import XtractedConfig
from xtracted.model import (
    AmazonProductUrl,
    CrawlJob,
    CrawlJobInput,
    CrawlJobStatus,
    XtractedUrl,
)


class Queue(ABC):
    @abstractmethod
    async def ack(self, msg_id: str) -> None:
        pass

    @abstractmethod
    async def submit_crawl_job(self, crawl_job_input: CrawlJobInput) -> CrawlJob:
        pass

    @abstractmethod
    async def enqueue(self) -> None:
        pass

    @abstractmethod
    async def get_crawl_job(self, job_id: str) -> Optional[CrawlJob]:
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
        self, crawl_url: XtractedUrl
    ) -> dict[
        bytes | memoryview | str | int | float, bytes | memoryview | str | int | float
    ]:
        import json

        json_model = crawl_url.model_dump_json()
        dict_model = json.loads(json_model)

        return cast(
            dict[
                bytes | memoryview | str | int | float,
                bytes | memoryview | str | int | float,
            ],
            dict_model,
        )

    async def ack(self, msg_id: str) -> None:
        redis = self.config.new_client()
        await redis.xack('crawl', 'crawlers', msg_id)
        await redis.aclose()

    async def get_crawl_job(self, job_id: str) -> Optional[CrawlJob]:
        redis = self.config.new_client()
        crawl_job = None
        try:
            ucnt = await redis.get(f'ucnt:{job_id}')
            if ucnt:
                crawl_job = CrawlJob(job_id=job_id, status=CrawlJobStatus.pending)
                for i in range(int(ucnt)):
                    value = await redis.hgetall(f'crawl_url:{job_id}:{i}')  # type: ignore
                    if value:
                        crawl_job.urls.add(AmazonProductUrl(**value))
        finally:
            await redis.aclose()
        return crawl_job

    async def submit_crawl_job(self, crawl_job_input: CrawlJobInput) -> CrawlJob:
        job_id = str(uuid.uuid1())
        redis = self.config.new_client()
        crawl_job = CrawlJob(job_id=job_id, status=CrawlJobStatus.pending)
        await self._create_crawlers_group()
        await redis.set(f'ucnt:{crawl_job.job_id}', '0')

        # add those urls to the main crawl stream
        for url in crawl_job_input.urls:
            ucnt = await redis.get(f'ucnt:{crawl_job.job_id}')

            await redis.zadd(f'job:{job_id}:pending', {str(ucnt): ucnt})

            crawl_url = AmazonProductUrl(
                job_id=job_id,
                url=url,
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

    async def enqueue(self) -> None:
        pass
