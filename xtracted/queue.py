import uuid
from abc import ABC, abstractmethod
from typing import Optional

from redis.asyncio import ResponseError
from xtracted_common.model import CrawlJob, CrawlJobStatus

from xtracted.configuration import XtractedConfig
from xtracted.model import (
    AmazonProductUrl,
    CrawlJobInput,
    UrlFactory,
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

    async def ack(self, msg_id: str) -> None:
        redis = self.config.new_client()
        await redis.xack('crawl', 'crawlers', msg_id)
        await redis.aclose()

    async def get_crawl_job(self, job_id: str) -> Optional[CrawlJob]:
        redis = self.config.new_client()
        crawl_job = None
        try:
            urls = await redis.smembers(f'job:{job_id}')  # type: ignore
            if urls:
                crawl_job = CrawlJob(job_id=job_id, status=CrawlJobStatus.pending)
                for url_id in urls:
                    value = await redis.hgetall(url_id)  # type: ignore
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
        # add those urls to the main crawl stream
        for url in crawl_job_input.urls:
            crawl_url = UrlFactory.new_url(
                job_id=job_id,
                url=url,
            )

            if crawl_url:
                await redis.sadd(f'job:{job_id}', crawl_url.url_id)  # type:ignore

                url_mapping = crawl_url.model_dump(mode='json')

                await redis.hset(  # type: ignore
                    name=crawl_url.url_id, mapping=url_mapping
                )

                await redis.xadd(name='crawl', fields=url_mapping)  # type: ignore

                crawl_job.urls.add(crawl_url)

        await redis.aclose()
        return crawl_job

    async def enqueue(self) -> None:
        pass
