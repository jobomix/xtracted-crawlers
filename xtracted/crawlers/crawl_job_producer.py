import asyncio
import uuid

from redis.asyncio.client import Redis
from redis.asyncio.cluster import RedisCluster

from xtracted.configuration import XtractedConfigFromDotEnv
from xtracted.model import CrawlJobInput


class CrawlJobProducer:
    def __init__(self, *, client: Redis | RedisCluster):
        self.client = client

    async def submit(self, job: CrawlJobInput) -> str:
        job_id = str(uuid.uuid1())

        # add those urls to the main crawl stream
        for url in job.urls:
            await self.client.hset(  # type: ignore
                name=f'job:{job_id}:{url.__str__()}',
                mapping={'attempts': 0, 'status': 'NEW'},
            )
            await self.client.xadd(
                name='crawl', fields={'url': url.__str__(), 'job_id': job_id}
            )
        await self.client.aclose()
        return job_id

    async def close(self) -> None:
        await self.client.aclose()


if __name__ == '__main__':
    config = XtractedConfigFromDotEnv()
    producer = CrawlJobProducer(
        client=RedisCluster.from_url(
            str(config.random_cluster_url()), decode_responses=True
        )
    )
    asyncio.run(
        producer.submit(CrawlJobInput(urls={'https://www.amazon.co.uk/dp/B0931VRJT5'}))
    )
