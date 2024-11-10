import asyncio

from xtracted_common.model import CrawlJob

from xtracted.configuration import XtractedConfigFromDotEnv
from xtracted.model import CrawlJobInput
from xtracted.queue import Queue, RedisQueue


class CrawlJobProducer:
    def __init__(self, queue: Queue):
        self.queue = queue

    async def submit(self, job: CrawlJobInput) -> CrawlJob:
        crawl_job = await self.queue.submit_crawl_job(job)
        return crawl_job


if __name__ == '__main__':
    config = XtractedConfigFromDotEnv()
    queue = RedisQueue(config)
    producer = CrawlJobProducer(queue=queue)
    asyncio.run(
        producer.submit(CrawlJobInput(urls={'https://www.amazon.co.uk/dp/B0931VRJT5'}))
    )
