import asyncio

from xtracted_common.configuration import XtractedConfig, XtractedConfigFromDotEnv
from xtracted_common.model import CrawlJobInput, CrawlJobInternal
from xtracted_common.services.jobs_service import PostgresJobService


class CrawlJobProducer:
    def __init__(self, config: XtractedConfig):
        self.job_service = PostgresJobService(config=config)

    async def submit(self, token: str, job: CrawlJobInput) -> CrawlJobInternal:
        crawl_job = await self.job_service.submit_job(token=token, crawl_job_input=job)
        return crawl_job


if __name__ == '__main__':
    config = XtractedConfigFromDotEnv()
    producer = CrawlJobProducer(config=config)
    asyncio.run(
        producer.submit(
            token='dummy-uid',
            job=CrawlJobInput(urls={'https://www.amazon.co.uk/dp/B0931VRJT5'}),
        )
    )
