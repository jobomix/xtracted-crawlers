import asyncio

from xtracted_common.configuration import XtractedConfig, XtractedConfigFromDotEnv
from xtracted_common.model import CrawlJob, CrawlJobInput
from xtracted_common.services.jobs_service import DefaultJobsService


class CrawlJobProducer:
    def __init__(self, config: XtractedConfig):
        self.job_service = DefaultJobsService(config=config)

    async def submit(self, uid: str, job: CrawlJobInput) -> CrawlJob:
        crawl_job = await self.job_service.submit_job(uid=uid, crawl_job_input=job)
        return crawl_job


if __name__ == '__main__':
    config = XtractedConfigFromDotEnv()
    producer = CrawlJobProducer(config=config)
    asyncio.run(
        producer.submit(
            'dummy-uid', CrawlJobInput(urls={'https://www.amazon.co.uk/dp/B0931VRJT5'})
        )
    )
