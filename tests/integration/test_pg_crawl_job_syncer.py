import asyncio
from typing import Any

from xtracted_common.configuration import XtractedConfig
from xtracted_common.model import CrawlUrlStatus, XtractedUrl
from xtracted_common.services.jobs_service import PostgresJobService

from tests.utilities import create_crawl_job
from xtracted.context import PostgresCrawlSyncer

urls = [
    'https://www.amazon.co.uk/dp/B0931VRJT5',
    'https://www.amazon.co.uk/dp/B0931VRJT6',
]


async def test_syncer_update_crawl_url(
    conf: XtractedConfig, pg_stack: Any, with_user: str, with_user_token: str
) -> None:
    async def list_crawl_urls() -> list[XtractedUrl]:
        return await job_service.list_job_urls(
            token=with_user_token, job_id=crawl_job.job_id, offset=0, limit=10
        )

    syncer = PostgresCrawlSyncer(conf)
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )
    crawl_urls = await list_crawl_urls()
    assert len(crawl_urls) == 2
    assert crawl_urls[0].status == CrawlUrlStatus.pending
    assert crawl_urls[1].status == CrawlUrlStatus.pending

    crawl_urls[0].status = CrawlUrlStatus.running
    await syncer.sync(crawl_urls[0])

    crawl_urls = await list_crawl_urls()
    assert crawl_urls[0].status == CrawlUrlStatus.running
    assert crawl_urls[1].status == CrawlUrlStatus.pending


async def test_syncer_ack_message(conf: XtractedConfig) -> None:
    pass


async def test_syncer_enqueue_url_does_nothing_when_url_exists(
    conf: XtractedConfig,
) -> None:
    pass
