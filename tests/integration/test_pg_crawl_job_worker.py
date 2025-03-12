from typing import Any

from asyncpg import Connection
from xtracted_common.configuration import XtractedConfig
from xtracted_common.services.jobs_service import PostgresJobService

from tests.utilities import create_crawl_job, wait_for_condition
from xtracted.workers.pg_crawl_job_worker import PGCrawlJobWorker

urls = [
    'https://www.amazon.co.uk/dp/B0931VRJT5',
    'https://www.amazon.co.uk/dp/B0931VRJT6',
]


async def test_start_job_append_job_urls_to_queue(
    conf: XtractedConfig,
    pg_client: Connection,
    with_user_token: str,
    with_user: str,
) -> None:
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )
    await job_service.start_job(token=with_user_token, job_id=crawl_job.job_id)

    worker = PGCrawlJobWorker(conf)

    worker.run()

    async def cond() -> Any:
        records = await pg_client.fetch(
            """select * from job_urls where job_id = $1 and user_id = $2""",
            crawl_job.job_id,
            with_user,
        )
        assert len(records) == 2

    await wait_for_condition(cond, timeout=5)
    await worker.cancel()


async def test_start_job_changes_job_status_to_running(
    conf: XtractedConfig,
    pg_client: Connection,
    with_user_token: str,
    with_user: str,
) -> None:
    async def cond(status: str) -> Any:
        job = await pg_client.fetchrow(
            """select * from jobs where job_id = $1 and user_id = $2 """,
            crawl_job.job_id,
            with_user,
        )
        assert job['job_status'] == status

    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    await wait_for_condition(lambda: cond('pending'), timeout=5)

    await job_service.start_job(token=with_user_token, job_id=crawl_job.job_id)

    worker = PGCrawlJobWorker(conf)

    worker.run()

    await wait_for_condition(lambda: cond('running'))
    await worker.cancel()
