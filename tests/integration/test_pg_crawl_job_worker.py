import json
from typing import Any

from asyncpg import Connection
from xtracted_common.configuration import XtractedConfig
from xtracted_common.services.jobs_service import PostgresJobService

from tests.integration.amazon_server import new_web_app
from tests.utilities import create_crawl_job, wait_for_condition
from xtracted.workers.pg_crawl_job_worker import PGCrawlJobWorker


async def test_start_job_append_job_urls_to_queue(
    conf: XtractedConfig,
    pg_client: Connection,
    with_user_token: str,
    with_user: str,
    aiohttp_server: Any,
) -> None:
    server = await aiohttp_server(new_web_app())

    urls = [f'http://localhost:{server.port}/dp/B01GFPWTI4?x=foo&bar=y']

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
        assert len(records) == 1

    await wait_for_condition(cond, timeout=5)
    await worker.cancel()


async def test_start_job_changes_job_status_to_running(
    conf: XtractedConfig,
    pg_client: Connection,
    with_user_token: str,
    with_user: str,
    aiohttp_server: Any,
) -> None:
    server = await aiohttp_server(new_web_app())

    async def cond(status: str) -> Any:
        job = await pg_client.fetchrow(
            """select * from jobs where job_id = $1 and user_id = $2 """,
            crawl_job.job_id,
            with_user,
        )
        assert job['job_status'] == status

    urls = [f'http://localhost:{server.port}/dp/B01GFPWTI4?x=foo&bar=y']

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


async def test_crawl_job_worker_crawls_url(
    conf: XtractedConfig,
    pg_client: Connection,
    with_user_token: str,
    with_user: str,
    aiohttp_server: Any,
) -> None:
    server = await aiohttp_server(new_web_app())

    async def cond() -> Any:
        job_url = await pg_client.fetchrow(
            """select * from job_urls where job_id = $1 and user_id = $2 """,
            crawl_job.job_id,
            with_user,
        )
        assert job_url is not None
        assert job_url['data'] is not None
        data = json.loads(job_url['data'])
        assert data['asin'] == 'B01GFPWTI4'

    urls = [f'http://localhost:{server.port}/dp/B01GFPWTI4?x=foo&bar=y']

    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    await job_service.start_job(token=with_user_token, job_id=crawl_job.job_id)

    worker = PGCrawlJobWorker(conf)

    worker.run()

    await wait_for_condition(cond)
    await worker.cancel()
