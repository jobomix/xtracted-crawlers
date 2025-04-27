import json
from typing import Any

from asyncpg import Connection
from xtracted_common.services.jobs_service import PostgresJobService

from tests.integration.amazon_server import new_web_app
from tests.utilities import create_crawl_job, wait_for_condition
from xtracted.crawler_configuration import CrawlerConfig
from xtracted.workers.pg_crawl_job_worker import PGCrawlJobWorker


async def test_start_job_append_job_urls_to_queue(
    conf: CrawlerConfig,
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
    conf: CrawlerConfig,
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
    conf: CrawlerConfig,
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
        assert data['asin'] == 'B08897N6HB'
        assert data['variants'] is not None

    urls = [f'http://localhost:{server.port}/dp/B08897N6HB?x=foo&bar=y']

    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    await job_service.start_job(token=with_user_token, job_id=crawl_job.job_id)

    worker = PGCrawlJobWorker(conf)

    worker.run()

    await wait_for_condition(cond)
    await worker.cancel()


async def test_crawl_job_worker_crawls_reset_all_urls(
    conf: CrawlerConfig,
    pg_client: Connection,
    with_user_token: str,
    with_user: str,
    aiohttp_server: Any,
) -> None:
    server = await aiohttp_server(new_web_app())

    date_created = None

    async def cond() -> Any:
        job_url = await pg_client.fetchrow(
            """select * from job_urls where job_id = $1 and user_id = $2 """,
            crawl_job.job_id,
            with_user,
        )
        assert job_url is not None
        assert job_url['retries'] == 0
        data = json.loads(job_url['data'])
        assert 'hello' not in data
        assert date_created is not None
        assert job_url['created_at'] != date_created

    urls = [f'http://localhost:{server.port}/dp/B08897N6HB?x=foo&bar=y']

    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    await pg_client.execute(
        """update job_urls set retries = 2,data = $1""", json.dumps({'hello': 'world'})
    )

    date_created = await pg_client.fetchval(
        """select created_at from job_urls where url_id = 'B08897N6HB'"""
    )

    assert date_created is not None

    await job_service.start_job(token=with_user_token, job_id=crawl_job.job_id)

    worker = PGCrawlJobWorker(conf)

    worker.run()

    await wait_for_condition(cond)
    await worker.cancel()


async def test_crawler_should_report_error_and_discard_after_3_attempts(
    conf: CrawlerConfig,
    pg_client: Connection,
    with_user_token: str,
    with_user: str,
    aiohttp_server: Any,
) -> None:
    server = await aiohttp_server(new_web_app())

    conf.crawler_url_vt = 6

    async def cond() -> Any:
        job_url = await pg_client.fetchrow(
            """select * from job_urls where job_id = $1 and user_id = $2 """,
            crawl_job.job_id,
            with_user,
        )
        assert job_url is not None
        assert job_url['errors'] is not None
        errors = job_url['errors']
        assert len(errors) == 3

        archived_errors = await pg_client.fetchrow("""select * from pgmq.a_job_urls""")
        assert archived_errors is not None

    urls = [f'http://localhost:{server.port}/dp/B0TFOUND10?x=foo&bar=y']

    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    await job_service.start_job(token=with_user_token, job_id=crawl_job.job_id)

    worker = PGCrawlJobWorker(conf)

    worker.run()

    await wait_for_condition(cond, timeout=30)

    await worker.cancel()
