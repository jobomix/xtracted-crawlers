import json
from typing import Any
from uuid import UUID

from asyncpg import Connection
from pydantic import HttpUrl
from tembo_pgmq_python.async_queue import PGMQueue
from xtracted_common.model import (
    CrawlJobUrlStatus,
)
from xtracted_common.services.jobs_service import JobsService, PostgresJobService

from tests.utilities import create_crawl_job, wait_for_condition
from xtracted.context import PostgresCrawlSyncer
from xtracted.crawler_configuration import CrawlerConfig
from xtracted.model import CrawlerUrl
from xtracted.services.crawlers_services import CrawlersService

urls = [
    'https://www.amazon.co.uk/dp/B0931VRJT5',
    'https://www.amazon.co.uk/dp/B0931VRJT6',
]


async def list_crawler_urls(
    crawlers_service: CrawlersService, with_uuid: UUID, job_id: int
) -> list[CrawlerUrl]:
    return await crawlers_service.list_crawler_urls(job_id=job_id, user_id=with_uuid)


async def test_syncer_update_crawler_url(
    conf: CrawlerConfig,
    crawlers_service: CrawlersService,
    jobs_service: JobsService,
    pg_stack: Any,
    with_uuid: UUID,
    with_user_token: str,
) -> None:
    syncer = PostgresCrawlSyncer(conf)
    crawl_job = await create_crawl_job(
        job_service=jobs_service, token=with_user_token, urls=urls
    )
    crawler_urls = await list_crawler_urls(
        crawlers_service, with_uuid, crawl_job.job_id
    )
    assert len(crawler_urls) == 2
    assert crawler_urls[0].status == CrawlJobUrlStatus.pending
    assert crawler_urls[1].status == CrawlJobUrlStatus.pending

    crawler_urls[0].status = CrawlJobUrlStatus.running
    await syncer.sync(crawler_urls[0])

    crawl_urls = await list_crawler_urls(crawlers_service, with_uuid, crawl_job.job_id)

    assert crawl_urls[0].status == CrawlJobUrlStatus.running
    assert crawl_urls[1].status == CrawlJobUrlStatus.pending


async def test_syncer_ack_message(
    conf: CrawlerConfig, pgmq_client: PGMQueue, pg_client: Connection
) -> None:
    msg_id = await pgmq_client.send('job_urls', {'hello': 'world'}, conn=pg_client)
    syncer = PostgresCrawlSyncer(conf)
    await syncer.ack(msg_id)
    archived = await pg_client.fetchrow(
        """select * from pgmq.a_job_urls where msg_id = $1""", msg_id
    )
    assert archived is not None
    assert archived['message'] == """{"hello": "world"}"""


async def test_syncer_enqueue_url_does_nothing_when_url_exists(
    conf: CrawlerConfig,
    pgmq_client: PGMQueue,
    pg_client: Connection,
    with_uuid: UUID,
    with_user_token: str,
) -> None:
    syncer = PostgresCrawlSyncer(conf)
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )
    enqueued = await syncer.enqueue(
        user_id=with_uuid,
        job_id=crawl_job.job_id,
        url=HttpUrl('https://www.amazon.co.uk/dp/B0931VRJT6?something=different'),
    )
    assert not enqueued


async def test_syncer_enqueue_url_when_url_does_not_exists(
    conf: CrawlerConfig,
    pgmq_client: PGMQueue,
    pg_client: Connection,
    crawlers_service: CrawlersService,
    with_uuid: UUID,
    with_user_token: str,
) -> None:
    syncer = PostgresCrawlSyncer(conf)
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )
    enqueued = await syncer.enqueue(
        user_id=with_uuid,
        job_id=crawl_job.job_id,
        url=HttpUrl('https://www.amazon.co.uk/dp/B0931VRJT9'),
    )
    assert enqueued

    crawl_urls = await list_crawler_urls(crawlers_service, with_uuid, crawl_job.job_id)
    assert len(crawl_urls) == 3
    assert crawl_urls[2].status == CrawlJobUrlStatus.pending
    assert str(crawl_urls[2].url) == 'https://www.amazon.co.uk/dp/B0931VRJT9'
    assert crawl_urls[2].url_id == 'B0931VRJT9'


async def test_syncer_enqueue_url_and_send_event_when_url_does_not_exists(
    conf: CrawlerConfig,
    pgmq_client: PGMQueue,
    pg_client: Connection,
    crawlers_service: CrawlersService,
    with_uuid: UUID,
    with_user_token: str,
) -> None:
    syncer = PostgresCrawlSyncer(conf)
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )
    enqueued = await syncer.enqueue(
        user_id=with_uuid,
        job_id=crawl_job.job_id,
        url=HttpUrl('https://www.amazon.co.uk/dp/B0931VRJT9'),
    )
    assert enqueued

    events = await pg_client.fetch("""select * from pgmq.q_job_urls""")
    assert events is not None
    assert len(events) == 1
    record = events[0]
    deserialised = json.loads(record['message'])
    assert deserialised['event'] == 'new_url'
    assert deserialised['url'] == 'https://www.amazon.co.uk/dp/B0931VRJT9'
    assert deserialised['user_id'] == str(with_uuid)
    assert deserialised['job_id'] == crawl_job.job_id


async def test_syncer_complete_archive_event(
    conf: CrawlerConfig,
    pgmq_client: PGMQueue,
    pg_client: Connection,
    with_uuid: UUID,
    with_user_token: str,
    crawlers_service: CrawlersService,
) -> None:
    syncer = PostgresCrawlSyncer(conf)
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    existing_url = await crawlers_service.get_crawler_url(
        user_id=with_uuid, job_id=crawl_job.job_id, url_id='B0931VRJT5'
    )

    assert existing_url is not None

    msg_id = await pgmq_client.send(
        'job_urls',
        {
            'event': 'new_url',
            'job_id': crawl_job.job_id,
            'user_id': with_uuid,
            'url_id': existing_url.url_id,
            'url': str(existing_url.url),
        },
        conn=pg_client,
    )
    await syncer.complete(existing_url, msg_id, {'hello': 'world'})

    row = await pg_client.fetchrow(
        """select * from job_urls where url_id = $1""", existing_url.url_id
    )

    assert row is not None
    assert json.loads(row['data']) == {'hello': 'world'}


async def test_syncer_should_report_errors(
    conf: CrawlerConfig,
    pgmq_client: PGMQueue,
    pg_client: Connection,
    with_uuid: UUID,
    with_user_token: str,
    crawlers_service: CrawlersService,
) -> None:
    async def cond() -> Any:
        job_url = await pg_client.fetchrow(
            """select * from job_urls where job_id = $1 and user_id = $2""",
            crawl_job.job_id,
            with_uuid,
        )
        assert job_url is not None
        assert job_url['errors'] is not None
        assert job_url['errors'] == [repr(ValueError('KABOOM!'))]
        assert job_url['retries'] == 1

    syncer = PostgresCrawlSyncer(conf)
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    existing_url = await crawlers_service.get_crawler_url(
        user_id=with_uuid, job_id=crawl_job.job_id, url_id='B0931VRJT5'
    )

    assert existing_url is not None

    msg_id = await pgmq_client.send(
        'job_urls',
        {
            'event': 'new_url',
            'job_id': crawl_job.job_id,
            'user_id': with_uuid,
            'url_id': existing_url.url_id,
            'url': str(existing_url.url),
        },
        conn=pg_client,
    )

    assert existing_url is not None

    await syncer.report_error(existing_url, msg_id, ValueError('KABOOM!'))

    await wait_for_condition(cond)


async def test_syncer_should_discard_message_when_3_consecutive_failures(
    conf: CrawlerConfig,
    pgmq_client: PGMQueue,
    pg_client: Connection,
    with_uuid: UUID,
    with_user_token: str,
    crawlers_service: CrawlersService,
) -> None:
    async def cond() -> Any:
        job_url = await pg_client.fetchrow(
            """select * from job_urls where job_id = $1 and user_id = $2""",
            crawl_job.job_id,
            with_uuid,
        )
        assert job_url is not None
        assert job_url['errors'] is not None
        assert len(job_url['errors']) == 3

        archived_message = await pg_client.fetchrow(
            """select * from pgmq.a_job_urls where msg_id = $1""", msg_id
        )
        assert archived_message is not None

    syncer = PostgresCrawlSyncer(conf)
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    existing_url = await crawlers_service.get_crawler_url(
        user_id=with_uuid, job_id=crawl_job.job_id, url_id='B0931VRJT5'
    )

    assert existing_url is not None

    msg_id = await pgmq_client.send(
        'job_urls',
        {
            'event': 'new_url',
            'job_id': crawl_job.job_id,
            'user_id': with_uuid,
            'url_id': existing_url.url_id,
            'url': str(existing_url.url),
        },
        conn=pg_client,
    )

    existing_url.retries = 1
    await syncer.report_error(existing_url, msg_id, ValueError('KABOOM!'))

    existing_url.retries = 2
    await syncer.report_error(existing_url, msg_id, ValueError('KABOOM2!'))

    existing_url.retries = 3
    await syncer.report_error(existing_url, msg_id, ValueError('KABOOM3!'))

    await wait_for_condition(cond)


async def test_syncer_complete_decrease_request() -> None:
    pass
