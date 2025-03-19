from typing import cast
from unittest.mock import Mock

import pytest
from pydantic import AnyHttpUrl, AnyUrl
from pytest import fail
from redis.asyncio import Redis, ResponseError
from xtracted_common.configuration import XtractedConfig
from xtracted_common.model import (
    AmazonProductUrl,
    CrawlJobInput,
    CrawlUrlStatus,
    XtractedUrl,
)
from xtracted_common.services.jobs_service import JobsService
from xtracted_common.storage import Storage

from xtracted.context import DefaultCrawlContext, RedisCrawlSyncer
from xtracted.crawlers.crawl_job_producer import CrawlJobProducer


async def new_default_crawl_context(
    job_service: JobsService, redis_client: Redis, conf: XtractedConfig, with_user: str
) -> DefaultCrawlContext:
    async def _create_crawlers_group() -> None:
        try:
            await redis_client.xinfo_stream('crawl')
        except ResponseError as e:
            if e.args and e.args[0] == 'no such key':
                await redis_client.xgroup_create('crawl', 'crawlers', mkstream=True)

    await _create_crawlers_group()
    storage = Mock(spec=Storage)
    syncer = RedisCrawlSyncer(redis=redis_client)
    producer = CrawlJobProducer(config=conf)
    crawl_job = await producer.submit(
        with_user,
        CrawlJobInput(
            urls={
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT5'),
            }
        ),
    )

    crawl_url = AmazonProductUrl(
        url='https://www.amazon.co.uk/dp/B0931VRJT5',
        job_id=crawl_job.job_id,
        uid=with_user,
    )
    res = await redis_client.xreadgroup(
        groupname='crawlers',
        consumername='dummy',
        streams={'crawl': '>'},
        count=1,
        block=5000,
        noack=False,
    )

    if res and len(res) == 1:
        x = res[0][1]
        message_id = x[0][0]
    else:
        fail('no results')

    return DefaultCrawlContext(
        storage=storage,
        crawl_url=crawl_url,
        crawl_syncer=syncer,
        message_id=message_id,
    )


async def _urls_status_is(
    redis_client: Redis, crawl_url: XtractedUrl, expected_status: CrawlUrlStatus
) -> bool:
    statuses = []
    for status in CrawlUrlStatus:
        if status != expected_status:
            statuses.append(status)

    for status in statuses:
        not_there = await redis_client.sismember(  # type:ignore
            f'u:{crawl_url.uid}:job:{crawl_url.job_id}:urls:{status.name}',
            crawl_url._url_id,
        )
        assert not_there is not None and not_there == 0

    res = await redis_client.sismember(  # type:ignore
        f'u:{crawl_url.uid}:job:{crawl_url.job_id}:urls:{expected_status.name}',
        crawl_url._url_id,
    )
    return res is not None and res == 1


async def urls_status_is_error(
    redis_client: Redis,
    crawl_url: XtractedUrl,
) -> bool:
    return await _urls_status_is(redis_client, crawl_url, CrawlUrlStatus.error)


async def urls_status_is_pending(
    redis_client: Redis,
    crawl_url: XtractedUrl,
) -> bool:
    return await _urls_status_is(redis_client, crawl_url, CrawlUrlStatus.pending)


async def urls_status_is_complete(
    redis_client: Redis,
    crawl_url: XtractedUrl,
) -> bool:
    return await _urls_status_is(redis_client, crawl_url, CrawlUrlStatus.complete)


async def urls_status_is_running(
    redis_client: Redis,
    crawl_url: XtractedUrl,
) -> bool:
    return await _urls_status_is(redis_client, crawl_url, CrawlUrlStatus.running)


@pytest.mark.skip(reason='deprecated')
async def test_fail_replay_the_same_crawl_if_less_than_3_attempts(
    job_service: JobsService, redis_client: Redis, conf: XtractedConfig, with_user: str
) -> None:
    ctx = await new_default_crawl_context(
        job_service=job_service,
        redis_client=redis_client,
        conf=conf,
        with_user=with_user,
    )
    assert ctx._crawl_url.status == CrawlUrlStatus.pending
    remote_url = await redis_client.hgetall(ctx._crawl_url._url_key)  # type: ignore
    assert remote_url['status'] == 'pending'

    res = await redis_client.xreadgroup(
        groupname='crawlers',
        consumername='dummy',
        streams={'crawl': '>'},
        count=1,
        block=1000,
        noack=False,
    )

    assert len(res) == 0

    await ctx.fail()
    assert ctx._crawl_url.retries == 1

    pending = await redis_client.xpending('crawl', 'crawlers')
    assert pending['pending'] == 0

    res = await redis_client.xreadgroup(
        groupname='crawlers',
        consumername='dummy',
        streams={'crawl': '>'},
        count=100,
        block=1000,
        noack=False,
    )

    assert len(res) == 1
    first = res[0][1]
    crawl_url = AmazonProductUrl(**first[0][1])
    assert crawl_url.status == CrawlUrlStatus.error
    assert crawl_url.retries == 1

    assert await urls_status_is_error(redis_client, crawl_url)


@pytest.mark.skip(reason='deprecated')
async def test_context_switch_to_running(
    job_service: JobsService, redis_client: Redis, conf: XtractedConfig, with_user: str
) -> None:
    ctx = await new_default_crawl_context(
        job_service=job_service,
        redis_client=redis_client,
        conf=conf,
        with_user=with_user,
    )
    assert ctx._crawl_url.status == CrawlUrlStatus.pending
    remote_url = await redis_client.hgetall(ctx._crawl_url._url_key)  # type: ignore
    assert remote_url['status'] == 'pending'

    await ctx.set_running()
    assert ctx._crawl_url.status == CrawlUrlStatus.running  # type: ignore
    remote_url = await redis_client.hgetall(ctx._crawl_url._url_key)
    assert remote_url['status'] == 'running'
    assert await urls_status_is_running(redis_client, ctx._crawl_url)

    pending = await redis_client.xpending('crawl', 'crawlers')
    assert pending['pending'] == 1


@pytest.mark.skip(reason='deprecated')
async def test_context_switch_to_complete(
    job_service: JobsService, redis_client: Redis, conf: XtractedConfig, with_user: str
) -> None:
    ctx = await new_default_crawl_context(
        job_service=job_service,
        redis_client=redis_client,
        conf=conf,
        with_user=with_user,
    )
    assert ctx._crawl_url.status == CrawlUrlStatus.pending
    remote_url = await redis_client.hgetall(ctx._crawl_url._url_key)  # type: ignore
    assert remote_url['status'] == 'pending'

    await ctx.complete(data={'foo': 'bar'})
    assert ctx._crawl_url.status == CrawlUrlStatus.complete  # type: ignore
    remote_url = await redis_client.hgetall(ctx._crawl_url._url_key)
    assert remote_url['status'] == 'complete'
    assert await urls_status_is_complete(redis_client, ctx._crawl_url)

    pending = await redis_client.xpending('crawl', 'crawlers')
    assert pending['pending'] == 0


@pytest.mark.skip(reason='deprecated')
async def test_enqueue_add_the_url_to_the_crawl_stream(
    job_service: JobsService, redis_client: Redis, conf: XtractedConfig, with_user: str
) -> None:
    ctx = await new_default_crawl_context(
        job_service=job_service,
        redis_client=redis_client,
        conf=conf,
        with_user=with_user,
    )
    assert ctx._crawl_url.status == CrawlUrlStatus.pending
    remote_url = await redis_client.hgetall(ctx._crawl_url._url_key)  # type: ignore
    assert remote_url['status'] == 'pending'

    enqueued_url = cast(
        XtractedUrl,
        await ctx.enqueue(AnyHttpUrl('https://www.amazon.co.uk/dp/B0931VRJAA')),
    )
    assert remote_url['job_id'] == str(enqueued_url.job_id)

    # enqueue appends the url id to the job set
    members = await redis_client.smembers(
        f'u:{enqueued_url.uid}:job:{enqueued_url.job_id}:urls'
    )  # type: ignore

    assert await urls_status_is_pending(redis_client, ctx._crawl_url)
    assert await urls_status_is_pending(redis_client, enqueued_url)

    assert len(members) == 2
    assert enqueued_url._url_id in members

    # enqueue creates a stream event to the consumer group crawlers
    res = await redis_client.xreadgroup(
        groupname='crawlers',
        consumername='dummy',
        streams={'crawl': '>'},
        count=1,
        block=1000,
        noack=False,
    )

    assert len(res) == 1


@pytest.mark.skip(reason='deprecated')
async def test_enqueue_do_nothing_when_url_exists(
    job_service: JobsService, redis_client: Redis, conf: XtractedConfig, with_user: str
) -> None:
    ctx = await new_default_crawl_context(
        job_service=job_service,
        redis_client=redis_client,
        conf=conf,
        with_user=with_user,
    )
    assert ctx._crawl_url.status == CrawlUrlStatus.pending
    remote_url = await redis_client.hgetall(ctx._crawl_url._url_key)  # type: ignore
    assert remote_url['status'] == 'pending'

    enqueued_url = cast(
        XtractedUrl,
        await ctx.enqueue(AnyHttpUrl('https://www.amazon.co.uk/dp/B0931VRJT5')),
    )
    assert enqueued_url is None

    # enqueue appends the url id to the job set
    members = await redis_client.smembers(f'u:{with_user}:job:1:urls')
    assert len(members) == 1

    # enqueue creates a stream event to the consumer group crawlers
    res = await redis_client.xreadgroup(
        groupname='crawlers',
        consumername='dummy',
        streams={'crawl': '>'},
        count=1,
        block=1000,
        noack=False,
    )

    assert len(res) == 0
