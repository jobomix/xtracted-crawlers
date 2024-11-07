from typing import cast
from unittest.mock import Mock

from pydantic import AnyHttpUrl, AnyUrl
from pytest import fail
from redis.asyncio import Redis, ResponseError

from xtracted.context import DefaultCrawlContext, RedisCrawlSyncer
from xtracted.crawlers.crawl_job_producer import CrawlJobProducer
from xtracted.model import AmazonProductUrl, CrawlJobInput, CrawlUrlStatus, XtractedUrl
from xtracted.queue import Queue
from xtracted.storage import Storage


async def new_default_crawl_context(
    queue: Queue, redis_client: Redis
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
    producer = CrawlJobProducer(queue=queue)
    crawl_job = await producer.submit(
        CrawlJobInput(
            urls={
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT5'),
            }
        )
    )

    crawl_url = AmazonProductUrl(
        url='https://www.amazon.co.uk/dp/B0931VRJT5', job_id=crawl_job.job_id
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


async def test_fail_replay_the_same_crawl_if_less_than_3_attempts(
    queue: Queue, redis_client: Redis
) -> None:
    ctx = await new_default_crawl_context(queue=queue, redis_client=redis_client)
    assert ctx._crawl_url.status == CrawlUrlStatus.pending
    remote_url = await redis_client.hgetall(ctx._crawl_url.url_id)  # type: ignore
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


async def test_context_switch_to_running(queue: Queue, redis_client: Redis) -> None:
    ctx = await new_default_crawl_context(queue=queue, redis_client=redis_client)
    assert ctx._crawl_url.status == CrawlUrlStatus.pending
    remote_url = await redis_client.hgetall(ctx._crawl_url.url_id)  # type: ignore
    assert remote_url['status'] == 'pending'

    await ctx.set_running()
    assert ctx._crawl_url.status == CrawlUrlStatus.running  # type: ignore
    remote_url = await redis_client.hgetall(ctx._crawl_url.url_id)
    assert remote_url['status'] == 'running'

    pending = await redis_client.xpending('crawl', 'crawlers')
    assert pending['pending'] == 1


async def test_context_switch_to_complete(queue: Queue, redis_client: Redis) -> None:
    ctx = await new_default_crawl_context(queue=queue, redis_client=redis_client)
    assert ctx._crawl_url.status == CrawlUrlStatus.pending
    remote_url = await redis_client.hgetall(ctx._crawl_url.url_id)  # type: ignore
    assert remote_url['status'] == 'pending'

    await ctx.complete(data={'foo': 'bar'})
    assert ctx._crawl_url.status == CrawlUrlStatus.complete  # type: ignore
    remote_url = await redis_client.hgetall(ctx._crawl_url.url_id)
    assert remote_url['status'] == 'complete'

    pending = await redis_client.xpending('crawl', 'crawlers')
    assert pending['pending'] == 0


async def test_enqueue_add_the_url_to_the_crawl_stream(
    queue: Queue, redis_client: Redis
) -> None:
    ctx = await new_default_crawl_context(queue=queue, redis_client=redis_client)
    assert ctx._crawl_url.status == CrawlUrlStatus.pending
    remote_url = await redis_client.hgetall(ctx._crawl_url.url_id)  # type: ignore
    assert remote_url['status'] == 'pending'

    enqueued_url = cast(
        XtractedUrl,
        await ctx.enqueue(AnyHttpUrl('https://www.amazon.co.uk/dp/B0931VRJAA')),
    )
    assert remote_url['job_id'] == enqueued_url.job_id

    # enqueue appends the url id to the job set
    members = await redis_client.smembers(f'job:{enqueued_url.job_id}')  # type: ignore

    assert len(members) == 2
    assert enqueued_url.url_id in members

    # enqueue creates a stream even to the consumer group crawlers
    res = await redis_client.xreadgroup(
        groupname='crawlers',
        consumername='dummy',
        streams={'crawl': '>'},
        count=1,
        block=1000,
        noack=False,
    )

    assert len(res) == 1


async def test_enqueue_do_nmothing_when_url_exists(
    queue: Queue, redis_client: Redis
) -> None:
    ctx = await new_default_crawl_context(queue=queue, redis_client=redis_client)
    assert ctx._crawl_url.status == CrawlUrlStatus.pending
    remote_url = await redis_client.hgetall(ctx._crawl_url.url_id)  # type: ignore
    assert remote_url['status'] == 'pending'

    enqueued_url = cast(
        XtractedUrl,
        await ctx.enqueue(AnyHttpUrl('https://www.amazon.co.uk/dp/B0931VRJT5')),
    )
    assert enqueued_url is None

    # enqueue appends the url id to the job set
    members = await redis_client.smembers(f'job:{remote_url["job_id"]}')

    assert len(members) == 1

    # enqueue creates a stream even to the consumer group crawlers
    res = await redis_client.xreadgroup(
        groupname='crawlers',
        consumername='dummy',
        streams={'crawl': '>'},
        count=1,
        block=1000,
        noack=False,
    )

    assert len(res) == 0
