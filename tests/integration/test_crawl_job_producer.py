import asyncio

from pydantic import AnyUrl
from redis.asyncio import StrictRedis
from redis.asyncio.client import Redis

from xtracted.crawlers.crawl_job_producer import CrawlJobProducer
from xtracted.model import CrawlJobInput
from xtracted.workers.crawl_job_worker import CrawlJobWorker


async def test_crawl_job_appended_to_redis_stream(redis_client: Redis) -> None:
    producer = CrawlJobProducer(client=redis_client)

    await producer.submit(
        CrawlJobInput(
            urls={
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT5'),
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT6'),
            }
        )
    )
    result = await redis_client.xread({'crawl': 0}, 100, 1000)
    assert len(result) == 1
    first = result[0][1]
    assert first[0][1]['url'] == 'https://www.amazon.co.uk/dp/B0931VRJT5'
    assert first[1][1]['url'] == 'https://www.amazon.co.uk/dp/B0931VRJT6'


async def test_crawl_job_submit_create_context(redis_client: Redis) -> None:
    producer = CrawlJobProducer(client=redis_client)

    job_id = await producer.submit(
        CrawlJobInput(
            urls={
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT5'),
            }
        )
    )
    result = await redis_client.hgetall(  # type: ignore
        f'job:{job_id}:https://www.amazon.co.uk/dp/B0931VRJT5'
    )
    assert result == {'attempts': '0', 'status': 'NEW'}


async def test_consumer(redis_client: Redis) -> None:
    worker = CrawlJobWorker(client=StrictRedis(decode_responses=True))
    producer = CrawlJobProducer(client=redis_client)

    await worker.start()
    await asyncio.sleep(1)
    job_id = await producer.submit(
        CrawlJobInput(
            urls={
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT5'),
            }
        )
    )
    await asyncio.sleep(1)
    await worker.stop()
    context = await redis_client.hgetall(
        f'job:{job_id}:https://www.amazon.co.uk/dp/B0931VRJT5'
    )  # type: ignore
    assert context == {'attempts': '0', 'status': 'STARTED'}
