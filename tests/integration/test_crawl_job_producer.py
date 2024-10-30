import asyncio
import pathlib
from collections.abc import Callable
from typing import Any, cast
from unittest.mock import Mock

from pydantic import AnyUrl
from redis.asyncio import StrictRedis
from redis.asyncio.client import Redis

from xtracted.crawlers.crawl_job_producer import CrawlJobProducer
from xtracted.model import CrawlJobFreeInput, CrawlJobInput, CrawlUrl, CrawlUrlStatus
from xtracted.queue import Queue
from xtracted.storage import Storage
from xtracted.workers.crawl_job_worker import CrawlJobWorker

filepath = pathlib.Path(__file__).resolve().parent.parent


async def test_crawl_job_appended_to_redis_stream(
    queue: Queue, redis_client: Redis
) -> None:
    producer = CrawlJobProducer(queue=queue)

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


async def test_crawl_job_submit_create_context(
    queue: Queue, redis_client: Redis
) -> None:
    producer = CrawlJobProducer(queue=queue)

    crawl_job = await producer.submit(
        CrawlJobInput(
            urls={
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT5'),
            }
        )
    )
    result = await redis_client.hgetall(  # type: ignore
        f'crawl_url:{crawl_job.job_id}:0'
    )
    assert result == {
        'crawl_url_id': f'crawl_url:{crawl_job.job_id}:0',
        'url': 'https://www.amazon.co.uk/dp/B0931VRJT5',
        'status': 'pending',
        'retries': '0',
    }


async def test_consumer(queue: Queue, redis_client: Redis) -> None:
    async def wait(condition: Callable[[], bool], timeout: int = 10) -> None:
        for i in range(timeout * 2):
            if not condition():
                await asyncio.sleep(0.5)
            else:
                break

    storage = Mock(spec=Storage)
    worker = CrawlJobWorker(
        client=StrictRedis(decode_responses=True),
        consumer_name='dummy',
        storage=storage,
    )
    producer = CrawlJobProducer(queue=queue)

    await worker.start()
    await asyncio.sleep(1)
    crawl_job = await producer.submit(
        CrawlJobFreeInput(
            urls={
                AnyUrl(f'file://{filepath}/en_GB/gopro.html'),
            }
        )
    )

    await wait(lambda: storage.append.call_args is not None)
    await worker.stop()

    crawl_url = cast(CrawlUrl, storage.append.call_args.args[0])
    data = cast(dict[str, Any], storage.append.call_args.args[1])
    storage.append.assert_called_once()
    assert crawl_url.crawl_url_id == f'crawl_url:{crawl_job.job_id}:0'
    assert crawl_url.status == CrawlUrlStatus.complete
    assert data['asin'] == 'B0CF7X369M'
    assert data['url'] == f'file://{filepath}/en_GB/gopro.html'
