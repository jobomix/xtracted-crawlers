import asyncio
from typing import Optional

from pydantic import AnyUrl
from redis.asyncio import Redis

from xtracted.model import CrawlJobInput, CrawlJobStatus, CrawlUrl
from xtracted.queue import Queue


async def test_submit_crawl_job_creates_a_crawl_job(
    queue: Queue, redis_client: Redis
) -> None:
    crawl_job = await queue.submit_crawl_job(
        CrawlJobInput(
            urls={
                'https://www.amazon.co.uk/dp/B0931VRJT5',
                'https://www.amazon.co.uk/dp/B0931VRJT6',
            }
        )
    )

    first_url = CrawlUrl(
        crawl_url_id=f'crawl_url:{crawl_job.job_id}:0',
        url='https://www.amazon.co.uk/dp/B0931VRJT5',
    )
    second_url = CrawlUrl(
        crawl_url_id=f'crawl_url:{crawl_job.job_id}:1',
        url='https://www.amazon.co.uk/dp/B0931VRJT6',
    )

    assert crawl_job.status == CrawlJobStatus.pending
    assert len(crawl_job.urls) == 2
    assert first_url in crawl_job.urls
    assert second_url in crawl_job.urls


async def test_submit_crawl_job_creates_metadata(
    queue: Queue, redis_client: Redis
) -> None:
    crawl_job = await queue.submit_crawl_job(
        CrawlJobInput(
            urls={
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT5'),
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT6'),
            }
        )
    )
    first_redis_hash_key = await redis_client.hgetall(  # type: ignore
        f'crawl_url:{crawl_job.job_id}:0'
    )

    second_redis_hash_key = await redis_client.hgetall(  # type: ignore
        f'crawl_url:{crawl_job.job_id}:1'
    )

    assert first_redis_hash_key == {
        'crawl_url_id': f'crawl_url:{crawl_job.job_id}:0',
        'retries': '0',
        'url': 'https://www.amazon.co.uk/dp/B0931VRJT5',
        'status': 'pending',
        'extracted': '{}',
    }
    assert second_redis_hash_key == {
        'crawl_url_id': f'crawl_url:{crawl_job.job_id}:1',
        'retries': '0',
        'url': 'https://www.amazon.co.uk/dp/B0931VRJT6',
        'status': 'pending',
        'extracted': '{}',
    }


async def test_submit_crawl_job_create_streams(
    queue: Queue, redis_client: Redis
) -> None:
    await queue.submit_crawl_job(
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


async def test_submit_crawl_jobs_creates_pending_ordered_set(
    queue: Queue, redis_client: Redis
) -> None:
    crawl_job = await queue.submit_crawl_job(
        CrawlJobInput(
            urls={
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT5'),
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT6'),
            }
        )
    )

    cnt = await redis_client.zcount(f'job:{crawl_job.job_id}:pending', 0, 3)
    assert cnt == 2

    first = await redis_client.zmpop(
        1, [f'job:{crawl_job.job_id}:pending'], min=True, count=1
    )  # type: ignore
    assert first[0] == f'job:{crawl_job.job_id}:pending'
    assert first[1] == [['0', '0']]

    second = await redis_client.zmpop(
        1, [f'job:{crawl_job.job_id}:pending'], min=True, count=1
    )  # type: ignore
    assert second[0] == f'job:{crawl_job.job_id}:pending'
    assert second[1] == [['1', '1']]


async def test_get_job_by_id_retrieves_urls(queue: Queue, redis_client: Redis) -> None:
    crawl_job = await queue.submit_crawl_job(
        CrawlJobInput(
            urls={
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT5'),
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT6'),
            }
        )
    )

    retrieved_job = await queue.get_crawl_job(crawl_job.job_id)
    assert retrieved_job is not None
    assert retrieved_job.job_id == crawl_job.job_id
    assert len(retrieved_job.urls) == 2


async def test_consume(queue: Queue, redis_client: Redis) -> None:
    async def readQueue() -> Optional[CrawlUrl]:
        await asyncio.sleep(1)
        return await queue.consume('nono')

    task = asyncio.create_task(readQueue())

    await queue.submit_crawl_job(
        CrawlJobInput(
            urls={
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT5'),
                AnyUrl('https://www.amazon.co.uk/dp/B0931VRJT6'),
            }
        )
    )

    await task

    result = task.result()
    assert result is not None

    if result:
        assert result.crawl_url_id[-1] == '0'
        assert result.url.__str__() == 'https://www.amazon.co.uk/dp/B0931VRJT5'
