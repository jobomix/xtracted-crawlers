from pydantic import AnyUrl
from redis.asyncio import Redis
from xtracted_common.model import AmazonProductUrl, CrawlJobStatus

from xtracted.model import CrawlJobInput
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

    first_url = AmazonProductUrl(
        job_id=crawl_job.job_id,
        url='https://www.amazon.co.uk/dp/B0931VRJT5',
    )
    second_url = AmazonProductUrl(
        job_id=crawl_job.job_id,
        url='https://www.amazon.co.uk/dp/B0931VRJT6',
    )


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
        f'crawl_url:{crawl_job.job_id}:B0931VRJT5'
    )

    second_redis_hash_key = await redis_client.hgetall(  # type: ignore
        f'crawl_url:{crawl_job.job_id}:B0931VRJT6'
    )

    assert first_redis_hash_key == {
        'job_id': str(crawl_job.job_id),
        'url_id': f'crawl_url:{crawl_job.job_id}:B0931VRJT5',
        'retries': '0',
        'url': 'https://www.amazon.co.uk/dp/B0931VRJT5',
        'status': 'pending',
    }
    assert second_redis_hash_key == {
        'job_id': str(crawl_job.job_id),
        'url_id': f'crawl_url:{crawl_job.job_id}:B0931VRJT6',
        'retries': '0',
        'url': 'https://www.amazon.co.uk/dp/B0931VRJT6',
        'status': 'pending',
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


async def test_submit_crawl_jobs_creates_urls_set(
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

    urls = await redis_client.smembers(f'job_urls:{crawl_job.job_id}')  # type: ignore
    assert len(urls) == 2

    assert f'crawl_url:{crawl_job.job_id}:B0931VRJT5' in urls
    assert f'crawl_url:{crawl_job.job_id}:B0931VRJT6' in urls


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
