from redis.asyncio.client import Redis

from crawlers.job_producer import submit
from crawlers.model import CrawlJobInput2


async def test_crawl_job_appended_to_redis(redis_client: Redis) -> None:
    await submit(
        CrawlJobInput2(job_id='1', urls={'https://www.amazon.co.uk/dp/B0931VRJT5'})
    )
    result = await redis_client.xrange(name='crawljob:1')
    assert result is not None
