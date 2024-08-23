import redis.asyncio as redis

from crawlers.model import CrawlJobInput2


async def submit(job: CrawlJobInput2) -> None:
    client = redis.Redis()
    for url in job.urls:
        await client.xadd(name=f'crawljob:{job.job_id}', fields={'url': str(url)})
    await client.aclose()
