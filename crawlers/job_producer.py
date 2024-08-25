import uuid

import redis.asyncio as redis

from crawlers.model import CrawlJobInput


async def submit(job: CrawlJobInput) -> str:
    client = redis.Redis()

    job_id = str(uuid.uuid1())

    # add those urls to the main crawl stream
    for url in job.urls:
        await client.hset(  # type: ignore
            name=f'job:{job_id}:{url.__str__()}',
            mapping={'attempts': 0, 'status': 'NEW'},
        )
        await client.xadd(name='crawl', fields={'url': url.__str__(), 'job_id': job_id})
    await client.aclose()

    return job_id
