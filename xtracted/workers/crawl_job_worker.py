import asyncio
from asyncio import Task

from redis.asyncio.client import Redis
from redis.asyncio.cluster import RedisCluster

from xtracted.configuration import XtractedConfig


class CrawlJobWorker:
    def __init__(self, *, client: Redis | RedisCluster) -> None:
        self.started = False
        self.client = client
        self.background_tasks = set[Task]()

    async def start(self) -> None:
        self.started = True
        consuming = asyncio.create_task(self.consume())
        self.background_tasks.add(consuming)
        consuming.add_done_callback(self.background_tasks.discard)

    async def main(self) -> None:
        await self.start()
        bg_task = self.background_tasks.pop()
        await bg_task

    async def consume(self) -> None:
        while True:
            print('Waiting for new event ...')
            r = await self.client.xread(streams={'crawl': '$'}, count=1, block=10000)
            if len(r) == 1:
                res = r[0][1]
                data = res[0][1]
                url = data['url']
                job_id = data['job_id']
                context = await self.client.hgetall(f'job:{job_id}:{url}')  # type: ignore
                context['status'] = 'STARTED'
                await self.client.hset(name=f'job:{job_id}:{url}', mapping=context)  # type: ignore
                # long running job ...
            if not self.started:
                await self.client.aclose()
                break

    async def stop(self) -> None:
        self.started = False


if __name__ == '__main__':
    config = XtractedConfig()
    worker = CrawlJobWorker(
        client=RedisCluster.from_url(
            str(config.redis_cluster_url), decode_responses=True
        )
    )
    asyncio.run(worker.main())
