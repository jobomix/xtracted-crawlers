import asyncio
import logging
from asyncio import CancelledError, Task

from redis.asyncio.client import Redis
from redis.asyncio.cluster import RedisCluster

from xtracted.configuration import XtractedConfigFromDotEnv

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger('redis-worker')


class CrawlJobWorker:
    def __init__(self, *, client: Redis | RedisCluster) -> None:
        self.client = client
        self.background_tasks = set[Task]()

    async def start(self) -> None:
        t = asyncio.create_task(self.consume())
        self.background_tasks.add(t)
        t.add_done_callback(self.background_tasks.discard)
        logger.info('crawler started')
        self.task = t

    async def main(self) -> None:
        await self.start()
        bg_task = self.background_tasks.pop()
        await bg_task

    async def consume(self) -> None:
        try:
            while True:
                r = await self.client.xread(streams={'crawl': '$'}, count=1, block=5000)
                if len(r) == 1:
                    logger.info('Received new job event')
                    res = r[0][1]
                    data = res[0][1]
                    url = data['url']
                    job_id = data['job_id']
                    context = await self.client.hgetall(f'job:{job_id}:{url}')  # type: ignore
                    context['status'] = 'STARTED'
                    await self.client.hset(name=f'job:{job_id}:{url}', mapping=context)  # type: ignore
                    # long running job ...
        except CancelledError:
            await self.client.aclose()
            logger.info('consume task cancelled.Connection closed')

    async def stop(self) -> None:
        self.task.cancel()
        logger.info('stopping cralwer')


if __name__ == '__main__':
    config = XtractedConfigFromDotEnv()
    worker = CrawlJobWorker(client=config.new_client())
    with asyncio.Runner() as runner:
        try:
            runner.run(worker.main())
        except KeyboardInterrupt:
            pass
