import asyncio
import logging
from asyncio import CancelledError, Task

from redis.asyncio import ResponseError
from redis.asyncio.client import Redis
from redis.asyncio.cluster import RedisCluster

from xtracted.configuration import XtractedConfigFromDotEnv
from xtracted.context import DefaultCrawlContext, RedisCrawlSyncer
from xtracted.crawlers.amazon.amazon_async_product import AmazonAsyncProduct
from xtracted.model import AmazonProductUrl
from xtracted.storage import Storage, TempFileStorage

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger('redis-worker')


class CrawlJobWorker:
    def __init__(
        self,
        *,
        client: Redis | RedisCluster,
        consumer_name: str,
        storage: Storage,
    ) -> None:
        self.client = client
        self.consumer_name = consumer_name
        self.background_tasks = set[Task]()
        self.storage = storage

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

    async def _create_crawlers_group(self) -> None:
        try:
            await self.client.xinfo_stream('crawl')
        except ResponseError as e:
            if e.args and e.args[0] == 'no such key':
                logger.info('crawler group "crawlers" does not exist. Creating...')
                await self.client.xgroup_create('crawl', 'crawlers', mkstream=True)
                logger.info('crawler group "crawlers" created.')

    async def consume(self) -> None:
        await self._create_crawlers_group()
        try:
            while True:
                res = await self.client.xreadgroup(
                    groupname='crawlers',
                    consumername=self.consumer_name,
                    streams={'crawl': '>'},
                    count=1,
                    block=5000,
                    noack=False,
                )

                if res and len(res) == 1:
                    logger.info('stream message received')
                    x = res[0][1]
                    message_id = x[0][0]
                    mapping = x[0][1]

                    crawl_url = AmazonProductUrl(**mapping)

                    async_product = AmazonAsyncProduct(
                        crawl_context=DefaultCrawlContext(
                            storage=self.storage,
                            message_id=message_id,
                            crawl_url=crawl_url,
                            crawl_syncer=RedisCrawlSyncer(
                                redis=self.client,
                                crawl_url=crawl_url,
                                message_id=message_id,
                            ),
                        )
                    )

                    await async_product.crawl()
                    logger.info(f'ack sent for url: {mapping["url"]}')

        except CancelledError:
            await self.client.aclose()
            logger.info('consume task cancelled.Connection closed')

    async def stop(self) -> None:
        self.task.cancel()
        logger.info('stopping cralwer')


if __name__ == '__main__':
    config = XtractedConfigFromDotEnv()
    worker = CrawlJobWorker(
        client=config.new_client(), consumer_name='dummy', storage=TempFileStorage()
    )
    with asyncio.Runner() as runner:
        try:
            runner.run(worker.main())
        except KeyboardInterrupt:
            pass
