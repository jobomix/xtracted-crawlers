import asyncio
import logging
from asyncio import CancelledError, Task
from typing import Any, Optional

from redis.asyncio import ResponseError
from xtracted_common.configuration import XtractedConfig, XtractedConfigFromDotEnv
from xtracted_common.storage import DBStorage, Storage

from xtracted.context import RedisCrawlSyncer
from xtracted.crawlers.extractor_factory import Extractorfactory

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger('redis-worker')


class CrawlJobWorker:
    def __init__(
        self,
        *,
        config: XtractedConfig,
        consumer_name: str,
        storage: Optional[Storage] = None,
    ) -> None:
        client = config.new_client()
        crawl_syncer = RedisCrawlSyncer(redis=client)
        store = storage if storage else DBStorage(config)
        self.config = config
        self.client = client
        self.consumer_name = consumer_name
        self.background_tasks = set[Task]()
        self.crawling_tasks = set[Task]()
        self.storage = storage
        self.crawl_syncer = crawl_syncer
        self.extractor_factory = Extractorfactory(
            storage=store, crawl_syncer=crawl_syncer
        )

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

    def crawl(self, res: Any) -> None:
        logger.info('stream message received')
        crawl_msgs = res[0][1]
        for message_id, mapping in crawl_msgs:
            extractor = self.extractor_factory.new_instance(
                message_id=message_id, mapping=mapping
            )

            if extractor:
                print(f"createing crawl task for url: {mapping['url']}")
                crawl_task = asyncio.create_task(extractor.crawl())
                self.crawling_tasks.add(crawl_task)
                crawl_task.add_done_callback(self.crawling_tasks.discard)

    async def consume(self) -> None:
        await self._create_crawlers_group()
        try:
            while True:
                while len(self.crawling_tasks) > self.config.max_tasks_per_worker:
                    logger.info(
                        f'Number concurrent tasks running for worker {self.consumer_name}: {len(self.crawling_tasks)} / {self.config.max_tasks_per_worker}'
                    )
                    await asyncio.sleep(1)

                res = await self.client.xreadgroup(
                    groupname='crawlers',
                    consumername=self.consumer_name,
                    streams={'crawl': '>'},
                    count=self.config.max_tasks_per_worker,
                    block=5000,
                    noack=False,
                )

                if res and len(res) == 1:
                    self.crawl(res)
        except CancelledError:
            await self.client.aclose()
            logger.info('consume task cancelled.Connection closed')

    async def stop(self) -> None:
        self.task.cancel()
        logger.info('stopping cralwer')


if __name__ == '__main__':
    config = XtractedConfigFromDotEnv()
    worker = CrawlJobWorker(config=config, consumer_name='dummy')
    with asyncio.Runner() as runner:
        try:
            runner.run(worker.main())
        except KeyboardInterrupt:
            pass
