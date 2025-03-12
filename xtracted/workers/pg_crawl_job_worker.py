# check for JOBS queue
# when new job received:
# change job status to running
# append all job urls to the JOB_URLS queue
# ack message JOB received

# check for JOB_URLS queue
# when new job_urls received:
# change JOB_URL status to running
# increment request attempt
# crawl job url
# decrement user requests
# change JOB_URL status to complete
# ack message JOB_URL received


import asyncio
import logging

from asyncpg import Connection
from tembo_pgmq_python.async_queue import PGMQueue
from tembo_pgmq_python.messages import Message
from xtracted_common.configuration import XtractedConfig
from xtracted_common.storage import DBStorage

from xtracted.context import PostgresCrawlSyncer
from xtracted.crawlers.extractor_factory import Extractorfactory

# logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger('crawljob-worker')


class PGCrawlJobWorker:
    def __init__(self, config: XtractedConfig) -> None:
        crawl_syncer = PostgresCrawlSyncer(config)
        storage = DBStorage(config)
        self.config = config
        self.tasks = set[asyncio.Task]()
        self.extractor_factory = Extractorfactory(storage, crawl_syncer)

    def run(self) -> None:
        """Starts the worker"""
        task = asyncio.create_task(self.check_for_new_job_task())
        self.tasks.add(task)
        # task.add_done_callback(self.tasks.discard)

        task = asyncio.create_task(self.check_for_new_job_urls())
        self.tasks.add(task)
        # task.add_done_callback(self.tasks.discard)

        # await asyncio.gather(*self.tasks)

    async def cancel(self) -> None:
        for task in self.tasks:
            if task.cancel():
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _log_job_error(self, error: Exception, db_client: Connection) -> None:
        print(error)

    async def _handle_start_job_event(
        self, message: Message, queue: PGMQueue, db_client: Connection
    ) -> None:
        try:
            async with db_client.transaction():
                job_id = message.message['job_id']
                user_id = message.message['user_id']
                async for record in db_client.cursor(
                    'SELECT * from job_urls where job_id = $1 and user_id = $2',
                    job_id,
                    user_id,
                ):
                    await queue.send(
                        'job_urls',
                        {
                            'event': 'new_url',
                            'job_id': job_id,
                            'user_uid': user_id,
                            'url_id': record['url_id'],
                            'url': record['url'],
                        },
                        conn=db_client,
                    )
                    await db_client.execute(
                        """update jobs set job_status = $1 where job_id = $2 and user_id = $3""",
                        'running',
                        job_id,
                        user_id,
                    )
                    await queue.archive('jobs', msg_id=message.msg_id, conn=db_client)
        except Exception as e:
            # await self._log_job_error(e, db_client)
            logger.error(e)
            if message.read_ct >= 3:
                await queue.archive(message)

    async def _handle_new_url_event(
        self, message: Message, queue: PGMQueue, db_client: Connection
    ) -> None:
        try:
            logger.debug(message)
        except Exception as e:
            logger.error(e)
            if message.read_ct >= 3:
                await queue.archive(message)

    async def check_for_new_job_task(self) -> None:
        queue = await self.config.new_pgmq_client()

        while True:
            db_client = await self.config.new_db_client()
            try:
                messages = await queue.read_with_poll(
                    'jobs',
                    vt=10,
                    qty=1,
                    max_poll_seconds=2,
                    poll_interval_ms=100,
                    conn=db_client,
                )

                if messages:
                    for msg in messages:
                        await self._handle_start_job_event(msg, queue, db_client)

            except asyncio.CancelledError:
                logger.warning('Check for new job task cancelled')
                raise
            finally:
                await db_client.close()

    async def check_for_new_job_urls(self) -> None:
        queue = await self.config.new_pgmq_client()
        db_client = await self.config.new_db_client()

        while True:
            try:
                messages = await queue.read_with_poll(
                    'job_urls',
                    vt=30,
                    qty=1,
                    max_poll_seconds=1,
                    poll_interval_ms=100,
                    conn=db_client,
                )
                if messages:
                    for msg in messages:
                        if 'event' in msg.message and msg.message['event'] == 'new_url':
                            await self._handle_new_url_event(msg, queue, db_client)

            except asyncio.CancelledError:
                logger.warning('Check for new url task cancelled')
                raise
            finally:
                await db_client.close()
