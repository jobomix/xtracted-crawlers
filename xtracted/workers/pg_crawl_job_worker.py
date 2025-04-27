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


import asyncio  # noqa: I001
from xtracted.xtracted_logging import logging
from asyncpg import Connection
from tembo_pgmq_python.async_queue import PGMQueue
from tembo_pgmq_python.messages import Message
from xtracted.context import PostgresCrawlSyncer
from xtracted.crawler_configuration import CrawlerConfig, CrawlerConfigFromDotEnv
from xtracted.crawlers.extractor_factory import Extractorfactory

logger = logging.getLogger(__name__)


class PGCrawlJobWorker:
    def __init__(self, config: CrawlerConfig) -> None:
        crawl_syncer = PostgresCrawlSyncer(config)
        self.config = config
        self.tasks = set[asyncio.Task]()
        self.crawling_tasks = set[asyncio.Task]()
        self.extractor_factory = Extractorfactory(crawl_syncer)

    def run(self) -> None:
        """Starts the worker"""
        task = asyncio.create_task(self.check_for_new_job_task())
        self.tasks.add(task)
        # task.add_done_callback(self.tasks.discard)

        task = asyncio.create_task(self.check_for_new_job_urls())
        self.tasks.add(task)
        # task.add_done_callback(self.tasks.discard)

        # await asyncio.gather(*self.tasks)

    async def start(self) -> None:
        self.run()
        for task in self.tasks:
            await task

    async def cancel(self) -> None:
        for task in self.crawling_tasks.copy():
            if task.cancel():
                try:
                    await task
                    self.crawling_tasks.discard(task)
                except asyncio.CancelledError:
                    pass

        for task in self.tasks:
            if task.cancel():
                try:
                    await task
                    self.tasks.discard(task)
                except asyncio.CancelledError:
                    pass

    async def _log_job_error(self, error: Exception, db_client: Connection) -> None:
        logger.error(error)

    def crawl(self, message: Message) -> None:
        logger.debug(f'stream message received -> {message.message}')

        mapping = message.message
        mapping['retries'] = message.read_ct
        extractor = self.extractor_factory.new_instance(
            message_id=message.msg_id, mapping=message.message
        )

        if extractor:
            logger.debug(f'creating crawl task for url: {message.message["url"]}')
            crawl_task = asyncio.create_task(extractor.crawl())
            self.crawling_tasks.add(crawl_task)
            crawl_task.add_done_callback(self.crawling_tasks.discard)

    async def _handle_start_job_event(
        self, message: Message, queue: PGMQueue, db_client: Connection
    ) -> None:
        try:
            async with db_client.transaction():
                job_id = message.message['job_id']
                user_id = message.message['user_id']
                # reset all
                await db_client.execute(
                    """update job_urls set data = NULL, created_at = now(), retries = 0, status = 'pending'::public.crawl_url_status where user_id = $1 and job_id=$2""",
                    user_id,
                    job_id,
                )

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
                            'user_id': user_id,
                            'url_type': record['url_type'],
                            'url_id': record['url_id'],
                            'url': record['url'],
                            'retries': record['retries'],
                            'job_urls_seq': record['job_urls_seq'],
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
            self.crawl(message)
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
                    vt=self.config.crawler_job_vt,
                    qty=self.config.crawler_job_qty,
                    max_poll_seconds=self.config.crawler_job_max_poll_seconds,
                    poll_interval_ms=self.config.crawler_job_poll_interval_ms,
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

        while True:
            if len(self.crawling_tasks) >= self.config.crawler_max_crawl_tasks:
                await asyncio.sleep(0.5)
                logger.debug(
                    f'Number of crawling tasks: {len(self.crawling_tasks)} -> sleeping ..'
                )
            else:
                db_client = await self.config.new_db_client()
                try:
                    messages = await queue.read_with_poll(
                        'job_urls',
                        vt=self.config.crawler_url_vt,
                        qty=self.config.crawler_url_qty,
                        max_poll_seconds=self.config.crawler_url_max_poll_seconds,
                        poll_interval_ms=self.config.crawler_url_poll_interval_ms,
                        conn=db_client,
                    )
                    logger.debug(f'polling: received {len(messages)} messages')
                    if messages:
                        for msg in messages:
                            if (
                                'event' in msg.message
                                and msg.message['event'] == 'new_url'
                            ):
                                await self._handle_new_url_event(msg, queue, db_client)

                except asyncio.CancelledError:
                    logger.warning('Check for new url task cancelled')
                    raise
                finally:
                    await db_client.close()


if __name__ == '__main__':
    config = CrawlerConfigFromDotEnv()
    logger.info('************* WORKER CONFIG *************')
    logger.info(
        '{:30s} {:10d}'.format(
            'Max conccurrent crawling tasks', config.crawler_max_crawl_tasks
        )
    )
    logger.info('{:30s} {:10d}'.format('Url visibility timeout', config.crawler_url_vt))
    logger.info('{:30s} {:10d}'.format('Url messages quantity', config.crawler_url_qty))
    logger.info(
        '{:30s} {:10d}'.format(
            'Url polling in seconds', config.crawler_url_max_poll_seconds
        )
    )
    logger.info(
        '{:30s} {:10d}'.format(
            'Url poll interval im ms', config.crawler_url_poll_interval_ms
        )
    )
    logger.info('{:30s} {:10d}'.format('Job visibility timeout', config.crawler_job_vt))
    logger.info('{:30s} {:10d}'.format('Job messages quantity', config.crawler_job_qty))
    logger.info(
        '{:30s} {:10d}'.format(
            'Job polling in seconds', config.crawler_job_max_poll_seconds
        )
    )
    logger.info(
        '{:30s} {:10d}'.format(
            'Job poll interval im ms', config.crawler_job_poll_interval_ms
        )
    )
    logger.info('*****************************************')

    worker = PGCrawlJobWorker(config=config)
    asyncio.run(worker.start())
