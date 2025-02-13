import asyncio
import pathlib
from typing import Any, AsyncGenerator, cast
from unittest.mock import Mock

import pytest
from redis.asyncio.client import Redis
from xtracted_common.configuration import XtractedConfig
from xtracted_common.model import (
    AmazonProductUrl,
    CrawlJobInput,
    CrawlUrlStatus,
)
from xtracted_common.storage import DBStorage, Storage

from tests.conftest import TConfig
from tests.integration.amazon_server import new_web_app
from tests.utils.common import wait
from xtracted.crawlers.crawl_job_producer import CrawlJobProducer
from xtracted.workers.crawl_job_worker import CrawlJobWorker

filepath = pathlib.Path(__file__).resolve().parent.parent


class TConfig5Threads(TConfig):
    max_tasks_per_worker: int = 5


@pytest.fixture(scope='function')
async def conf5threads() -> AsyncGenerator[XtractedConfig, Any]:
    yield TConfig5Threads()


async def test_crawl_job_worker_happy_path(
    conf: XtractedConfig, redis_client: Redis, aiohttp_server: Any, with_user: str
) -> None:
    server = await aiohttp_server(new_web_app())

    storage = Mock(spec=Storage)
    worker = CrawlJobWorker(config=conf, consumer_name='dummy', storage=storage)
    producer = CrawlJobProducer(config=conf)

    await worker.start()
    await asyncio.sleep(1)
    await producer.submit(
        with_user,
        CrawlJobInput(
            urls={
                f'http://localhost:{server.port}/dp/B01GFPWTI4?x=foo&bar=y',
            }
        ),
    )
    await asyncio.sleep(2)
    await worker.stop()

    crawl_url = cast(AmazonProductUrl, storage.append_crawled_data.call_args.args[0])
    data = cast(dict[str, Any], storage.append_crawled_data.call_args.args[1])
    storage.append_crawled_data.assert_called_once()
    assert crawl_url.uid == with_user
    assert crawl_url.job_id == 1
    assert crawl_url._url_id == 'B01GFPWTI4'
    assert crawl_url.status == CrawlUrlStatus.complete
    assert data['asin'] == 'B01GFPWTI4'
    assert data['url'] == f'http://localhost:{server.port}/dp/B01GFPWTI4?x=foo&bar=y'


async def test_crawl_job_worker_and_max_threads_per_worker(
    conf5threads: XtractedConfig,
    redis_client: Redis,
    aiohttp_server: Any,
    with_user: str,
) -> None:
    server = await aiohttp_server(new_web_app())

    storage = Mock(spec=Storage)
    worker = CrawlJobWorker(config=conf5threads, consumer_name='dummy', storage=storage)
    producer = CrawlJobProducer(config=conf5threads)

    await worker.start()
    await asyncio.sleep(1)
    await producer.submit(
        with_user,
        CrawlJobInput(
            urls={
                f'http://localhost:{server.port}/dp/B01GFPWTI4?x=foo&bar=y',
                f'http://localhost:{server.port}/dp/B00CL6353A?x=foo&bar=y',
                f'http://localhost:{server.port}/dp/B00N2S94ZY?x=foo&bar=y',
                f'http://localhost:{server.port}/dp/B07Y6FH6XK?x=foo&bar=y',
                f'http://localhost:{server.port}/dp/B08897N6HB?x=foo&bar=y',
                f'http://localhost:{server.port}/dp/B08HMWZBXC?x=foo&bar=y',
                f'http://localhost:{server.port}/dp/B094R672D3?x=foo&bar=y',
                f'http://localhost:{server.port}/dp/B09Y58N791?x=foo&bar=y',
                f'http://localhost:{server.port}/dp/B09YVBJH4S?x=foo&bar=y',
                f'http://localhost:{server.port}/dp/B09ZDPP43X?x=foo&bar=y',
            }
        ),
    )

    await wait(lambda: storage.append_crawled_data.call_count == 10)
    await worker.stop()
