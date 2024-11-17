import asyncio
import pathlib
from typing import Any, cast
from unittest.mock import Mock

from pydantic import AnyUrl
from redis.asyncio import StrictRedis
from redis.asyncio.client import Redis
from xtracted_common.configuration import XtractedConfig
from xtracted_common.model import (
    AmazonProductUrl,
    CrawlJobInput,
    CrawlUrlStatus,
)
from xtracted_common.storage import Storage

from tests.integration.amazon_server import new_web_app
from tests.utils.common import wait
from xtracted.crawlers.crawl_job_producer import CrawlJobProducer
from xtracted.workers.crawl_job_worker import CrawlJobWorker

filepath = pathlib.Path(__file__).resolve().parent.parent


async def test_crawl_job_worker_happy_path(
    conf: XtractedConfig, redis_client: Redis, aiohttp_server: Any
) -> None:
    server = await aiohttp_server(new_web_app())

    storage = Mock(spec=Storage)
    worker = CrawlJobWorker(
        client=StrictRedis(decode_responses=True),
        consumer_name='dummy',
        storage=storage,
    )
    producer = CrawlJobProducer(config=conf)

    await worker.start()
    await asyncio.sleep(1)
    await producer.submit(
        'dummy-uid',
        CrawlJobInput(
            urls={
                AnyUrl(f'http://localhost:{server.port}/dp/B01GFPWTI4?x=foo&bar=y'),
            }
        ),
    )

    await wait(lambda: storage.append.call_args is not None)
    await worker.stop()

    crawl_url = cast(AmazonProductUrl, storage.append.call_args.args[0])
    data = cast(dict[str, Any], storage.append.call_args.args[1])
    storage.append.assert_called_once()
    assert crawl_url.uid == 'dummy-uid'
    assert crawl_url.job_id == 1
    assert crawl_url.url_id == 'B01GFPWTI4'  # type: ignore
    assert crawl_url.status == CrawlUrlStatus.complete
    assert data['asin'] == 'B01GFPWTI4'
    assert data['url'] == f'http://localhost:{server.port}/dp/B01GFPWTI4?x=foo&bar=y'
