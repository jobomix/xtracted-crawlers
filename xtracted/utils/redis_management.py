import asyncio
import os

from xtracted_common.configuration import XtractedConfigFromDotEnv
from xtracted_common.model import CrawlJobInput

from xtracted.crawlers.crawl_job_producer import CrawlJobProducer

config = XtractedConfigFromDotEnv()


async def _remove_keys() -> None:
    client = config.new_client()
    await client.initialize()
    await client.flushall()
    await client.aclose()


async def _send_message() -> None:
    producer = CrawlJobProducer(config=config)

    await producer.submit(
        os.environ['USER_KEY'],
        CrawlJobInput(
            urls={'https://www.amazon.co.uk/dp/B0B2SDTSJ8?ref=MarsFS_TAB_sun'}
        ),
    )


async def _send_messages() -> None:
    producer = CrawlJobProducer(config=config)

    await producer.submit(
        os.environ['USER_KEY'],
        CrawlJobInput(
            urls={
                'http://localhost:8080/dp/B0BXD1PRJQ?x=foo&bar=y',
                'http://localhost:8080/dp/B0C346GMKS?x=foo&bar=y',
                'http://localhost:8080/dp/B0DD41VWT9?x=foo&bar=y',
                'http://localhost:8080/dp/B01GFPWTI4?x=foo&bar=y',
                'http://localhost:8080/dp/B09YVBJH4S?x=foo&bar=y',
                'http://localhost:8080/dp/B0BZGPTLPG?x=foo&bar=y',
                'http://localhost:8080/dp/B0CF5W257V?x=foo&bar=y',
                'http://localhost:8080/dp/B00CL6353A?x=foo&bar=y',
                'http://localhost:8080/dp/B0797JRLQC?x=foo&bar=y',
                'http://localhost:8080/dp/B09ZDPP43X?x=foo&bar=y',
                'http://localhost:8080/dp/B094R672D3?x=foo&bar=y',
                'http://localhost:8080/dp/B0CHYV6312?x=foo&bar=y',
                'http://localhost:8080/dp/B0C9D4KL9D?x=foo&bar=y',
                'http://localhost:8080/dp/B0B1J1GG9T?x=foo&bar=y',
                'http://localhost:8080/dp/B09Y58N791?x=foo&bar=y',
                'http://localhost:8080/dp/B08897N6HB?x=foo&bar=y',
                'http://localhost:8080/dp/B00N2S94ZY?x=foo&bar=y',
                'http://localhost:8080/dp/B0CZ3MV2GL?x=foo&bar=y',
                'http://localhost:8080/dp/B07Y6FH6XK?x=foo&bar=y',
                'http://localhost:8080/dp/B08HMWZBXC?x=foo&bar=y',
            }
        ),
    )


def remove_keys() -> None:
    asyncio.run(_remove_keys())


def send_message() -> None:
    asyncio.run(_send_message())


def send_messages() -> None:
    asyncio.run(_send_messages())
