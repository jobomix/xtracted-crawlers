import asyncio

from xtracted.configuration import XtractedConfigFromDotEnv
from xtracted.crawlers.crawl_job_producer import CrawlJobProducer
from xtracted.model import CrawlJobInput

config = XtractedConfigFromDotEnv()


async def _remove_keys() -> None:
    client = config.new_client()
    await client.initialize()
    await client.flushall()
    await client.aclose()


async def _send_message() -> None:
    client = config.new_client()

    producer = CrawlJobProducer(client=client)

    await producer.submit(
        CrawlJobInput(urls={'https://www.amazon.co.uk/dp/B0931VRJT5'})
    )

    await client.aclose()


def remove_keys() -> None:
    asyncio.run(_remove_keys())


def send_message() -> None:
    asyncio.run(_send_message())
