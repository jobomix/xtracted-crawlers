from typing import Any
from unittest.mock import Mock

from xtracted_common.model import AmazonProductUrl

from tests.integration.amazon_server import new_web_app
from xtracted.context import CrawlContext
from xtracted.crawlers.amazon.amazon_async_product import AmazonAsyncProduct


async def test_extract_data_update_crawl_context(aiohttp_server: Any) -> None:
    server = await aiohttp_server(new_web_app())

    ctx = Mock(spec=CrawlContext)
    crawler_url = AmazonProductUrl(
        job_id='124667',
        url=f'http://localhost:{server.port}/dp/B0CX9DVZDP?x=foo&bar=y',
        uid='dummy-uid',
    )
    ctx.get_crawler_url.return_value = crawler_url
    aap = AmazonAsyncProduct(crawl_context=ctx)
    await aap.crawl()
    extracted = ctx.complete.call_args.args[0]
    ctx.complete.assert_called_once()
    assert extracted['asin'] == 'B0CX9DVZDP'
    assert (
        extracted['url'] == f'http://localhost:{server.port}/dp/B0CX9DVZDP?x=foo&bar=y'
    )
    assert extracted['variants'] is not None
    assert len(extracted['variants']) == 5


# async def test_extract_real_data(aiohttp_server: Any) -> None:
#     ctx = Mock(spec=CrawlContext)
#     crawler_url = AmazonProductUrl(
#         job_id='124667',
#         url=f'https://www.amazon.co.uk/Apple-iPad-11-inch-Display-All-Day/dp/B0DZ77X9FQ',
#         uid='dummy-uid',
#     )
#     ctx.get_crawler_url.return_value = crawler_url
#     aap = AmazonAsyncProduct(crawl_context=ctx)
#     await aap.crawl()
#     extracted = ctx.complete.call_args.args[0]
#     ctx.complete.assert_called_once()
#     print(extracted['variants'])


async def test_with_failing_product(aiohttp_server: Any) -> None:
    server = await aiohttp_server(new_web_app())

    ctx = Mock(spec=CrawlContext)
    crawler_url = AmazonProductUrl(
        job_id='124667',
        url=f'http://localhost:{server.port}/dp/B0BXD1PRJQ?x=foo&bar=y',
        uid='dummy-uid',
    )
    ctx.get_crawler_url.return_value = crawler_url
    aap = AmazonAsyncProduct(crawl_context=ctx)
    await aap.crawl()
    ctx.fail.assert_called_once()
