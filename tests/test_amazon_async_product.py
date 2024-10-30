import pathlib
from unittest.mock import Mock

from xtracted.context import CrawlContext
from xtracted.crawlers.amazon.amazon_async_product import AmazonAsyncProduct
from xtracted.model import CrawlUrl

filepath = pathlib.Path(__file__).resolve().parent


async def test_extract_data_update_crawl_context() -> None:
    ctx = Mock(spec=CrawlContext)
    crawl_url = CrawlUrl(
        crawl_url_id='crawl_url:124667:0', url=f'file://{filepath}/en_GB/gopro.html'
    )
    ctx.get_crawl_url.return_value = crawl_url
    aap = AmazonAsyncProduct(crawl_context=ctx)
    await aap.crawl()
    extracted = ctx.complete.call_args.args[0]
    ctx.complete.assert_called_once()
    assert extracted['asin'] == 'B0CF7X369M'
    assert extracted['url'] == f'file://{filepath}/en_GB/gopro.html'
