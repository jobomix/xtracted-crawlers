import asyncio
import logging
from typing import Any, Optional
from urllib.parse import urlparse
from uuid import UUID

from camoufox.async_api import AsyncCamoufox
from playwright.async_api import Browser, BrowserContext, Page
from pydantic import HttpUrl

from xtracted.context import CrawlContext, CrawlSyncer, DefaultCrawlContext
from xtracted.model import CrawlerUrl, Extractor

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger('amazon-async-crawler')


class AmazonAsyncProduct(Extractor):
    def __init__(self, *, crawl_context: CrawlContext):
        self.crawl_context = crawl_context

    @staticmethod
    def extract_root_url(url: str) -> Optional[str]:
        parsed_url = urlparse(url)
        if parsed_url.scheme.startswith('http'):
            return f'{parsed_url.scheme}://{parsed_url.netloc}'
        return None

    async def extract_variants(self, page: Page) -> dict[str, Any]:
        href = await page.evaluate('document.location.href')
        root_url = AmazonAsyncProduct.extract_root_url(href)

        matrix = await page.evaluate(
            'mw:window.twisterController.twisterModel.twisterJSInitData'
        )

        # scripts = await page.evaluate('document.scripts["100"].textContent')
        #
        # logger.error(scripts)

        result = {}
        if 'num_total_variations' in matrix:
            result['variants_count'] = matrix['num_total_variations']
        if 'current_asin' in matrix:
            result['current_asin'] = matrix['current_asin']
        if 'parent_asin' in matrix:
            result['parent_asin'] = matrix['parent_asin']
        if 'variationDisplayLabels' in matrix:
            result['variationDisplayLabels'] = matrix['variationDisplayLabels']

        if 'dimensionValuesDisplayData' in matrix and 'dimensionsDisplay' in matrix:
            variants = []
            dimension_display = matrix['dimensionsDisplay']
            for variant in matrix['dimensionValuesDisplayData']:
                detail = []
                display_data = matrix['dimensionValuesDisplayData'][variant]
                for idx, _ in enumerate(dimension_display):
                    detail.append({dimension_display[idx]: display_data[idx]})
                variants.append(
                    {
                        'asin': variant,
                        'detail': detail,
                        'url': f'{"" if root_url is None else root_url}/dp/{variant}?psc=1',
                    }
                )
            result['variants'] = variants
        return result

    async def extract_asin(self, page: Page) -> Any:
        return await page.locator('#averageCustomerReviews').first.get_attribute(
            'data-asin', timeout=5000
        )

    async def extract_feature_bullets(self, page: Page) -> list[str]:
        res = []
        for element in await page.locator('#feature-bullets ul li').all():
            text_content = await element.text_content()
            if text_content:
                res.append(text_content.strip())
        return res

    async def extract_variations_matrix(self, page: Page) -> dict[str, Any]:
        try:
            for i in range(2):
                try:
                    return await self.extract_variants(page)
                except Exception:
                    await asyncio.sleep(1)
            raise Exception('cannot retrieve variation matrix')
        except Exception as e:
            logger.error(e)
            return {}

    async def extract(self, page: Page) -> dict[str, Any]:
        crawl_url = self.crawl_context.get_crawler_url()
        await page.goto(str(crawl_url.url), wait_until='load')
        asin = await self.extract_asin(page)
        feature_bullets = await self.extract_feature_bullets(page)
        variants = await self.extract_variations_matrix(page)
        extracted = {}
        extracted['asin'] = asin
        extracted['feature_bullets'] = feature_bullets
        extracted['url'] = str(self.crawl_context.get_crawler_url().url)
        extracted['variants'] = variants
        return extracted

    async def run(self, browser: Browser | BrowserContext) -> None:
        try:
            page = await browser.new_page()
            extracted = await self.extract(page)
            await self.crawl_context.complete(extracted)
        except Exception as e:
            logger.error('Error occurred')
            await self.crawl_context.fail(e)
        finally:
            await browser.close()

    async def crawl(self) -> None:
        try:
            await self.crawl_context.set_running()
            async with AsyncCamoufox(main_world_eval=True) as browser:  # type: ignore
                await self.run(browser)
        except Exception as e:
            logger.error('Error occurred', e)


if __name__ == '__main__':

    class DummyCrawlSyncer(CrawlSyncer):
        async def ack(self, message_id: str | int) -> None:
            pass

        async def sync(self, crawl_url: CrawlerUrl) -> None:
            pass

        async def report_error(
            self, crawl_url: CrawlerUrl, msg_id: str | int, error: Exception
        ) -> None:
            pass

        async def enqueue(
            self, user_id: UUID, job_id: int, url: HttpUrl
        ) -> Optional[CrawlerUrl]:
            return None

        async def complete(
            self, crawl_url: CrawlerUrl, msg_id: int | str, data: dict[str, Any]
        ) -> None:
            return None

    aap = AmazonAsyncProduct(
        crawl_context=DefaultCrawlContext(
            crawl_syncer=DummyCrawlSyncer(),
            crawler_url=CrawlerUrl(
                user_id='dummy-uid',
                job_id='123456',
                url_id='B012345678',
                url_type='amazon_product',
                url='file:///home/nono/projects/xtracted/crawlers-python/tests/en_GB/gopro.html',
            ),
            message_id='some-msg-id',
        )
    )
    asyncio.run(aap.crawl())
