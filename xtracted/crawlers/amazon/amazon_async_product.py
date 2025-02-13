import asyncio
import logging
from typing import Any, Optional
from urllib.parse import urlparse

from playwright.async_api import Page, Playwright, async_playwright
from xtracted_common.model import (
    AmazonProductUrl,
    CrawlJobInternal,
    XtractedUrl,
)
from xtracted_common.storage import DBStorage, Storage

from xtracted.context import CrawlContext, CrawlSyncer, DefaultCrawlContext
from xtracted.model import Extractor

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
        matrix = await page.evaluate('twisterController.twisterModel.twisterJSInitData')

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
            return await self.extract_variants(page)
        except Exception:
            return {}

    async def extract(self, page: Page) -> dict[str, Any]:
        crawl_url = self.crawl_context.get_crawl_url()
        await page.goto(str(crawl_url.url))
        asin = await self.extract_asin(page)
        feature_bullets = await self.extract_feature_bullets(page)
        variants = await self.extract_variations_matrix(page)
        extracted = {}
        extracted['asin'] = asin
        extracted['feature_bullets'] = feature_bullets
        extracted['url'] = str(self.crawl_context.get_crawl_url().url)
        extracted['variants'] = variants
        return extracted

    async def run(self, playwright: Playwright) -> None:
        chromium = playwright.chromium
        browser = await chromium.launch(headless=True)
        page = await browser.new_page()
        extracted = await self.extract(page)
        await browser.close()
        await self.crawl_context.complete(extracted)

    async def crawl(self) -> None:
        try:
            await self.crawl_context.set_running()
            async with async_playwright() as playwright:
                await self.run(playwright)
        except Exception as e:
            print('Error occurred', e)
            await self.crawl_context.fail()


if __name__ == '__main__':

    class DummyCrawlSyncer(CrawlSyncer):
        async def ack(self, message_id: str) -> None:
            pass

        async def sync(self, crawl_url: XtractedUrl) -> None:
            pass

        async def replay(self, crawl_url: XtractedUrl) -> None:
            pass

        async def enqueue(self, to_enqueue: XtractedUrl) -> bool:
            return True

    class DummyStorage(Storage):
        async def latest_job_id(self, uid: str) -> int:
            return 0

        async def save_job(self, job: CrawlJobInternal) -> None:
            pass

        async def append_crawled_data(
            self, crawl_url: XtractedUrl, data: dict[str, Any]
        ) -> None:
            pass

        async def get_crawled_data(
            self, uid: str, crawl_job_id: int, offset: int, limit: int
        ) -> list[dict[str, Any]]:
            return []

    aap = AmazonAsyncProduct(
        crawl_context=DefaultCrawlContext(
            storage=DummyStorage(),
            crawl_syncer=DummyCrawlSyncer(),
            crawl_url=AmazonProductUrl(
                uid='dummy-uid',
                job_id='123456',
                url='file:///home/nono/projects/xtracted/crawlers-python/tests/en_GB/gopro.html',
            ),
            message_id='some-msg-id',
        )
    )
    asyncio.run(aap.crawl())
