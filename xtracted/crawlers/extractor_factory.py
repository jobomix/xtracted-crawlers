import re
from typing import Optional

from pydantic_core import Url

from xtracted.context import CrawlSyncer, DefaultCrawlContext
from xtracted.crawlers.amazon.amazon_async_product import AmazonAsyncProduct
from xtracted.model import AmazonProductUrl, Extractor
from xtracted.storage import Storage

amazon_product_url_regexp = re.compile(r'.*/dp/([A-Z0-9]{10}.*')


class Extractorfactory:
    def __init__(self, storage: Storage, crawl_syncer: CrawlSyncer):
        self.storage = storage
        self.crawl_syncer = crawl_syncer

    def new_instance(
        self, job_id: str, message_id: str, url: Url
    ) -> Optional[Extractor]:
        if url.path and amazon_product_url_regexp.match(url.path):
            crawl_url = AmazonProductUrl(job_id=job_id, url=url)
            return AmazonAsyncProduct(
                crawl_context=DefaultCrawlContext(
                    message_id=message_id,
                    crawl_url=crawl_url,
                    crawl_syncer=self.crawl_syncer,
                    storage=self.storage,
                )
            )
        return None
