from typing import Any, Optional

from pydantic import AnyUrl
from xtracted_common.model import AmazonProductUrl

from xtracted.context import CrawlSyncer, DefaultCrawlContext
from xtracted.crawlers.amazon.amazon_async_product import AmazonAsyncProduct
from xtracted.model import CrawlerUrl, Extractor


class Extractorfactory:
    def __init__(self, crawl_syncer: CrawlSyncer):
        self.crawl_syncer = crawl_syncer

    def new_instance(
        self, message_id: str | int, mapping: dict[str, Any]
    ) -> Optional[Extractor]:
        url = AnyUrl(mapping['url'])
        if url.path:
            if AmazonProductUrl.match_url.match(url.path):
                return AmazonAsyncProduct(
                    crawl_context=DefaultCrawlContext(
                        message_id=message_id,
                        crawler_url=CrawlerUrl(**mapping),
                        crawl_syncer=self.crawl_syncer,
                    )
                )
        return None
