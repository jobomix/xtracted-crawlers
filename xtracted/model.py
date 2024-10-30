import re
from enum import Enum
from typing import Generator, Optional

from pydantic import (
    AfterValidator,
    AnyHttpUrl,
    AnyUrl,
    BaseModel,
)
from typing_extensions import Annotated

from xtracted.crawlers.amazon.amazon_params import AmazonParams

allowed_hosts = [
    'www.amazon.com.au',
    'www.amazon.com.be',
    'www.amazon.com.br',
    'www.amazon.ca',
    'www.amazon.cn',
    'www.amazon.eg',
    'www.amazon.fr',
    'www.amazon.de',
    'www.amazon.in',
    'www.amazon.it',
    'www.amazon.co.jp',
    'www.amazon.com.mx',
    'www.amazon.nl',
    'www.amazon.pl',
    'www.amazon.sa',
    'www.amazon.sg',
    'www.amazon.co.za',
    'www.amazon.es',
    'www.amazon.se',
    'www.amazon.com.tr',
    'www.amazon.ae',
    'www.amazon.co.uk',
    'www.amazon.com',
]

amazon_url_asin_path = re.compile(r'.*/dp/[A-Z0-9]{10}.*')


def check_amazon_valid_url(url: AnyHttpUrl) -> Optional[AnyHttpUrl]:
    if url.scheme != 'https':
        raise InvalidUrlException(
            f'Url {url} is invalid. Only url with https scheme are valid.'
        )

    if url.host not in allowed_hosts:
        domains = ',\n'.join(allowed_hosts)
        raise InvalidUrlException(
            f"""Url {url} is invalid. Valid domain are one of \n{domains}"""
        )

    if url.path and not amazon_url_asin_path.match(url.path):
        raise InvalidUrlException(
            f'Url {url} is invalid. Url must be an Amazon product page url'
        )
    return url


ValidAmazonUrl = Annotated[AnyHttpUrl, AfterValidator(check_amazon_valid_url)]


class CrawlUrlStatus(str, Enum):
    complete = 'complete'
    error = 'error'
    pending = 'pending'
    running = 'running'


class CrawlJobStatus(str, Enum):
    complete = 'complete'
    running = 'running'
    pending = 'pending'


class InvalidUrlException(Exception):
    pass


class CrawlJobInput(BaseModel):
    """
    Crawl job Representation. Contains mainly a set of urls.
    """

    urls: set[ValidAmazonUrl] = set()
    params: Optional[AmazonParams] = None


class CrawlJobFreeInput(CrawlJobInput):
    urls: set[AnyUrl] = set()


class CrawlUrl(BaseModel):
    crawl_url_id: str
    url: AnyUrl
    status: CrawlUrlStatus = CrawlUrlStatus.pending
    retries: int = 0

    def get_job_id(self) -> str:
        split = self.crawl_url_id.split(':')
        return split[1]

    def get_url_id_suffix(self) -> str:
        split = self.crawl_url_id.split(':')
        return split[2]

    def __hash__(self) -> int:
        return self.crawl_url_id.__hash__()

    @staticmethod
    def extract_url_id_suffix(crawl_url_id: str) -> str:
        split = crawl_url_id.split(':')
        return split[-1]


class CrawlJob(BaseModel):
    job_id: str
    status: CrawlJobStatus
    urls: set[CrawlUrl] = set()

    def _filter_url_by_status(
        self, status: CrawlUrlStatus
    ) -> Generator[CrawlUrl, None, None]:
        return (url for url in self.urls if url.status == status)

    def get_pending_urls(self) -> Generator[CrawlUrl, None, None]:
        return self._filter_url_by_status(CrawlUrlStatus.pending)

    def get_running_urls(self) -> Generator[CrawlUrl, None, None]:
        return self._filter_url_by_status(CrawlUrlStatus.running)

    def get_completed_urls(self) -> Generator[CrawlUrl, None, None]:
        return self._filter_url_by_status(CrawlUrlStatus.complete)

    def get_failed_urls(self) -> Generator[CrawlUrl, None, None]:
        return self._filter_url_by_status(CrawlUrlStatus.error)

    def get_status(self) -> CrawlJobStatus:
        complete = 0
        error = 0
        running = 0
        pending = 0

        for url in self.urls:
            match url.status:
                case CrawlUrlStatus.pending:
                    pending += 1
                case CrawlUrlStatus.running:
                    running += 1
                case CrawlUrlStatus.complete:
                    complete += 1
                case CrawlUrlStatus.error:
                    error += 1

        if running > 0:
            return CrawlJobStatus.running
        elif pending == 0 and running == 0:
            return CrawlJobStatus.complete
        else:
            return CrawlJobStatus.pending

    def __hash__(self) -> int:
        return self.job_id.__hash__()
