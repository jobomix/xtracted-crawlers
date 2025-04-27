from uuid import UUID

from pydantic import HttpUrl
from xtracted_common.model import XtractedUrlType
from xtracted_common.services.jobs_service import PostgresJobService

from tests.utilities import create_crawl_job
from xtracted.crawler_configuration import CrawlerConfig
from xtracted.services.crawlers_services import CrawlersService

urls = [
    'https://www.amazon.co.uk/dp/B0931VRJT5',
    'https://www.amazon.co.uk/dp/B0931VRJT6',
]


async def test_list_crawler_urls(
    crawlers_service: CrawlersService,
    with_uuid: UUID,
    conf: CrawlerConfig,
    with_user_token: str,
) -> None:
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    crawl_job_urls = await crawlers_service.list_crawler_urls(
        user_id=with_uuid, job_id=crawl_job.job_id
    )

    assert len(crawl_job_urls) == 2


async def test_get_crawler_url_returns_something_when_exists(
    crawlers_service: CrawlersService,
    with_uuid: UUID,
    conf: CrawlerConfig,
    with_user_token: str,
) -> None:
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    crawler_url = await crawlers_service.get_crawler_url(
        job_id=crawl_job.job_id, user_id=with_uuid, url_id='B0931VRJT6'
    )

    assert crawler_url is not None
    assert crawler_url.url_type == 'amazon_product'


async def test_get_crawler_url_returns_nothing_when_not_exists(
    crawlers_service: CrawlersService,
    with_uuid: UUID,
    conf: CrawlerConfig,
    with_user_token: str,
) -> None:
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    crawler_url = await crawlers_service.get_crawler_url(
        job_id=crawl_job.job_id, user_id=with_uuid, url_id='B0931VRJT9'
    )

    assert crawler_url is None


async def test_add_crawler_url_returns_none_when_url_exists(
    crawlers_service: CrawlersService,
    with_uuid: UUID,
    conf: CrawlerConfig,
    with_user_token: str,
) -> None:
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    added = await crawlers_service.add_crawler_url(
        user_id=with_uuid,
        job_id=crawl_job.job_id,
        url=HttpUrl('https://www.amazon.co.uk/dp/B0931VRJT5'),
    )

    assert added is None


async def test_add_crawler_url_returns_url_when_does_not_exsist(
    crawlers_service: CrawlersService,
    with_uuid: UUID,
    conf: CrawlerConfig,
    with_user_token: str,
) -> None:
    job_service = PostgresJobService(config=conf)
    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    crawl_job = await create_crawl_job(
        job_service=job_service, token=with_user_token, urls=urls
    )

    added = await crawlers_service.add_crawler_url(
        user_id=with_uuid,
        job_id=crawl_job.job_id,
        url=HttpUrl('https://www.amazon.co.uk/dp/B0931VRJT9'),
    )

    assert added is not None
    assert added.url == HttpUrl('https://www.amazon.co.uk/dp/B0931VRJT9')
    assert added.url_id == 'B0931VRJT9'
    assert added.url_type == XtractedUrlType.amazon_product
    assert added.job_urls_seq > 3
    assert added.user_id == with_uuid
