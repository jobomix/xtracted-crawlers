from abc import ABC, abstractmethod
from typing import Optional
from uuid import UUID

from asyncpg import Record
from pydantic import HttpUrl
from xtracted_common.model import (
    CrawlJobUrl,
    CrawlJobUrlInput,
    CrawlJobUrlStatus,
    UrlFactory,
)


class CrawlException(Exception):
    pass


class Extractor(ABC):
    @abstractmethod
    async def crawl(self) -> None:
        pass


class InvalidUrlException(Exception):
    pass


class CrawlerUrlInput(CrawlJobUrlInput):
    user_id: UUID
    url_id: str
    url_type: str


class CrawlerUrl(CrawlJobUrl):
    user_id: UUID

    @staticmethod
    def from_record(record: Record) -> 'CrawlerUrl':
        return CrawlerUrl(
            user_id=record['user_id'],
            job_id=record['job_id'],
            status=record['status'],
            url=record['url'],
            url_id=record['url_id'],
            url_type=record['url_type'],
            job_urls_seq=record['job_urls_seq'],
        )


class CrawlerUrlFactory:
    @staticmethod
    def new_url_input(
        url: HttpUrl | str,
        job_id: int,
        user_id: UUID,
        status: CrawlJobUrlStatus = CrawlJobUrlStatus.pending,
    ) -> Optional[CrawlerUrlInput]:
        xtracted_url = UrlFactory.new_url(url=url)
        if xtracted_url:
            return CrawlerUrlInput(
                job_id=job_id,
                user_id=user_id,
                url=xtracted_url.url,
                status=status,
                url_id=xtracted_url.url_id,
                url_type=xtracted_url.type,
            )
        return None

    @staticmethod
    def new_url(
        url: HttpUrl | str,
        job_id: int,
        user_id: UUID,
        job_urls_seq: int = 0,
        status: CrawlJobUrlStatus = CrawlJobUrlStatus.pending,
    ) -> Optional[CrawlerUrl]:
        xtracted_url = UrlFactory.new_url(url=url)
        if xtracted_url:
            return CrawlerUrl(
                job_id=job_id,
                user_id=user_id,
                url=xtracted_url.url,
                url_id=xtracted_url.url_id,
                url_type=xtracted_url.type,
                job_urls_seq=job_urls_seq,
                status=status,
            )
        return None
