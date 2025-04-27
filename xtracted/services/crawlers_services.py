from abc import ABC, abstractmethod
from typing import Optional
from uuid import UUID

from pydantic import HttpUrl

from xtracted.crawler_configuration import CrawlerConfig
from xtracted.model import CrawlerUrl, CrawlerUrlFactory


class CrawlersService(ABC):
    @abstractmethod
    async def list_crawler_urls(self, user_id: UUID, job_id: int) -> list[CrawlerUrl]:
        pass

    @abstractmethod
    async def get_crawler_url(
        self, user_id: UUID, job_id: int, url_id: str
    ) -> Optional[CrawlerUrl]:
        pass

    @abstractmethod
    async def add_crawler_url(
        self, user_id: UUID, job_id: int, url: HttpUrl
    ) -> Optional[CrawlerUrl]:
        pass


class PostgresCrawlersService(CrawlersService):
    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config

    async def list_crawler_urls(self, user_id: UUID, job_id: int) -> list[CrawlerUrl]:
        conn = await self.config.new_db_client()
        try:
            async with conn.transaction():
                return [
                    CrawlerUrl.from_record(record)
                    async for record in conn.cursor(
                        'SELECT * from job_urls where job_id = $1 and user_id = $2',
                        job_id,
                        user_id,
                    )
                ]
        finally:
            await conn.close()

    async def get_crawler_url(
        self, user_id: UUID, job_id: int, url_id: str
    ) -> Optional[CrawlerUrl]:
        conn = await self.config.new_db_client()
        try:
            record = await conn.fetchrow(
                """select * from job_urls where user_id = $1 and job_id = $2 and url_id = $3""",
                user_id,
                job_id,
                url_id,
            )
            if record:
                return CrawlerUrl.from_record(record)
            return None
        finally:
            await conn.close()

    async def add_crawler_url(
        self, user_id: UUID, job_id: int, url: HttpUrl | str
    ) -> Optional[CrawlerUrl]:
        conn = await self.config.new_db_client()
        input = CrawlerUrlFactory.new_url_input(
            user_id=user_id,
            job_id=job_id,
            url=url,
        )
        if input:
            try:
                record = await conn.fetchrow(
                    """select * from job_urls where user_id = $1 and job_id = $2 and url_id = $3""",
                    input.user_id,
                    input.job_id,
                    input.url_id,
                )
                if record:
                    return None

                queue = await self.config.new_pgmq_client()
                async with conn.transaction():
                    job_urls_seq = await conn.fetchval(
                        """insert into job_urls(user_id,job_id,url_id,url,job_urls_seq,url_type) values($1,$2,$3,$4,nextval($5),$6) returning job_urls_seq""",
                        input.user_id,
                        input.job_id,
                        input.url_id,
                        str(input.url),
                        f'job_urls_seq_{input.user_id}',
                        input.url_type,
                    )
                    await queue.send(
                        'job_urls',
                        {
                            'event': 'new_url',
                            'job_id': input.job_id,
                            'user_id': input.user_id,
                            'url_id': input.url_id,
                            'url': str(input.url),
                        },
                        conn=conn,
                    )
                return CrawlerUrl(
                    user_id=input.user_id,
                    job_id=input.job_id,
                    url_id=input.url_id,
                    url=input.url,
                    url_type=input.url_type,
                    job_urls_seq=job_urls_seq,
                )
            finally:
                await conn.close()
        return None
