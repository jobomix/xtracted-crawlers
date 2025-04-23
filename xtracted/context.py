import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional
from uuid import UUID

from pydantic import HttpUrl
from xtracted_common.configuration import XtractedConfig
from xtracted_common.model import CrawlJobUrlStatus

from xtracted.model import CrawlerUrl
from xtracted.services.crawlers_services import PostgresCrawlersService

logger = logging.getLogger('crawljob-syncer')


class CrawlSyncer(ABC):
    @abstractmethod
    async def sync(self, crawler_url: CrawlerUrl) -> None:
        pass

    @abstractmethod
    async def ack(self, msg_id: str | int) -> None:
        pass

    @abstractmethod
    async def report_error(
        self, crawler_url: CrawlerUrl, msg_id: str | int, error: Exception
    ) -> None:
        pass

    @abstractmethod
    async def enqueue(
        self, user_id: UUID, job_id: int, url: HttpUrl
    ) -> Optional[CrawlerUrl]:
        pass

    @abstractmethod
    async def complete(
        self, crawler_url: CrawlerUrl, msg_id: int | str, data: dict[str, Any]
    ) -> None:
        pass


class CrawlContext(ABC):
    @abstractmethod
    def get_crawler_url(self) -> CrawlerUrl:
        pass

    @abstractmethod
    async def enqueue(self, url: HttpUrl) -> Optional[CrawlerUrl]:
        pass

    @abstractmethod
    async def fail(self, error: Exception) -> None:
        pass

    @abstractmethod
    async def complete(self, data: dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def set_running(self) -> None:
        pass


class PostgresCrawlSyncer(CrawlSyncer):
    def __init__(self, config: XtractedConfig):
        self.config = config
        self.crawlers_service = PostgresCrawlersService(config)

    async def ack(self, msg_id: str | int) -> None:
        queue = await self.config.new_pgmq_client()
        conn = await self.config.new_db_client()
        try:
            await queue.archive('job_urls', msg_id=msg_id, conn=conn)
        except Exception as e:
            logger.error(e)
        finally:
            await conn.close()

    async def report_error(
        self, crawler_url: CrawlerUrl, msg_id: str | int, error: Exception
    ) -> None:
        conn = await self.config.new_db_client()
        queue = await self.config.new_pgmq_client()

        try:
            retries = await conn.fetchval(
                """update job_urls set errors = errors || $1, retries = retries + 1 where user_id = $2 and job_id = $3 and url_id = $4 returning retries""",
                [repr(error)],
                crawler_url.user_id,
                crawler_url.job_id,
                crawler_url.url_id,
            )
            if retries >= 3:
                await queue.archive('job_urls', msg_id=msg_id, conn=conn)
        except Exception as e:
            logger.error(e)
        finally:
            await conn.close()

    async def sync(self, crawler_url: CrawlerUrl) -> None:
        conn = await self.config.new_db_client()
        try:
            logger.info(crawler_url)
            await conn.execute(
                """update job_urls set status = $1 where job_id = $2 and user_id = $3 and url_id = $4""",
                crawler_url.status,
                crawler_url.job_id,
                crawler_url.user_id,
                crawler_url.url_id,
            )
        except Exception as e:
            logger.error(e)
        finally:
            await conn.close()

    async def complete(
        self, crawler_url: CrawlerUrl, msg_id: int | str, data: dict[str, Any]
    ) -> None:
        conn = await self.config.new_db_client()
        queue = await self.config.new_pgmq_client()
        try:
            async with conn.transaction():
                await conn.execute(
                    'update job_urls set status = $1, data = $2 where job_id = $3 and user_id = $4 and url_id = $5',
                    crawler_url.status,
                    json.dumps(data),
                    crawler_url.job_id,
                    crawler_url.user_id,
                    crawler_url.url_id,
                )  # update urls
                await queue.archive('job_urls', msg_id=msg_id, conn=conn)

        except Exception as e:
            logger.error(e)
        finally:
            await conn.close()

    async def enqueue(
        self, user_id: UUID, job_id: int, url: HttpUrl
    ) -> Optional[CrawlerUrl]:
        return await self.crawlers_service.add_crawler_url(
            user_id=user_id, job_id=job_id, url=url
        )


class DefaultCrawlContext(CrawlContext):
    def __init__(
        self,
        *,
        crawl_syncer: CrawlSyncer,
        crawler_url: CrawlerUrl,
        message_id: str | int,
    ) -> None:
        self._crawl_syncer = crawl_syncer
        self._crawler_url = crawler_url
        self._message_id = message_id

    def get_crawler_url(self) -> CrawlerUrl:
        return self._crawler_url

    async def enqueue(self, url: HttpUrl) -> Optional[CrawlerUrl]:
        return await self._crawl_syncer.enqueue(
            user_id=self._crawler_url.user_id,
            job_id=self._crawler_url.job_id,
            url=url,
        )

    async def fail(self, error: Exception) -> None:
        await self._crawl_syncer.report_error(
            self._crawler_url, self._message_id, error
        )

    async def set_running(self) -> None:
        self._crawler_url.status = CrawlJobUrlStatus.running
        await self._crawl_syncer.sync(self._crawler_url)

    async def complete(self, data: dict[str, Any]) -> None:
        self._crawler_url.status = CrawlJobUrlStatus.complete
        await self._crawl_syncer.complete(self._crawler_url, self._message_id, data)
