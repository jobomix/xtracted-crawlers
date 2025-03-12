import asyncio
from typing import Any, Callable, Coroutine

from xtracted_common.model import CrawlJobInput, CrawlJobInternal
from xtracted_common.services.jobs_service import JobsService


async def create_crawl_job(
    job_service: JobsService, token: str, urls: list[str]
) -> CrawlJobInternal:
    return await job_service.submit_job(
        token,
        CrawlJobInput(urls=urls),
    )


async def wait_for_condition(
    condition: Callable[[], Coroutine[Any, Any, Any]], timeout: int = 10
) -> None:
    for i in range(timeout):
        try:
            await condition()
            return
        except Exception:
            await asyncio.sleep(1)

    await condition()
