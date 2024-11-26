import logging
from typing import Any, AsyncGenerator

import pytest
from pydantic import RedisDsn
from redis.asyncio.client import Redis
from xtracted_common.configuration import XtractedConfig
from xtracted_common.services.jobs_service import DefaultJobsService, JobsService

from xtracted.queue import Queue, RedisQueue

logger = logging.getLogger(__name__)

pytest_plugins = 'xtracted_tests.fixtures'


class TConfig(XtractedConfig):
    redis_cluster_url: RedisDsn = 'redis://'  # type: ignore
    db_url: str = 'postgresql://postgres:postgres@localhost:5432/postgres'


@pytest.fixture(scope='function')
async def queue(conf: TConfig, redis_client: Redis) -> AsyncGenerator[Queue, Any]:
    yield RedisQueue(conf)


@pytest.fixture(scope='session')
async def conf() -> AsyncGenerator[TConfig, Any]:
    yield TConfig()


@pytest.fixture(scope='function')
async def job_service(
    conf: TConfig, redis_client: Redis
) -> AsyncGenerator[JobsService, Any]:
    yield DefaultJobsService(config=conf)
