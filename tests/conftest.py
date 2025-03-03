import logging
from typing import Any, AsyncGenerator, cast

import pytest
from redis.asyncio.client import Redis
from xtracted_common.configuration import TestConfig
from xtracted_common.services.jobs_service import DefaultJobsService, JobsService

from xtracted.queue import Queue, RedisQueue

logger = logging.getLogger(__name__)

pytest_plugins = 'xtracted_tests.fixtures'


@pytest.fixture(scope='function')
async def queue(conf: TestConfig, redis_client: Redis) -> AsyncGenerator[Queue, Any]:
    yield RedisQueue(conf)


@pytest.fixture(scope='session')
async def conf() -> AsyncGenerator[TestConfig, Any]:
    yield TestConfig()


@pytest.fixture(scope='function')
async def job_service(
    conf: TestConfig, redis_client: Redis
) -> AsyncGenerator[JobsService, Any]:
    yield DefaultJobsService(config=conf)


@pytest.fixture(scope='session')
def project_root_path(request: Any) -> str:
    return cast(str, request.config.rootpath)


@pytest.fixture(scope='session')
def docker_compose_file(project_root_path: str) -> str:
    print(project_root_path)
    return f'{project_root_path}/../xtracted-tests/xtracted_tests/docker-compose.yml'
