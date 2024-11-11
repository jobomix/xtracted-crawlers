import logging
from typing import Any, AsyncGenerator

import pytest
import redis.asyncio as redis
from pydantic import RedisDsn
from pytest_docker.plugin import Services
from redis.asyncio.client import Redis
from xtracted_common.configuration import XtractedConfig

from xtracted.queue import Queue, RedisQueue

logger = logging.getLogger(__name__)


class TestConfig(XtractedConfig):
    redis_cluster_url: RedisDsn = 'redis://'  # type: ignore


def is_reponsive() -> bool:
    import redis as redis_sync

    try:
        pool = redis_sync.ConnectionPool()
        client = redis_sync.Redis(connection_pool=pool)
        client.ping()
        return True
    except Exception:
        return False


@pytest.fixture(scope='session')
async def default_stack(docker_services: Services) -> None:
    logger.info('docker stack started')
    docker_services.wait_until_responsive(
        timeout=5.0, pause=0.5, check=lambda: is_reponsive()
    )


@pytest.fixture(scope='function')
async def redis_client(default_stack: Any) -> AsyncGenerator[Redis, Any]:
    client = redis.StrictRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture(scope='session')
async def conf() -> AsyncGenerator[TestConfig, Any]:
    yield TestConfig()


@pytest.fixture(scope='function')
async def queue(conf: TestConfig, redis_client: Redis) -> AsyncGenerator[Queue, Any]:
    yield RedisQueue(conf)
