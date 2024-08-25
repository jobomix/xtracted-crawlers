import logging
from typing import Any, AsyncGenerator

import pytest
import redis.asyncio as redis
from pytest_docker.plugin import Services
from redis.asyncio.client import Redis

logger = logging.getLogger(__name__)


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
