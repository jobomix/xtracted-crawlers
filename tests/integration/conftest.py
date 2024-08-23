import logging
from typing import Any, AsyncGenerator

import pytest
import redis.asyncio as redis
from redis.asyncio.client import Redis

logger = logging.getLogger(__name__)


async def is_reponsive() -> bool:
    try:
        client = redis.Redis()
        await client.ping()
        await client.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope='session')
async def default_stack(docker_services: Any) -> None:
    logger.info('docker stack started')
    docker_services.wait_until_responsive(
        timeout=5.0, pause=0.5, check=lambda: is_reponsive()
    )


@pytest.fixture(scope='function')
async def redis_client(default_stack: Any) -> AsyncGenerator[Redis, Any]:
    client = redis.Redis()
    yield client
    await client.aclose()
