import asyncio

# from redis.asyncio.client import Redis
from redis.asyncio.cluster import RedisCluster

from xtracted.configuration import XtractedConfigFromDotEnv

config = XtractedConfigFromDotEnv()


async def _remove_keys() -> None:
    client = RedisCluster.from_url(url=str(config.random_cluster_url()))
    await client.initialize()
    await client.flushall()
    await client.aclose()


def remove_keys() -> None:
    asyncio.run(_remove_keys())
