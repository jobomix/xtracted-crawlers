from abc import ABC, abstractmethod

from pydantic import AnyHttpUrl
from xtracted_common.configuration import XtractedConfig


class Queue(ABC):
    @abstractmethod
    async def ack(self, msg_id: str) -> None:
        pass

    @abstractmethod
    async def enqueue(self, url: AnyHttpUrl) -> None:
        pass


class RedisQueue(Queue):
    def __init__(self, config: XtractedConfig) -> None:
        self.config = config

    async def ack(self, msg_id: str) -> None:
        """Ack message"""
        redis = self.config.new_redis_client()
        await redis.xack('crawl', 'crawlers', msg_id)
        await redis.aclose()

    async def enqueue(self, url: AnyHttpUrl) -> None:
        """Enqueue a URL"""
        pass
