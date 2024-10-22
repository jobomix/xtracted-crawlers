import random
from enum import Enum

from pydantic import IPvAnyAddress, RedisDsn
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio.client import Redis
from redis.asyncio.cluster import RedisCluster


class RedisEnvEnum(str, Enum):
    local = 'local'
    cluster = 'cluster'


class XtractedConfig(BaseSettings):
    redis_env: RedisEnvEnum = RedisEnvEnum.local
    redis_cluster_ips: list[IPvAnyAddress] = []
    redis_cluster_url: RedisDsn
    redis_cluster_password: str = ''

    def random_cluster_url(self) -> RedisDsn:
        match self.redis_env:
            case RedisEnvEnum.local:
                return 'redis://'  # type: ignore
            case RedisEnvEnum.cluster:
                return f'redis://default:{self.redis_cluster_password}@{random.choice(self.redis_cluster_ips)}/0'  # type: ignore

    def new_client(self) -> Redis | RedisCluster:
        match self.redis_env:
            case RedisEnvEnum.local:
                return Redis.from_url('redis://', decode_responses=True)  # type: ignore
            case RedisEnvEnum.cluster:
                return RedisCluster.from_url(
                    f'redis://default:{self.redis_cluster_password}@{random.choice(self.redis_cluster_ips)}/0',
                    decode_responses=True,
                )


class XtractedConfigFromDotEnv(XtractedConfig):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')
