import random

from pydantic import IPvAnyAddress, RedisDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class XtractedConfig(BaseSettings):
    redis_cluster_ips: list[IPvAnyAddress] = []
    redis_cluster_url: RedisDsn
    redis_cluster_password: str = ''

    def random_cluster_url(self) -> RedisDsn:
        return f'redis://default:{self.redis_cluster_password}@{random.choice(self.redis_cluster_ips)}/0'  # type: ignore


class XtractedConfigFromDotEnv(XtractedConfig):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')
