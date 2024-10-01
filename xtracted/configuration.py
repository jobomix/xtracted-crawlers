from pydantic import RedisDsn
from pydantic_settings import BaseSettings


class XtractedConfig(BaseSettings):
    redis_cluster_url: RedisDsn
