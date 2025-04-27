from pydantic_settings import SettingsConfigDict
from xtracted_common.configuration import XtractedConfig


class CrawlerConfig(XtractedConfig):
    crawler_max_crawl_tasks: int = 1

    crawler_url_vt: int = 6
    crawler_url_qty: int = 1
    crawler_url_max_poll_seconds: int = 5
    crawler_url_poll_interval_ms: int = 1000

    crawler_job_vt: int = 10
    crawler_job_qty: int = 1
    crawler_job_max_poll_seconds: int = 5
    crawler_job_poll_interval_ms: int = 1000


class CrawlerConfigFromDotEnv(CrawlerConfig):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')


class CrawlerConfigFactory:
    @classmethod
    def new_instance(cls) -> CrawlerConfig:
        return CrawlerConfigFromDotEnv()
