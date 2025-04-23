import logging
from typing import Any, AsyncGenerator, cast
from uuid import UUID

import pytest
from asyncpg import Connection
from pydantic_settings import BaseSettings
from xtracted_common.configuration import XtractedConfig
from xtracted_common.services.jobs_service import PostgresJobService

from xtracted.context import CrawlSyncer, PostgresCrawlSyncer
from xtracted.services.crawlers_services import CrawlersService, PostgresCrawlersService

logger = logging.getLogger(__name__)

pytest_plugins = ['xtracted_tests.fixtures']


@pytest.fixture(scope='session')
async def conf(testing_config: BaseSettings) -> AsyncGenerator[XtractedConfig, Any]:
    yield XtractedConfig(**testing_config.model_dump())


@pytest.fixture(scope='session')
def project_root_path(request: Any) -> str:
    return cast(str, request.config.rootpath)


@pytest.fixture(scope='session')
def docker_compose_file(project_root_path: str) -> str:
    return f'{project_root_path}/../xtracted-tests/xtracted_tests/docker-compose.yml'


@pytest.fixture(scope='function')
async def crawlers_service(
    conf: XtractedConfig, pg_client: Connection
) -> AsyncGenerator[CrawlersService, Any]:
    yield PostgresCrawlersService(conf)


@pytest.fixture(scope='function')
async def with_uuid(with_user: str) -> AsyncGenerator[UUID, Any]:
    yield UUID(with_user)


@pytest.fixture(scope='function')
async def jobs_service(
    conf: XtractedConfig, pg_client: Connection
) -> AsyncGenerator[PostgresJobService, Any]:
    yield PostgresJobService(conf)


async def crawl_syncer(conf: XtractedConfig) -> CrawlSyncer:
    return PostgresCrawlSyncer(conf)
