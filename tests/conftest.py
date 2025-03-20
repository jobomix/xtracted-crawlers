import logging
from typing import Any, AsyncGenerator, cast

import pytest
from pydantic_settings import BaseSettings
from xtracted_common.configuration import XtractedConfig

logger = logging.getLogger(__name__)

pytest_plugins = 'xtracted_tests.fixtures'


@pytest.fixture(scope='session')
async def conf(testing_config: BaseSettings) -> AsyncGenerator[XtractedConfig, Any]:
    yield XtractedConfig(**testing_config.model_dump())


@pytest.fixture(scope='session')
def project_root_path(request: Any) -> str:
    return cast(str, request.config.rootpath)


@pytest.fixture(scope='session')
def docker_compose_file(project_root_path: str) -> str:
    return f'{project_root_path}/../xtracted-tests/xtracted_tests/docker-compose.yml'
