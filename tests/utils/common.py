import asyncio
from typing import Callable

import pytest


async def wait(condition: Callable[[], bool], timeout: int = 10) -> None:
    for i in range(timeout * 2):
        if not condition():
            await asyncio.sleep(0.5)
        else:
            break
    if i >= (timeout * 2) - 1:
        pytest.fail(f'Wait condition has not been met within {timeout} seconds')
