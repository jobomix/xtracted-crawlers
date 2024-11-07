import asyncio
from typing import Callable


async def wait(condition: Callable[[], bool], timeout: int = 10) -> None:
    for i in range(timeout * 2):
        if not condition():
            await asyncio.sleep(0.5)
        else:
            break
