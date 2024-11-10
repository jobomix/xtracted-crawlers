import asyncio
import json
import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import aiofiles
from xtracted_common.model import XtractedUrl


class Storage(ABC):
    @abstractmethod
    async def append(self, crawl_url: XtractedUrl, data: dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def get_crawled_data(self, crawl_job_id: str) -> list[dict[str, Any]]:
        pass


class TempFileStorage(Storage):
    def __init__(self) -> None:
        jobs_dir = Path(tempfile.gettempdir()) / 'xtracted' / 'jobs'
        self.jobs_dir = jobs_dir

    def _get_crawled_data(self, crawl_job_id: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        job_id_dir = self.jobs_dir / crawl_job_id
        if job_id_dir.exists() and job_id_dir.is_dir():
            for url_dir in job_id_dir.iterdir():
                if url_dir.is_dir():
                    data = url_dir / 'data.json'
                    if data.exists() and data.is_file():
                        with open(data, 'r') as f:
                            split = str(url_dir).split(os.sep)
                            id = split[-1]
                            result.append({id: json.load(f)})

        return result

    async def append(self, crawl_url: XtractedUrl, data: dict[str, Any]) -> None:
        job_id = crawl_url.job_id
        url_suffix = crawl_url.get_url_id_suffix()
        url_location = self.jobs_dir / job_id / url_suffix
        url_location.mkdir(exist_ok=True, parents=True)

        data_path = url_location / 'data.json'
        async with aiofiles.open(data_path, 'w', encoding='utf-8') as f:
            as_str = json.dumps(data, ensure_ascii=False, indent=2)
            await f.write(as_str)
            await f.flush()
        return None

    async def get_crawled_data(self, crawl_job_id: str) -> list[dict[str, Any]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._get_crawled_data, crawl_job_id)
