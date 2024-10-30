import json
import shutil
import tempfile
from pathlib import Path

import pytest

from xtracted.model import CrawlUrl
from xtracted.storage import TempFileStorage


def _tmp_dir() -> Path:
    return Path(tempfile.gettempdir())


@pytest.fixture(scope='function')
def new_temp_store() -> None:
    tmp = _tmp_dir()
    xtracted_dir = tmp / 'xtracted'
    shutil.rmtree(xtracted_dir, ignore_errors=True)


async def test_data_appendend_to_tmp_storage(new_temp_store: None) -> None:
    storage = TempFileStorage()
    crawl_url = CrawlUrl(crawl_url_id='crawl:123456:0', url='http://example.com')
    await storage.append(crawl_url, data={'foo': 'bar'})

    to_verify = _tmp_dir() / 'xtracted' / 'jobs' / '123456' / '0' / 'data.json'
    with open(to_verify) as f:
        data = json.load(f)
        assert data == {'foo': 'bar'}


async def test_get_jobs_data(new_temp_store: None) -> None:
    storage = TempFileStorage()
    for i in range(3):
        crawl_url = CrawlUrl(
            crawl_url_id=f'crawl:123456:{i}', url=f'http://www.example{1}.com'
        )
        await storage.append(crawl_url, data={f'foo{i}': f'bar{i}'})

    result = await storage.get_crawled_data('123456')
    assert result == [
        {'0': {'foo0': 'bar0'}},
        {'1': {'foo1': 'bar1'}},
        {'2': {'foo2': 'bar2'}},
    ]
