import json
import shutil
import tempfile
from pathlib import Path

import pytest

from xtracted.model import AmazonProductUrl
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
    crawl_url = AmazonProductUrl(
        job_id='123456', url='https://amazon.co.uk/dp/1234567ABC'
    )
    await storage.append(crawl_url, data={'foo': 'bar'})

    to_verify = _tmp_dir() / 'xtracted' / 'jobs' / '123456' / '1234567ABC' / 'data.json'
    with open(to_verify) as f:
        data = json.load(f)
        assert data == {'foo': 'bar'}


async def test_get_jobs_data(new_temp_store: None) -> None:
    storage = TempFileStorage()
    for i in range(3):
        crawl_url = AmazonProductUrl(
            job_id='123456', url=f'http://www.amazon.co.uk/dp/ABCDEF000{i}'
        )
        await storage.append(crawl_url, data={f'foo{i}': f'bar{i}'})

    result = await storage.get_crawled_data('123456')
    assert result == [
        {'ABCDEF0000': {'foo0': 'bar0'}},
        {'ABCDEF0001': {'foo1': 'bar1'}},
        {'ABCDEF0002': {'foo2': 'bar2'}},
    ]
