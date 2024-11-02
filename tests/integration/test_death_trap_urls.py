import pytest
from pydantic import ValidationError

from xtracted.model import AmazonProductUrl


def test_match_url() -> None:
    url = AmazonProductUrl(
        url='https://www.amazon.co.uk/dp/B01GAXFI14?foo=bar',
        job_id='ABCD456',
    )
    assert url.url_id == 'crawl_url:ABCD456:B01GAXFI14'


def test_exception_when_url_does_not_match() -> None:
    with pytest.raises(ValidationError) as e:
        AmazonProductUrl(
            url='https://www.amazon.co.uk/dp/B01GAXI14?foo=bar', job_id='ABCD456'
        )
        assert 'https://www.amazon.co.uk/dp/B01GAXI14?foo=bar' in repr(e.value)
