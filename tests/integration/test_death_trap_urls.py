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


def test_url_with_same_Asin_have_same_hash() -> None:
    url1 = AmazonProductUrl(
        url='https://www.amazon.co.uk/dp/B01GAXFI14?foo=bar', job_id='123456'
    )

    url2 = AmazonProductUrl(
        url='https://www.amazon.co.uk/Russel-Hobbs-Iron/dp/B01GAXFI14?foo=bar',
        job_id='123456',
    )

    url3 = AmazonProductUrl(
        url='https://www.amazon.co.uk/Russel-Hobbs-Iron/dp/B01GAXFI14?foo=bar',
        job_id='ABCDEF',
    )

    assert url1.__hash__() == url2.__hash__()
    assert url1 == url2

    assert url1.__hash__() != url3.__hash__()
    assert url1 != url3
