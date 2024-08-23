import pytest
from pydantic import AnyUrl

from crawlers.model import CrawlJobInput, InvalidUrlException


def test_urls_must_be_on_of_amazon_domain() -> None:
    with pytest.raises(InvalidUrlException) as e:
        crawl_job = CrawlJobInput(urls={AnyUrl('https://www.amazonial.co.uk')})
        getattr(crawl_job, 'validate_urls')()
    assert str(e.value) == (
        'Url https://www.amazonial.co.uk/ is invalid. Valid domain are one of \n'
        'www.amazon.com.au,\n'
        'www.amazon.com.be,\n'
        'www.amazon.com.br,\n'
        'www.amazon.ca,\n'
        'www.amazon.cn,\n'
        'www.amazon.eg,\n'
        'www.amazon.fr,\n'
        'www.amazon.de,\n'
        'www.amazon.in,\n'
        'www.amazon.it,\n'
        'www.amazon.co.jp,\n'
        'www.amazon.com.mx,\n'
        'www.amazon.nl,\n'
        'www.amazon.pl,\n'
        'www.amazon.sa,\n'
        'www.amazon.sg,\n'
        'www.amazon.co.za,\n'
        'www.amazon.es,\n'
        'www.amazon.se,\n'
        'www.amazon.com.tr,\n'
        'www.amazon.ae,\n'
        'www.amazon.co.uk,\n'
        'www.amazon.com'
    )


def test_urls_scheme_must_be_https() -> None:
    with pytest.raises(InvalidUrlException) as e:
        crawl_job = CrawlJobInput(urls={AnyUrl('http://www.amazon.co.uk')})
        getattr(crawl_job, 'validate_urls')()
    assert (
        str(e.value)
        == 'Url http://www.amazon.co.uk/ is invalid. Only url with https scheme are valid.'
    )


def test_validate_urls_from_amazon() -> None:
    with pytest.raises(InvalidUrlException) as e:
        crawl_job = CrawlJobInput(
            urls={
                AnyUrl(
                    'https://www.amazon.co.uk/Shark-FlexBreeze-High-Velocity-Water-Resistant-FA220UK/dp/BAD?ref=dlx_deals_dg_dcl_B0CSSZ5SJF_dt_sl14_86'
                )
            }
        )
        getattr(crawl_job, 'validate_urls')()
    assert str(e.value) == (
        'Url https://www.amazon.co.uk/Shark-FlexBreeze-High-Velocity-Water-Resistant-FA220UK/dp/BAD?ref=dlx_deals_dg_dcl_B0CSSZ5SJF_dt_sl14_86 '
        'is invalid. Url must be an Amazon product page url'
    )
