from typing import Any
from urllib.parse import urlparse

from playwright.sync_api import Page, sync_playwright

from crawlers.amazon.amazon_params import AmazonParams


def extract_root_url(url: str) -> str | None:
    parsed_url = urlparse(url)
    if parsed_url.scheme.startswith('http'):
        return f'{parsed_url.scheme}://{parsed_url.netloc}'
    return None


def extract_variants(page: Page) -> dict[str, Any]:
    href = page.evaluate('document.location.href')
    root_url = extract_root_url(href)
    matrix = page.evaluate('twisterController.twisterModel.twisterJSInitData')

    result = {}
    if 'num_total_variations' in matrix:
        result['variants_count'] = matrix['num_total_variations']
    if 'current_asin' in matrix:
        result['current_asin'] = matrix['current_asin']
    if 'parent_asin' in matrix:
        result['parent_asin'] = matrix['parent_asin']
    if 'variationDisplayLabels' in matrix:
        result['variationDisplayLabels'] = matrix['variationDisplayLabels']

    if 'dimensionValuesDisplayData' in matrix and 'dimensionsDisplay' in matrix:
        variants = []
        dimension_display = matrix['dimensionsDisplay']
        for variant in matrix['dimensionValuesDisplayData']:
            detail = []
            display_data = matrix['dimensionValuesDisplayData'][variant]
            for idx, _ in enumerate(dimension_display):
                detail.append({dimension_display[idx]: display_data[idx]})
            variants.append(
                {
                    'asin': variant,
                    'detail': detail,
                    'url': f"{'' if root_url is None else root_url}/dp/{variant}?psc=1",
                }
            )
        result['variants'] = variants
    return result


def extract_asin(page: Page) -> Any:
    return page.locator('#averageCustomerReviews').first.get_attribute('data-asin')


def extract_feature_bullets(page: Page) -> list[str]:
    res = []
    for element in page.locator('#feature-bullets ul li').all():
        text_content = element.text_content()
        if text_content:
            res.append(text_content.strip())
    return res


def extract_variations_matrix(page: Page) -> dict[str, Any]:
    try:
        return extract_variants(page)
    except Exception:
        return {}


def crawl_amazon_product(url: str, amazon_params: AmazonParams) -> dict[str, Any]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        page.wait_for_load_state('domcontentloaded')
        asin = extract_asin(page)
        feature_bullets = extract_feature_bullets(page)
        variants = extract_variations_matrix(page)

        browser.close()

        if amazon_params.with_variants:
            print('===== enqueuing variants =====')
        return {'asin': asin, 'feature_bullets': feature_bullets, 'variants': variants}


if __name__ == '__main__':
    res = crawl_amazon_product(
        'file:///data/dev/xtracted/crawlers-python/tests/en_GB/gopro.html',
        AmazonParams(with_variants=True),
    )
    print(res)
