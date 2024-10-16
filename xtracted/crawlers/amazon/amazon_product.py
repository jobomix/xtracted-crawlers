from typing import Any
from urllib.parse import urlparse

from playwright.sync_api import Page, sync_playwright

from xtracted.crawlers.amazon.amazon_params import AmazonParams


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
    # res = crawl_amazon_product(
    #     'file:///data/dev/xtracted/crawlers-python/tests/en_GB/gopro.html',
    #     AmazonParams(with_variants=True),
    # )
    res = crawl_amazon_product(
        'https://www.amazon.co.uk/BenQ-GW2490E-Monitor-Full-HD-Eye-Care/dp/B0CT5R7XXL/ref=pd_ci_mcx_mh_mcx_views_1?pd_rd_w=jQdaJ&content-id=amzn1.sym.2ddb7b00-379e-47b0-93b5-ad8e369c4ba1%3Aamzn1.symc.45dc5f4c-d617-4dba-aa26-2cadef3da899&pf_rd_p=2ddb7b00-379e-47b0-93b5-ad8e369c4ba1&pf_rd_r=RAZAQEJYD18V1CHS134P&pd_rd_wg=PqAEY&pd_rd_r=2e7d2762-9ab8-499f-a336-17dbf2daeceb&pd_rd_i=B0CT5R7XXL',
        AmazonParams(with_variants=True),
    )
    print(res)
