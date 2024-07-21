from crawlers.model import ValidAmazonUrl


def enqueue(job_id: str, url: ValidAmazonUrl):
    pass


def list_all_urls(job_id: str) -> set[ValidAmazonUrl]:
    pass


def list_visited_urls(job_id: str) -> set[ValidAmazonUrl]:
    pass


def list_remaining_urls(job_id: str) -> set[ValidAmazonUrl]:
    pass
