import logging
from typing import Any

from crawlers.amazon.amazon_params import AmazonParams
from celery import Celery, Task
from crawlers.amazon.amazon_product import crawl_amazon_product
from crawlers.model import CrawlJobInput
import time

logger = logging.getLogger(__name__)

CONFIG = {
    'RESULT_EXPIRES': 15,  # 15 secs
}

app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0', config_source=CONFIG)


@app.task(
    name="new_crawl_job",
    bind=True
)
def new_crawl_job(self: Task, params: dict[str,Any]):
    job_in = CrawlJobInput(**params)
    for url in job_in.urls:
        app.send_task("tasks.hello", [str(url)])
    return self.request.id


@app.task(
    bind=True
)
def hello(param: str):
    logger.info("Starting hello task :)")
    app.send_task("tasks.hello_brothers_and_sisters", [param])
    return {"url": param}


@app.task(bind=True)
def hello_brothers_and_sisters(self: Task, param: str):
    print(f'PARENT #ID: {self.request.parent_id}')
    time.sleep(1)
    self.update_state(state='PROGRESS', meta={'progress': 50})
    time.sleep(1)
    self.update_state(state='PROGRESS', meta={'progress': 70})
    time.sleep(1)
    self.update_state(state='PROGRESS', meta={'progress': 90})
    time.sleep(1)
    return {"url": param, "hello": "brothers and sister"}


@app.task(
    name="crawl_amazon_product",
    soft_time_limit=60,
    default_retry_delay=10,
    max_retries=3)
def crawl_amazon_product_task(url: str, *, params: dict[str, Any]):
    amazon_params = AmazonParams(**params)
    try:
        return crawl_amazon_product(url, amazon_params)
    except Exception as e:
        logger.error(e)
        crawl_amazon_product_task.retry()
