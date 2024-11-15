from abc import ABC, abstractmethod


class CrawlException(Exception):
    pass


class Extractor(ABC):
    @abstractmethod
    async def crawl(self) -> None:
        pass


class InvalidUrlException(Exception):
    pass
