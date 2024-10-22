from abc import ABC, abstractmethod


class Storage(ABC):
    @abstractmethod
    def new_job(self) -> None:
        pass

    @abstractmethod
    def update(self) -> None:
        pass

    @abstractmethod
    def list_job_items(self) -> None:
        pass

    @abstractmethod
    def get_item(self, job: str) -> None:
        pass
