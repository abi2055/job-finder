from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from job_notifier.http_client import HttpClient
from job_notifier.models import SourceResult


class JobSource(ABC):
    source_type: str

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.name = str(config["name"])

    @abstractmethod
    def fetch(self, client: HttpClient) -> SourceResult:
        """Fetch raw job data from the source."""

