from __future__ import annotations

from job_notifier.http_client import HttpClient
from job_notifier.models import SourceResult
from job_notifier.sources.base import JobSource


class GreenhouseSource(JobSource):
    source_type = "greenhouse"

    def fetch(self, client: HttpClient) -> SourceResult:
        board_token = str(self.config["board_token"])
        url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"
        response = client.get(url, query={"content": "true"})

        return SourceResult.from_response(
            source_name=self.name,
            source_type=self.source_type,
            url=response.url,
            status=response.status,
            content_type=response.headers.get("Content-Type"),
            payload=response.json(),
        )

