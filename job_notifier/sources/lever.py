from __future__ import annotations

from job_notifier.http_client import HttpClient
from job_notifier.models import SourceResult
from job_notifier.sources.base import JobSource


class LeverSource(JobSource):
    source_type = "lever"

    def fetch(self, client: HttpClient) -> SourceResult:
        company = str(self.config["company"])
        url = f"https://api.lever.co/v0/postings/{company}"
        response = client.get(url, query={"mode": "json"})

        return SourceResult.from_response(
            source_name=self.name,
            source_type=self.source_type,
            url=response.url,
            status=response.status,
            content_type=response.headers.get("Content-Type"),
            payload=response.json(),
        )

