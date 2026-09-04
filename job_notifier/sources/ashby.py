from __future__ import annotations

from job_notifier.http_client import HttpClient
from job_notifier.models import SourceResult
from job_notifier.sources.base import JobSource


class AshbySource(JobSource):
    source_type = "ashby"

    def fetch(self, client: HttpClient) -> SourceResult:
        company = str(self.config["company"])
        url = f"https://api.ashbyhq.com/posting-api/job-board/{company}"
        response = client.get(url, query={"includeCompensation": "true"})

        return SourceResult.from_response(
            source_name=self.name,
            source_type=self.source_type,
            url=response.url,
            status=response.status,
            content_type=response.headers.get("Content-Type"),
            payload=response.json(),
        )
