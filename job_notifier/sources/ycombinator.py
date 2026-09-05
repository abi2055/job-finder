from __future__ import annotations

from job_notifier.http_client import HttpClient
from job_notifier.models import SourceResult
from job_notifier.sources.base import JobSource


class YCombinatorSource(JobSource):
    """Fetches YC companies that are currently hiring.

    Note: This returns company data with isHiring flags, not individual job postings.
    Use this to get a list of YC startups that are actively hiring.
    """
    source_type = "ycombinator"

    def fetch(self, client: HttpClient) -> SourceResult:
        # Fetch all companies (API is paginated but we'll get first page for now)
        # You can add page parameter in config to fetch specific pages
        page = int(self.config.get("page", 1))
        url = f"https://api.ycombinator.com/v0.1/companies"
        response = client.get(url, query={"page": str(page)})

        return SourceResult.from_response(
            source_name=self.name,
            source_type=self.source_type,
            url=response.url,
            status=response.status,
            content_type=response.headers.get("Content-Type"),
            payload=response.json(),
        )
