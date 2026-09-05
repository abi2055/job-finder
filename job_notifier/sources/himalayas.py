from __future__ import annotations

from job_notifier.http_client import HttpClient
from job_notifier.models import SourceResult
from job_notifier.sources.base import JobSource


class HimalayasSource(JobSource):
    """Fetches remote jobs from Himalayas.app API.

    Free public API with no authentication required.
    Supports browsing all jobs or searching with filters.
    """
    source_type = "himalayas"

    def fetch(self, client: HttpClient) -> SourceResult:
        # Determine if this is a search or browse request
        use_search = any(key in self.config for key in ["q", "country", "seniority", "employment_type"])

        if use_search:
            url = "https://himalayas.app/jobs/api/search"
        else:
            url = "https://himalayas.app/jobs/api"

        # Build query parameters from config
        query_params = {"limit": "20"}  # Max limit per request

        # Add search/filter parameters if provided
        for param in ["q", "country", "worldwide", "seniority", "employment_type", "company", "timezone", "sort", "page", "cursor"]:
            if param in self.config:
                query_params[param] = str(self.config[param])

        response = client.get(url, query=query_params)

        return SourceResult.from_response(
            source_name=self.name,
            source_type=self.source_type,
            url=response.url,
            status=response.status,
            content_type=response.headers.get("Content-Type"),
            payload=response.json(),
        )
