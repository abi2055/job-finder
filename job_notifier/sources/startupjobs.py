from __future__ import annotations

import os

from job_notifier.http_client import FetchError, HttpClient
from job_notifier.models import SourceResult
from job_notifier.sources.base import JobSource


class StartupJobsSource(JobSource):
    """Fetches startup jobs from startup.jobs API.

    Requires: STARTUPJOBS_API_KEY environment variable
    Get a free API key at: https://startup.jobs/account/api_keys
    """
    source_type = "startupjobs"

    def fetch(self, client: HttpClient) -> SourceResult:
        api_key = os.getenv("STARTUPJOBS_API_KEY")
        if not api_key:
            raise FetchError(
                "STARTUPJOBS_API_KEY environment variable not set. "
                "Get a free API key at https://startup.jobs/account/api_keys"
            )

        url = "https://api.startup.jobs/v1/jobs"

        # Build query parameters from config
        query_params = {"limit": "100"}  # Default limit

        # Allow filtering by role, workplace_type, etc.
        if "role" in self.config:
            query_params["role"] = str(self.config["role"])
        if "workplace_type" in self.config:
            query_params["workplace_type"] = str(self.config["workplace_type"])
        if "limit" in self.config:
            query_params["limit"] = str(self.config["limit"])

        headers = {"Authorization": f"Bearer {api_key}"}
        response = client.get(url, headers=headers, query=query_params)

        return SourceResult.from_response(
            source_name=self.name,
            source_type=self.source_type,
            url=response.url,
            status=response.status,
            content_type=response.headers.get("Content-Type"),
            payload=response.json(),
        )
