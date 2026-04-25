from __future__ import annotations

import json
from typing import Any

from job_notifier.http_client import HttpClient
from job_notifier.models import SourceResult
from job_notifier.sources.base import JobSource


class GitHubRawSource(JobSource):
    source_type = "github_raw"

    def fetch(self, client: HttpClient) -> SourceResult:
        url = str(self.config["url"])
        response = client.get(url)
        content_type = response.headers.get("Content-Type")
        payload: Any = response.body

        if self._looks_like_json(url, content_type):
            payload = json.loads(response.body)

        return SourceResult.from_response(
            source_name=self.name,
            source_type=self.source_type,
            url=response.url,
            status=response.status,
            content_type=content_type,
            payload=payload,
        )

    @staticmethod
    def _looks_like_json(url: str, content_type: str | None) -> bool:
        return url.endswith(".json") or bool(content_type and "json" in content_type.lower())

