from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class FetchError(RuntimeError):
    """Raised when an HTTP request cannot be completed."""


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status: int
    headers: dict[str, str]
    body: str

    def json(self) -> Any:
        return json.loads(self.body)


@dataclass
class HttpClient:
    timeout_seconds: float = 30.0
    max_retries: int = 2
    backoff_seconds: float = 1.0
    default_headers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        token = os.getenv("GITHUB_TOKEN")
        self.default_headers.setdefault("User-Agent", "job-notifier/0.1")
        self.default_headers.setdefault("Accept", "application/json, text/plain, */*")
        if token:
            self.default_headers.setdefault("Authorization", f"Bearer {token}")

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        query: dict[str, str | int | bool] | None = None,
    ) -> HttpResponse:
        if query:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{urlencode(query)}"

        request_headers = {**self.default_headers, **(headers or {})}
        last_error: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                request = Request(url, headers=request_headers, method="GET")
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    body = response.read().decode(response.headers.get_content_charset() or "utf-8")
                    return HttpResponse(
                        url=response.url,
                        status=response.status,
                        headers=dict(response.headers.items()),
                        body=body,
                    )
            except HTTPError as error:
                body = error.read().decode("utf-8", errors="replace")
                if error.code < 500 or attempt >= self.max_retries:
                    raise FetchError(f"GET {url} failed with HTTP {error.code}: {body[:500]}") from error
                last_error = error
            except URLError as error:
                if attempt >= self.max_retries:
                    raise FetchError(f"GET {url} failed: {error.reason}") from error
                last_error = error

            time.sleep(self.backoff_seconds * (attempt + 1))

        raise FetchError(f"GET {url} failed: {last_error}")

