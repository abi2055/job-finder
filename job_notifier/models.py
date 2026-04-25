from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class SourceResult:
    source_name: str
    source_type: str
    url: str
    fetched_at: str
    status: int
    content_type: str | None
    payload: Any

    @classmethod
    def from_response(
        cls,
        *,
        source_name: str,
        source_type: str,
        url: str,
        status: int,
        content_type: str | None,
        payload: Any,
    ) -> "SourceResult":
        return cls(
            source_name=source_name,
            source_type=source_type,
            url=url,
            fetched_at=datetime.now(timezone.utc).isoformat(),
            status=status,
            content_type=content_type,
            payload=payload,
        )

