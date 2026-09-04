from __future__ import annotations

from job_notifier.sources.ashby import AshbySource
from job_notifier.sources.base import JobSource
from job_notifier.sources.github_raw import GitHubRawSource
from job_notifier.sources.greenhouse import GreenhouseSource
from job_notifier.sources.lever import LeverSource

SOURCE_TYPES: dict[str, type[JobSource]] = {
    "ashby": AshbySource,
    "github_raw": GitHubRawSource,
    "greenhouse": GreenhouseSource,
    "lever": LeverSource,
}

__all__ = ["JobSource", "SOURCE_TYPES"]

