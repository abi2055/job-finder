from __future__ import annotations

from job_notifier.sources.ashby import AshbySource
from job_notifier.sources.base import JobSource
from job_notifier.sources.github_raw import GitHubRawSource
from job_notifier.sources.greenhouse import GreenhouseSource
from job_notifier.sources.himalayas import HimalayasSource
from job_notifier.sources.lever import LeverSource
from job_notifier.sources.startupjobs import StartupJobsSource
from job_notifier.sources.ycombinator import YCombinatorSource

SOURCE_TYPES: dict[str, type[JobSource]] = {
    "ashby": AshbySource,
    "github_raw": GitHubRawSource,
    "greenhouse": GreenhouseSource,
    "himalayas": HimalayasSource,
    "lever": LeverSource,
    "startupjobs": StartupJobsSource,
    "ycombinator": YCombinatorSource,
}

__all__ = ["JobSource", "SOURCE_TYPES"]

