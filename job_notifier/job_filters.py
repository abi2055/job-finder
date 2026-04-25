from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Any

from job_notifier.models import SourceResult


LATEST_FIRST_FIELDS = (
    "date_updated",
    "updated_at",
    "updatedAt",
    "date_posted",
    "created_at",
    "createdAt",
    "first_published",
)


def prioritize_jobs(
    results: list[SourceResult],
    *,
    include_closed: bool,
    prioritize_latest: bool,
) -> list[SourceResult]:
    return [
        prioritize_jobs_for_source(
            result,
            include_closed=include_closed,
            prioritize_latest=prioritize_latest,
        )
        for result in results
    ]


def prioritize_jobs_for_source(
    result: SourceResult,
    *,
    include_closed: bool,
    prioritize_latest: bool,
) -> SourceResult:
    payload = result.payload

    if isinstance(payload, list):
        return replace(
            result,
            payload=_filter_and_sort_jobs(
                payload,
                include_closed=include_closed,
                prioritize_latest=prioritize_latest,
            ),
        )

    if isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
        filtered_payload = {
            **payload,
            "jobs": _filter_and_sort_jobs(
                payload["jobs"],
                include_closed=include_closed,
                prioritize_latest=prioritize_latest,
            ),
        }
        return replace(result, payload=filtered_payload)

    return result


def _filter_and_sort_jobs(
    jobs: list[Any],
    *,
    include_closed: bool,
    prioritize_latest: bool,
) -> list[Any]:
    filtered_jobs = jobs if include_closed else [job for job in jobs if _is_open_job(job)]
    if not prioritize_latest:
        return filtered_jobs
    return sorted(filtered_jobs, key=_latest_timestamp, reverse=True)


def _is_open_job(job: Any) -> bool:
    if not isinstance(job, dict):
        return True

    if job.get("active") is False:
        return False
    if job.get("is_visible") is False:
        return False
    if _has_closed_state(job.get("status")):
        return False
    if _has_closed_state(job.get("state")):
        return False

    return True


def _has_closed_state(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return value.strip().lower() in {"closed", "inactive", "archived", "filled", "expired"}


def _latest_timestamp(job: Any) -> float:
    if not isinstance(job, dict):
        return 0.0

    for field_name in LATEST_FIRST_FIELDS:
        if field_name in job:
            timestamp = _coerce_timestamp(job[field_name])
            if timestamp is not None:
                return timestamp

    return 0.0


def _coerce_timestamp(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None

    if isinstance(value, int | float):
        return float(value / 1000 if value > 10_000_000_000 else value)

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit():
            return _coerce_timestamp(int(stripped))
        try:
            return datetime.fromisoformat(stripped.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None

    return None
