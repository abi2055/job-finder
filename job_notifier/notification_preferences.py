from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_PREFERENCES_PATH = Path("notification_preferences.json")


@dataclass(frozen=True)
class NotificationProfile:
    name: str
    description: str
    max_age_hours: int | None
    include_any: list[dict[str, list[str]]]
    include_all: list[dict[str, list[str]]]
    exclude_text: list[str]


def load_notification_profile(
    path: Path | None = None,
    *,
    profile_name: str | None = None,
) -> NotificationProfile | None:
    preferences_path = path or DEFAULT_PREFERENCES_PATH
    if not preferences_path.exists():
        return None

    data = json.loads(preferences_path.read_text(encoding="utf-8"))
    selected_profile = profile_name or data.get("active_profile")
    profile_data = data.get("profiles", {}).get(selected_profile)
    if not selected_profile or not isinstance(profile_data, dict):
        raise ValueError(f"No active notification profile found in {preferences_path}")

    return NotificationProfile(
        name=str(selected_profile),
        description=str(profile_data.get("description") or ""),
        max_age_hours=_optional_int(profile_data.get("max_age_hours")),
        include_any=_groups(profile_data.get("include_any")),
        include_all=_groups(profile_data.get("include_all")),
        exclude_text=[str(value) for value in profile_data.get("exclude_text", [])],
    )


def filter_jobs_by_profile(
    jobs: list[dict[str, Any]],
    profile: NotificationProfile | None,
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    if profile is None:
        return jobs

    current_time = now or datetime.now(timezone.utc)
    return [job for job in jobs if _matches_profile(job, profile, now=current_time)]


def _matches_profile(job: dict[str, Any], profile: NotificationProfile, *, now: datetime) -> bool:
    if profile.max_age_hours is not None and not _within_age(job, profile.max_age_hours, now=now):
        return False

    searchable_text = _searchable_job_text(job)
    if any(_contains(searchable_text, value) for value in profile.exclude_text):
        return False

    if profile.include_all and not all(_matches_group(job, group) for group in profile.include_all):
        return False

    if profile.include_any and not any(_matches_group(job, group) for group in profile.include_any):
        return False

    return True


def _matches_group(job: dict[str, Any], group: dict[str, list[str]]) -> bool:
    return all(_field_contains_any(job, field_name, values) for field_name, values in group.items())


def _field_contains_any(job: dict[str, Any], field_name: str, values: list[str]) -> bool:
    field_text = _field_text(job, field_name)
    return any(_contains(field_text, value) for value in values)


def _field_text(job: dict[str, Any], field_name: str) -> str:
    value = job.get(field_name)
    if isinstance(value, list):
        return " ".join(str(item) for item in value)
    if value is None:
        return ""
    return str(value)


def _searchable_job_text(job: dict[str, Any]) -> str:
    return " ".join(
        _field_text(job, field_name)
        for field_name in (
            "company_name",
            "title",
            "category",
            "locations",
            "terms",
            "degrees",
            "sponsorship",
            "url",
        )
    )


def _within_age(job: dict[str, Any], max_age_hours: int, *, now: datetime) -> bool:
    timestamp = _latest_timestamp(job)
    if timestamp is None:
        return False
    age_seconds = now.timestamp() - timestamp
    return age_seconds <= max_age_hours * 60 * 60


def _latest_timestamp(job: dict[str, Any]) -> float | None:
    value = job.get("date_updated") or job.get("updatedAt") or job.get("date_posted") or job.get("createdAt")
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        return float(value / 1000 if value > 10_000_000_000 else value)
    if isinstance(value, str) and value.strip().isdigit():
        return _latest_timestamp({"date_updated": int(value.strip())})
    return None


def _contains(text: str, value: str) -> bool:
    return value.casefold() in text.casefold()


def _groups(value: Any) -> list[dict[str, list[str]]]:
    if not isinstance(value, list):
        return []

    groups: list[dict[str, list[str]]] = []
    for group in value:
        if not isinstance(group, dict):
            continue
        groups.append(
            {
                str(field_name): [str(keyword) for keyword in keywords]
                for field_name, keywords in group.items()
                if isinstance(keywords, list)
            }
        )
    return groups


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
