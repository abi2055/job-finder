from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse

from job_notifier.models import SourceResult

EARLY_CAREER_TERMS = (
    "intern",
    "internship",
    "co-op",
    "coop",
    "student",
    "university",
    "new grad",
    "new college grad",
    "graduate",
    "early career",
    "entry level",
    "apprentice",
)
TECH_TERMS = (
    "software",
    "developer",
    "frontend",
    "front end",
    "backend",
    "back end",
    "fullstack",
    "full stack",
    "data",
    "machine learning",
    "ml",
    "ai",
    "artificial intelligence",
    "security",
    "infrastructure",
    "platform",
    "cloud",
    "devops",
    "sre",
    "site reliability",
    "technical",
    "technology",
    "systems",
    "quant",
    "robotics",
)
NON_SOFTWARE_TECH_EXCLUSIONS = (
    "electrical",
    "mechanical",
    "hardware",
    "manufacturing",
    "industrial",
    "civil",
    "aerospace",
    "chemical",
    "materials",
    "firmware",
    "embedded hardware",
    "silicon",
    "asic",
    "fpga",
    "pcb",
    "interconnect",
)
SENIORITY_EXCLUSIONS = (
    "senior",
    "sr.",
    "staff",
    "principal",
    "lead ",
    "manager",
    "director",
    "head of",
    "vp ",
    "vice president",
)
NON_TECH_EXCLUSIONS = (
    "account executive",
    "mba",
    "sales",
    "marketing",
    "finance",
    "accountant",
    "counsel",
    "legal",
    "recruiter",
    "people partner",
    "human resources",
    "customer success",
    "support",
    "operations",
    "policy",
    "communications",
    "payroll",
)

@dataclass(frozen=True)
class NormalizedJob:
    record_key: str
    source_name: str
    source_type: str
    source_url: str
    source_record_index: int
    upstream_source: str | None
    external_id: str | None
    company_name: str | None
    company_url: str | None
    title: str | None
    job_url: str | None
    category: str | None
    locations: list[Any]
    terms: list[Any]
    degrees: list[Any]
    sponsorship: str | None
    active: bool | None
    is_visible: bool | None
    date_posted_at: datetime | None
    date_updated_at: datetime | None
    fetched_at: datetime
    raw_payload: dict[str, Any]


def iter_normalized_jobs(result: SourceResult, *, seen_at: datetime) -> list[NormalizedJob]:
    jobs: list[NormalizedJob] = []
    for index, payload in enumerate(_job_payloads(result.payload)):
        if not isinstance(payload, dict):
            continue
        normalized = normalize_job(result, payload, index=index, seen_at=seen_at)
        if normalized and _is_open_job(normalized):
            jobs.append(normalized)
    return jobs


def normalize_job(
    result: SourceResult,
    payload: dict[str, Any],
    *,
    index: int,
    seen_at: datetime,
) -> NormalizedJob | None:
    if result.source_type == "greenhouse":
        return _normalize_greenhouse(result, payload, index=index, seen_at=seen_at)
    if result.source_type == "lever":
        return _normalize_lever(result, payload, index=index, seen_at=seen_at)
    return _normalize_generic(result, payload, index=index, seen_at=seen_at)


def _normalize_greenhouse(
    result: SourceResult,
    payload: dict[str, Any],
    *,
    index: int,
    seen_at: datetime,
) -> NormalizedJob:
    board_token = _greenhouse_board_token(result.url)
    company_name = _company_from_source_name(result.source_name)
    offices = payload.get("offices") if isinstance(payload.get("offices"), list) else []
    departments = payload.get("departments") if isinstance(payload.get("departments"), list) else []
    locations = _unique_list(
        [
            _nested_string(payload.get("location"), "name"),
            *[_string_or_none(office.get("location") or office.get("name")) for office in offices],
        ]
    )
    category = _first_string([department.get("name") for department in departments])
    external_id = _string_or_none(payload.get("id"))
    job_url = _string_or_none(payload.get("absolute_url"))

    job = _build_job(
        result,
        payload,
        index=index,
        seen_at=seen_at,
        upstream_source="greenhouse",
        external_id=external_id,
        company_name=company_name,
        company_url=f"https://boards.greenhouse.io/{board_token}" if board_token else None,
        title=_string_or_none(payload.get("title")),
        job_url=job_url,
        category=category,
        locations=locations,
        terms=[],
        degrees=[],
        sponsorship=_metadata_value(payload.get("metadata"), ("sponsorship", "visa")),
        active=True,
        is_visible=True,
        date_posted_at=None,
        date_updated_at=_parse_datetime(payload.get("updated_at")),
    )
    return job if _is_tech_early_career_job(job) else None


def _normalize_lever(
    result: SourceResult,
    payload: dict[str, Any],
    *,
    index: int,
    seen_at: datetime,
) -> NormalizedJob:
    categories = payload.get("categories") if isinstance(payload.get("categories"), dict) else {}
    company_name = _company_from_source_name(result.source_name)
    all_locations = categories.get("allLocations")
    locations = all_locations if isinstance(all_locations, list) else _list_value(categories.get("location"))
    external_id = _string_or_none(payload.get("id"))
    job_url = _string_or_none(payload.get("hostedUrl") or payload.get("applyUrl"))

    job = _build_job(
        result,
        payload,
        index=index,
        seen_at=seen_at,
        upstream_source="lever",
        external_id=external_id,
        company_name=company_name,
        company_url=_lever_site_url(result.url),
        title=_string_or_none(payload.get("text")),
        job_url=job_url,
        category=_string_or_none(categories.get("team") or categories.get("department")),
        locations=locations,
        terms=_list_value(categories.get("commitment")),
        degrees=[],
        sponsorship=None,
        active=True,
        is_visible=True,
        date_posted_at=_parse_datetime(payload.get("createdAt")),
        date_updated_at=_parse_datetime(payload.get("updatedAt") or payload.get("createdAt")),
    )
    return job if _is_tech_early_career_job(job) else None


def _normalize_generic(
    result: SourceResult,
    payload: dict[str, Any],
    *,
    index: int,
    seen_at: datetime,
) -> NormalizedJob:
    external_id = _string_or_none(payload.get("id") or payload.get("job_id") or payload.get("postingId"))
    job_url = _string_or_none(payload.get("url") or payload.get("absolute_url") or payload.get("hostedUrl"))
    return _build_job(
        result,
        payload,
        index=index,
        seen_at=seen_at,
        upstream_source=_string_or_none(payload.get("source")) or result.source_type,
        external_id=external_id,
        company_name=_string_or_none(
            payload.get("company_name") or payload.get("company") or payload.get("companyName")
        ),
        company_url=_string_or_none(payload.get("company_url")),
        title=_string_or_none(payload.get("title") or payload.get("text")),
        job_url=job_url,
        category=_string_or_none(payload.get("category") or payload.get("department")),
        locations=_list_value(payload.get("locations") or payload.get("location")),
        terms=_list_value(payload.get("terms")),
        degrees=_list_value(payload.get("degrees")),
        sponsorship=_string_or_none(payload.get("sponsorship")),
        active=_bool_or_none(payload.get("active")),
        is_visible=_bool_or_none(payload.get("is_visible")),
        date_posted_at=_parse_datetime(payload.get("date_posted") or payload.get("createdAt")),
        date_updated_at=_parse_datetime(payload.get("date_updated") or payload.get("updatedAt")),
    )


def _build_job(
    result: SourceResult,
    payload: dict[str, Any],
    *,
    index: int,
    seen_at: datetime,
    upstream_source: str | None,
    external_id: str | None,
    company_name: str | None,
    company_url: str | None,
    title: str | None,
    job_url: str | None,
    category: str | None,
    locations: list[Any],
    terms: list[Any],
    degrees: list[Any],
    sponsorship: str | None,
    active: bool | None,
    is_visible: bool | None,
    date_posted_at: datetime | None,
    date_updated_at: datetime | None,
) -> NormalizedJob:
    fetched_at = _parse_datetime(result.fetched_at) or seen_at
    canonical_url = _canonical_job_url(job_url)
    return NormalizedJob(
        record_key=_record_key(
            external_id=external_id,
            job_url=canonical_url or job_url,
            company_name=company_name,
            title=title,
            payload=payload,
        ),
        source_name=result.source_name,
        source_type=result.source_type,
        source_url=result.url,
        source_record_index=index,
        upstream_source=upstream_source,
        external_id=external_id,
        company_name=company_name,
        company_url=company_url,
        title=title,
        job_url=job_url,
        category=category,
        locations=_unique_list(locations),
        terms=_unique_list(terms),
        degrees=_unique_list(degrees),
        sponsorship=sponsorship,
        active=active,
        is_visible=is_visible,
        date_posted_at=date_posted_at,
        date_updated_at=date_updated_at,
        fetched_at=fetched_at,
        raw_payload=payload,
    )


def _is_open_job(job: NormalizedJob) -> bool:
    if job.active is False or job.is_visible is False:
        return False
    searchable = " ".join(
        str(value)
        for value in (
            job.title,
            job.category,
            job.sponsorship,
            " ".join(map(str, job.locations)),
            " ".join(map(str, job.terms)),
        )
        if value
    ).casefold()
    return not any(value in searchable for value in ("closed", "inactive", "archived", "filled", "expired"))


def _is_tech_early_career_job(job: NormalizedJob) -> bool:
    text = _job_search_text(job)
    title_text = (job.title or "").casefold()
    has_early_career_signal = any(term in text for term in EARLY_CAREER_TERMS)
    has_tech_signal = any(term in title_text for term in TECH_TERMS)
    has_seniority_exclusion = any(term in text for term in SENIORITY_EXCLUSIONS)
    has_non_tech_exclusion = any(term in text for term in NON_TECH_EXCLUSIONS)
    has_non_software_exclusion = any(term in title_text for term in NON_SOFTWARE_TECH_EXCLUSIONS)

    if not has_early_career_signal or not has_tech_signal:
        return False
    if has_seniority_exclusion or has_non_tech_exclusion or has_non_software_exclusion:
        return False
    return True


def _job_search_text(job: NormalizedJob) -> str:
    return " ".join(
        str(value)
        for value in (
            job.title,
            job.category,
            " ".join(map(str, job.locations)),
            " ".join(map(str, job.terms)),
            " ".join(map(str, job.degrees)),
            job.sponsorship,
        )
        if value
    ).casefold()


def _job_payloads(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
        return payload["jobs"]
    return []


def _record_key(
    *,
    external_id: str | None,
    job_url: str | None,
    company_name: str | None,
    title: str | None,
    payload: dict[str, Any],
) -> str:
    if job_url:
        digest_input = f"url:{job_url}"
    elif external_id and company_name:
        digest_input = f"id:{_clean(company_name)}:{external_id}"
    elif company_name and title:
        digest_input = f"role:{_clean(company_name)}:{_clean(title)}"
    else:
        digest_input = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(digest_input.encode("utf-8")).hexdigest()


def _canonical_job_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url.strip())
    host = parsed.netloc.lower().removeprefix("www.")
    path = re.sub(r"/+", "/", parsed.path.rstrip("/"))
    query = parse_qs(parsed.query)

    if "boards.greenhouse.io" in host and "token" in query:
        return f"https://boards.greenhouse.io/job/{query['token'][0]}"

    return f"{parsed.scheme.lower() or 'https'}://{host}{path}".casefold()


def _metadata_value(metadata: Any, needles: tuple[str, ...]) -> str | None:
    if not isinstance(metadata, list):
        return None
    for item in metadata:
        if not isinstance(item, dict):
            continue
        label = str(item.get("name") or item.get("label") or "").casefold()
        if any(needle in label for needle in needles):
            value = item.get("value")
            if isinstance(value, list):
                return ", ".join(map(str, value))
            return _string_or_none(value)
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, int | float):
        seconds = float(value / 1000 if value > 10_000_000_000 else value)
        return datetime.fromtimestamp(seconds, timezone.utc)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit():
            return _parse_datetime(int(stripped))
        try:
            parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _greenhouse_board_token(url: str) -> str | None:
    match = re.search(r"/boards/([^/]+)/jobs", url)
    return match.group(1) if match else None


def _lever_site_url(url: str) -> str | None:
    match = re.search(r"/postings/([^/?]+)", url)
    if not match:
        return None
    return f"https://jobs.lever.co/{match.group(1)}"


def _company_from_source_name(source_name: str) -> str:
    cleaned = re.sub(r"_(greenhouse|lever|board|jobs|postings)$", "", source_name, flags=re.I)
    return cleaned.replace("_", " ").replace("-", " ").title()


def _clean(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _nested_string(value: Any, key: str) -> str | None:
    if isinstance(value, dict):
        return _string_or_none(value.get(key))
    return None


def _first_string(values: list[Any]) -> str | None:
    for value in values:
        string_value = _string_or_none(value)
        if string_value:
            return string_value
    return None


def _unique_list(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    unique_values: list[Any] = []
    for value in values:
        if value is None or value == "":
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        seen.add(key)
        unique_values.append(value)
    return unique_values


def _list_value(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _bool_or_none(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
