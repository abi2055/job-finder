from __future__ import annotations

import base64
import gzip
import html
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from job_notifier.models import SourceResult
from job_notifier.normalizer import iter_normalized_jobs
from job_notifier.notification_preferences import (
    NotificationProfile,
    NotificationSection,
    filter_jobs_by_profile,
)

RESEND_EMAILS_URL = "https://api.resend.com/emails"
DEFAULT_FROM_EMAIL = "Job Notifier <onboarding@resend.dev>"


class EmailNotificationError(RuntimeError):
    """Raised when an email notification cannot be sent."""


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def collect_latest_jobs(results: list[SourceResult], *, limit: int) -> list[dict[str, Any]]:
    jobs_by_key: dict[str, dict[str, Any]] = {}
    seen_at = datetime.now(timezone.utc)
    for result in results:
        for normalized_job in iter_normalized_jobs(result, seen_at=seen_at):
            email_job = _email_job(normalized_job)
            existing_job = jobs_by_key.get(normalized_job.record_key)
            if existing_job is None or _latest_timestamp(email_job) >= _latest_timestamp(existing_job):
                jobs_by_key[normalized_job.record_key] = email_job

    return sorted(jobs_by_key.values(), key=_latest_timestamp, reverse=True)[:limit]


def build_email_payload(
    *,
    results: list[SourceResult],
    errors: list[dict[str, str]],
    output_path: Path,
    top_jobs: int,
    attach_raw: bool,
    profile: NotificationProfile | None = None,
    sections: list[NotificationSection] | None = None,
) -> dict[str, Any]:
    raw_job_count = sum(_job_count(result.payload) for result in results)
    unique_jobs = collect_latest_jobs(results, limit=raw_job_count)
    email_sections = _build_email_sections(
        unique_jobs,
        profile=profile,
        sections=sections,
        fallback_limit=top_jobs,
    )
    matching_job_count = sum(section["total"] for section in email_sections)
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    payload: dict[str, Any] = {
        "from": os.getenv("RESEND_FROM_EMAIL") or DEFAULT_FROM_EMAIL,
        "to": [_required_env("RESEND_TO_EMAIL")],
        "subject": f"Job Notifier: {matching_job_count:,} matching jobs",
        "html": _render_html(
            fetched_at=fetched_at,
            job_count=len(unique_jobs),
            matching_job_count=matching_job_count,
            source_count=len(results),
            errors=errors,
            email_sections=email_sections,
        ),
        "text": _render_text(
            fetched_at=fetched_at,
            job_count=len(unique_jobs),
            matching_job_count=matching_job_count,
            source_count=len(results),
            errors=errors,
            email_sections=email_sections,
        ),
    }

    if attach_raw:
        payload["attachments"] = [_build_gzip_attachment(output_path)]

    return payload


def send_resend_email(payload: dict[str, Any]) -> dict[str, Any]:
    api_key = _required_env("RESEND_API_KEY")
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        RESEND_EMAILS_URL,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "job-notifier/0.1",
        },
    )

    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise EmailNotificationError(f"Resend failed with HTTP {error.code}: {error_body}") from error
    except URLError as error:
        raise EmailNotificationError(f"Resend request failed: {error.reason}") from error


def _render_html(
    *,
    fetched_at: str,
    job_count: int,
    matching_job_count: int,
    source_count: int,
    errors: list[dict[str, str]],
    email_sections: list[dict[str, Any]],
) -> str:
    sections_html = "\n".join(_render_html_section(section) for section in email_sections)
    error_html = ""
    if errors:
        error_items = "".join(
            f"<li>{html.escape(error.get('source_name', 'unknown'))}: "
            f"{html.escape(error.get('error', 'unknown error'))}</li>"
            for error in errors
        )
        error_html = f"<h2>Errors</h2><ul>{error_items}</ul>"

    return f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #1d211f;">
        <h1>Job Notifier</h1>
        <p>Fetched at {html.escape(fetched_at)}.</p>
        <p><strong>{matching_job_count:,}</strong> matching jobs from <strong>{job_count:,}</strong> open structured jobs and <strong>{source_count}</strong> source payloads.</p>
        {error_html}
        {sections_html}
      </body>
    </html>
    """


def _render_text(
    *,
    fetched_at: str,
    job_count: int,
    matching_job_count: int,
    source_count: int,
    errors: list[dict[str, str]],
    email_sections: list[dict[str, Any]],
) -> str:
    lines = [
        "Job Notifier",
        f"Fetched at {fetched_at}.",
        f"{matching_job_count:,} matching jobs from {job_count:,} open structured jobs and {source_count} source payloads.",
    ]
    for section in email_sections:
        lines.extend(["", f"{section['title']} ({section['total']:,} matches):"])
        if section.get("description"):
            lines.append(section["description"])
        for job in section["jobs"]:
            lines.append(
                " - "
                f"{_job_date(job)} | {job.get('company_name') or ''} | "
                f"{job.get('title') or ''} | {', '.join(map(str, job.get('locations') or []))} | "
                f"{job.get('source_type') or ''} | {job.get('job_url') or ''}"
            )
    if errors:
        lines.extend(["", "Errors:"])
        lines.extend(
            f" - {error.get('source_name', 'unknown')}: {error.get('error', 'unknown error')}"
            for error in errors
        )
    return "\n".join(lines)


def _build_email_sections(
    unique_jobs: list[dict[str, Any]],
    *,
    profile: NotificationProfile | None,
    sections: list[NotificationSection] | None,
    fallback_limit: int,
) -> list[dict[str, Any]]:
    if sections:
        return [
            _build_email_section(
                unique_jobs,
                title=section.title,
                profile=section.profile,
                limit=section.limit or fallback_limit,
            )
            for section in sections
        ]

    matching_jobs = filter_jobs_by_profile(unique_jobs, profile)
    title = profile.name if profile else "Latest jobs"
    return [
        {
            "title": title,
            "description": profile.description if profile else "",
            "total": len(matching_jobs),
            "jobs": matching_jobs[:fallback_limit],
        }
    ]


def _build_email_section(
    unique_jobs: list[dict[str, Any]],
    *,
    title: str,
    profile: NotificationProfile,
    limit: int,
) -> dict[str, Any]:
    matching_jobs = filter_jobs_by_profile(unique_jobs, profile)
    return {
        "title": title,
        "description": profile.description,
        "total": len(matching_jobs),
        "jobs": matching_jobs[:limit],
    }


def _render_html_section(section: dict[str, Any]) -> str:
    rows = "\n".join(_render_html_job_row(job) for job in section["jobs"])
    if not rows:
        rows = '<tr><td colspan="6">No matching jobs.</td></tr>'
    description = section.get("description") or ""
    description_html = f"<p>{html.escape(description)}</p>" if description else ""
    return f"""
        <h2>{html.escape(str(section["title"]))} ({section["total"]:,})</h2>
        {description_html}
        <table cellpadding="8" cellspacing="0" border="1" style="border-collapse: collapse;">
          <thead>
            <tr>
              <th>Updated</th>
              <th>Company</th>
              <th>Role</th>
              <th>Location</th>
              <th>Category</th>
              <th>Source</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
    """


def _render_html_job_row(job: dict[str, Any]) -> str:
    return f"""
        <tr>
          <td>{html.escape(_job_date(job))}</td>
          <td>{html.escape(str(job.get("company_name") or ""))}</td>
          <td><a href="{html.escape(str(job.get("job_url") or ""))}">{html.escape(str(job.get("title") or ""))}</a></td>
          <td>{html.escape(", ".join(map(str, job.get("locations") or [])))}</td>
          <td>{html.escape(str(job.get("category") or ""))}</td>
          <td>{html.escape(str(job.get("source_type") or ""))}</td>
        </tr>
    """


def _build_gzip_attachment(output_path: Path) -> dict[str, str]:
    raw_bytes = output_path.read_bytes()
    compressed = gzip.compress(raw_bytes)
    return {
        "filename": f"{output_path.name}.gz",
        "content": base64.b64encode(compressed).decode("ascii"),
    }


def _job_count(payload: Any) -> int:
    if isinstance(payload, list):
        return sum(1 for job in payload if isinstance(job, dict))
    if isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
        return sum(1 for job in payload["jobs"] if isinstance(job, dict))
    return 0


def _latest_timestamp(job: dict[str, Any]) -> float:
    value = job.get("date_updated_at") or job.get("date_posted_at")
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, int | float):
        return float(value / 1000 if value > 10_000_000_000 else value)
    return 0.0


def _job_date(job: dict[str, Any]) -> str:
    timestamp = _latest_timestamp(job)
    if not timestamp:
        return ""
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d")


def _email_job(job: Any) -> dict[str, Any]:
    return {
        "record_key": job.record_key,
        "source_name": job.source_name,
        "source_type": job.source_type,
        "company_name": job.company_name,
        "title": job.title,
        "job_url": job.job_url,
        "category": job.category,
        "locations": job.locations,
        "terms": job.terms,
        "degrees": job.degrees,
        "sponsorship": job.sponsorship,
        "date_posted_at": job.date_posted_at,
        "date_updated_at": job.date_updated_at,
    }


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EmailNotificationError(f"Missing required environment variable: {name}")
    return value


def describe_payload(payload: dict[str, Any]) -> dict[str, Any]:
    description = {key: value for key, value in payload.items() if key != "attachments"}
    description["to"] = ["<configured recipient>"]
    if "attachments" in payload:
        description["attachments"] = [
            {
                "filename": attachment["filename"],
                "content_base64_bytes": len(attachment["content"]),
            }
            for attachment in payload["attachments"]
        ]
    return description


def results_to_dict(results: list[SourceResult], errors: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "results": [asdict(result) for result in results],
        "errors": errors,
    }
