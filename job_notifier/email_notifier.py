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
    jobs: list[dict[str, Any]] = []
    for result in results:
        payload = result.payload
        if isinstance(payload, list):
            jobs.extend(job for job in payload if isinstance(job, dict))
        elif isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
            jobs.extend(job for job in payload["jobs"] if isinstance(job, dict))

    return sorted(jobs, key=_latest_timestamp, reverse=True)[:limit]


def build_email_payload(
    *,
    results: list[SourceResult],
    errors: list[dict[str, str]],
    output_path: Path,
    top_jobs: int,
    attach_raw: bool,
) -> dict[str, Any]:
    latest_jobs = collect_latest_jobs(results, limit=top_jobs)
    job_count = sum(_job_count(result.payload) for result in results)
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    payload: dict[str, Any] = {
        "from": os.getenv("RESEND_FROM_EMAIL") or DEFAULT_FROM_EMAIL,
        "to": [_required_env("RESEND_TO_EMAIL")],
        "subject": f"Job Notifier: {job_count:,} open jobs fetched",
        "html": _render_html(
            fetched_at=fetched_at,
            job_count=job_count,
            source_count=len(results),
            errors=errors,
            latest_jobs=latest_jobs,
        ),
        "text": _render_text(
            fetched_at=fetched_at,
            job_count=job_count,
            source_count=len(results),
            errors=errors,
            latest_jobs=latest_jobs,
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
    source_count: int,
    errors: list[dict[str, str]],
    latest_jobs: list[dict[str, Any]],
) -> str:
    rows = "\n".join(
        f"""
        <tr>
          <td>{html.escape(_job_date(job))}</td>
          <td>{html.escape(str(job.get("company_name") or ""))}</td>
          <td><a href="{html.escape(str(job.get("url") or ""))}">{html.escape(str(job.get("title") or ""))}</a></td>
          <td>{html.escape(", ".join(map(str, job.get("locations") or [])))}</td>
          <td>{html.escape(str(job.get("category") or ""))}</td>
        </tr>
        """
        for job in latest_jobs
    )
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
        <p><strong>{job_count:,}</strong> open structured jobs from <strong>{source_count}</strong> source payloads.</p>
        {error_html}
        <h2>Latest jobs</h2>
        <table cellpadding="8" cellspacing="0" border="1" style="border-collapse: collapse;">
          <thead>
            <tr>
              <th>Updated</th>
              <th>Company</th>
              <th>Role</th>
              <th>Location</th>
              <th>Category</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </body>
    </html>
    """


def _render_text(
    *,
    fetched_at: str,
    job_count: int,
    source_count: int,
    errors: list[dict[str, str]],
    latest_jobs: list[dict[str, Any]],
) -> str:
    lines = [
        "Job Notifier",
        f"Fetched at {fetched_at}.",
        f"{job_count:,} open structured jobs from {source_count} source payloads.",
        "",
        "Latest jobs:",
    ]
    for job in latest_jobs:
        lines.append(
            " - "
            f"{_job_date(job)} | {job.get('company_name') or ''} | "
            f"{job.get('title') or ''} | {', '.join(map(str, job.get('locations') or []))} | "
            f"{job.get('url') or ''}"
        )
    if errors:
        lines.extend(["", "Errors:"])
        lines.extend(
            f" - {error.get('source_name', 'unknown')}: {error.get('error', 'unknown error')}"
            for error in errors
        )
    return "\n".join(lines)


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
    value = job.get("date_updated") or job.get("updatedAt") or job.get("date_posted") or job.get("createdAt")
    if isinstance(value, int | float):
        return float(value / 1000 if value > 10_000_000_000 else value)
    return 0.0


def _job_date(job: dict[str, Any]) -> str:
    timestamp = _latest_timestamp(job)
    if not timestamp:
        return ""
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d")


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
