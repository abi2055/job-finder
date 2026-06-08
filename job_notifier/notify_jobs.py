from __future__ import annotations

import argparse
import json
from pathlib import Path

from job_notifier.config import load_config
from job_notifier.email_notifier import (
    build_email_payload,
    describe_payload,
    load_dotenv,
    send_resend_email,
)
from job_notifier.http_client import HttpClient
from job_notifier.notification_preferences import load_notification_profile, load_notification_sections
from job_notifier.service import fetch_sources, write_output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch jobs and email a Resend notification.")
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to a JSON source config. Defaults to the built-in starter sources.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw_jobs.json"),
        help="Path to write the raw aggregate JSON output.",
    )
    parser.add_argument(
        "--top-jobs",
        type=int,
        default=25,
        help="Number of latest jobs to include in the email body.",
    )
    parser.add_argument(
        "--preferences",
        type=Path,
        default=Path("notification_preferences.json"),
        help="Path to notification preferences JSON. Omit the file to email all open jobs.",
    )
    parser.add_argument(
        "--profile",
        help="Override the active profile from the notification preferences file.",
    )
    parser.add_argument(
        "--attach-raw",
        action="store_true",
        help="Attach the raw output JSON as a gzipped file.",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path("data/seen_jobs.json"),
        help="Path to JSON file tracking previously-notified job record_keys. "
        "If this file exists and no new matching jobs are found, the email is skipped.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and render the email payload without sending it.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    client = HttpClient()

    results, errors = fetch_sources(
        config,
        client=client,
        continue_on_error=True,
        include_closed=False,
        prioritize_latest=True,
    )
    write_output(args.output, results, errors)
    profile = load_notification_profile(args.preferences, profile_name=args.profile)
    sections = [] if args.profile else load_notification_sections(args.preferences)

    payload, matching_record_keys = build_email_payload(
        results=results,
        errors=errors,
        output_path=args.output,
        top_jobs=args.top_jobs,
        attach_raw=args.attach_raw,
        profile=profile,
        sections=sections,
    )

    seen_keys, state_exists = _load_seen_keys(args.state_file)
    new_keys = matching_record_keys - seen_keys

    if state_exists and not new_keys:
        print(
            json.dumps(
                {
                    "skipped": True,
                    "reason": "no new matching jobs since previous run",
                    "matching_jobs": len(matching_record_keys),
                    "seen_jobs": len(seen_keys),
                },
                indent=2,
            )
        )
        return 0

    if args.dry_run:
        description = describe_payload(payload)
        description["new_matching_jobs"] = len(new_keys)
        description["state_file_exists"] = state_exists
        print(json.dumps(description, indent=2))
        return 0

    response = send_resend_email(payload)
    _write_seen_keys(args.state_file, seen_keys | matching_record_keys)
    print(json.dumps(response, indent=2))
    return 0


def _load_seen_keys(state_path: Path) -> tuple[set[str], bool]:
    if not state_path.exists():
        return set(), False
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set(), False
    keys = data.get("record_keys") if isinstance(data, dict) else None
    if not isinstance(keys, list):
        return set(), False
    return {str(key) for key in keys if isinstance(key, str)}, True


def _write_seen_keys(state_path: Path, keys: set[str]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({"record_keys": sorted(keys)}, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    raise SystemExit(main())
