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
        "--attach-raw",
        action="store_true",
        help="Attach the raw output JSON as a gzipped file.",
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

    payload = build_email_payload(
        results=results,
        errors=errors,
        output_path=args.output,
        top_jobs=args.top_jobs,
        attach_raw=args.attach_raw,
    )

    if args.dry_run:
        print(json.dumps(describe_payload(payload), indent=2))
        return 0

    response = send_resend_email(payload)
    print(json.dumps(response, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

