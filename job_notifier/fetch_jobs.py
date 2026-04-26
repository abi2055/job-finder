from __future__ import annotations

import argparse
import sys
from pathlib import Path

from job_notifier.config import load_config
from job_notifier.database import build_engine, save_fetch_results
from job_notifier.http_client import HttpClient
from job_notifier.service import fetch_sources, write_output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch raw internship job data.")
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
        "--continue-on-error",
        action="store_true",
        help="Write successful source payloads even if some sources fail.",
    )
    parser.add_argument(
        "--include-closed",
        action="store_true",
        help="Keep jobs marked inactive, closed, archived, filled, or expired.",
    )
    parser.add_argument(
        "--preserve-source-order",
        action="store_true",
        help="Do not sort structured job lists newest-first.",
    )
    parser.add_argument(
        "--save-to-db",
        action="store_true",
        help="Save structured job records and source snapshots to the database.",
    )
    parser.add_argument(
        "--database-url",
        help="Database URL. Defaults to DATABASE_URL or sqlite:///data/job_notifier.db.",
    )
    parser.add_argument(
        "--stale-after-days",
        type=int,
        default=14,
        help="Delete database jobs not seen again after this many days. Use -1 to disable.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    client = HttpClient()

    try:
        results, errors = fetch_sources(
            config,
            client=client,
            continue_on_error=args.continue_on_error,
            include_closed=args.include_closed,
            prioritize_latest=not args.preserve_source_order,
        )
    except Exception as error:
        print(f"Fetch failed: {error}", file=sys.stderr)
        return 1

    write_output(args.output, results, errors)
    print(f"Wrote {len(results)} source payload(s) to {args.output}")

    if args.save_to_db:
        db_summary = save_fetch_results(
            build_engine(args.database_url),
            results=results,
            errors=errors,
            stale_after_days=args.stale_after_days,
        )
        print(
            "Saved "
            f"{db_summary['job_record_count']} job record(s) "
            f"({db_summary['unique_job_record_count']} unique in database) "
            f"to database fetch_run_id={db_summary['fetch_run_id']} "
            f"deleted_duplicates={db_summary['deleted_duplicate_count']} "
            f"deleted_stale={db_summary['deleted_stale_count']}"
        )

    if errors:
        print(f"Encountered {len(errors)} source error(s); see output JSON.", file=sys.stderr)
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
