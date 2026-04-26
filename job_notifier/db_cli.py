from __future__ import annotations

import argparse
import json
from pathlib import Path

from job_notifier.database import build_engine, count_job_records, create_database, save_fetch_results
from job_notifier.models import SourceResult


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the Job Notifier database.")
    parser.add_argument(
        "--database-url",
        help="Database URL. Defaults to DATABASE_URL or sqlite:///data/job_notifier.db.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Create database tables.")

    import_parser = subparsers.add_parser("import-json", help="Import a raw fetch JSON file.")
    import_parser.add_argument(
        "path",
        type=Path,
        help="Path to a raw output file, for example data/raw_jobs.json.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    engine = build_engine(args.database_url)

    if args.command == "init":
        create_database(engine)
        print("Database tables are ready.")
        return 0

    if args.command == "import-json":
        data = json.loads(args.path.read_text(encoding="utf-8"))
        results = [SourceResult(**result) for result in data.get("results", [])]
        summary = save_fetch_results(engine, results=results, errors=data.get("errors", []))
        total = count_job_records(engine)
        print(f"Imported {summary['job_record_count']} job record(s).")
        print(f"Database now has {total} unique job record(s).")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

