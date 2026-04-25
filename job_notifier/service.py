from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from job_notifier.http_client import FetchError, HttpClient
from job_notifier.job_filters import prioritize_jobs
from job_notifier.models import SourceResult
from job_notifier.sources import SOURCE_TYPES


def enabled_sources(config: dict[str, Any]) -> list[dict[str, Any]]:
    return [source for source in config.get("sources", []) if source.get("enabled", True) is not False]


def fetch_sources(
    config: dict[str, Any],
    *,
    client: HttpClient,
    continue_on_error: bool,
    include_closed: bool = False,
    prioritize_latest: bool = True,
) -> tuple[list[SourceResult], list[dict[str, str]]]:
    results: list[SourceResult] = []
    errors: list[dict[str, str]] = []

    for source_config in enabled_sources(config):
        source_type = source_config.get("type")
        source_class = SOURCE_TYPES.get(source_type)
        if source_class is None:
            message = f"Unsupported source type: {source_type}"
            if not continue_on_error:
                raise ValueError(message)
            errors.append({"source_name": str(source_config.get("name", "unknown")), "error": message})
            continue

        source = source_class(source_config)
        try:
            results.append(source.fetch(client))
        except (FetchError, json.JSONDecodeError, KeyError, ValueError) as error:
            if not continue_on_error:
                raise
            errors.append({"source_name": source.name, "error": str(error)})

    if include_closed is False or prioritize_latest is True:
        results = prioritize_jobs(
            results,
            include_closed=include_closed,
            prioritize_latest=prioritize_latest,
        )

    return results, errors


def build_output_payload(
    results: list[SourceResult],
    errors: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "results": [asdict(result) for result in results],
        "errors": errors,
    }


def write_output(path: Path, results: list[SourceResult], errors: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output_file:
        json.dump(build_output_payload(results, errors), output_file, indent=2, ensure_ascii=False)
        output_file.write("\n")
