from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel, Field

from job_notifier.config import load_config
from job_notifier.database import build_engine, list_job_records, save_fetch_results
from job_notifier.http_client import HttpClient
from job_notifier.service import build_output_payload, enabled_sources, fetch_sources, write_output

DASHBOARD_PATH = Path(__file__).parent / "static" / "dashboard.html"


class FetchRequest(BaseModel):
    config_path: str | None = Field(
        default=None,
        description="Optional path to a JSON source config.",
    )
    output_path: str | None = Field(
        default=None,
        description="Optional path where the raw aggregate JSON should be written.",
    )
    continue_on_error: bool = Field(
        default=True,
        description="Return successful payloads even if some sources fail.",
    )
    include_closed: bool = Field(
        default=False,
        description="Keep jobs marked inactive, closed, archived, filled, or expired.",
    )
    prioritize_latest: bool = Field(
        default=True,
        description="Sort structured job lists newest-first where date fields are present.",
    )
    save_to_db: bool = Field(
        default=False,
        description="Save structured job records and source snapshots to the database.",
    )
    database_url: str | None = Field(
        default=None,
        description="Optional database URL. Defaults to DATABASE_URL or sqlite:///data/job_notifier.db.",
    )
    stale_after_days: int = Field(
        default=14,
        description="Delete database jobs not seen again after this many days. Use -1 to disable.",
    )


def create_app() -> FastAPI:
    app = FastAPI(
        title="Job Notifier",
        version="0.1.0",
        description="Fetch raw internship and early-career job data from public sources.",
    )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/")
    def root() -> RedirectResponse:
        return RedirectResponse(url="/dashboard")

    @app.get("/dashboard")
    def dashboard() -> FileResponse:
        return FileResponse(DASHBOARD_PATH)

    @app.get("/api/jobs")
    def list_jobs(
        q: str | None = Query(default=None),
        company: str | None = Query(default=None),
        location: str | None = Query(default=None),
        category: str | None = Query(default=None),
        sponsorship: str | None = Query(default=None),
        active_only: bool = Query(default=True),
        include_raw: bool = Query(default=False),
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        try:
            return list_job_records(
                build_engine(),
                search=q,
                company=company,
                location=location,
                category=category,
                sponsorship=sponsorship,
                active_only=active_only,
                include_raw=include_raw,
                limit=limit,
                offset=offset,
            )
        except Exception as error:
            raise HTTPException(status_code=500, detail=str(error)) from error

    @app.get("/sources")
    def list_sources(config_path: str | None = Query(default=None)) -> dict[str, Any]:
        config = _load_api_config(config_path)
        sources = enabled_sources(config)
        return {"count": len(sources), "sources": sources}

    @app.post("/fetch")
    def fetch_jobs(request: FetchRequest) -> dict[str, Any]:
        config = _load_api_config(request.config_path)
        client = HttpClient()

        try:
            results, errors = fetch_sources(
                config,
                client=client,
                continue_on_error=request.continue_on_error,
                include_closed=request.include_closed,
                prioritize_latest=request.prioritize_latest,
            )
        except Exception as error:
            raise HTTPException(status_code=502, detail=str(error)) from error

        if request.output_path:
            write_output(Path(request.output_path), results, errors)

        db_summary = None
        if request.save_to_db:
            db_summary = save_fetch_results(
                build_engine(request.database_url),
                results=results,
                errors=errors,
                stale_after_days=request.stale_after_days,
            )

        payload = build_output_payload(results, errors)
        payload["summary"] = {
            "source_count": len(enabled_sources(config)),
            "result_count": len(results),
            "error_count": len(errors),
            "output_path": request.output_path,
            "include_closed": request.include_closed,
            "prioritize_latest": request.prioritize_latest,
            "database": db_summary,
        }
        return payload

    return app


def _load_api_config(config_path: str | None) -> dict[str, Any]:
    try:
        return load_config(Path(config_path) if config_path else None)
    except Exception as error:
        raise HTTPException(status_code=400, detail=f"Invalid config: {error}") from error


app = create_app()
