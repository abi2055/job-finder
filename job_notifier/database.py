from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    cast,
    create_engine,
    desc,
    func,
    or_,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlalchemy.types import JSON

from job_notifier.models import SourceResult
from job_notifier.normalizer import NormalizedJob, iter_normalized_jobs

DATABASE_URL_ENV = "DATABASE_URL"
DEFAULT_DATABASE_URL = "sqlite:///data/job_notifier.db"

metadata = MetaData()
json_type = JSON().with_variant(JSONB, "postgresql")

fetch_runs = Table(
    "fetch_runs",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=False),
    Column("source_count", Integer, nullable=False),
    Column("job_count", Integer, nullable=False),
    Column("error_count", Integer, nullable=False),
    Column("errors", json_type, nullable=False, default=list),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

source_snapshots = Table(
    "source_snapshots",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("fetch_run_id", ForeignKey("fetch_runs.id", ondelete="CASCADE"), nullable=False),
    Column("source_name", String(160), nullable=False),
    Column("source_type", String(80), nullable=False),
    Column("source_url", Text, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("status", Integer, nullable=False),
    Column("content_type", Text),
    Column("payload_kind", String(40), nullable=False),
    Column("record_count", Integer, nullable=False),
    Column("raw_payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

raw_job_records = Table(
    "raw_job_records",
    metadata,
    Column("record_key", String(64), primary_key=True),
    Column("fetch_run_id", ForeignKey("fetch_runs.id", ondelete="SET NULL")),
    Column("source_name", String(160), nullable=False),
    Column("source_type", String(80), nullable=False),
    Column("source_url", Text, nullable=False),
    Column("source_record_index", Integer, nullable=False),
    Column("upstream_source", String(160)),
    Column("external_id", Text),
    Column("company_name", Text),
    Column("company_url", Text),
    Column("title", Text),
    Column("job_url", Text),
    Column("category", Text),
    Column("locations", json_type, nullable=False, default=list),
    Column("terms", json_type, nullable=False, default=list),
    Column("degrees", json_type, nullable=False, default=list),
    Column("sponsorship", Text),
    Column("active", Boolean),
    Column("is_visible", Boolean),
    Column("date_posted_at", DateTime(timezone=True)),
    Column("date_updated_at", DateTime(timezone=True)),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("raw_payload", json_type, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

Index("idx_raw_job_records_company_name", raw_job_records.c.company_name)
Index("idx_raw_job_records_title", raw_job_records.c.title)
Index("idx_raw_job_records_date_updated_at", raw_job_records.c.date_updated_at)
Index("idx_raw_job_records_source_name", raw_job_records.c.source_name)
Index("idx_raw_job_records_job_url", raw_job_records.c.job_url)


def database_url_from_env() -> str:
    return os.getenv(DATABASE_URL_ENV, DEFAULT_DATABASE_URL)


def build_engine(database_url: str | None = None) -> Engine:
    return create_engine(database_url or database_url_from_env(), future=True)


def create_database(engine: Engine) -> None:
    metadata.create_all(engine)


def save_fetch_results(
    engine: Engine,
    *,
    results: list[SourceResult],
    errors: list[dict[str, str]],
    stale_after_days: int = 14,
) -> dict[str, int]:
    create_database(engine)
    started_at = _utcnow()
    completed_at = _utcnow()
    job_rows = list(_iter_job_rows(results, completed_at))
    snapshot_rows = list(_iter_snapshot_rows(results))

    with engine.begin() as connection:
        fetch_run_id = connection.execute(
            fetch_runs.insert().values(
                started_at=started_at,
                completed_at=completed_at,
                source_count=len(results),
                job_count=len(job_rows),
                error_count=len(errors),
                errors=errors,
            )
        ).inserted_primary_key[0]

        if snapshot_rows:
            for row in snapshot_rows:
                row["fetch_run_id"] = fetch_run_id
            connection.execute(source_snapshots.insert(), snapshot_rows)

        if job_rows:
            for row in job_rows:
                row["fetch_run_id"] = fetch_run_id
                row["last_seen_at"] = completed_at
            _upsert_job_rows(connection, engine, job_rows)

        deleted_duplicate_count = _delete_duplicate_jobs(connection)
        deleted_stale_count = _delete_stale_jobs(
            connection,
            completed_at=completed_at,
            stale_after_days=stale_after_days,
        )
        unique_job_record_count = int(
            connection.execute(select(func.count()).select_from(raw_job_records)).scalar_one()
        )

    return {
        "fetch_run_id": int(fetch_run_id),
        "source_snapshot_count": len(snapshot_rows),
        "job_record_count": len(job_rows),
        "unique_job_record_count": unique_job_record_count,
        "deleted_duplicate_count": deleted_duplicate_count,
        "deleted_stale_count": deleted_stale_count,
    }


def count_job_records(engine: Engine) -> int:
    with engine.connect() as connection:
        return int(connection.execute(select(func.count()).select_from(raw_job_records)).scalar_one())


def list_job_records(
    engine: Engine,
    *,
    search: str | None = None,
    company: str | None = None,
    location: str | None = None,
    category: str | None = None,
    sponsorship: str | None = None,
    active_only: bool = True,
    include_raw: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    filters = _job_filters(
        search=search,
        company=company,
        location=location,
        category=category,
        sponsorship=sponsorship,
        active_only=active_only,
    )
    base_query = raw_job_records.select()
    count_query = select(func.count()).select_from(raw_job_records)

    if filters:
        base_query = base_query.where(*filters)
        count_query = count_query.where(*filters)

    base_query = (
        base_query.order_by(
            desc(raw_job_records.c.date_updated_at).nulls_last(),
            desc(raw_job_records.c.date_posted_at).nulls_last(),
            raw_job_records.c.company_name,
        )
        .limit(limit)
        .offset(offset)
    )

    with engine.connect() as connection:
        total = int(connection.execute(count_query).scalar_one())
        rows = [dict(row) for row in connection.execute(base_query).mappings()]

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "jobs": [_serialize_job_row(row, include_raw=include_raw) for row in rows],
    }


def _job_filters(
    *,
    search: str | None,
    company: str | None,
    location: str | None,
    category: str | None,
    sponsorship: str | None,
    active_only: bool,
) -> list[Any]:
    filters: list[Any] = []
    if active_only:
        filters.append(raw_job_records.c.active.is_not(False))
        filters.append(raw_job_records.c.is_visible.is_not(False))
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                raw_job_records.c.company_name.ilike(pattern),
                raw_job_records.c.title.ilike(pattern),
                raw_job_records.c.category.ilike(pattern),
                raw_job_records.c.sponsorship.ilike(pattern),
                raw_job_records.c.job_url.ilike(pattern),
            )
        )
    if company:
        filters.append(raw_job_records.c.company_name.ilike(f"%{company.strip()}%"))
    if location:
        filters.append(cast(raw_job_records.c.locations, Text).ilike(f"%{location.strip()}%"))
    if category:
        filters.append(raw_job_records.c.category.ilike(f"%{category.strip()}%"))
    if sponsorship:
        filters.append(raw_job_records.c.sponsorship.ilike(f"%{sponsorship.strip()}%"))
    return filters


def _serialize_job_row(row: dict[str, Any], *, include_raw: bool) -> dict[str, Any]:
    serialized = {
        key: _serialize_value(value)
        for key, value in row.items()
        if key not in {"raw_payload"}
    }
    if include_raw:
        serialized["raw_payload"] = row.get("raw_payload")
    return serialized


def _serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _upsert_job_rows(connection: Any, engine: Engine, rows: list[dict[str, Any]]) -> None:
    for chunk in _chunks(rows, size=200):
        if engine.dialect.name == "postgresql":
            statement = postgres_insert(raw_job_records).values(chunk)
            update_columns = _upsert_update_columns(statement)
            connection.execute(
                statement.on_conflict_do_update(
                    index_elements=[raw_job_records.c.record_key],
                    set_=update_columns,
                )
            )
            continue

        if engine.dialect.name == "sqlite":
            statement = sqlite_insert(raw_job_records).values(chunk)
            update_columns = _upsert_update_columns(statement)
            connection.execute(
                statement.on_conflict_do_update(
                    index_elements=[raw_job_records.c.record_key],
                    set_=update_columns,
                )
            )
            continue

        connection.execute(raw_job_records.insert(), chunk)


def _upsert_update_columns(statement: Any) -> dict[str, Any]:
    return {
        column.name: getattr(statement.excluded, column.name)
        for column in raw_job_records.columns
        if column.name not in {"record_key", "created_at"}
    }


def _chunks(rows: list[dict[str, Any]], *, size: int) -> list[list[dict[str, Any]]]:
    return [rows[index : index + size] for index in range(0, len(rows), size)]


def _iter_snapshot_rows(results: list[SourceResult]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in results:
        record_count = _record_count(result.payload)
        rows.append(
            {
                "source_name": result.source_name,
                "source_type": result.source_type,
                "source_url": result.url,
                "fetched_at": _parse_datetime(result.fetched_at) or _utcnow(),
                "status": result.status,
                "content_type": result.content_type,
                "payload_kind": type(result.payload).__name__,
                "record_count": record_count,
                "raw_payload": result.payload,
            }
        )
    return rows


def _iter_job_rows(results: list[SourceResult], seen_at: datetime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in results:
        rows.extend(_job_row(normalized_job) for normalized_job in iter_normalized_jobs(result, seen_at=seen_at))
    return rows


def _job_payloads(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
        return payload["jobs"]
    return []


def _record_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
        return len(payload["jobs"])
    return 1


def _job_row(job: NormalizedJob) -> dict[str, Any]:
    return {
        "record_key": job.record_key,
        "source_name": job.source_name,
        "source_type": job.source_type,
        "source_url": job.source_url,
        "source_record_index": job.source_record_index,
        "upstream_source": job.upstream_source,
        "external_id": job.external_id,
        "company_name": job.company_name,
        "company_url": job.company_url,
        "title": job.title,
        "job_url": job.job_url,
        "category": job.category,
        "locations": job.locations,
        "terms": job.terms,
        "degrees": job.degrees,
        "sponsorship": job.sponsorship,
        "active": job.active,
        "is_visible": job.is_visible,
        "date_posted_at": job.date_posted_at,
        "date_updated_at": job.date_updated_at,
        "fetched_at": job.fetched_at,
        "raw_payload": job.raw_payload,
    }


def _delete_stale_jobs(connection: Any, *, completed_at: datetime, stale_after_days: int) -> int:
    if stale_after_days < 0:
        return 0
    cutoff = completed_at - timedelta(days=stale_after_days)
    result = connection.execute(raw_job_records.delete().where(raw_job_records.c.last_seen_at < cutoff))
    return int(result.rowcount or 0)


def _delete_duplicate_jobs(connection: Any) -> int:
    rows = connection.execute(
        select(
            raw_job_records.c.record_key,
            raw_job_records.c.job_url,
            raw_job_records.c.last_seen_at,
            raw_job_records.c.date_updated_at,
        ).where(raw_job_records.c.job_url.is_not(None))
    ).mappings()

    by_url: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        normalized_url = str(row["job_url"]).strip().casefold().rstrip("/")
        if not normalized_url:
            continue
        by_url.setdefault(normalized_url, []).append(dict(row))

    duplicate_keys: list[str] = []
    for grouped_rows in by_url.values():
        if len(grouped_rows) < 2:
            continue
        grouped_rows.sort(
            key=lambda row: (
                row.get("last_seen_at") or datetime.min.replace(tzinfo=timezone.utc),
                row.get("date_updated_at") or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=True,
        )
        duplicate_keys.extend(row["record_key"] for row in grouped_rows[1:])

    if not duplicate_keys:
        return 0

    deleted_count = 0
    for chunk in _chunks([{"record_key": key} for key in duplicate_keys], size=500):
        keys = [row["record_key"] for row in chunk]
        result = connection.execute(raw_job_records.delete().where(raw_job_records.c.record_key.in_(keys)))
        deleted_count += int(result.rowcount or 0)
    return deleted_count


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


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)
