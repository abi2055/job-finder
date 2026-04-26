from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
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
    create_engine,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlalchemy.types import JSON

from job_notifier.models import SourceResult

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

    return {
        "fetch_run_id": int(fetch_run_id),
        "source_snapshot_count": len(snapshot_rows),
        "job_record_count": len(job_rows),
    }


def count_job_records(engine: Engine) -> int:
    with engine.connect() as connection:
        return int(connection.execute(select(func.count()).select_from(raw_job_records)).scalar_one())


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
        for index, payload in enumerate(_job_payloads(result.payload)):
            if not isinstance(payload, dict):
                continue
            rows.append(_build_job_row(result, payload, index, seen_at))
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


def _build_job_row(
    result: SourceResult,
    payload: dict[str, Any],
    index: int,
    seen_at: datetime,
) -> dict[str, Any]:
    fetched_at = _parse_datetime(result.fetched_at) or seen_at
    external_id = _string_or_none(payload.get("id") or payload.get("job_id") or payload.get("postingId"))
    job_url = _string_or_none(payload.get("url") or payload.get("absolute_url") or payload.get("hostedUrl"))

    return {
        "record_key": _record_key(result.source_name, external_id, job_url, payload),
        "source_name": result.source_name,
        "source_type": result.source_type,
        "source_url": result.url,
        "source_record_index": index,
        "upstream_source": _string_or_none(payload.get("source")),
        "external_id": external_id,
        "company_name": _string_or_none(
            payload.get("company_name") or payload.get("company") or payload.get("companyName")
        ),
        "company_url": _string_or_none(payload.get("company_url")),
        "title": _string_or_none(payload.get("title") or payload.get("text")),
        "job_url": job_url,
        "category": _string_or_none(payload.get("category") or payload.get("department")),
        "locations": _list_value(payload.get("locations") or payload.get("location")),
        "terms": _list_value(payload.get("terms")),
        "degrees": _list_value(payload.get("degrees")),
        "sponsorship": _string_or_none(payload.get("sponsorship")),
        "active": _bool_or_none(payload.get("active")),
        "is_visible": _bool_or_none(payload.get("is_visible")),
        "date_posted_at": _parse_datetime(payload.get("date_posted") or payload.get("createdAt")),
        "date_updated_at": _parse_datetime(payload.get("date_updated") or payload.get("updatedAt")),
        "fetched_at": fetched_at,
        "last_seen_at": seen_at,
        "raw_payload": payload,
    }


def _record_key(
    source_name: str,
    external_id: str | None,
    job_url: str | None,
    payload: dict[str, Any],
) -> str:
    stable_parts = [
        source_name,
        external_id or "",
        job_url or "",
        _string_or_none(payload.get("company_name")) or "",
        _string_or_none(payload.get("title")) or "",
    ]
    digest_input = "|".join(stable_parts)
    if not external_id and not job_url:
        digest_input = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(digest_input.encode("utf-8")).hexdigest()


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


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)
