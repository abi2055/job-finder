# Job Notifier

Fetch raw internship and early-career job data from public GitHub repos and ATS job board APIs.

This first pass intentionally avoids normalization. Each source result keeps its raw payload and source metadata so downstream parsing can evolve separately.

## CLI Usage

```bash
python -m job_notifier.fetch_jobs --output data/raw_jobs.json
```

By default, structured job lists are filtered to remove closed/inactive jobs and sorted newest-first by available update or post dates.

Use a custom source list:

```bash
python -m job_notifier.fetch_jobs --config sources.example.json --output data/raw_jobs.json
```

Optional GitHub token for higher API limits:

```bash
GITHUB_TOKEN=ghp_... python -m job_notifier.fetch_jobs --output data/raw_jobs.json
```

## API Usage

Install the project dependencies, then run the FastAPI app:

```bash
pip install -e .
uvicorn job_notifier.api:app --reload
```

Useful endpoints:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/sources
curl -X POST http://127.0.0.1:8000/fetch \
  -H 'Content-Type: application/json' \
  -d '{"output_path":"data/raw_jobs.json","continue_on_error":true}'
```

Use `include_closed: true` only when you intentionally want inactive/closed records in the raw output.

## Docker Compose

Compose runs the API, Postgres, and pgAdmin:

```bash
docker compose up --build
```

Access:

- FastAPI docs: `http://127.0.0.1:8000/docs`
- API health: `http://127.0.0.1:8000/health`
- pgAdmin: `http://127.0.0.1:5050`

pgAdmin login:

```text
email: admin@example.com
password: admin
```

pgAdmin includes a preset server named `job-notifier-postgres`. When prompted for the database password, use:

```text
job_notifier
```

If you need to register it manually, use:

```text
host: postgres
port: 5432
database: job_notifier
username: job_notifier
password: job_notifier
```

Fetch and save jobs into Postgres through the API:

```bash
curl -X POST http://127.0.0.1:8000/fetch \
  -H 'Content-Type: application/json' \
  -d '{"output_path":"data/raw_jobs.json","continue_on_error":true,"save_to_db":true}'
```

## Database

The app can persist fetches into a Postgres-style schema. By default it uses a local SQLite file for development:

```bash
python -m job_notifier.db_cli init
python -m job_notifier.fetch_jobs --save-to-db --output data/raw_jobs.json
```

For Postgres, set `DATABASE_URL`:

```bash
export DATABASE_URL='postgresql+psycopg://user:password@localhost:5432/job_notifier'
python -m job_notifier.db_cli init
python -m job_notifier.fetch_jobs --save-to-db --output data/raw_jobs.json
```

Main tables:

- `fetch_runs`: one row per fetch.
- `source_snapshots`: raw source payloads, including Markdown and source-level JSON.
- `raw_job_records`: one row per structured job, with queryable columns like `company_name`, `title`, `job_url`, `locations`, `date_posted_at`, `date_updated_at`, plus the complete original `raw_payload`.

## Source Types

- `github_raw`: fetches raw JSON, Markdown, or text files from public GitHub repositories.
- `greenhouse`: fetches a public Greenhouse board through `boards-api.greenhouse.io`.
- `lever`: fetches a public Lever postings board through `api.lever.co`.
