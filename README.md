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

- Dashboard: `http://127.0.0.1:8000/dashboard`
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
- `greenhouse`: fetches published public jobs from Greenhouse Job Board API via `boards-api.greenhouse.io/v1/boards/{board_token}/jobs?content=true`.
- `lever`: fetches published public jobs from Lever Postings API via `api.lever.co/v0/postings/{company}?mode=json`.

The database writer normalizes GitHub, Greenhouse, and Lever jobs into the same query columns while preserving the original record in `raw_payload`. Dedupe is based on a canonical job key, preferring canonical job URLs when available, so repeated fetches and overlapping GitHub/ATS postings do not flood the database or email digest. Jobs marked closed/inactive are filtered before storage, and database rows not seen again are removed after `--stale-after-days` days, defaulting to `14`.

Company ATS boards are broad, so Greenhouse and Lever jobs pass an additional relevance gate before storage/email: they must look like tech internships, co-ops, new-grad, graduate, or early-career roles. Senior/staff/manager/director and non-tech sales, finance, legal, HR, support, and operations roles are excluded.

## Scheduled Email Notifications

The GitHub Actions workflow in `.github/workflows/fetch-and-email.yml` runs every 3 hours and can also be triggered manually from the Actions tab.

Add these repository secrets in GitHub:

```text
RESEND_API_KEY
RESEND_TO_EMAIL
RESEND_FROM_EMAIL
```

`RESEND_FROM_EMAIL` must be a sender allowed by your Resend account. For early testing, Resend examples use:

```text
Job Notifier <onboarding@resend.dev>
```

Run a local dry run:

```bash
python -m job_notifier.notify_jobs --dry-run --top-jobs 5
```

Send locally using `.env`:

```bash
python -m job_notifier.notify_jobs --top-jobs 25 --attach-raw
```

### Email Preferences

The scheduled workflow reads `notification_preferences.json` each time it runs. The cron stays fixed at every 3 hours, while the email content changes based on the active profile in that file.

Example active profile:

```json
{
  "active_profile": "canada_or_sponsorship",
  "profiles": {
    "canada_or_sponsorship": {
      "description": "Email jobs that are in Canada or mention sponsorship.",
      "max_age_hours": null,
      "include_any": [
        {
          "locations": ["Canada", "Toronto", "Vancouver", "Montreal", "Ottawa", "Calgary", "Waterloo"]
        },
        {
          "sponsorship": ["Sponsorship", "Offers Sponsorship", "Visa"]
        }
      ],
      "include_all": [],
      "exclude_text": []
    }
  }
}
```

Use `max_age_hours: 24` to only email jobs updated or posted in the last 24 hours. Use `include_any` for OR-style rules, and `include_all` for AND-style rules.

Preview your current profile:

```bash
python -m job_notifier.notify_jobs --dry-run --preferences notification_preferences.json --top-jobs 10
```

Preview a different profile without changing `active_profile`:

```bash
python -m job_notifier.notify_jobs --dry-run --preferences notification_preferences.example.json --profile last_24_hours --top-jobs 10
```
