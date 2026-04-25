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

## Source Types

- `github_raw`: fetches raw JSON, Markdown, or text files from public GitHub repositories.
- `greenhouse`: fetches a public Greenhouse board through `boards-api.greenhouse.io`.
- `lever`: fetches a public Lever postings board through `api.lever.co`.
