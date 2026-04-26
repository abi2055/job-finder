FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY job_notifier ./job_notifier
COPY sources.example.json ./

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e .

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "job_notifier.api:app", "--host", "0.0.0.0", "--port", "8000"]

