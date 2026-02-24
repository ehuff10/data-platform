FROM python:3.11-slim

WORKDIR /app

COPY requirements-etl.txt /app/requirements-etl.txt
RUN pip install --no-cache-dir -r /app/requirements-etl.txt

COPY src /app/src
COPY .env.example /app/.env.example

# Default command (Airflow will override with the same)
CMD ["python", "-m", "src.etl.ingest_api"]
