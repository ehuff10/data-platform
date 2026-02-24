# LoanPulse Data Platform

This project is a small but realistic data platform built from scratch using Docker, Postgres, FastAPI, and Airflow.

## What it does
This platform:
- Pulls loan application data from an API  
- Ingests it incrementally (no duplicates)  
- Stores raw data in a bronze layer  
- Loads structured records into Postgres staging tables  
- Tracks pipeline runs and ingestion state  
- Applies data quality checks  
- Is fully containerized and reproducible  
- Can be orchestrated with Airflow  

Everything runs with:

```
docker compose up --build
```
No local dependency setup required.
## Why I Built It
Most data projects stop at a notebook.
I wanted to understand:
- How incremental ingestion actually works  
- How production pipelines avoid duplicate data  
- How metadata tables are used in real systems  
- How orchestration tools like Airflow fit into the picture  
- How to make everything reproducible with containers  
This project is my attempt at building those patterns intentionally.
## Architecture Overview
The system includes:
- FastAPI → mock operational data source  
- ETL service → ingestion logic + quality checks  
- Postgres → warehouse + metadata store  
- Airflow → orchestration layer  
- Docker Compose → infrastructure management  

High-level data flow:

API → ETL → Bronze (raw files) → Postgres Staging
                              ↑
                       Airflow Scheduler

## What Makes This Production-Style
A few design decisions I intentionally implemented:
### Incremental Loading
The pipeline uses a watermark stored in `metadata.ingestion_state`.  
Each run only processes new records.
### Idempotent Upserts
If the pipeline runs twice, it does not duplicate rows.
### Metadata Tracking
Every run is recorded in `metadata.pipeline_runs` with:
- run_id  
- status  
- row_count  
- timestamps  
- error message (if any)  
### Quality Gating
If validation checks fail, the run fails instead of silently inserting bad data.
### Reproducibility
Everything is containerized.  
If someone clones this repo, they can stand up the entire platform with Docker.
## How to Run

Clone the repository:

```
git clone https://github.com/ehuff10/data-platform.git
cd data-platform
```

Start the platform:

```
docker compose up --build
```

Available services (local development only):

- API → http://localhost:8000  
- Airflow UI → http://localhost:8080  
- Postgres → localhost:5432  

To manually trigger ingestion:

```
./scripts/run_ingest_api.sh
```
## Project Structure

```
services/
  api/
  etl/
src/
  etl/
postgres/
  init/
airflow/
docker-compose.yml
```


