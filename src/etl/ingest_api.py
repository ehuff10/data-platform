import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import requests
from dotenv import load_dotenv

from src.etl.db import get_db_conn
from src.etl.logging_utils import log_event
from src.etl.quality import run_staging_checks


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_ingestion_state(conn, source_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.ingestion_state (source_name, last_ingested_at)
            VALUES (%s, NULL)
            ON CONFLICT (source_name) DO NOTHING
            """,
            (source_name,),
        )
    conn.commit()


def get_watermark(conn, source_name: str) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_ingested_at FROM metadata.ingestion_state WHERE source_name = %s",
            (source_name,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def set_watermark(conn, source_name: str, new_watermark: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE metadata.ingestion_state
            SET last_ingested_at = %s,
                updated_at = NOW()
            WHERE source_name = %s
            """,
            (new_watermark, source_name),
        )
    conn.commit()


def create_pipeline_run(conn, run_id: str, pipeline_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata.pipeline_runs (run_id, pipeline_name, status)
            VALUES (%s, %s, %s)
            """,
            (run_id, pipeline_name, "running"),
        )
    conn.commit()


def finish_pipeline_run(
    conn,
    run_id: str,
    status: str,
    row_count: Optional[int] = None,
    error_message: Optional[str] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE metadata.pipeline_runs
            SET finished_at = NOW(),
                status = %s,
                row_count = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (status, row_count, error_message, run_id),
        )
    conn.commit()


def fetch_api_records(base_url: str, since: Optional[datetime], limit: int = 500) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"limit": limit}
    if since is not None:
        params["since"] = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    url = f"{base_url.rstrip('/')}/loan_applications"
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def write_bronze_jsonl(records: List[Dict[str, Any]], run_id: str) -> Path:
    dt = utc_now().strftime("%Y-%m-%d")
    base = Path("data/bronze/api/loan_applications") / f"dt={dt}"
    base.mkdir(parents=True, exist_ok=True)

    out_path = base / f"run_id={run_id}.jsonl"
    with out_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r))
            f.write("\n")
    return out_path


def upsert_staging(conn, records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0

    rows = []
    for r in records:
        submitted_at = datetime.fromisoformat(r["submitted_at"].replace("Z", "+00:00"))
        rows.append(
            (
                r["application_id"],
                r["applicant_id"],
                submitted_at,
                float(r["loan_amount"]),
                r["purpose"],
                r["state"],
                float(r["annual_income"]),
            )
        )

    sql = """
        INSERT INTO staging.loan_applications (
            application_id, applicant_id, submitted_at,
            loan_amount, purpose, state, annual_income
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (application_id) DO UPDATE
        SET applicant_id = EXCLUDED.applicant_id,
            submitted_at = EXCLUDED.submitted_at,
            loan_amount = EXCLUDED.loan_amount,
            purpose = EXCLUDED.purpose,
            state = EXCLUDED.state,
            annual_income = EXCLUDED.annual_income
    """

    with conn.cursor() as cur:
        cur.executemany(sql, rows)

    conn.commit()
    return len(rows)


def compute_new_watermark(records: List[Dict[str, Any]]) -> Optional[datetime]:
    if not records:
        return None
    submitted_ats = [
        datetime.fromisoformat(r["submitted_at"].replace("Z", "+00:00"))
        for r in records
    ]
    return max(submitted_ats)


def main() -> None:
    load_dotenv()

    source_name = "mock_api.loan_applications"
    pipeline_name = "ingest_api_loan_applications"
    run_id = str(uuid4())

    base_url = os.getenv("API_BASE_URL", "http://localhost:8000")

    log_event("pipeline_start", pipeline=pipeline_name, run_id=run_id, base_url=base_url)

    conn = get_db_conn()
    try:
        create_pipeline_run(conn, run_id, pipeline_name)
        ensure_ingestion_state(conn, source_name)

        watermark = get_watermark(conn, source_name)
        log_event("watermark_loaded", source=source_name, watermark=watermark)

        records = fetch_api_records(base_url, watermark, limit=500)
        log_event("api_fetched", count=len(records))

        bronze_path = write_bronze_jsonl(records, run_id)
        log_event("bronze_written", path=str(bronze_path), count=len(records))

        row_count = upsert_staging(conn, records)
        log_event("staging_upserted", row_count=row_count)

        # Quality gate (fail run if checks fail)
        quality = run_staging_checks(conn)
        if not quality.passed:
            msg = " | ".join(quality.failures)
            log_event("quality_failed", failures=quality.failures)
            raise RuntimeError(f"Data quality checks failed: {msg}")
        log_event("quality_passed")

        new_wm = compute_new_watermark(records)
        if new_wm is not None:
            set_watermark(conn, source_name, new_wm)
            log_event("watermark_updated", source=source_name, new_watermark=new_wm)

        finish_pipeline_run(conn, run_id, status="success", row_count=row_count)
        log_event("pipeline_success", pipeline=pipeline_name, run_id=run_id, row_count=row_count)

    except Exception as e:
        try:
            finish_pipeline_run(conn, run_id, status="failed", error_message=str(e))
        except Exception:
            pass
        log_event("pipeline_failed", pipeline=pipeline_name, run_id=run_id, error=str(e))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
