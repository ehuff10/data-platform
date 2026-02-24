from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

app = FastAPI(title="LoanPulse Mock API", version="0.1.0")


class LoanApplication(BaseModel):
    application_id: str = Field(..., description="Deterministic app id (uuid for now)")
    applicant_id: str
    submitted_at: datetime
    loan_amount: float
    purpose: str
    state: str
    annual_income: float


STATES = ["NY", "NJ", "PA", "CT", "MA", "FL", "GA", "TX", "CA", "IL"]
PURPOSES = ["debt_consolidation", "home_improvement", "auto", "medical", "small_business"]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fake_record(submitted_at: datetime) -> LoanApplication:
    # simple deterministic-ish values
    idx = int(submitted_at.timestamp()) % len(STATES)
    purpose_idx = int(submitted_at.timestamp()) % len(PURPOSES)

    return LoanApplication(
        application_id=str(uuid4()),
        applicant_id=str(uuid4()),
        submitted_at=submitted_at,
        loan_amount=5000 + (int(submitted_at.timestamp()) % 20000),
        purpose=PURPOSES[purpose_idx],
        state=STATES[idx],
        annual_income=40000 + (int(submitted_at.timestamp()) % 90000),
    )


@app.get("/health")
def health():
    return {"status": "ok", "ts": _now_utc().isoformat()}


@app.get("/loan_applications", response_model=List[LoanApplication])
def loan_applications(
    since: Optional[str] = Query(
        default=None,
        description="ISO-8601 timestamp. Return records with submitted_at > since.",
        examples=["2026-02-24T00:00:00Z"],
    ),
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    Simulates a stream of loan applications.
    - If `since` is provided, returns apps after that timestamp.
    - Generates synthetic records spaced 1 minute apart.
    """
    now = _now_utc()

    if since is None:
        # default: last 60 minutes
        start = now.replace(second=0, microsecond=0)
        start = start.replace(minute=max(0, start.minute - 60))
    else:
        # accept 'Z' suffix
        since_clean = since.replace("Z", "+00:00")
        start = datetime.fromisoformat(since_clean)
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

    records: List[LoanApplication] = []
    # generate 1 record per minute after `start`
    cursor = start

    while len(records) < limit:
        cursor = cursor.replace(second=0, microsecond=0)
        cursor = cursor.replace(minute=cursor.minute + 1) if cursor.minute < 59 else cursor.replace(hour=cursor.hour + 1, minute=0)
        if cursor >= now:
            break
        records.append(_fake_record(cursor))

    return records
