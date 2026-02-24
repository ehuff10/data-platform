from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Tuple


@dataclass
class QualityResult:
    passed: bool
    failures: List[str]


def run_staging_checks(conn) -> QualityResult:
    """
    Basic but real checks:
    - no nulls in required columns
    - loan_amount > 0
    - annual_income > 0
    - submitted_at not too far in the future (clock skew guard)
    """
    failures: List[str] = []

    checks: List[Tuple[str, str]] = [
        (
            "null_required_fields",
            """
            SELECT COUNT(*) FROM staging.loan_applications
            WHERE application_id IS NULL
               OR applicant_id IS NULL
               OR submitted_at IS NULL
               OR loan_amount IS NULL
               OR purpose IS NULL
               OR state IS NULL
               OR annual_income IS NULL
            """,
        ),
        (
            "loan_amount_positive",
            "SELECT COUNT(*) FROM staging.loan_applications WHERE loan_amount <= 0",
        ),
        (
            "annual_income_positive",
            "SELECT COUNT(*) FROM staging.loan_applications WHERE annual_income <= 0",
        ),
        (
            "submitted_at_future_guard",
            """
            SELECT COUNT(*) FROM staging.loan_applications
            WHERE submitted_at > (NOW() AT TIME ZONE 'UTC') + INTERVAL '5 minutes'
            """,
        ),
    ]

    with conn.cursor() as cur:
        for name, sql in checks:
            cur.execute(sql)
            count = cur.fetchone()[0]
            if count and int(count) > 0:
                failures.append(f"{name}: {count} failing rows")

    return QualityResult(passed=(len(failures) == 0), failures=failures)
