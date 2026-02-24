import os
import psycopg


def get_db_conn() -> psycopg.Connection:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    db = os.getenv("POSTGRES_DB", "loanpulse")
    user = os.getenv("POSTGRES_USER", "loanpulse")
    pw = os.getenv("POSTGRES_PASSWORD", "")

    return psycopg.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pw,
        autocommit=False,
    )
