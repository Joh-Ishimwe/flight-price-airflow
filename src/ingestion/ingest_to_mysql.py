"""Load the flight price CSV into the MySQL staging table.

Append-only: each run is its own batch, never a wholesale replace.
"""

import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from src.utils.settings import get_mysql_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = PROJECT_ROOT / "data" / "raw" / "Flight_Price_Dataset_of_Bangladesh.csv"
TABLE = "stg_flight_prices"
RUNS_TABLE = "ingestion_runs"
CHUNK_SIZE = 10_000
# Rows per INSERT statement, to stay under MySQL's max_allowed_packet.
SQL_INSERT_CHUNK = 1_000

# CSV header -> staging column name.
COLUMN_MAP = {
    "Airline": "airline",
    "Source": "source_code",
    "Source Name": "source_name",
    "Destination": "destination_code",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hrs",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "travel_class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare_bdt",
    "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
    "Total Fare (BDT)": "total_fare_bdt",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}


def get_engine():
    """Build a SQLAlchemy engine from central settings."""
    config = get_mysql_config()
    print(f"Connecting to {config}")  # safe: __repr__ masks the password
    return create_engine(
        config.url,
        connect_args={"connect_timeout": config.connect_timeout},
    )


def get_latest_batch_id(engine) -> str:
    """Most recent successfully-ingested batch."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"SELECT batch_id FROM {RUNS_TABLE} "
                "WHERE status = 'SUCCESS' ORDER BY started_at DESC LIMIT 1"
            )
        ).fetchone()
    if row is None:
        raise ValueError("No successful ingestion batch found.")
    return row[0]


def _now() -> datetime:
    """UTC timestamp, naive - MySQL's DATETIME columns don't store tzinfo."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _start_run(engine, batch_id: str, source_file: str) -> None:
    """Log the run before loading data - staging's FK needs the batch to exist first."""
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {RUNS_TABLE} (batch_id, source_file, status, started_at)
                VALUES (:batch_id, :source_file, 'RUNNING', :started_at)
                """
            ),
            {"batch_id": batch_id, "source_file": source_file, "started_at": _now()},
        )


def _finish_run(engine, batch_id: str, status: str, rows: int, error_message: str = None) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                UPDATE {RUNS_TABLE}
                SET status = :status,
                    rows_in_source = :rows,
                    rows_loaded = :rows,
                    error_message = :error_message,
                    finished_at = :finished_at
                WHERE batch_id = :batch_id
                """
            ),
            {
                "status": status,
                "rows": rows,
                "error_message": error_message,
                "finished_at": _now(),
                "batch_id": batch_id,
            },
        )


def ingest() -> dict:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    engine = get_engine()
    batch_id = str(uuid.uuid4())
    source_file = CSV_PATH.name
    total = 0

    _start_run(engine, batch_id, source_file)
    print(f"batch {batch_id}: started")

    try:
        reader = pd.read_csv(
            CSV_PATH,
            chunksize=CHUNK_SIZE,
            parse_dates=["Departure Date & Time", "Arrival Date & Time"],
        )

        for i, chunk in enumerate(reader, start=1):
            chunk = chunk.rename(columns=COLUMN_MAP)
            chunk = chunk[list(COLUMN_MAP.values())]  # drop anything unexpected
            chunk["source_file"] = source_file
            chunk["batch_id"] = batch_id

            chunk.to_sql(
                TABLE,
                con=engine,
                if_exists="append",   # always append - staging never gets wiped
                index=False,
                method="multi",       # batch rows per INSERT instead of one at a time
                chunksize=SQL_INSERT_CHUNK,
            )

            total += len(chunk)
            print(f"chunk {i}: {len(chunk):,} rows  (running total {total:,})")

    except Exception as exc:
        _finish_run(engine, batch_id, status="FAILED", rows=total, error_message=str(exc))
        raise

    _finish_run(engine, batch_id, status="SUCCESS", rows=total)
    return {"batch_id": batch_id, "rows_loaded": total}


if __name__ == "__main__":
    try:
        result = ingest()
        print(f"\nLoaded {result['rows_loaded']:,} rows into {TABLE} (batch {result['batch_id']})")
    except Exception as exc:
        print(f"INGESTION FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)  # non-zero exit tells Airflow the task failed
