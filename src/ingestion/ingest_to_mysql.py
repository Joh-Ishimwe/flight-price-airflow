"""Load the flight price CSV into the MySQL staging table."""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from src.utils.settings import get_mysql_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = PROJECT_ROOT / "data" / "raw" / "Flight_Price_Dataset_of_Bangladesh.csv"
TABLE = "stg_flight_prices"
CHUNK_SIZE = 10_000

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


def ingest() -> int:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    engine = get_engine()
    total = 0

    # Empty the table first so re-running produces the same result
    # instead of duplicating every row.
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {TABLE}"))

    reader = pd.read_csv(
        CSV_PATH,
        chunksize=CHUNK_SIZE,
        parse_dates=["Departure Date & Time", "Arrival Date & Time"],
    )

    for i, chunk in enumerate(reader, start=1):
        chunk = chunk.rename(columns=COLUMN_MAP)
        chunk = chunk[list(COLUMN_MAP.values())]  # drop anything unexpected

        chunk.to_sql(
            TABLE,
            con=engine,
            if_exists="append",   # never "replace" - that would drop your schema
            index=False,
            method="multi",       # batch rows per INSERT instead of one at a time
        )

        total += len(chunk)
        print(f"chunk {i}: {len(chunk):,} rows  (running total {total:,})")

    return total


if __name__ == "__main__":
    try:
        rows = ingest()
        print(f"\nLoaded {rows:,} rows into {TABLE}")
    except Exception as exc:
        print(f"INGESTION FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)  # non-zero exit tells Airflow the task failed