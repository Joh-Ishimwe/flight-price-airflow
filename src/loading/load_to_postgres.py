"""Load transformed data + KPIs into Postgres, for one batch.

Two things this is built around:
- Consistency: the audit row (load_runs) is logged in its own transaction
  (so a failure is still visible), but the actual data tables (fact +
  3 KPI tables) all write in ONE transaction - all land, or none do.
  Re-running for the same batch is safe: old rows for that batch are
  deleted first, not duplicated.
- Latency: the fact table uses Postgres COPY instead of row-by-row
  INSERT - COPY is the standard fast-bulk-load path for Postgres.
"""

import csv
import io
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

from src.ingestion.ingest_to_mysql import get_engine as get_mysql_engine, get_latest_batch_id
from src.transform.compute_kpis import transform
from src.utils.settings import get_postgres_config

FACT_TABLE = "fact_flight_prices"
RUNS_TABLE = "load_runs"
KPI_TABLES = {
    "avg_fare_by_airline": "kpi_avg_fare_by_airline",
    "seasonal_fare": "kpi_seasonal_fare",
    "popular_routes": "kpi_popular_routes",
}
FACT_COLUMNS = [
    "id", "airline", "source_code", "source_name",
    "destination_code", "destination_name",
    "departure_datetime", "arrival_datetime", "duration_hrs",
    "stopovers", "aircraft_type", "travel_class", "booking_source",
    "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt",
    "seasonality", "days_before_departure", "batch_id",
]


def get_pg_engine():
    """Build a SQLAlchemy engine for postgres-analytics from central settings."""
    config = get_postgres_config()
    print(f"Connecting to {config}")  # safe: __repr__ masks the password
    return create_engine(config.url, connect_args={"connect_timeout": config.connect_timeout})


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _start_run(engine, batch_id: str) -> None:
    """Insert or, on a re-run of the same batch, reset the run to RUNNING.
    Unlike MySQL ingestion, a Postgres load can legitimately retry the same
    batch_id, so this can't be a plain INSERT."""
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {RUNS_TABLE} (batch_id, status, started_at)
                VALUES (:batch_id, 'RUNNING', :started_at)
                ON CONFLICT (batch_id) DO UPDATE
                SET status = 'RUNNING', started_at = EXCLUDED.started_at,
                    rows_loaded = NULL, error_message = NULL, finished_at = NULL
                """
            ),
            {"batch_id": batch_id, "started_at": _now()},
        )


def _finish_run(engine, batch_id: str, status: str, rows: int, error_message: str = None) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                UPDATE {RUNS_TABLE}
                SET status = :status, rows_loaded = :rows,
                    error_message = :error_message, finished_at = :finished_at
                WHERE batch_id = :batch_id
                """
            ),
            {
                "status": status, "rows": rows, "error_message": error_message,
                "finished_at": _now(), "batch_id": batch_id,
            },
        )


def _pg_copy_insert(table, conn, keys, data_iter):
    """pandas to_sql 'method' callback: bulk-load via Postgres COPY instead
    of row-by-row INSERT. This is what actually delivers 'minimal latency'."""
    dbapi_conn = conn.connection
    buf = io.StringIO()
    csv.writer(buf).writerows(data_iter)
    buf.seek(0)
    columns = ", ".join(f'"{k}"' for k in keys)
    with dbapi_conn.cursor() as cur:
        cur.copy_expert(f'COPY "{table.name}" ({columns}) FROM STDIN WITH CSV', buf)


def load(batch_id: str = None) -> dict:
    mysql_engine = get_mysql_engine()
    batch_id = batch_id or get_latest_batch_id(mysql_engine)

    result = transform(batch_id)
    fact_rows = result["rows"][FACT_COLUMNS]
    kpis = result["kpis"]
    for kpi_df in kpis.values():
        kpi_df.insert(0, "batch_id", batch_id)

    pg_engine = get_pg_engine()
    _start_run(pg_engine, batch_id)

    try:
        with pg_engine.begin() as conn:
            # Idempotent: wipe this batch's rows before re-writing them.
            conn.execute(text(f"DELETE FROM {FACT_TABLE} WHERE batch_id = :b"), {"b": batch_id})
            for table in KPI_TABLES.values():
                conn.execute(text(f"DELETE FROM {table} WHERE batch_id = :b"), {"b": batch_id})

            fact_rows.to_sql(
                FACT_TABLE, con=conn, if_exists="append", index=False,
                method=_pg_copy_insert,
            )
            for key, table in KPI_TABLES.items():
                kpis[key].to_sql(table, con=conn, if_exists="append", index=False)

    except Exception as exc:
        _finish_run(pg_engine, batch_id, status="FAILED", rows=0, error_message=str(exc))
        raise

    _finish_run(pg_engine, batch_id, status="SUCCESS", rows=len(fact_rows))
    return {"batch_id": batch_id, "rows_loaded": len(fact_rows)}


if __name__ == "__main__":
    try:
        result = load()
        print(f"batch {result['batch_id']}: loaded {result['rows_loaded']:,} rows into {FACT_TABLE}")
    except Exception as exc:
        print(f"LOAD FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
