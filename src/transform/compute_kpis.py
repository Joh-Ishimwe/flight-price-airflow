"""Transform validated staging data and compute KPIs.

Reads only rows flagged valid by validate_staging. Recomputes Total Fare
from Base Fare + Tax & Surcharge unconditionally (staging's stored value
isn't trusted - see validate_staging's fare_mismatch history). Output is
one dataframe per KPI, ready for the Postgres load step.
"""

import sys

import pandas as pd
from sqlalchemy import text

from src.ingestion.ingest_to_mysql import get_engine, get_latest_batch_id

STAGING_TABLE = "stg_flight_prices"
RESULTS_TABLE = "stg_validation_results"
FARE_TOLERANCE = 0.01

# Seasonality values treated as high-demand travel periods.
PEAK_SEASONS = {"Eid", "Winter Holidays", "Hajj"}
TOP_N_ROUTES = 10


def load_valid_rows(engine, batch_id: str) -> pd.DataFrame:
    query = text(
        f"""
        SELECT s.*
        FROM {STAGING_TABLE} s
        JOIN {RESULTS_TABLE} v ON v.batch_id = s.batch_id AND v.id = s.id
        WHERE s.batch_id = :batch_id AND v.is_valid = 1
        """
    )
    df = pd.read_sql(query, engine, params={"batch_id": batch_id})
    if df.empty:
        raise ValueError(f"No valid rows for batch {batch_id}. Run validate_staging first.")
    return df


def recompute_total_fare(df: pd.DataFrame) -> tuple:
    """Total Fare = Base Fare + Tax & Surcharge, always. Returns (df, rows_corrected)."""
    recomputed = df["base_fare_bdt"] + df["tax_surcharge_bdt"]
    corrected = int((recomputed - df["total_fare_bdt"]).abs().gt(FARE_TOLERANCE).sum())
    df = df.copy()
    df["total_fare_bdt"] = recomputed
    return df, corrected


def kpi_avg_fare_by_airline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("airline")["total_fare_bdt"]
        .agg(avg_fare="mean", booking_count="count")
        .reset_index()
        .sort_values("avg_fare", ascending=False)
    )


def kpi_seasonal_fare(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("seasonality")["total_fare_bdt"]
        .agg(avg_fare="mean", booking_count="count")
        .reset_index()
    )
    out["is_peak"] = out["seasonality"].isin(PEAK_SEASONS)
    return out.sort_values("avg_fare", ascending=False)


def kpi_booking_count_by_airline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("airline")
        .size()
        .reset_index(name="booking_count")
        .sort_values("booking_count", ascending=False)
    )


def kpi_popular_routes(df: pd.DataFrame, top_n: int = TOP_N_ROUTES) -> pd.DataFrame:
    routes = (
        df.groupby(["source_code", "destination_code"])
        .size()
        .reset_index(name="booking_count")
        .sort_values("booking_count", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    routes.insert(0, "rank", routes.index + 1)
    return routes


def transform(batch_id: str = None) -> dict:
    engine = get_engine()
    batch_id = batch_id or get_latest_batch_id(engine)

    df = load_valid_rows(engine, batch_id)
    df, corrected = recompute_total_fare(df)

    kpis = {
        "avg_fare_by_airline": kpi_avg_fare_by_airline(df),
        "seasonal_fare": kpi_seasonal_fare(df),
        "booking_count_by_airline": kpi_booking_count_by_airline(df),
        "popular_routes": kpi_popular_routes(df),
    }

    return {
        "batch_id": batch_id,
        "rows": df,
        "rows_total_fare_corrected": corrected,
        "kpis": kpis,
    }


def _print_summary(result: dict) -> None:
    print(f"batch {result['batch_id']}")
    print(f"  rows transformed: {len(result['rows']):,}")
    print(f"  total_fare corrected: {result['rows_total_fare_corrected']:,}")
    for name, kpi_df in result["kpis"].items():
        print(f"\n{name}:")
        print(kpi_df.to_string(index=False))


if __name__ == "__main__":
    try:
        result = transform()
        _print_summary(result)
    except Exception as exc:
        print(f"TRANSFORM FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
