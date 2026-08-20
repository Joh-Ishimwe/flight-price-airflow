"""Validate one batch of stg_flight_prices. Flags bad rows, deletes nothing.

Results go to stg_validation_results (see mysql.sql), keyed by (batch_id, id).
Re-running for the same batch is safe: old results for that batch are
replaced, not duplicated.
"""

import sys

import pandas as pd
from sqlalchemy import text

from src.ingestion.ingest_to_mysql import get_engine, get_latest_batch_id

STAGING_TABLE = "stg_flight_prices"
RESULTS_TABLE = "stg_validation_results"
ALERTS_TABLE = "stg_quality_alerts"
# Above this invalid-row percentage, a batch gets flagged for human review.
REVIEW_THRESHOLD_PCT = 5.0

REQUIRED_COLUMNS = [
    "airline", "source_code", "source_name",
    "destination_code", "destination_name",
    "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt",
]
VALID_ROUTE_CODE = r"^[A-Z]{3}$"


def _check_missing(df: pd.DataFrame) -> pd.Series:
    reasons = pd.Series([[] for _ in range(len(df))], index=df.index)
    for col in REQUIRED_COLUMNS:
        missing = df[col].isnull() | (df[col].astype(str).str.strip() == "")
        for idx in df.index[missing]:
            reasons[idx].append(f"missing_{col}")
    return reasons


def _check_negative_fares(df: pd.DataFrame) -> pd.Series:
    bad = (
        (df["base_fare_bdt"] < 0)
        | (df["tax_surcharge_bdt"] < 0)
        | (df["total_fare_bdt"] < 0)
    )
    return bad.map(lambda x: ["negative_fare"] if x else [])


def _check_invalid_route(df: pd.DataFrame) -> pd.Series:
    bad_codes = (
        ~df["source_code"].astype(str).str.match(VALID_ROUTE_CODE)
        | ~df["destination_code"].astype(str).str.match(VALID_ROUTE_CODE)
    )
    same_route = df["source_code"] == df["destination_code"]
    reasons = pd.Series([[] for _ in range(len(df))], index=df.index)
    for idx in df.index[bad_codes]:
        reasons[idx].append("invalid_route_code")
    for idx in df.index[same_route]:
        reasons[idx].append("source_equals_destination")
    return reasons


def _check_invalid_duration(df: pd.DataFrame) -> pd.Series:
    bad = df["duration_hrs"] <= 0
    return bad.map(lambda x: ["invalid_duration"] if x else [])


CHECKS = [
    _check_missing,
    _check_negative_fares,
    _check_invalid_route,
    _check_invalid_duration,
]


def validate(batch_id: str = None) -> dict:
    engine = get_engine()
    batch_id = batch_id or get_latest_batch_id(engine)

    df = pd.read_sql(
        text(f"SELECT * FROM {STAGING_TABLE} WHERE batch_id = :batch_id"),
        engine,
        params={"batch_id": batch_id},
    )
    if df.empty:
        raise ValueError(f"Batch {batch_id} has no rows in {STAGING_TABLE}.")

    all_reasons = pd.Series([[] for _ in range(len(df))], index=df.index)
    for check in CHECKS:
        for idx, reasons in check(df).items():
            all_reasons[idx].extend(reasons)

    results = pd.DataFrame({
        "id": df["id"],
        "batch_id": batch_id,
        "is_valid": all_reasons.map(len) == 0,
        "reasons": all_reasons.map(lambda r: ",".join(r) if r else None),
    })

    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {RESULTS_TABLE} WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        )
    results.to_sql(RESULTS_TABLE, con=engine, if_exists="append", index=False, method="multi", chunksize=1000)

    valid_count = int(results["is_valid"].sum())
    invalid_count = len(results) - valid_count
    invalid_pct = round(100 * invalid_count / len(results), 2)
    reason_counts = (
        all_reasons.explode().dropna().value_counts().to_dict()
    )
    needs_review = invalid_pct > REVIEW_THRESHOLD_PCT

    _record_quality_alert(
        engine, batch_id, len(results), invalid_count, invalid_pct,
        reason_counts, needs_review,
    )

    summary = {
        "batch_id": batch_id,
        "total_rows": len(results),
        "valid_rows": valid_count,
        "invalid_rows": invalid_count,
        "invalid_pct": invalid_pct,
        "needs_review": needs_review,
        "reason_counts": reason_counts,
    }
    return summary


def _record_quality_alert(engine, batch_id, total_rows, invalid_rows, invalid_pct, reason_counts, needs_review) -> None:
    """One row per batch in stg_quality_alerts - a human can check this
    table for anything needing review, without scanning per-row results."""
    reason_summary = ",".join(f"{k}:{v}" for k, v in reason_counts.items()) or None
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {ALERTS_TABLE} WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        )
        conn.execute(
            text(
                f"""
                INSERT INTO {ALERTS_TABLE}
                    (batch_id, total_rows, invalid_rows, invalid_pct, reason_summary, needs_review)
                VALUES (:batch_id, :total_rows, :invalid_rows, :invalid_pct, :reason_summary, :needs_review)
                """
            ),
            {
                "batch_id": batch_id, "total_rows": total_rows, "invalid_rows": invalid_rows,
                "invalid_pct": invalid_pct, "reason_summary": reason_summary, "needs_review": needs_review,
            },
        )


def _print_summary(summary: dict) -> None:
    print(f"batch {summary['batch_id']}")
    print(f"  total:   {summary['total_rows']:,}")
    print(f"  valid:   {summary['valid_rows']:,}")
    print(f"  invalid: {summary['invalid_rows']:,} ({summary['invalid_pct']}%)")
    if summary["needs_review"]:
        print(f"  NEEDS REVIEW - invalid rate above {REVIEW_THRESHOLD_PCT}% threshold")
    if summary["reason_counts"]:
        print("  reasons:")
        for reason, count in sorted(summary["reason_counts"].items(), key=lambda x: -x[1]):
            print(f"    {reason}: {count:,}")


if __name__ == "__main__":
    try:
        result = validate()
        _print_summary(result)
    except Exception as exc:
        print(f"VALIDATION FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
