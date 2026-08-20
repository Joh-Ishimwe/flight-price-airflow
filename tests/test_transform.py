"""Unit tests for transform/KPI logic - pure pandas logic, no DB needed."""

import pandas as pd

from src.transform.compute_kpis import (
    kpi_avg_fare_by_airline,
    kpi_booking_count_by_airline,
    kpi_popular_routes,
    kpi_seasonal_fare,
    recompute_total_fare,
)


def _df(rows):
    return pd.DataFrame(rows)


def test_recompute_total_fare_fixes_mismatch():
    df = _df([
        {"base_fare_bdt": 1000.0, "tax_surcharge_bdt": 200.0, "total_fare_bdt": 9999.0},  # wrong
        {"base_fare_bdt": 500.0, "tax_surcharge_bdt": 50.0, "total_fare_bdt": 550.0},      # already correct
    ])
    out, corrected = recompute_total_fare(df)
    assert corrected == 1
    assert out["total_fare_bdt"].tolist() == [1200.0, 550.0]


def test_recompute_total_fare_does_not_mutate_input():
    df = _df([{"base_fare_bdt": 1000.0, "tax_surcharge_bdt": 200.0, "total_fare_bdt": 0.0}])
    recompute_total_fare(df)
    assert df["total_fare_bdt"].iloc[0] == 0.0  # original untouched


def test_kpi_avg_fare_by_airline():
    df = _df([
        {"airline": "A", "total_fare_bdt": 100.0},
        {"airline": "A", "total_fare_bdt": 300.0},
        {"airline": "B", "total_fare_bdt": 200.0},
    ])
    out = kpi_avg_fare_by_airline(df).set_index("airline")
    assert out.loc["A", "avg_fare"] == 200.0
    assert out.loc["A", "booking_count"] == 2
    assert out.loc["B", "avg_fare"] == 200.0
    assert out.loc["B", "booking_count"] == 1


def test_kpi_seasonal_fare_marks_peak_seasons():
    df = _df([
        {"seasonality": "Eid", "total_fare_bdt": 500.0},
        {"seasonality": "Regular", "total_fare_bdt": 100.0},
    ])
    out = kpi_seasonal_fare(df).set_index("seasonality")
    assert bool(out.loc["Eid", "is_peak"]) is True
    assert bool(out.loc["Regular", "is_peak"]) is False


def test_kpi_booking_count_by_airline():
    df = _df([{"airline": "A"}, {"airline": "A"}, {"airline": "B"}])
    out = kpi_booking_count_by_airline(df).set_index("airline")
    assert out.loc["A", "booking_count"] == 2
    assert out.loc["B", "booking_count"] == 1


def test_kpi_popular_routes_ranks_and_limits_top_n():
    rows = (
        [{"source_code": "X", "destination_code": "Y"}] * 3
        + [{"source_code": "A", "destination_code": "B"}]
    )
    df = _df(rows)
    out = kpi_popular_routes(df, top_n=1)
    assert len(out) == 1
    assert out.iloc[0]["source_code"] == "X"
    assert out.iloc[0]["destination_code"] == "Y"
    assert out.iloc[0]["booking_count"] == 3
    assert out.iloc[0]["rank"] == 1
