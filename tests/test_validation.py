"""Unit tests for validation checks - pure pandas logic, no DB needed."""

import pandas as pd

from src.validation.validate_staging import (
    _check_invalid_duration,
    _check_invalid_route,
    _check_missing,
    _check_negative_fares,
)


def _row(**overrides):
    base = {
        "airline": "Biman Bangladesh Airlines",
        "source_code": "DAC",
        "source_name": "Dhaka",
        "destination_code": "CXB",
        "destination_name": "Cox's Bazar",
        "base_fare_bdt": 1000.0,
        "tax_surcharge_bdt": 200.0,
        "total_fare_bdt": 1200.0,
        "duration_hrs": 1.5,
    }
    base.update(overrides)
    return base


def _df(*rows):
    return pd.DataFrame(list(rows))


def test_check_missing_flags_blank_required_field():
    df = _df(_row(airline=""), _row())
    reasons = _check_missing(df)
    assert "missing_airline" in reasons[0]
    assert reasons[1] == []


def test_check_missing_flags_null_required_field():
    df = _df(_row(source_name=None))
    reasons = _check_missing(df)
    assert "missing_source_name" in reasons[0]


def test_check_negative_fares_flags_negative_base_fare():
    df = _df(_row(base_fare_bdt=-50.0), _row())
    reasons = _check_negative_fares(df)
    assert reasons[0] == ["negative_fare"]
    assert reasons[1] == []


def test_check_negative_fares_allows_zero():
    df = _df(_row(base_fare_bdt=0.0))
    reasons = _check_negative_fares(df)
    assert reasons[0] == []


def test_check_invalid_route_flags_bad_code_format():
    df = _df(_row(source_code="D1"))
    reasons = _check_invalid_route(df)
    assert "invalid_route_code" in reasons[0]


def test_check_invalid_route_flags_same_source_and_destination():
    df = _df(_row(source_code="DAC", destination_code="DAC"))
    reasons = _check_invalid_route(df)
    assert "source_equals_destination" in reasons[0]


def test_check_invalid_route_allows_valid_distinct_codes():
    df = _df(_row())
    reasons = _check_invalid_route(df)
    assert reasons[0] == []


def test_check_invalid_duration_flags_zero_and_negative():
    df = _df(_row(duration_hrs=0), _row(duration_hrs=-1), _row())
    reasons = _check_invalid_duration(df)
    assert reasons[0] == ["invalid_duration"]
    assert reasons[1] == ["invalid_duration"]
    assert reasons[2] == []
