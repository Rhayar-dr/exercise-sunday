import math

import pytest

from arr_pipeline.cleaning.revenue import parse_revenue, parse_revenue_usd


@pytest.mark.parametrize("raw, expected", [
    # spec examples
    ("5.2B", 5_200_000_000),
    ("$5,200,000,000", 5_200_000_000),
    ("5.2 billion", 5_200_000_000),
    ("500M USD", 500_000_000),
    ("$10M - $20M", 15_000_000),
    # shapes seen in the source file
    ("$980.0M", 980_000_000),
    ("$9.5 million", 9_500_000),
    ("$3.2 billion", 3_200_000_000),
    ("$38475.0M - $42525.0M", 40_500_000_000),
    ("$1.2B - $1.8B", 1_500_000_000),
    ("$52.500B", 52_500_000_000),
    ("44500.0M USD", 44_500_000_000),
    # other reasonable variants
    ("$10 - 20M", 15_000_000),          # scale inherited across the range
    ("$10M to $20M", 15_000_000),
    ("$10M–$20M", 15_000_000),          # en dash
    ("US$3M", 3_000_000),
    ("750k", 750_000),
    ("1.5 bn", 1_500_000_000),
    ("USD 2B", 2_000_000_000),
])
def test_usd_formats(raw, expected):
    assert parse_revenue_usd(raw) == expected


@pytest.mark.parametrize("raw, expected, currency", [
    ("€2.1B", 2_310_000_000, "EUR"),                   # x1.1 (the spec's Snowflake example)
    ("£244,094,488", 310_000_000, "GBP"),              # x1.27, rounded half-up
    ("¥360,000,000,000", 2_400_000_000, "JPY"),        # /150
    ("€1,254,545,455", 1_380_000_001, "EUR"),
    ("2,1B EUR", 2_310_000_000, "EUR"),                # decimal comma
    ("JPY 15B", 100_000_000, "JPY"),
])
def test_fx_conversion(raw, expected, currency):
    r = parse_revenue(raw)
    assert r.is_valid and r.arr_usd == expected and r.currency == currency


def test_returns_python_int():
    assert type(parse_revenue_usd("¥1,000")) is int
    assert parse_revenue_usd("¥1,000") == 7  # 6.67 -> 7


def test_range_keeps_bounds():
    r = parse_revenue("$10M - $20M")
    assert r.is_range and (r.arr_usd_low, r.arr_usd, r.arr_usd_high) == (10_000_000, 15_000_000, 20_000_000)


@pytest.mark.parametrize("raw, status", [
    (None, "missing"), ("", "missing"), ("   ", "missing"), (float("nan"), "missing"), ("null", "missing"),
    ("N/A", "not_available"), ("n/a", "not_available"),
    ("Not disclosed", "not_disclosed"), ("UNDISCLOSED", "not_disclosed"),
])
def test_non_values_are_classified_not_zero(raw, status):
    r = parse_revenue(raw)
    assert r.status == status and r.arr_usd is None and not r.is_valid


@pytest.mark.parametrize("raw", ["abc", "-5M", "$0", "€5M USD", "$5M - €6M", "1,23,45", "$5M - $6M - $7M"])
def test_unparseable_is_flagged_with_reason(raw):
    r = parse_revenue(raw)
    assert r.status == "unparseable" and r.arr_usd is None and r.error_reason


def test_numeric_input():
    assert parse_revenue_usd(1500000) == 1_500_000
    assert parse_revenue(math.nan).status == "missing"
