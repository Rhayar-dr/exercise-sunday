from datetime import date

import pytest

from arr_pipeline.cleaning.dates import parse_published_date

TODAY = date(2026, 1, 1)


@pytest.mark.parametrize("raw, expected, fmt", [
    ("2023-05-12", date(2023, 5, 12), "ISO_DATE"),
    ("2023-05-12T08:30:00Z", date(2023, 5, 12), "ISO_DATETIME"),
    ("2023/05/12", date(2023, 5, 12), "YYYY/MM/DD"),
    ("02/23/2023", date(2023, 2, 23), "MM/DD/YYYY"),
    ("23-08-2023", date(2023, 8, 23), "DD-MM-YYYY"),
    ("06-21-2024", date(2024, 6, 21), "MM-DD-YYYY"),   # month-first dash, unambiguous
    ("21 Feb 2020", date(2020, 2, 21), "D Mon YYYY"),
    ("October 19, 2022", date(2022, 10, 19), "Month D, YYYY"),
    ("March 09, 2021", date(2021, 3, 9), "Month D, YYYY"),
])
def test_formats(raw, expected, fmt):
    r = parse_published_date(raw, today=TODAY)
    assert r.is_valid and r.value == expected and r.format == fmt and not r.is_ambiguous


def test_ambiguous_slash_is_us_and_flagged():
    r = parse_published_date("04/05/2021", today=TODAY)
    assert r.value == date(2021, 4, 5) and r.is_ambiguous and r.alternative_date == date(2021, 5, 4)


def test_ambiguous_dash_is_eu_and_flagged():
    r = parse_published_date("04-05-2021", today=TODAY)
    assert r.value == date(2021, 5, 4) and r.is_ambiguous and r.alternative_date == date(2021, 4, 5)


def test_same_day_and_month_is_not_ambiguous():
    r = parse_published_date("05/05/2023", today=TODAY)
    assert r.value == date(2023, 5, 5) and not r.is_ambiguous


def test_year_quarter_month():
    r = parse_published_date("2023-08-01", today=TODAY)
    assert (r.year, r.quarter, r.month) == (2023, 3, 8)


@pytest.mark.parametrize("raw, status", [
    (None, "missing"), ("", "missing"), ("N/A", "missing"),
    ("2023-02-30", "invalid"), ("13/13/2023", "invalid"), ("yesterday", "invalid"),
    ("2030-01-01", "out_of_range"), ("1900-01-01", "out_of_range"),
])
def test_bad_dates(raw, status):
    r = parse_published_date(raw, today=TODAY)
    assert r.status == status and r.value is None and r.year is None
