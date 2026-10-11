"""Bronze -> silver transforms (pure pandas functions; no database access).

Every silver row keeps the raw value next to the cleaned one, plus a status
column, so a failed parse is debuggable without going back to the file.
"""
from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd

from .cleaning import CompanyResolver, normalize_category, parse_published_date, parse_revenue
from .config import (
    FX_RATE_SET,
    PARSER_VERSION,
    SIZE_MEDIUM_MAX_INCLUSIVE,
    SIZE_SMALL_MAX_EXCLUSIVE,
)


def _to_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        f = float(str(value).replace(",", "").strip())
    except ValueError:
        return None
    return int(f) if f == f and f.is_integer() else None  # NaN / fractional -> None


def _to_bool(value) -> Optional[bool]:
    if value is None:
        return None
    token = str(value).strip().lower()
    if token in {"true", "1", "yes", "y", "public"}:
        return True
    if token in {"false", "0", "no", "n", "private"}:
        return False
    return None


def _blank_to_none(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return None if s.lower() in {"", "null", "none", "nan", "n/a"} else s


def company_size_category(employee_count: Optional[int]) -> Optional[str]:
    """Small: < 10,000; Medium: 10,000-30,000 (inclusive); Large: > 30,000."""
    if employee_count is None or pd.isna(employee_count):
        return None
    if employee_count < SIZE_SMALL_MAX_EXCLUSIVE:
        return "Small"
    if employee_count <= SIZE_MEDIUM_MAX_INCLUSIVE:
        return "Medium"
    return "Large"


def company_age(founded_year: Optional[int], published: Optional[date]) -> Optional[int]:
    """Whole years between founding and publication (calendar-year difference).

    Returns None when either side is missing or the result would be negative
    (company "founded" after the article) - that is a data error, flagged in DQ.
    """
    if founded_year is None or published is None or pd.isna(founded_year) or pd.isna(published):
        return None
    age = pd.Timestamp(published).year - int(founded_year)
    return age if age >= 0 else None


def clean_company_metadata(bronze_meta: pd.DataFrame, resolver: CompanyResolver) -> pd.DataFrame:
    """One row per metadata company (current version), typed and normalized."""
    rows = []
    for r in bronze_meta.itertuples(index=False):
        match = resolver.resolve(r.company_name)
        employees = _to_int(r.employee_count)
        is_public = _to_bool(r.is_public)
        ticker = _blank_to_none(r.stock_ticker)
        notes = []
        if is_public is False and ticker:
            notes.append("ticker_on_private_company")
        if is_public is True and not ticker:
            notes.append("public_company_without_ticker")
        rows.append({
            "company_id": match.company_id,
            "company_name": r.company_name,
            "founded_year": _to_int(r.founded_year),
            "headquarters": _blank_to_none(r.headquarters),
            "employee_count": employees,
            "industry_raw": _blank_to_none(r.industry),
            "industry": normalize_category(r.industry),
            "is_public": is_public,
            "stock_ticker": ticker,
            "company_size_category": company_size_category(employees),
            "metadata_quality_notes": ";".join(notes) or None,
            "source_batch_id": r.batch_id,
            "source_record_hash": r.record_hash,
        })
    return pd.DataFrame(rows)


def build_company_resolution(raw_names: pd.Series, resolver: CompanyResolver) -> pd.DataFrame:
    """One row per distinct raw company string -> resolved identity (+ how)."""
    counts = raw_names.fillna("").value_counts()
    rows = []
    for raw, n in sorted(counts.items()):
        m = resolver.resolve(raw)
        rows.append({
            "company_name_raw": raw if raw else None,
            "company_id": m.company_id,
            "canonical_name": m.canonical_name,
            "match_method": m.match_method,
            "match_score": m.match_score,
            "in_metadata": m.in_metadata,
            "relationship": m.relationship,
            "match_note": m.note,
            "article_count": int(n),
        })
    return pd.DataFrame(rows)


def clean_articles(bronze_articles: pd.DataFrame, resolution: pd.DataFrame,
                   today: Optional[date] = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (silver_articles, silver_arr_parse); both one row per article_id."""
    by_raw = resolution.set_index(resolution["company_name_raw"].fillna(""))
    articles, parses = [], []
    for r in bronze_articles.itertuples(index=False):
        d = parse_published_date(r.published_date or None, today=today)
        res = by_raw.loc[r.company_name or ""]
        articles.append({
            "article_id": r.article_id,
            "title": _blank_to_none(r.title),
            "summary": _blank_to_none(r.summary),
            "url": _blank_to_none(r.url),
            "author": _blank_to_none(r.author),
            "word_count": _to_int(r.word_count),
            "company_name_raw": _blank_to_none(r.company_name),
            "company_id": res["company_id"],
            "company_match_method": res["match_method"],
            "company_relationship": res["relationship"],
            "published_date_raw": _blank_to_none(r.published_date),
            "published_date": d.value,
            "date_status": d.status,
            "date_format": d.format,
            "date_is_ambiguous": d.is_ambiguous,
            "date_alternative": d.alternative_date,
            "published_year": d.year,
            "published_quarter": d.quarter,
            "published_month": d.month,
            "year_quarter": f"{d.year}-Q{d.quarter}" if d.value else None,
            "category_raw": _blank_to_none(r.category),
            "category": normalize_category(r.category),
            "source_batch_id": r.batch_id,
            "source_file": r.source_file,
            "source_row_number": r.source_row_number,
            "source_record_hash": r.record_hash,
            "parser_version": PARSER_VERSION,
        })
        p = parse_revenue(r.revenue)
        parses.append({
            "article_id": r.article_id,
            "revenue_raw": r.revenue if r.revenue != "" else None,
            "parse_status": p.status,
            "arr_usd": p.arr_usd,
            "arr_usd_low": p.arr_usd_low,
            "arr_usd_high": p.arr_usd_high,
            "is_range": p.is_range,
            "currency_original": p.currency,
            "amount_original": p.amount_original,
            "fx_rate_to_usd": p.fx_rate_to_usd,
            "fx_rate_set": FX_RATE_SET if p.is_valid else None,
            "error_reason": p.error_reason,
            "source_record_hash": r.record_hash,
            "parser_version": PARSER_VERSION,
        })
    silver_articles = pd.DataFrame(articles)
    for col in ("published_date", "date_alternative"):
        silver_articles[col] = pd.to_datetime(silver_articles[col]).dt.date
    int_cols = ["word_count", "published_year", "published_quarter", "published_month"]
    silver_articles[int_cols] = silver_articles[int_cols].astype("Int64")
    arr = pd.DataFrame(parses)
    arr[["arr_usd", "arr_usd_low", "arr_usd_high"]] = arr[["arr_usd", "arr_usd_low", "arr_usd_high"]].astype("Int64")
    return silver_articles, arr
