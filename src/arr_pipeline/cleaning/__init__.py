"""Pure, reusable cleaning functions (no I/O). Each returns a result object that
carries both the cleaned value and a status, so failures are inspectable
instead of silently becoming nulls."""

from .categories import normalize_category
from .companies import CompanyResolver, CompanyMatch, normalize_company_name
from .dates import DateParseResult, parse_published_date
from .revenue import RevenueParseResult, parse_revenue, parse_revenue_usd

__all__ = [
    "CompanyMatch",
    "CompanyResolver",
    "DateParseResult",
    "RevenueParseResult",
    "normalize_category",
    "normalize_company_name",
    "parse_published_date",
    "parse_revenue",
    "parse_revenue_usd",
]
