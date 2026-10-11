"""Normalize ``published_date`` strings to a ``datetime.date``.

Formats are matched explicitly (no "guess anything" parser) so every parsed
value records *which* rule produced it.

Ambiguous numeric dates (both leading fields <= 12):

* ``NN/NN/YYYY`` (slash)  -> read as **US MM/DD/YYYY**. In the source, no
  slash date has a first field > 12, while many have a second field > 12,
  so the slash convention is consistently US.
* ``NN-NN-YYYY`` (dash)   -> read as **EU DD-MM-YYYY**. Unambiguous dash dates
  are mostly day-first (63 vs 17 month-first), so DMY is the majority reading.
* When the alternative reading is also a valid date, the row is flagged
  ``is_ambiguous=True`` and ``alternative_date`` holds the other reading, so
  analysts can exclude or re-interpret those rows. Both readings always share
  the same year, so year-level filters are unaffected; month/quarter may not be.

A field > 12 resolves the order unambiguously regardless of separator.
ISO timestamps (``2023-05-12T08:00:00Z``) are reduced to their date part.
"""
from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Optional

from ..config import MIN_VALID_YEAR

VALID = "valid"
MISSING = "missing"
INVALID = "invalid"
OUT_OF_RANGE = "out_of_range"

_ISO_RE = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})(?:[T ][\d:.]+(?:Z|[+-]\d{2}:?\d{2})?)?$")
_ISO_SLASH_RE = re.compile(r"^(\d{4})/(\d{1,2})/(\d{1,2})$")
_NUMERIC_RE = re.compile(r"^(\d{1,2})([/.-])(\d{1,2})\2(\d{4})$")
_TEXT_FORMATS = (
    ("%d %b %Y", "D Mon YYYY"),
    ("%d %B %Y", "D Month YYYY"),
    ("%B %d, %Y", "Month D, YYYY"),
    ("%b %d, %Y", "Mon D, YYYY"),
    ("%B %d %Y", "Month D YYYY"),
)


@dataclass(frozen=True)
class DateParseResult:
    status: str
    value: Optional[date] = None
    format: Optional[str] = None
    is_ambiguous: bool = False
    alternative_date: Optional[date] = None
    error_reason: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        return self.status == VALID

    @property
    def year(self) -> Optional[int]:
        return self.value.year if self.value else None

    @property
    def quarter(self) -> Optional[int]:
        return (self.value.month - 1) // 3 + 1 if self.value else None

    @property
    def month(self) -> Optional[int]:
        return self.value.month if self.value else None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_date(y: int, m: int, d: int) -> Optional[date]:
    try:
        return date(y, m, d)
    except ValueError:
        return None


def _numeric(match: re.Match) -> DateParseResult:
    a, sep, b, y = int(match.group(1)), match.group(2), int(match.group(3)), int(match.group(4))
    us = _safe_date(y, a, b)  # month-first
    eu = _safe_date(y, b, a)  # day-first
    us_fmt, eu_fmt = f"MM{sep}DD{sep}YYYY", f"DD{sep}MM{sep}YYYY"
    if us is None and eu is None:
        return DateParseResult(status=INVALID, error_reason="no valid day/month reading")
    if us is None:
        return DateParseResult(status=VALID, value=eu, format=eu_fmt)
    if eu is None:
        return DateParseResult(status=VALID, value=us, format=us_fmt)
    if us == eu:  # e.g. 05/05/2023
        return DateParseResult(status=VALID, value=us, format=us_fmt if sep == "/" else eu_fmt)
    # Genuinely ambiguous: apply the separator convention documented above.
    if sep == "/":
        return DateParseResult(status=VALID, value=us, format=us_fmt, is_ambiguous=True, alternative_date=eu)
    return DateParseResult(status=VALID, value=eu, format=eu_fmt, is_ambiguous=True, alternative_date=us)


def _parse(text: str) -> DateParseResult:
    if m := _ISO_RE.match(text):
        d = _safe_date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        fmt = "ISO_DATETIME" if len(text) > 10 else "ISO_DATE"
        return DateParseResult(status=VALID, value=d, format=fmt) if d else DateParseResult(
            status=INVALID, error_reason="impossible ISO date")
    if m := _ISO_SLASH_RE.match(text):
        d = _safe_date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        return DateParseResult(status=VALID, value=d, format="YYYY/MM/DD") if d else DateParseResult(
            status=INVALID, error_reason="impossible date")
    if m := _NUMERIC_RE.match(text):
        return _numeric(m)
    normalized = re.sub(r"\s+", " ", text.replace("Sept ", "Sep "))
    for fmt, label in _TEXT_FORMATS:
        try:
            return DateParseResult(status=VALID, value=datetime.strptime(normalized, fmt).date(), format=label)
        except ValueError:
            continue
    return DateParseResult(status=INVALID, error_reason="unrecognized date format")


def parse_published_date(raw: Any, today: Optional[date] = None) -> DateParseResult:
    """Parse one raw date string. Never raises; failures become statuses."""
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return DateParseResult(status=MISSING)
    if isinstance(raw, datetime):
        raw = raw.date()
    if isinstance(raw, date):
        result = DateParseResult(status=VALID, value=raw, format="date")
    else:
        text = str(raw).strip()
        if text.lower() in {"", "null", "none", "nan", "n/a", "nat"}:
            return DateParseResult(status=MISSING)
        result = _parse(text)
    if result.is_valid:
        upper = today or date.today()
        if not (MIN_VALID_YEAR <= result.value.year and result.value <= upper):
            return DateParseResult(status=OUT_OF_RANGE, format=result.format,
                                   error_reason=f"{result.value.isoformat()} outside [{MIN_VALID_YEAR}, today]")
    return result
