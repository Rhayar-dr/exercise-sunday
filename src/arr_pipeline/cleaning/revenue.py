"""Parse the messy source ``revenue`` column into an integer USD ARR value.

Supported shapes (all seen in the source or listed in the spec)::

    5.2B   $5.2B   $5,200,000,000   5.2 billion   $980.0M   500M USD
    €1,254,545,455   £244,094,488   ¥360,000,000,000   EUR 2.1B
    $10M - $20M   $10 - 20M   (ranges -> midpoint)

Non-values are *classified*, never coerced to 0:
    ""/null/None/nan          -> status "missing"
    N/A, NA, n.a.             -> status "not_available"
    Not disclosed, undisclosed -> status "not_disclosed"
    anything else unparseable -> status "unparseable" (with error_reason)

Only status == "valid" rows are allowed into the ARR fact table.
"""
from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Optional

from ..config import FX_TO_USD

VALID = "valid"
MISSING = "missing"
NOT_AVAILABLE = "not_available"
NOT_DISCLOSED = "not_disclosed"
UNPARSEABLE = "unparseable"

_MISSING_TOKENS = {"", "null", "none", "nan", "nil", "-", "--"}
_NOT_AVAILABLE_TOKENS = {"n/a", "na", "n.a.", "n.a", "not available"}
_NOT_DISCLOSED_TOKENS = {"not disclosed", "undisclosed", "not publicly disclosed", "confidential", "tbd", "unknown"}

_CURRENCY_SYMBOLS = {"$": "USD", "US$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY", "￥": "JPY"}
_CURRENCY_CODES = {"USD", "EUR", "GBP", "JPY"}

_SCALES = {
    "k": Decimal(10) ** 3, "thousand": Decimal(10) ** 3,
    "m": Decimal(10) ** 6, "mm": Decimal(10) ** 6, "mn": Decimal(10) ** 6, "million": Decimal(10) ** 6,
    "b": Decimal(10) ** 9, "bn": Decimal(10) ** 9, "billion": Decimal(10) ** 9,
    "t": Decimal(10) ** 12, "tn": Decimal(10) ** 12, "trillion": Decimal(10) ** 12,
}

# One side of a value: [currency] number [scale] [currency]
_AMOUNT_RE = re.compile(
    r"""^\s*
    (?P<pre>US\$|[$€£¥￥]|USD|EUR|GBP|JPY)?\s*
    (?P<num>\d[\d,]*(?:\.\d+)?|\.\d+)\s*
    (?P<scale>thousand|million|billion|trillion|mm|mn|bn|tn|k|m|b|t)?\.?\s*
    (?P<post>USD|EUR|GBP|JPY)?\s*$""",
    re.IGNORECASE | re.VERBOSE,
)
# Range separators: hyphen/en dash/em dash surrounded by optional spaces, or "to".
_RANGE_SPLIT_RE = re.compile(r"\s*(?:–|—|-|\bto\b)\s*", re.IGNORECASE)


@dataclass(frozen=True)
class RevenueParseResult:
    status: str
    arr_usd: Optional[int] = None
    arr_usd_low: Optional[int] = None
    arr_usd_high: Optional[int] = None
    currency: Optional[str] = None
    fx_rate_to_usd: Optional[float] = None
    amount_original: Optional[float] = None  # midpoint in original currency units
    is_range: bool = False
    error_reason: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        return self.status == VALID

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _Amount:
    value: Optional[Decimal]  # number as written (before scale)
    scale: Optional[Decimal]
    currency: Optional[str]


def _to_decimal(num: str) -> Decimal:
    """Parse '5,200,000,000', '980.0', '2,1' (decimal comma) into a Decimal."""
    if re.fullmatch(r"\d{1,3}(,\d{3})+(\.\d+)?", num):
        num = num.replace(",", "")
    elif re.fullmatch(r"\d+,\d{1,2}", num):
        num = num.replace(",", ".")  # European decimal comma, e.g. "2,1B"
    elif "," in num:
        raise InvalidOperation(f"irregular digit grouping in {num!r}")
    return Decimal(num)


def _parse_amount(text: str) -> Optional[_Amount]:
    m = _AMOUNT_RE.match(text)
    if not m:
        return None
    pre, post = m.group("pre"), m.group("post")
    currencies = {_CURRENCY_SYMBOLS.get(c.upper(), c.upper()) for c in (pre, post) if c}
    if len(currencies) > 1:
        return None  # e.g. "€5M USD" - contradictory
    scale = m.group("scale")
    return _Amount(
        value=_to_decimal(m.group("num")),
        scale=_SCALES[scale.lower()] if scale else None,
        currency=currencies.pop() if currencies else None,
    )


def _classify_non_value(raw: Any) -> Optional[str]:
    if raw is None:
        return MISSING
    if isinstance(raw, float) and math.isnan(raw):
        return MISSING
    token = str(raw).strip().lower()
    if token in _MISSING_TOKENS:
        return MISSING
    if token in _NOT_AVAILABLE_TOKENS:
        return NOT_AVAILABLE
    if token in _NOT_DISCLOSED_TOKENS:
        return NOT_DISCLOSED
    return None


def _round_usd(value: Decimal) -> int:
    return int(value.quantize(Decimal(1), rounding=ROUND_HALF_UP))


def parse_revenue(raw: Any, default_currency: str = "USD") -> RevenueParseResult:
    """Parse one raw revenue string. Never raises; failures become statuses."""
    status = _classify_non_value(raw)
    if status:
        return RevenueParseResult(status=status)
    if isinstance(raw, (int, float, Decimal)) and not isinstance(raw, bool):
        text = format(Decimal(str(raw)), "f")
    else:
        text = str(raw).strip()

    if text.startswith(("-", "(")):
        return RevenueParseResult(status=UNPARSEABLE, error_reason="negative amount")
    parts = _RANGE_SPLIT_RE.split(text)
    if len(parts) not in (1, 2) or any(p == "" for p in parts):
        return RevenueParseResult(status=UNPARSEABLE, error_reason="unrecognized range structure")

    try:
        amounts = [_parse_amount(p) for p in parts]
    except InvalidOperation as exc:
        return RevenueParseResult(status=UNPARSEABLE, error_reason=str(exc))
    if any(a is None for a in amounts):
        return RevenueParseResult(status=UNPARSEABLE, error_reason="unrecognized amount format")

    # In a range, a side missing currency/scale inherits from the other side ("$10 - 20M").
    currencies = {a.currency for a in amounts if a.currency}
    if len(currencies) > 1:
        return RevenueParseResult(status=UNPARSEABLE, error_reason="range mixes currencies")
    currency = currencies.pop() if currencies else default_currency
    scales = [a.scale for a in amounts]
    known_scale = next((s for s in reversed(scales) if s is not None), Decimal(1))
    originals = [a.value * (a.scale if a.scale is not None else known_scale) for a in amounts]

    low, high = min(originals), max(originals)
    if high <= 0:
        return RevenueParseResult(status=UNPARSEABLE, error_reason="non-positive amount")
    midpoint = (low + high) / 2
    fx = FX_TO_USD[currency]
    return RevenueParseResult(
        status=VALID,
        arr_usd=_round_usd(midpoint * fx),
        arr_usd_low=_round_usd(low * fx),
        arr_usd_high=_round_usd(high * fx),
        currency=currency,
        fx_rate_to_usd=float(fx),
        amount_original=float(midpoint),
        is_range=len(amounts) == 2,
    )


def parse_revenue_usd(raw: Any) -> Optional[int]:
    """Convenience wrapper: integer USD for valid values, else None."""
    return parse_revenue(raw).arr_usd
