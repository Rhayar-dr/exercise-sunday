"""Resolve raw article company names to a company identity.

Resolution cascade (first hit wins, the method is recorded on every row):

1. ``exact``       raw name equals a metadata key
2. ``normalized``  equal after case-folding, stripping legal suffixes
                   (Inc., Corporation, Technologies...), parentheticals and spaces
                   ("Open AI", "Mongo DB", "NVIDIA Corporation", "Amazon Web Services (AWS)")
3. ``alias``       curated alias table (AWS, DeepMind, Facebook AI Research...)
4. ``ambiguous``   names that reference several entities ("Foo / Bar", "Foo & Bar")
                   are deliberately NOT resolved - attributing a figure to the wrong
                   company is worse than leaving it unattributed. A reviewed decision
                   can still be recorded in the alias table (checked first).
5. ``fuzzy``       difflib similarity >= threshold with a clear margin over the runner-up
6. ``unmatched``   not in metadata -> still gets a stable company_id (so its ARR
                   time series is queryable) but ``in_metadata = False``

``relationship`` distinguishes a name for the entity itself (``self``) from a
product/segment rolled up to its parent (``product_of_parent``). Segment ARR is
not the parent's ARR, so gold latest/quarterly views only use ``self`` rows.
"""
from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from typing import Any, Iterable, Optional

EXACT = "exact"
NORMALIZED = "normalized"
ALIAS = "alias"
FUZZY = "fuzzy"
AMBIGUOUS = "ambiguous"
UNMATCHED = "unmatched"
MISSING = "missing"

SELF = "self"
PRODUCT_OF_PARENT = "product_of_parent"

# Curated aliases: normalized alias -> (canonical metadata name, relationship).
COMPANY_ALIASES: dict[str, tuple[str, str]] = {
    "aws": ("Amazon Web Services", SELF),
    "amazon aws": ("Amazon Web Services", SELF),
    "deepmind": ("Google DeepMind", SELF),
    "google deep mind": ("Google DeepMind", SELF),
    "meta ai research": ("Meta AI", SELF),
    "facebook ai research": ("Meta AI", SELF),
    "fair": ("Meta AI", SELF),
    "facebook ai": ("Meta AI", SELF),
    # Reviewed decision: in this dataset "Azure" ARR values sit exactly on Microsoft's
    # series (e.g. $79.17B, $83.85B), so they are name variants, not a segment. Switch to
    # PRODUCT_OF_PARENT if a source ever reports true segment revenue.
    "azure": ("Microsoft", SELF),
    "microsoft azure": ("Microsoft", SELF),
    # Reviewed override of the multi-entity rule: all 3 values are on SpaceX's series
    # ($8.5B 2023-12, $9.945B 2024-11/12). New compound names still stay unresolved.
    "the boring company / spacex": ("SpaceX", SELF),
}

_LEGAL_SUFFIXES = (
    "incorporated", "inc", "corporation", "corp", "company", "co", "technologies",
    "technology", "holdings", "group", "ltd", "limited", "llc", "plc", "gmbh", "sa", "ag",
)
_MULTI_ENTITY_RE = re.compile(r"\s(?:/|\+)\s|/|\s&\s|\band\b", re.IGNORECASE)

FUZZY_THRESHOLD = 0.88
FUZZY_MARGIN = 0.05


@dataclass(frozen=True)
class CompanyMatch:
    raw_name: Optional[str]
    company_id: Optional[str]
    canonical_name: Optional[str]
    match_method: str
    match_score: float
    in_metadata: bool
    relationship: Optional[str] = None
    note: Optional[str] = None

    @property
    def is_resolved(self) -> bool:
        return self.company_id is not None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_company_name(name: str) -> str:
    """Lowercase, drop parentheticals, punctuation and trailing legal suffixes."""
    s = name.casefold()
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    tokens = s.split()
    while len(tokens) > 1 and tokens[-1] in _LEGAL_SUFFIXES:
        tokens.pop()
    if len(tokens) > 1 and tokens[0] == "the":
        tokens.pop(0)
    return " ".join(tokens)


def _compact(name: str) -> str:
    return normalize_company_name(name).replace(" ", "")


def company_id_for(name: str) -> str:
    """Stable, human-readable surrogate key: 'Amazon Web Services' -> 'amazon-web-services'."""
    return re.sub(r"[^a-z0-9]+", "-", name.casefold()).strip("-")


class CompanyResolver:
    def __init__(self, canonical_names: Iterable[str], aliases: Optional[dict[str, tuple[str, str]]] = None):
        self.canonical_names = sorted(set(canonical_names))
        self._by_compact = {_compact(n): n for n in self.canonical_names}
        aliases = COMPANY_ALIASES if aliases is None else aliases
        self._aliases = {
            normalize_company_name(k): v for k, v in aliases.items() if v[0] in self.canonical_names
        }

    def _hit(self, raw: str, canonical: str, method: str, score: float,
             relationship: str = SELF, note: Optional[str] = None) -> CompanyMatch:
        return CompanyMatch(raw, company_id_for(canonical), canonical, method, round(score, 4), True, relationship, note)

    def resolve(self, raw: Any) -> CompanyMatch:
        if raw is None or (isinstance(raw, float) and math.isnan(raw)) or not str(raw).strip():
            return CompanyMatch(None, None, None, MISSING, 0.0, False, note="empty company name")
        raw = str(raw).strip()
        if raw in self.canonical_names:
            return self._hit(raw, raw, EXACT, 1.0)

        norm = normalize_company_name(raw)
        if norm in self._aliases:
            canonical, relationship = self._aliases[norm]
            return self._hit(raw, canonical, ALIAS, 1.0, relationship)
        if _compact(raw) in self._by_compact:
            return self._hit(raw, self._by_compact[_compact(raw)], NORMALIZED, 1.0)

        if _MULTI_ENTITY_RE.search(raw):
            return CompanyMatch(raw, None, None, AMBIGUOUS, 0.0, False,
                                note="name references multiple entities; left unresolved")

        scored = sorted(
            ((SequenceMatcher(None, _compact(raw), key).ratio(), name) for key, name in self._by_compact.items()),
            reverse=True,
        )
        if scored:
            best_score, best = scored[0]
            runner_up = scored[1][0] if len(scored) > 1 else 0.0
            if best_score >= FUZZY_THRESHOLD and best_score - runner_up >= FUZZY_MARGIN:
                return self._hit(raw, best, FUZZY, best_score, note=f"runner-up score {runner_up:.3f}")

        return CompanyMatch(raw, company_id_for(norm or raw), raw, UNMATCHED, 0.0, False, SELF,
                            note="not present in company metadata")
