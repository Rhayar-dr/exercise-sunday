"""Shared sector taxonomy for article ``category`` AND company metadata ``industry``.

Using one taxonomy for both lets the AI filter be a simple
``category = 'AI_ML' OR industry = 'AI_ML'``. Unknown labels map to ``OTHER``
(and keep their raw value in the ``*_raw`` column), they are never dropped.
"""
from __future__ import annotations

import math
import re
from typing import Any, Optional

AI_ML = "AI_ML"
CLOUD = "CLOUD"
DATA_ANALYTICS = "DATA_ANALYTICS"
FINTECH = "FINTECH"
CYBERSECURITY = "CYBERSECURITY"
SOFTWARE_SAAS = "SOFTWARE_SAAS"
OTHER = "OTHER"

TAXONOMY = (AI_ML, CLOUD, DATA_ANALYTICS, FINTECH, CYBERSECURITY, SOFTWARE_SAAS, OTHER)

# Keys are normalized with _key(): lowercase, '&' -> 'and', punctuation -> space.
CATEGORY_MAPPING: dict[str, str] = {
    # AI / ML
    "ai ml": AI_ML,
    "ai and ml": AI_ML,
    "ai": AI_ML,
    "ml": AI_ML,
    "artificial intelligence": AI_ML,
    "machine learning": AI_ML,
    "generative ai": AI_ML,
    "deep learning": AI_ML,
    # Cloud
    "cloud": CLOUD,
    "cloud computing": CLOUD,
    "cloud services": CLOUD,
    "cloud infrastructure": CLOUD,
    # Data / analytics
    "analytics": DATA_ANALYTICS,
    "data analytics": DATA_ANALYTICS,
    "big data": DATA_ANALYTICS,
    "data": DATA_ANALYTICS,
    # FinTech
    "fintech": FINTECH,
    "financial technology": FINTECH,
    "finance": FINTECH,
    "payments": FINTECH,
    # Security
    "cybersecurity": CYBERSECURITY,
    "cyber security": CYBERSECURITY,
    "infosec": CYBERSECURITY,
    "information security": CYBERSECURITY,
    "security": CYBERSECURITY,
    # Software
    "software": SOFTWARE_SAAS,
    "enterprise software": SOFTWARE_SAAS,
    "saas": SOFTWARE_SAAS,
    "software as a service": SOFTWARE_SAAS,
}


def _key(label: str) -> str:
    label = label.strip().lower().replace("&", " and ")
    label = re.sub(r"[^a-z0-9]+", " ", label)
    return re.sub(r"\s+", " ", label).strip()


def normalize_category(raw: Any) -> Optional[str]:
    """Map a raw category/industry label to the taxonomy.

    Returns None for missing input and ``OTHER`` for unmapped labels.
    """
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return None
    key = _key(str(raw))
    if not key:
        return None
    return CATEGORY_MAPPING.get(key, OTHER)
