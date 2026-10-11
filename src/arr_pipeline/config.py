"""Static configuration: paths, FX rates, thresholds and version stamps.

Anything that changes the *meaning* of modeled data (FX rates, parsing rules,
taxonomy) is versioned here so every derived row can be traced back to the
rules that produced it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "output"
WAREHOUSE_CSV_DIR = OUTPUT_DIR / "warehouse"
DEFAULT_DB_PATH = OUTPUT_DIR / "warehouse.duckdb"
EMBEDDINGS_DIR = OUTPUT_DIR / "embeddings"

DEFAULT_ARTICLES_PATH = RAW_DIR / "tech_news.csv"
DEFAULT_METADATA_PATH = RAW_DIR / "company_metadata.json"

# Bump when parsing/normalization rules change. Stored on every silver/gold row
# so a backfill (re-deriving silver from bronze) is visible in the data.
PARSER_VERSION = "1.0.0"

# Fixed FX rates for this exercise (multiplier to convert 1 unit -> USD).
# Decimal so that e.g. JPY (divide by 150) does not pick up float error before
# the final rounding to integer USD.
FX_TO_USD: dict[str, Decimal] = {
    "USD": Decimal("1"),
    "EUR": Decimal("1.1"),
    "GBP": Decimal("1.27"),
    "JPY": Decimal("1") / Decimal("150"),
}
FX_RATE_SET = "assignment_static_v1"

# Company size thresholds (employees).
SIZE_SMALL_MAX_EXCLUSIVE = 10_000   # Small: < 10,000
SIZE_MEDIUM_MAX_INCLUSIVE = 30_000  # Medium: 10,000..30,000 ; Large: > 30,000

# Plausible publication window; dates outside are flagged out_of_range.
MIN_VALID_YEAR = 1990

EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
TOP_SIMILAR_K = 3


@dataclass(frozen=True)
class AIExportFilter:
    """Parameters for the required ai_articles_enriched.csv export."""

    min_year: int = 2022
    max_year: int = 2024
    min_arr_usd_exclusive: int = 50_000_000
    ai_value: str = "AI_ML"
    columns: tuple[str, ...] = field(
        default=(
            "article_id", "title", "company_name", "published_date", "category",
            "arr_usd", "summary", "url", "industry", "founded_year", "headquarters",
            "employee_count", "is_public", "stock_ticker", "company_age",
            "company_size_category", "embedding", "top_similar_articles",
        )
    )
