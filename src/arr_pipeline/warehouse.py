"""DuckDB warehouse: bronze upserts, silver loads, gold SQL, grain checks, CSV export.

Layering
--------
bronze_*   append-only, raw strings, PK = (natural key, record_hash). Re-ingesting
           the same file or record is a no-op (INSERT OR IGNORE).
silver_*   typed/cleaned, one row per entity *current version*; fully re-derived
           from bronze on each run, so a parser change + re-run == backfill.
gold       dim_company, fct_arr_observation, article_enriched, company_arr_latest,
           company_arr_quarterly, dq_issues, article_embeddings.

Derived layers are rebuilt with CREATE OR REPLACE inside one transaction and
then checked against the declared grain (TABLE_GRAIN): a re-run can never
produce duplicate modeled rows, and a grain violation fails the run loudly.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from .config import EMBEDDING_DIM
from .ingest import ARTICLE_COLUMNS, METADATA_COLUMNS, Batch

# Declared grain (unique key) of every modeled table. Used for runtime checks,
# deterministic CSV ordering, and documentation.
TABLE_GRAIN: dict[str, tuple[str, ...]] = {
    "ingest_batches": ("batch_id",),
    "bronze_articles": ("article_id", "record_hash"),
    "bronze_company_metadata": ("company_name", "record_hash"),
    "silver_articles": ("article_id",),
    "silver_arr_parse": ("article_id",),
    "silver_company_metadata": ("company_id",),
    "silver_company_resolution": ("company_name_raw",),
    "dim_company": ("company_id",),
    "fct_arr_observation": ("observation_id",),
    "article_enriched": ("article_id",),
    "company_arr_latest": ("company_id",),
    "company_arr_quarterly": ("company_id", "year_quarter"),
    "dq_issues": ("entity_type", "entity_id", "issue_type"),
    "article_embeddings": ("article_id",),
    "pipeline_runs": ("run_id",),
}

# Tables exported as one CSV each (the required per-table warehouse exports).
EXPORT_TABLES = (
    "ingest_batches", "bronze_articles", "bronze_company_metadata",
    "silver_articles", "silver_arr_parse", "silver_company_metadata", "silver_company_resolution",
    "dim_company", "fct_arr_observation", "article_enriched",
    "company_arr_latest", "company_arr_quarterly", "dq_issues", "article_embeddings", "pipeline_runs",
)

_ARTICLE_COLS_SQL = ", ".join(f"{c} VARCHAR" for c in ARTICLE_COLUMNS)
_META_COLS_SQL = ", ".join(f"{c} VARCHAR" for c in METADATA_COLUMNS)

DDL = f"""
CREATE TABLE IF NOT EXISTS ingest_batches (
    batch_id VARCHAR PRIMARY KEY,
    batch_seq INTEGER NOT NULL,
    source_type VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    file_sha256 VARCHAR NOT NULL,
    row_count INTEGER NOT NULL,
    new_record_versions INTEGER NOT NULL,
    ingested_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS bronze_articles (
    {_ARTICLE_COLS_SQL},
    extra_columns_json VARCHAR,
    source_row_number INTEGER NOT NULL,
    record_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    PRIMARY KEY (article_id, record_hash)
);
CREATE TABLE IF NOT EXISTS bronze_company_metadata (
    company_name VARCHAR NOT NULL,
    {_META_COLS_SQL},
    raw_json VARCHAR,
    source_row_number INTEGER NOT NULL,
    record_hash VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    PRIMARY KEY (company_name, record_hash)
);
CREATE TABLE IF NOT EXISTS embedding_cache (
    text_hash VARCHAR NOT NULL,
    model_name VARCHAR NOT NULL,
    embedding FLOAT[{EMBEDDING_DIM}] NOT NULL,
    PRIMARY KEY (text_hash, model_name)
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    parser_version VARCHAR,
    embedding_model VARCHAR,
    batches_seen VARCHAR,
    new_record_versions INTEGER,
    row_counts_json VARCHAR,
    status VARCHAR
);

-- Current version of each source record: latest batch wins, then latest row in that batch.
CREATE OR REPLACE VIEW bronze_articles_current AS
SELECT * EXCLUDE (rn) FROM (
    SELECT b.*, row_number() OVER (
        PARTITION BY b.article_id ORDER BY i.batch_seq DESC, b.source_row_number DESC) AS rn
    FROM bronze_articles b JOIN ingest_batches i USING (batch_id)
) WHERE rn = 1;
CREATE OR REPLACE VIEW bronze_company_metadata_current AS
SELECT * EXCLUDE (rn) FROM (
    SELECT b.*, row_number() OVER (
        PARTITION BY b.company_name ORDER BY i.batch_seq DESC, b.source_row_number DESC) AS rn
    FROM bronze_company_metadata b JOIN ingest_batches i USING (batch_id)
) WHERE rn = 1;
"""


def connect(db_path: Optional[Path] = None) -> duckdb.DuckDBPyConnection:
    """Open (or create) the warehouse. ``None`` -> in-memory (tests)."""
    if db_path is not None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path) if db_path else ":memory:")
    con.execute(DDL)
    return con


# --------------------------------------------------------------------------- bronze
def ingest_batch(con: duckdb.DuckDBPyConnection, batch: Batch) -> int:
    """Idempotently append a batch to bronze. Returns # of new record versions."""
    table = "bronze_articles" if batch.source_type == "articles" else "bronze_company_metadata"
    key = "article_id" if batch.source_type == "articles" else "company_name"
    con.begin()
    try:
        con.register("incoming", batch.frame)
        cols = ", ".join(batch.frame.columns)
        new_versions = con.execute(f"""
            SELECT count(*) FROM (SELECT DISTINCT {key}, record_hash FROM incoming) n
            WHERE NOT EXISTS (SELECT 1 FROM {table} b WHERE b.{key} = n.{key} AND b.record_hash = n.record_hash)
        """).fetchone()[0]
        con.execute(f"INSERT OR IGNORE INTO {table} ({cols}) SELECT {cols} FROM incoming")
        con.execute("""
            INSERT OR IGNORE INTO ingest_batches
            SELECT ?, coalesce((SELECT max(batch_seq) FROM ingest_batches), 0) + 1, ?, ?, ?, ?, ?, now()
        """, [batch.batch_id, batch.source_type, batch.source_file, batch.file_sha256,
              batch.row_count, new_versions])
        con.unregister("incoming")
        con.commit()
    except Exception:
        con.rollback()
        raise
    return int(new_versions)


def current_bronze(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, pd.DataFrame]:
    articles = con.execute("SELECT * FROM bronze_articles_current ORDER BY article_id").df()
    metadata = con.execute("SELECT * FROM bronze_company_metadata_current ORDER BY company_name").df()
    return articles, metadata


# --------------------------------------------------------------------------- silver
def load_silver(con: duckdb.DuckDBPyConnection, tables: dict[str, pd.DataFrame]) -> None:
    for name, df in tables.items():
        # An all-null object column would otherwise be inferred as INTEGER by DuckDB
        # (schema would then flip between runs depending on the data). Pin it to text.
        df = df.copy()
        for col in df.columns:
            if df[col].dtype == object and df[col].isna().all():
                df[col] = df[col].astype("string")
        con.register("_df", df)
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _df")
        con.unregister("_df")


# --------------------------------------------------------------------------- gold
GOLD_SQL = """
-- Grain: one row per resolved company identity (metadata companies + article-only companies).
CREATE OR REPLACE TABLE dim_company AS
SELECT company_id, company_name, TRUE AS in_metadata, founded_year, headquarters, employee_count,
       industry_raw, industry, is_public, stock_ticker, company_size_category, metadata_quality_notes,
       source_batch_id, source_record_hash
FROM silver_company_metadata
UNION ALL
SELECT company_id, min(canonical_name), FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
       'not_in_metadata', NULL, NULL
FROM silver_company_resolution
WHERE company_id IS NOT NULL AND NOT in_metadata
  AND company_id NOT IN (SELECT company_id FROM silver_company_metadata)
GROUP BY company_id;

-- Grain: one row per VALID reported ARR observation (= one article reporting ARR for one
-- company as of its published_date). Only rows with a valid amount, a valid date and a
-- resolved company qualify; everything else is explained in dq_issues.
CREATE OR REPLACE TABLE fct_arr_observation AS
SELECT
    'ARR-' || a.article_id                 AS observation_id,
    a.company_id,
    a.article_id,
    a.published_date                       AS as_of_date,
    a.published_year                       AS as_of_year,
    a.published_quarter                    AS as_of_quarter,
    a.year_quarter,
    p.arr_usd,
    p.arr_usd_low,
    p.arr_usd_high,
    p.is_range,
    p.currency_original,
    p.amount_original,
    p.fx_rate_to_usd,
    p.fx_rate_set,
    p.revenue_raw,
    'article_reported'                     AS observation_type,
    a.company_name_raw,
    a.company_match_method,
    a.company_relationship,
    a.date_is_ambiguous,
    a.date_alternative,
    count(*) OVER (PARTITION BY a.company_id, a.published_date) AS same_day_observation_count,
    a.url                                  AS source_url,
    a.source_batch_id,
    a.source_file,
    a.source_row_number,
    a.source_record_hash,
    p.parser_version
FROM silver_articles a
JOIN silver_arr_parse p USING (article_id)
WHERE p.parse_status = 'valid' AND a.date_status = 'valid' AND a.company_id IS NOT NULL;

-- Grain: one row per article (current version). Wide, filter-friendly analytical table.
CREATE OR REPLACE TABLE article_enriched AS
SELECT
    a.article_id, a.title,
    coalesce(d.company_name, a.company_name_raw) AS company_name,
    a.company_name_raw, a.company_id, a.company_match_method, a.company_relationship,
    a.published_date, a.published_year, a.published_quarter, a.published_month, a.year_quarter,
    a.date_status, a.date_is_ambiguous, a.published_date_raw,
    a.category, a.category_raw,
    p.parse_status AS arr_status,
    CASE WHEN p.parse_status = 'valid' THEN p.arr_usd END AS arr_usd,
    p.revenue_raw, p.currency_original,
    f.observation_id AS arr_observation_id,
    a.summary, a.url, a.author, a.word_count,
    d.industry, d.industry_raw, d.founded_year, d.headquarters, d.employee_count,
    d.is_public, d.stock_ticker, d.company_size_category, coalesce(d.in_metadata, FALSE) AS in_metadata,
    CASE WHEN a.published_year - d.founded_year >= 0 THEN a.published_year - d.founded_year END AS company_age,
    (a.category = 'AI_ML' OR coalesce(d.industry = 'AI_ML', FALSE)) AS is_ai_related,
    a.source_batch_id, a.source_row_number, a.source_record_hash, a.parser_version
FROM silver_articles a
JOIN silver_arr_parse p USING (article_id)
LEFT JOIN dim_company d USING (company_id)
LEFT JOIN fct_arr_observation f ON f.article_id = a.article_id;

-- Grain: one row per company. Latest ARR = observation with the max as_of_date.
-- Ties (same day): prefer unambiguous dates, then highest article_id. Product/segment
-- aliases (e.g. Azure -> Microsoft) are excluded so they don't masquerade as the parent's ARR.
CREATE OR REPLACE TABLE company_arr_latest AS
WITH obs AS (
    SELECT f.*,
           count(*) OVER (PARTITION BY company_id) AS observation_count,
           min(as_of_date) OVER (PARTITION BY company_id) AS first_as_of_date,
           lead(arr_usd) OVER w AS previous_arr_usd,
           lead(as_of_date) OVER w AS previous_as_of_date,
           row_number() OVER w AS rn
    FROM fct_arr_observation f
    WHERE company_relationship = 'self'
    WINDOW w AS (PARTITION BY company_id ORDER BY as_of_date DESC, date_is_ambiguous ASC, article_id DESC)
)
SELECT o.company_id, d.company_name, d.industry, d.in_metadata,
       o.as_of_date AS latest_as_of_date, o.arr_usd AS latest_arr_usd,
       o.observation_id AS latest_observation_id, o.article_id AS latest_article_id,
       o.revenue_raw AS latest_revenue_raw, o.source_url AS latest_source_url,
       o.previous_as_of_date, o.previous_arr_usd,
       o.first_as_of_date, o.observation_count
FROM obs o JOIN dim_company d USING (company_id)
WHERE o.rn = 1;

-- Grain: one row per company x calendar quarter WITH at least one observation (no forward fill).
-- arr_usd_end_of_quarter = last observation in the quarter (same tie-break as latest).
CREATE OR REPLACE TABLE company_arr_quarterly AS
WITH ranked AS (
    SELECT f.*, row_number() OVER (
        PARTITION BY company_id, year_quarter
        ORDER BY as_of_date DESC, date_is_ambiguous ASC, article_id DESC) AS rn
    FROM fct_arr_observation f
    WHERE company_relationship = 'self'
), q AS (
    SELECT company_id, as_of_year AS year, as_of_quarter AS quarter, year_quarter,
           max(arr_usd) FILTER (WHERE rn = 1)        AS arr_usd_end_of_quarter,
           max(observation_id) FILTER (WHERE rn = 1) AS end_of_quarter_observation_id,
           max(as_of_date)                           AS last_observation_date,
           count(*)                                  AS observation_count,
           min(arr_usd)                              AS arr_usd_min,
           max(arr_usd)                              AS arr_usd_max,
           CAST(round(median(arr_usd)) AS BIGINT)    AS arr_usd_median,
           bool_or(date_is_ambiguous)                AS has_ambiguous_dates,
           string_agg(observation_id, ',' ORDER BY as_of_date, observation_id) AS observation_ids
    FROM ranked GROUP BY ALL
)
SELECT q.*,
       lag(arr_usd_end_of_quarter) OVER (PARTITION BY company_id ORDER BY year, quarter) AS prior_observed_quarter_arr_usd,
       lag(year_quarter) OVER (PARTITION BY company_id ORDER BY year, quarter)            AS prior_observed_quarter
FROM q;

-- Grain: one row per (entity, issue). Every excluded/suspicious record is explained here.
CREATE OR REPLACE TABLE dq_issues AS
SELECT 'article' AS entity_type, article_id AS entity_id, 'arr_' || parse_status AS issue_type,
       CASE WHEN parse_status = 'unparseable' THEN 'error' ELSE 'info' END AS severity,
       coalesce(error_reason, 'no ARR value reported; excluded from fct_arr_observation') AS detail,
       revenue_raw AS raw_value
FROM silver_arr_parse WHERE parse_status <> 'valid'
UNION ALL
SELECT 'article', article_id, 'date_' || date_status, 'error',
       'published_date could not be used; excluded from time-based outputs', published_date_raw
FROM silver_articles WHERE date_status <> 'valid'
UNION ALL
SELECT 'article', article_id, 'date_ambiguous', 'warning',
       'day/month order ambiguous; read as ' || date_format || ', alternative ' || CAST(date_alternative AS VARCHAR),
       published_date_raw
FROM silver_articles WHERE date_is_ambiguous
UNION ALL
SELECT 'article', article_id, 'company_' || company_match_method,
       CASE company_match_method WHEN 'ambiguous' THEN 'error' WHEN 'missing' THEN 'error'
                                 WHEN 'unmatched' THEN 'warning' ELSE 'info' END,
       CASE company_match_method
            WHEN 'unmatched' THEN 'company not in metadata; enrichment fields are null'
            WHEN 'ambiguous' THEN 'company name references multiple entities; not attributed'
            ELSE 'company name variant resolved to ' || coalesce(company_id, '?') END,
       company_name_raw
FROM silver_articles WHERE company_match_method <> 'exact'
UNION ALL
SELECT 'article', article_id, 'company_product_of_parent', 'warning',
       'name is a product/segment of ' || company_id || '; excluded from latest/quarterly ARR views',
       company_name_raw
FROM silver_articles WHERE company_relationship = 'product_of_parent'
UNION ALL
SELECT 'article', a.article_id, 'arr_valid_but_not_modeled', 'warning',
       'valid ARR value excluded from fct_arr_observation (unresolved company or unusable date)',
       p.revenue_raw
FROM silver_articles a JOIN silver_arr_parse p USING (article_id)
WHERE p.parse_status = 'valid' AND (a.company_id IS NULL OR a.date_status <> 'valid')
UNION ALL
SELECT 'article', article_id, 'category_unmapped', 'warning', 'category not in taxonomy; mapped to OTHER', category_raw
FROM silver_articles WHERE category = 'OTHER' OR category IS NULL
UNION ALL
SELECT 'article', article_id, 'company_age_negative', 'warning',
       'founded_year is after published_date; company_age set to null', CAST(founded_year AS VARCHAR)
FROM article_enriched WHERE company_age IS NULL AND founded_year IS NOT NULL AND published_year IS NOT NULL
UNION ALL
SELECT 'observation', observation_id, 'arr_multiple_same_day', 'info',
       CAST(same_day_observation_count AS VARCHAR) || ' observations for this company on ' || CAST(as_of_date AS VARCHAR),
       revenue_raw
FROM fct_arr_observation WHERE same_day_observation_count > 1
UNION ALL
SELECT 'company', company_id, unnest(string_split(metadata_quality_notes, ';')), 'warning',
       'inconsistent company metadata (is_public vs stock_ticker)', stock_ticker
FROM silver_company_metadata WHERE metadata_quality_notes IS NOT NULL;
"""


def build_gold(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(GOLD_SQL)


# --------------------------------------------------------------------------- checks / export
def assert_grain(con: duckdb.DuckDBPyConnection, tables: Optional[list[str]] = None) -> None:
    """Fail loudly if any table has duplicate or null keys for its declared grain."""
    existing = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    for table, keys in TABLE_GRAIN.items():
        if (tables and table not in tables) or table not in existing:
            continue
        cols = ", ".join(keys)
        dupes = con.execute(
            f"SELECT count(*) FROM (SELECT {cols} FROM {table} GROUP BY {cols} HAVING count(*) > 1)"
        ).fetchone()[0]
        nulls = con.execute(
            f"SELECT count(*) FROM {table} WHERE " + " OR ".join(f"{k} IS NULL" for k in keys)
        ).fetchone()[0]
        if dupes or nulls:
            raise AssertionError(f"grain violation in {table} on ({cols}): {dupes} duplicate keys, {nulls} null keys")


def row_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    existing = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in EXPORT_TABLES if t in existing}


def export_tables(con: duckdb.DuckDBPyConnection, out_dir: Path) -> dict[str, Path]:
    """Write every modeled table to <out_dir>/<table>.csv, deterministically ordered."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written = {}
    for table in EXPORT_TABLES:
        order = ", ".join(TABLE_GRAIN[table])
        select = f"SELECT * FROM {table}"
        if table == "article_embeddings":  # JSON arrays are the most portable CSV encoding
            select = ("SELECT * REPLACE (to_json(embedding) AS embedding, "
                      "to_json(top_similar_articles) AS top_similar_articles, "
                      "to_json(top_similar_scores) AS top_similar_scores) FROM article_embeddings")
        path = out_dir / f"{table}.csv"
        con.execute(f"COPY ({select} ORDER BY {order}) TO '{path}' (HEADER, DELIMITER ',')")
        written[table] = path
    return written
